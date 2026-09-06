"""企业微信 (WeCom) Channel Adapter.

Implements the WeCom smart-bot callback protocol over the channel platform:
SHA-1 signature verification, AES-256-CBC message decryption and encryption
(official EncodingAESKey scheme), event normalization into the channel
inbound ledger, revoke (撤回) handling, per-bot reply rate limiting and a
stream coalescer that merges model deltas so WeCom rate limits never cause
per-token API calls. Single-chat replies use merged incremental updates;
group chats answer with a processing notice plus the final reply.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
from collections.abc import Callable
from typing import Any

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pydantic import BaseModel, ConfigDict

AES_KEY_BYTES = 32
RANDOM_PREFIX_BYTES = 16
LENGTH_PREFIX_BYTES = 4


class WeComProtocolError(RuntimeError):
    """Safe, stable error for callers; never embeds key material."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def wecom_message_signature(token: str, timestamp: str, nonce: str, encrypted: str) -> str:
    """The official WeCom callback signature: SHA-1 over the sorted join."""

    return hashlib.sha1("".join(sorted([token, timestamp, nonce, encrypted])).encode()).hexdigest()


def decode_aes_key(encoding_aes_key: str) -> bytes:
    """Decode the 43-char EncodingAESKey into the 32-byte AES key."""

    try:
        key = base64.b64decode(encoding_aes_key + "=")
    except (ValueError, TypeError) as error:
        raise WeComProtocolError("WECOM_AES_KEY_INVALID") from error
    if len(key) != AES_KEY_BYTES:
        raise WeComProtocolError("WECOM_AES_KEY_INVALID")
    return key


def _pkcs7_pad(payload: bytes, block: int = AES_KEY_BYTES) -> bytes:
    padding = block - (len(payload) % block)
    return payload + bytes([padding]) * padding


def _pkcs7_unpad(payload: bytes, block: int = AES_KEY_BYTES) -> bytes:
    padding = payload[-1]
    if not 1 <= padding <= block:
        raise WeComProtocolError("WECOM_DECRYPT_FAILED")
    if payload[-padding:] != bytes([padding]) * padding:
        raise WeComProtocolError("WECOM_DECRYPT_FAILED")
    return payload[:-padding]


class WeComCrypto:
    """Official WeCom callback crypto: verify, decrypt and encrypt messages."""

    def __init__(self, *, token: str, encoding_aes_key: str, receive_id: str = "") -> None:
        self._token = token
        self._key = decode_aes_key(encoding_aes_key)
        self._receive_id = receive_id

    def verify(self, *, timestamp: str, nonce: str, encrypted: str, signature: str) -> None:
        expected = wecom_message_signature(self._token, timestamp, nonce, encrypted)
        if not hmac_compare(expected, signature):
            raise WeComProtocolError("WECOM_SIGNATURE_INVALID")

    def decrypt(self, encrypted: str) -> str:
        try:
            cipher_text = base64.b64decode(encrypted)
            decryptor = Cipher(
                algorithms.AES(self._key), modes.CBC(self._key[: AES_KEY_BYTES // 2])
            ).decryptor()
            padded = decryptor.update(cipher_text) + decryptor.finalize()
            plain = _pkcs7_unpad(padded)
        except (ValueError, TypeError) as error:
            raise WeComProtocolError("WECOM_DECRYPT_FAILED") from error
        if len(plain) < RANDOM_PREFIX_BYTES + LENGTH_PREFIX_BYTES:
            raise WeComProtocolError("WECOM_DECRYPT_FAILED")
        body = plain[RANDOM_PREFIX_BYTES:]
        msg_len = int.from_bytes(body[:LENGTH_PREFIX_BYTES], "big")
        message = body[LENGTH_PREFIX_BYTES : LENGTH_PREFIX_BYTES + msg_len]
        receive_id = body[LENGTH_PREFIX_BYTES + msg_len :]
        if self._receive_id and receive_id.decode("utf-8", "replace") != self._receive_id:
            raise WeComProtocolError("WECOM_RECEIVE_ID_MISMATCH")
        return message.decode("utf-8", "replace")

    def encrypt(self, plaintext: str) -> str:
        message = plaintext.encode("utf-8")
        body = (
            os.urandom(RANDOM_PREFIX_BYTES)
            + len(message).to_bytes(LENGTH_PREFIX_BYTES, "big")
            + message
            + self._receive_id.encode("utf-8")
        )
        encryptor = Cipher(
            algorithms.AES(self._key), modes.CBC(self._key[: AES_KEY_BYTES // 2])
        ).encryptor()
        encrypted = encryptor.update(_pkcs7_pad(body)) + encryptor.finalize()
        return base64.b64encode(encrypted).decode("utf-8")


def hmac_compare(expected: str, received: str) -> bool:
    return hmac.compare_digest(expected, received)


class WeComEvent(BaseModel):
    """One decrypted WeCom smart-bot callback event."""

    model_config = ConfigDict(extra="allow", frozen=True)

    msgtype: str
    msgid: str = ""
    aibotid: str = ""
    chattype: str = "single"
    chatid: str = ""
    from_userid: str = ""
    text_content: str = ""
    response_url: str = ""
    revoke_msgid: str = ""
    timestamp: str = ""

    @property
    def is_revoke(self) -> bool:
        return self.msgtype == "event" and bool(self.revoke_msgid)

    @property
    def is_text_message(self) -> bool:
        return self.msgtype == "text"


def parse_event(plaintext: str) -> WeComEvent:
    """Normalize a decrypted callback payload into the adapter event model."""

    try:
        payload = json.loads(plaintext)
    except json.JSONDecodeError as error:
        raise WeComProtocolError("WECOM_EVENT_INVALID") from error
    if not isinstance(payload, dict):
        raise WeComProtocolError("WECOM_EVENT_INVALID")
    event = payload.get("event") or {}
    from_field = payload.get("from") or {}
    text = payload.get("text") or {}
    return WeComEvent(
        msgtype=str(payload.get("msgtype", "")),
        msgid=str(payload.get("msgid", "")),
        aibotid=str(payload.get("aibotid", "")),
        chattype=str(payload.get("chattype", "single")),
        chatid=str(payload.get("chatid", "")),
        from_userid=str(from_field.get("userid", "")),
        text_content=str(text.get("content", "")),
        response_url=str(payload.get("response_url", "")),
        revoke_msgid=str(event.get("revoke_msgid", "")) if isinstance(event, dict) else "",
        timestamp=str(payload.get("timestamp", "")),
    )


def normalize_to_inbound(event: WeComEvent) -> dict[str, str]:
    """Normalize one verified WeCom message into the internal ledger shape.

    The WeCom callback signature was already verified at the protocol layer;
    the caller obtains the ledger integrity signature through the inbound
    service so it is derived from the same binding secret material.
    """

    return {
        "channel_type": "WECOM",
        "external_bot_id": event.aibotid,
        "message_key": event.msgid,
        "text": event.text_content,
        "external_user_id": event.from_userid,
    }


class WeComRateLimiter:
    """Token bucket honoring WeCom per-bot reply limits."""

    def __init__(self, *, capacity: int = 20, refill_per_second: float = 20 / 60) -> None:
        self._capacity = float(capacity)
        self._refill_per_second = refill_per_second
        self._tokens = float(capacity)
        self._last = time.monotonic()

    def try_acquire(self) -> bool:
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def retry_after(self) -> float:
        missing = max(1.0 - self._tokens, 0.0)
        return missing / self._refill_per_second

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(
            self._capacity, self._tokens + (now - self._last) * self._refill_per_second
        )
        self._last = now


class WeComStreamBatcher:
    """Coalesce model deltas into merged increments for WeCom.

    企业微信 limits reply calls: merged incremental updates are flushed only
    when both the character and time thresholds are met, and the final reply
    always flushes exactly once. Per-token API calls are impossible by
    construction.
    """

    def __init__(
        self,
        *,
        min_chars: int = 256,
        min_interval_seconds: float = 2.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._min_chars = min_chars
        self._min_interval = min_interval_seconds
        self._clock = clock or time.monotonic
        self._pending: list[str] = []
        self._pending_chars = 0
        self._last_flush = self._clock()

    def add(self, delta: str) -> None:
        self._pending.append(delta)
        self._pending_chars += len(delta)

    def due(self) -> bool:
        if not self._pending:
            return False
        if self._pending_chars < self._min_chars:
            return False
        return self._clock() - self._last_flush >= self._min_interval

    def take_pending(self) -> str:
        merged = "".join(self._pending)
        self._pending = []
        self._pending_chars = 0
        self._last_flush = self._clock()
        return merged

    def has_pending(self) -> bool:
        return bool(self._pending)


GROUP_PROCESSING_NOTICE = "处理中,请稍候"


class WeComReplyTransport:
    """ReplyTransport posting final replies to the WeCom response URL.

    Every send respects the per-bot rate limiter: a exhausted bucket returns
    a rate-limited outcome so the delivery state machine backs off without
    consuming the failure budget. Timeouts park the delivery as
    OUTCOME_UNKNOWN so reconciliation can decide before any resend.
    """

    def __init__(
        self,
        http: Any,
        *,
        limiter: WeComRateLimiter,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._http = http
        self._limiter = limiter
        self._timeout = timeout_seconds

    async def send(self, delivery: Any, attempt_no: int) -> Any:
        from trpc_service.channels.delivery import ChannelTransportOutcome

        del attempt_no
        if not self._limiter.try_acquire():
            return ChannelTransportOutcome(
                delivered=False, rate_limited=True, error_code="WECOM_RATE_LIMITED"
            )
        url = delivery.external_conversation_id
        body = {"msgtype": "text", "text": {"content": delivery.content}}
        try:
            response = await self._http.post(url, json=body, timeout=self._timeout)
        except TimeoutError:
            return ChannelTransportOutcome(
                delivered=False, outcome_unknown=True, error_code="WECOM_REPLY_TIMEOUT"
            )
        except Exception:
            return ChannelTransportOutcome(delivered=False, error_code="WECOM_REPLY_FAILED")
        if response.status_code == 429:
            return ChannelTransportOutcome(
                delivered=False, rate_limited=True, error_code="WECOM_RATE_LIMITED"
            )
        if 200 <= response.status_code < 300:
            return ChannelTransportOutcome(delivered=True)
        return ChannelTransportOutcome(
            delivered=False, error_code=f"WECOM_REPLY_HTTP_{response.status_code}"
        )

    def reconcile(self, delivery: Any, attempt_no: int) -> str:
        """WeCom response URLs have no query API: assume the attempt landed."""

        del delivery, attempt_no
        return "delivered"


class WeComStreamSession:
    """Merged-increment updates for one single-chat reply (spec 条目 20).

    Deltas are buffered in the coalescer; a WeCom API call happens only when
    both the character and time thresholds are met, so token-level streaming
    never turns into token-level API calls.
    """

    def __init__(
        self,
        http: Any,
        *,
        response_url: str,
        batcher: WeComStreamBatcher,
        limiter: WeComRateLimiter,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._http = http
        self._response_url = response_url
        self._batcher = batcher
        self._limiter = limiter
        self._timeout = timeout_seconds
        self.calls = 0

    async def append(self, delta: str) -> int:
        self._batcher.add(delta)
        if self._batcher.due():
            merged = self._batcher.take_pending()
            await self._post_increment(merged)
            self.calls += 1
            return 1
        return 0

    async def flush(self) -> int:
        if not self._batcher.has_pending():
            return 0
        merged = self._batcher.take_pending()
        await self._post_increment(merged)
        self.calls += 1
        return 1

    async def _post_increment(self, content: str) -> None:
        await self._limiter_wait()
        await self._http.post(
            self._response_url,
            json={"msgtype": "stream", "stream": {"content": content}},
            timeout=self._timeout,
        )

    async def _limiter_wait(self) -> None:
        while not self._limiter.try_acquire():
            await asyncio.sleep(min(self._limiter.retry_after(), 1.0))
