"""Unit tests for the 企业微信 (WeCom) Channel Adapter protocol layer."""

import base64
import json

import pytest

from trpc_service.channels.wecom import (
    WeComCrypto,
    WeComProtocolError,
    WeComRateLimiter,
    WeComStreamBatcher,
    WeComStreamSession,
    decode_aes_key,
    normalize_to_inbound,
    parse_event,
    wecom_message_signature,
)

TOKEN = "wecom-smoke-token"
AES_KEY = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"
RECEIVE_ID = "wecom-smoke-corpus"

SINGLE_PLAINTEXT = json.dumps(
    {
        "msgtype": "text",
        "msgid": "msg-single-1",
        "aibotid": "wecom-bot-1",
        "chattype": "single",
        "from": {"userid": "user-zhang"},
        "text": {"content": "帮我看下今天的日程"},
        "response_url": "https://qyapi.weixin.qq.com/cgi-bin/aibot/response",
    },
    ensure_ascii=False,
)


def test_decode_aes_key_rejects_wrong_lengths() -> None:
    assert len(decode_aes_key(AES_KEY)) == 32
    with pytest.raises(WeComProtocolError, match="WECOM_AES_KEY_INVALID"):
        decode_aes_key("too-short")
    with pytest.raises(WeComProtocolError, match="WECOM_AES_KEY_INVALID"):
        decode_aes_key("!!!not-base64!!!")


def test_message_signature_is_sha1_over_sorted_join() -> None:
    import hashlib

    parts = [TOKEN, "1756900000", "nonce-1", "cipher"]
    expected = hashlib.sha1("".join(sorted(parts)).encode()).hexdigest()
    assert wecom_message_signature(TOKEN, "1756900000", "nonce-1", "cipher") == expected


def test_crypto_round_trip_preserves_payload_and_receive_id() -> None:
    crypto = WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID)
    encrypted = crypto.encrypt(SINGLE_PLAINTEXT)
    assert crypto.decrypt(encrypted) == SINGLE_PLAINTEXT


def test_verify_rejects_tampered_or_forged_callbacks() -> None:
    crypto = WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID)
    encrypted = crypto.encrypt(SINGLE_PLAINTEXT)
    signature = wecom_message_signature(TOKEN, "1756900000", "nonce-1", encrypted)
    crypto.verify(timestamp="1756900000", nonce="nonce-1", encrypted=encrypted, signature=signature)
    with pytest.raises(WeComProtocolError, match="WECOM_SIGNATURE_INVALID"):
        crypto.verify(
            timestamp="1756900000",
            nonce="nonce-1",
            encrypted=encrypted,
            signature="0" * 40,
        )
    with pytest.raises(WeComProtocolError, match="WECOM_SIGNATURE_INVALID"):
        other = WeComCrypto(token="other", encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID)
        other.verify(
            timestamp="1756900000",
            nonce="nonce-1",
            encrypted=encrypted,
            signature=signature,
        )


def test_decrypt_fails_closed_on_foreign_key_or_receive_id() -> None:
    crypto = WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID)
    encrypted = crypto.encrypt(SINGLE_PLAINTEXT)
    short_key = base64.b64encode(b"0123456789abcdef").decode()
    with pytest.raises(WeComProtocolError, match="WECOM_AES_KEY_INVALID"):
        foreign = WeComCrypto(token=TOKEN, encoding_aes_key=short_key, receive_id=RECEIVE_ID)
        foreign.decrypt(encrypted)
    strict = WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id="another-corp")
    with pytest.raises(WeComProtocolError, match="WECOM_RECEIVE_ID_MISMATCH"):
        strict.decrypt(encrypted)


def test_normalization_maps_single_and_group_messages() -> None:
    event = parse_event(SINGLE_PLAINTEXT)
    assert event.is_text_message and not event.is_revoke
    assert event.chattype == "single"
    normalized = normalize_to_inbound(event)
    assert normalized == {
        "channel_type": "WECOM",
        "external_bot_id": "wecom-bot-1",
        "message_key": "msg-single-1",
        "text": "帮我看下今天的日程",
        "external_user_id": "user-zhang",
        "session_key": "direct:user-zhang",
    }
    group = parse_event(
        json.dumps(
            {
                "msgtype": "text",
                "msgid": "msg-group-1",
                "aibotid": "wecom-bot-1",
                "chattype": "group",
                "chatid": "wr-room-1",
                "from": {"userid": "user-li"},
                "text": {"content": "会议室有空吗"},
            }
        )
    )
    assert group.chattype == "group" and group.chatid == "wr-room-1"
    assert normalize_to_inbound(group)["session_key"] == "group:wr-room-1"


def test_group_normalization_rejects_a_missing_stable_chat_id() -> None:
    event = parse_event(
        json.dumps(
            {
                "msgtype": "text",
                "msgid": "msg-group-without-chat-id",
                "aibotid": "wecom-bot-1",
                "chattype": "group",
                "from": {"userid": "user-li"},
                "text": {"content": "hello"},
            }
        )
    )

    with pytest.raises(WeComProtocolError, match="WECOM_GROUP_CHAT_ID_REQUIRED"):
        normalize_to_inbound(event)


def test_revoke_events_are_recognized() -> None:
    event = parse_event(
        json.dumps(
            {
                "msgtype": "event",
                "msgid": "msg-event-1",
                "aibotid": "wecom-bot-1",
                "event": {"type": "revoke", "revoke_msgid": "msg-single-1"},
            }
        )
    )
    assert event.is_revoke


def test_rate_limiter_bounds_the_burst_and_refills() -> None:
    limiter = WeComRateLimiter(capacity=3, refill_per_second=0.001)
    acquired = [limiter.try_acquire() for _ in range(5)]
    assert acquired == [True, True, True, False, False]
    assert limiter.retry_after() > 0.0


def _recording_session(recording: list[dict]) -> WeComStreamSession:
    from trpc_service.channels.wecom import WeComRateLimiter

    class RecordingHttp:
        async def post(self, url: str, json: dict, timeout: float) -> object:
            recording.append(json)
            return object()

    return WeComStreamSession(
        RecordingHttp(),
        response_url="https://qyapi.weixin.qq.com/cgi-bin/aibot/response",
        batcher=WeComStreamBatcher(min_chars=10_000, min_interval_seconds=0.0),
        limiter=WeComRateLimiter(capacity=1000, refill_per_second=1000),
    )


async def test_stream_session_coalesces_deltas_without_per_token_calls() -> None:
    recording: list[dict] = []
    session = _recording_session(recording)
    for index in range(50):
        calls = await session.append(f"token-{index} ")
        assert calls == 0  # no single token ever triggers an API call
    await session.flush()
    assert len(recording) <= 3
    merged = "".join(item["stream"]["content"] for item in recording)
    assert merged == "".join(f"token-{index} " for index in range(50))
    assert await session.flush() == 0  # idempotent: nothing pending


def test_batcher_flush_thresholds() -> None:
    now = {"value": 0.0}
    batcher = WeComStreamBatcher(min_chars=10, min_interval_seconds=5.0, clock=lambda: now["value"])
    batcher.add("short")
    assert not batcher.due()
    batcher.add("x" * 20)
    assert not batcher.due()  # size met, interval not elapsed
    now["value"] = 6.0
    assert batcher.due()
    assert batcher.take_pending() == "short" + "x" * 20
    assert not batcher.has_pending()


async def test_transport_outcomes_cover_timeout_429_and_errors() -> None:
    from trpc_service.channels.wecom import WeComReplyTransport

    class StubHttp:
        def __init__(self, behaviour: str) -> None:
            self._behaviour = behaviour

        async def post(self, url: str, json: dict, timeout: float) -> object:
            if self._behaviour == "timeout":
                raise TimeoutError()
            if self._behaviour == "error":
                raise RuntimeError("connection reset")
            if self._behaviour == "429":
                return type("R", (), {"status_code": 429})()
            return type("R", (), {"status_code": 200})()

    def make_delivery() -> object:
        from trpc_service.channels.delivery import ReplyDelivery

        return ReplyDelivery(
            tenant_id="t",
            delivery_id="d",
            binding_id="b",
            execution_id="e",
            external_conversation_id="https://im.test/response",
            content="hello",
            created_at="2026-09-06T00:00:00+00:00",
        )

    delivery = make_delivery()
    limited = WeComReplyTransport(StubHttp("429"), limiter=WeComRateLimiter(capacity=50))
    outcome = await limited.send(delivery, 1)
    assert outcome.delivered is False and outcome.rate_limited
    timeout_transport = WeComReplyTransport(
        StubHttp("timeout"), limiter=WeComRateLimiter(capacity=50)
    )
    outcome = await timeout_transport.send(delivery, 1)
    assert outcome.outcome_unknown and outcome.error_code == "WECOM_REPLY_TIMEOUT"
    failing = WeComReplyTransport(StubHttp("error"), limiter=WeComRateLimiter(capacity=50))
    outcome = await failing.send(delivery, 1)
    assert outcome.error_code == "WECOM_REPLY_FAILED"
    ok = WeComReplyTransport(StubHttp("ok"), limiter=WeComRateLimiter(capacity=50))
    assert (await ok.send(delivery, 1)).delivered


def test_decrypt_rejects_truncated_and_unpadded_ciphertexts() -> None:
    crypto = WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID)
    import base64

    for broken in (base64.b64encode(b"short").decode(), base64.b64encode(b"x" * 64).decode()):
        with pytest.raises(WeComProtocolError, match="WECOM_DECRYPT_FAILED"):
            crypto.decrypt(broken)


def test_parse_event_rejects_non_json_payloads() -> None:
    with pytest.raises(WeComProtocolError, match="WECOM_EVENT_INVALID"):
        parse_event("not-json")
    with pytest.raises(WeComProtocolError, match="WECOM_EVENT_INVALID"):
        parse_event(json.dumps(["a", "list"]))


async def test_stream_session_waits_for_rate_limiter_tokens() -> None:
    import asyncio

    from trpc_service.channels.wecom import WeComRateLimiter

    recording: list[dict] = []

    class SlowHttp:
        async def post(self, url: str, json: dict, timeout: float) -> object:
            recording.append(json)
            return object()

    session = WeComStreamSession(
        SlowHttp(),
        response_url="https://im.test/response",
        batcher=WeComStreamBatcher(min_chars=1, min_interval_seconds=0.0),
        limiter=WeComRateLimiter(capacity=1, refill_per_second=500.0),
    )
    await session.append("delta-1")
    await asyncio.sleep(0.05)
    await session.append("delta-2")
    assert len(recording) == 2
