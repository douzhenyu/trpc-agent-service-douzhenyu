"""Feishu protocol adapter hosted inside the Channel Gateway."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from collections.abc import Callable, Coroutine, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from time import monotonic
from typing import Any, Protocol
from uuid import UUID

from trpc_service.admin_api.database import Database
from trpc_service.agent_gateway import AgentExecutionAccepted
from trpc_service.channels.bindings import ChannelBinding, ChannelBindingRegistry
from trpc_service.channels.delivery import ChannelTransportOutcome, ReplyDelivery
from trpc_service.channels.inbound import ChannelInboundService, InboundError, SecretResolver

_LOGGER = logging.getLogger("uvicorn.error")


class FeishuAdapterError(RuntimeError):
    """Stable, safe error codes at the Feishu protocol boundary."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class FeishuLongConnectionSource(Protocol):
    """Provider boundary for a single Feishu long-connection event stream."""

    async def consume(
        self,
        app_id: str,
        on_event: Callable[[Mapping[str, object]], Coroutine[Any, Any, None]],
    ) -> None: ...

    async def close(self) -> None: ...


class BlockingFeishuClient(Protocol):
    """Small synchronous seam around the official SDK WebSocket client."""

    def start(self) -> None: ...

    def close(self) -> None: ...


BlockingFeishuClientFactory = Callable[[Callable[[object], None]], BlockingFeishuClient]


class LarkSdkLongConnectionSource:
    """Bridge the official synchronous SDK into Channel Gateway's async runtime.

    The SDK acknowledges the provider event immediately; the durable inbound
    pipeline continues on the gateway loop. This keeps provider retries from
    blocking a slow Agent execution while preserving the adapter's own ledger
    and binding checks.
    """

    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        client_factory: BlockingFeishuClientFactory | None = None,
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._client_factory = client_factory
        self._client: BlockingFeishuClient | None = None

    async def consume(
        self,
        app_id: str,
        on_event: Callable[[Mapping[str, object]], Coroutine[Any, Any, None]],
    ) -> None:
        if app_id != self._app_id:
            raise FeishuAdapterError("FEISHU_LONG_CONNECTION_APP_MISMATCH")
        gateway_loop = asyncio.get_running_loop()

        def receive(event: object) -> None:
            future: Future[None] = asyncio.run_coroutine_threadsafe(
                on_event(lark_event_payload(event)), gateway_loop
            )
            future.add_done_callback(_report_background_event_failure)

        def start_client() -> None:
            # The official SDK captures its event loop while the WebSocket
            # client is constructed.  Build and start it in the same worker
            # thread; constructing it on Uvicorn's running loop makes the SDK
            # attempt to nest ``run_until_complete``.
            client = self._build_client(receive)
            self._client = client
            client.start()

        try:
            await asyncio.to_thread(start_client)
        finally:
            self._client = None

    async def close(self) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)

    def _build_client(self, receive: Callable[[object], None]) -> BlockingFeishuClient:
        if self._client_factory is not None:
            return self._client_factory(receive)
        return _OfficialLarkClient(self._app_id, self._app_secret, receive)


def lark_event_payload(event: object) -> dict[str, object]:
    """Convert an official SDK event object into the validated adapter envelope."""

    import lark_oapi as lark  # type: ignore[import-untyped]

    payload = json.loads(lark.JSON.marshal(event))
    if not isinstance(payload, dict):
        raise FeishuAdapterError("FEISHU_EVENT_INVALID")
    return {str(key): value for key, value in payload.items()}


def _report_background_event_failure(future: Future[None]) -> None:
    """Record only the stable failure class; provider payloads never reach logs."""

    try:
        future.result()
    except Exception as error:
        _LOGGER.warning(
            "feishu_long_connection_event_failed error_code=%s",
            getattr(error, "code", type(error).__name__),
        )


class _OfficialLarkClient:
    """Deferred import keeps unit tests independent from the provider package."""

    def __init__(self, app_id: str, app_secret: str, receive: Callable[[object], None]) -> None:
        import lark_oapi as lark

        handler = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(receive)
            .build()
        )
        self._client = lark.ws.Client(
            app_id,
            app_secret,
            log_level=lark.LogLevel.ERROR,
            event_handler=handler,
            auto_reconnect=False,
        )

    def start(self) -> None:
        self._client.start()

    def close(self) -> None:
        import lark_oapi.ws.client as ws_client  # type: ignore[import-untyped]

        if ws_client.loop.is_running():
            asyncio.run_coroutine_threadsafe(self._client._disconnect(), ws_client.loop).result(2)


@dataclass(frozen=True)
class LongConnectionLease:
    connection_key: str
    owner_id: str
    fencing_token: int
    expires_at: float


class LongConnectionLeaseStore(Protocol):
    async def acquire(
        self, connection_key: str, owner_id: str, *, now: float, ttl_seconds: float
    ) -> LongConnectionLease | None: ...

    async def renew(
        self, lease: LongConnectionLease, *, now: float, ttl_seconds: float
    ) -> LongConnectionLease | None: ...

    async def release(self, lease: LongConnectionLease) -> None: ...


class MemoryLongConnectionLeaseStore:
    """Shared test/local store that fences concurrent Gateway instances."""

    def __init__(self) -> None:
        self._leases: dict[str, LongConnectionLease] = {}
        self._tokens: dict[str, int] = {}

    async def acquire(
        self, connection_key: str, owner_id: str, *, now: float, ttl_seconds: float
    ) -> LongConnectionLease | None:
        current = self._leases.get(connection_key)
        if current is not None and current.expires_at > now and current.owner_id != owner_id:
            return None
        if current is not None and current.expires_at > now:
            lease = LongConnectionLease(
                connection_key, owner_id, current.fencing_token, now + ttl_seconds
            )
        else:
            token = self._tokens.get(connection_key, 0) + 1
            self._tokens[connection_key] = token
            lease = LongConnectionLease(connection_key, owner_id, token, now + ttl_seconds)
        self._leases[connection_key] = lease
        return lease

    async def renew(
        self, lease: LongConnectionLease, *, now: float, ttl_seconds: float
    ) -> LongConnectionLease | None:
        if self._leases.get(lease.connection_key) != lease or lease.expires_at <= now:
            return None
        renewed = LongConnectionLease(
            lease.connection_key, lease.owner_id, lease.fencing_token, now + ttl_seconds
        )
        self._leases[lease.connection_key] = renewed
        return renewed

    async def release(self, lease: LongConnectionLease) -> None:
        if self._leases.get(lease.connection_key) == lease:
            self._leases.pop(lease.connection_key)


class DatabaseLongConnectionLeaseStore:
    """Durable tenant-scoped lease store for Feishu long connections."""

    def __init__(self, database: Database, tenant_id: str) -> None:
        self._database = database
        self._tenant_id = UUID(tenant_id)

    async def acquire(
        self, connection_key: str, owner_id: str, *, now: float, ttl_seconds: float
    ) -> LongConnectionLease | None:
        async with self._database.tenant_transaction(self._tenant_id) as connection:
            row = await connection.fetchrow(
                """INSERT INTO tenant.channel_connection_lease
                    (tenant_id,connection_key,owner_id,fencing_token,expires_at)
                    VALUES ($1,$2,$3,1,now() + make_interval(secs => $4))
                    ON CONFLICT (tenant_id,connection_key) DO UPDATE SET
                      owner_id=EXCLUDED.owner_id,
                      fencing_token=CASE
                        WHEN tenant.channel_connection_lease.owner_id=EXCLUDED.owner_id
                          THEN tenant.channel_connection_lease.fencing_token
                        ELSE tenant.channel_connection_lease.fencing_token + 1
                      END,
                      expires_at=EXCLUDED.expires_at,
                      renewed_at=now()
                    WHERE tenant.channel_connection_lease.expires_at <= now()
                      OR tenant.channel_connection_lease.owner_id=EXCLUDED.owner_id
                    RETURNING fencing_token""",
                self._tenant_id,
                connection_key,
                owner_id,
                ttl_seconds,
            )
        if row is None:
            return None
        return LongConnectionLease(
            connection_key, owner_id, int(row["fencing_token"]), now + ttl_seconds
        )

    async def renew(
        self, lease: LongConnectionLease, *, now: float, ttl_seconds: float
    ) -> LongConnectionLease | None:
        async with self._database.tenant_transaction(self._tenant_id) as connection:
            row = await connection.fetchrow(
                """UPDATE tenant.channel_connection_lease
                    SET expires_at=now() + make_interval(secs => $5), renewed_at=now()
                    WHERE tenant_id=$1 AND connection_key=$2 AND owner_id=$3
                      AND fencing_token=$4 AND expires_at > now()
                    RETURNING fencing_token""",
                self._tenant_id,
                lease.connection_key,
                lease.owner_id,
                lease.fencing_token,
                ttl_seconds,
            )
        if row is None:
            return None
        return LongConnectionLease(
            lease.connection_key, lease.owner_id, int(row["fencing_token"]), now + ttl_seconds
        )

    async def release(self, lease: LongConnectionLease) -> None:
        async with self._database.tenant_transaction(self._tenant_id) as connection:
            await connection.execute(
                """UPDATE tenant.channel_connection_lease SET expires_at=now()
                WHERE tenant_id=$1 AND connection_key=$2 AND owner_id=$3 AND fencing_token=$4""",
                self._tenant_id,
                lease.connection_key,
                lease.owner_id,
                lease.fencing_token,
            )


@dataclass(frozen=True)
class FeishuCardResponse:
    """The channel client's safe, protocol-independent send result."""

    delivered: bool = False
    rate_limited: bool = False
    outcome_unknown: bool = False
    error_code: str | None = None


class FeishuCardClient(Protocol):
    async def upsert_card(
        self, *, conversation_id: str, card: dict[str, object], idempotency_key: str
    ) -> FeishuCardResponse: ...

    def reconcile_card(self, idempotency_key: str) -> str: ...


class LarkSdkCardClient:
    """Official SDK-backed reply transport for one inbound Feishu message.

    ``conversation_id`` is intentionally the provider message ID rather than a
    chat identifier: replying to the original message works for both P2P and
    group conversations and lets Feishu retain the reply relationship.  The
    stable delivery ID is passed to the provider as its idempotency UUID.
    """

    def __init__(self, *, app_id: str, app_secret: str) -> None:
        self._app_id = app_id
        self._app_secret = app_secret

    async def upsert_card(
        self, *, conversation_id: str, card: dict[str, object], idempotency_key: str
    ) -> FeishuCardResponse:
        try:
            return await asyncio.to_thread(self._reply, conversation_id, card, idempotency_key)
        except TimeoutError:
            raise
        except Exception:
            return FeishuCardResponse(error_code="FEISHU_CARD_SEND_FAILED")

    def reconcile_card(self, idempotency_key: str) -> str:
        del idempotency_key
        # Feishu's reply endpoint does not expose a lookup keyed by client UUID.
        # Preserve the delivery state machine's fail-safe reconciliation path.
        return "unknown"

    def _reply(
        self,
        message_id: str,
        card: dict[str, object],
        idempotency_key: str,
    ) -> FeishuCardResponse:
        import lark_oapi as lark

        client = (
            lark.Client.builder()
            .app_id(self._app_id)
            .app_secret(self._app_secret)
            .log_level(lark.LogLevel.ERROR)
            .build()
        )
        body = (
            lark.im.v1.ReplyMessageRequestBody.builder()
            .msg_type("interactive")
            .content(json.dumps(card, ensure_ascii=False))
            .uuid(idempotency_key)
            .build()
        )
        request = (
            lark.im.v1.ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(body)
            .build()
        )
        response = client.im.v1.message.reply(request)
        if response.success():
            return FeishuCardResponse(delivered=True)
        return FeishuCardResponse(error_code="FEISHU_CARD_REJECTED")


class FeishuCardTransport:
    """Render reply updates as interactive cards without token-by-token sends."""

    def __init__(
        self,
        *,
        client: FeishuCardClient,
        min_update_interval_seconds: float = 1.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._client = client
        self._min_update_interval_seconds = min_update_interval_seconds
        self._clock = clock
        self._updated_at: dict[str, float] = {}

    async def send(self, delivery: ReplyDelivery, attempt_no: int) -> ChannelTransportOutcome:
        del attempt_no
        now = self._clock()
        last_update = self._updated_at.get(delivery.delivery_id)
        if last_update is not None and now - last_update < self._min_update_interval_seconds:
            return ChannelTransportOutcome(
                False, rate_limited=True, error_code="FEISHU_CARD_THROTTLED"
            )
        try:
            response = await self._client.upsert_card(
                conversation_id=delivery.external_conversation_id,
                card=render_feishu_card(delivery.content),
                idempotency_key=delivery.delivery_id,
            )
        except TimeoutError:
            return ChannelTransportOutcome(
                False, outcome_unknown=True, error_code="FEISHU_CARD_TIMEOUT"
            )
        self._updated_at[delivery.delivery_id] = now
        return ChannelTransportOutcome(
            response.delivered,
            rate_limited=response.rate_limited,
            outcome_unknown=response.outcome_unknown,
            error_code=response.error_code,
        )

    def reconcile(self, delivery: ReplyDelivery, attempt_no: int) -> str:
        del attempt_no
        return self._client.reconcile_card(delivery.delivery_id)


def render_feishu_card(content: str) -> dict[str, object]:
    """Return the minimal interactive-card payload used for an updatable reply."""

    return {
        "config": {"wide_screen_mode": True},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": content}}],
    }


def feishu_webhook_signature(*, token: str, timestamp: str, nonce: str, body: bytes) -> str:
    """Return the HMAC SHA-256 signature over the raw callback envelope."""

    return hmac.new(
        token.encode(), timestamp.encode() + nonce.encode() + body, hashlib.sha256
    ).hexdigest()


class FeishuChannelAdapter:
    """Verify and normalize Feishu events before handing them to the common ledger."""

    def __init__(
        self,
        *,
        tenant_id: str,
        registry: ChannelBindingRegistry,
        secrets: SecretResolver,
        inbound: ChannelInboundService,
        lease_store: LongConnectionLeaseStore | None = None,
        owner_id: str = "channel-gateway",
        lease_ttl_seconds: float = 30.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._tenant_id = tenant_id
        self._registry = registry
        self._secrets = secrets
        self._inbound = inbound
        self._lease_store = lease_store
        self._owner_id = owner_id
        self._lease_ttl_seconds = lease_ttl_seconds
        self._clock = clock
        self._lease: LongConnectionLease | None = None

    async def receive_webhook(
        self, headers: Mapping[str, str], body: bytes
    ) -> AgentExecutionAccepted:
        payload = _parse_payload(body)
        binding = await self._binding_for(payload)
        timestamp = _header(headers, "X-Lark-Request-Timestamp")
        nonce = _header(headers, "X-Lark-Request-Nonce")
        signature = _header(headers, "X-Lark-Signature")
        expected = feishu_webhook_signature(
            token=self._secrets.resolve(binding.secret_ref),
            timestamp=timestamp,
            nonce=nonce,
            body=body,
        )
        if not hmac.compare_digest(signature, expected):
            raise FeishuAdapterError("FEISHU_SIGNATURE_INVALID")
        event = normalize_feishu_message(payload, binding)
        try:
            return await self._inbound.ingest_verified(tenant_id=self._tenant_id, event=event)
        except InboundError as error:
            raise FeishuAdapterError(error.code) from error

    async def acquire_long_connection(self, app_id: str) -> LongConnectionLease:
        if self._lease_store is None:
            raise FeishuAdapterError("FEISHU_LONG_CONNECTION_LEASE_UNAVAILABLE")
        lease = await self._lease_store.acquire(
            self._connection_key(app_id),
            self._owner_id,
            now=self._clock(),
            ttl_seconds=self._lease_ttl_seconds,
        )
        if lease is None:
            raise FeishuAdapterError("FEISHU_LONG_CONNECTION_LEASE_HELD")
        self._lease = lease
        return lease

    async def renew_long_connection(self) -> LongConnectionLease:
        if self._lease_store is None or self._lease is None:
            raise FeishuAdapterError("FEISHU_LONG_CONNECTION_LEASE_NOT_HELD")
        lease = await self._lease_store.renew(
            self._lease, now=self._clock(), ttl_seconds=self._lease_ttl_seconds
        )
        if lease is None:
            self._lease = None
            raise FeishuAdapterError("FEISHU_LONG_CONNECTION_LEASE_LOST")
        self._lease = lease
        return lease

    async def release_long_connection(self) -> None:
        if self._lease_store is not None and self._lease is not None:
            await self._lease_store.release(self._lease)
        self._lease = None

    async def receive_long_connection_event(
        self, payload: Mapping[str, object]
    ) -> AgentExecutionAccepted:
        if self._lease is None or self._lease.expires_at <= self._clock():
            raise FeishuAdapterError("FEISHU_LONG_CONNECTION_LEASE_NOT_HELD")
        copied = {str(key): value for key, value in payload.items()}
        binding = await self._binding_for(copied)
        if self._lease.connection_key != self._connection_key(binding.external_bot_id):
            raise FeishuAdapterError("FEISHU_LONG_CONNECTION_APP_MISMATCH")
        event = normalize_feishu_message(copied, binding)
        try:
            return await self._inbound.ingest_verified(tenant_id=self._tenant_id, event=event)
        except InboundError as error:
            raise FeishuAdapterError(error.code) from error

    async def _binding_for(self, payload: dict[str, object]) -> ChannelBinding:
        header = payload.get("header")
        app_id = header.get("app_id") if isinstance(header, dict) else None
        if not isinstance(app_id, str) or not app_id:
            raise FeishuAdapterError("FEISHU_EVENT_INVALID")
        binding = await self._registry.resolve(
            tenant_id=self._tenant_id, channel_type="FEISHU", external_bot_id=app_id
        )
        if binding is None:
            raise FeishuAdapterError("FEISHU_BINDING_NOT_FOUND")
        return binding

    def _connection_key(self, app_id: str) -> str:
        return f"feishu:{self._tenant_id}:{app_id}"


def normalize_feishu_message(
    payload: Mapping[str, object], binding: ChannelBinding
) -> dict[str, str]:
    """Normalize Feishu direct, group and topic text into the common inbound shape."""

    header = payload.get("header")
    event = payload.get("event")
    if not isinstance(header, Mapping) or not isinstance(event, Mapping):
        raise FeishuAdapterError("FEISHU_EVENT_INVALID")
    sender = event.get("sender")
    message = event.get("message")
    sender_id = sender.get("sender_id") if isinstance(sender, Mapping) else None
    external_user_id = sender_id.get("open_id") if isinstance(sender_id, Mapping) else None
    content = message.get("content") if isinstance(message, Mapping) else None
    try:
        decoded = json.loads(content) if isinstance(content, str) else None
    except json.JSONDecodeError as error:
        raise FeishuAdapterError("FEISHU_CONTENT_INVALID") from error
    text = decoded.get("text") if isinstance(decoded, dict) else None
    event_id, app_id = header.get("event_id"), header.get("app_id")
    chat_type = message.get("chat_type") if isinstance(message, Mapping) else None
    chat_id = message.get("chat_id") if isinstance(message, Mapping) else None
    root_id = message.get("root_id") if isinstance(message, Mapping) else None
    if (
        header.get("event_type") != "im.message.receive_v1"
        or not isinstance(event_id, str)
        or not isinstance(app_id, str)
        or not isinstance(external_user_id, str)
        or not isinstance(text, str)
        or not text
        or not isinstance(message, Mapping)
        or message.get("message_type") != "text"
        or chat_type not in {"p2p", "group"}
    ):
        raise FeishuAdapterError("FEISHU_EVENT_INVALID")
    if chat_type == "p2p":
        session_key = f"direct:{external_user_id}"
    elif not isinstance(chat_id, str) or not chat_id:
        raise FeishuAdapterError("FEISHU_CONVERSATION_INVALID")
    elif isinstance(root_id, str) and root_id:
        session_key = f"thread:{chat_id}:{root_id}"
    else:
        session_key = f"group:{chat_id}"
    return {
        "channel_type": "FEISHU",
        "external_bot_id": app_id,
        "message_key": event_id,
        "text": text,
        "external_user_id": external_user_id,
        "session_key": session_key,
    }


def feishu_reply_message_id(payload: Mapping[str, object]) -> str:
    """Return the provider message ID used as the durable reply target."""

    event = payload.get("event")
    message = event.get("message") if isinstance(event, Mapping) else None
    message_id = message.get("message_id") if isinstance(message, Mapping) else None
    if not isinstance(message_id, str) or not message_id:
        raise FeishuAdapterError("FEISHU_EVENT_INVALID")
    return message_id


def _header(headers: Mapping[str, str], name: str) -> str:
    value = next((value for key, value in headers.items() if key.lower() == name.lower()), None)
    if not value:
        raise FeishuAdapterError("FEISHU_SIGNATURE_INVALID")
    return value


def _parse_payload(body: bytes) -> dict[str, object]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FeishuAdapterError("FEISHU_PAYLOAD_INVALID") from error
    if not isinstance(payload, dict):
        raise FeishuAdapterError("FEISHU_PAYLOAD_INVALID")
    return {str(key): value for key, value in payload.items()}
