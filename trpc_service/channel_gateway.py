"""Channel Gateway: the WeCom data-plane entry for IM traffic.

WeCom smart-bot callbacks are verified and decrypted by the adapter,
normalized into the channel inbound ledger, executed against the pinned
Agent Release through the Runner, and answered through the reconcilable
reply delivery state machine. Reply calls are rate-limited and coalesced:
企业微信 limits never cause per-token API calls — single chats receive
merged incremental updates plus one final tracked delivery, group chats
receive a processing notice first.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from hashlib import sha256
from typing import Annotated, Any, Literal, Protocol

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import PlainTextResponse
from httpx import AsyncClient, HTTPError
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.admin_api.database import Database
from trpc_service.agent.runner import ReleasePinnedRunnerRuntime, RunnerExecutionCommand
from trpc_service.agent_gateway import AgentExecutionSubmitter
from trpc_service.agent_worker import (
    DatabaseDeploymentRouteResolver,
    DatabaseReleaseRouteResolver,
)
from trpc_service.channels.bindings import ChannelBindingRegistry
from trpc_service.channels.delivery import ReplyDeliveryService
from trpc_service.channels.feishu import (
    DatabaseLongConnectionLeaseStore,
    FeishuCardClient,
    FeishuCardTransport,
    FeishuChannelAdapter,
    FeishuLongConnectionSource,
    LarkSdkCardClient,
    LarkSdkLongConnectionSource,
    feishu_reply_message_id,
    normalize_feishu_message,
)
from trpc_service.channels.inbound import ChannelInboundService, InboundError
from trpc_service.channels.store import (
    DatabaseBindingStore,
    DatabaseDeliveryStore,
    DatabaseInboundStore,
)
from trpc_service.channels.wecom import (
    GROUP_PROCESSING_NOTICE,
    WeComCrypto,
    WeComEvent,
    WeComProtocolError,
    WeComRateLimiter,
    WeComReplyTransport,
    WeComStreamBatcher,
    WeComStreamSession,
    parse_event,
)
from trpc_service.channels.wecom_long_connection import (
    DEFAULT_WECOM_WEBSOCKET_URL,
    TextHandler,
    WeComLongConnectionClient,
)
from trpc_service.runtime_health import RuntimeHealthResponse
from trpc_service.version import TRPC_AGENT_VERSION, __version__


class ChannelGatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = ""
    llm_gateway_access_key: str = ""
    llm_gateway_url: str = ""
    wecom_enabled: bool = True
    wecom_token: str = ""
    wecom_encoding_aes_key: str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"
    wecom_transport: Literal["callback", "long_connection"] = "callback"
    wecom_tenant_id: str = ""
    wecom_bot_id: str = ""
    wecom_bot_secret: SecretStr = SecretStr("")
    wecom_websocket_url: str = DEFAULT_WECOM_WEBSOCKET_URL
    wecom_heartbeat_seconds: float = 30.0
    wecom_reconnect_base_seconds: float = 1.0
    wecom_reconnect_max_seconds: float = 30.0
    wecom_max_reconnect_attempts: int = 10
    feishu_enabled: bool = False
    feishu_tenant_id: str = ""
    feishu_app_id: str = ""
    feishu_app_secret: SecretStr = SecretStr("")
    feishu_lease_ttl_seconds: float = 30.0
    feishu_lease_renew_seconds: float = 10.0
    reply_rate_capacity: int = 20
    reply_rate_refill_per_second: float = 20 / 60
    stream_min_chars: int = 256
    stream_min_interval_seconds: float = 2.0

    def validate_runtime(self) -> None:
        required = {"DATABASE_URL": self.database_url}
        if self.wecom_enabled and self.wecom_transport == "long_connection":
            required.update(
                {
                    "WECOM_TENANT_ID": self.wecom_tenant_id,
                    "WECOM_BOT_ID": self.wecom_bot_id,
                    "WECOM_BOT_SECRET": self.wecom_bot_secret.get_secret_value(),
                }
            )
        elif self.wecom_enabled:
            required["WECOM_TOKEN"] = self.wecom_token
        if self.feishu_enabled:
            required.update(
                {
                    "FEISHU_TENANT_ID": self.feishu_tenant_id,
                    "FEISHU_APP_ID": self.feishu_app_id,
                    "FEISHU_APP_SECRET": self.feishu_app_secret.get_secret_value(),
                    "LLM_GATEWAY_URL": self.llm_gateway_url,
                }
            )
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError(f"Channel Gateway configuration is incomplete: {', '.join(missing)}")
        if self.feishu_enabled and not (
            0 < self.feishu_lease_renew_seconds < self.feishu_lease_ttl_seconds
        ):
            raise RuntimeError("FEISHU lease renewal interval must be within its lease TTL")


class DatabaseChannelSecretResolver:
    """Derives stable per-binding signing material from the vault reference.

    The material only seeds the ledger's integrity signature; the production
    secret service resolves the actual WeCom callback token for the
    protocol-level verification that happens before this point.
    """

    def __init__(self, database: Database) -> None:
        del database

    def resolve(self, secret_ref: str) -> str:
        return sha256(secret_ref.encode()).hexdigest()


class LongConnectionRuntime(Protocol):
    async def run(self) -> None: ...

    async def close(self) -> None: ...


LongConnectionFactory = Callable[[TextHandler], LongConnectionRuntime]


class FeishuReplyExecutor(Protocol):
    """Credential-free Release completion boundary used by the Feishu adapter."""

    async def complete(
        self,
        *,
        tenant_id: str,
        application_id: str,
        release_id: str,
        execution_id: str,
        messages: list[dict[str, str]],
    ) -> str: ...


class LLMGatewayReplyExecutor:
    """Call the LLM Gateway without resolving provider credentials locally."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def complete(
        self,
        *,
        tenant_id: str,
        application_id: str,
        release_id: str,
        execution_id: str,
        messages: list[dict[str, str]],
    ) -> str:
        try:
            response = await self._client.post(
                "/internal/v1/llm-completions",
                json={
                    "tenant_id": tenant_id,
                    "application_id": application_id,
                    "release_id": release_id,
                    "execution_id": execution_id,
                    "messages": messages,
                },
            )
            response.raise_for_status()
            payload = response.json()
            choices = payload.get("completion", {}).get("choices", [])
            content = choices[0].get("message", {}).get("content", "") if choices else ""
            if not isinstance(content, str) or not content:
                raise ValueError("completion content missing")
            return content
        except (HTTPError, TypeError, ValueError, KeyError, IndexError) as error:
            raise RuntimeError("FEISHU_LLM_GATEWAY_UNAVAILABLE") from error


def create_app(
    settings: ChannelGatewaySettings | None = None,
    *,
    runner: ReleasePinnedRunnerRuntime | None = None,
    inbound: ChannelInboundService | None = None,
    deliveries: ReplyDeliveryService | None = None,
    database: Database | None = None,
    wecom_long_connection_factory: LongConnectionFactory | None = None,
    feishu_long_connection_source: FeishuLongConnectionSource | None = None,
    feishu_card_client: FeishuCardClient | None = None,
    feishu_reply_executor: FeishuReplyExecutor | None = None,
) -> FastAPI:
    """Create the WeCom Channel Gateway data-plane entry."""

    configured = settings or ChannelGatewaySettings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        configured.validate_runtime()
        active_database = database or Database(configured.database_url)
        if database is None:
            await active_database.open()
        registry = ChannelBindingRegistry(DatabaseBindingStore(active_database))
        application.state.registry = registry
        application.state.inbound = inbound or ChannelInboundService(
            registry=registry,
            secrets=DatabaseChannelSecretResolver(active_database),
            store=DatabaseInboundStore(active_database),
            submitter=AgentExecutionSubmitter(
                active_database,
                DatabaseDeploymentRouteResolver(active_database),
            ),
        )
        application.state.deliveries = deliveries or ReplyDeliveryService(
            store=DatabaseDeliveryStore(active_database),
            transport=WeComReplyTransport(
                _http(application),
                limiter=WeComRateLimiter(
                    capacity=configured.reply_rate_capacity,
                    refill_per_second=configured.reply_rate_refill_per_second,
                ),
            ),
        )
        application.state.runner = runner or ReleasePinnedRunnerRuntime(
            releases=DatabaseReleaseRouteResolver(active_database),
            llm_gateway_access_key=configured.llm_gateway_access_key,
        )
        long_connection: LongConnectionRuntime | None = None
        long_connection_task: asyncio.Task[None] | None = None
        if configured.wecom_enabled and configured.wecom_transport == "long_connection":

            async def handle_long_connection_text(message: dict[str, str]) -> str | None:
                event = WeComEvent(
                    msgtype="text",
                    msgid=message["message_id"],
                    aibotid=message["bot_id"],
                    chattype=message["chat_type"],
                    chatid=message["chat_id"],
                    from_userid=message["from_user_id"],
                    text_content=message["text"],
                    response_url=message["response_url"],
                )
                try:
                    processed = await _process_text_event(
                        application, configured.wecom_tenant_id, event
                    )
                    return processed[1] if processed is not None else None
                except InboundError:
                    # Binding and ledger failures must not become a channel
                    # error detail visible to external users.
                    return None

            long_connection = (
                wecom_long_connection_factory(handle_long_connection_text)
                if wecom_long_connection_factory is not None
                else WeComLongConnectionClient(
                    bot_id=configured.wecom_bot_id,
                    bot_secret=configured.wecom_bot_secret.get_secret_value(),
                    on_text=handle_long_connection_text,
                    url=configured.wecom_websocket_url,
                    heartbeat_seconds=configured.wecom_heartbeat_seconds,
                    reconnect_base_seconds=configured.wecom_reconnect_base_seconds,
                    reconnect_max_seconds=configured.wecom_reconnect_max_seconds,
                    max_reconnect_attempts=configured.wecom_max_reconnect_attempts,
                )
            )
            application.state.wecom_long_connection = long_connection
            long_connection_task = asyncio.create_task(long_connection.run())
        feishu_source: FeishuLongConnectionSource | None = None
        feishu_source_task: asyncio.Task[None] | None = None
        feishu_renew_task: asyncio.Task[None] | None = None
        feishu_adapter: FeishuChannelAdapter | None = None
        feishu_gateway_client: AsyncClient | None = None
        if configured.feishu_enabled:
            feishu_adapter = FeishuChannelAdapter(
                tenant_id=configured.feishu_tenant_id,
                registry=registry,
                secrets=DatabaseChannelSecretResolver(active_database),
                inbound=application.state.inbound,
                lease_store=DatabaseLongConnectionLeaseStore(
                    active_database, configured.feishu_tenant_id
                ),
                owner_id=f"channel-gateway:{configured.feishu_app_id}",
                lease_ttl_seconds=configured.feishu_lease_ttl_seconds,
            )
            await feishu_adapter.acquire_long_connection(configured.feishu_app_id)
            feishu_source = feishu_long_connection_source or LarkSdkLongConnectionSource(
                app_id=configured.feishu_app_id,
                app_secret=configured.feishu_app_secret.get_secret_value(),
            )
            feishu_deliveries = ReplyDeliveryService(
                store=DatabaseDeliveryStore(active_database),
                transport=FeishuCardTransport(
                    client=feishu_card_client
                    or LarkSdkCardClient(
                        app_id=configured.feishu_app_id,
                        app_secret=configured.feishu_app_secret.get_secret_value(),
                    )
                ),
            )
            feishu_gateway_client = AsyncClient(base_url=configured.llm_gateway_url, timeout=45)
            executor = feishu_reply_executor or LLMGatewayReplyExecutor(feishu_gateway_client)

            async def handle_feishu_event(payload: Mapping[str, object]) -> None:
                accepted = await feishu_adapter.receive_long_connection_event(payload)
                if accepted.deduplicated:
                    return
                header = payload.get("header")
                app_id = header.get("app_id") if isinstance(header, Mapping) else None
                if not isinstance(app_id, str):
                    raise RuntimeError("FEISHU_EVENT_INVALID")
                binding = await registry.resolve(
                    tenant_id=configured.feishu_tenant_id,
                    channel_type="FEISHU",
                    external_bot_id=app_id,
                )
                if binding is None:
                    raise RuntimeError("FEISHU_BINDING_NOT_FOUND")
                normalized = normalize_feishu_message(payload, binding)
                content = await executor.complete(
                    tenant_id=configured.feishu_tenant_id,
                    application_id=binding.application_id,
                    release_id=str(accepted.release_id),
                    execution_id=str(accepted.execution_id),
                    messages=[{"role": "user", "content": normalized["text"]}],
                )
                delivery = await feishu_deliveries.enqueue(
                    tenant_id=configured.feishu_tenant_id,
                    binding_id=binding.binding_id,
                    execution_id=str(accepted.execution_id),
                    external_conversation_id=feishu_reply_message_id(payload),
                    content=content,
                )
                await feishu_deliveries.run(
                    delivery.delivery_id, tenant_id=configured.feishu_tenant_id
                )

            async def renew_feishu_lease() -> None:
                while True:
                    await asyncio.sleep(configured.feishu_lease_renew_seconds)
                    await feishu_adapter.renew_long_connection()

            application.state.feishu_adapter = feishu_adapter
            application.state.feishu_deliveries = feishu_deliveries
            application.state.feishu_long_connection = feishu_source
            feishu_renew_task = asyncio.create_task(renew_feishu_lease())
            feishu_source_task = asyncio.create_task(
                feishu_source.consume(configured.feishu_app_id, handle_feishu_event)
            )
        try:
            yield
        finally:
            if feishu_source is not None:
                await feishu_source.close()
            if feishu_renew_task is not None:
                feishu_renew_task.cancel()
                with suppress(asyncio.CancelledError):
                    await feishu_renew_task
            if feishu_source_task is not None:
                feishu_source_task.cancel()
                with suppress(asyncio.CancelledError):
                    await feishu_source_task
            if feishu_adapter is not None:
                await feishu_adapter.release_long_connection()
            if feishu_gateway_client is not None:
                await feishu_gateway_client.aclose()
            if long_connection is not None:
                await long_connection.close()
            if long_connection_task is not None:
                long_connection_task.cancel()
                with suppress(asyncio.CancelledError):
                    await long_connection_task
            await application.state.runner.close()
            if database is None:
                await active_database.close()

    application = FastAPI(
        title="tRPC-Agent Platform channel-gateway",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    def health() -> RuntimeHealthResponse:
        return RuntimeHealthResponse(
            service="channel-gateway", version=__version__, trpc_agent_version=TRPC_AGENT_VERSION
        )

    @application.get("/health/live", response_model=RuntimeHealthResponse)
    async def live() -> RuntimeHealthResponse:
        return health()

    @application.get("/health/ready", response_model=RuntimeHealthResponse)
    async def ready() -> RuntimeHealthResponse:
        return health()

    def _crypto() -> WeComCrypto:
        return WeComCrypto(
            token=configured.wecom_token,
            encoding_aes_key=configured.wecom_encoding_aes_key,
        )

    @application.get("/internal/v1/wecom/callback/{tenant_id}/{bot_id}")
    async def verify_callback(
        tenant_id: str,
        bot_id: str,
        msg_signature: Annotated[str, Query()],
        timestamp: Annotated[str, Query()],
        nonce: Annotated[str, Query()],
        echostr: Annotated[str, Query()],
    ) -> PlainTextResponse:
        crypto = _crypto()
        crypto.verify(timestamp=timestamp, nonce=nonce, encrypted=echostr, signature=msg_signature)
        return PlainTextResponse(crypto.decrypt(echostr))

    @application.post("/internal/v1/wecom/callback/{tenant_id}/{bot_id}")
    async def receive_callback(
        tenant_id: str,
        bot_id: str,
        request: Request,
        msg_signature: Annotated[str, Query()],
        timestamp: Annotated[str, Query()],
        nonce: Annotated[str, Query()],
    ) -> Response:
        del bot_id
        payload = await request.json()
        encrypted = str(payload.get("encrypt", ""))
        crypto = _crypto()
        try:
            crypto.verify(
                timestamp=timestamp, nonce=nonce, encrypted=encrypted, signature=msg_signature
            )
            event = parse_event(crypto.decrypt(encrypted))
        except WeComProtocolError as error:
            return PlainTextResponse(error.code, status_code=400)
        try:
            processed = await _process_text_event(application, tenant_id, event)
        except InboundError as error:
            return PlainTextResponse(error.code, status_code=409)
        if processed is None:
            return PlainTextResponse("")
        accepted, content = processed
        await _deliver(application, tenant_id, event, accepted.execution_id, content, configured)
        return PlainTextResponse("")

    return application


async def _process_text_event(
    application: FastAPI, tenant_id: str, event: WeComEvent
) -> tuple[Any, str] | None:
    """Submit a verified text event once and produce its agent reply text."""

    if event.is_revoke or not event.is_text_message:
        return None
    accepted = await _accepted_event(application, tenant_id, event)
    if accepted.deduplicated:
        return None
    registry: ChannelBindingRegistry = application.state.registry
    binding = await registry.resolve(
        tenant_id=tenant_id, channel_type="WECOM", external_bot_id=event.aibotid
    )
    if binding is None:
        raise InboundError("BINDING_NOT_FOUND")
    runner: ReleasePinnedRunnerRuntime = application.state.runner
    result = await runner.complete(
        RunnerExecutionCommand(
            tenant_id=tenant_id,
            application_id=binding.application_id,
            execution_id=str(accepted.execution_id),
            release_id=str(accepted.release_id),
            session_id=accepted.session_id,
            user_id=event.from_userid,
            message=event.text_content,
        )
    )
    return accepted, result.content


async def _accepted_event(application: FastAPI, tenant_id: str, event: WeComEvent) -> Any:
    inbound: ChannelInboundService = application.state.inbound
    signed = await inbound.signed_event(
        tenant_id=tenant_id,
        channel_type="WECOM",
        external_bot_id=event.aibotid,
        message_key=event.msgid,
        text=event.text_content,
        external_user_id=event.from_userid,
    )
    return await inbound.ingest(tenant_id=tenant_id, event=signed)


async def _deliver(
    application: FastAPI,
    tenant_id: str,
    event: WeComEvent,
    execution_id: Any,
    content: str,
    configured: ChannelGatewaySettings,
) -> None:
    deliveries: ReplyDeliveryService = application.state.deliveries
    registry: ChannelBindingRegistry = application.state.registry
    binding = await registry.resolve(
        tenant_id=tenant_id, channel_type="WECOM", external_bot_id=event.aibotid
    )
    if event.response_url and event.chattype == "single":
        # 单聊: merged incremental updates — never per-token API calls.
        session = WeComStreamSession(
            _http(application),
            response_url=event.response_url,
            batcher=WeComStreamBatcher(
                min_chars=configured.stream_min_chars,
                min_interval_seconds=configured.stream_min_interval_seconds,
            ),
            limiter=WeComRateLimiter(
                capacity=configured.reply_rate_capacity,
                refill_per_second=configured.reply_rate_refill_per_second,
            ),
        )
        for start in range(0, len(content), configured.stream_min_chars):
            await session.append(content[start : start + configured.stream_min_chars])
        await session.flush()
    elif event.chattype == "group" and event.response_url:
        # 群聊: a processing notice precedes the final tracked delivery.
        await _http(application).post(
            event.response_url,
            json={"msgtype": "text", "text": {"content": GROUP_PROCESSING_NOTICE}},
        )
    delivery = await deliveries.enqueue(
        tenant_id=tenant_id,
        binding_id=binding.binding_id if binding is not None else "unknown-binding",
        execution_id=str(execution_id),
        external_conversation_id=event.response_url or f"wecom:{event.chatid}",
        content=content,
    )
    await deliveries.run(delivery.delivery_id, tenant_id=tenant_id)


def _http(application: FastAPI) -> AsyncClient:
    if not hasattr(application.state, "http"):
        application.state.http = AsyncClient()
    client: AsyncClient = application.state.http
    return client
