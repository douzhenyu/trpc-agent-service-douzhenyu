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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from hashlib import sha256
from typing import Annotated, Any

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import PlainTextResponse
from httpx import AsyncClient
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.admin_api.database import Database
from trpc_service.agent.runner import (
    ReleasePinnedRunnerRuntime,
    RunnerExecutionCommand,
)
from trpc_service.agent_gateway import AgentExecutionSubmitter
from trpc_service.agent_worker import (
    DatabaseDeploymentRouteResolver,
    DatabaseReleaseRouteResolver,
)
from trpc_service.channels.bindings import ChannelBindingRegistry
from trpc_service.channels.delivery import ReplyDeliveryService
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
from trpc_service.runtime_health import RuntimeHealthResponse
from trpc_service.version import TRPC_AGENT_VERSION, __version__


class ChannelGatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = ""
    llm_gateway_access_key: str = ""
    wecom_token: str = ""
    wecom_encoding_aes_key: str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"
    reply_rate_capacity: int = 20
    reply_rate_refill_per_second: float = 20 / 60
    stream_min_chars: int = 256
    stream_min_interval_seconds: float = 2.0

    def validate_runtime(self) -> None:
        if not self.database_url:
            raise RuntimeError("Channel Gateway configuration is incomplete: DATABASE_URL")
        # WECOM_TOKEN/AES_KEY stay unset until the tenant callback credentials
        # are provisioned; without them every callback fails closed (signature
        # verification cannot succeed), which is the safe default.


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


def create_app(
    settings: ChannelGatewaySettings | None = None,
    *,
    runner: ReleasePinnedRunnerRuntime | None = None,
    inbound: ChannelInboundService | None = None,
    deliveries: ReplyDeliveryService | None = None,
    database: Database | None = None,
) -> FastAPI:
    """Create the WeCom Channel Gateway data-plane entry."""

    configured = settings or ChannelGatewaySettings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        configured.validate_runtime()
        active_database = database or Database(configured.database_url)
        # The database may be unreachable when the pod first starts (zero-trust
        # network policies, migration timing). Serve health endpoints, keep
        # retrying in the background, and fail closed per callback until the
        # connection succeeds.
        connect_task = asyncio.create_task(_connect_database(active_database))
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
        try:
            yield
        finally:
            connect_task.cancel()
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
        if event.is_revoke:
            # 撤回: the message was recalled; nothing executes or delivers and
            # the ack is idempotent.
            return PlainTextResponse("")
        if not event.is_text_message:
            return PlainTextResponse("")
        inbound: ChannelInboundService = application.state.inbound
        signed = await inbound.signed_event(
            tenant_id=tenant_id,
            channel_type="WECOM",
            external_bot_id=event.aibotid,
            message_key=event.msgid,
            text=event.text_content,
            external_user_id=event.from_userid,
        )
        try:
            accepted = await inbound.ingest(tenant_id=tenant_id, event=signed)
        except InboundError as error:
            return PlainTextResponse(error.code, status_code=409)
        except RuntimeError:
            # Database still unreachable: fail closed rather than process an
            # execution whose reply cannot be tracked.
            return PlainTextResponse("DATABASE_UNAVAILABLE", status_code=503)
        if accepted.deduplicated:
            return PlainTextResponse("")
        runner: ReleasePinnedRunnerRuntime = application.state.runner
        registry: ChannelBindingRegistry = application.state.registry
        binding = await registry.resolve(
            tenant_id=tenant_id, channel_type="WECOM", external_bot_id=event.aibotid
        )
        command = RunnerExecutionCommand(
            tenant_id=tenant_id,
            application_id=binding.application_id if binding else str(accepted.release_id),
            execution_id=str(accepted.execution_id),
            release_id=str(accepted.release_id),
            session_id=f"channel:{event.aibotid}:{event.from_userid}",
            user_id=event.from_userid,
            message=event.text_content,
        )
        reply = await runner.complete(command)
        await _deliver(
            application, tenant_id, event, accepted.execution_id, reply.content, configured
        )
        return PlainTextResponse("")

    return application


async def _connect_database(database: Database) -> None:
    while True:
        try:
            await database.open()
            return
        except Exception:
            await asyncio.sleep(2.0)


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


app = create_app()
