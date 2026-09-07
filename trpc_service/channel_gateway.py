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
import inspect
import json
import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import PlainTextResponse
from httpx import AsyncClient
from pydantic import BaseModel, Field
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
from trpc_service.artifacts import (
    ArtifactAccessError,
    ArtifactError,
    ArtifactService,
    DatabaseArtifactAuditSink,
    DatabaseArtifactStore,
    TenantArtifactRetention,
)
from trpc_service.channels.adaptive_reply import (
    ChannelReplyCapabilities,
    ReplyStrategy,
    plan_reply,
)
from trpc_service.channels.bindings import ChannelBinding, ChannelBindingRegistry
from trpc_service.channels.delivery import DeliveryStore, ReplyDeliveryService
from trpc_service.channels.feishu import (
    DatabaseLongConnectionLeaseStore,
    FeishuAdapterError,
    FeishuCardTransport,
    FeishuChannelAdapter,
    FeishuLongConnectionSource,
    FeishuLongConnectionSupervisor,
    FeishuSecretResolver,
    HttpFeishuCardClient,
    LarkChannelLongConnectionSource,
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
    normalize_to_inbound,
    parse_event,
)
from trpc_service.governance import DataClassification, scan_messages
from trpc_service.llm_gateway import VaultSecretProvider
from trpc_service.memory_access import SubjectMemoryReader, memory_policy_for_session_scope
from trpc_service.runtime_health import RuntimeHealthResponse
from trpc_service.version import TRPC_AGENT_VERSION, __version__

logger = logging.getLogger(__name__)


class FeishuLongConnectionSettings(BaseModel):
    """A non-secret production connection declaration supplied by Helm."""

    tenant_id: str = Field(min_length=1)
    app_id: str = Field(min_length=1)


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
    wecom_max_text_chars: int = 4096
    feishu_max_text_chars: int = 4096
    artifact_inline_threshold_chars: int = 4096
    artifact_public_base_url: str = ""
    artifact_access_key: str = ""
    feishu_api_base_url: str = "https://open.feishu.cn"
    vault_url: str = ""
    vault_kubernetes_role: str = "channel-gateway"
    kubernetes_jwt_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    feishu_long_connections: list[FeishuLongConnectionSettings] = Field(default_factory=list)
    gateway_instance_id: str = ""

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


class FixedChannelSecretResolver:
    """Pass a protocol token already resolved from a binding's vault reference."""

    def __init__(self, token: str) -> None:
        self._token = token

    def resolve(self, secret_ref: str) -> str:
        del secret_ref
        return self._token


@dataclass(frozen=True)
class FeishuLongConnection:
    """One provider SDK connection that the Channel Gateway must supervise."""

    tenant_id: str
    app_id: str
    source: FeishuLongConnectionSource


def create_app(
    settings: ChannelGatewaySettings | None = None,
    *,
    runner: ReleasePinnedRunnerRuntime | None = None,
    inbound: ChannelInboundService | None = None,
    deliveries: ReplyDeliveryService | None = None,
    feishu_secrets: FeishuSecretResolver | None = None,
    feishu_http: AsyncClient | None = None,
    feishu_delivery_store: DeliveryStore | None = None,
    artifact_service: ArtifactService | None = None,
    feishu_long_connections: Sequence[FeishuLongConnection] = (),
    database: Database | None = None,
) -> FastAPI:
    """Create the Channel Gateway data-plane entry for installed IM adapters."""

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
        application.state.database = active_database
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
        vault_client: AsyncClient | None = None
        resolved_feishu_secrets = feishu_secrets
        if resolved_feishu_secrets is None and configured.vault_url:
            jwt = Path(configured.kubernetes_jwt_path).read_text().strip()
            if not jwt:
                raise RuntimeError("Kubernetes service account token is empty")
            vault_client = AsyncClient(base_url=configured.vault_url)
            resolved_feishu_secrets = VaultSecretProvider(
                vault_client,
                kubernetes_jwt=jwt,
                role=configured.vault_kubernetes_role,
            )
        application.state.feishu_secrets = resolved_feishu_secrets
        application.state.feishu_http = feishu_http
        application.state.feishu_delivery_store = feishu_delivery_store
        resolved_artifact_service = artifact_service
        if resolved_artifact_service is None and configured.artifact_access_key:
            resolved_artifact_service = ArtifactService(
                store=DatabaseArtifactStore(active_database),
                access_key=configured.artifact_access_key.encode(),
                audit_sink=DatabaseArtifactAuditSink(active_database),
                retention_days=TenantArtifactRetention(active_database).days_for,
            )
        if configured.artifact_public_base_url and resolved_artifact_service is None:
            raise RuntimeError("ARTIFACT_ACCESS_KEY_REQUIRED")
        application.state.artifact_service = resolved_artifact_service
        application.state.runner = runner or ReleasePinnedRunnerRuntime(
            releases=DatabaseReleaseRouteResolver(active_database),
            llm_gateway_access_key=configured.llm_gateway_access_key,
        )
        gateway_owner_id = _gateway_owner_id(configured)
        long_connection_tasks: list[asyncio.Task[None]] = []
        if feishu_long_connections:
            if resolved_feishu_secrets is None:
                raise RuntimeError("FEISHU_SECRET_RESOLUTION_UNAVAILABLE")
            long_connection_tasks.append(
                asyncio.create_task(
                    _run_feishu_long_connections(
                        application=application,
                        configured=configured,
                        registry=registry,
                        database=active_database,
                        connections=feishu_long_connections,
                        secrets=resolved_feishu_secrets,
                        owner_id=gateway_owner_id,
                    )
                )
            )
        elif configured.feishu_long_connections:
            if resolved_feishu_secrets is None:
                raise RuntimeError("FEISHU_SECRET_RESOLUTION_UNAVAILABLE")
            long_connection_tasks.append(
                asyncio.create_task(
                    _run_declared_feishu_long_connections(
                        application=application,
                        configured=configured,
                        registry=registry,
                        database=active_database,
                        declarations=configured.feishu_long_connections,
                        secrets=resolved_feishu_secrets,
                        owner_id=gateway_owner_id,
                        database_ready=connect_task,
                    )
                )
            )
        try:
            yield
        finally:
            for task in long_connection_tasks:
                task.cancel()
            if long_connection_tasks:
                await asyncio.gather(*long_connection_tasks, return_exceptions=True)
            connect_task.cancel()
            await application.state.runner.close()
            if vault_client is not None:
                await vault_client.aclose()
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

    @application.get("/internal/v1/artifacts/{artifact_id}")
    async def download_artifact(
        artifact_id: str,
        tenant_id: Annotated[str, Header(alias="X-Artifact-Tenant")],
        subject_id: Annotated[str, Header(alias="X-Artifact-Subject")],
        access_token: Annotated[str, Header(alias="X-Artifact-Access-Token")],
    ) -> Response:
        service: ArtifactService | None = application.state.artifact_service
        if service is None:
            return PlainTextResponse("ARTIFACT_UNAVAILABLE", status_code=503)
        try:
            content = await service.download(
                tenant_id=tenant_id,
                artifact_id=artifact_id,
                subject_id=subject_id,
                access_token=access_token,
            )
        except ArtifactAccessError as error:
            return PlainTextResponse(error.code, status_code=403)
        return Response(content=content, media_type="application/octet-stream")

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
        if event.is_attachment:
            # The callback has no trusted byte stream.  Never route attachment
            # metadata to a model: a channel-specific fetcher must first put
            # bytes through ArtifactService.create() and its DLP/AV gate.
            return PlainTextResponse("ATTACHMENT_REQUIRES_ARTIFACT_SCAN", status_code=400)
        if not event.is_text_message:
            return PlainTextResponse("")
        inbound: ChannelInboundService = application.state.inbound
        try:
            normalized = normalize_to_inbound(event)
            signed = await inbound.signed_event(tenant_id=tenant_id, **normalized)
            accepted = await inbound.ingest(tenant_id=tenant_id, event=signed)
        except WeComProtocolError as error:
            return PlainTextResponse(error.code, status_code=400)
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
            session_id=accepted.session_id,
            user_id=event.from_userid,
            message=event.text_content,
            session_user_id=_runner_session_user_id(normalized["session_key"], accepted.session_id),
            memory_context=await _im_memory_context(
                application,
                tenant_id=tenant_id,
                binding=binding,
                external_user_id=event.from_userid,
                session_key=normalized["session_key"],
            ),
        )
        streamed = await _stream_wecom_direct_reply(
            application, configured, runner=runner, command=command, event=event
        )
        reply = streamed or await runner.complete(command)
        await _deliver(
            application,
            tenant_id,
            event,
            accepted.execution_id,
            reply.content,
            configured,
            streamed=streamed is not None,
        )
        return PlainTextResponse("")

    @application.post("/internal/v1/feishu/callback/{tenant_id}/{bot_id}")
    async def receive_feishu_callback(
        tenant_id: str,
        bot_id: str,
        request: Request,
    ) -> Response:
        raw_body = await request.body()
        inbound: ChannelInboundService = application.state.inbound
        registry: ChannelBindingRegistry = application.state.registry
        try:
            payload = json.loads(raw_body)
            if not isinstance(payload, dict):
                raise FeishuAdapterError("FEISHU_PAYLOAD_INVALID")
            event = normalize_feishu_message(payload)
            if event["external_bot_id"] != bot_id:
                raise FeishuAdapterError("FEISHU_BINDING_NOT_FOUND")
        except FeishuAdapterError as error:
            return PlainTextResponse(error.code, status_code=400)
        binding = await registry.resolve(
            tenant_id=tenant_id, channel_type="FEISHU", external_bot_id=bot_id
        )
        if binding is None:
            return PlainTextResponse("FEISHU_BINDING_NOT_FOUND", status_code=400)
        secrets: FeishuSecretResolver | None = application.state.feishu_secrets
        if secrets is None:
            return PlainTextResponse("FEISHU_SECRET_RESOLUTION_UNAVAILABLE", status_code=503)
        try:
            verification_token = await _resolve_feishu_secret(
                secrets, tenant_id, binding.secret_ref
            )
            tenant_access_token = await _resolve_feishu_secret(
                secrets, tenant_id, _secret_ref_with_field(binding, "tenant-access-token")
            )
        except Exception:
            return PlainTextResponse("FEISHU_SECRET_RESOLUTION_FAILED", status_code=503)
        adapter = FeishuChannelAdapter(
            tenant_id=tenant_id,
            registry=registry,
            secrets=FixedChannelSecretResolver(verification_token),
            inbound=inbound,
            lease_store=DatabaseLongConnectionLeaseStore(application.state.database, tenant_id),
        )
        try:
            accepted = await adapter.receive_webhook(request.headers, raw_body)
        except FeishuAdapterError as error:
            return PlainTextResponse(error.code, status_code=400)
        except RuntimeError:
            return PlainTextResponse("DATABASE_UNAVAILABLE", status_code=503)
        if accepted.deduplicated:
            return PlainTextResponse("")
        await _execute_feishu_reply(
            application,
            configured,
            tenant_id=tenant_id,
            binding=binding,
            accepted=accepted,
            event=event,
            tenant_access_token=tenant_access_token,
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


async def _run_supervisor(
    supervisor: FeishuLongConnectionSupervisor, *, retry_delay_seconds: float = 2.0
) -> None:
    """Keep a standby gateway eligible to take over after lease loss or disconnect."""

    while True:
        try:
            await supervisor.run()
        except FeishuAdapterError as error:
            if error.code != "FEISHU_LONG_CONNECTION_LEASE_HELD":
                logger.warning("Feishu long connection stopped: %s", error.code)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Provider transport failures (DNS, TLS and handshake errors) are
            # transient at this boundary. ``supervisor.run`` has released its
            # fenced lease before this retry, so another replica may take over.
            logger.exception("Feishu long connection stopped; retrying")
        await asyncio.sleep(retry_delay_seconds)


def _gateway_owner_id(configured: ChannelGatewaySettings) -> str:
    """Return one stable lease owner for this Gateway process."""

    return configured.gateway_instance_id or f"channel-gateway-{uuid4()}"


async def _run_declared_feishu_long_connections(
    *,
    application: FastAPI,
    configured: ChannelGatewaySettings,
    registry: ChannelBindingRegistry,
    database: Database,
    declarations: Sequence[FeishuLongConnectionSettings],
    secrets: FeishuSecretResolver,
    owner_id: str,
    database_ready: asyncio.Task[None],
) -> None:
    """Wait for the database in the background before starting declared SDK streams."""

    await database_ready
    while True:
        try:
            connections = await _configured_feishu_long_connections(
                registry=registry,
                declarations=declarations,
                secrets=secrets,
            )
            await _run_feishu_long_connections(
                application=application,
                configured=configured,
                registry=registry,
                database=database,
                connections=connections,
                secrets=secrets,
                owner_id=owner_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # A binding can be created after a rollout or repaired after an
            # operator mistake. Keep the declaration alive and retry rather
            # than permanently abandoning its stream.
            logger.exception("Declared Feishu long connection is not ready; retrying")
        await asyncio.sleep(2.0)


async def _run_feishu_long_connections(
    *,
    application: FastAPI,
    configured: ChannelGatewaySettings,
    registry: ChannelBindingRegistry,
    database: Database,
    connections: Sequence[FeishuLongConnection],
    secrets: FeishuSecretResolver,
    owner_id: str,
) -> None:
    """Run all connections for this process while their owner lease remains valid."""

    supervisors = [
        _feishu_supervisor(
            application=application,
            configured=configured,
            registry=registry,
            database=database,
            connection=connection,
            secrets=secrets,
            owner_id=owner_id,
        )
        for connection in connections
    ]
    await asyncio.gather(*(_run_supervisor(supervisor) for supervisor in supervisors))


def _feishu_supervisor(
    *,
    application: FastAPI,
    configured: ChannelGatewaySettings,
    registry: ChannelBindingRegistry,
    database: Database,
    connection: FeishuLongConnection,
    secrets: FeishuSecretResolver,
    owner_id: str,
) -> FeishuLongConnectionSupervisor:
    adapter = FeishuChannelAdapter(
        tenant_id=connection.tenant_id,
        registry=registry,
        secrets=DatabaseChannelSecretResolver(database),
        inbound=application.state.inbound,
        lease_store=DatabaseLongConnectionLeaseStore(database, connection.tenant_id),
        owner_id=owner_id,
    )

    async def on_accepted(accepted: Any, payload: Mapping[str, object]) -> None:
        if accepted.deduplicated:
            return
        event = normalize_feishu_message(payload)
        binding = await registry.resolve(
            tenant_id=connection.tenant_id,
            channel_type="FEISHU",
            external_bot_id=connection.app_id,
        )
        if binding is None:
            raise FeishuAdapterError("FEISHU_BINDING_NOT_FOUND")
        access_token = await _resolve_feishu_secret(
            secrets,
            connection.tenant_id,
            _secret_ref_with_field(binding, "tenant-access-token"),
        )
        await _execute_feishu_reply(
            application,
            configured,
            tenant_id=connection.tenant_id,
            binding=binding,
            accepted=accepted,
            event=event,
            tenant_access_token=access_token,
        )

    return FeishuLongConnectionSupervisor(
        adapter=adapter,
        source=connection.source,
        app_id=connection.app_id,
        on_accepted=on_accepted,
    )


async def _configured_feishu_long_connections(
    *,
    registry: ChannelBindingRegistry,
    declarations: Sequence[FeishuLongConnectionSettings],
    secrets: FeishuSecretResolver,
) -> list[FeishuLongConnection]:
    """Create SDK sources from explicit, RLS-safe deployment declarations."""

    connections: list[FeishuLongConnection] = []
    for declaration in declarations:
        binding = await registry.resolve(
            tenant_id=declaration.tenant_id,
            channel_type="FEISHU",
            external_bot_id=declaration.app_id,
        )
        if binding is None:
            raise RuntimeError("FEISHU_BINDING_NOT_FOUND")
        active_binding: ChannelBinding = binding

        async def app_secret(
            *,
            tenant_id: str = declaration.tenant_id,
            binding: ChannelBinding = active_binding,
        ) -> str:
            return await _resolve_feishu_secret(
                secrets, tenant_id, _secret_ref_with_field(binding, "app-secret")
            )

        connections.append(
            FeishuLongConnection(
                tenant_id=declaration.tenant_id,
                app_id=declaration.app_id,
                source=LarkChannelLongConnectionSource(app_secret=app_secret),
            )
        )
    return connections


async def _execute_feishu_reply(
    application: FastAPI,
    configured: ChannelGatewaySettings,
    *,
    tenant_id: str,
    binding: ChannelBinding,
    accepted: Any,
    event: dict[str, str],
    tenant_access_token: str,
) -> None:
    deliveries = ReplyDeliveryService(
        store=application.state.feishu_delivery_store
        or DatabaseDeliveryStore(application.state.database),
        transport=FeishuCardTransport(
            client=HttpFeishuCardClient(
                application.state.feishu_http or _http(application),
                tenant_access_token=tenant_access_token,
                api_base_url=configured.feishu_api_base_url,
            )
        ),
    )
    runner: ReleasePinnedRunnerRuntime = application.state.runner
    command = RunnerExecutionCommand(
        tenant_id=tenant_id,
        application_id=binding.application_id,
        execution_id=str(accepted.execution_id),
        release_id=str(accepted.release_id),
        session_id=accepted.session_id,
        user_id=event["external_user_id"],
        message=event["text"],
        session_user_id=_runner_session_user_id(event["session_key"], accepted.session_id),
        memory_context=await _im_memory_context(
            application,
            tenant_id=tenant_id,
            binding=binding,
            external_user_id=event["external_user_id"],
            session_key=event["session_key"],
        ),
    )
    is_group = event["session_key"].startswith(("group:", "thread:"))
    existing_delivery = None
    streamed_content = ""
    if not is_group and hasattr(runner, "stream"):
        existing_delivery = await _feishu_processing_delivery(
            tenant_id=tenant_id,
            binding_id=binding.binding_id,
            execution_id=accepted.execution_id,
            event=event,
            deliveries=deliveries,
        )
        pending_chars = 0
        async for chunk in runner.stream(command):
            if chunk.kind == "delta":
                streamed_content += chunk.delta
                pending_chars += len(chunk.delta)
                # Cards have a hard provider maximum.  Once it is reached,
                # wait for the final bounded/artifactized delivery.
                if (
                    len(streamed_content) <= configured.feishu_max_text_chars
                    and pending_chars >= configured.stream_min_chars
                    and not scan_messages([{"content": streamed_content}]).blocked
                ):
                    updated = await deliveries.update(
                        existing_delivery.delivery_id,
                        tenant_id=tenant_id,
                        content=streamed_content,
                    )
                    await deliveries.run(updated.delivery_id, tenant_id=tenant_id)
                    pending_chars = 0
            elif chunk.reply is not None:
                reply = chunk.reply
        if "reply" not in locals():
            raise RuntimeError("RUNNER_EMPTY_REPLY")
    else:
        reply = await runner.complete(command)
    safe_content = await _content_for_channel(
        application,
        configured,
        tenant_id=tenant_id,
        binding=binding,
        external_user_id=event["external_user_id"],
        execution_id=str(accepted.execution_id),
        content=reply.content,
    )
    await _deliver_feishu(
        tenant_id=tenant_id,
        binding_id=binding.binding_id,
        execution_id=accepted.execution_id,
        event=event,
        content=safe_content,
        deliveries=deliveries,
        configured=configured,
        is_group=is_group,
        existing_delivery=existing_delivery,
        already_streamed=bool(streamed_content),
    )


async def _deliver(
    application: FastAPI,
    tenant_id: str,
    event: WeComEvent,
    execution_id: Any,
    content: str,
    configured: ChannelGatewaySettings,
    *,
    streamed: bool = False,
) -> None:
    deliveries: ReplyDeliveryService = application.state.deliveries
    registry: ChannelBindingRegistry = application.state.registry
    binding = await registry.resolve(
        tenant_id=tenant_id, channel_type="WECOM", external_bot_id=event.aibotid
    )
    safe_content = await _content_for_channel(
        application,
        configured,
        tenant_id=tenant_id,
        binding=binding,
        external_user_id=event.from_userid,
        execution_id=str(execution_id),
        content=content,
    )
    plan = plan_reply(
        safe_content,
        capabilities=ChannelReplyCapabilities(
            supports_updates=bool(event.response_url and event.chattype == "single"),
            max_text_chars=configured.wecom_max_text_chars,
            stream_chunk_chars=configured.stream_min_chars,
        ),
        is_group=event.chattype == "group",
    )
    if plan.stream_chunks and event.response_url and not streamed:
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
        for stream_chunk in plan.stream_chunks:
            await session.append(stream_chunk)
        await session.flush()
    elif plan.processing_notice and event.response_url:
        # 群聊: a processing notice precedes the final tracked delivery.
        await _http(application).post(
            event.response_url,
            json={"msgtype": "text", "text": {"content": GROUP_PROCESSING_NOTICE}},
        )
    for final_message in plan.final_messages:
        delivery = await deliveries.enqueue(
            tenant_id=tenant_id,
            binding_id=binding.binding_id if binding is not None else "unknown-binding",
            execution_id=str(execution_id),
            external_conversation_id=event.response_url or f"wecom:{event.chatid}",
            content=final_message,
        )
        await deliveries.run(delivery.delivery_id, tenant_id=tenant_id)


async def _deliver_feishu(
    *,
    tenant_id: str,
    binding_id: str,
    execution_id: Any,
    event: dict[str, str],
    content: str,
    deliveries: ReplyDeliveryService,
    configured: ChannelGatewaySettings,
    is_group: bool,
    existing_delivery: Any | None = None,
    already_streamed: bool = False,
) -> None:
    plan = plan_reply(
        content,
        capabilities=ChannelReplyCapabilities(
            supports_updates=True,
            max_text_chars=configured.feishu_max_text_chars,
            stream_chunk_chars=configured.stream_min_chars,
        ),
        is_group=is_group,
    )
    delivery = existing_delivery or await _feishu_processing_delivery(
        tenant_id=tenant_id,
        binding_id=binding_id,
        execution_id=execution_id,
        event=event,
        deliveries=deliveries,
    )
    if plan.strategy is ReplyStrategy.MERGED_STREAM and not already_streamed:
        streamed = ""
        for stream_chunk in plan.stream_chunks:
            streamed += stream_chunk
            if streamed == content or len(streamed) > configured.feishu_max_text_chars:
                continue
            updated = await deliveries.update(
                delivery.delivery_id, tenant_id=tenant_id, content=streamed
            )
            await deliveries.run(updated.delivery_id, tenant_id=tenant_id)
    for index, final_message in enumerate(plan.final_messages):
        if index == 0:
            updated = await deliveries.update(
                delivery.delivery_id, tenant_id=tenant_id, content=final_message
            )
            await deliveries.run(updated.delivery_id, tenant_id=tenant_id)
            continue
        follow_up = await deliveries.enqueue(
            tenant_id=tenant_id,
            binding_id=binding_id,
            execution_id=str(execution_id),
            external_conversation_id=_feishu_conversation(event),
            content=final_message,
        )
        await deliveries.run(follow_up.delivery_id, tenant_id=tenant_id)


async def _feishu_processing_delivery(
    *,
    tenant_id: str,
    binding_id: str,
    execution_id: Any,
    event: dict[str, str],
    deliveries: ReplyDeliveryService,
) -> Any:
    delivery = await deliveries.enqueue(
        tenant_id=tenant_id,
        binding_id=binding_id,
        execution_id=str(execution_id),
        external_conversation_id=_feishu_conversation(event),
        content="处理中",
    )
    await deliveries.run(delivery.delivery_id, tenant_id=tenant_id)
    return delivery


async def _stream_wecom_direct_reply(
    application: FastAPI,
    configured: ChannelGatewaySettings,
    *,
    runner: ReleasePinnedRunnerRuntime,
    command: RunnerExecutionCommand,
    event: WeComEvent,
) -> Any | None:
    """Forward actual model deltas in a single chat, never token-by-token."""

    if event.chattype != "single" or not event.response_url or not hasattr(runner, "stream"):
        return None
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
    reply = None
    # Never release a suffix until it is long enough for a secret detector to
    # see a value split across model deltas.  This intentionally trades a
    # small amount of latency for a fail-closed DLP boundary.
    held = ""
    blocked = False
    holdback_chars = max(configured.stream_min_chars, 512)
    async for chunk in runner.stream(command):
        if chunk.kind == "delta":
            held += chunk.delta
            if len(held) > holdback_chars:
                releasable, held = held[:-holdback_chars], held[-holdback_chars:]
                if scan_messages([{"content": releasable + held}]).blocked:
                    blocked = True
                elif not blocked:
                    await session.append(releasable)
        elif chunk.reply is not None:
            reply = chunk.reply
    if scan_messages([{"content": held}]).blocked:
        blocked = True
    if held and not blocked:
        await session.append(held)
    await session.flush()
    return reply


async def _content_for_channel(
    application: FastAPI,
    configured: ChannelGatewaySettings,
    *,
    tenant_id: str,
    binding: ChannelBinding | None,
    external_user_id: str,
    execution_id: str,
    content: str,
) -> str:
    """Artifactize only through a configured safe store; otherwise segment later.

    The output scan runs before either path.  A detected secret never falls
    back to a visible text segment, and an unavailable Artifact store merely
    leaves non-sensitive content for the bounded channel planner.
    """

    if scan_messages([{"content": content}]).blocked:
        return "回复因安全策略未投递。"
    service: ArtifactService | None = application.state.artifact_service
    if (
        service is None
        or not configured.artifact_public_base_url
        or len(content) <= configured.artifact_inline_threshold_chars
        or binding is None
    ):
        return content
    subject_id = f"im:{binding.channel_type}:{binding.binding_id}:{external_user_id}"
    try:
        artifact = await service.create(
            tenant_id=tenant_id,
            subject_id=subject_id,
            execution_id=execution_id,
            filename=f"reply-{execution_id}.txt",
            media_type="text/plain",
            content=content.encode(),
            declared_classification=DataClassification.INTERNAL,
        )
    except ArtifactError:
        return "完整回复不能作为附件交付。"
    except Exception:
        logger.exception("Artifact delivery unavailable")
        return "完整回复暂不可用，请稍后重试。"
    base = configured.artifact_public_base_url.rstrip("/")
    # ``artifact_public_base_url`` is the authenticated user portal.  The IM
    # message deliberately carries neither a tenant/subject identifier nor a
    # bearer capability: the portal maps the signed-in IM identity server-side
    # before it asks this internal endpoint for a short-lived access token.
    return f"完整回复已生成，请在受控工作台查看：{base}/artifacts/{artifact.artifact_id}"


def _feishu_conversation(event: dict[str, str]) -> str:
    """Preserve the API receive-id type with the persisted delivery."""

    session_key = event["session_key"]
    if session_key.startswith("direct:"):
        return f"open_id:{event['external_user_id']}"
    if session_key.startswith("group:"):
        return f"chat_id:{session_key.removeprefix('group:')}"
    if session_key.startswith("thread:"):
        _, chat_id, _ = session_key.split(":", 2)
        return f"chat_id:{chat_id}"
    raise RuntimeError("FEISHU_CONVERSATION_INVALID")


def _runner_session_user_id(session_key: str, session_id: str) -> str | None:
    """Give every group/topic one opaque SDK owner, independent of its sender."""

    if session_key.startswith("direct:"):
        return None
    if session_key.startswith(("group:", "thread:")):
        return f"conversation:{session_id}"
    raise RuntimeError("SESSION_SCOPE_INVALID")


async def _im_memory_context(
    application: FastAPI,
    *,
    tenant_id: str,
    binding: ChannelBinding | None,
    external_user_id: str,
    session_key: str,
) -> tuple[str, ...]:
    """Load only policy-approved Memory; group/topic lookups are always empty."""

    policy = memory_policy_for_session_scope(session_key)
    if policy != "im-subject-direct-v1" or binding is None:
        return ()
    try:
        reader = SubjectMemoryReader(application.state.database)
        subject_id = application.state.inbound.subject_for(
            binding=binding, external_user_id=external_user_id
        )
        memories = await reader.list_visible(
            tenant_id=UUID(tenant_id),
            subject_id=subject_id,
            memory_policy_version=policy,
        )
    except Exception:
        # Memory is eventually consistent and must never delay a verified IM
        # request. A failed read means no context, never a wider query.
        logger.warning("IM memory lookup unavailable", exc_info=True)
        return ()
    return tuple(memory.content for memory in memories)


async def _resolve_feishu_secret(
    resolver: FeishuSecretResolver, tenant_id: str, secret_ref: str
) -> str:
    value = resolver.resolve(tenant_id, secret_ref)
    resolved = await value if inspect.isawaitable(value) else value
    if not isinstance(resolved, str) or not resolved:
        raise RuntimeError("FEISHU_SECRET_RESOLUTION_FAILED")
    return resolved


def _secret_ref_with_field(binding: ChannelBinding, field: str) -> str:
    """Keep the binding path tenant-scoped while selecting a vault credential field."""

    parsed = urlsplit(binding.secret_ref)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, field))


def _http(application: FastAPI) -> AsyncClient:
    if not hasattr(application.state, "http"):
        application.state.http = AsyncClient()
    client: AsyncClient = application.state.http
    return client


app = create_app()
