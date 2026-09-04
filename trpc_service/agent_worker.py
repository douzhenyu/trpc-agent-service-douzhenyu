"""Agent Worker runtime module that routes released executions through LLM Gateway."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.exc import SQLAlchemyError

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.database import Database
from trpc_service.execution_bus import ExecutionEnvelope, ExecutionRequestedData
from trpc_service.llm_gateway import (
    DataClassification,
    GatewayCompletionClient,
    GatewayModel,
    GatewayRequest,
    GatewayResult,
    ModelGatewayError,
    ModelProfile,
)
from trpc_service.runtime_health import RuntimeHealthResponse
from trpc_service.sandbox.executor import assert_no_unsafe_local_execution
from trpc_service.sessions import (
    LeaseGrant,
    SessionEvent,
    SessionLeaseManager,
    commit_session_events,
)
from trpc_service.version import TRPC_AGENT_VERSION, __version__


@dataclass(frozen=True)
class ReleaseRoute:
    release_id: str
    tenant_id: str
    model_alias: str
    data_classification: DataClassification
    region: str
    allowed_fallback_aliases: frozenset[str]
    profile_snapshots: tuple[ModelProfile, ...] = ()
    instructions: str = ""


@dataclass(frozen=True)
class DeploymentRoute:
    release_id: str
    previous_release_id: str | None
    rollout_percentage: int


@dataclass(frozen=True)
class AgentExecutionRequest:
    tenant_id: str
    release_id: str
    messages: list[dict[str, str]]
    application_id: str | None = None
    execution_id: str | None = None


class ReleaseRouteResolver(Protocol):
    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None: ...


class DeploymentRouteResolver(Protocol):
    async def resolve(
        self, tenant_id: str, application_id: str, environment: str, session_id: str
    ) -> str | None: ...


class FallbackAuditor(Protocol):
    async def record(self, route: ReleaseRoute, result: GatewayResult) -> None: ...


class DatabaseFallbackAuditor:
    """Append a safe audit record for an explicit release-authorized degradation."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def record(self, route: ReleaseRoute, result: GatewayResult) -> None:
        async with self._database.tenant_transaction(UUID(route.tenant_id)) as connection:
            await insert_audit(
                connection,
                None,
                "agent_execution.model_fallback",
                "ALLOW",
                target_type="agent_release",
                target_id=route.release_id,
                tenant_id=UUID(route.tenant_id),
                details={
                    "requested_model_alias": route.model_alias,
                    "resolved_model_alias": result.model_alias,
                    "fallback_used": True,
                },
            )


class InMemoryReleaseRouteResolver:
    def __init__(self, routes: list[ReleaseRoute]) -> None:
        self._routes = {(route.tenant_id, route.release_id): route for route in routes}

    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None:
        return self._routes.get((tenant_id, release_id))


class DatabaseReleaseRouteResolver:
    """Resolves only immutable, tenant-scoped published execution configuration."""

    def __init__(self, database: Database) -> None:
        self._database = database
        self._routes: dict[tuple[str, str], ReleaseRoute] = {}

    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None:
        try:
            parsed_tenant_id, parsed_release_id = UUID(tenant_id), UUID(release_id)
        except ValueError:
            return None
        cache_key = (tenant_id, release_id)
        if cached := self._routes.get(cache_key):
            return cached
        try:
            async with self._database.tenant_transaction(parsed_tenant_id) as connection:
                row = await connection.fetchrow(
                    """SELECT id,tenant_id,model_alias,data_classification,region,
                        fallback_aliases,model_profiles,draft_snapshot
                        FROM tenant.agent_release WHERE tenant_id=$1 AND id=$2""",
                    parsed_tenant_id,
                    parsed_release_id,
                )
        except SQLAlchemyError:
            return self._routes.get(cache_key)
        if row is None:
            return None
        snapshot = row["draft_snapshot"] if isinstance(row["draft_snapshot"], dict) else {}
        snapshots = tuple(
            ModelProfile(
                tenant_id=str(profile["tenant_id"]),
                alias=str(profile["alias"]),
                provider_model=str(profile["provider_model"]),
                endpoint_url=str(profile["endpoint_url"]),
                secret_ref=str(profile["secret_ref"]),
                data_classification=DataClassification(str(profile["data_classification"])),
                region=str(profile["region"]),
                fallback_aliases=tuple(str(alias) for alias in profile["fallback_aliases"]),
                requests_per_minute=int(profile["requests_per_minute"]),
            )
            for profile in row["model_profiles"]
        )
        if not snapshots:
            return None
        route = ReleaseRoute(
            release_id=str(row["id"]),
            tenant_id=str(row["tenant_id"]),
            model_alias=str(row["model_alias"]),
            data_classification=DataClassification(str(row["data_classification"])),
            region=str(row["region"]),
            allowed_fallback_aliases=frozenset(str(alias) for alias in row["fallback_aliases"]),
            profile_snapshots=snapshots,
            instructions=str(snapshot.get("instructions") or ""),
        )
        self._routes[cache_key] = route
        return route


class DatabaseDeploymentRouteResolver:
    """Resolve an environment Deployment once per execution using a stable Session bucket."""

    def __init__(self, database: Database) -> None:
        self._database = database
        self._routes: dict[tuple[str, str, str], DeploymentRoute] = {}

    async def resolve(
        self, tenant_id: str, application_id: str, environment: str, session_id: str
    ) -> str | None:
        try:
            parsed_tenant_id, parsed_application_id = UUID(tenant_id), UUID(application_id)
        except ValueError:
            return None
        cache_key = (tenant_id, application_id, environment)
        try:
            async with self._database.tenant_transaction(parsed_tenant_id) as connection:
                row = await connection.fetchrow(
                    """SELECT release_id,previous_release_id,rollout_percentage
                    FROM tenant.agent_deployment
                    WHERE tenant_id=$1 AND application_id=$2 AND environment=$3 AND status='ACTIVE'
                    ORDER BY activated_at DESC, id DESC LIMIT 1""",
                    parsed_tenant_id,
                    parsed_application_id,
                    environment,
                )
        except SQLAlchemyError:
            deployment = self._routes.get(cache_key)
            if deployment is None:
                return None
        else:
            if row is None:
                return None
            deployment = DeploymentRoute(
                release_id=str(row["release_id"]),
                previous_release_id=(
                    str(row["previous_release_id"])
                    if row["previous_release_id"] is not None
                    else None
                ),
                rollout_percentage=int(row["rollout_percentage"]),
            )
            self._routes[cache_key] = deployment
        previous_release_id = deployment.previous_release_id
        if (
            previous_release_id is not None
            and deployment.rollout_percentage < 100
            and _session_rollout_bucket(session_id) >= deployment.rollout_percentage
        ):
            return previous_release_id
        return deployment.release_id


def _session_rollout_bucket(session_id: str) -> int:
    return int.from_bytes(hashlib.sha256(session_id.encode()).digest()[:8], "big") % 100


class AgentWorker:
    """Deep runtime module that resolves a Release once before running an execution."""

    def __init__(
        self,
        gateway: GatewayCompletionClient,
        releases: ReleaseRouteResolver,
        fallback_auditor: FallbackAuditor | None = None,
        deployments: DeploymentRouteResolver | None = None,
    ) -> None:
        self._gateway, self._releases, self._fallback_auditor = gateway, releases, fallback_auditor
        self._deployments = deployments

    async def complete_for_deployment(
        self,
        tenant_id: str,
        application_id: str,
        environment: str,
        session_id: str,
        messages: list[dict[str, str]],
    ) -> GatewayResult:
        if self._deployments is None:
            raise ModelGatewayError("DEPLOYMENT_ROUTING_UNAVAILABLE")
        release_id = await self._deployments.resolve(
            tenant_id, application_id, environment, session_id
        )
        if release_id is None:
            raise ModelGatewayError("DEPLOYMENT_NOT_FOUND")
        return await self.complete(AgentExecutionRequest(tenant_id, release_id, messages))

    async def complete(self, request: AgentExecutionRequest) -> GatewayResult:
        route = await self._releases.resolve(request.tenant_id, request.release_id)
        if route is None:
            raise ModelGatewayError("RELEASE_NOT_FOUND")
        result = await GatewayModel(
            gateway=self._gateway,
            tenant_id=route.tenant_id,
            model_alias=route.model_alias,
            data_classification=route.data_classification,
            region=route.region,
            allowed_fallback_aliases=route.allowed_fallback_aliases,
            profile_snapshots=route.profile_snapshots,
            release_id=route.release_id,
            application_id=request.application_id,
            execution_id=request.execution_id,
        ).complete(request.messages)
        if result.fallback_used and self._fallback_auditor is not None:
            try:
                await self._fallback_auditor.record(route, result)
            except Exception as error:
                raise ModelGatewayError("FALLBACK_AUDIT_UNAVAILABLE") from error
        return result


class AgentExecutionProcessor:
    """At-least-once bus consumer committing one authoritative Session state per message.

    Duplicates are collapsed twice: redelivered messages skip executions that
    already succeeded, and concurrent consumers arbitrate at commit time through
    the idempotency key, the Session lease fencing token and the expected
    Session version. Any lost race or expired lease fails closed — the result
    is discarded and the message is redelivered.
    """

    def __init__(
        self,
        worker: AgentWorker,
        database: Database,
        releases: ReleaseRouteResolver,
        leases: SessionLeaseManager,
        owner_id: str,
    ) -> None:
        self._worker = worker
        self._database = database
        self._releases = releases
        self._leases = leases
        self._owner_id = owner_id

    async def handle(self, envelope: ExecutionEnvelope) -> None:
        data = ExecutionRequestedData.model_validate(envelope.data)
        tenant_id = UUID(data.tenant_id)
        session_id = data.session_id
        if await self._execution_status(tenant_id, envelope.message_id) == "SUCCEEDED":
            return
        grant: LeaseGrant = await self._leases.acquire(tenant_id, session_id, self._owner_id)
        route = await self._releases.resolve(data.tenant_id, data.release_id)
        if route is None:
            raise ModelGatewayError("RELEASE_NOT_FOUND")
        result = await self._worker.complete(
            AgentExecutionRequest(
                data.tenant_id,
                data.release_id,
                data.messages,
                application_id=data.application_id,
                execution_id=data.execution_id,
            )
        )
        await self._leases.renew(tenant_id, session_id, self._owner_id, grant.fencing_token)
        events = [
            SessionEvent(
                kind="USER_MESSAGE",
                payload={"content": data.messages[-1].get("content", "")},
            ),
            SessionEvent(
                kind="AGENT_REPLY",
                payload={
                    "model_alias": result.model_alias,
                    "fallback_used": result.fallback_used,
                    "completion": result.completion,
                },
            ),
        ]
        async with self._database.tenant_transaction(tenant_id) as connection:
            new_version = await commit_session_events(
                connection,
                tenant_id=tenant_id,
                session_id=session_id,
                owner_id=self._owner_id,
                fencing_token=grant.fencing_token,
                expected_version=grant.session_version,
                execution_id=UUID(data.execution_id),
                idempotency_key=envelope.message_id,
                events=events,
                data_classification=str(route.data_classification),
            )
            if new_version is None:
                return
            await connection.execute(
                """UPDATE tenant.agent_execution SET status='SUCCEEDED',updated_at=now()
                WHERE tenant_id=$1 AND id=$2""",
                tenant_id,
                UUID(data.execution_id),
            )

    async def _execution_status(self, tenant_id: UUID, message_id: str) -> str | None:
        async with self._database.tenant_transaction(tenant_id) as connection:
            status: str | None = await connection.fetchval(
                """SELECT status FROM tenant.agent_execution
                WHERE tenant_id=$1 AND message_id=$2""",
                tenant_id,
                message_id,
            )
            return status


class AgentWorkerSettings(BaseSettings):
    """Runtime-only settings; provider credentials never enter this boundary."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = ""
    llm_gateway_url: str = ""
    environment: str = "PRODUCTION"
    code_executor_kind: str = "SANDBOX"
    docker_socket_mounted: bool = False

    def validate_runtime(self) -> None:
        missing = [
            name
            for name, value in {
                "DATABASE_URL": self.database_url,
                "LLM_GATEWAY_URL": self.llm_gateway_url,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"Agent Worker configuration is incomplete: {', '.join(missing)}")
        assert_no_unsafe_local_execution(
            self.code_executor_kind,
            environment=self.environment,
            docker_socket_mounted=self.docker_socket_mounted,
        )


class HttpGatewayClient:
    """The Worker can submit routing metadata and content, but never resolves credentials."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def complete(self, request: GatewayRequest) -> GatewayResult:
        try:
            response = await self._client.post(
                "/internal/v1/llm-completions",
                json={
                    "tenant_id": request.tenant_id,
                    "messages": request.messages,
                    "release_id": request.release_id,
                    "application_id": request.application_id,
                    "execution_id": request.execution_id,
                },
            )
            if response.status_code >= 400:
                raise ModelGatewayError("MODEL_GATEWAY_UNAVAILABLE")
            payload = response.json()
            if not isinstance(payload, dict):
                raise ModelGatewayError("MODEL_GATEWAY_UNAVAILABLE")
            return GatewayResult(
                model_alias=str(payload["model_alias"]),
                fallback_used=bool(payload["fallback_used"]),
                completion=dict(payload["completion"]),
            )
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
            raise ModelGatewayError("MODEL_GATEWAY_UNAVAILABLE") from error


class WorkerExecutionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: UUID
    release_id: UUID
    messages: list[dict[str, str]] = Field(min_length=1, max_length=200)


class DeploymentExecutionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: UUID
    application_id: UUID
    environment: str = Field(pattern=r"^(DEVELOPMENT|STAGING|PRODUCTION)$")
    session_id: str = Field(min_length=1, max_length=512)
    messages: list[dict[str, str]] = Field(min_length=1, max_length=200)


class WorkerExecutionResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    model_alias: str
    fallback_used: bool
    completion: dict[str, Any]


def create_app(
    settings: AgentWorkerSettings | None = None, *, worker: AgentWorker | None = None
) -> FastAPI:
    """Create the credential-free internal Agent execution process boundary."""

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        if worker is not None:
            application.state.worker = worker
            yield
            return
        configured = settings or AgentWorkerSettings()
        configured.validate_runtime()
        database = Database(configured.database_url)
        gateway_client = httpx.AsyncClient(base_url=configured.llm_gateway_url)
        await database.open()
        application.state.worker = AgentWorker(
            HttpGatewayClient(gateway_client),
            DatabaseReleaseRouteResolver(database),
            DatabaseFallbackAuditor(database),
            DatabaseDeploymentRouteResolver(database),
        )
        try:
            yield
        finally:
            await gateway_client.aclose()
            await database.close()

    application = FastAPI(
        title="tRPC-Agent Platform agent-worker",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    def health() -> RuntimeHealthResponse:
        return RuntimeHealthResponse(
            service="agent-worker", version=__version__, trpc_agent_version=TRPC_AGENT_VERSION
        )

    @application.get("/health/live", response_model=RuntimeHealthResponse)
    async def live() -> RuntimeHealthResponse:
        return health()

    @application.get("/health/ready", response_model=RuntimeHealthResponse)
    async def ready() -> RuntimeHealthResponse:
        return health()

    @application.post("/internal/v1/agent-executions", response_model=WorkerExecutionResponse)
    async def execute(payload: WorkerExecutionPayload) -> WorkerExecutionResponse:
        try:
            result = await application.state.worker.complete(
                AgentExecutionRequest(
                    tenant_id=str(payload.tenant_id),
                    release_id=str(payload.release_id),
                    messages=payload.messages,
                )
            )
        except ModelGatewayError as error:
            raise HTTPException(status_code=409, detail=error.code) from error
        return WorkerExecutionResponse(
            model_alias=result.model_alias,
            fallback_used=result.fallback_used,
            completion=result.completion,
        )

    @application.post("/internal/v1/deployment-executions", response_model=WorkerExecutionResponse)
    async def execute_deployment(payload: DeploymentExecutionPayload) -> WorkerExecutionResponse:
        try:
            result = await application.state.worker.complete_for_deployment(
                str(payload.tenant_id),
                str(payload.application_id),
                payload.environment,
                payload.session_id,
                payload.messages,
            )
        except ModelGatewayError as error:
            raise HTTPException(status_code=409, detail=error.code) from error
        return WorkerExecutionResponse(
            model_alias=result.model_alias,
            fallback_used=result.fallback_used,
            completion=result.completion,
        )

    return application


app = create_app()
