"""Agent Worker runtime module that routes released executions through LLM Gateway."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.database import Database
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


@dataclass(frozen=True)
class AgentExecutionRequest:
    tenant_id: str
    release_id: str
    messages: list[dict[str, str]]


class ReleaseRouteResolver(Protocol):
    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None: ...


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

    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None:
        try:
            parsed_tenant_id, parsed_release_id = UUID(tenant_id), UUID(release_id)
        except ValueError:
            return None
        async with self._database.tenant_transaction(parsed_tenant_id) as connection:
            row = await connection.fetchrow(
                """SELECT id,tenant_id,model_alias,data_classification,region,fallback_aliases,
                model_profiles
                FROM tenant.agent_release WHERE tenant_id=$1 AND id=$2""",
                parsed_tenant_id,
                parsed_release_id,
            )
        if row is None:
            return None
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
        return ReleaseRoute(
            release_id=str(row["id"]),
            tenant_id=str(row["tenant_id"]),
            model_alias=str(row["model_alias"]),
            data_classification=DataClassification(str(row["data_classification"])),
            region=str(row["region"]),
            allowed_fallback_aliases=frozenset(str(alias) for alias in row["fallback_aliases"]),
            profile_snapshots=snapshots,
        )


class AgentWorker:
    """Deep runtime module: callers supply only a released execution and messages."""

    def __init__(
        self,
        gateway: GatewayCompletionClient,
        releases: ReleaseRouteResolver,
        fallback_auditor: FallbackAuditor | None = None,
    ) -> None:
        self._gateway, self._releases, self._fallback_auditor = gateway, releases, fallback_auditor

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
        ).complete(request.messages)
        if result.fallback_used and self._fallback_auditor is not None:
            try:
                await self._fallback_auditor.record(route, result)
            except Exception as error:
                raise ModelGatewayError("FALLBACK_AUDIT_UNAVAILABLE") from error
        return result


class AgentWorkerSettings(BaseSettings):
    """Runtime-only settings; provider credentials never enter this boundary."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = ""
    llm_gateway_url: str = ""

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
                    "model_alias": request.model_alias,
                    "messages": request.messages,
                    "data_classification": request.data_classification,
                    "region": request.region,
                    "allowed_fallback_aliases": sorted(request.allowed_fallback_aliases),
                    "profile_snapshots": [
                        {
                            "tenant_id": profile.tenant_id,
                            "alias": profile.alias,
                            "provider_model": profile.provider_model,
                            "endpoint_url": profile.endpoint_url,
                            "secret_ref": profile.secret_ref,
                            "data_classification": profile.data_classification,
                            "region": profile.region,
                            "fallback_aliases": profile.fallback_aliases,
                            "requests_per_minute": profile.requests_per_minute,
                        }
                        for profile in request.profile_snapshots
                    ],
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

    return application


app = create_app()
