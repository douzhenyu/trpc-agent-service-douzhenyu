"""Credential-injecting, tenant-scoped LLM Gateway routing boundary."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.exc import SQLAlchemyError

from trpc_service.admin_api.database import Database
from trpc_service.budgets import (
    BudgetCommand,
    BudgetExceeded,
    BudgetReservationBundle,
    BudgetService,
    BudgetStateUnknown,
    estimate_cost_micros,
    estimate_tokens,
)
from trpc_service.runtime_health import RuntimeHealthResponse
from trpc_service.version import TRPC_AGENT_VERSION, __version__


class DataClassification(StrEnum):
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"


_CLASSIFICATION_RANK = {
    DataClassification.PUBLIC: 0,
    DataClassification.INTERNAL: 1,
    DataClassification.CONFIDENTIAL: 2,
    DataClassification.RESTRICTED: 3,
}
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelProfile:
    tenant_id: str
    alias: str
    provider_model: str
    endpoint_url: str
    secret_ref: str
    data_classification: DataClassification
    region: str
    fallback_aliases: tuple[str, ...]
    requests_per_minute: int


@dataclass(frozen=True)
class GatewayRequest:
    tenant_id: str
    model_alias: str
    messages: list[dict[str, str]]
    data_classification: DataClassification
    region: str
    allowed_fallback_aliases: frozenset[str] = frozenset()
    profile_snapshots: tuple[ModelProfile, ...] = ()
    release_id: str | None = None
    application_id: str | None = None
    execution_id: str | None = None


@dataclass(frozen=True)
class GatewayResult:
    model_alias: str
    fallback_used: bool
    completion: dict[str, Any]


class GatewayCompletionClient(Protocol):
    async def complete(self, request: GatewayRequest) -> GatewayResult: ...


@dataclass(frozen=True)
class GatewayEvent:
    tenant_id: str
    model_alias: str
    fallback_used: bool
    outcome: str
    error_code: str | None
    request_content_hash: str
    response_content_hash: str | None
    latency_ms: int
    input_tokens: int | None
    output_tokens: int | None
    cost_micros: int | None


class ModelGatewayError(RuntimeError):
    """Safe, stable error for callers; never embeds provider response bodies."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ModelProfileResolver(Protocol):
    async def resolve(self, tenant_id: str, alias: str) -> ModelProfile | None: ...


class SecretProvider(Protocol):
    async def resolve(self, tenant_id: str, secret_ref: str) -> str: ...


class OutboundPolicy(Protocol):
    async def allows(self, request: GatewayRequest, profile: ModelProfile) -> bool: ...


class BudgetGuard(Protocol):
    """Reserve before a billable model call; settle after, release on failure."""

    async def reserve(self, request: GatewayRequest) -> BudgetReservationBundle: ...

    async def settle(self, bundle: BudgetReservationBundle) -> None: ...

    async def release(self, bundle: BudgetReservationBundle, *, reason: str) -> None: ...


class VaultSecretProvider:
    """Vault KV v2 adapter authenticated through Kubernetes Auth."""

    def __init__(self, client: httpx.AsyncClient, *, kubernetes_jwt: str, role: str) -> None:
        self._client, self._kubernetes_jwt, self._role = client, kubernetes_jwt, role
        self._token: str | None = None

    async def resolve(self, tenant_id: str, secret_ref: str) -> str:
        parsed = urlsplit(secret_ref)
        path = parsed.path.lstrip("/")
        if (
            parsed.scheme != "vault"
            or parsed.netloc != "tenant"
            or not parsed.fragment
            or "/" not in path
            or path.split("/", maxsplit=1)[0] != tenant_id
        ):
            raise ModelGatewayError("SECRET_REFERENCE_INVALID")
        if self._token is None:
            response = await self._client.post(
                "/v1/auth/kubernetes/login", json={"jwt": self._kubernetes_jwt, "role": self._role}
            )
            response.raise_for_status()
            payload = response.json()
            token = (
                payload.get("auth", {}).get("client_token")
                if isinstance(payload, Mapping)
                else None
            )
            if not isinstance(token, str) or not token:
                raise ModelGatewayError("SECRET_RESOLUTION_FAILED")
            self._token = token
        response = await self._client.get(
            f"/v1/tenant/data/{path}", headers={"X-Vault-Token": self._token}
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", {}).get("data", {}) if isinstance(payload, Mapping) else {}
        value = data.get(parsed.fragment) if isinstance(data, Mapping) else None
        if not isinstance(value, str):
            raise ModelGatewayError("SECRET_RESOLUTION_FAILED")
        return value


class OpaOutboundPolicy:
    """Fail-closed OPA adapter evaluated before any provider request."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def allows(self, request: GatewayRequest, profile: ModelProfile) -> bool:
        try:
            response = await self._client.post(
                "/v1/data/platform/llm/allow",
                json={
                    "input": {
                        "tenant_id": request.tenant_id,
                        "model_alias": profile.alias,
                        "endpoint_url": profile.endpoint_url,
                        "data_classification": request.data_classification,
                        "region": request.region,
                        "messages": request.messages,
                    }
                },
            )
            response.raise_for_status()
            result = response.json().get("result")
            if result is True:
                return request.data_classification is not DataClassification.CONFIDENTIAL
            if not isinstance(result, Mapping) or result.get("allow") is not True:
                return False
            return (
                request.data_classification is not DataClassification.CONFIDENTIAL
                or result.get("private_endpoint") is True
            )
        except (httpx.HTTPError, ValueError, AttributeError):
            return False


class GatewayObserver(Protocol):
    def record(self, event: GatewayEvent) -> None: ...


class LoggingGatewayObserver:
    """Default safe observability sink; never receives credentials or content bodies."""

    def record(self, event: GatewayEvent) -> None:
        _LOGGER.info("llm_gateway_event=%s", event)


class InMemoryGatewayObserver:
    """Test-only safe observability sink."""

    def __init__(self) -> None:
        self.events: list[GatewayEvent] = []

    def record(self, event: GatewayEvent) -> None:
        self.events.append(event)


class InMemoryModelProfileResolver:
    """Test-only profile source. Production adapters must read tenant-scoped metadata."""

    def __init__(self, profiles: Sequence[ModelProfile]) -> None:
        self._profiles = {(profile.tenant_id, profile.alias): profile for profile in profiles}

    async def resolve(self, tenant_id: str, alias: str) -> ModelProfile | None:
        return self._profiles.get((tenant_id, alias))


class DatabaseModelProfileResolver:
    """Runtime resolver for tenant-scoped configuration stored by the Admin API."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def resolve(self, tenant_id: str, alias: str) -> ModelProfile | None:
        try:
            parsed_tenant_id = UUID(tenant_id)
        except ValueError:
            return None
        async with self._database.tenant_transaction(parsed_tenant_id) as connection:
            row = await connection.fetchrow(
                """SELECT tenant_id,alias,provider_model,endpoint_url,secret_ref,
                data_classification,region,fallback_aliases,requests_per_minute
                FROM tenant.model_profile WHERE tenant_id=$1 AND alias=$2""",
                parsed_tenant_id,
                alias,
            )
        if row is None:
            return None
        return ModelProfile(
            tenant_id=str(row["tenant_id"]),
            alias=str(row["alias"]),
            provider_model=str(row["provider_model"]),
            endpoint_url=str(row["endpoint_url"]),
            secret_ref=str(row["secret_ref"]),
            data_classification=DataClassification(str(row["data_classification"])),
            region=str(row["region"]),
            fallback_aliases=tuple(str(item) for item in row["fallback_aliases"]),
            requests_per_minute=int(row["requests_per_minute"]),
        )


class LLMGateway:
    def __init__(
        self,
        profiles: ModelProfileResolver,
        secrets: SecretProvider,
        client: httpx.AsyncClient,
        *,
        circuit_failure_threshold: int = 3,
        circuit_reset_seconds: float = 30,
        observer: GatewayObserver | None = None,
        policy: OutboundPolicy,
        budget: BudgetGuard | None = None,
    ) -> None:
        self._profiles = profiles
        self._secrets = secrets
        self._client = client
        self._circuit_failure_threshold = circuit_failure_threshold
        self._circuit_reset_seconds = circuit_reset_seconds
        self._observer = observer or LoggingGatewayObserver()
        self._policy = policy
        self._budget = budget
        self._failures: dict[tuple[str, str], int] = defaultdict(int)
        self._opened_at: dict[tuple[str, str], float] = {}
        self._requests: dict[tuple[str, str], deque[float]] = defaultdict(deque)

    async def complete(self, request: GatewayRequest) -> GatewayResult:
        started_at = monotonic()
        request_content_hash = _content_hash(request.messages)
        bundle = None
        if self._budget is not None:
            # The guard maps its domain failures to BUDGET_EXCEEDED and
            # BUDGET_STATE_UNKNOWN; both fail closed before any provider call.
            bundle = await self._budget.reserve(request)
        try:
            return await self._complete_with_guard(
                request,
                bundle,
                started_at=started_at,
                request_content_hash=request_content_hash,
            )
        except ModelGatewayError as error:
            if bundle is not None and self._budget is not None:
                try:
                    await self._budget.release(bundle, reason=error.code)
                except Exception as release_error:
                    raise ModelGatewayError("BUDGET_STATE_UNKNOWN") from release_error
            raise

    async def _complete_with_guard(
        self,
        request: GatewayRequest,
        bundle: BudgetReservationBundle | None,
        *,
        started_at: float,
        request_content_hash: str,
    ) -> GatewayResult:
        attempted = False
        rate_limited = False
        circuit_open = False
        last_failure_code: str | None = None
        failure_recorded = False
        for profile in await self._candidate_profiles(request):
            if not await self._allows(request, profile):
                continue
            attempted = True
            key = (profile.tenant_id, profile.alias)
            if self._circuit_open(key):
                circuit_open = True
                continue
            if not self._within_rate_limit(key, profile.requests_per_minute):
                rate_limited = True
                continue
            try:
                credential = await self._secrets.resolve(profile.tenant_id, profile.secret_ref)
            except Exception:  # Secret backends must never leak implementation errors to callers.
                last_failure_code = "SECRET_RESOLUTION_FAILED"
                self._record_failure(key)
                self._record_event(
                    request,
                    profile.alias,
                    False,
                    "FAILURE",
                    last_failure_code,
                    request_content_hash,
                    None,
                    started_at,
                    None,
                )
                failure_recorded = True
                continue
            try:
                completion = await self._provider_completion(profile, request.messages, credential)
            except (httpx.HTTPError, ModelGatewayError):
                last_failure_code = "UPSTREAM_FAILURE"
                self._record_failure(key)
                self._record_event(
                    request,
                    profile.alias,
                    False,
                    "FAILURE",
                    last_failure_code,
                    request_content_hash,
                    None,
                    started_at,
                    None,
                )
                failure_recorded = True
                continue
            self._record_success(key)
            result = GatewayResult(
                model_alias=profile.alias,
                fallback_used=profile.alias != request.model_alias,
                completion=completion,
            )
            if bundle is not None and self._budget is not None:
                # Fail closed on settlement uncertainty: the bookkeeping must
                # not silently diverge from actual spend.
                await self._budget.settle(bundle)
            self._record_event(
                request,
                result.model_alias,
                result.fallback_used,
                "SUCCESS",
                None,
                request_content_hash,
                _content_hash(completion),
                started_at,
                completion,
            )
            return result
        if not attempted:
            error = ModelGatewayError("MODEL_POLICY_DENIED")
        elif rate_limited and not circuit_open:
            error = ModelGatewayError("RATE_LIMITED")
        elif circuit_open and not rate_limited:
            error = ModelGatewayError("CIRCUIT_OPEN")
        elif last_failure_code == "SECRET_RESOLUTION_FAILED":
            error = ModelGatewayError(last_failure_code)
        else:
            error = ModelGatewayError("MODEL_UNAVAILABLE")
        if not failure_recorded:
            self._record_event(
                request,
                request.model_alias,
                False,
                "FAILURE",
                error.code,
                request_content_hash,
                None,
                started_at,
                None,
            )
        raise error

    async def _candidate_profiles(self, request: GatewayRequest) -> list[ModelProfile]:
        aliases = [request.model_alias]
        profiles: list[ModelProfile] = []
        seen: set[str] = set()
        snapshots = {profile.alias: profile for profile in request.profile_snapshots}
        while aliases:
            alias = aliases.pop(0)
            if alias in seen:
                continue
            seen.add(alias)
            profile = snapshots.get(alias)
            if profile is None and not snapshots:
                profile = await self._profiles.resolve(request.tenant_id, alias)
            if profile is None:
                continue
            if profile.tenant_id != request.tenant_id:
                continue
            profiles.append(profile)
            aliases.extend(
                fallback
                for fallback in profile.fallback_aliases
                if fallback in request.allowed_fallback_aliases
            )
        return profiles

    async def _allows(self, request: GatewayRequest, profile: ModelProfile) -> bool:
        allowed = (
            request.data_classification is not DataClassification.RESTRICTED
            and request.region == profile.region
            and _CLASSIFICATION_RANK[request.data_classification]
            <= _CLASSIFICATION_RANK[profile.data_classification]
        )
        return allowed and await self._policy.allows(request, profile)

    def _record_event(
        self,
        request: GatewayRequest,
        model_alias: str,
        fallback_used: bool,
        outcome: str,
        error_code: str | None,
        request_content_hash: str,
        response_content_hash: str | None,
        started_at: float,
        completion: Mapping[str, Any] | None,
    ) -> None:
        usage = completion.get("usage") if completion is not None else None
        usage_mapping = usage if isinstance(usage, Mapping) else {}
        self._observer.record(
            GatewayEvent(
                tenant_id=request.tenant_id,
                model_alias=model_alias,
                fallback_used=fallback_used,
                outcome=outcome,
                error_code=error_code,
                request_content_hash=request_content_hash,
                response_content_hash=response_content_hash,
                latency_ms=int((monotonic() - started_at) * 1000),
                input_tokens=_token_count(usage_mapping, "prompt_tokens", "input_tokens"),
                output_tokens=_token_count(usage_mapping, "completion_tokens", "output_tokens"),
                cost_micros=None,
            )
        )

    def _circuit_open(self, key: tuple[str, str]) -> bool:
        opened_at = self._opened_at.get(key)
        if opened_at is None:
            return False
        if monotonic() - opened_at < self._circuit_reset_seconds:
            return True
        self._opened_at.pop(key, None)
        self._failures[key] = 0
        return False

    def _within_rate_limit(self, key: tuple[str, str], limit: int) -> bool:
        now = monotonic()
        requests = self._requests[key]
        while requests and requests[0] <= now - 60:
            requests.popleft()
        if len(requests) >= limit:
            return False
        requests.append(now)
        return True

    def _record_failure(self, key: tuple[str, str]) -> None:
        self._failures[key] += 1
        if self._failures[key] >= self._circuit_failure_threshold:
            self._opened_at[key] = monotonic()

    def _record_success(self, key: tuple[str, str]) -> None:
        self._failures[key] = 0
        self._opened_at.pop(key, None)

    async def _provider_completion(
        self,
        profile: ModelProfile,
        messages: list[dict[str, str]],
        credential: str,
    ) -> dict[str, Any]:
        response = await self._client.post(
            profile.endpoint_url,
            headers={
                "Authorization": f"Bearer {credential}",
                "X-Model-Alias": profile.alias,
            },
            json={"model": profile.provider_model, "messages": messages},
        )
        if response.status_code >= 400:
            raise ModelGatewayError("UPSTREAM_FAILURE")
        try:
            payload = response.json()
        except ValueError as error:
            raise ModelGatewayError("UPSTREAM_INVALID_RESPONSE") from error
        if not isinstance(payload, Mapping):
            raise ModelGatewayError("UPSTREAM_INVALID_RESPONSE")
        return dict(payload)


class GatewayModel:
    """Credential-free model adapter for Agent Worker runtime integration."""

    def __init__(
        self,
        *,
        gateway: GatewayCompletionClient,
        tenant_id: str,
        model_alias: str,
        data_classification: DataClassification,
        region: str,
        allowed_fallback_aliases: frozenset[str] = frozenset(),
        profile_snapshots: tuple[ModelProfile, ...] = (),
        release_id: str | None = None,
        application_id: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        self._gateway = gateway
        self._tenant_id = tenant_id
        self._model_alias = model_alias
        self._data_classification = data_classification
        self._region = region
        self._allowed_fallback_aliases = allowed_fallback_aliases
        self._profile_snapshots = profile_snapshots
        self._release_id = release_id
        self._application_id = application_id
        self._execution_id = execution_id

    async def complete(self, messages: list[dict[str, str]]) -> GatewayResult:
        return await self._gateway.complete(
            GatewayRequest(
                tenant_id=self._tenant_id,
                model_alias=self._model_alias,
                messages=messages,
                data_classification=self._data_classification,
                region=self._region,
                allowed_fallback_aliases=self._allowed_fallback_aliases,
                profile_snapshots=self._profile_snapshots,
                release_id=self._release_id,
                application_id=self._application_id,
                execution_id=self._execution_id,
            )
        )


@dataclass(frozen=True)
class ModelPrice:
    model_alias: str
    input_micros_per_1k: int
    output_micros_per_1k: int


class DatabaseBudgetGuard:
    """LLM Gateway BudgetGuard: estimates, reserves, settles and releases.

    Domain failures are mapped to stable ModelGatewayError codes so the model
    call path fails closed with its own error vocabulary.
    """

    def __init__(self, service: BudgetService) -> None:
        self._service = service

    async def latest_prices(self, tenant_id: str) -> dict[str, ModelPrice]:
        async with self._service._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                """SELECT model_alias,input_micros_per_1k,output_micros_per_1k
                FROM tenant.model_price
                WHERE tenant_id=$1
                  AND version=(SELECT max(version) FROM tenant.model_price
                WHERE tenant_id=$1)""",
                UUID(tenant_id),
            )
        return {
            str(row["model_alias"]): ModelPrice(
                model_alias=str(row["model_alias"]),
                input_micros_per_1k=int(row["input_micros_per_1k"]),
                output_micros_per_1k=int(row["output_micros_per_1k"]),
            )
            for row in rows
        }

    async def reserve(self, request: GatewayRequest) -> BudgetReservationBundle:
        prices = await self.latest_prices(request.tenant_id)
        price = prices.get(request.model_alias, ModelPrice(request.model_alias, 0, 0))
        input_tokens, output_tokens = estimate_tokens(request.messages)
        estimated = estimate_cost_micros(
            input_tokens,
            output_tokens,
            input_micros_per_1k=price.input_micros_per_1k,
            output_micros_per_1k=price.output_micros_per_1k,
        )
        try:
            return await self._service.reserve(
                BudgetCommand(
                    tenant_id=request.tenant_id,
                    application_id=request.application_id,
                    execution_id=request.execution_id,
                    estimated_micros=estimated,
                )
            )
        except BudgetExceeded as error:
            raise ModelGatewayError(error.code) from error
        except BudgetStateUnknown as error:
            raise ModelGatewayError(error.code) from error

    async def settle(self, bundle: BudgetReservationBundle) -> None:
        try:
            await self._service.settle(bundle)
        except SQLAlchemyError as error:
            raise ModelGatewayError("BUDGET_STATE_UNKNOWN") from error

    async def release(self, bundle: BudgetReservationBundle, *, reason: str) -> None:
        try:
            await self._service.release(bundle, reason=reason)
        except SQLAlchemyError as error:
            raise ModelGatewayError("BUDGET_STATE_UNKNOWN") from error


def _content_hash(content: object) -> str:
    return sha256(repr(content).encode()).hexdigest()


def _token_count(usage: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = usage.get(key)
        if isinstance(value, int):
            return value
    return None


class GatewayRuntimeSettings(BaseSettings):
    """Gateway-only settings; this is the sole provider credential injection process."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = ""
    vault_url: str = ""
    vault_kubernetes_role: str = "agent-gateway"
    opa_url: str = ""
    kubernetes_jwt_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    def validate_runtime(self) -> None:
        missing = [
            name
            for name, value in {
                "DATABASE_URL": self.database_url,
                "VAULT_URL": self.vault_url,
                "OPA_URL": self.opa_url,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"LLM Gateway configuration is incomplete: {', '.join(missing)}")


class GatewayCompletionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str = Field(min_length=36, max_length=36)
    messages: list[dict[str, str]] = Field(min_length=1, max_length=200)
    release_id: str = Field(min_length=36, max_length=36)
    application_id: str | None = Field(default=None, min_length=1, max_length=64)
    execution_id: str | None = Field(default=None, min_length=1, max_length=64)


class GatewayCompletionResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    model_alias: str
    fallback_used: bool
    completion: dict[str, Any]


def create_app(
    settings: GatewayRuntimeSettings | None = None,
    *,
    budget: BudgetGuard | None = None,
) -> FastAPI:
    """Create the sole in-cluster process allowed to resolve provider credentials."""

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        configured = settings or GatewayRuntimeSettings()
        configured.validate_runtime()
        jwt = Path(configured.kubernetes_jwt_path).read_text().strip()
        if not jwt:
            raise RuntimeError("Kubernetes service account token is empty")
        database = Database(configured.database_url)
        vault_client = httpx.AsyncClient(base_url=configured.vault_url)
        opa_client = httpx.AsyncClient(base_url=configured.opa_url)
        provider_client = httpx.AsyncClient()
        await database.open()
        application.state.gateway = LLMGateway(
            DatabaseModelProfileResolver(database),
            VaultSecretProvider(
                vault_client, kubernetes_jwt=jwt, role=configured.vault_kubernetes_role
            ),
            provider_client,
            policy=OpaOutboundPolicy(opa_client),
            budget=budget or DatabaseBudgetGuard(BudgetService(database)),
        )
        application.state.database = database
        try:
            yield
        finally:
            await provider_client.aclose()
            await opa_client.aclose()
            await vault_client.aclose()
            await database.close()

    application = FastAPI(
        title="tRPC-Agent Platform LLM Gateway",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    def health() -> RuntimeHealthResponse:
        return RuntimeHealthResponse(
            service="agent-gateway", version=__version__, trpc_agent_version=TRPC_AGENT_VERSION
        )

    @application.get("/health/live", response_model=RuntimeHealthResponse)
    async def live() -> RuntimeHealthResponse:
        return health()

    @application.get("/health/ready", response_model=RuntimeHealthResponse)
    async def ready() -> RuntimeHealthResponse:
        return health()

    @application.post("/internal/v1/llm-completions", response_model=GatewayCompletionResponse)
    async def complete(payload: GatewayCompletionPayload) -> GatewayCompletionResponse:
        try:
            request = await _released_request(
                application.state.database,
                payload.tenant_id,
                payload.release_id,
                payload.messages,
                application_id=payload.application_id,
                execution_id=payload.execution_id,
            )
            result = await application.state.gateway.complete(request)
        except ModelGatewayError as error:
            raise HTTPException(status_code=409, detail=error.code) from error
        return GatewayCompletionResponse(
            model_alias=result.model_alias,
            fallback_used=result.fallback_used,
            completion=result.completion,
        )

    return application


async def _released_request(
    database: Database,
    tenant_id: str,
    release_id: str,
    messages: list[dict[str, str]],
    *,
    application_id: str | None = None,
    execution_id: str | None = None,
) -> GatewayRequest:
    try:
        tenant_uuid, release_uuid = UUID(tenant_id), UUID(release_id)
    except ValueError as error:
        raise ModelGatewayError("RELEASE_NOT_FOUND") from error
    async with database.tenant_transaction(tenant_uuid) as connection:
        row = await connection.fetchrow(
            """SELECT model_alias,data_classification,region,fallback_aliases,model_profiles
            FROM tenant.agent_release WHERE tenant_id=$1 AND id=$2""",
            tenant_uuid,
            release_uuid,
        )
    if row is None or not row["model_profiles"]:
        raise ModelGatewayError("RELEASE_NOT_FOUND")
    snapshots = tuple(
        ModelProfile(
            tenant_id=str(item["tenant_id"]),
            alias=str(item["alias"]),
            provider_model=str(item["provider_model"]),
            endpoint_url=str(item["endpoint_url"]),
            secret_ref=str(item["secret_ref"]),
            data_classification=DataClassification(str(item["data_classification"])),
            region=str(item["region"]),
            fallback_aliases=tuple(str(alias) for alias in item["fallback_aliases"]),
            requests_per_minute=int(item["requests_per_minute"]),
        )
        for item in row["model_profiles"]
    )
    return GatewayRequest(
        tenant_id=tenant_id,
        model_alias=str(row["model_alias"]),
        messages=messages,
        data_classification=DataClassification(str(row["data_classification"])),
        region=str(row["region"]),
        allowed_fallback_aliases=frozenset(str(alias) for alias in row["fallback_aliases"]),
        profile_snapshots=snapshots,
        release_id=release_id,
    )


app = create_app()
