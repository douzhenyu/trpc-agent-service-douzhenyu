"""Credential-injecting, tenant-scoped LLM Gateway routing boundary."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from time import monotonic
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from trpc_service.admin_api.database import Database


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


@dataclass(frozen=True)
class GatewayResult:
    model_alias: str
    fallback_used: bool
    completion: dict[str, Any]


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
                        "data_classification": request.data_classification,
                        "region": request.region,
                        "messages": request.messages,
                    }
                },
            )
            response.raise_for_status()
            result = response.json().get("result")
            return result is True or (isinstance(result, Mapping) and result.get("allow") is True)
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
    ) -> None:
        self._profiles = profiles
        self._secrets = secrets
        self._client = client
        self._circuit_failure_threshold = circuit_failure_threshold
        self._circuit_reset_seconds = circuit_reset_seconds
        self._observer = observer or LoggingGatewayObserver()
        self._policy = policy
        self._failures: dict[tuple[str, str], int] = defaultdict(int)
        self._opened_at: dict[tuple[str, str], float] = {}
        self._requests: dict[tuple[str, str], deque[float]] = defaultdict(deque)

    async def complete(self, request: GatewayRequest) -> GatewayResult:
        started_at = monotonic()
        request_content_hash = _content_hash(request.messages)
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
        while aliases:
            alias = aliases.pop(0)
            if alias in seen:
                continue
            seen.add(alias)
            profile = await self._profiles.resolve(request.tenant_id, alias)
            if profile is None:
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
        gateway: LLMGateway,
        tenant_id: str,
        model_alias: str,
        data_classification: DataClassification,
        region: str,
        allowed_fallback_aliases: frozenset[str] = frozenset(),
    ) -> None:
        self._gateway = gateway
        self._tenant_id = tenant_id
        self._model_alias = model_alias
        self._data_classification = data_classification
        self._region = region
        self._allowed_fallback_aliases = allowed_fallback_aliases

    async def complete(self, messages: list[dict[str, str]]) -> GatewayResult:
        return await self._gateway.complete(
            GatewayRequest(
                tenant_id=self._tenant_id,
                model_alias=self._model_alias,
                messages=messages,
                data_classification=self._data_classification,
                region=self._region,
                allowed_fallback_aliases=self._allowed_fallback_aliases,
            )
        )


def _content_hash(content: object) -> str:
    return sha256(repr(content).encode()).hexdigest()


def _token_count(usage: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = usage.get(key)
        if isinstance(value, int):
            return value
    return None
