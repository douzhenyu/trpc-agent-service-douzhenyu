from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from trpc_service.admin_api.schemas import AgentReleaseSource
from trpc_service.agent_worker import (
    AgentExecutionRequest,
    AgentWorker,
    DatabaseDeploymentRouteResolver,
    DatabaseReleaseRouteResolver,
    HttpGatewayClient,
    InMemoryReleaseRouteResolver,
    ReleaseRoute,
    create_app,
)
from trpc_service.llm_gateway import (
    DataClassification,
    GatewayRequest,
    GatewayResult,
    InMemoryModelProfileResolver,
    LLMGateway,
    ModelGatewayError,
    ModelProfile,
    OpaOutboundPolicy,
    VaultSecretProvider,
)


class AllowOutboundPolicy:
    async def allows(self, _request: object, _profile: object) -> bool:
        return True


class FakeSecretProvider:
    async def resolve(self, tenant_id: str, secret_ref: str) -> str:
        assert tenant_id in secret_ref
        return "secret"


class FakeWorker:
    async def complete(self, request: AgentExecutionRequest) -> GatewayResult:
        assert request.messages == [{"role": "user", "content": "hello"}]
        return GatewayResult("economy", True, {"choices": [{"message": {"content": "reply"}}]})


class MutableDeploymentResolver:
    def __init__(self, release_id: str) -> None:
        self.release_id = release_id
        self.resolved = asyncio.Event()

    async def resolve(
        self, _tenant_id: str, _application_id: str, _environment: str, _session_id: str
    ) -> str | None:
        self.resolved.set()
        return self.release_id


class BlockingGateway:
    def __init__(self) -> None:
        self.requests: list[GatewayRequest] = []
        self.release_completion = asyncio.Event()

    async def complete(self, request: GatewayRequest) -> GatewayResult:
        self.requests.append(request)
        await self.release_completion.wait()
        return GatewayResult("balanced", False, {"choices": []})


class CachedRouteDatabase:
    def __init__(self, row: dict[str, object]) -> None:
        self.row = row
        self.unavailable = False

    @asynccontextmanager
    async def tenant_transaction(self, _tenant_id: object):
        yield self

    async def fetchrow(self, _query: str, *_args: object) -> dict[str, object] | None:
        if self.unavailable:
            raise SQLAlchemyError("control plane unavailable")
        return self.row


def test_worker_execution_api_accepts_only_release_and_messages_and_surfaces_fallback() -> None:
    tenant_id, release_id = str(uuid4()), str(uuid4())
    with TestClient(create_app(worker=FakeWorker())) as client:
        response = client.post(
            "/internal/v1/agent-executions",
            json={
                "tenant_id": tenant_id,
                "release_id": release_id,
                "messages": [{"role": "user", "content": "hello"}],
                "api_key": "provider-secret-that-must-not-be-accepted",
            },
        )
        assert response.status_code == 422

        response = client.post(
            "/internal/v1/agent-executions",
            json={
                "tenant_id": tenant_id,
                "release_id": release_id,
                "messages": [{"role": "user", "content": "hello"}],
            },
        )

    assert response.status_code == 200
    assert response.json()["model_alias"] == "economy"
    assert response.json()["fallback_used"] is True


def test_worker_gateway_client_sends_only_release_identity_and_messages() -> None:
    tenant_id, release_id = str(uuid4()), str(uuid4())

    async def gateway(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/internal/v1/llm-completions"
        assert "authorization" not in request.headers
        payload = json.loads(request.content)
        assert set(payload) == {
            "tenant_id",
            "messages",
            "release_id",
        }
        assert payload["release_id"] == release_id
        return httpx.Response(
            200,
            json={"model_alias": "balanced", "fallback_used": False, "completion": {"choices": []}},
        )

    client = HttpGatewayClient(
        httpx.AsyncClient(
            base_url="https://agent-gateway.test", transport=httpx.MockTransport(gateway)
        )
    )
    result = asyncio.run(
        client.complete(
            GatewayRequest(
                tenant_id=tenant_id,
                model_alias="balanced",
                messages=[{"role": "user", "content": "hello"}],
                data_classification=DataClassification.INTERNAL,
                region="cn-north-1",
                release_id=release_id,
            )
        )
    )

    assert result.model_alias == "balanced"


def test_vault_provider_uses_kubernetes_auth_and_returns_only_requested_field() -> None:
    tenant_id = str(uuid4())
    calls: list[str] = []

    async def vault(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/v1/auth/kubernetes/login":
            assert request.json() if False else True
            return httpx.Response(200, json={"auth": {"client_token": "short-lived-token"}})
        assert request.headers["X-Vault-Token"] == "short-lived-token"
        return httpx.Response(200, json={"data": {"data": {"api_key": "provider-secret"}}})

    provider = VaultSecretProvider(
        httpx.AsyncClient(base_url="https://vault.test", transport=httpx.MockTransport(vault)),
        kubernetes_jwt="workload-jwt",
        role="agent-worker",
    )

    secret = asyncio.run(
        provider.resolve(tenant_id, f"vault://tenant/{tenant_id}/llm/openai#api_key")
    )

    assert secret == "provider-secret"
    assert calls == ["/v1/auth/kubernetes/login", f"/v1/tenant/data/{tenant_id}/llm/openai"]


def test_vault_provider_rejects_secret_reference_for_another_tenant_before_network_access() -> None:
    tenant_id, other_tenant_id = str(uuid4()), str(uuid4())
    provider = VaultSecretProvider(
        httpx.AsyncClient(
            base_url="https://vault.test",
            transport=httpx.MockTransport(lambda _request: pytest.fail("Vault must not be called")),
        ),
        kubernetes_jwt="workload-jwt",
        role="agent-worker",
    )

    with pytest.raises(ModelGatewayError, match="SECRET_REFERENCE_INVALID"):
        asyncio.run(
            provider.resolve(tenant_id, f"vault://tenant/{other_tenant_id}/llm/openai#api_key")
        )


def test_opa_policy_fails_closed_and_never_calls_provider_when_denied() -> None:
    tenant_id = str(uuid4())
    provider_calls = 0

    async def opa(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": {"allow": False}})

    async def provider(_request: httpx.Request) -> httpx.Response:
        nonlocal provider_calls
        provider_calls += 1
        return httpx.Response(200, json={"choices": []})

    profile = ModelProfile(
        tenant_id=tenant_id,
        alias="balanced",
        provider_model="fake-balanced",
        endpoint_url="https://provider.test/v1",
        secret_ref=f"vault://tenant/{tenant_id}/llm/openai#api_key",
        data_classification=DataClassification.CONFIDENTIAL,
        region="cn-north-1",
        fallback_aliases=(),
        requests_per_minute=60,
    )
    gateway = LLMGateway(
        InMemoryModelProfileResolver([profile]),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(provider)),
        policy=OpaOutboundPolicy(
            httpx.AsyncClient(base_url="https://opa.test", transport=httpx.MockTransport(opa))
        ),
    )

    with pytest.raises(ModelGatewayError, match="MODEL_POLICY_DENIED"):
        asyncio.run(
            gateway.complete(
                __import__("trpc_service.llm_gateway", fromlist=["GatewayRequest"]).GatewayRequest(
                    tenant_id=tenant_id,
                    model_alias="balanced",
                    messages=[{"role": "user", "content": "confidential"}],
                    data_classification=DataClassification.CONFIDENTIAL,
                    region="cn-north-1",
                )
            )
        )
    assert provider_calls == 0


def test_opa_policy_requires_an_explicit_private_endpoint_for_confidential_content() -> None:
    tenant_id = str(uuid4())
    provider_calls = 0

    async def opa(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content)["input"]["endpoint_url"] == "https://provider.test/v1"
        return httpx.Response(200, json={"result": {"allow": True}})

    async def provider(_request: httpx.Request) -> httpx.Response:
        nonlocal provider_calls
        provider_calls += 1
        return httpx.Response(200, json={"choices": []})

    profile = ModelProfile(
        tenant_id=tenant_id,
        alias="balanced",
        provider_model="fake-balanced",
        endpoint_url="https://provider.test/v1",
        secret_ref=f"vault://tenant/{tenant_id}/llm/openai#api_key",
        data_classification=DataClassification.CONFIDENTIAL,
        region="cn-north-1",
        fallback_aliases=(),
        requests_per_minute=60,
    )
    gateway = LLMGateway(
        InMemoryModelProfileResolver([profile]),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(provider)),
        policy=OpaOutboundPolicy(
            httpx.AsyncClient(
                base_url="https://opa.test",
                transport=httpx.MockTransport(opa),
            )
        ),
    )

    with pytest.raises(ModelGatewayError, match="MODEL_POLICY_DENIED"):
        asyncio.run(
            gateway.complete(
                GatewayRequest(
                    tenant_id=tenant_id,
                    model_alias="balanced",
                    messages=[{"role": "user", "content": "confidential"}],
                    data_classification=DataClassification.CONFIDENTIAL,
                    region="cn-north-1",
                )
            )
        )
    assert provider_calls == 0


def test_agent_worker_uses_release_route_and_rejects_unreleased_fallbacks() -> None:
    tenant_id = str(uuid4())
    release_id = str(uuid4())
    primary_calls = 0

    async def provider(request: httpx.Request) -> httpx.Response:
        nonlocal primary_calls
        primary_calls += 1
        if request.headers["X-Model-Alias"] == "balanced":
            return httpx.Response(503)
        return httpx.Response(200, json={"choices": []})

    profiles = [
        ModelProfile(
            tenant_id,
            "balanced",
            "fake-balanced",
            "https://provider.test/v1",
            f"vault://tenant/{tenant_id}/llm/balanced#api_key",
            DataClassification.CONFIDENTIAL,
            "cn-north-1",
            ("economy",),
            60,
        ),
        ModelProfile(
            tenant_id,
            "economy",
            "fake-economy",
            "https://provider.test/v1",
            f"vault://tenant/{tenant_id}/llm/economy#api_key",
            DataClassification.CONFIDENTIAL,
            "cn-north-1",
            (),
            60,
        ),
    ]
    gateway = LLMGateway(
        InMemoryModelProfileResolver(profiles),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(provider)),
        policy=AllowOutboundPolicy(),
    )
    worker = AgentWorker(
        gateway,
        InMemoryReleaseRouteResolver(
            [
                ReleaseRoute(
                    release_id,
                    tenant_id,
                    "balanced",
                    DataClassification.INTERNAL,
                    "cn-north-1",
                    frozenset(),
                )
            ]
        ),
    )

    with pytest.raises(ModelGatewayError, match="MODEL_UNAVAILABLE"):
        asyncio.run(
            worker.complete(
                AgentExecutionRequest(tenant_id, release_id, [{"role": "user", "content": "hello"}])
            )
        )
    assert primary_calls == 1


def test_execution_started_before_a_rollback_keeps_its_resolved_release() -> None:
    tenant_id, application_id = str(uuid4()), str(uuid4())
    old_release, new_release = str(uuid4()), str(uuid4())
    deployments = MutableDeploymentResolver(old_release)
    gateway = BlockingGateway()
    worker = AgentWorker(
        gateway,
        InMemoryReleaseRouteResolver(
            [
                ReleaseRoute(
                    old_release,
                    tenant_id,
                    "balanced",
                    DataClassification.INTERNAL,
                    "cn-north-1",
                    frozenset(),
                ),
                ReleaseRoute(
                    new_release,
                    tenant_id,
                    "balanced",
                    DataClassification.INTERNAL,
                    "cn-north-1",
                    frozenset(),
                ),
            ]
        ),
        deployments=deployments,
    )

    async def execute_then_rollback() -> GatewayResult:
        task = asyncio.create_task(
            worker.complete_for_deployment(
                tenant_id,
                application_id,
                "PRODUCTION",
                "stable-session",
                [{"role": "user", "content": "hello"}],
            )
        )
        await deployments.resolved.wait()
        deployments.release_id = new_release
        gateway.release_completion.set()
        return await task

    result = asyncio.run(execute_then_rollback())

    assert result.model_alias == "balanced"
    assert [request.release_id for request in gateway.requests] == [old_release]


def test_database_route_resolvers_use_immutable_snapshots_during_control_plane_outage() -> None:
    tenant_id, application_id, release_id = str(uuid4()), str(uuid4()), str(uuid4())
    profile = {
        "tenant_id": tenant_id,
        "alias": "balanced",
        "provider_model": "fake-balanced",
        "endpoint_url": "https://fake-llm.test/v1",
        "secret_ref": f"vault://tenant/{tenant_id}/llm/balanced#api_key",
        "data_classification": "CONFIDENTIAL",
        "region": "cn-north-1",
        "fallback_aliases": [],
        "requests_per_minute": 60,
    }
    release_database = CachedRouteDatabase(
        {
            "id": release_id,
            "tenant_id": tenant_id,
            "model_alias": "balanced",
            "data_classification": "INTERNAL",
            "region": "cn-north-1",
            "fallback_aliases": [],
            "model_profiles": [profile],
        }
    )
    deployment_database = CachedRouteDatabase(
        {
            "release_id": release_id,
            "previous_release_id": None,
            "rollout_percentage": 100,
        }
    )
    releases = DatabaseReleaseRouteResolver(release_database)  # type: ignore[arg-type]
    deployments = DatabaseDeploymentRouteResolver(deployment_database)  # type: ignore[arg-type]

    async def resolve_before_and_during_outage() -> tuple[object, object, object, object]:
        first_release = await releases.resolve(tenant_id, release_id)
        first_deployment = await deployments.resolve(
            tenant_id, application_id, "PRODUCTION", "stable-session"
        )
        release_database.unavailable = deployment_database.unavailable = True
        cached_release = await releases.resolve(tenant_id, release_id)
        cached_deployment = await deployments.resolve(
            tenant_id, application_id, "PRODUCTION", "stable-session"
        )
        return first_release, first_deployment, cached_release, cached_deployment

    first_release, first_deployment, cached_release, cached_deployment = asyncio.run(
        resolve_before_and_during_outage()
    )

    assert cached_release == first_release
    assert cached_deployment == first_deployment == release_id


def test_release_source_can_distinguish_a_legacy_snapshot_from_a_draft_release() -> None:
    source = AgentReleaseSource(
        draft_version=None,
        actor="legacy-migration",
        kind="LEGACY",
    )

    assert source.draft_version is None
    assert source.kind == "LEGACY"
