from __future__ import annotations

import asyncio
from uuid import uuid4

import httpx
import pytest

from trpc_service.agent_worker import (
    AgentExecutionRequest,
    AgentWorker,
    InMemoryReleaseRouteResolver,
    ReleaseRoute,
)
from trpc_service.llm_gateway import (
    DataClassification,
    InMemoryModelProfileResolver,
    LLMGateway,
    ModelGatewayError,
    ModelProfile,
    OpaOutboundPolicy,
    VaultSecretProvider,
)


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

    secret = asyncio.run(provider.resolve(f"vault://tenant/{tenant_id}/llm/openai#api_key"))

    assert secret == "provider-secret"
    assert calls == ["/v1/auth/kubernetes/login", f"/v1/tenant/data/{tenant_id}/llm/openai"]


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
        type("Secret", (), {"resolve": lambda _self, _ref: asyncio.sleep(0, result="secret")})(),
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
        type("Secret", (), {"resolve": lambda _self, _ref: asyncio.sleep(0, result="secret")})(),
        httpx.AsyncClient(transport=httpx.MockTransport(provider)),
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
