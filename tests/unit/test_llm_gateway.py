from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from uuid import uuid4

import httpx
import pytest

from trpc_service.llm_gateway import (
    DataClassification,
    GatewayModel,
    GatewayRequest,
    InMemoryGatewayObserver,
    InMemoryModelProfileResolver,
    LLMGateway,
    ModelGatewayError,
    ModelProfile,
)


class FakeSecretProvider:
    async def resolve(self, secret_ref: str) -> str:
        assert secret_ref.startswith("vault://tenant/")
        return "provider-secret-that-must-not-leak"


def profile(
    tenant_id: str,
    alias: str,
    *,
    fallbacks: Sequence[str] = (),
    requests_per_minute: int = 60,
    data_classification: DataClassification = DataClassification.CONFIDENTIAL,
) -> ModelProfile:
    return ModelProfile(
        tenant_id=tenant_id,
        alias=alias,
        provider_model=f"fake-{alias}",
        endpoint_url="https://fake-llm.test/v1/chat/completions",
        secret_ref=f"vault://tenant/{tenant_id}/llm/{alias}#api_key",
        data_classification=data_classification,
        region="cn-north-1",
        fallback_aliases=tuple(fallbacks),
        requests_per_minute=requests_per_minute,
    )


def request(
    tenant_id: str,
    alias: str = "balanced",
    *,
    data_classification: DataClassification = DataClassification.INTERNAL,
    allowed_fallback_aliases: frozenset[str] = frozenset(),
) -> GatewayRequest:
    return GatewayRequest(
        tenant_id=tenant_id,
        model_alias=alias,
        messages=[{"role": "user", "content": "prompt that must not leak"}],
        data_classification=data_classification,
        region="cn-north-1",
        allowed_fallback_aliases=allowed_fallback_aliases,
    )


def test_gateway_injects_a_resolved_secret_and_returns_a_successful_fake_completion() -> None:
    tenant_id = str(uuid4())

    async def fake_llm(http_request: httpx.Request) -> httpx.Response:
        assert http_request.headers["authorization"] == "Bearer provider-secret-that-must-not-leak"
        assert json.loads(http_request.content) == {
            "model": "fake-balanced",
            "messages": [{"role": "user", "content": "prompt that must not leak"}],
        }
        return httpx.Response(
            200,
            json={"model": "fake-balanced", "choices": [{"message": {"content": "fake reply"}}]},
        )

    gateway = LLMGateway(
        InMemoryModelProfileResolver([profile(tenant_id, "balanced")]),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(fake_llm)),
    )

    result = asyncio.run(gateway.complete(request(tenant_id)))

    assert result.model_alias == "balanced"
    assert result.fallback_used is False
    assert result.completion == {
        "model": "fake-balanced",
        "choices": [{"message": {"content": "fake reply"}}],
    }
    assert "provider-secret-that-must-not-leak" not in str(result)
    assert "prompt that must not leak" not in str(result)


def test_gateway_uses_fallback_and_opens_the_primary_circuit_without_leaking_prompt_or_secret(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tenant_id = str(uuid4())
    calls: list[str] = []

    async def fake_llm(http_request: httpx.Request) -> httpx.Response:
        calls.append(str(http_request.url))
        if http_request.headers["x-model-alias"] == "balanced":
            return httpx.Response(503, json={"error": "provider-secret-that-must-not-leak"})
        return httpx.Response(200, json={"model": "fake-economy", "choices": []})

    gateway = LLMGateway(
        InMemoryModelProfileResolver(
            [profile(tenant_id, "balanced", fallbacks=["economy"]), profile(tenant_id, "economy")]
        ),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(fake_llm)),
        circuit_failure_threshold=1,
    )

    first = asyncio.run(
        gateway.complete(request(tenant_id, allowed_fallback_aliases=frozenset({"economy"})))
    )
    second = asyncio.run(
        gateway.complete(request(tenant_id, allowed_fallback_aliases=frozenset({"economy"})))
    )

    assert first.model_alias == second.model_alias == "economy"
    assert first.fallback_used is second.fallback_used is True
    assert len(calls) == 3
    assert "provider-secret-that-must-not-leak" not in caplog.text
    assert "prompt that must not leak" not in caplog.text


def test_gateway_rate_limits_the_profile_before_calling_the_fake_llm_again() -> None:
    tenant_id = str(uuid4())
    calls = 0

    async def fake_llm(_http_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"choices": []})

    gateway = LLMGateway(
        InMemoryModelProfileResolver([profile(tenant_id, "balanced", requests_per_minute=1)]),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(fake_llm)),
    )

    asyncio.run(gateway.complete(request(tenant_id)))
    with pytest.raises(ModelGatewayError, match="RATE_LIMITED"):
        asyncio.run(gateway.complete(request(tenant_id)))

    assert calls == 1


def test_gateway_rejects_restricted_content_for_an_external_endpoint() -> None:
    tenant_id = str(uuid4())
    calls = 0

    async def fake_llm(_http_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"choices": []})

    gateway = LLMGateway(
        InMemoryModelProfileResolver(
            [
                profile(
                    tenant_id,
                    "restricted",
                    data_classification=DataClassification.RESTRICTED,
                )
            ]
        ),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(fake_llm)),
    )

    with pytest.raises(ModelGatewayError, match="MODEL_POLICY_DENIED"):
        asyncio.run(
            gateway.complete(
                request(
                    tenant_id,
                    "restricted",
                    data_classification=DataClassification.RESTRICTED,
                )
            )
        )

    assert calls == 0


def test_gateway_does_not_fallback_without_runtime_authorization() -> None:
    tenant_id = str(uuid4())

    async def fake_llm(_http_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    gateway = LLMGateway(
        InMemoryModelProfileResolver(
            [profile(tenant_id, "balanced", fallbacks=["economy"]), profile(tenant_id, "economy")]
        ),
        FakeSecretProvider(),
        httpx.AsyncClient(transport=httpx.MockTransport(fake_llm)),
    )

    with pytest.raises(ModelGatewayError, match="MODEL_UNAVAILABLE"):
        asyncio.run(gateway.complete(request(tenant_id)))


def test_gateway_sanitizes_secret_resolution_failures_and_records_safe_metadata() -> None:
    tenant_id = str(uuid4())
    observer = InMemoryGatewayObserver()

    class FailingSecretProvider:
        async def resolve(self, _secret_ref: str) -> str:
            raise RuntimeError("provider-secret-that-must-not-leak prompt that must not leak")

    gateway = LLMGateway(
        InMemoryModelProfileResolver([profile(tenant_id, "balanced")]),
        FailingSecretProvider(),
        httpx.AsyncClient(),
        observer=observer,
    )

    with pytest.raises(ModelGatewayError, match="SECRET_RESOLUTION_FAILED") as error:
        asyncio.run(gateway.complete(request(tenant_id)))

    assert "provider-secret-that-must-not-leak" not in str(error.value)
    assert "prompt that must not leak" not in str(error.value)
    assert len(observer.events) == 1
    assert observer.events[0].model_alias == "balanced"
    assert observer.events[0].error_code == "SECRET_RESOLUTION_FAILED"
    assert "provider-secret-that-must-not-leak" not in str(observer.events[0])
    assert "prompt that must not leak" not in str(observer.events[0])


def test_gateway_model_adapter_does_not_accept_provider_credentials() -> None:
    tenant_id = str(uuid4())

    async def fake_llm(_http_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": "reply"}}]})

    model = GatewayModel(
        gateway=LLMGateway(
            InMemoryModelProfileResolver([profile(tenant_id, "balanced")]),
            FakeSecretProvider(),
            httpx.AsyncClient(transport=httpx.MockTransport(fake_llm)),
        ),
        tenant_id=tenant_id,
        model_alias="balanced",
        data_classification=DataClassification.INTERNAL,
        region="cn-north-1",
    )

    result = asyncio.run(model.complete([{"role": "user", "content": "safe prompt"}]))

    assert result.completion["choices"][0]["message"]["content"] == "reply"
