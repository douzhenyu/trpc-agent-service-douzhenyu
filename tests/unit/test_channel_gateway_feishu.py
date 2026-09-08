from __future__ import annotations

import json

import httpx
import pytest
from pydantic import SecretStr

from trpc_service.channel_gateway import ChannelGatewaySettings, LLMGatewayReplyExecutor


def test_feishu_only_gateway_requires_its_own_runtime_inputs() -> None:
    settings = ChannelGatewaySettings(
        database_url="postgresql://unused",
        wecom_enabled=False,
        feishu_enabled=True,
        feishu_tenant_id="tenant-1",
        feishu_app_id="cli-feishu",
        feishu_app_secret=SecretStr("not-a-real-secret"),
        llm_gateway_url="http://llm-gateway",
    )

    settings.validate_runtime()

    with pytest.raises(RuntimeError, match="LLM_GATEWAY_URL"):
        ChannelGatewaySettings(
            database_url="postgresql://unused",
            wecom_enabled=False,
            feishu_enabled=True,
            feishu_tenant_id="tenant-1",
            feishu_app_id="cli-feishu",
            feishu_app_secret=SecretStr("not-a-real-secret"),
        ).validate_runtime()


@pytest.mark.asyncio
async def test_feishu_executor_sends_release_scoped_request_to_llm_gateway() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "model_alias": "deepseek-e2e",
                "fallback_used": False,
                "completion": {"choices": [{"message": {"content": "飞书回包"}}]},
            },
        )

    async with httpx.AsyncClient(
        base_url="http://llm-gateway", transport=httpx.MockTransport(handler)
    ) as client:
        result = await LLMGatewayReplyExecutor(client).complete(
            tenant_id="tenant-1",
            application_id="application-1",
            release_id="release-1",
            execution_id="execution-1",
            messages=[{"role": "user", "content": "hello"}],
        )

    assert result == "飞书回包"
    assert requests[0].url.path == "/internal/v1/llm-completions"
    assert json.loads(requests[0].content) == {
        "tenant_id": "tenant-1",
        "application_id": "application-1",
        "release_id": "release-1",
        "execution_id": "execution-1",
        "messages": [{"role": "user", "content": "hello"}],
    }
