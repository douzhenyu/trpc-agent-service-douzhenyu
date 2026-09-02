from importlib.metadata import version

import httpx
import pytest

from trpc_service.admin_api.app import app


@pytest.mark.asyncio
async def test_public_health_reports_service_and_pinned_sdk_version() -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://admin-api",
    ) as client:
        response = await client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "admin-api",
        "version": "0.1.0",
        "trpc_agent_version": "1.1.19",
    }
    assert version("trpc-agent-py") == "1.1.19"
