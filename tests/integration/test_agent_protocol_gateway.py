from __future__ import annotations

import asyncio
import json
import os
import threading
from http.server import ThreadingHTTPServer
from uuid import uuid4

import asyncpg
import httpx
import pytest
from ag_ui.core import EventType

from dev.fake_external.server import FakeExternalHandler
from trpc_service.agent_gateway import AgentGatewaySettings, create_app
from trpc_service.database_migrations import apply_migrations
from trpc_service.execution_bus import InMemoryExecutionBus
from trpc_service.version import PINNED_TRPC_AGENT_VERSION

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)
BASE_URL = "http://gateway.test"

pytestmark = pytest.mark.integration


def _start_fake_llm() -> tuple[ThreadingHTTPServer, int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeExternalHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.session_event, tenant.session_lease, tenant.agent_execution, "
            "tenant.agent_session, platform.outbox_record, "
            "tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _seed_release_stack(llm_endpoint: str) -> tuple[str, str, str]:
    tenant_id, application_id, release_id, deployment_id = uuid4(), uuid4(), uuid4(), uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            "Agent Protocol Tenant",
        )
        await connection.execute(
            "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) VALUES ($1,$2,$3,$4)",
            tenant_id,
            application_id,
            f"app-{application_id.hex[:8]}",
            "Agent Protocol App",
        )
        model_profiles = [
            {
                "tenant_id": str(tenant_id),
                "alias": "primary-alias",
                "provider_model": "gpt-test",
                "endpoint_url": llm_endpoint,
                "secret_ref": f"vault://tenant/{tenant_id}/llm#primary",
                "data_classification": "CONFIDENTIAL",
                "region": "cn-test",
                "fallback_aliases": [],
                "requests_per_minute": 60,
            }
        ]
        await connection.execute(
            """INSERT INTO tenant.agent_release
            (tenant_id,id,application_id,model_alias,data_classification,region,
            fallback_aliases,model_profiles,release_version,draft_snapshot)
            VALUES ($1,$2,$3,'primary-alias','CONFIDENTIAL','cn-test','[]'::jsonb,$4::jsonb,1,
            $5::jsonb)""",
            tenant_id,
            release_id,
            application_id,
            json.dumps(model_profiles),
            json.dumps({"instructions": "Answer in one short sentence."}),
        )
        await connection.execute(
            """INSERT INTO tenant.agent_deployment
            (tenant_id,id,application_id,environment,release_id,rollout_percentage,status,
            initiator,version,activated_at)
            VALUES ($1,$2,$3,'PRODUCTION',$4,100,'ACTIVE','seed',1,now())""",
            tenant_id,
            deployment_id,
            application_id,
            release_id,
        )
    finally:
        await connection.close()
    return str(tenant_id), str(application_id), str(release_id)


def _sse_payloads(raw: str) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for line in raw.splitlines():
        if line.startswith("data:"):
            payloads.append(json.loads(line[len("data:") :].strip()))
    return payloads


def _runner_request(tenant_id: str, application_id: str, session_id: str) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "application_id": application_id,
        "environment": "PRODUCTION",
        "session_id": session_id,
        "user_id": "platform-user",
        "message": "Say hi in one short sentence.",
    }


async def _scenario() -> None:
    server, port = _start_fake_llm()
    try:
        tenant_id, application_id, release_id = await _seed_release_stack(
            f"http://127.0.0.1:{port}/llm/v1/chat/completions"
        )
        app = create_app(
            AgentGatewaySettings(
                database_url=APP_URL,
                dispatch_interval_seconds=0.0,
                llm_gateway_access_key="fake-key",
                public_base_url=BASE_URL,
            ),
            bus=InMemoryExecutionBus(partition_count=2),
        )
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
            async with httpx.AsyncClient(
                transport=transport, base_url=BASE_URL, timeout=30.0
            ) as client:
                # AG-UI and A2A run first: direct protocol clients must work
                # without any HTTP/SSE warm-up of the release surfaces.
                await _execute_ag_ui(client, tenant_id, release_id)
                await _execute_a2a(client, tenant_id, release_id)
                await _execute_http_and_sse(client, tenant_id, application_id, release_id)
    finally:
        server.shutdown()
        server.server_close()


async def _execute_http_and_sse(
    client: httpx.AsyncClient, tenant_id: str, application_id: str, release_id: str
) -> None:
    completed = await client.post(
        "/internal/v1/agent-runner/completions",
        json=_runner_request(tenant_id, application_id, "protocol-http-1"),
    )
    assert completed.status_code == 200, completed.text
    reply = completed.json()
    assert reply["content"] == "fake reply"
    assert reply["session_id"] == "protocol-http-1"
    assert reply["release_id"] == release_id
    assert reply["tenant_id"] == tenant_id
    assert reply["model_alias"] == "primary-alias"
    assert reply["trace_id"].startswith("e-")
    assert reply["sdk_version"] == PINNED_TRPC_AGENT_VERSION
    assert reply["platform_version"]

    status = await client.get(f"/internal/v1/agent-runner/statuses/{reply['execution_id']}")
    assert status.status_code == 200
    assert status.json()["status"] == "SUCCEEDED"
    assert status.json()["content"] == "fake reply"
    missing = await client.get(f"/internal/v1/agent-runner/statuses/{uuid4()}")
    assert missing.status_code == 404

    streamed = await client.post(
        "/internal/v1/agent-runner/stream",
        json=_runner_request(tenant_id, application_id, "protocol-sse-1"),
    )
    assert streamed.status_code == 200
    assert streamed.headers["content-type"].startswith("text/event-stream")
    payloads = _sse_payloads(streamed.text)
    deltas = [payload for payload in payloads if payload["type"] == "delta"]
    results = [payload for payload in payloads if payload["type"] == "result"]
    assert deltas, payloads
    assert len(results) == 1
    result = results[0]
    assert result["content"] == "fake reply"
    assert result["release_id"] == release_id
    assert result["trace_id"].startswith("e-")
    assert result["sdk_version"] == PINNED_TRPC_AGENT_VERSION


async def _execute_ag_ui(client: httpx.AsyncClient, tenant_id: str, release_id: str) -> None:
    from ag_ui.core import RunFinishedEvent, TextMessageContentEvent
    from ag_ui.core.types import RunAgentInput

    response = await client.post(
        f"/internal/v1/agent-runner/ag-ui/{tenant_id}/{release_id}",
        json=RunAgentInput(
            thread_id="agui-thread-1",
            run_id="agui-run-1",
            state={},
            messages=[{"id": "m-1", "role": "user", "content": "Say hi in one short sentence."}],
            tools=[],
            context=[],
            forwarded_props={},
        ).model_dump(),
    )
    assert response.status_code == 200, response.text
    events = _sse_payloads(response.text)
    types = [event.get("type") for event in events]
    assert EventType.RUN_STARTED.value in types
    assert EventType.TEXT_MESSAGE_CONTENT.value in types
    assert EventType.RUN_FINISHED.value in types
    content = "".join(
        str(event.get("delta", ""))
        for event in events
        if event.get("type") == EventType.TEXT_MESSAGE_CONTENT.value
    )
    assert content == "fake reply"
    for event in events:
        if event.get("type") == EventType.TEXT_MESSAGE_CONTENT.value:
            TextMessageContentEvent.model_validate(event)
        if event.get("type") == EventType.RUN_FINISHED.value:
            RunFinishedEvent.model_validate(event)


async def _execute_a2a(client: httpx.AsyncClient, tenant_id: str, release_id: str) -> None:
    from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
    from a2a.client.helpers import create_text_message_object

    resolver = A2ACardResolver(
        httpx_client=client,
        base_url=f"{BASE_URL}/internal/v1/agent-runner/a2a/{tenant_id}/{release_id}",
    )
    card = await resolver.get_agent_card()
    assert card.capabilities.streaming is True
    factory = ClientFactory(ClientConfig(httpx_client=client, supported_transports=["JSONRPC"]))
    a2a_client = factory.create(card)
    replies: list[str] = []
    async for response in a2a_client.send_message(
        create_text_message_object(content="Say hi in one short sentence.")
    ):
        reply_json = (
            response[0].model_dump_json()
            if isinstance(response, tuple)
            else response.model_dump_json()
        )
        replies.append(reply_json)
    assert replies, "A2A upstream client received no response"
    assert any("fake reply" in reply for reply in replies)


def test_agent_gateway_exposes_runner_protocols() -> None:
    asyncio.run(_scenario())
