"""Tool/MCP registration API, governed invocation persistence and tenant isolation."""

from __future__ import annotations

import asyncio
import os
import uuid as uuid_module
from uuid import uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.database_migrations import apply_migrations
from trpc_service.governance import DataClassification
from trpc_service.tool.executor import (
    ToolBackendResult,
    ToolInvocationService,
    ToolInvocationStatus,
)
from trpc_service.tool.registry import ToolDefinition, ToolSideEffect
from trpc_service.tool.store import DatabaseToolCallStore, DatabaseToolRegistry

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)
PASSWORD_HASH = (
    "$argon2id$v=19$m=65536,t=3,p=4$MRV7DB8RCvU73jcYXzxkUA$"
    "z7yjdKaXuCwuYoWzAqb25/+4f8tW5j3cxFm/pComAo4"
)

pytestmark = pytest.mark.integration


def _definition(tenant_id: str, name: str = "crm_lookup", version: int = 1) -> ToolDefinition:
    return ToolDefinition(
        tenant_id=tenant_id,
        name=name,
        version=version,
        description="Look up a CRM account.",
        side_effect=ToolSideEffect.READ_ONLY,
        input_schema={
            "type": "object",
            "properties": {"account_id": {"type": "string"}},
            "required": ["account_id"],
        },
        output_schema={"type": "object"},
        scopes=("crm:read",),
        timeout_seconds=5,
        cost_per_call_micros=120,
        data_classification=DataClassification.INTERNAL,
    )


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.tool_call, tenant.tool_definition, tenant.policy_bundle, "
            "tenant.cost_ledger, tenant.budget_alert, tenant.budget_period_state, tenant.budget, "
            "tenant.model_price, tenant.session_event, tenant.session_lease, "
            "tenant.agent_execution, tenant.agent_session, platform.outbox_record, "
            "tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _seed_tenant() -> str:
    tenant_id = uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            "Tool Governance Tenant",
        )
    finally:
        await connection.close()
    return str(tenant_id)


def _settings() -> AdminSettings:
    return AdminSettings(
        database_url=APP_URL,
        session_signing_key="test-session-key-that-is-long-enough-for-hs256",
        emergency_admin_username="break-glass",
        emergency_admin_password_hash=PASSWORD_HASH,
        session_cookie_secure=False,
        oidc_enabled=False,
    )


def _login(client: TestClient) -> None:
    login = client.post(
        "/api/v1/auth/emergency/session",
        json={"username": "break-glass", "password": "correct-horse"},
    )
    assert login.status_code == 200


def test_tool_registration_api_round_trip() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        registered = client.put(
            f"/api/v1/tenants/{tenant_id}/tools",
            json={
                "name": "crm_lookup",
                "version": 1,
                "description": "Look up a CRM account.",
                "side_effect": "READ_ONLY",
                "input_schema": {
                    "type": "object",
                    "properties": {"account_id": {"type": "string"}},
                    "required": ["account_id"],
                },
                "output_schema": {"type": "object"},
                "scopes": ["crm:read"],
                "timeout_seconds": 5,
                "cost_per_call_micros": 120,
                "data_classification": "INTERNAL",
            },
        )
        assert registered.status_code == 200, registered.text
        assert registered.json()["source"] == "DECLARED"

        mcp = client.put(
            f"/api/v1/tenants/{tenant_id}/mcp-servers",
            json={
                "server_name": "jira-mcp",
                "server_version": 2,
                "tools": [
                    {
                        "name": "jira_search",
                        "version": 1,
                        "description": "Search Jira issues.",
                        "side_effect": "READ_ONLY",
                        "input_schema": {"type": "object"},
                        "output_schema": {"type": "object"},
                        "data_classification": "INTERNAL",
                    },
                    {
                        "name": "jira_comment",
                        "version": 1,
                        "description": "Comment on a Jira issue.",
                        "side_effect": "IDEMPOTENT_WRITE",
                        "input_schema": {"type": "object"},
                        "output_schema": {"type": "object"},
                        "data_classification": "INTERNAL",
                        "supports_idempotency": True,
                    },
                ],
            },
        )
        assert mcp.status_code == 200, mcp.text
        mcp_tools = {tool["name"]: tool for tool in mcp.json()["tools"]}
        assert mcp_tools["jira_search"]["source"] == "MCP"
        assert mcp_tools["jira_search"]["mcp_server"] == "jira-mcp"

        listing = client.get(f"/api/v1/tenants/{tenant_id}/tools")
        assert listing.status_code == 200
        names = {tool["name"] for tool in listing.json()["tools"]}
        assert names == {"crm_lookup", "jira_search", "jira_comment"}

        duplicate_names = client.put(
            f"/api/v1/tenants/{tenant_id}/mcp-servers",
            json={
                "server_name": "bad-mcp",
                "server_version": 1,
                "tools": [
                    {
                        "name": "same_name",
                        "version": 1,
                        "description": "dup",
                        "side_effect": "READ_ONLY",
                        "input_schema": {"type": "object"},
                        "output_schema": {"type": "object"},
                        "data_classification": "INTERNAL",
                    }
                ]
                * 2,
            },
        )
        assert duplicate_names.status_code == 422


def test_database_registry_store_round_trip_and_replay() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        tenant_id = await _seed_tenant()
        database = Database(APP_URL)
        await database.open()
        try:
            registry = DatabaseToolRegistry(database)
            await registry.register(_definition(tenant_id), created_by="tester")
            resolved = await registry.resolve(tenant_id, "crm_lookup")
            assert resolved is not None and resolved.version == 1
            assert await registry.resolve(tenant_id, "missing") is None
            assert (
                await registry.resolve("00000000-0000-0000-0000-000000000000", "crm_lookup") is None
            )

            class Backend:
                def __init__(self) -> None:
                    self.calls = 0

                async def execute(self, definition, params):
                    self.calls += 1
                    return ToolBackendResult(
                        ok=True, result={"name": "Acme"}, error_code=None, transient=False
                    )

            backend = Backend()
            store = DatabaseToolCallStore(database)
            service = ToolInvocationService(registry, backend, store)
            first = await service.invoke(
                tenant_id=tenant_id,
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset({"crm:read"}),
                idempotency_key="replay-key-1",
                execution_id="exec-1",
                requested_by="subject-1",
            )
            assert first.status == ToolInvocationStatus.SUCCEEDED

            # A fresh service (new process) replays the durable record.
            replay_service = ToolInvocationService(
                registry, backend, DatabaseToolCallStore(database)
            )
            replay = await replay_service.invoke(
                tenant_id=tenant_id,
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset({"crm:read"}),
                idempotency_key="replay-key-1",
                requested_by="subject-1",
            )
            assert replay.replayed is True
            assert replay.record.result == {"name": "Acme"}
            assert backend.calls == 1

            async with database.tenant_transaction(uuid_module.UUID(tenant_id)) as connection:
                row = await connection.fetchrow(
                    "SELECT * FROM tenant.tool_call WHERE idempotency_key=$1", "replay-key-1"
                )
            assert row is not None
            assert row["status"] == "SUCCEEDED"
            assert row["attempts"] == 1
            assert row["cost_micros"] == 120
        finally:
            await database.close()

    asyncio.run(scenario())


def test_tool_call_rows_enforce_tenant_isolation() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    other_tenant = asyncio.run(_seed_tenant())

    async def scenario() -> None:
        connection = await asyncpg.connect(APP_URL)
        try:
            async with connection.transaction():
                await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
                await connection.execute(
                    """INSERT INTO tenant.tool_call
                        (tenant_id,call_id,tool_name,tool_version,params,params_hash,status,
                         attempts,cost_micros,requested_by)
                        VALUES ($1,$2,'crm_lookup',1,'{}',
                         'a' || repeat('b', 63),'SUCCEEDED',1,0,'subject')""",
                    uuid_module.UUID(tenant_id),
                    uuid_module.uuid4(),
                )
        finally:
            await connection.close()

        other = await asyncpg.connect(APP_URL)
        try:
            async with other.transaction():
                await other.execute("SELECT set_config('app.tenant_id', $1, true)", other_tenant)
                visible = await other.fetchval("SELECT count(*) FROM tenant.tool_call")
                assert visible == 0
        finally:
            await other.close()

    asyncio.run(scenario())


def test_database_registry_supports_mcp_versions_and_pins() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        tenant_id = await _seed_tenant()
        database = Database(APP_URL)
        await database.open()
        try:
            registry = DatabaseToolRegistry(database)
            registered = await registry.register_mcp_server(
                tenant_id=tenant_id,
                server_name="jira-mcp",
                server_version=3,
                tools=[_definition(tenant_id, name="jira_search", version=1)],
                created_by="tester",
            )
            assert registered == 1
            resolved = await registry.resolve(tenant_id, "jira_search")
            assert resolved is not None
            assert resolved.version == 3
            assert resolved.mcp_server == "jira-mcp"
            pinned = await registry.resolve(tenant_id, "jira_search", version=1)
            assert pinned is None
            with pytest.raises(AttributeError):
                registry.has_name  # noqa: B018 - DB registry must not fake cross-tenant knowledge
        finally:
            await database.close()

    asyncio.run(scenario())


def test_conflicting_reregistration_is_rejected_and_identical_is_idempotent() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        payload = {
            "name": "crm_lookup",
            "version": 1,
            "description": "Look up a CRM account.",
            "side_effect": "READ_ONLY",
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
            "data_classification": "INTERNAL",
        }
        first = client.put(f"/api/v1/tenants/{tenant_id}/tools", json=payload)
        assert first.status_code == 200, first.text

        conflicting = client.put(
            f"/api/v1/tenants/{tenant_id}/tools",
            json={**payload, "cost_per_call_micros": 999},
        )
        assert conflicting.status_code == 409, conflicting.text

        identical = client.put(f"/api/v1/tenants/{tenant_id}/tools", json=payload)
        assert identical.status_code == 200
        assert identical.json()["cost_per_call_micros"] == 0


def test_retry_after_failed_outcome_reuses_the_same_idempotency_key() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        tenant_id = await _seed_tenant()
        database = Database(APP_URL)
        await database.open()
        try:
            registry = DatabaseToolRegistry(database)
            await registry.register(
                _definition(tenant_id).model_copy(
                    update={
                        "side_effect": ToolSideEffect.NON_IDEMPOTENT_WRITE,
                        "scopes": (),
                    }
                ),
                created_by="tester",
            )

            class FlakyBackend:
                def __init__(self) -> None:
                    self.calls = 0

                async def execute(self, definition, params):
                    self.calls += 1
                    if self.calls == 1:
                        return ToolBackendResult(
                            ok=False, result=None, error_code="UPSTREAM_500", transient=True
                        )
                    return ToolBackendResult(
                        ok=True, result={"done": True}, error_code=None, transient=False
                    )

            backend = FlakyBackend()
            service = ToolInvocationService(registry, backend, DatabaseToolCallStore(database))
            failed = await service.invoke(
                tenant_id=tenant_id,
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset(),
                idempotency_key="retry-key",
                mode="direct",
                requested_by="subject-1",
            )
            assert failed.status == ToolInvocationStatus.OUTCOME_UNKNOWN

            retry_service = ToolInvocationService(
                registry, backend, DatabaseToolCallStore(database)
            )
            retried = await retry_service.invoke(
                tenant_id=tenant_id,
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset(),
                idempotency_key="retry-key",
                mode="direct",
                requested_by="subject-1",
            )
            assert retried.status == ToolInvocationStatus.SUCCEEDED
            assert backend.calls == 2

            replay = await retry_service.invoke(
                tenant_id=tenant_id,
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset(),
                idempotency_key="retry-key",
                mode="direct",
                requested_by="subject-1",
            )
            assert replay.replayed is True
            assert backend.calls == 2
        finally:
            await database.close()

    asyncio.run(scenario())
