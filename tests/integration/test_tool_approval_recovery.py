"""Integration tests: approval persistence, checkpoint recovery and reconciliation."""

from __future__ import annotations

import asyncio
import os
import uuid as uuid_module

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.database_migrations import apply_migrations
from trpc_service.sessions import SessionLeaseManager
from trpc_service.tool.approvals import ApprovalService, ApprovalStatus
from trpc_service.tool.checkpoints import CheckpointService
from trpc_service.tool.store import (
    DatabaseApprovalStore,
    DatabaseCheckpointStore,
)

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


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.tool_call_reconciliation, tenant.execution_checkpoint, "
            "tenant.tool_approval, tenant.tool_call, tenant.tool_definition, "
            "tenant.policy_bundle, tenant.cost_ledger, tenant.budget_alert, "
            "tenant.budget_period_state, tenant.budget, tenant.model_price, "
            "tenant.session_event, tenant.session_lease, tenant.agent_execution, "
            "tenant.agent_session, platform.outbox_record, tenant.model_profile, "
            "tenant.agent_release, tenant.agent_draft, tenant.agent_application, "
            "platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _seed_tenant() -> str:
    tenant_id = uuid_module.uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            "Approval Recovery Tenant",
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


async def _seed_unknown_call(tenant_id: str) -> str:
    call_id = uuid_module.uuid4()
    connection = await asyncpg.connect(APP_URL)
    try:
        async with connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                """INSERT INTO tenant.tool_call
                    (tenant_id,call_id,execution_id,session_id,release_id,tool_name,
                     tool_version,side_effect,params,params_hash,idempotency_key,status,
                     error_code,attempts,cost_micros,requested_by)
                    VALUES ($1,$2,'exec-1','sess-1','rel-1','charge_card',1,
                     'NON_IDEMPOTENT_WRITE','{}','a' || repeat('b', 63),null,
                     'OUTCOME_UNKNOWN','TOOL_TIMEOUT',1,0,'subject-1')""",
                uuid_module.UUID(tenant_id),
                call_id,
            )
    finally:
        await connection.close()
    return str(call_id)


def test_approval_checkpoint_reconciliation_lifecycle() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)

        async def scenario() -> None:
            database = Database(APP_URL)
            await database.open()
            try:
                approvals = ApprovalService(DatabaseApprovalStore(database))
                request = await approvals.create(
                    tenant_id=tenant_id,
                    release_id="release-1",
                    tool_name="wipe_data",
                    tool_version=2,
                    params_hash="a" * 64,
                    params={"scope": "all"},
                    side_effect="HIGH_RISK",
                    requested_by="subject-1",
                    requester_role="AGENT_RUNNER",
                    policy_version="policy-v7",
                )
                stored = await approvals.get(tenant_id, request.approval_id)
                assert stored.status == ApprovalStatus.PENDING
                assert stored.policy_version == "policy-v7"

                # Seed the application/release/session chain the lease FK needs.
                application_id = uuid_module.uuid4()
                async with database.tenant_transaction(uuid_module.UUID(tenant_id)) as connection:
                    await connection.execute(
                        "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) "
                        "VALUES ($1,$2,$3,$4)",
                        uuid_module.UUID(tenant_id),
                        application_id,
                        f"app-{application_id.hex[:8]}",
                        "Approval Recovery App",
                    )
                    await connection.execute(
                        """INSERT INTO tenant.agent_release
                        (tenant_id,id,application_id,model_alias,data_classification,region,
                        fallback_aliases,model_profiles,release_version)
                        VALUES ($1,$2,$3,'primary-alias','CONFIDENTIAL','cn-test',
                        '[]'::jsonb,'[]'::jsonb,1)""",
                        uuid_module.UUID(tenant_id),
                        uuid_module.uuid4(),
                        application_id,
                    )
                    await connection.execute(
                        "INSERT INTO tenant.agent_session (tenant_id,application_id,id) "
                        "VALUES ($1,$2,'session-1') ON CONFLICT DO NOTHING",
                        uuid_module.UUID(tenant_id),
                        application_id,
                    )

                # Park a waiting execution with a real session lease held.
                checkpoints = CheckpointService(DatabaseCheckpointStore(database))
                leases = SessionLeaseManager(database)
                await leases.acquire(uuid_module.UUID(tenant_id), "session-1", "worker-1")
                checkpoint = await checkpoints.park(
                    tenant_id=tenant_id,
                    execution_id="exec-1",
                    session_id="session-1",
                    release_id="release-1",
                    approval_id=request.approval_id,
                    tool_name="wipe_data",
                    tool_version=2,
                    params_hash="a" * 64,
                    requested_by="subject-1",
                    parked_by="worker-1",
                    lease_manager=leases,
                )
                # The worker's lease was released on park: another owner wins.
                grant = await leases.acquire(uuid_module.UUID(tenant_id), "session-1", "worker-2")
                assert grant.fencing_token == 2

                # Resume before approval is rejected.
                try:
                    await checkpoints.resume(
                        tenant_id,
                        checkpoint.checkpoint_id,
                        approvals=approvals,
                        lease_manager=leases,
                        resumed_by="worker-2",
                    )
                    raise AssertionError("resume must fail before approval")
                except Exception as error:
                    assert getattr(error, "code", "") == "CHECKPOINT_APPROVAL_NOT_GRANTED"

                # Decide through the Admin API with the emergency principal.
                decided = client.post(
                    f"/api/v1/tenants/{tenant_id}/tool-approvals/{request.approval_id}/decisions",
                    json={"decision": "APPROVE"},
                )
                assert decided.status_code == 200, decided.text
                assert decided.json()["status"] == "APPROVED"
                assert decided.json()["decided_by"] != "subject-1"

                resumed = await checkpoints.resume(
                    tenant_id,
                    checkpoint.checkpoint_id,
                    approvals=approvals,
                    lease_manager=leases,
                    resumed_by="worker-2",
                )
                assert resumed.fencing_token == 3
                consumed = await approvals.get(tenant_id, request.approval_id)
                assert consumed.status == ApprovalStatus.CONSUMED

                listing = client.get(f"/api/v1/tenants/{tenant_id}/tool-approvals?status=CONSUMED")
                assert listing.status_code == 200
                assert listing.json()["approvals"][0]["approval_id"] == request.approval_id
            finally:
                await database.close()

        asyncio.run(scenario())


def test_reconciliation_endpoints_close_unknown_outcomes() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    call_id = asyncio.run(_seed_unknown_call(tenant_id))
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        open_calls = client.get(f"/api/v1/tenants/{tenant_id}/tool-call-reconciliation")
        assert open_calls.status_code == 200
        assert [row["call_id"] for row in open_calls.json()] == [call_id]

        resolved = client.post(
            f"/api/v1/tenants/{tenant_id}/tool-call-reconciliation/{call_id}/resolutions",
            json={"decision": "CONFIRMED_EXECUTED", "note": "ledger matched"},
        )
        assert resolved.status_code == 200, resolved.text
        assert resolved.json()["decision"] == "CONFIRMED_EXECUTED"

        assert client.get(f"/api/v1/tenants/{tenant_id}/tool-call-reconciliation").json() == []

        again = client.post(
            f"/api/v1/tenants/{tenant_id}/tool-call-reconciliation/{call_id}/resolutions",
            json={"decision": "CONFIRMED_NOT_EXECUTED"},
        )
        assert again.status_code == 409
        assert again.json()["error"]["code"] == "RECONCILIATION_ALREADY_RESOLVED"


def test_approval_rows_enforce_tenant_isolation() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    other_tenant = asyncio.run(_seed_tenant())

    async def scenario() -> None:
        connection = await asyncpg.connect(APP_URL)
        try:
            async with connection.transaction():
                await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
                await connection.execute(
                    """INSERT INTO tenant.tool_approval
                        (tenant_id,approval_id,release_id,tool_name,tool_version,params_hash,
                         params,side_effect,requested_by,requester_role,policy_version,status,
                         requested_at,expires_at)
                        VALUES ($1,$2,'rel','wipe_data',2,'a' || repeat('b', 63),'{}',
                         'HIGH_RISK','subject-1','AGENT_RUNNER','p1','PENDING',now(),
                         now() + interval '15 minutes')""",
                    uuid_module.UUID(tenant_id),
                    uuid_module.uuid4(),
                )
        finally:
            await connection.close()

        other = await asyncpg.connect(APP_URL)
        try:
            async with other.transaction():
                await other.execute("SELECT set_config('app.tenant_id', $1, true)", other_tenant)
                visible = await other.fetchval("SELECT count(*) FROM tenant.tool_approval")
                assert visible == 0
        finally:
            await other.close()

    asyncio.run(scenario())
