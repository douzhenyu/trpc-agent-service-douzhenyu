"""Integration tests: audit hash chain, outbox dispatch, manifest WORM archive."""

from __future__ import annotations

import asyncio
import json
import os
import uuid as uuid_module
from datetime import UTC, datetime

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.audit_chain.manifest import AuditManifestService
from trpc_service.audit_chain.outbox import AuditOutboxDispatcher, enqueue_audit
from trpc_service.audit_chain.worm import MemoryWormArchive
from trpc_service.database_migrations import apply_migrations

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
SIGNING_KEY = "test-audit-signing-key"

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
            "tenant.agent_session, platform.outbox_record, platform.audit_outbox, "
            "platform.audit_manifest, platform.audit_chain_state, platform.audit_event, "
            "tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, platform.idempotency_record, "
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
            "Audit Chain Tenant",
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


async def _write_business_event(tenant_id: str, action: str, trace_id: str | None = None) -> None:
    database = Database(APP_URL)
    await database.open()
    try:
        # The business write and the audit event share this transaction.
        async with database.tenant_transaction(uuid_module.UUID(tenant_id)) as connection:
            await connection.execute(
                "SELECT set_config('app.trace_id', $1, true)",
                trace_id or "",
            )
            await insert_audit(
                connection,
                Principal(subject="agent-worker-1", auth_method="oidc", roles=frozenset()),
                action,
                "ALLOW",
                target_type="tool_call",
                target_id="call-1",
                tenant_id=uuid_module.UUID(tenant_id),
                details={"cost_micros": 120},
                trace_id=trace_id,
            )
    finally:
        await database.close()


async def _verify_chain(tenant_id: str) -> dict:
    database = Database(APP_URL)
    await database.open()
    try:
        from trpc_service.audit_chain.chain import verify_tenant_chain

        async with database.tenant_transaction(uuid_module.UUID(tenant_id)) as connection:
            return await verify_tenant_chain(connection, tenant_id)
    finally:
        await database.close()


def test_chain_appends_atomically_and_detects_tampering() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    asyncio.run(_write_business_event(tenant_id, "tool.register", trace_id="trace-1"))
    asyncio.run(_write_business_event(tenant_id, "tool.invoke", trace_id="trace-1"))

    report = asyncio.run(_verify_chain(tenant_id))
    assert report["valid"] is True
    assert report["events"] == 2

    async def tamper() -> None:
        connection = await asyncpg.connect(ADMIN_URL)
        try:
            await connection.execute(
                "UPDATE platform.audit_event SET actor='attacker' "
                "WHERE tenant_id=$1 AND chain_index=1",
                uuid_module.UUID(tenant_id),
            )
        finally:
            await connection.close()

    asyncio.run(tamper())
    broken = asyncio.run(_verify_chain(tenant_id))
    assert broken["valid"] is False
    assert broken["broken_at_index"] == 1


def test_audit_outbox_dispatches_in_chain_order() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            for index in range(3):
                async with database.transaction() as connection:
                    # A real producer would also write business state here.
                    await enqueue_audit(
                        connection,
                        tenant_id=tenant_id,
                        actor="job-worker-1",
                        auth_method="oidc",
                        action="budget.settled",
                        decision="ALLOW",
                        target_type="budget",
                        target_id=f"b-{index}",
                        details={"index": index},
                    )
            dispatched = await AuditOutboxDispatcher(database).dispatch_pending(tenant_id)
            assert dispatched == 3
            again = await AuditOutboxDispatcher(database).dispatch_pending(tenant_id)
            assert again == 0
        finally:
            await database.close()

    asyncio.run(scenario())
    report = asyncio.run(_verify_chain(tenant_id))
    assert report["valid"] is True
    assert report["events"] == 3


def test_query_correction_manifest_and_self_audit() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    asyncio.run(_write_business_event(tenant_id, "tool.register", trace_id="trace-9"))
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        os.environ["AUDIT_SIGNING_KEY"] = SIGNING_KEY

        queried = client.get(
            f"/api/v1/tenants/{tenant_id}/audit-events",
            params={"actor": "agent-worker-1"},
        )
        assert queried.status_code == 200, queried.text
        events = queried.json()["events"]
        assert [event["action"] for event in events] == ["tool.register"]

        corrected = client.post(
            f"/api/v1/tenants/{tenant_id}/audit-events/{events[0]['id']}/corrections",
            json={"explanation": "target_id recorded a stale identifier"},
        )
        assert corrected.status_code == 200, corrected.text

        # The query itself and the break-glass access are audited.
        all_events = client.get(f"/api/v1/tenants/{tenant_id}/audit-events")
        actions = {event["action"] for event in all_events.json()["events"]}
        assert "audit.query" in actions
        assert "audit.break_glass" in actions
        assert "audit.correction" in actions
        corrections = client.get(
            f"/api/v1/tenants/{tenant_id}/audit-events",
            params={"target_type": "audit_event"},
        )
        correction_events = [
            event for event in corrections.json()["events"] if event["action"] == "audit.correction"
        ]
        assert len(correction_events) == 1

        archived = client.post(f"/api/v1/tenants/{tenant_id}/audit-manifests")
        assert archived.status_code == 200, archived.text

        verification = client.get(f"/api/v1/tenants/{tenant_id}/audit-manifests/verification")
        assert verification.status_code == 200, verification.text
        assert verification.json()["chain"]["valid"] is True
        assert verification.json()["tamper_detected"] is False
        os.environ.pop("AUDIT_SIGNING_KEY", None)


def test_manifest_archives_to_worm_and_detects_tampering() -> None:
    asyncio.run(_prepare_database())
    tenant_id = asyncio.run(_seed_tenant())
    for index in range(4):
        asyncio.run(_write_business_event(tenant_id, f"action.{index}"))

    async def scenario() -> tuple[dict, MemoryWormArchive, dict]:
        worm = MemoryWormArchive()
        database = Database(APP_URL)
        await database.open()
        try:
            service = AuditManifestService(database, signing_key=SIGNING_KEY, worm=worm)
            record = await service.build_and_archive(tenant_id)
            assert record is not None
            assert record.event_count == 4
            stored, retain = worm.objects[f"trpc-audit-worm/{tenant_id}/manifest-000000000001.json"]
            document = json.loads(stored)
            assert document["signature"] == record.signature
            assert retain > datetime.now(UTC)

            empty = await service.build_and_archive(tenant_id)
            assert empty is None
            report = await service.verify_tenant_evidence(tenant_id)
            return document, worm, report
        finally:
            await database.close()

    document, worm, report = asyncio.run(scenario())
    assert report["chain"]["valid"] is True
    assert report["manifests"] == 1
    assert report["manifest_signatures_valid"] is True

    async def tamper_and_verify() -> dict:
        connection = await asyncpg.connect(ADMIN_URL)
        try:
            await connection.execute(
                "UPDATE platform.audit_event SET actor='attacker' "
                "WHERE tenant_id=$1 AND chain_index=2",
                uuid_module.UUID(tenant_id),
            )
        finally:
            await connection.close()
        database = Database(APP_URL)
        await database.open()
        try:
            service = AuditManifestService(database, signing_key=SIGNING_KEY, worm=worm)
            return await service.verify_tenant_evidence(tenant_id)
        finally:
            await database.close()

    tampered_report = asyncio.run(tamper_and_verify())
    assert tampered_report["tamper_detected"] is True
    assert tampered_report["chain"]["valid"] is False
