from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.content_lifecycle import (
    ContentBackend,
    ContentDeletionWorker,
    DeletionExecutor,
    RetentionSweep,
)
from trpc_service.database_migrations import apply_migrations

pytestmark = pytest.mark.integration

ADMIN_URL = os.getenv(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)
PASSWORD_HASH = (
    "$argon2id$v=19$m=65536,t=3,p=4$MRV7DB8RCvU73jcYXzxkUA$"
    "z7yjdKaXuCwuYoWzAqb25/+4f8tW5j3cxFm/pComAo4"
)


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.content_retention_policy, tenant.content_retention_change, "
            "tenant.legal_hold, "
            "tenant.content_deletion_request, platform.audit_event, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


def _settings() -> AdminSettings:
    return AdminSettings(
        database_url=APP_URL,
        session_signing_key="test-session-key-that-is-long-enough-for-hs256",
        emergency_admin_username="break-glass",
        emergency_admin_password_hash=PASSWORD_HASH,
        session_cookie_secure=False,
        oidc_enabled=False,
    )


def test_retention_hold_and_deletion_request_are_auditable() -> None:
    asyncio.run(_prepare_database())
    app = create_app(_settings())
    with TestClient(app) as client:
        assert (
            client.post(
                "/api/v1/auth/emergency/session",
                json={"username": "break-glass", "password": "correct-horse"},
            ).status_code
            == 200
        )
        tenant = client.post(
            "/api/v1/tenants",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "lifecycle-tenant", "name": "Lifecycle Tenant"},
        )
        assert tenant.status_code == 201
        tenant_id = tenant.json()["id"]
        policy_url = f"/api/v1/tenants/{tenant_id}/retention-policy"
        policy_values = {
            "inbound_payload_days": 7,
            "session_days": 90,
            "memory_days": 365,
            "artifact_days": 30,
            "idempotency_tombstone_days": 365,
            "audit_days": 90,
            "backup_days": 35,
        }
        assert client.put(policy_url, json={"audit_days": 90}).status_code == 422
        policy = client.put(policy_url, json=policy_values)
        assert policy.status_code == 202, policy.text
        assert policy.json()["status"] == "PENDING_APPROVAL"
        assert client.get(policy_url).json()["audit_days"] == 365

        app.dependency_overrides[principal_from_request] = lambda: Principal(
            "operator-two", "emergency", frozenset({"PLATFORM_ADMIN"})
        )
        approved_policy = client.post(f"{policy_url}/{policy.json()['id']}/approve")
        assert approved_policy.status_code == 200, approved_policy.text
        assert approved_policy.json()["audit_days"] == 90

        app.dependency_overrides[principal_from_request] = lambda: Principal(
            "operator-one", "emergency", frozenset({"PLATFORM_ADMIN"})
        )
        hold = client.post(f"/api/v1/tenants/{tenant_id}/legal-holds", json={"reason": "matter-42"})
        assert hold.status_code == 201
        assert (
            client.post(
                f"/api/v1/tenants/{tenant_id}/legal-holds/{hold.json()['id']}/approve"
            ).status_code
            == 409
        )
        app.dependency_overrides[principal_from_request] = lambda: Principal(
            "operator-two", "emergency", frozenset({"PLATFORM_ADMIN"})
        )
        assert (
            client.post(
                f"/api/v1/tenants/{tenant_id}/legal-holds/{hold.json()['id']}/approve"
            ).status_code
            == 200
        )
        assert (
            client.post(
                f"/api/v1/tenants/{tenant_id}/deletion-requests", json={"reason": "subject erasure"}
            ).status_code
            == 409
        )
        app.dependency_overrides[principal_from_request] = lambda: Principal(
            "operator-three", "emergency", frozenset({"PLATFORM_ADMIN"})
        )
        assert (
            client.post(
                f"/api/v1/tenants/{tenant_id}/legal-holds/{hold.json()['id']}/release"
            ).status_code
            == 200
        )

        deletion = client.post(
            f"/api/v1/tenants/{tenant_id}/deletion-requests", json={"reason": "subject erasure"}
        )
        assert deletion.status_code == 201
        assert deletion.json()["status"] == "PENDING"
        assert deletion.json()["proofs"] == []
        asyncio.run(_complete_deletion(tenant_id, deletion.json()["id"]))
        assert asyncio.run(_run_retention_sweep()) == 0
        completed = client.get(
            f"/api/v1/tenants/{tenant_id}/deletion-requests/{deletion.json()['id']}"
        )
        assert completed.status_code == 200
        assert completed.json()["status"] == "COMPLETED"
        assert {proof["backend"] for proof in completed.json()["proofs"]} == {
            kind.value for kind in ContentBackend
        }


class _Backend:
    async def erase(self, _tenant_id: str) -> int:
        return 0

    async def verify_erased(self, _tenant_id: str) -> bool:
        return True


async def _complete_deletion(tenant_id: str, request_id: str) -> None:
    database = Database(APP_URL)
    await database.open()
    try:
        worker = ContentDeletionWorker(
            database, DeletionExecutor({backend: _Backend() for backend in ContentBackend})
        )
        assert await worker.process(UUID(tenant_id), UUID(request_id))
    finally:
        await database.close()


async def _run_retention_sweep() -> int:
    database = Database(APP_URL)
    await database.open()
    try:
        return await RetentionSweep(database).run_once()
    finally:
        await database.close()
