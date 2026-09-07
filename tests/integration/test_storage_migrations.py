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
from trpc_service.database_migrations import apply_migrations
from trpc_service.storage import InMemorySqlAdapter
from trpc_service.storage_migration import StorageMigrationExecutor
from trpc_service.storage_migration_worker import StorageMigrationWorker

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
            "TRUNCATE tenant.storage_migration, tenant.storage_profile, "
            "platform.idempotency_record, platform.audit_event, platform.platform_role_assignment, "
            "platform.platform_user, tenant.member_role, tenant.member, platform.tenant CASCADE"
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


def _backends(tenant_id: str) -> list[dict[str, object]]:
    return [
        {
            "kind": kind,
            "endpoint": f"https://{kind.lower()}.migration.test",
            "dedicated": False,
            "secret_ref": f"vault://tenant/{tenant_id}/storage/{kind.lower()}#credential",
        }
        for kind in ("SQL", "REDIS", "VECTOR", "OBJECT")
    ]


async def _run_worker(factory: object, tenant_id: UUID, count: int) -> None:
    database = Database(APP_URL)
    await database.open()
    try:
        worker = StorageMigrationWorker(database, StorageMigrationExecutor(factory))  # type: ignore[arg-type]
        for _ in range(count):
            assert await worker.run_once(tenant_id)
    finally:
        await database.close()


def test_approved_online_migration_switches_once_and_rolls_back_in_observation() -> None:
    asyncio.run(_prepare_database())
    current = Principal("migration-requester", "emergency", frozenset({"PLATFORM_ADMIN"}))

    async def principal_override() -> Principal:
        return current

    app = create_app(_settings())
    app.dependency_overrides[principal_from_request] = principal_override
    with TestClient(app) as client:
        tenant = client.post(
            "/api/v1/tenants",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "migration", "name": "Migration"},
        )
        assert tenant.status_code == 201, tenant.text
        tenant_id = tenant.json()["id"]

        def profile(alias: str, active: bool) -> dict[str, object]:
            return {
                "alias": alias,
                "classification": "INTERNAL",
                "worker_pool": "shared-workers",
                "encryption_key_ref": f"vault://tenant/{tenant_id}/storage#encryption_key",
                "backends": _backends(tenant_id),
                "activate": active,
            }

        source = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-profiles",
            headers={"Idempotency-Key": str(uuid4())},
            json=profile("source", True),
        )
        target = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-profiles",
            headers={"Idempotency-Key": str(uuid4())},
            json=profile("target", False),
        )
        assert source.status_code == target.status_code == 201

        created = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-migrations",
            json={"target_profile_id": target.json()["id"], "observation_seconds": 60},
        )
        assert created.status_code == 201, created.text
        migration_id = created.json()["id"]
        assert created.json()["state"] == "PREPARED"
        assert created.json()["approval_status"] == "PENDING"
        assert (
            client.post(
                f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}/approvals",
                json={"decision": "APPROVE"},
            ).status_code
            == 409
        )

        current = Principal("migration-approver", "emergency", frozenset({"PLATFORM_ADMIN"}))
        approved = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}/approvals",
            json={"decision": "APPROVE"},
        )
        assert approved.status_code == 200
        started = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}/start"
        )
        assert started.status_code == 200, started.text
        assert started.json()["state"] == "BACKFILLING"

        source_adapter = InMemorySqlAdapter()
        target_adapter = InMemorySqlAdapter()
        asyncio.run(source_adapter.put(tenant_id, "session:1", b"immutable-event"))

        class AdapterFactory:
            async def adapters(
                self, **kwargs: str
            ) -> list[tuple[InMemorySqlAdapter, InMemorySqlAdapter]]:
                if kwargs["source_profile_id"] == source.json()["id"]:
                    return [(source_adapter, target_adapter)]
                return [(target_adapter, source_adapter)]

        factory = AdapterFactory()
        asyncio.run(_run_worker(factory, UUID(tenant_id), 3))
        ready = client.get(f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}")
        assert ready.json()["state"] == "READY_TO_SWITCH"
        assert (
            client.get(f"/api/v1/tenants/{tenant_id}/storage-profiles/active").json()["id"]
            == source.json()["id"]
        )
        switched = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}/advance",
            json={"operation": "SWITCH"},
        )
        assert switched.status_code == 200
        assert switched.json()["state"] == "OBSERVING"
        assert (
            client.get(f"/api/v1/tenants/{tenant_id}/storage-profiles/active").json()["id"]
            == target.json()["id"]
        )
        asyncio.run(target_adapter.put(tenant_id, "session:2", b"observation-write"))

        current = Principal("migration-requester", "emergency", frozenset({"PLATFORM_ADMIN"}))
        requested = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}/rollback-approvals",
            json={"decision": "REQUEST"},
        )
        assert requested.status_code == 200
        current = Principal("migration-approver", "emergency", frozenset({"PLATFORM_ADMIN"}))
        assert (
            client.post(
                f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}/rollback-approvals",
                json={"decision": "APPROVE"},
            ).status_code
            == 200
        )
        rolled_back = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}/rollback"
        )
        assert rolled_back.status_code == 200
        assert rolled_back.json()["state"] == "ROLLING_BACK"
        asyncio.run(_run_worker(factory, UUID(tenant_id), 3))
        rolled_back = client.get(f"/api/v1/tenants/{tenant_id}/storage-migrations/{migration_id}")
        assert rolled_back.json()["state"] == "ROLLED_BACK"
        assert asyncio.run(source_adapter.get(tenant_id, "session:2")) == b"observation-write"
        assert (
            client.get(f"/api/v1/tenants/{tenant_id}/storage-profiles/active").json()["id"]
            == source.json()["id"]
        )
