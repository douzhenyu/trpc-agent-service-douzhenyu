from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.settings import AdminSettings
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
            "TRUNCATE tenant.storage_profile, platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
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


def _backends(tenant_id: str, *, dedicated: bool) -> list[dict[str, object]]:
    return [
        {
            "kind": kind,
            "endpoint": f"https://{kind.lower()}.example.test",
            "dedicated": dedicated,
            "secret_ref": f"vault://tenant/{tenant_id}/storage/{kind.lower()}#credential",
        }
        for kind in ("SQL", "REDIS", "VECTOR", "OBJECT", "EXTERNAL_MEMORY")
    ]


def test_storage_profiles_validate_isolation_and_expose_active_worker_pool() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        login = client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        assert login.status_code == 200
        tenant = client.post(
            "/api/v1/tenants",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "storage-tenant", "name": "Storage Tenant"},
        )
        assert tenant.status_code == 201
        tenant_id = tenant.json()["id"]

        rejected = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-profiles",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "alias": "restricted-shared",
                "classification": "RESTRICTED",
                "worker_pool": "shared-workers",
                "encryption_key_ref": f"vault://tenant/{tenant_id}/storage#encryption_key",
                "backends": _backends(tenant_id, dedicated=False),
            },
        )
        assert rejected.status_code == 422
        assert rejected.json()["error"]["code"] == "DEDICATED_STORAGE_REQUIRED"

        created = client.post(
            f"/api/v1/tenants/{tenant_id}/storage-profiles",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "alias": "restricted-dedicated",
                "classification": "RESTRICTED",
                "worker_pool": "tenant-storage-workers",
                "encryption_key_ref": f"vault://tenant/{tenant_id}/storage#encryption_key",
                "backends": _backends(tenant_id, dedicated=True),
                "activate": True,
            },
        )
        assert created.status_code == 201, created.text
        assert created.json()["active"] is True
        assert created.json()["worker_pool"] == "tenant-storage-workers"
        assert created.json()["encryption_key_ref"].endswith("#encryption_key")

        active = client.get(f"/api/v1/tenants/{tenant_id}/storage-profiles/active")
        assert active.status_code == 200
        assert active.json()["alias"] == "restricted-dedicated"

        second_tenant = client.post(
            "/api/v1/tenants",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "storage-tenant-two", "name": "Storage Tenant Two"},
        )
        assert second_tenant.status_code == 201
        second_tenant_id = second_tenant.json()["id"]
        conflict = client.post(
            f"/api/v1/tenants/{second_tenant_id}/storage-profiles",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "alias": "must-not-share-dedicated-store",
                "classification": "RESTRICTED",
                "worker_pool": "tenant-storage-workers",
                "encryption_key_ref": (f"vault://tenant/{second_tenant_id}/storage#encryption_key"),
                "backends": _backends(second_tenant_id, dedicated=True),
                "activate": True,
            },
        )
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "STORAGE_RESOURCE_ALREADY_CLAIMED"
