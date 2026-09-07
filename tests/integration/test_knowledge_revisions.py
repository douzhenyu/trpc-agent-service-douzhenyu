from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import asyncpg
from fastapi.testclient import TestClient
from pydantic import SecretStr

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.database_migrations import apply_migrations
from trpc_service.knowledge import (
    DatabaseKnowledgeDeploymentResolver,
    DatabaseKnowledgeRetriever,
    KnowledgeRevisionBuilder,
)

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:5432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://trpc_platform_app:app-password@127.0.0.1:5432/trpc_platform"
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
            "TRUNCATE platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


def _settings() -> AdminSettings:
    return AdminSettings(
        database_url=APP_URL,
        session_signing_key=SecretStr("test-session-key-that-is-long-enough-for-hs256"),
        emergency_admin_username="break-glass",
        emergency_admin_password_hash=SecretStr(PASSWORD_HASH),
        session_cookie_secure=False,
        oidc_enabled=False,
    )


def _login_and_create_tenant(client: TestClient) -> str:
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
        json={"slug": f"tenant-{uuid4().hex[:8]}", "name": "Knowledge Tenant"},
    )
    assert tenant.status_code == 201
    return str(tenant.json()["id"])


def _revision_payload(content: str, *, subject: str) -> dict[str, object]:
    return {
        "sources": [
            {
                "source_ref": "artifact://handbook/benefits-v1",
                "content": content,
                "acl_subjects": [subject],
                "data_classification": "INTERNAL",
            },
            {
                "source_ref": "artifact://handbook/private-v1",
                "content": "Only Bob may read this private compensation policy.",
                "acl_subjects": ["im:bob"],
                "data_classification": "CONFIDENTIAL",
            },
        ],
        "chunking": {"max_chars": 120, "overlap_chars": 0},
        "embedding_model": "deterministic-test-v1",
        "index_config": {"kind": "postgres-fts-v1"},
    }


def test_knowledge_revisions_build_independently_and_retrieval_enforces_acl() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        bases = f"/api/v1/tenants/{tenant_id}/knowledge-bases"
        base = client.post(
            bases,
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "handbook", "name": "Employee Handbook"},
        )
        assert base.status_code == 201
        base_id = str(base.json()["id"])

        first = client.post(
            f"{bases}/{base_id}/revisions",
            headers={"Idempotency-Key": str(uuid4())},
            json=_revision_payload("Benefits include 25 days of annual leave.", subject="im:alice"),
        )
        assert first.status_code == 202
        first_revision = str(first.json()["id"])
        assert first.json()["status"] == "BUILDING"
        assert len(first.json()["content_hash"]) == 64

        second = client.post(
            f"{bases}/{base_id}/revisions",
            headers={"Idempotency-Key": str(uuid4())},
            json=_revision_payload("Benefits include 30 days of annual leave.", subject="im:alice"),
        )
        assert second.status_code == 202
        second_revision = str(second.json()["id"])

        async def build_and_retrieve() -> None:
            database = Database(APP_URL)
            await database.open()
            try:
                builder = KnowledgeRevisionBuilder(database)
                await builder.build(tenant_id, first_revision)
                retriever = DatabaseKnowledgeRetriever(database)
                alice = await retriever.retrieve(
                    tenant_id=tenant_id,
                    base_id=base_id,
                    revision_id=first_revision,
                    subject_id="im:alice",
                    query="annual leave",
                )
                assert [result.content for result in alice] == [
                    "Benefits include 25 days of annual leave."
                ]
                assert (
                    await retriever.retrieve(
                        tenant_id=tenant_id,
                        base_id=base_id,
                        revision_id=first_revision,
                        subject_id="im:mallory",
                        query="annual leave",
                    )
                    == []
                )
                await builder.build(tenant_id, second_revision)
            finally:
                await database.close()

        asyncio.run(build_and_retrieve())

        deployed = client.post(
            f"{bases}/{base_id}/deployments",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "environment": "STAGING",
                "revision_id": first_revision,
                "rollout_percentage": 100,
            },
        )
        assert deployed.status_code == 201
        canary = client.post(
            f"{bases}/{base_id}/deployments",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "environment": "STAGING",
                "revision_id": second_revision,
                "rollout_percentage": 10,
            },
        )
        assert canary.status_code == 201
        assert canary.json()["previous_revision_id"] == first_revision

        rolled_back = client.post(
            f"{bases}/{base_id}/deployments/rollback",
            headers={"Idempotency-Key": str(uuid4())},
            json={"revision_id": first_revision, "environment": "STAGING"},
        )
        assert rolled_back.status_code == 201
        assert rolled_back.json()["revision_id"] == first_revision

        async def resolve_rolled_back_revision() -> str | None:
            database = Database(APP_URL)
            await database.open()
            try:
                return await DatabaseKnowledgeDeploymentResolver(database).resolve(
                    tenant_id, base_id, "STAGING", "session-knowledge-1"
                )
            finally:
                await database.close()

        assert asyncio.run(resolve_rolled_back_revision()) == first_revision
