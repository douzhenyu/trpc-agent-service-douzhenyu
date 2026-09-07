from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.database_migrations import apply_migrations
from trpc_service.ids import uuid7

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


async def _prepare_database() -> dict[str, UUID]:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.agent_session, tenant.memory_record, tenant.artifact, "
            "tenant.reply_delivery, tenant.reply_delivery_attempt, "
            "tenant.content_deletion_request, tenant.storage_migration, "
            "tenant.storage_profile, platform.audit_event, platform.tenant CASCADE"
        )
        first = await connection.fetchval(
            "INSERT INTO platform.tenant (id, slug, name) VALUES ($1, 'ops-one', 'Ops One') "
            "RETURNING id",
            uuid7(),
        )
        second = await connection.fetchval(
            "INSERT INTO platform.tenant (id, slug, name) VALUES ($1, 'ops-two', 'Ops Two') "
            "RETURNING id",
            uuid7(),
        )
        for tenant_id in (first, second):
            await connection.execute(
                """INSERT INTO tenant.agent_application (tenant_id, id, slug, name)
                VALUES ($1, $2, 'ops-app', 'Ops App')""",
                tenant_id,
                uuid4(),
            )
        sessions: list[tuple[UUID, str]] = []
        for index in range(3):
            session_id = f"s-{index}"
            await connection.execute(
                """INSERT INTO tenant.agent_session (tenant_id, application_id, id)
                VALUES ($1, (SELECT id FROM tenant.agent_application
                WHERE tenant_id=$1 LIMIT 1), $2)""",
                first,
                session_id,
            )
            sessions.append((first, session_id))
        # A second-tenant session that must never leak into the first tenant's list.
        await connection.execute(
            """INSERT INTO tenant.agent_session (tenant_id, application_id, id)
            VALUES ($1, (SELECT id FROM tenant.agent_application
            WHERE tenant_id=$1 LIMIT 1), 's-other')""",
            second,
        )
        await connection.execute(
            """INSERT INTO tenant.memory_record
            (tenant_id, id, subject_id, source_session_id, source_from_version,
             source_to_version, policy_version, content)
            VALUES ($1, $2, 'user-1', 's-0', 1, 2, 'v1', 'remembers the launch date'),
                   ($1, $3, 'user-1', 's-0', 3, 4, 'v1', 'erased memory')""",
            first,
            uuid4(),
            uuid4(),
        )
        await connection.execute(
            "UPDATE tenant.memory_record SET is_valid=false, invalidated_at=now(), "
            "invalidation_reason='OPERATOR_CORRECTION' WHERE content='erased memory'"
        )
        await connection.execute(
            """INSERT INTO tenant.artifact
            (tenant_id, artifact_id, subject_id, execution_id, filename, media_type,
             content, size_bytes, sha256, classification, expires_at)
            VALUES ($1, $2, 'user-1', 'exec-1', 'report.pdf', 'application/pdf',
             'x', 1, repeat('a', 64), 'INTERNAL', now() + interval '1 day')""",
            first,
            uuid4(),
        )
        await connection.execute(
            """INSERT INTO tenant.knowledge_base (tenant_id, id, slug, name)
            VALUES ($1, $2, 'ops-kb', 'Ops KB')""",
            first,
            uuid7(),
        )
        dead_delivery = uuid4()
        await connection.execute(
            """INSERT INTO tenant.reply_delivery
            (tenant_id, delivery_id, binding_id, execution_id, external_conversation_id,
             content, status, attempts)
            VALUES ($1, $2, $3, 'exec-1', 'oc_dead', 'payload', 'DEAD_LETTER', 4)""",
            first,
            dead_delivery,
            uuid4(),
        )
        await connection.execute(
            """INSERT INTO tenant.reply_delivery
            (tenant_id, delivery_id, binding_id, execution_id, external_conversation_id,
             content, status, attempts)
            VALUES ($1, $2, $3, 'exec-2', 'oc_live', 'payload', 'DELIVERED', 1)""",
            first,
            uuid4(),
            uuid4(),
        )
        return {
            "first": first,
            "second": second,
            "dead_delivery": dead_delivery,
            "memory_valid": await connection.fetchval(
                "SELECT id FROM tenant.memory_record WHERE is_valid"
            ),
        }
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


def _login(client: TestClient) -> None:
    assert (
        client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        ).status_code
        == 200
    )


def test_ops_lists_are_tenant_scoped_and_cursor_paginated() -> None:
    ids = asyncio.run(_prepare_database())
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        base = f"/api/v1/tenants/{ids['first']}/ops"

        sessions = client.get(f"{base}/sessions", params={"limit": 2})
        assert sessions.status_code == 200
        body = sessions.json()
        assert len(body["items"]) == 2
        assert body["next_cursor"]
        assert all(item["tenant_id"] == str(ids["first"]) for item in body["items"])

        following = client.get(
            f"{base}/sessions", params={"limit": 2, "cursor": body["next_cursor"]}
        )
        assert following.status_code == 200
        seen = {item["id"] for item in body["items"]} | {
            item["id"] for item in following.json()["items"]
        }
        assert seen == {"s-0", "s-1", "s-2"}

        broken = client.get(f"{base}/sessions", params={"cursor": "not-a-cursor"})
        assert broken.status_code == 400
        assert broken.json()["error"]["code"] == "INVALID_OPS_CURSOR"

        other = client.get(f"/api/v1/tenants/{ids['second']}/ops/sessions")
        assert [item["id"] for item in other.json()["items"]] == ["s-other"]

        knowledge = client.get(f"/api/v1/tenants/{ids['first']}/knowledge-bases")
        assert knowledge.status_code == 200
        assert [base["slug"] for base in knowledge.json()["items"]] == ["ops-kb"]

        memories = client.get(f"{base}/memories", params={"valid_only": "true"})
        assert memories.status_code == 200
        assert [item["id"] for item in memories.json()["items"]] == [str(ids["memory_valid"])]

        artifacts = client.get(f"{base}/artifacts")
        assert artifacts.status_code == 200
        artifact = artifacts.json()["items"][0]
        assert artifact["filename"] == "report.pdf"
        assert "content" not in artifact


def test_dead_letter_requeue_is_guarded_and_audited() -> None:
    ids = asyncio.run(_prepare_database())
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        base = f"/api/v1/tenants/{ids['first']}/ops"

        letters = client.get(f"{base}/dead-letters")
        assert letters.status_code == 200
        items = letters.json()["items"]
        assert [item["delivery_id"] for item in items] == [str(ids["dead_delivery"])]
        assert items[0]["attempts"] == 4

        requeue = client.post(f"{base}/dead-letters/{ids['dead_delivery']}/retries")
        assert requeue.status_code == 200
        assert requeue.json()["status"] == "QUEUED"

        again = client.post(f"{base}/dead-letters/{ids['dead_delivery']}/retries")
        assert again.status_code == 409
        assert again.json()["error"]["code"] == "DEAD_LETTER_NOT_RETRYABLE"

        missing = client.post(f"{base}/dead-letters/{uuid4()}/retries")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "DEAD_LETTER_NOT_FOUND"

        audits = client.get(
            f"/api/v1/tenants/{ids['first']}/audit-events",
            params={"target_type": "reply_delivery"},
        )
        assert audits.status_code == 200
        actions = [event["action"] for event in audits.json()["events"]]
        assert "ops.dead_letter.requeue" in actions


def test_operations_view_surfaces_status_retry_and_evidence() -> None:
    ids = asyncio.run(_prepare_database())
    asyncio.run(_seed_operations(ids["first"]))
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        operations = client.get(f"/api/v1/tenants/{ids['first']}/ops/operations")
        assert operations.status_code == 200
        by_kind = {item["kind"]: item for item in operations.json()["items"]}
        deletion = by_kind["CONTENT_DELETION"]
        assert deletion["status"] == "RETRYABLE"
        assert deletion["attempts"] == 2
        assert deletion["next_action_at"] is not None
        assert deletion["evidence"]["proofs"][0]["backend"] == "SQL"

        migration = by_kind["STORAGE_MIGRATION"]
        assert migration["status"] == "VALIDATING"
        assert migration["evidence"]["approval_status"] == "APPROVED"


async def _seed_operations(tenant_id: UUID) -> None:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        now = datetime.now(UTC)
        source_profile = await connection.fetchval(
            """INSERT INTO tenant.storage_profile
            (tenant_id, id, alias, classification, worker_pool, backends, active,
             encryption_key_ref)
            VALUES ($1, $2, 'src', 'INTERNAL', 'default', '[{"kind":"POSTGRES"}]'::jsonb,
             false, 'kms://ops-test')
            RETURNING id""",
            tenant_id,
            uuid7(),
        )
        target_profile = await connection.fetchval(
            """INSERT INTO tenant.storage_profile
            (tenant_id, id, alias, classification, worker_pool, backends, active,
             encryption_key_ref)
            VALUES ($1, $2, 'dst', 'INTERNAL', 'default', '[{"kind":"POSTGRES"}]'::jsonb,
             false, 'kms://ops-test')
            RETURNING id""",
            tenant_id,
            uuid7(),
        )
        await connection.execute(
            """INSERT INTO tenant.storage_migration
            (tenant_id, id, source_profile_id, target_profile_id, state, approval_status,
             requested_by, approved_by, approved_at, observation_seconds)
            VALUES ($1, $2, $3, $4, 'VALIDATING', 'APPROVED', 'op', 'approver', $5, 60)""",
            tenant_id,
            uuid4(),
            source_profile,
            target_profile,
            now - timedelta(minutes=1),
        )
        request_id = uuid4()
        await connection.execute(
            """INSERT INTO tenant.content_deletion_request
            (tenant_id, id, requested_by, reason, status, primary_due_at, backup_due_at,
             attempts, next_attempt_at, last_error)
            VALUES ($1, $2, 'op', 'tenant erasure', 'RETRYABLE', $3, $4, 2, $5,
             'DELETION_BACKEND_RETRYABLE')""",
            tenant_id,
            request_id,
            now + timedelta(hours=24),
            now + timedelta(days=35),
            now + timedelta(minutes=5),
        )
        await connection.execute(
            """INSERT INTO tenant.content_deletion_proof
            (tenant_id, request_id, backend, deleted_count, verified, evidence_digest, completed_at)
            VALUES ($1, $2, 'SQL', 3, true, repeat('b', 64), $3)""",
            tenant_id,
            request_id,
            now,
        )
    finally:
        await connection.close()
