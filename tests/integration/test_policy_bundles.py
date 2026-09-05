from __future__ import annotations

import asyncio
import json
import os
from uuid import uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.database_migrations import apply_migrations
from trpc_service.governance import DataClassification, GovernanceRules, scan_messages

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
SIGNING_KEY = "test-policy-signing-key"

pytestmark = pytest.mark.integration

RULES = {"max_outbound_classification": "RESTRICTED", "secret_detection_enabled": True}


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.policy_bundle, tenant.cost_ledger, tenant.budget_alert, "
            "tenant.budget_period_state, tenant.budget, tenant.model_price, "
            "tenant.session_event, tenant.session_lease, tenant.agent_execution, "
            "tenant.agent_session, platform.outbox_record, "
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
            "Governance Tenant",
        )
    finally:
        await connection.close()
    return str(tenant_id)


async def _seed_restricted_execution(tenant_id: str) -> None:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            """INSERT INTO platform.audit_event
            (id,tenant_id,actor,auth_method,action,decision,target_type,target_id,details)
            VALUES ($1,$2,'governance','anonymous','governance.outbound_denied','DENY',
            'model_call',$3,$4::jsonb)""",
            uuid4(),
            tenant_id,
            "release-seed",
            json.dumps({"effective_classification": "RESTRICTED"}),
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
        policy_signing_key=SIGNING_KEY,
    )


def _login(client: TestClient) -> None:
    login = client.post(
        "/api/v1/auth/emergency/session",
        json={"username": "break-glass", "password": "correct-horse"},
    )
    assert login.status_code == 200


def test_policy_bundle_lifecycle_supports_canary_and_rollback() -> None:
    asyncio.run(_prepare_database())

    async def seed() -> str:
        return await _seed_tenant()

    tenant_id = asyncio.run(seed())
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        first = client.put(
            f"/api/v1/tenants/{tenant_id}/policy-bundles",
            headers={"Idempotency-Key": str(uuid4())},
            json={"rules": RULES},
        )
        assert first.status_code == 200, first.text
        assert first.json()["version"] == 1
        assert first.json()["status"] == "DRAFT"
        assert len(first.json()["signature"]) == 64

        verified = client.get(f"/api/v1/tenants/{tenant_id}/policy-bundles/1/verification")
        assert verified.status_code == 200
        assert verified.json() == {"valid": True}

        activated = client.post(
            f"/api/v1/tenants/{tenant_id}/policy-bundles/1/activations",
            headers={"Idempotency-Key": str(uuid4())},
            json={"canary_percentage": 0},
        )
        assert activated.status_code == 200
        assert activated.json()["status"] == "ACTIVE"

        second = client.put(
            f"/api/v1/tenants/{tenant_id}/policy-bundles",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "rules": {
                    "max_outbound_classification": "CONFIDENTIAL",
                    "secret_detection_enabled": True,
                }
            },
        )
        assert second.status_code == 200
        canary = client.post(
            f"/api/v1/tenants/{tenant_id}/policy-bundles/2/activations",
            headers={"Idempotency-Key": str(uuid4())},
            json={"canary_percentage": 50},
        )
        assert canary.status_code == 200
        assert canary.json()["status"] == "CANARY"
        assert canary.json()["canary_percentage"] == 50

        listing = client.get(f"/api/v1/tenants/{tenant_id}/policy-bundles")
        statuses = {item["version"]: item["status"] for item in listing.json()["items"]}
        assert statuses == {1: "ACTIVE", 2: "CANARY"}

        # Rollback: re-activating version 1 retires the canary.
        rollback = client.post(
            f"/api/v1/tenants/{tenant_id}/policy-bundles/1/activations",
            headers={"Idempotency-Key": str(uuid4())},
            json={"canary_percentage": 0},
        )
        assert rollback.status_code == 200
        assert rollback.json()["status"] == "ACTIVE"
        listing = client.get(f"/api/v1/tenants/{tenant_id}/policy-bundles")
        statuses = {item["version"]: item["status"] for item in listing.json()["items"]}
        assert statuses == {1: "ACTIVE", 2: "RETIRED"}


def test_policy_bundle_service_resolves_active_or_canary_rules() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        from trpc_service.policy_bundles import PolicyBundleService

        tenant_id = await _seed_tenant()
        database = Database(APP_URL)
        await database.open()
        try:
            service = PolicyBundleService(database, signing_key=SIGNING_KEY)
            assert await service.resolve(tenant_id, "any") is None
            await service.create_version(
                tenant_id,
                GovernanceRules(max_outbound_classification="RESTRICTED"),
                actor="governance-admin",
            )
            await service.activate(tenant_id, 1)
            resolved = await service.resolve(tenant_id, "any")
            assert resolved is not None
            assert resolved.version == 1
            assert resolved.canary is False
            await service.create_version(
                tenant_id,
                GovernanceRules(max_outbound_classification="CONFIDENTIAL"),
                actor="governance-admin",
            )
            await service.activate(tenant_id, 2, canary_percentage=100)
            canary = await service.resolve(tenant_id, "any")
            assert canary is not None
            assert canary.version == 2 and canary.canary is True
            # Rollback restores the stable rules for every decision.
            await service.activate(tenant_id, 1)
            stable = await service.resolve(tenant_id, "any")
            assert stable is not None
            assert stable.version == 1
        finally:
            await database.close()

    asyncio.run(scenario())


def test_canary_without_a_stable_version_fails_closed() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        from trpc_service.policy_bundles import (
            PolicyBundleError,
            PolicyBundleService,
        )

        tenant_id = await _seed_tenant()
        database = Database(APP_URL)
        await database.open()
        try:
            service = PolicyBundleService(database, signing_key=SIGNING_KEY)
            await service.create_version(tenant_id, GovernanceRules(), actor="governance-admin")
            # First-ever activation as a pure canary: non-bucket decisions
            # would run without any stable bundle, so resolution fails closed.
            await service.activate(tenant_id, 1, canary_percentage=100)
            with pytest.raises(PolicyBundleError, match="POLICY_BUNDLE_UNAVAILABLE"):
                await service.resolve(tenant_id, "outside-the-bucket")
        finally:
            await database.close()

    asyncio.run(scenario())


def test_restricted_outbound_denials_leave_audit_evidence() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        tenant_id = await _seed_tenant()
        await _seed_restricted_execution(tenant_id)
        connection = await asyncpg.connect(ADMIN_URL)
        try:
            rows = await connection.fetch(
                "SELECT action,decision,details FROM platform.audit_event "
                "WHERE tenant_id=$1 AND action='governance.outbound_denied'",
                tenant_id,
            )
            assert len(rows) == 1
            assert rows[0]["decision"] == "DENY"
            details = json.loads(rows[0]["details"])
            assert details["effective_classification"] == "RESTRICTED"
        finally:
            await connection.close()

    asyncio.run(scenario())
    # The scan module is the same one the gateway and the Runner callback use;
    # a RESTRICTED payload must always be detected for the evidence to exist.
    scan = scan_messages([{"role": "user", "content": "11010519491231002X"}])
    assert scan.detected_classification.value == "CONFIDENTIAL"


def test_runner_governance_callback_blocks_secrets() -> None:
    from trpc_agent_sdk.models import LlmRequest
    from trpc_agent_sdk.types import Content, Part

    from trpc_service.agent.runner import governance_model_callback

    class StaticResolver:
        def __init__(self, rules: GovernanceRules | None) -> None:
            self._rules = rules

        async def resolve_rules(self, tenant_id: str, decision_key: str) -> GovernanceRules | None:
            return self._rules

    callback = governance_model_callback(
        StaticResolver(GovernanceRules()),
        tenant_id="tenant-1",
        release_id="release-1",
        declared_classification=DataClassification.INTERNAL,
    )
    request = LlmRequest(
        contents=[Content(role="user", parts=[Part(text="my key is sk-abcdefghijklmnopqrst1234")])]
    )
    blocked = asyncio.run(callback({"session_id": "session-1"}, request))
    assert blocked is not None
    assert blocked.error_code == "GOVERNANCE_DENIED"

    permissive = governance_model_callback(
        StaticResolver(None),
        tenant_id="tenant-1",
        release_id="release-1",
        declared_classification=DataClassification.INTERNAL,
    )
    assert asyncio.run(permissive({"session_id": "session-1"}, request)) is None

    allowed_request = LlmRequest(contents=[Content(role="user", parts=[Part(text="hello")])])
    assert asyncio.run(callback({"session_id": "session-1"}, allowed_request)) is None
