from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import asyncpg
from fastapi.testclient import TestClient
from pydantic import SecretStr

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.auth import Principal, encode_session
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.agent_worker import DatabaseDeploymentRouteResolver
from trpc_service.database_migrations import apply_migrations

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform"
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


async def _seed_release(tenant_id: str, application_id: str, *, version: int = 1) -> str:
    release_id = uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            """INSERT INTO tenant.agent_release
            (tenant_id,id,application_id,model_alias,data_classification,region,
            fallback_aliases,model_profiles,release_version,draft_snapshot)
            VALUES ($1,$2,$3,'eval-model','INTERNAL','cn-test','[]'::jsonb,
            '[{"alias":"eval-model","provider_model":"test"}]'::jsonb,$4,
            '{"knowledge_refs":["knowledge-base"],"tool_aliases":["search"],
            "governance_policy_ref":"standard"}'::jsonb)""",
            tenant_id,
            release_id,
            application_id,
            version,
        )
    finally:
        await connection.close()
    return str(release_id)


async def _seed_active_production_deployment(
    tenant_id: str, application_id: str, release_id: str
) -> str:
    deployment_id = uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            """INSERT INTO tenant.agent_deployment
            (tenant_id,id,application_id,environment,release_id,rollout_percentage,status,initiator,
            version,activated_at)
            VALUES ($1,$2,$3,'PRODUCTION',$4,100,'ACTIVE','seeded-deployment',1,now())""",
            tenant_id,
            deployment_id,
            application_id,
            release_id,
        )
    finally:
        await connection.close()
    return str(deployment_id)


async def _seed_eval_dependencies(tenant_id: str) -> None:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            """INSERT INTO tenant.tool_definition
            (tenant_id,name,version,description,side_effect,input_schema,output_schema,
            timeout_seconds,data_classification,created_by)
            VALUES ($1,'search',2,'Search trusted documents','READ_ONLY','{}'::jsonb,'{}'::jsonb,
            30,'INTERNAL','eval-test')""",
            tenant_id,
        )
        await connection.execute(
            """INSERT INTO tenant.policy_bundle
            (tenant_id,version,rules,bundle,signature,status,created_by)
            VALUES ($1,7,'{}'::jsonb,'{}'::jsonb,$2,'ACTIVE','eval-test')""",
            tenant_id,
            "a" * 64,
        )
    finally:
        await connection.close()


async def _resolve_production_release(tenant_id: str, application_id: str) -> str | None:
    database = Database(APP_URL)
    await database.open()
    try:
        return await DatabaseDeploymentRouteResolver(database).resolve(
            tenant_id, application_id, "PRODUCTION", "eval-canary-session"
        )
    finally:
        await database.close()


async def _seed_tenant_developer(tenant_id: str) -> str:
    user_id, member_id, role_id = uuid4(), uuid4(), uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            """INSERT INTO platform.platform_user (id,issuer,subject,display_name)
            VALUES ($1,'https://identity.example.test',$2,'Eval Approver')""",
            user_id,
            f"approver-{user_id}",
        )
        await connection.execute(
            "INSERT INTO tenant.member (tenant_id,id,user_id) VALUES ($1,$2,$3)",
            tenant_id,
            member_id,
            user_id,
        )
        await connection.execute(
            """INSERT INTO tenant.member_role (tenant_id,id,member_id,role)
            VALUES ($1,$2,$3,'AGENT_DEVELOPER')""",
            tenant_id,
            role_id,
            member_id,
        )
    finally:
        await connection.close()
    return encode_session(_settings(), Principal(str(user_id), "oidc", frozenset()))


def _settings() -> AdminSettings:
    return AdminSettings(
        database_url=APP_URL,
        session_signing_key=SecretStr("test-session-key-that-is-long-enough-for-hs256"),
        emergency_admin_username="break-glass",
        emergency_admin_password_hash=SecretStr(PASSWORD_HASH),
        session_cookie_secure=False,
        oidc_enabled=False,
    )


def test_eval_suite_is_versioned_with_dataset_scorers_and_thresholds() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
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
            json={"slug": f"tenant-{uuid4().hex[:8]}", "name": "Eval Tenant"},
        ).json()
        application = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "eval-agent", "name": "Eval Agent"},
        ).json()

        created = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/eval-suites",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "slug": "production-safety",
                "dataset": {"ref": "artifact://eval/safety-v1", "version": "sha256:dataset-v1"},
                "scorers": [{"kind": "rouge", "version": "1"}],
                "thresholds": {"quality_min": 0.8, "latency_ms_max": 1500, "cost_micros_max": 9000},
                "deterministic_assertions": ["NO_SECRET_LEAK", "NO_DISABLED_TOOL"],
            },
        )

        assert created.status_code == 201
        assert created.json()["version"] == 1
        assert created.json()["dataset"]["version"] == "sha256:dataset-v1"
        assert created.json()["deterministic_assertions"] == [
            "NO_SECRET_LEAK",
            "NO_DISABLED_TOOL",
        ]


def test_eval_run_snapshots_release_dependencies_and_fails_closed_on_safety() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
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
            json={"slug": f"tenant-{uuid4().hex[:8]}", "name": "Eval Tenant"},
        ).json()
        application = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "eval-run-agent", "name": "Eval Run Agent"},
        ).json()
        release_id = asyncio.run(_seed_release(tenant["id"], application["id"]))
        asyncio.run(_seed_eval_dependencies(tenant["id"]))
        suite = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/eval-suites",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "slug": "production-safety",
                "dataset": {"ref": "artifact://eval/safety-v1", "version": "sha256:dataset-v1"},
                "scorers": [{"kind": "rouge", "version": "1"}],
                "thresholds": {"quality_min": 0.8, "latency_ms_max": 1500, "cost_micros_max": 9000},
                "deterministic_assertions": ["NO_SECRET_LEAK", "NO_DISABLED_TOOL"],
            },
        ).json()

        run = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/eval-runs",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "suite_id": suite["id"],
                "release_id": release_id,
                "environment": "PRODUCTION",
                "evidence": {
                    "assertions": {"NO_SECRET_LEAK": True, "NO_DISABLED_TOOL": False},
                    "metrics": {"quality": 0.95, "latency_ms": 200, "cost_micros": 1000},
                },
            },
        )

        assert run.status_code == 201
        assert run.json()["status"] == "FAILED"
        assert run.json()["dependency_snapshot"]["sdk_version"] == "1.1.19"
        assert run.json()["dependency_snapshot"]["tools"] == ["search"]
        assert run.json()["dependency_snapshot"]["tool_versions"] == [
            {"name": "search", "version": 2}
        ]
        assert run.json()["dependency_snapshot"]["policy_versions"] == [
            {"version": 7, "status": "ACTIVE"}
        ]


def test_production_approval_requires_a_passing_eval_run() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
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
            json={"slug": f"tenant-{uuid4().hex[:8]}", "name": "Eval Gate Tenant"},
        ).json()
        application = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "gated-agent", "name": "Gated Agent"},
        ).json()
        release_id = asyncio.run(_seed_release(tenant["id"], application["id"]))
        deployment = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/deployments",
            headers={"Idempotency-Key": str(uuid4())},
            json={"environment": "PRODUCTION", "release_id": release_id, "rollout_percentage": 100},
        ).json()
        approver = {"Authorization": f"Bearer {asyncio.run(_seed_tenant_developer(tenant['id']))}"}

        blocked = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/deployments/{deployment['id']}/approve",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"', **approver},
        )

        assert blocked.status_code == 409
        assert blocked.json()["error"]["message"] == "EVAL_RUN_REQUIRED"

        suite = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/eval-suites",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "slug": "production-safety",
                "dataset": {"ref": "artifact://eval/safety-v1", "version": "sha256:dataset-v1"},
                "scorers": [{"kind": "rouge", "version": "1"}],
                "thresholds": {"quality_min": 0.8, "latency_ms_max": 1500, "cost_micros_max": 9000},
                "deterministic_assertions": ["NO_SECRET_LEAK", "NO_DISABLED_TOOL"],
            },
        ).json()
        passed = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/eval-runs",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "suite_id": suite["id"],
                "release_id": release_id,
                "environment": "PRODUCTION",
                "evidence": {
                    "assertions": {"NO_SECRET_LEAK": True, "NO_DISABLED_TOOL": True},
                    "metrics": {"quality": 0.95, "latency_ms": 200, "cost_micros": 1000},
                },
            },
        )
        assert passed.json()["status"] == "PASSED"
        approved = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/deployments/{deployment['id']}/approve",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"', **approver},
        )
        assert approved.status_code == 200
        assert approved.json()["status"] == "ACTIVE"


def test_canary_threshold_failure_halts_new_deployment_and_restores_previous_route() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
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
            json={"slug": f"tenant-{uuid4().hex[:8]}", "name": "Canary Tenant"},
        ).json()
        application = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "canary-agent", "name": "Canary Agent"},
        ).json()
        previous_release_id = asyncio.run(_seed_release(tenant["id"], application["id"]))
        asyncio.run(
            _seed_active_production_deployment(tenant["id"], application["id"], previous_release_id)
        )
        release_id = asyncio.run(_seed_release(tenant["id"], application["id"], version=2))
        suite = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/eval-suites",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "slug": "canary-safety",
                "dataset": {"ref": "artifact://eval/canary-v1", "version": "sha256:dataset-v1"},
                "scorers": [{"kind": "rouge", "version": "1"}],
                "thresholds": {"quality_min": 0.8, "latency_ms_max": 1500, "cost_micros_max": 9000},
                "deterministic_assertions": ["NO_SECRET_LEAK"],
            },
        ).json()
        run = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/eval-runs",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "suite_id": suite["id"],
                "release_id": release_id,
                "environment": "PRODUCTION",
                "evidence": {
                    "assertions": {"NO_SECRET_LEAK": True},
                    "metrics": {"quality": 0.95, "latency_ms": 200, "cost_micros": 1000},
                },
            },
        ).json()
        deployment = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/deployments",
            headers={"Idempotency-Key": str(uuid4())},
            json={"environment": "PRODUCTION", "release_id": release_id, "rollout_percentage": 10},
        ).json()
        approver = {"Authorization": f"Bearer {asyncio.run(_seed_tenant_developer(tenant['id']))}"}
        approved = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/deployments/{deployment['id']}/approve",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"2"', **approver},
        )
        assert approved.status_code == 200

        observation = client.post(
            f"/api/v1/tenants/{tenant['id']}/agent-applications/{application['id']}/deployments/{deployment['id']}/eval-canary-observations",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "eval_run_id": run["id"],
                "metrics": {"quality": 0.5, "latency_ms": 200, "cost_micros": 1000},
            },
        )

        assert observation.status_code == 201
        assert observation.json()["decision"] == "HALTED"
        assert (
            asyncio.run(_resolve_production_release(tenant["id"], application["id"]))
            == previous_release_id
        )
