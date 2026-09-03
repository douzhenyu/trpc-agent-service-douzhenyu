from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.auth import Principal, encode_session
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.database_migrations import apply_migrations
from trpc_service.llm_gateway import (
    DatabaseModelProfileResolver,
    DataClassification,
    ModelProfile,
)

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

pytestmark = pytest.mark.integration


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.model_profile, tenant.agent_draft, tenant.agent_application, "
            "platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
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


def _login_and_create_tenant(client: TestClient) -> str:
    login = client.post(
        "/api/v1/auth/emergency/session",
        json={"username": "break-glass", "password": "correct-horse"},
    )
    assert login.status_code == 200
    tenant = client.post(
        "/api/v1/tenants",
        headers={"Idempotency-Key": str(uuid4())},
        json={"slug": f"tenant-{uuid4().hex[:8]}", "name": "Agent Tenant"},
    )
    assert tenant.status_code == 201
    return str(tenant.json()["id"])


async def _seed_tenant_developer(tenant_id: str) -> tuple[str, str]:
    user_id, member_id, role_id = uuid4(), uuid4(), uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            """INSERT INTO platform.platform_user (id,issuer,subject,display_name)
            VALUES ($1,'https://identity.example.test',$2,'Agent Developer')""",
            user_id,
            f"developer-{user_id}",
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
    token = encode_session(_settings(), Principal(str(user_id), "oidc", frozenset()))
    return token, str(user_id)


def test_platform_admin_can_create_and_read_a_tenant_agent_application() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        created = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "slug": "support-agent",
                "name": "Support Agent",
                "description": "Answers internal support questions",
            },
        )

        assert created.status_code == 201
        assert created.headers["etag"] == '"1"'
        assert {
            key: created.json()[key]
            for key in ("tenant_id", "slug", "name", "description", "version")
        } == {
            "tenant_id": tenant_id,
            "slug": "support-agent",
            "name": "Support Agent",
            "description": "Answers internal support questions",
            "version": 1,
        }

        application_id = created.json()["id"]
        fetched = client.get(f"/api/v1/tenants/{tenant_id}/agent-applications/{application_id}")
        listing = client.get(f"/api/v1/tenants/{tenant_id}/agent-applications")

        assert fetched.status_code == 200
        assert fetched.headers["etag"] == '"1"'
        assert fetched.json() == created.json()
        assert listing.status_code == 200
        assert listing.json() == {"items": [created.json()], "next_cursor": None}


def test_agent_application_updates_are_versioned_and_delete_is_idempotent() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        created = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "writer", "name": "Writer"},
        ).json()
        resource = f"/api/v1/tenants/{tenant_id}/agent-applications/{created['id']}"
        update_key = str(uuid4())
        updated = client.patch(
            resource,
            headers={"Idempotency-Key": update_key, "If-Match": '"1"'},
            json={"name": "Editorial Writer", "description": "Drafts articles"},
        )
        replayed = client.patch(
            resource,
            headers={"Idempotency-Key": update_key, "If-Match": '"1"'},
            json={"name": "Editorial Writer", "description": "Drafts articles"},
        )
        stale = client.patch(
            resource,
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
            json={"name": "Stale Writer"},
        )

        assert updated.status_code == replayed.status_code == 200
        assert updated.headers["etag"] == replayed.headers["etag"] == '"2"'
        assert replayed.headers["idempotency-replayed"] == "true"
        assert updated.json()["name"] == "Editorial Writer"
        assert updated.json()["version"] == 2
        assert stale.status_code == 412
        assert stale.json()["error"]["code"] == "VERSION_MISMATCH"

        delete_key = str(uuid4())
        deleted = client.delete(
            resource,
            headers={"Idempotency-Key": delete_key, "If-Match": '"2"'},
        )
        replayed_delete = client.delete(
            resource,
            headers={"Idempotency-Key": delete_key, "If-Match": '"2"'},
        )

        assert deleted.status_code == replayed_delete.status_code == 204
        assert replayed_delete.headers["idempotency-replayed"] == "true"
        assert client.get(resource).status_code == 404


def test_agent_draft_crud_never_exposes_a_production_traffic_state() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        application = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "assistant", "name": "Assistant"},
        ).json()
        draft_url = f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/draft"
        created = client.put(
            draft_url,
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "instructions": "Answer with cited internal sources.",
                "model_alias": "balanced",
                "tool_aliases": ["search"],
                "knowledge_refs": ["support-handbook"],
                "governance_policy_ref": "standard-policy",
            },
        )

        assert created.status_code == 201
        assert created.headers["etag"] == '"1"'
        assert created.json()["lifecycle"] == "DRAFT"
        assert created.json()["serves_production_traffic"] is False
        assert client.get(draft_url).json() == created.json()

        update_key = str(uuid4())
        updated = client.patch(
            draft_url,
            headers={"Idempotency-Key": update_key, "If-Match": '"1"'},
            json={"instructions": "Answer briefly and cite internal sources."},
        )
        replayed = client.patch(
            draft_url,
            headers={"Idempotency-Key": update_key, "If-Match": '"1"'},
            json={"instructions": "Answer briefly and cite internal sources."},
        )
        stale = client.patch(
            draft_url,
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
            json={"instructions": "Overwrite concurrent changes."},
        )
        assert updated.status_code == 200
        assert updated.headers["etag"] == '"2"'
        assert updated.json()["version"] == 2
        assert updated.json()["instructions"].startswith("Answer briefly")
        assert replayed.json() == updated.json()
        assert replayed.headers["idempotency-replayed"] == "true"
        assert stale.status_code == 412
        assert stale.json()["error"]["code"] == "VERSION_MISMATCH"

        deleted = client.delete(
            draft_url,
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"2"'},
        )
        assert deleted.status_code == 204
        assert client.get(draft_url).status_code == 404
        assert (
            client.get(
                f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}"
            ).status_code
            == 200
        )


def test_draft_validation_returns_stable_locatable_issues_for_a_pinned_version() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        application = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "validator", "name": "Validator"},
        ).json()
        draft_url = f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/draft"
        client.put(
            draft_url,
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "instructions": "   ",
                "model_alias": "bad alias",
                "tool_aliases": ["search", "search"],
                "knowledge_refs": ["handbook", "handbook"],
                "governance_policy_ref": "",
            },
        )

        invalid = client.post(
            f"{draft_url}/validate",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
        )
        assert invalid.status_code == 200
        assert invalid.headers["etag"] == '"1"'
        assert invalid.json() == {
            "valid": False,
            "draft_version": 1,
            "issues": [
                {
                    "code": "DRAFT_INSTRUCTIONS_REQUIRED",
                    "path": "/instructions",
                    "message": "Instructions must not be blank.",
                },
                {
                    "code": "DRAFT_MODEL_ALIAS_INVALID",
                    "path": "/model_alias",
                    "message": "Model alias must be a stable resource name.",
                },
                {
                    "code": "DRAFT_DUPLICATE_TOOL_ALIAS",
                    "path": "/tool_aliases/1",
                    "message": "Tool alias duplicates /tool_aliases/0.",
                },
                {
                    "code": "DRAFT_DUPLICATE_KNOWLEDGE_REF",
                    "path": "/knowledge_refs/1",
                    "message": "Knowledge reference duplicates /knowledge_refs/0.",
                },
                {
                    "code": "DRAFT_GOVERNANCE_POLICY_REF_INVALID",
                    "path": "/governance_policy_ref",
                    "message": "Governance policy reference must be a stable resource name.",
                },
            ],
        }

        client.patch(
            draft_url,
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
            json={
                "instructions": "Answer helpfully.",
                "model_alias": "balanced",
                "tool_aliases": ["search"],
                "knowledge_refs": ["handbook"],
                "governance_policy_ref": "standard-policy",
            },
        )
        valid = client.post(
            f"{draft_url}/validate",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"2"'},
        )
        stale = client.post(
            f"{draft_url}/validate",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
        )
        audit = client.get("/api/v1/audit-events", params={"tenant_id": tenant_id})

        assert valid.json() == {"valid": True, "draft_version": 2, "issues": []}
        assert stale.status_code == 412
        assert stale.json()["error"]["code"] == "VERSION_MISMATCH"
        assert {event["action"] for event in audit.json()["items"]} >= {
            "agent_draft.create",
            "agent_draft.update",
            "agent_draft.validate",
        }


def test_tenant_developer_is_confined_to_its_tenant_and_denials_are_audited() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_a = _login_and_create_tenant(client)
        tenant_b = _login_and_create_tenant(client)
        token, developer_id = asyncio.run(_seed_tenant_developer(tenant_a))
        developer = {"Authorization": f"Bearer {token}"}

        own = client.post(
            f"/api/v1/tenants/{tenant_a}/agent-applications",
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json={"slug": "owned", "name": "Owned Agent"},
        )
        own_draft_url = f"/api/v1/tenants/{tenant_a}/agent-applications/{own.json()['id']}/draft"
        own_draft = client.put(
            own_draft_url,
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json={"instructions": "Stay within the tenant.", "model_alias": "balanced"},
        )
        own_listing = client.get(
            f"/api/v1/tenants/{tenant_a}/agent-applications", headers=developer
        )
        visible_tenants = client.get("/api/v1/tenants", headers=developer)
        foreign_listing = client.get(
            f"/api/v1/tenants/{tenant_b}/agent-applications", headers=developer
        )
        foreign_create = client.post(
            f"/api/v1/tenants/{tenant_b}/agent-applications",
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json={"slug": "intruder", "name": "Intruder Agent"},
        )
        foreign_draft_url = (
            f"/api/v1/tenants/{tenant_b}/agent-applications/{own.json()['id']}/draft"
        )
        foreign_draft_responses = {
            "agent_draft.get": client.get(foreign_draft_url, headers=developer),
            "agent_draft.create": client.put(
                foreign_draft_url,
                headers={**developer, "Idempotency-Key": str(uuid4())},
                json={"instructions": "Intrude.", "model_alias": "balanced"},
            ),
            "agent_draft.update": client.patch(
                foreign_draft_url,
                headers={
                    **developer,
                    "Idempotency-Key": str(uuid4()),
                    "If-Match": '"1"',
                },
                json={"instructions": "Intrude again."},
            ),
            "agent_draft.delete": client.delete(
                foreign_draft_url,
                headers={
                    **developer,
                    "Idempotency-Key": str(uuid4()),
                    "If-Match": '"1"',
                },
            ),
            "agent_draft.validate": client.post(
                f"{foreign_draft_url}/validate",
                headers={
                    **developer,
                    "Idempotency-Key": str(uuid4()),
                    "If-Match": '"1"',
                },
            ),
        }

        assert own.status_code == 201
        assert own_draft.status_code == 201
        assert [item["id"] for item in own_listing.json()["items"]] == [own.json()["id"]]
        assert [item["id"] for item in visible_tenants.json()["items"]] == [tenant_a]
        assert foreign_listing.status_code == foreign_create.status_code == 403
        assert foreign_listing.json()["error"]["code"] == "FORBIDDEN"
        assert foreign_create.json()["error"]["code"] == "FORBIDDEN"
        assert {
            action: (response.status_code, response.json()["error"]["code"])
            for action, response in foreign_draft_responses.items()
        } == {action: (403, "FORBIDDEN") for action in foreign_draft_responses}

        audit = client.get("/api/v1/audit-events", params={"tenant_id": tenant_b})
        assert {
            event["action"]
            for event in audit.json()["items"]
            if event["decision"] == "DENY" and event["actor"] == developer_id
        } >= {"agent_application.list", "agent_application.create"}
        assert {
            event["action"]
            for event in audit.json()["items"]
            if event["decision"] == "DENY" and event["actor"] == developer_id
        } >= set(foreign_draft_responses)


def test_unknown_tenant_is_denied_without_breaking_the_public_error_contract() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        own_tenant = _login_and_create_tenant(client)
        token, developer_id = asyncio.run(_seed_tenant_developer(own_tenant))
        missing_tenant = str(uuid4())

        response = client.get(
            f"/api/v1/tenants/{missing_tenant}/agent-applications",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 403
        assert response.json()["error"]["code"] == "FORBIDDEN"
        audit = client.get("/api/v1/audit-events")
        denied = next(
            event
            for event in audit.json()["items"]
            if event["decision"] == "DENY" and event["actor"] == developer_id
        )
        assert denied["tenant_id"] is None
        assert denied["details"]["requested_tenant_id"] == missing_tenant


def test_draft_reference_items_are_bounded_at_the_public_api() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        application = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "bounded", "name": "Bounded Draft"},
        ).json()

        response = client.put(
            f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/draft",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "tool_aliases": ["t" * 129],
                "knowledge_refs": ["k" * 129],
            },
        )

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_agent_openapi_contract_declares_stable_errors_and_version_preconditions() -> None:
    schema = create_app(_settings()).openapi()
    paths = schema["paths"]
    collection = paths["/api/v1/tenants/{tenant_id}/agent-applications"]
    application = paths["/api/v1/tenants/{tenant_id}/agent-applications/{application_id}"]
    validation = paths[
        "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/draft/validate"
    ]

    assert set(collection["post"]["responses"]) == {
        "201",
        "401",
        "403",
        "404",
        "409",
        "422",
        "500",
    }
    assert set(application["patch"]["responses"]) == {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "412",
        "422",
        "500",
    }
    assert set(validation["post"]["responses"]) == {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "412",
        "422",
        "500",
    }
    if_match = next(
        parameter
        for parameter in application["patch"]["parameters"]
        if parameter["name"] == "If-Match"
    )
    assert if_match["required"] is True
    assert schema["components"]["schemas"]["ErrorDetail"]["properties"]["code"]["enum"]


def test_tenant_admin_manages_model_profiles_without_exposing_secret_contents() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        collection = f"/api/v1/tenants/{tenant_id}/model-profiles"
        secret_ref = f"vault://tenant/{tenant_id}/llm/openai#api_key"
        created = client.post(
            collection,
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "alias": "balanced",
                "provider_model": "fake-balanced",
                "endpoint_url": "http://fake-llm.internal/v1/chat/completions",
                "secret_ref": secret_ref,
                "data_classification": "CONFIDENTIAL",
                "region": "cn-north-1",
                "fallback_aliases": ["economy"],
                "requests_per_minute": 120,
            },
        )

        assert created.status_code == 201
        assert created.headers["etag"] == '"1"'
        assert created.json() == {
            **created.json(),
            "tenant_id": tenant_id,
            "alias": "balanced",
            "provider_model": "fake-balanced",
            "endpoint_url": "http://fake-llm.internal/v1/chat/completions",
            "secret_ref": secret_ref,
            "data_classification": "CONFIDENTIAL",
            "region": "cn-north-1",
            "fallback_aliases": ["economy"],
            "requests_per_minute": 120,
            "version": 1,
        }
        assert "api_key" not in {key.lower() for key in created.json()}

        fetched = client.get(f"{collection}/balanced")
        assert fetched.status_code == 200
        assert fetched.json() == created.json()

        async def resolve_runtime_profile() -> ModelProfile | None:
            database = Database(APP_URL)
            await database.open()
            try:
                return await DatabaseModelProfileResolver(database).resolve(tenant_id, "balanced")
            finally:
                await database.close()

        runtime_profile = asyncio.run(resolve_runtime_profile())
        assert runtime_profile is not None
        assert runtime_profile.secret_ref == secret_ref
        assert runtime_profile.data_classification is DataClassification.CONFIDENTIAL

        updated = client.patch(
            f"{collection}/balanced",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
            json={"fallback_aliases": ["premium"], "requests_per_minute": 60},
        )
        assert updated.status_code == 200
        assert updated.headers["etag"] == '"2"'
        assert updated.json()["fallback_aliases"] == ["premium"]
        assert updated.json()["requests_per_minute"] == 60

        audit = client.get("/api/v1/audit-events", params={"tenant_id": tenant_id})
        events = [
            event for event in audit.json()["items"] if event["action"].startswith("model_profile.")
        ]
        assert {event["action"] for event in events} == {
            "model_profile.create",
            "model_profile.update",
        }
        assert secret_ref not in str(events)
