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
from trpc_service.agent_worker import DatabaseDeploymentRouteResolver, DatabaseReleaseRouteResolver
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
            "TRUNCATE tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, "
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


def test_model_profile_rejects_endpoint_credentials() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        response = client.post(
            f"/api/v1/tenants/{tenant_id}/model-profiles",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "alias": "balanced",
                "provider_model": "fake-balanced",
                "endpoint_url": "https://provider-secret@example.test/v1/chat/completions?api_key=leak",
                "secret_ref": f"vault://tenant/{tenant_id}/llm/openai#api_key",
                "data_classification": "CONFIDENTIAL",
                "region": "cn-north-1",
            },
        )

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_published_agent_release_is_the_runtime_source_of_allowed_model_fallbacks() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        profiles = f"/api/v1/tenants/{tenant_id}/model-profiles"
        for alias, fallbacks in (("balanced", ["economy"]), ("economy", [])):
            response = client.post(
                profiles,
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "alias": alias,
                    "provider_model": f"fake-{alias}",
                    "endpoint_url": "https://fake-llm.internal/v1/chat/completions",
                    "secret_ref": f"vault://tenant/{tenant_id}/llm/{alias}#api_key",
                    "data_classification": "CONFIDENTIAL",
                    "region": "cn-north-1",
                    "fallback_aliases": fallbacks,
                },
            )
            assert response.status_code == 201
        application = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "support", "name": "Support"},
        ).json()
        draft_url = f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/draft"
        assert (
            client.put(
                draft_url,
                headers={"Idempotency-Key": str(uuid4())},
                json={"instructions": "Answer helpfully.", "model_alias": "balanced"},
            ).status_code
            == 201
        )

        released = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/releases",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "data_classification": "INTERNAL",
                "region": "cn-north-1",
                "fallback_aliases": ["economy"],
            },
        )

        assert released.status_code == 201
        assert released.json()["model_alias"] == "balanced"
        assert released.json()["fallback_aliases"] == ["economy"]
        changed = client.patch(
            f"{profiles}/balanced",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
            json={"provider_model": "changed-after-release"},
        )
        assert changed.status_code == 200

        async def resolve_route() -> object:
            database = Database(APP_URL)
            await database.open()
            try:
                return await DatabaseReleaseRouteResolver(database).resolve(
                    tenant_id, released.json()["id"]
                )
            finally:
                await database.close()

        route = asyncio.run(resolve_route())
        assert route is not None
        assert route.model_alias == "balanced"
        assert route.allowed_fallback_aliases == frozenset({"economy"})
        assert route.profile_snapshots[0].provider_model == "fake-balanced"
        audit = client.get("/api/v1/audit-events", params={"tenant_id": tenant_id})
        assert any(event["action"] == "agent_release.publish" for event in audit.json()["items"])


def test_published_agent_releases_preserve_a_complete_versioned_draft_snapshot() -> None:
    """The public release API is the only transition out of a mutable Draft."""
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        profiles = f"/api/v1/tenants/{tenant_id}/model-profiles"
        assert (
            client.post(
                profiles,
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "alias": "balanced",
                    "provider_model": "fake-balanced",
                    "endpoint_url": "https://fake-llm.internal/v1/chat/completions",
                    "secret_ref": f"vault://tenant/{tenant_id}/llm/balanced#api_key",
                    "data_classification": "CONFIDENTIAL",
                    "region": "cn-north-1",
                },
            ).status_code
            == 201
        )
        application = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "releaseable", "name": "Releaseable Agent"},
        ).json()
        draft_url = f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/draft"
        assert (
            client.put(
                draft_url,
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "instructions": "Answer only with cited sources.",
                    "model_alias": "balanced",
                    "tool_aliases": ["search"],
                    "knowledge_refs": ["handbook"],
                    "governance_policy_ref": "standard-policy",
                },
            ).status_code
            == 201
        )

        releases_url = (
            f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/releases"
        )
        first = client.post(
            releases_url,
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "data_classification": "INTERNAL",
                "region": "cn-north-1",
                "fallback_aliases": [],
            },
        )

        assert first.status_code == 201
        assert first.json() == {
            **first.json(),
            "tenant_id": tenant_id,
            "application_id": application["id"],
            "version": 1,
            "source": {
                "draft_version": 1,
                "actor": "emergency:break-glass",
                "kind": "DRAFT",
            },
            "draft_snapshot": {
                "instructions": "Answer only with cited sources.",
                "model_alias": "balanced",
                "tool_aliases": ["search"],
                "knowledge_refs": ["handbook"],
                "governance_policy_ref": "standard-policy",
                "version": 1,
            },
        }
        assert len(first.json()["content_hash"]) == 64
        assert set(first.json()["content_hash"]) <= set("0123456789abcdef")

        assert (
            client.patch(
                draft_url,
                headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
                json={"instructions": "The mutable Draft changed after publishing."},
            ).status_code
            == 200
        )
        second = client.post(
            releases_url,
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "data_classification": "INTERNAL",
                "region": "cn-north-1",
                "fallback_aliases": [],
            },
        )
        assert second.status_code == 201
        assert second.json()["version"] == 2
        assert second.json()["source"]["draft_version"] == 2
        assert second.json()["draft_snapshot"]["instructions"].startswith("The mutable")

        listing = client.get(releases_url)
        assert listing.status_code == 200
        assert [item["id"] for item in listing.json()["items"]] == [
            first.json()["id"],
            second.json()["id"],
        ]
        assert listing.json()["items"][0]["draft_snapshot"] == first.json()["draft_snapshot"]

        async def direct_release_mutation() -> None:
            connection = await asyncpg.connect(APP_URL)
            try:
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(
                        "UPDATE tenant.agent_release SET content_hash=$1 WHERE id=$2",
                        "f" * 64,
                        first.json()["id"],
                    )
            finally:
                await connection.close()

        asyncio.run(direct_release_mutation())


def test_environment_deployments_require_production_approval_and_route_sessions_stably() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        developer_token, developer_id = asyncio.run(_seed_tenant_developer(tenant_id))
        approver_token, approver_id = asyncio.run(_seed_tenant_developer(tenant_id))
        profiles = f"/api/v1/tenants/{tenant_id}/model-profiles"
        assert (
            client.post(
                profiles,
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "alias": "balanced",
                    "provider_model": "fake-balanced",
                    "endpoint_url": "https://fake-llm.internal/v1/chat/completions",
                    "secret_ref": f"vault://tenant/{tenant_id}/llm/balanced#api_key",
                    "data_classification": "CONFIDENTIAL",
                    "region": "cn-north-1",
                },
            ).status_code
            == 201
        )
        application = client.post(
            f"/api/v1/tenants/{tenant_id}/agent-applications",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "deployed", "name": "Deployed Agent"},
        ).json()
        draft_url = f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/draft"
        assert (
            client.put(
                draft_url,
                headers={"Idempotency-Key": str(uuid4())},
                json={"instructions": "Release one.", "model_alias": "balanced"},
            ).status_code
            == 201
        )
        releases_url = (
            f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/releases"
        )
        release_payload = {
            "data_classification": "INTERNAL",
            "region": "cn-north-1",
            "fallback_aliases": [],
        }
        first_release = client.post(
            releases_url,
            headers={"Idempotency-Key": str(uuid4())},
            json=release_payload,
        ).json()
        assert (
            client.patch(
                draft_url,
                headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
                json={"instructions": "Release two."},
            ).status_code
            == 200
        )
        second_release = client.post(
            releases_url,
            headers={"Idempotency-Key": str(uuid4())},
            json=release_payload,
        ).json()
        deployments_url = (
            f"/api/v1/tenants/{tenant_id}/agent-applications/{application['id']}/deployments"
        )
        developer = {"Authorization": f"Bearer {developer_token}"}

        baseline = client.post(
            deployments_url,
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json={
                "environment": "STAGING",
                "release_id": first_release["id"],
                "rollout_percentage": 100,
            },
        )
        missing_precondition = client.post(
            deployments_url,
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json={
                "environment": "STAGING",
                "release_id": second_release["id"],
                "rollout_percentage": 25,
            },
        )
        canary = client.post(
            deployments_url,
            headers={
                **developer,
                "Idempotency-Key": str(uuid4()),
                "If-Match": f'"{baseline.json()["version"]}"',
            },
            json={
                "environment": "STAGING",
                "release_id": second_release["id"],
                "rollout_percentage": 25,
            },
        )

        assert baseline.status_code == canary.status_code == 201
        assert missing_precondition.status_code == 428
        assert canary.headers["ETag"] == '"2"'
        assert baseline.json()["status"] == canary.json()["status"] == "ACTIVE"
        assert canary.json()["previous_release_id"] == first_release["id"]
        assert canary.json()["rollout_percentage"] == 25

        stale_deployment = client.post(
            deployments_url,
            headers={
                **developer,
                "Idempotency-Key": str(uuid4()),
                "If-Match": f'"{baseline.json()["version"]}"',
            },
            json={
                "environment": "STAGING",
                "release_id": first_release["id"],
                "rollout_percentage": 100,
            },
        )
        assert stale_deployment.status_code == 412

        async def routes_for_stable_sessions() -> dict[str, str]:
            database = Database(APP_URL)
            await database.open()
            try:
                resolver = DatabaseDeploymentRouteResolver(database)
                return {
                    f"session-{index}": str(
                        await resolver.resolve(
                            tenant_id,
                            application["id"],
                            "STAGING",
                            f"session-{index}",
                        )
                    )
                    for index in range(200)
                }
            finally:
                await database.close()

        first_routes = asyncio.run(routes_for_stable_sessions())
        assert set(first_routes.values()) == {first_release["id"], second_release["id"]}
        assert asyncio.run(routes_for_stable_sessions()) == first_routes

        rollback = client.post(
            f"{deployments_url}/{canary.json()['id']}/rollback",
            headers={
                **developer,
                "Idempotency-Key": str(uuid4()),
                "If-Match": f'"{canary.json()["version"]}"',
            },
            json={"release_id": first_release["id"]},
        )
        assert rollback.status_code == 201
        assert rollback.headers["ETag"] == '"3"'
        assert rollback.json()["release_id"] == first_release["id"]
        assert set(asyncio.run(routes_for_stable_sessions()).values()) == {first_release["id"]}
        assert [item["content_hash"] for item in client.get(releases_url).json()["items"]] == [
            first_release["content_hash"],
            second_release["content_hash"],
        ]

        stale_rollback = client.post(
            f"{deployments_url}/{canary.json()['id']}/rollback",
            headers={
                **developer,
                "Idempotency-Key": str(uuid4()),
                "If-Match": f'"{canary.json()["version"]}"',
            },
            json={"release_id": second_release["id"]},
        )
        assert stale_rollback.status_code == 412

        production = client.post(
            deployments_url,
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json={
                "environment": "PRODUCTION",
                "release_id": second_release["id"],
                "rollout_percentage": 100,
            },
        )
        assert production.status_code == 202
        assert production.json()["status"] == "PENDING_APPROVAL"
        assert production.json()["initiator"] == developer_id

        own_approval = client.post(
            f"{deployments_url}/{production.json()['id']}/approve",
            headers={**developer, "Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
        )
        approved = client.post(
            f"{deployments_url}/{production.json()['id']}/approve",
            headers={
                "Authorization": f"Bearer {approver_token}",
                "Idempotency-Key": str(uuid4()),
                "If-Match": '"1"',
            },
        )
        assert own_approval.status_code == 409
        assert approved.status_code == 200
        assert approved.json()["status"] == "ACTIVE"
        assert approved.json()["approver"] == approver_id
        deployment_history = client.get(deployments_url, headers=developer)
        assert deployment_history.status_code == 200
        assert [item["id"] for item in deployment_history.json()["items"]] == [
            baseline.json()["id"],
            canary.json()["id"],
            rollback.json()["id"],
            production.json()["id"],
        ]


def test_agent_release_and_deployment_commands_replay_idempotently() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        developer_token, _developer_id = asyncio.run(_seed_tenant_developer(tenant_id))
        approver_token, _approver_id = asyncio.run(_seed_tenant_developer(tenant_id))
        developer = {"Authorization": f"Bearer {developer_token}"}
        app_key = str(uuid4())
        applications_url = f"/api/v1/tenants/{tenant_id}/agent-applications"
        application = client.post(
            applications_url,
            headers={"Idempotency-Key": app_key},
            json={"slug": "replayable", "name": "Replayable Agent"},
        )
        replayed_application = client.post(
            applications_url,
            headers={"Idempotency-Key": app_key},
            json={"slug": "replayable", "name": "Replayable Agent"},
        )
        assert replayed_application.headers["Idempotency-Replayed"] == "true"
        application_id = application.json()["id"]

        profiles_url = f"/api/v1/tenants/{tenant_id}/model-profiles"
        assert (
            client.post(
                profiles_url,
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "alias": "balanced",
                    "provider_model": "fake-balanced",
                    "endpoint_url": "https://fake-llm.internal/v1/chat/completions",
                    "secret_ref": f"vault://tenant/{tenant_id}/llm/balanced#api_key",
                    "data_classification": "CONFIDENTIAL",
                    "region": "cn-north-1",
                },
            ).status_code
            == 201
        )
        draft_url = f"{applications_url}/{application_id}/draft"
        draft_key = str(uuid4())
        draft_payload = {"instructions": "Replay safely.", "model_alias": "balanced"}
        assert (
            client.put(
                draft_url, headers={"Idempotency-Key": draft_key}, json=draft_payload
            ).status_code
            == 201
        )
        assert (
            client.put(
                draft_url, headers={"Idempotency-Key": draft_key}, json=draft_payload
            ).headers["Idempotency-Replayed"]
            == "true"
        )

        releases_url = f"{applications_url}/{application_id}/releases"
        release_key = str(uuid4())
        release_payload = {
            "data_classification": "INTERNAL",
            "region": "cn-north-1",
            "fallback_aliases": [],
        }
        release = client.post(
            releases_url,
            headers={**developer, "Idempotency-Key": release_key},
            json=release_payload,
        )
        assert release.status_code == 201
        assert (
            client.post(
                releases_url,
                headers={**developer, "Idempotency-Key": release_key},
                json=release_payload,
            ).json()["id"]
            == release.json()["id"]
        )
        assert client.get(releases_url, headers=developer).json()["items"] == [release.json()]

        deployments_url = f"{applications_url}/{application_id}/deployments"
        production_key = str(uuid4())
        production_payload = {
            "environment": "PRODUCTION",
            "release_id": release.json()["id"],
            "rollout_percentage": 100,
        }
        production = client.post(
            deployments_url,
            headers={**developer, "Idempotency-Key": production_key},
            json=production_payload,
        )
        assert production.status_code == 202
        assert (
            client.post(
                deployments_url,
                headers={**developer, "Idempotency-Key": production_key},
                json=production_payload,
            ).headers["Idempotency-Replayed"]
            == "true"
        )
        approval_url = f"{deployments_url}/{production.json()['id']}/approve"
        approval_key = str(uuid4())
        approval_headers = {
            "Authorization": f"Bearer {approver_token}",
            "Idempotency-Key": approval_key,
            "If-Match": f'"{production.json()["version"]}"',
        }
        approved = client.post(approval_url, headers=approval_headers)
        assert approved.status_code == 200
        assert (
            client.post(approval_url, headers=approval_headers).headers["Idempotency-Replayed"]
            == "true"
        )

        rollback_key = str(uuid4())
        rollback_url = f"{deployments_url}/{approved.json()['id']}/rollback"
        rollback_headers = {
            **developer,
            "Idempotency-Key": rollback_key,
            "If-Match": f'"{approved.json()["version"]}"',
        }
        rollback = client.post(
            rollback_url,
            headers=rollback_headers,
            json={"release_id": release.json()["id"]},
        )
        assert rollback.status_code == 202
        assert (
            client.post(
                rollback_url,
                headers=rollback_headers,
                json={"release_id": release.json()["id"]},
            ).headers["Idempotency-Replayed"]
            == "true"
        )


def test_agent_release_and_deployment_api_reject_invalid_lifecycle_transitions() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        developer_token, _developer_id = asyncio.run(_seed_tenant_developer(tenant_id))
        developer = {"Authorization": f"Bearer {developer_token}"}
        applications_url = f"/api/v1/tenants/{tenant_id}/agent-applications"
        missing_id = str(uuid4())
        release_payload = {
            "data_classification": "INTERNAL",
            "region": "cn-north-1",
            "fallback_aliases": [],
        }

        assert (
            client.get(f"{applications_url}?cursor=not-a-uuid", headers=developer).status_code
            == 400
        )
        assert client.get(f"{applications_url}/{missing_id}", headers=developer).status_code == 404
        assert (
            client.get(f"{applications_url}/{missing_id}/draft", headers=developer).status_code
            == 404
        )
        assert (
            client.post(
                f"{applications_url}/{missing_id}/releases",
                headers={**developer, "Idempotency-Key": str(uuid4())},
                json=release_payload,
            ).status_code
            == 404
        )

        application = client.post(
            applications_url,
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "guarded", "name": "Guarded Agent"},
        ).json()
        application_id = application["id"]
        releases_url = f"{applications_url}/{application_id}/releases"
        assert (
            client.post(
                releases_url,
                headers={**developer, "Idempotency-Key": str(uuid4())},
                json=release_payload,
            ).status_code
            == 409
        )
        draft_url = f"{applications_url}/{application_id}/draft"
        assert (
            client.put(
                draft_url,
                headers={"Idempotency-Key": str(uuid4())},
                json={"instructions": "Guard transitions.", "model_alias": "balanced"},
            ).status_code
            == 201
        )
        assert (
            client.post(
                releases_url,
                headers={**developer, "Idempotency-Key": str(uuid4())},
                json=release_payload,
            ).status_code
            == 409
        )
        profiles_url = f"/api/v1/tenants/{tenant_id}/model-profiles"
        assert (
            client.post(
                profiles_url,
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "alias": "balanced",
                    "provider_model": "fake-balanced",
                    "endpoint_url": "https://fake-llm.internal/v1/chat/completions",
                    "secret_ref": f"vault://tenant/{tenant_id}/llm/balanced#api_key",
                    "data_classification": "CONFIDENTIAL",
                    "region": "cn-north-1",
                },
            ).status_code
            == 201
        )
        invalid_fallback = {**release_payload, "fallback_aliases": ["economy"]}
        assert (
            client.post(
                releases_url,
                headers={**developer, "Idempotency-Key": str(uuid4())},
                json=invalid_fallback,
            ).status_code
            == 422
        )
        release = client.post(
            releases_url,
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json=release_payload,
        ).json()

        deployments_url = f"{applications_url}/{application_id}/deployments"
        deployment_payload = {
            "environment": "STAGING",
            "release_id": release["id"],
            "rollout_percentage": 100,
        }
        assert (
            client.post(
                deployments_url,
                headers={**developer, "Idempotency-Key": str(uuid4()), "If-Match": "bad"},
                json=deployment_payload,
            ).status_code
            == 400
        )
        assert (
            client.post(
                f"{applications_url}/{missing_id}/deployments",
                headers={**developer, "Idempotency-Key": str(uuid4())},
                json=deployment_payload,
            ).status_code
            == 404
        )
        assert (
            client.post(
                deployments_url,
                headers={**developer, "Idempotency-Key": str(uuid4())},
                json={**deployment_payload, "rollout_percentage": 25},
            ).status_code
            == 409
        )
        deployment = client.post(
            deployments_url,
            headers={**developer, "Idempotency-Key": str(uuid4())},
            json=deployment_payload,
        ).json()
        assert (
            client.post(
                deployments_url,
                headers={
                    **developer,
                    "Idempotency-Key": str(uuid4()),
                    "If-Match": f'"{deployment["version"]}"',
                },
                json=deployment_payload,
            ).status_code
            == 409
        )
        assert (
            client.get(f"{deployments_url}?cursor=not-a-uuid", headers=developer).status_code == 400
        )
        assert (
            client.post(
                f"{deployments_url}/{deployment['id']}/approve",
                headers={**developer, "Idempotency-Key": str(uuid4()), "If-Match": "bad"},
            ).status_code
            == 400
        )
        assert (
            client.post(
                f"{deployments_url}/{deployment['id']}/rollback",
                headers={**developer, "Idempotency-Key": str(uuid4()), "If-Match": "bad"},
                json={"release_id": release["id"]},
            ).status_code
            == 400
        )


def test_model_profile_api_supports_versioned_update_delete_and_idempotent_replay() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        tenant_id = _login_and_create_tenant(client)
        profiles_url = f"/api/v1/tenants/{tenant_id}/model-profiles"
        profile_key = str(uuid4())
        payload = {
            "alias": "balanced",
            "provider_model": "fake-balanced",
            "endpoint_url": "https://fake-llm.internal/v1/chat/completions",
            "secret_ref": f"vault://tenant/{tenant_id}/llm/balanced#api_key",
            "data_classification": "CONFIDENTIAL",
            "region": "cn-north-1",
        }
        created = client.post(
            profiles_url,
            headers={"Idempotency-Key": profile_key},
            json=payload,
        )
        assert created.status_code == 201
        assert (
            client.post(
                profiles_url,
                headers={"Idempotency-Key": profile_key},
                json=payload,
            ).headers["Idempotency-Replayed"]
            == "true"
        )
        assert client.get(profiles_url).json()["items"] == [created.json()]
        assert client.get(f"{profiles_url}?cursor=not-a-uuid").status_code == 400
        assert client.get(f"{profiles_url}/missing").status_code == 404
        assert (
            client.patch(
                f"{profiles_url}/balanced",
                headers={"Idempotency-Key": str(uuid4()), "If-Match": "bad"},
                json={"provider_model": "next-balanced"},
            ).status_code
            == 400
        )
        update_key = str(uuid4())
        update_headers = {"Idempotency-Key": update_key, "If-Match": '"1"'}
        updated = client.patch(
            f"{profiles_url}/balanced",
            headers=update_headers,
            json={"provider_model": "next-balanced"},
        )
        assert updated.status_code == 200
        assert updated.json()["version"] == 2
        assert (
            client.patch(
                f"{profiles_url}/balanced",
                headers=update_headers,
                json={"provider_model": "next-balanced"},
            ).headers["Idempotency-Replayed"]
            == "true"
        )
        delete_key = str(uuid4())
        delete_headers = {"Idempotency-Key": delete_key, "If-Match": '"2"'}
        assert client.delete(f"{profiles_url}/balanced", headers=delete_headers).status_code == 204
        assert (
            client.delete(f"{profiles_url}/balanced", headers=delete_headers).headers[
                "Idempotency-Replayed"
            ]
            == "true"
        )
