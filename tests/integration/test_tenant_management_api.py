from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import asyncpg
import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.database import Connection, Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.database_migrations import apply_migrations

pytestmark = pytest.mark.integration

ADMIN_URL = os.getenv(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.getenv(
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
            "platform.platform_role_assignment, "
            "platform.platform_user, platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _denied_login_count() -> int:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        return int(
            await connection.fetchval(
                "SELECT count(*) FROM platform.audit_event "
                "WHERE action = 'auth.emergency.login' AND decision = 'DENY'"
            )
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


def test_emergency_admin_can_manage_tenants_and_audit_is_visible() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        login = client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        assert login.status_code == 200
        assert login.json()["auth_method"] == "emergency"

        create = client.post(
            "/api/v1/tenants",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "acme", "name": "Acme"},
        )
        assert create.status_code == 201
        assert create.headers["etag"] == '"1"'
        tenant_id = create.json()["id"]

        listing = client.get("/api/v1/tenants")
        assert listing.status_code == 200
        assert [item["id"] for item in listing.json()["items"]] == [tenant_id]

        audit = client.get("/api/v1/audit-events")
        assert audit.status_code == 200
        assert {(event["action"], event["auth_method"]) for event in audit.json()["items"]} >= {
            ("auth.emergency.login", "emergency"),
            ("tenant.create", "emergency"),
        }


def test_request_validation_and_unexpected_failures_use_the_stable_error_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        invalid = client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass"},
        )
        assert invalid.status_code == 422
        assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"

    @asynccontextmanager
    async def broken_transaction(_database: Database) -> AsyncIterator[Connection]:
        raise RuntimeError("database detail must not escape")
        yield  # pragma: no cover

    monkeypatch.setattr(Database, "transaction", broken_transaction)
    with TestClient(create_app(_settings()), raise_server_exceptions=False) as client:
        failed = client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        assert failed.status_code == 500
        assert failed.json() == {
            "error": {"code": "INTERNAL_ERROR", "message": "internal server error"}
        }


def test_tenant_create_replays_a_matching_idempotency_key_and_rejects_reuse() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        headers = {"Idempotency-Key": str(uuid4())}
        payload = {"slug": "idempotent", "name": "Idempotent Tenant"}

        created = client.post("/api/v1/tenants", headers=headers, json=payload)
        replayed = client.post("/api/v1/tenants", headers=headers, json=payload)
        conflicting = client.post(
            "/api/v1/tenants",
            headers=headers,
            json={"slug": "different", "name": "Different Tenant"},
        )

        assert created.status_code == replayed.status_code == 201
        assert replayed.json() == created.json()
        assert replayed.headers["Idempotency-Replayed"] == "true"
        assert conflicting.status_code == 409
        assert conflicting.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
        assert len(client.get("/api/v1/tenants").json()["items"]) == 1


def test_tenant_list_uses_an_opaque_cursor() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        for slug in ("cursor-a", "cursor-b"):
            response = client.post(
                "/api/v1/tenants",
                headers={"Idempotency-Key": str(uuid4())},
                json={"slug": slug, "name": slug},
            )
            assert response.status_code == 201

        first_page = client.get("/api/v1/tenants", params={"limit": 1})
        assert first_page.status_code == 200
        assert len(first_page.json()["items"]) == 1
        assert first_page.json()["next_cursor"]

        second_page = client.get(
            "/api/v1/tenants",
            params={"limit": 1, "cursor": first_page.json()["next_cursor"]},
        )
        assert second_page.status_code == 200
        assert len(second_page.json()["items"]) == 1
        assert second_page.json()["items"][0]["id"] != first_page.json()["items"][0]["id"]
        assert second_page.json()["next_cursor"] is None


def test_admin_can_group_tenants_and_assign_platform_roles() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        tenant = client.post(
            "/api/v1/tenants",
            headers={"Idempotency-Key": str(uuid4())},
            json={"slug": "north", "name": "North"},
        ).json()
        group = client.post(
            "/api/v1/tenant-groups",
            headers={"Idempotency-Key": str(uuid4())},
            json={"name": "North Region", "tenant_ids": [tenant["id"]]},
        )
        assert group.status_code == 201
        assert group.json()["tenant_ids"] == [tenant["id"]]

        user = client.post(
            "/api/v1/platform-users",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "issuer": "https://identity.example.test",
                "subject": "auditor",
                "email": "auditor@example.test",
                "display_name": "Platform Auditor",
            },
        )
        assert user.status_code == 201
        assignment_key = str(uuid4())
        assigned = client.put(
            f"/api/v1/platform-users/{user.json()['id']}/roles/PLATFORM_AUDITOR",
            headers={"Idempotency-Key": assignment_key, "If-Match": '"1"'},
        )
        assert assigned.status_code == 200
        assert assigned.headers["etag"] == '"2"'
        replayed = client.put(
            f"/api/v1/platform-users/{user.json()['id']}/roles/PLATFORM_AUDITOR",
            headers={"Idempotency-Key": assignment_key, "If-Match": '"1"'},
        )
        assert replayed.status_code == 200
        assert replayed.headers["idempotency-replayed"] == "true"
        assert replayed.headers["etag"] == '"2"'
        already_assigned = client.put(
            f"/api/v1/platform-users/{user.json()['id']}/roles/PLATFORM_AUDITOR",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"2"'},
        )
        assert already_assigned.status_code == 200
        assert already_assigned.headers["etag"] == '"2"'
        assert client.get("/api/v1/platform-users").json()["items"][0]["roles"] == [
            "PLATFORM_AUDITOR"
        ]


def test_role_assignment_requires_the_current_platform_user_version() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        user = client.post(
            "/api/v1/platform-users",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "issuer": "https://identity.example.test",
                "subject": "versioned-user",
                "display_name": "Versioned User",
            },
        ).json()
        assert user["version"] == 1

        missing = client.put(
            f"/api/v1/platform-users/{user['id']}/roles/PLATFORM_ADMIN",
            headers={"Idempotency-Key": str(uuid4())},
        )
        assert missing.status_code == 422
        assert missing.json()["error"]["code"] == "VALIDATION_ERROR"

        stale = client.put(
            f"/api/v1/platform-users/{user['id']}/roles/PLATFORM_ADMIN",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"9"'},
        )
        assert stale.status_code == 412
        assert stale.json()["error"]["code"] == "VERSION_MISMATCH"

        missing = client.put(
            f"/api/v1/platform-users/{uuid4()}/roles/PLATFORM_ADMIN",
            headers={"Idempotency-Key": str(uuid4()), "If-Match": '"1"'},
        )
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "NOT_FOUND"


def test_emergency_login_failure_is_audited_without_granting_a_session() -> None:
    asyncio.run(_prepare_database())
    with TestClient(create_app(_settings())) as client:
        response = client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "wrong"},
        )
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "INVALID_CREDENTIALS"

        assert asyncio.run(_denied_login_count()) == 1


def test_enterprise_oidc_authorization_code_flow_uses_pkce_and_nonce() -> None:
    asyncio.run(_prepare_database())
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    jwk["kid"] = "test-key"
    expected: dict[str, str] = {}

    def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            now = __import__("time").time()
            token = jwt.encode(
                {
                    "iss": "https://identity.example.test",
                    "aud": "admin-console",
                    "sub": "alice",
                    "name": "Alice",
                    "nonce": expected["nonce"],
                    "iat": int(now),
                    "exp": int(now) + 300,
                },
                private_key,
                algorithm="RS256",
                headers={"kid": "test-key"},
            )
            assert b"code_verifier=" in request.content
            return httpx.Response(200, json={"id_token": token})
        if request.url.path == "/jwks":
            return httpx.Response(200, json={"keys": [jwk]})
        return httpx.Response(404)

    settings = _settings().model_copy(
        update={
            "oidc_enabled": True,
            "oidc_issuer": "https://identity.example.test",
            "oidc_authorization_endpoint": "https://identity.example.test/authorize",
            "oidc_token_endpoint": "https://identity.example.test/token",
            "oidc_jwks_uri": "https://identity.example.test/jwks",
            "oidc_client_id": "admin-console",
            "oidc_redirect_uri": "http://testserver/api/v1/auth/oidc/callback",
            "web_console_url": "http://console.example.test/",
        }
    )
    with TestClient(create_app(settings, oidc_transport=httpx.MockTransport(provider))) as client:
        start = client.get("/api/v1/auth/oidc/login", follow_redirects=False)
        assert start.status_code == 302
        query = parse_qs(urlparse(start.headers["location"]).query)
        assert query["code_challenge_method"] == ["S256"]
        assert query["response_type"] == ["code"]
        flow = jwt.decode(client.cookies["trpc_oidc_flow"], options={"verify_signature": False})
        expected["nonce"] = flow["nonce"]

        callback = client.get(
            "/api/v1/auth/oidc/callback",
            params={"code": "provider-code", "state": query["state"][0]},
            follow_redirects=False,
        )
        assert callback.status_code == 303
        assert callback.headers["location"] == "http://console.example.test/"
        session = client.get("/api/v1/auth/session")
        assert session.status_code == 200
        assert session.json()["auth_method"] == "oidc"


def test_oidc_provider_failure_uses_a_stable_upstream_error() -> None:
    asyncio.run(_prepare_database())

    def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return httpx.Response(200, json={"id_token": "not-yet-verified"})
        return httpx.Response(503)

    settings = _settings().model_copy(
        update={
            "oidc_enabled": True,
            "oidc_issuer": "https://identity.example.test",
            "oidc_authorization_endpoint": "https://identity.example.test/authorize",
            "oidc_token_endpoint": "https://identity.example.test/token",
            "oidc_jwks_uri": "https://identity.example.test/jwks",
            "oidc_client_id": "admin-console",
        }
    )
    with TestClient(create_app(settings, oidc_transport=httpx.MockTransport(provider))) as client:
        start = client.get("/api/v1/auth/oidc/login", follow_redirects=False)
        state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
        callback = client.get(
            "/api/v1/auth/oidc/callback",
            params={"code": "provider-code", "state": state},
        )

        assert callback.status_code == 502
        assert callback.json()["error"]["code"] == "IDENTITY_PROVIDER_UNAVAILABLE"
