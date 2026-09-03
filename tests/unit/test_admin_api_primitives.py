from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import httpx
import jwt
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.requests import Request

from trpc_service.admin_api.auth import (
    Principal,
    begin_oidc_flow,
    complete_oidc_flow,
    decode_session,
    encode_session,
    principal_from_request,
    require_role,
    verify_emergency_password,
)
from trpc_service.admin_api.database import Connection, Database, record_to_dict, sqlalchemy_url
from trpc_service.admin_api.idempotency import (
    IdempotencyConflictError,
    remember,
    replay_for,
    request_hash,
)
from trpc_service.admin_api.pagination import decode_cursor, encode_cursor
from trpc_service.admin_api.preconditions import parse_if_match
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.runtime_health import RUNTIME_UNITS, create_app

PASSWORD_HASH = (
    "$argon2id$v=19$m=65536,t=3,p=4$MRV7DB8RCvU73jcYXzxkUA$"
    "z7yjdKaXuCwuYoWzAqb25/+4f8tW5j3cxFm/pComAo4"
)


def _settings() -> AdminSettings:
    return AdminSettings(
        database_url="postgresql://app:secret@database/platform",
        session_signing_key="test-session-key-that-is-long-enough-for-hs256",
        emergency_admin_username="break-glass",
        emergency_admin_password_hash=PASSWORD_HASH,
        oidc_enabled=True,
        oidc_issuer="https://identity.example.test",
        oidc_authorization_endpoint="https://identity.example.test/authorize",
        oidc_token_endpoint="https://identity.example.test/token",
        oidc_jwks_uri="https://identity.example.test/jwks",
        oidc_client_id="admin-console",
    )


def test_runtime_health_supports_every_data_plane_unit(monkeypatch: pytest.MonkeyPatch) -> None:
    for unit in RUNTIME_UNITS:
        with TestClient(create_app(unit)) as client:
            assert client.get("/health/live").json()["service"] == unit
            assert client.get("/health/ready").json()["status"] == "ok"

    monkeypatch.setenv("PLATFORM_UNIT", "job-worker")
    with TestClient(create_app()) as client:
        assert client.get("/health/live").json()["service"] == "job-worker"
    monkeypatch.setenv("PLATFORM_UNIT", "unsupported")
    with pytest.raises(RuntimeError, match="unsupported PLATFORM_UNIT"):
        create_app()


def test_database_and_http_primitives_validate_and_normalize_inputs() -> None:
    assert sqlalchemy_url("postgresql+asyncpg://host/db") == "postgresql+asyncpg://host/db"
    assert sqlalchemy_url("postgresql://host/db") == "postgresql+asyncpg://host/db"
    assert sqlalchemy_url("postgres://host/db") == "postgresql+asyncpg://host/db"
    assert sqlalchemy_url("sqlite+aiosqlite:///test.db") == "sqlite+aiosqlite:///test.db"
    assert Connection._statement("SELECT $2, $1", ("first", "second")) == (
        "SELECT :p2, :p1",
        {"p1": "first", "p2": "second"},
    )
    assert record_to_dict({"answer": 42}) == {"answer": 42}

    database = Database("postgresql://host/db")
    with pytest.raises(RuntimeError, match="not open"):
        _ = database.engine

    identifier = UUID("00000000-0000-0000-0000-000000000001")
    cursor = encode_cursor(identifier)
    assert decode_cursor(cursor) == identifier
    with pytest.raises(ValueError):
        decode_cursor("not-base64")
    assert parse_if_match('"12"') == 12
    for invalid in ("12", '"0"', "*", '"1", "2"'):
        with pytest.raises(ValueError, match="quoted positive version"):
            parse_if_match(invalid)


class _LedgerConnection:
    def __init__(self, row: dict[str, Any] | None = None) -> None:
        self.row = row
        self.executed: tuple[str, tuple[Any, ...]] | None = None
        self.locked = False

    async def fetchval(self, _sql: str, *_args: Any) -> None:
        self.locked = True

    async def fetchrow(self, _sql: str, *_args: Any) -> dict[str, Any] | None:
        return self.row

    async def execute(self, sql: str, *args: Any) -> int:
        self.executed = (sql, args)
        return 1


def test_idempotency_ledger_replays_only_the_same_command() -> None:
    payload = {"name": "Acme", "version": 1}

    async def exercise() -> None:
        empty = _LedgerConnection()
        assert (
            await replay_for(
                empty, actor="admin", key="key", operation="tenant.create", payload=payload
            )
            is None
        )
        assert empty.locked

        matching = _LedgerConnection(
            {
                "operation": "tenant.create",
                "request_hash": request_hash(payload),
                "response": json.dumps({"id": "tenant-1"}),
            }
        )
        assert await replay_for(
            matching, actor="admin", key="key", operation="tenant.create", payload=payload
        ) == {"id": "tenant-1"}
        matching.row["response"] = {"id": "tenant-1"}
        assert await replay_for(
            matching, actor="admin", key="key", operation="tenant.create", payload=payload
        ) == {"id": "tenant-1"}

        matching.row["operation"] = "tenant.update"
        with pytest.raises(IdempotencyConflictError):
            await replay_for(
                matching, actor="admin", key="key", operation="tenant.create", payload=payload
            )

        target = _LedgerConnection()
        await remember(
            target,
            actor="admin",
            key="key",
            operation="tenant.create",
            payload=payload,
            response={"id": "tenant-1"},
        )
        assert target.executed is not None
        assert target.executed[1][3] == request_hash(payload)

    asyncio.run(exercise())


def test_session_and_emergency_authentication_reject_invalid_inputs() -> None:
    settings = _settings()
    principal = Principal("admin", "emergency", frozenset({"PLATFORM_ADMIN"}))
    assert decode_session(settings, encode_session(settings, principal)) == principal

    for claims in (
        {"sub": "admin", "auth_method": "emergency", "roles": [], "type": "wrong"},
        {"sub": "admin", "auth_method": "wrong", "roles": [], "type": "session"},
        {"auth_method": "emergency", "roles": [], "type": "session"},
    ):
        token = jwt.encode(
            claims, settings.session_signing_key.get_secret_value(), algorithm="HS256"
        )
        with pytest.raises(HTTPException) as error:
            decode_session(settings, token)
        assert error.value.status_code == 401

    require_role(principal, "PLATFORM_ADMIN")
    with pytest.raises(HTTPException) as error:
        require_role(principal, "PLATFORM_AUDITOR")
    assert error.value.status_code == 403
    assert verify_emergency_password(settings, "break-glass", "correct-horse")
    assert not verify_emergency_password(settings, "wrong", "correct-horse")
    broken = settings.model_copy(update={"emergency_admin_password_hash": SecretStr("not-a-hash")})
    assert not verify_emergency_password(broken, "break-glass", "correct-horse")


def test_principal_dependency_requires_a_session() -> None:
    request = Request({"type": "http", "headers": [], "app": type("App", (), {})()})
    request.app.state = type("State", (), {"settings": _settings()})()
    with pytest.raises(HTTPException) as error:
        asyncio.run(principal_from_request(request))
    assert error.value.status_code == 401


def test_oidc_flow_rejects_invalid_protocol_responses() -> None:
    settings = _settings()
    authorization_url, flow_token = begin_oidc_flow(settings)
    state = parse_qs(urlparse(authorization_url).query)["state"][0]

    async def complete(transport: httpx.MockTransport, flow: str = flow_token) -> None:
        await complete_oidc_flow(settings, "code", state, flow, transport)

    with pytest.raises(HTTPException) as error:
        asyncio.run(complete(httpx.MockTransport(lambda _request: httpx.Response(500)), "bad"))
    assert error.value.status_code == 400

    def token_error(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401 if request.url.path == "/token" else 200)

    with pytest.raises(HTTPException) as error:
        asyncio.run(complete(httpx.MockTransport(token_error)))
    assert error.value.status_code == 401

    def missing_token(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    with pytest.raises(HTTPException) as error:
        asyncio.run(complete(httpx.MockTransport(missing_token)))
    assert error.value.status_code == 401

    def unsupported_key(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return httpx.Response(
                200,
                json={
                    "id_token": jwt.encode(
                        {}, "long-enough-test-key-for-hs256-signature", algorithm="HS256"
                    )
                },
            )
        return httpx.Response(200, json={"keys": []})

    with pytest.raises(HTTPException) as error:
        asyncio.run(complete(httpx.MockTransport(unsupported_key)))
    assert error.value.status_code == 401

    def malformed_key(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return httpx.Response(
                200,
                json={"id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleSJ9.e30.signature"},
            )
        return httpx.Response(200, json={"keys": [{"kid": "key"}]})

    with pytest.raises(HTTPException) as error:
        asyncio.run(complete(httpx.MockTransport(malformed_key)))
    assert error.value.status_code == 401
