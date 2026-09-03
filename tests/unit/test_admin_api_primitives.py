from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Any, cast
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm
from pydantic import SecretStr
from sqlalchemy.exc import IntegrityError
from starlette.requests import Request

from trpc_service.admin_api.app import create_app as create_admin_app
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
from trpc_service.admin_api.roles import (
    PlatformUserNotFoundError,
    PlatformUserVersionChangedError,
    assign_platform_role,
)
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.runtime_health import RUNTIME_UNITS
from trpc_service.runtime_health import create_app as create_runtime_app

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
        with TestClient(create_runtime_app(unit)) as client:
            assert client.get("/health/live").json()["service"] == unit
            assert client.get("/health/ready").json()["status"] == "ok"

    monkeypatch.setenv("PLATFORM_UNIT", "job-worker")
    with TestClient(create_runtime_app()) as client:
        assert client.get("/health/live").json()["service"] == "job-worker"
    monkeypatch.setenv("PLATFORM_UNIT", "unsupported")
    with pytest.raises(RuntimeError, match="unsupported PLATFORM_UNIT"):
        create_runtime_app()


def test_openapi_contract_declares_stable_errors_and_conditional_write_headers() -> None:
    schema = create_admin_app(_settings()).openapi()
    role_assignment = schema["paths"]["/api/v1/platform-users/{user_id}/roles/{role}"]["put"]
    assert set(role_assignment["responses"]) >= {
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
    assert "ETag" in role_assignment["responses"]["200"]["headers"]
    error_codes = schema["components"]["schemas"]["ErrorDetail"]["properties"]["code"]["enum"]
    assert {"VERSION_MISMATCH", "IDEMPOTENCY_CONFLICT", "IDENTITY_PROVIDER_UNAVAILABLE"} <= set(
        error_codes
    )


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


class _Mappings:
    def first(self) -> dict[str, int]:
        return {"answer": 42}

    def all(self) -> list[dict[str, int]]:
        return [{"answer": 42}]


class _Result:
    rowcount = 1

    def mappings(self) -> _Mappings:
        return _Mappings()

    def scalar(self) -> int:
        return 42


class _AsyncConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object]] = []

    async def execute(self, statement: object, parameters: object = None) -> _Result:
        self.calls.append((statement, parameters))
        return _Result()


class _Engine:
    def __init__(self) -> None:
        self.connection = _AsyncConnection()
        self.disposed = False

    @asynccontextmanager
    async def begin(self) -> Any:
        yield self.connection

    async def dispose(self) -> None:
        self.disposed = True


def test_database_boundary_executes_statements_and_sets_tenant_context() -> None:
    async def exercise() -> None:
        raw = _AsyncConnection()
        connection = Connection(cast(Any, raw))
        assert await connection.execute("UPDATE things SET value=$1", 1) == 1
        assert await connection.fetchrow("SELECT $1", 1) == {"answer": 42}
        assert await connection.fetch("SELECT $1", 1) == [{"answer": 42}]
        assert await connection.fetchval("SELECT $1", 1) == 42
        await connection.executemany("INSERT INTO things VALUES ($1)", [])
        await connection.executemany("INSERT INTO things VALUES ($1)", [(1,), (2,)])

        engine = _Engine()
        database = Database("postgresql://host/db")
        database._engine = cast(Any, engine)
        async with database.tenant_transaction(
            UUID("00000000-0000-0000-0000-000000000001")
        ) as tenant_connection:
            assert isinstance(tenant_connection, Connection)
        await database.close()
        assert engine.disposed
        assert len(engine.connection.calls) == 1

    asyncio.run(exercise())


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


class _RoleConnection:
    def __init__(
        self,
        *,
        replay: dict[str, Any] | None = None,
        current: dict[str, Any] | None = None,
        updated: int | None = 2,
        failure: Exception | None = None,
    ) -> None:
        self.replay = replay
        self.current = current
        self.updated = updated
        self.failure = failure
        self.fetchrow_calls = 0

    async def fetchval(self, sql: str, *_args: Any) -> Any:
        return None if "pg_advisory_xact_lock" in sql else self.updated

    async def fetchrow(self, _sql: str, *_args: Any) -> dict[str, Any] | None:
        if self.failure is not None:
            raise self.failure
        self.fetchrow_calls += 1
        return self.replay if self.fetchrow_calls == 1 else self.current

    async def execute(self, _sql: str, *_args: Any) -> int:
        return 1


class _RoleDatabase:
    def __init__(self, connection: _RoleConnection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def transaction(self) -> Any:
        yield self.connection


def test_role_assignment_domain_handles_replay_conflicts_and_noop() -> None:
    principal = Principal("admin", "emergency", frozenset({"PLATFORM_ADMIN"}))
    user_id = UUID("00000000-0000-0000-0000-000000000001")

    async def assign(connection: _RoleConnection) -> Any:
        return await assign_platform_role(
            cast(Any, _RoleDatabase(connection)),
            principal,
            user_id=user_id,
            role="PLATFORM_ADMIN",
            expected_version=1,
            idempotency_key="key",
        )

    replay = _RoleConnection(
        replay={
            "operation": "platform_role.assign",
            "request_hash": request_hash(
                {"user_id": str(user_id), "role": "PLATFORM_ADMIN", "version": 1}
            ),
            "response": {"version": 2},
        }
    )
    assert asyncio.run(assign(replay)).replayed

    with pytest.raises(PlatformUserNotFoundError):
        asyncio.run(assign(_RoleConnection()))
    with pytest.raises(PlatformUserVersionChangedError):
        asyncio.run(assign(_RoleConnection(current={"version": 9, "assigned": False})))

    no_op = asyncio.run(assign(_RoleConnection(current={"version": 1, "assigned": True})))
    assert no_op.version == 1
    with pytest.raises(PlatformUserVersionChangedError):
        asyncio.run(
            assign(_RoleConnection(current={"version": 1, "assigned": False}, updated=None))
        )

    applied = asyncio.run(
        assign(_RoleConnection(current={"version": 1, "assigned": False}, updated=2))
    )
    assert applied.version == 2

    integrity_error = IntegrityError("statement", {}, RuntimeError("constraint"))
    with pytest.raises(PlatformUserNotFoundError):
        asyncio.run(assign(_RoleConnection(failure=integrity_error)))


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


def test_oidc_id_token_requires_lifecycle_subject_and_authorized_party() -> None:
    settings = _settings()
    authorization_url, flow_token = begin_oidc_flow(settings)
    state = parse_qs(urlparse(authorization_url).query)["state"][0]
    flow = jwt.decode(flow_token, options={"verify_signature": False})
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    jwk["kid"] = "test-key"
    now = int(time.time())
    base_claims: dict[str, Any] = {
        "iss": settings.oidc_issuer,
        "aud": settings.oidc_client_id,
        "sub": "alice",
        "nonce": flow["nonce"],
        "iat": now,
        "exp": now + 300,
    }

    async def complete(claims: dict[str, Any]) -> dict[str, Any]:
        token = jwt.encode(
            claims,
            private_key,
            algorithm="RS256",
            headers={"kid": "test-key"},
        )

        def provider(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/token":
                return httpx.Response(200, json={"id_token": token})
            return httpx.Response(200, json={"keys": [jwk]})

        return await complete_oidc_flow(
            settings,
            "code",
            state,
            flow_token,
            httpx.MockTransport(provider),
        )

    for required_claim in ("exp", "iat", "sub"):
        invalid = {key: value for key, value in base_claims.items() if key != required_claim}
        with pytest.raises(HTTPException) as error:
            asyncio.run(complete(invalid))
        assert error.value.status_code == 401

    multiple_audiences = {**base_claims, "aud": [settings.oidc_client_id, "other-client"]}
    with pytest.raises(HTTPException) as error:
        asyncio.run(complete(multiple_audiences))
    assert error.value.status_code == 401

    accepted = asyncio.run(complete({**multiple_audiences, "azp": settings.oidc_client_id}))
    assert accepted["sub"] == "alice"
