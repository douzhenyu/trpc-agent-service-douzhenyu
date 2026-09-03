"""Public Admin API for tenant, identity, RBAC, and audit administration."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal
from uuid import UUID

import asyncpg
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse

from trpc_service.admin_api.auth import (
    Principal,
    begin_oidc_flow,
    complete_oidc_flow,
    encode_session,
    principal_from_request,
    require_role,
    verify_emergency_password,
)
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.schemas import (
    AuditEventList,
    EmergencyLoginRequest,
    HealthResponse,
    PlatformUserCreate,
    PlatformUserList,
    PlatformUserResponse,
    SessionResponse,
    TenantCreate,
    TenantGroupCreate,
    TenantGroupList,
    TenantGroupResponse,
    TenantList,
    TenantResponse,
)
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.ids import uuid7
from trpc_service.version import TRPC_AGENT_VERSION, __version__


async def _insert_audit(
    connection: asyncpg.Connection[Any],
    principal: Principal | None,
    action: str,
    decision: str,
    *,
    target_type: str | None = None,
    target_id: str | None = None,
    tenant_id: UUID | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    await connection.execute(
        """INSERT INTO platform.audit_event
            (id,tenant_id,actor,auth_method,action,decision,target_type,target_id,details)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)""",
        uuid7(),
        tenant_id,
        principal.subject if principal else "anonymous",
        principal.auth_method if principal else "anonymous",
        action,
        decision,
        target_type,
        target_id,
        json.dumps(details or {}),
    )


async def _audit(
    db: Database, principal: Principal | None, action: str, decision: str, **fields: Any
) -> None:
    async with db.transaction() as connection:
        await _insert_audit(connection, principal, action, decision, **fields)


def _set_session(response: Response, settings: AdminSettings, principal: Principal) -> None:
    response.set_cookie(
        settings.session_cookie_name,
        encode_session(settings, principal),
        httponly=True,
        secure=settings.session_cookie_secure,
        samesite="lax",
        max_age=settings.session_ttl_seconds,
        path="/",
    )


def create_app(
    settings: AdminSettings | None = None, *, oidc_transport: httpx.AsyncBaseTransport | None = None
) -> FastAPI:
    configured = settings or AdminSettings()
    db = Database(configured.database_url)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        configured.validate_runtime_security()
        await db.open()
        application.state.db = db
        yield
        await db.close()

    application = FastAPI(
        title="tRPC-Agent Platform Admin API",
        version=__version__,
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    application.state.settings = configured

    @application.exception_handler(HTTPException)
    async def stable_http_error(_request: Request, exc: HTTPException) -> JSONResponse:
        codes = {
            400: "INVALID_REQUEST",
            401: "UNAUTHENTICATED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            409: "CONFLICT",
            412: "VERSION_MISMATCH",
        }
        detail = str(exc.detail)
        code = (
            "INVALID_CREDENTIALS"
            if detail == "invalid credentials"
            else codes.get(exc.status_code, "REQUEST_FAILED")
        )
        return JSONResponse(
            status_code=exc.status_code, content={"error": {"code": code, "message": detail}}
        )

    @application.get("/api/v1/health", response_model=HealthResponse, tags=["platform"])
    async def health() -> HealthResponse:
        return HealthResponse(version=__version__, trpc_agent_version=TRPC_AGENT_VERSION)

    @application.post("/api/v1/auth/emergency/session", response_model=SessionResponse)
    async def emergency_login(
        payload: EmergencyLoginRequest, response: Response
    ) -> SessionResponse:
        if not verify_emergency_password(configured, payload.username, payload.password):
            await _audit(
                db, None, "auth.emergency.login", "DENY", details={"username": payload.username}
            )
            raise HTTPException(status_code=401, detail="invalid credentials")
        principal = Principal(
            f"emergency:{configured.emergency_admin_username}",
            "emergency",
            frozenset({"PLATFORM_ADMIN"}),
        )
        await _audit(db, principal, "auth.emergency.login", "ALLOW")
        _set_session(response, configured, principal)
        return SessionResponse(
            subject=principal.subject, auth_method="emergency", roles=sorted(principal.roles)
        )

    @application.get("/api/v1/auth/oidc/login")
    async def oidc_login() -> Response:
        if not configured.oidc_enabled:
            raise HTTPException(status_code=404, detail="oidc is disabled")
        location, flow = begin_oidc_flow(configured)
        response = RedirectResponse(location, status_code=302)
        response.set_cookie(
            "trpc_oidc_flow",
            flow,
            httponly=True,
            secure=configured.session_cookie_secure,
            samesite="lax",
            max_age=300,
        )
        return response

    @application.get("/api/v1/auth/oidc/callback")
    async def oidc_callback(request: Request, code: str, state: str) -> Response:
        flow = request.cookies.get("trpc_oidc_flow")
        if not flow:
            raise HTTPException(status_code=400, detail="missing oidc flow")
        claims = await complete_oidc_flow(configured, code, state, flow, oidc_transport)
        async with db.transaction() as connection:
            row = await connection.fetchrow(
                """INSERT INTO platform.platform_user (id,issuer,subject,email,display_name)
                VALUES ($1,$2,$3,$4,$5) ON CONFLICT (issuer,subject) DO UPDATE SET
                email=EXCLUDED.email,display_name=EXCLUDED.display_name,
                version=platform.platform_user.version+1,updated_at=now() RETURNING id""",
                uuid7(),
                configured.oidc_issuer,
                str(claims["sub"]),
                claims.get("email"),
                str(claims.get("name") or claims.get("preferred_username") or claims["sub"]),
            )
            assert row is not None
            roles = await connection.fetch(
                "SELECT role FROM platform.platform_role_assignment WHERE user_id=$1", row["id"]
            )
        principal = Principal(str(row["id"]), "oidc", frozenset(item["role"] for item in roles))
        await _audit(
            db,
            principal,
            "auth.oidc.login",
            "ALLOW",
            target_type="platform_user",
            target_id=principal.subject,
        )
        response = RedirectResponse(configured.web_console_url, status_code=303)
        _set_session(response, configured, principal)
        response.delete_cookie("trpc_oidc_flow")
        return response

    @application.get("/api/v1/auth/session", response_model=SessionResponse)
    async def session(
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> SessionResponse:
        return SessionResponse(
            subject=principal.subject,
            auth_method=principal.auth_method,
            roles=sorted(principal.roles),
        )

    @application.delete("/api/v1/auth/session", status_code=200)
    async def logout(response: Response) -> None:
        response.delete_cookie(configured.session_cookie_name)

    @application.post("/api/v1/tenants", response_model=TenantResponse, status_code=201)
    async def create_tenant(
        payload: TenantCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        _key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN")
        tenant_id = uuid7()
        try:
            async with db.transaction() as connection:
                row = await connection.fetchrow(
                    """INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)
                    RETURNING id,slug,name,status,version,created_at,updated_at""",
                    tenant_id,
                    payload.slug,
                    payload.name,
                )
                await _insert_audit(
                    connection,
                    principal,
                    "tenant.create",
                    "ALLOW",
                    target_type="tenant",
                    target_id=str(tenant_id),
                    tenant_id=tenant_id,
                )
        except asyncpg.UniqueViolationError as error:
            raise HTTPException(status_code=409, detail="tenant slug already exists") from error
        response.headers["ETag"] = '"1"'
        assert row is not None
        return record_to_dict(row)

    @application.get("/api/v1/tenants", response_model=TenantList)
    async def list_tenants(
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN", "PLATFORM_AUDITOR")
        async with db.transaction() as connection:
            rows = await connection.fetch(
                """SELECT id,slug,name,status,version,created_at,updated_at
                FROM platform.tenant ORDER BY created_at,id"""
            )
        return {"items": [record_to_dict(row) for row in rows]}

    @application.post("/api/v1/tenant-groups", response_model=TenantGroupResponse, status_code=201)
    async def create_group(
        payload: TenantGroupCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        _key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN")
        group_id = uuid7()
        try:
            async with db.transaction() as connection:
                row = await connection.fetchrow(
                    """INSERT INTO platform.tenant_group (id,name) VALUES ($1,$2)
                    RETURNING id,name,version""",
                    group_id,
                    payload.name,
                )
                await connection.executemany(
                    "INSERT INTO platform.tenant_group_member (group_id,tenant_id) VALUES ($1,$2)",
                    [(group_id, tenant_id) for tenant_id in payload.tenant_ids],
                )
                await _insert_audit(
                    connection,
                    principal,
                    "tenant_group.create",
                    "ALLOW",
                    target_type="tenant_group",
                    target_id=str(group_id),
                )
        except asyncpg.IntegrityConstraintViolationError as error:
            raise HTTPException(
                status_code=409, detail="tenant group conflicts with existing data"
            ) from error
        assert row is not None
        return {**record_to_dict(row), "tenant_ids": payload.tenant_ids}

    @application.get("/api/v1/tenant-groups", response_model=TenantGroupList)
    async def list_groups(
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN", "PLATFORM_AUDITOR")
        async with db.transaction() as connection:
            rows = await connection.fetch(
                """SELECT g.id,g.name,g.version,
                coalesce(array_agg(m.tenant_id)
                  FILTER (WHERE m.tenant_id IS NOT NULL),'{}') tenant_ids
                FROM platform.tenant_group g LEFT JOIN platform.tenant_group_member m
                  ON m.group_id=g.id
                GROUP BY g.id ORDER BY g.name"""
            )
        return {"items": [record_to_dict(row) for row in rows]}

    @application.post(
        "/api/v1/platform-users", response_model=PlatformUserResponse, status_code=201
    )
    async def create_user(
        payload: PlatformUserCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        _key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN")
        user_id = uuid7()
        try:
            async with db.transaction() as connection:
                row = await connection.fetchrow(
                    """INSERT INTO platform.platform_user (id,issuer,subject,email,display_name)
                    VALUES ($1,$2,$3,$4,$5) RETURNING id,issuer,subject,email,display_name""",
                    user_id,
                    payload.issuer,
                    payload.subject,
                    payload.email,
                    payload.display_name,
                )
                await _insert_audit(
                    connection,
                    principal,
                    "platform_user.create",
                    "ALLOW",
                    target_type="platform_user",
                    target_id=str(user_id),
                )
        except asyncpg.UniqueViolationError as error:
            raise HTTPException(
                status_code=409, detail="platform identity already exists"
            ) from error
        assert row is not None
        return {**record_to_dict(row), "roles": []}

    @application.put(
        "/api/v1/platform-users/{user_id}/roles/{role}",
        status_code=200,
    )
    async def assign_role(
        user_id: UUID,
        role: Literal["PLATFORM_ADMIN", "PLATFORM_AUDITOR"],
        principal: Annotated[Principal, Depends(principal_from_request)],
        _key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> None:
        require_role(principal, "PLATFORM_ADMIN")
        validated_role = role
        try:
            async with db.transaction() as connection:
                await connection.execute(
                    """INSERT INTO platform.platform_role_assignment (user_id,role)
                    VALUES ($1,$2) ON CONFLICT DO NOTHING""",
                    user_id,
                    validated_role,
                )
                await _insert_audit(
                    connection,
                    principal,
                    "platform_role.assign",
                    "ALLOW",
                    target_type="platform_user",
                    target_id=str(user_id),
                    details={"role": validated_role},
                )
        except asyncpg.ForeignKeyViolationError as error:
            raise HTTPException(status_code=404, detail="platform user not found") from error

    @application.get("/api/v1/platform-users", response_model=PlatformUserList)
    async def list_users(
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN", "PLATFORM_AUDITOR")
        async with db.transaction() as connection:
            rows = await connection.fetch(
                """SELECT u.id,u.issuer,u.subject,u.email,u.display_name,
                coalesce(array_agg(r.role) FILTER (WHERE r.role IS NOT NULL),'{}') roles
                FROM platform.platform_user u LEFT JOIN platform.platform_role_assignment r
                  ON r.user_id=u.id
                GROUP BY u.id ORDER BY u.display_name"""
            )
        return {"items": [record_to_dict(row) for row in rows]}

    @application.get("/api/v1/audit-events", response_model=AuditEventList)
    async def list_audit(
        principal: Annotated[Principal, Depends(principal_from_request)],
        tenant_id: Annotated[UUID | None, Query()] = None,
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN", "PLATFORM_AUDITOR")
        async with db.transaction() as connection:
            rows = await connection.fetch(
                """SELECT id,occurred_at,tenant_id,actor,auth_method,action,decision,
                target_type,target_id,details FROM platform.audit_event
                WHERE ($1::uuid IS NULL OR tenant_id=$1)
                ORDER BY occurred_at DESC,id DESC LIMIT 200""",
                tenant_id,
            )
        return {"items": [record_to_dict(row) for row in rows]}

    return application


app = create_app()
