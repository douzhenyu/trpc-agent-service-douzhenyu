"""Public Admin API for tenant, identity, RBAC, and audit administration."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any
from uuid import UUID

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy.exc import IntegrityError

from trpc_service.admin_api.agents import create_agent_router
from trpc_service.admin_api.audit import insert_audit, write_audit
from trpc_service.admin_api.auth import (
    Principal,
    begin_oidc_flow,
    complete_oidc_flow,
    encode_session,
    principal_from_request,
    require_role,
    verify_emergency_password,
)
from trpc_service.admin_api.budgets import create_budget_router
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.http_contract import ETAG_HEADER, error_responses
from trpc_service.admin_api.idempotency import (
    IdempotencyConflictError,
    remember,
    replay_for,
)
from trpc_service.admin_api.model_profiles import create_model_profile_router
from trpc_service.admin_api.pagination import decode_cursor, encode_cursor
from trpc_service.admin_api.policies import create_policy_router
from trpc_service.admin_api.preconditions import parse_if_match
from trpc_service.admin_api.roles import (
    PlatformRole,
    PlatformUserNotFoundError,
    PlatformUserVersionChangedError,
    assign_platform_role,
)
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
from trpc_service.admin_api.tool_approvals import create_tool_approval_router
from trpc_service.admin_api.tools import create_tool_router
from trpc_service.ids import uuid7
from trpc_service.version import TRPC_AGENT_VERSION, __version__

LOGGER = logging.getLogger(__name__)


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
        responses=error_responses(500),
    )
    application.state.settings = configured
    application.include_router(create_agent_router(db))
    application.include_router(create_budget_router(db))
    if configured.policy_signing_key:
        application.include_router(
            create_policy_router(db, signing_key=configured.policy_signing_key)
        )
    application.include_router(create_model_profile_router(db))
    application.include_router(create_tool_router(db))
    application.include_router(create_tool_approval_router(db))

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
        detail_codes = {
            "invalid credentials": "INVALID_CREDENTIALS",
            "identity provider unavailable": "IDENTITY_PROVIDER_UNAVAILABLE",
            "TOOL_VERSION_CONFLICT": "TOOL_VERSION_CONFLICT",
            "APPROVAL_ALREADY_DECIDED": "APPROVAL_ALREADY_DECIDED",
            "APPROVAL_SELF_DENIED": "APPROVAL_SELF_DENIED",
            "APPROVER_ROLE_REQUIRED": "APPROVER_ROLE_REQUIRED",
            "APPROVAL_NOT_FOUND": "APPROVAL_NOT_FOUND",
            "APPROVAL_EXPIRED": "APPROVAL_EXPIRED",
            "APPROVAL_BINDING_MISMATCH": "APPROVAL_BINDING_MISMATCH",
            "RECONCILIATION_ALREADY_RESOLVED": "RECONCILIATION_ALREADY_RESOLVED",
            "RECONCILIATION_CALL_NOT_FOUND": "RECONCILIATION_CALL_NOT_FOUND",
            "RECONCILIATION_CALL_NOT_UNKNOWN": "RECONCILIATION_CALL_NOT_UNKNOWN",
        }
        code = detail_codes.get(detail, codes.get(exc.status_code, "REQUEST_FAILED"))
        return JSONResponse(
            status_code=exc.status_code, content={"error": {"code": code, "message": detail}}
        )

    @application.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict(
        _request: Request, _exc: IdempotencyConflictError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={
                "error": {
                    "code": "IDEMPOTENCY_CONFLICT",
                    "message": "idempotency key reused with a different request",
                }
            },
        )

    @application.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "VALIDATION_ERROR", "message": "request validation failed"}},
        )

    @application.exception_handler(Exception)
    async def internal_error(_request: Request, exc: Exception) -> JSONResponse:
        LOGGER.error(
            "Unhandled Admin API exception",
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "INTERNAL_ERROR", "message": "internal server error"}},
        )

    @application.get("/api/v1/health", response_model=HealthResponse, tags=["platform"])
    async def health() -> HealthResponse:
        return HealthResponse(version=__version__, trpc_agent_version=TRPC_AGENT_VERSION)

    @application.post(
        "/api/v1/auth/emergency/session",
        response_model=SessionResponse,
        responses=error_responses(401, 422),
    )
    async def emergency_login(
        payload: EmergencyLoginRequest, response: Response
    ) -> SessionResponse:
        if not verify_emergency_password(configured, payload.username, payload.password):
            await write_audit(
                db, None, "auth.emergency.login", "DENY", details={"username": payload.username}
            )
            raise HTTPException(status_code=401, detail="invalid credentials")
        principal = Principal(
            f"emergency:{configured.emergency_admin_username}",
            "emergency",
            frozenset({"PLATFORM_ADMIN"}),
        )
        await write_audit(db, principal, "auth.emergency.login", "ALLOW")
        _set_session(response, configured, principal)
        return SessionResponse(
            subject=principal.subject, auth_method="emergency", roles=sorted(principal.roles)
        )

    @application.get("/api/v1/auth/oidc/login", responses=error_responses(404))
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

    @application.get(
        "/api/v1/auth/oidc/callback",
        responses=error_responses(400, 401, 422, 502),
    )
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
        await write_audit(
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

    @application.get(
        "/api/v1/auth/session",
        response_model=SessionResponse,
        responses=error_responses(401),
    )
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

    @application.post(
        "/api/v1/tenants",
        response_model=TenantResponse,
        status_code=201,
        responses={**error_responses(401, 403, 409, 422), 201: {"headers": ETAG_HEADER}},
    )
    async def create_tenant(
        payload: TenantCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN")
        tenant_id = uuid7()
        request_payload = payload.model_dump(mode="json")
        try:
            async with db.transaction() as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="tenant.create",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    response.headers["ETag"] = f'"{replayed["version"]}"'
                    return replayed
                row = await connection.fetchrow(
                    """INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)
                    RETURNING id,slug,name,status,version,created_at,updated_at""",
                    tenant_id,
                    payload.slug,
                    payload.name,
                )
                await insert_audit(
                    connection,
                    principal,
                    "tenant.create",
                    "ALLOW",
                    target_type="tenant",
                    target_id=str(tenant_id),
                    tenant_id=tenant_id,
                )
                assert row is not None
                result = record_to_dict(row)
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="tenant.create",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(status_code=409, detail="tenant slug already exists") from error
        response.headers["ETag"] = '"1"'
        return result

    @application.get(
        "/api/v1/tenants",
        response_model=TenantList,
        responses=error_responses(400, 401, 403, 422),
    )
    async def list_tenants(
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query()] = None,
    ) -> dict[str, Any]:
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        platform_reader = bool(principal.roles.intersection({"PLATFORM_ADMIN", "PLATFORM_AUDITOR"}))
        if not platform_reader and principal.auth_method != "oidc":
            raise HTTPException(status_code=403, detail="insufficient role")
        transaction = (
            db.transaction() if platform_reader else db.user_transaction(UUID(principal.subject))
        )
        async with transaction as connection:
            rows = await connection.fetch(
                """SELECT id,slug,name,status,version,created_at,updated_at
                FROM platform.tenant t
                WHERE (CAST($1 AS uuid) IS NULL OR t.id > $1)
                  AND (CAST($3 AS boolean) OR EXISTS (
                    SELECT 1 FROM tenant.member m
                    WHERE m.tenant_id=t.id AND m.user_id=$4
                  ))
                ORDER BY t.id LIMIT $2""",
                cursor_id,
                limit + 1,
                platform_reader,
                UUID(principal.subject) if principal.auth_method == "oidc" else None,
            )
        page = rows[:limit]
        return {
            "items": [record_to_dict(row) for row in page],
            "next_cursor": encode_cursor(page[-1]["id"]) if len(rows) > limit else None,
        }

    @application.post(
        "/api/v1/tenant-groups",
        response_model=TenantGroupResponse,
        status_code=201,
        responses=error_responses(401, 403, 409, 422),
    )
    async def create_group(
        payload: TenantGroupCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN")
        group_id = uuid7()
        request_payload = payload.model_dump(mode="json")
        try:
            async with db.transaction() as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="tenant_group.create",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    return replayed
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
                await insert_audit(
                    connection,
                    principal,
                    "tenant_group.create",
                    "ALLOW",
                    target_type="tenant_group",
                    target_id=str(group_id),
                )
                assert row is not None
                result = {**record_to_dict(row), "tenant_ids": payload.tenant_ids}
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="tenant_group.create",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="tenant group conflicts with existing data"
            ) from error
        return result

    @application.get(
        "/api/v1/tenant-groups",
        response_model=TenantGroupList,
        responses=error_responses(400, 401, 403, 422),
    )
    async def list_groups(
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query()] = None,
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN", "PLATFORM_AUDITOR")
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with db.transaction() as connection:
            rows = await connection.fetch(
                """SELECT g.id,g.name,g.version,
                coalesce(array_agg(m.tenant_id)
                  FILTER (WHERE m.tenant_id IS NOT NULL),'{}') tenant_ids
                FROM platform.tenant_group g LEFT JOIN platform.tenant_group_member m
                  ON m.group_id=g.id
                WHERE (CAST($1 AS uuid) IS NULL OR g.id > $1)
                GROUP BY g.id ORDER BY g.id LIMIT $2""",
                cursor_id,
                limit + 1,
            )
        page = rows[:limit]
        return {
            "items": [record_to_dict(row) for row in page],
            "next_cursor": encode_cursor(page[-1]["id"]) if len(rows) > limit else None,
        }

    @application.post(
        "/api/v1/platform-users",
        response_model=PlatformUserResponse,
        status_code=201,
        responses=error_responses(401, 403, 409, 422),
    )
    async def create_user(
        payload: PlatformUserCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN")
        user_id = uuid7()
        request_payload = payload.model_dump(mode="json")
        try:
            async with db.transaction() as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="platform_user.create",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    return replayed
                row = await connection.fetchrow(
                    """INSERT INTO platform.platform_user (id,issuer,subject,email,display_name)
                    VALUES ($1,$2,$3,$4,$5)
                    RETURNING id,issuer,subject,email,display_name,version""",
                    user_id,
                    payload.issuer,
                    payload.subject,
                    payload.email,
                    payload.display_name,
                )
                await insert_audit(
                    connection,
                    principal,
                    "platform_user.create",
                    "ALLOW",
                    target_type="platform_user",
                    target_id=str(user_id),
                )
                assert row is not None
                result = {**record_to_dict(row), "roles": []}
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="platform_user.create",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="platform identity already exists"
            ) from error
        return result

    @application.put(
        "/api/v1/platform-users/{user_id}/roles/{role}",
        status_code=200,
        responses={
            **error_responses(400, 401, 403, 404, 409, 412, 422),
            200: {"headers": ETAG_HEADER},
        },
    )
    async def assign_role(
        user_id: UUID,
        role: PlatformRole,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> None:
        require_role(principal, "PLATFORM_ADMIN")
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        try:
            result = await assign_platform_role(
                db,
                principal,
                user_id=user_id,
                role=role,
                expected_version=expected_version,
                idempotency_key=key,
            )
        except PlatformUserNotFoundError as error:
            raise HTTPException(status_code=404, detail="platform user not found") from error
        except PlatformUserVersionChangedError as error:
            raise HTTPException(status_code=412, detail="platform user version changed") from error
        if result.replayed:
            response.headers["Idempotency-Replayed"] = "true"
        response.headers["ETag"] = f'"{result.version}"'

    @application.get(
        "/api/v1/platform-users",
        response_model=PlatformUserList,
        responses=error_responses(400, 401, 403, 422),
    )
    async def list_users(
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query()] = None,
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN", "PLATFORM_AUDITOR")
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with db.transaction() as connection:
            rows = await connection.fetch(
                """SELECT u.id,u.issuer,u.subject,u.email,u.display_name,u.version,
                coalesce(array_agg(r.role) FILTER (WHERE r.role IS NOT NULL),'{}') roles
                FROM platform.platform_user u LEFT JOIN platform.platform_role_assignment r
                  ON r.user_id=u.id
                WHERE (CAST($1 AS uuid) IS NULL OR u.id > $1)
                GROUP BY u.id ORDER BY u.id LIMIT $2""",
                cursor_id,
                limit + 1,
            )
        page = rows[:limit]
        return {
            "items": [record_to_dict(row) for row in page],
            "next_cursor": encode_cursor(page[-1]["id"]) if len(rows) > limit else None,
        }

    @application.get(
        "/api/v1/audit-events",
        response_model=AuditEventList,
        responses=error_responses(400, 401, 403, 422),
    )
    async def list_audit(
        principal: Annotated[Principal, Depends(principal_from_request)],
        tenant_id: Annotated[UUID | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        cursor: Annotated[str | None, Query()] = None,
    ) -> dict[str, Any]:
        require_role(principal, "PLATFORM_ADMIN", "PLATFORM_AUDITOR")
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with db.transaction() as connection:
            rows = await connection.fetch(
                """SELECT id,occurred_at,tenant_id,actor,auth_method,action,decision,
                target_type,target_id,details FROM platform.audit_event
                WHERE (CAST($1 AS uuid) IS NULL OR tenant_id=$1)
                  AND (CAST($2 AS uuid) IS NULL OR id < $2)
                ORDER BY id DESC LIMIT $3""",
                tenant_id,
                cursor_id,
                limit + 1,
            )
        page = rows[:limit]
        return {
            "items": [record_to_dict(row) for row in page],
            "next_cursor": encode_cursor(page[-1]["id"]) if len(rows) > limit else None,
        }

    return application


app = create_app()
