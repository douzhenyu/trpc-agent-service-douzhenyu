"""Tenant-scoped Agent application and Agent Draft Admin API routes."""

from __future__ import annotations

import json
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy.exc import IntegrityError

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.draft_validation import validate_draft_configuration
from trpc_service.admin_api.http_contract import ETAG_HEADER, error_responses
from trpc_service.admin_api.idempotency import remember, replay_for
from trpc_service.admin_api.pagination import decode_cursor, encode_cursor
from trpc_service.admin_api.preconditions import parse_if_match
from trpc_service.admin_api.schemas import (
    AgentApplicationCreate,
    AgentApplicationList,
    AgentApplicationResponse,
    AgentApplicationUpdate,
    AgentDraftCreate,
    AgentDraftResponse,
    AgentDraftUpdate,
    DraftValidationResponse,
)
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.ids import uuid7


def create_agent_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}/agent-applications", tags=["agents"])

    @router.post(
        "",
        response_model=AgentApplicationResponse,
        status_code=201,
        responses={**error_responses(401, 403, 404, 409, 422), 201: {"headers": ETAG_HEADER}},
    )
    async def create_application(
        tenant_id: UUID,
        payload: AgentApplicationCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "agent_application.create",
            target_type="agent_application",
        )
        request_payload = {"tenant_id": str(tenant_id), **payload.model_dump(mode="json")}
        application_id = uuid7()
        try:
            async with database.tenant_transaction(tenant_id) as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="agent_application.create",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    response.headers["ETag"] = f'"{replayed["version"]}"'
                    return replayed
                row = await connection.fetchrow(
                    """INSERT INTO tenant.agent_application
                    (tenant_id,id,slug,name,description) VALUES ($1,$2,$3,$4,$5)
                    RETURNING tenant_id,id,slug,name,description,version,created_at,updated_at""",
                    tenant_id,
                    application_id,
                    payload.slug,
                    payload.name,
                    payload.description,
                )
                assert row is not None
                result = record_to_dict(row)
                await insert_audit(
                    connection,
                    principal,
                    "agent_application.create",
                    "ALLOW",
                    target_type="agent_application",
                    target_id=str(application_id),
                    tenant_id=tenant_id,
                )
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="agent_application.create",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="Agent application already exists"
            ) from error
        response.headers["ETag"] = '"1"'
        return result

    @router.get(
        "", response_model=AgentApplicationList, responses=error_responses(400, 401, 403, 422)
    )
    async def list_applications(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query()] = None,
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "agent_application.list",
            target_type="agent_application",
        )
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                """SELECT tenant_id,id,slug,name,description,version,created_at,updated_at
                FROM tenant.agent_application
                WHERE (CAST($1 AS uuid) IS NULL OR id > $1) ORDER BY id LIMIT $2""",
                cursor_id,
                limit + 1,
            )
        page = rows[:limit]
        return {
            "items": [record_to_dict(row) for row in page],
            "next_cursor": encode_cursor(page[-1]["id"]) if len(rows) > limit else None,
        }

    @router.get(
        "/{application_id}",
        response_model=AgentApplicationResponse,
        responses={**error_responses(401, 403, 404, 422), 200: {"headers": ETAG_HEADER}},
    )
    async def get_application(
        tenant_id: UUID,
        application_id: UUID,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "agent_application.get",
            target_type="agent_application",
            target_id=str(application_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """SELECT tenant_id,id,slug,name,description,version,created_at,updated_at
                FROM tenant.agent_application WHERE tenant_id=$1 AND id=$2""",
                tenant_id,
                application_id,
            )
        if row is None:
            raise HTTPException(status_code=404, detail="Agent application not found")
        result = record_to_dict(row)
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @router.put(
        "/{application_id}/draft",
        response_model=AgentDraftResponse,
        status_code=201,
        responses={**error_responses(401, 403, 404, 409, 422), 201: {"headers": ETAG_HEADER}},
    )
    async def create_draft(
        tenant_id: UUID,
        application_id: UUID,
        payload: AgentDraftCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "agent_draft.create",
            target_type="agent_draft",
            target_id=str(application_id),
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            **payload.model_dump(mode="json"),
        }
        try:
            async with database.tenant_transaction(tenant_id) as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="agent_draft.create",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    response.headers["ETag"] = f'"{replayed["version"]}"'
                    return replayed
                row = await connection.fetchrow(
                    """INSERT INTO tenant.agent_draft
                    (tenant_id,application_id,instructions,model_alias,tool_aliases,
                    knowledge_refs,governance_policy_ref)
                    VALUES ($1,$2,$3,$4,CAST($5 AS jsonb),CAST($6 AS jsonb),$7)
                    RETURNING tenant_id,application_id,instructions,model_alias,tool_aliases,
                    knowledge_refs,governance_policy_ref,version,created_at,updated_at""",
                    tenant_id,
                    application_id,
                    payload.instructions,
                    payload.model_alias,
                    json.dumps(payload.tool_aliases),
                    json.dumps(payload.knowledge_refs),
                    payload.governance_policy_ref,
                )
                assert row is not None
                result = record_to_dict(row)
                await insert_audit(
                    connection,
                    principal,
                    "agent_draft.create",
                    "ALLOW",
                    target_type="agent_draft",
                    target_id=str(application_id),
                    tenant_id=tenant_id,
                )
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="agent_draft.create",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(status_code=409, detail="Agent Draft cannot be created") from error
        response.headers["ETag"] = '"1"'
        return result

    @router.get(
        "/{application_id}/draft",
        response_model=AgentDraftResponse,
        responses={**error_responses(401, 403, 404, 422), 200: {"headers": ETAG_HEADER}},
    )
    async def get_draft(
        tenant_id: UUID,
        application_id: UUID,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "agent_draft.get",
            target_type="agent_draft",
            target_id=str(application_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """SELECT tenant_id,application_id,instructions,model_alias,tool_aliases,
                knowledge_refs,governance_policy_ref,version,created_at,updated_at
                FROM tenant.agent_draft WHERE tenant_id=$1 AND application_id=$2""",
                tenant_id,
                application_id,
            )
        if row is None:
            raise HTTPException(status_code=404, detail="Agent Draft not found")
        result = record_to_dict(row)
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @router.post(
        "/{application_id}/draft/validate",
        response_model=DraftValidationResponse,
        responses={
            **error_responses(400, 401, 403, 404, 409, 412, 422),
            200: {"headers": ETAG_HEADER},
        },
    )
    async def validate_draft(
        tenant_id: UUID,
        application_id: UUID,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "agent_draft.validate",
            target_type="agent_draft",
            target_id=str(application_id),
        )
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            "expected_version": expected_version,
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_draft.validate",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                response.headers["ETag"] = f'"{replayed["draft_version"]}"'
                return replayed
            draft = await connection.fetchrow(
                """SELECT instructions,model_alias,tool_aliases,knowledge_refs,
                governance_policy_ref,version FROM tenant.agent_draft
                WHERE tenant_id=$1 AND application_id=$2 AND version=$3""",
                tenant_id,
                application_id,
                expected_version,
            )
            if draft is None:
                exists = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM tenant.agent_draft "
                    "WHERE tenant_id=$1 AND application_id=$2)",
                    tenant_id,
                    application_id,
                )
                if exists:
                    raise HTTPException(status_code=412, detail="Agent Draft version changed")
                raise HTTPException(status_code=404, detail="Agent Draft not found")
            issues = validate_draft_configuration(draft)
            result = {
                "valid": not issues,
                "draft_version": expected_version,
                "issues": [issue.model_dump(mode="json") for issue in issues],
            }
            await insert_audit(
                connection,
                principal,
                "agent_draft.validate",
                "ALLOW",
                target_type="agent_draft",
                target_id=str(application_id),
                tenant_id=tenant_id,
                details={
                    "valid": result["valid"],
                    "draft_version": expected_version,
                    "issue_codes": [issue.code for issue in issues],
                },
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_draft.validate",
                payload=request_payload,
                response=result,
            )
        response.headers["ETag"] = f'"{expected_version}"'
        return result

    @router.patch(
        "/{application_id}/draft",
        response_model=AgentDraftResponse,
        responses={
            **error_responses(400, 401, 403, 404, 409, 412, 422),
            200: {"headers": ETAG_HEADER},
        },
    )
    async def update_draft(
        tenant_id: UUID,
        application_id: UUID,
        payload: AgentDraftUpdate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "agent_draft.update",
            target_type="agent_draft",
            target_id=str(application_id),
        )
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            "expected_version": expected_version,
            **payload.model_dump(mode="json", exclude_unset=True),
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_draft.update",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                response.headers["ETag"] = f'"{replayed["version"]}"'
                return replayed
            row = await connection.fetchrow(
                """UPDATE tenant.agent_draft SET
                instructions=COALESCE($3,instructions),model_alias=COALESCE($4,model_alias),
                tool_aliases=COALESCE(CAST($5 AS jsonb),tool_aliases),
                knowledge_refs=COALESCE(CAST($6 AS jsonb),knowledge_refs),
                governance_policy_ref=CASE WHEN $7 THEN $8 ELSE governance_policy_ref END,
                version=version+1,updated_at=now()
                WHERE tenant_id=$1 AND application_id=$2 AND version=$9
                RETURNING tenant_id,application_id,instructions,model_alias,tool_aliases,
                knowledge_refs,governance_policy_ref,version,created_at,updated_at""",
                tenant_id,
                application_id,
                payload.instructions,
                payload.model_alias,
                json.dumps(payload.tool_aliases) if payload.tool_aliases is not None else None,
                json.dumps(payload.knowledge_refs) if payload.knowledge_refs is not None else None,
                "governance_policy_ref" in payload.model_fields_set,
                payload.governance_policy_ref,
                expected_version,
            )
            if row is None:
                exists = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM tenant.agent_draft "
                    "WHERE tenant_id=$1 AND application_id=$2)",
                    tenant_id,
                    application_id,
                )
                if exists:
                    raise HTTPException(status_code=412, detail="Agent Draft version changed")
                raise HTTPException(status_code=404, detail="Agent Draft not found")
            result = record_to_dict(row)
            await insert_audit(
                connection,
                principal,
                "agent_draft.update",
                "ALLOW",
                target_type="agent_draft",
                target_id=str(application_id),
                tenant_id=tenant_id,
                details={"version": result["version"]},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_draft.update",
                payload=request_payload,
                response=result,
            )
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @router.delete(
        "/{application_id}/draft",
        status_code=204,
        response_class=Response,
        response_model=None,
        responses=error_responses(400, 401, 403, 404, 409, 412, 422),
    )
    async def delete_draft(
        tenant_id: UUID,
        application_id: UUID,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> None:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "agent_draft.delete",
            target_type="agent_draft",
            target_id=str(application_id),
        )
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            "expected_version": expected_version,
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_draft.delete",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                return
            deleted = await connection.fetchval(
                """DELETE FROM tenant.agent_draft
                WHERE tenant_id=$1 AND application_id=$2 AND version=$3 RETURNING application_id""",
                tenant_id,
                application_id,
                expected_version,
            )
            if deleted is None:
                exists = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM tenant.agent_draft "
                    "WHERE tenant_id=$1 AND application_id=$2)",
                    tenant_id,
                    application_id,
                )
                if exists:
                    raise HTTPException(status_code=412, detail="Agent Draft version changed")
                raise HTTPException(status_code=404, detail="Agent Draft not found")
            await insert_audit(
                connection,
                principal,
                "agent_draft.delete",
                "ALLOW",
                target_type="agent_draft",
                target_id=str(application_id),
                tenant_id=tenant_id,
                details={"version": expected_version},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_draft.delete",
                payload=request_payload,
                response={"deleted": True},
            )

    @router.patch(
        "/{application_id}",
        response_model=AgentApplicationResponse,
        responses={
            **error_responses(400, 401, 403, 404, 409, 412, 422),
            200: {"headers": ETAG_HEADER},
        },
    )
    async def update_application(
        tenant_id: UUID,
        application_id: UUID,
        payload: AgentApplicationUpdate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "agent_application.update",
            target_type="agent_application",
            target_id=str(application_id),
        )
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            "expected_version": expected_version,
            **payload.model_dump(mode="json", exclude_unset=True),
        }
        try:
            async with database.tenant_transaction(tenant_id) as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="agent_application.update",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    response.headers["ETag"] = f'"{replayed["version"]}"'
                    return replayed
                row = await connection.fetchrow(
                    """UPDATE tenant.agent_application SET
                    slug=COALESCE($3,slug),name=COALESCE($4,name),
                    description=COALESCE($5,description),version=version+1,updated_at=now()
                    WHERE tenant_id=$1 AND id=$2 AND version=$6
                    RETURNING tenant_id,id,slug,name,description,version,created_at,updated_at""",
                    tenant_id,
                    application_id,
                    payload.slug,
                    payload.name,
                    payload.description,
                    expected_version,
                )
                if row is None:
                    exists = await connection.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM tenant.agent_application "
                        "WHERE tenant_id=$1 AND id=$2)",
                        tenant_id,
                        application_id,
                    )
                    if exists:
                        raise HTTPException(
                            status_code=412, detail="Agent application version changed"
                        )
                    raise HTTPException(status_code=404, detail="Agent application not found")
                result = record_to_dict(row)
                await insert_audit(
                    connection,
                    principal,
                    "agent_application.update",
                    "ALLOW",
                    target_type="agent_application",
                    target_id=str(application_id),
                    tenant_id=tenant_id,
                    details={"version": result["version"]},
                )
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="agent_application.update",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="Agent application already exists"
            ) from error
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @router.delete(
        "/{application_id}",
        status_code=204,
        response_class=Response,
        response_model=None,
        responses=error_responses(400, 401, 403, 404, 409, 412, 422),
    )
    async def delete_application(
        tenant_id: UUID,
        application_id: UUID,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> None:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "agent_application.delete",
            target_type="agent_application",
            target_id=str(application_id),
        )
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            "expected_version": expected_version,
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_application.delete",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                return
            deleted = await connection.fetchval(
                """DELETE FROM tenant.agent_application
                WHERE tenant_id=$1 AND id=$2 AND version=$3 RETURNING id""",
                tenant_id,
                application_id,
                expected_version,
            )
            if deleted is None:
                exists = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM tenant.agent_application "
                    "WHERE tenant_id=$1 AND id=$2)",
                    tenant_id,
                    application_id,
                )
                if exists:
                    raise HTTPException(status_code=412, detail="Agent application version changed")
                raise HTTPException(status_code=404, detail="Agent application not found")
            await insert_audit(
                connection,
                principal,
                "agent_application.delete",
                "ALLOW",
                target_type="agent_application",
                target_id=str(application_id),
                tenant_id=tenant_id,
                details={"version": expected_version},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="agent_application.delete",
                payload=request_payload,
                response={"deleted": True},
            )

    return router
