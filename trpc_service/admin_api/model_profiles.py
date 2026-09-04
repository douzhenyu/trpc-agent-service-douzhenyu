"""Tenant-scoped model profile management without credential material."""

from __future__ import annotations

import json
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy.exc import IntegrityError

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.http_contract import ETAG_HEADER, error_responses
from trpc_service.admin_api.idempotency import remember, replay_for
from trpc_service.admin_api.pagination import decode_cursor, encode_cursor
from trpc_service.admin_api.preconditions import parse_if_match
from trpc_service.admin_api.schemas import (
    ModelProfileCreate,
    ModelProfileList,
    ModelProfileResponse,
    ModelProfileUpdate,
)
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.ids import uuid7

_PROFILE_COLUMNS = """id,tenant_id,alias,provider_model,endpoint_url,secret_ref,
data_classification,region,fallback_aliases,requests_per_minute,version,created_at,updated_at"""


def _validate_secret_tenant(secret_ref: str, tenant_id: UUID) -> None:
    if not secret_ref.startswith(f"vault://tenant/{tenant_id}/"):
        raise HTTPException(
            status_code=422, detail="secret_ref must be scoped to the target tenant"
        )


def _validate_fallbacks(alias: str, fallbacks: list[str]) -> None:
    if alias in fallbacks or len(set(fallbacks)) != len(fallbacks):
        raise HTTPException(
            status_code=422, detail="fallback aliases must be unique and exclude the profile alias"
        )


def create_model_profile_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}/model-profiles", tags=["model-profiles"])

    @router.post(
        "",
        response_model=ModelProfileResponse,
        status_code=201,
        responses={**error_responses(401, 403, 409, 422), 201: {"headers": ETAG_HEADER}},
    )
    async def create_profile(
        tenant_id: UUID,
        payload: ModelProfileCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "model_profile.create",
            target_type="model_profile",
        )
        _validate_secret_tenant(payload.secret_ref, tenant_id)
        _validate_fallbacks(payload.alias, payload.fallback_aliases)
        request_payload = {"tenant_id": str(tenant_id), **payload.model_dump(mode="json")}
        profile_id = uuid7()
        try:
            async with database.tenant_transaction(tenant_id) as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="model_profile.create",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    response.headers["ETag"] = f'"{replayed["version"]}"'
                    return replayed
                row = await connection.fetchrow(
                    f"""INSERT INTO tenant.model_profile
                    (tenant_id,id,alias,provider_model,endpoint_url,secret_ref,data_classification,
                    region,fallback_aliases,requests_per_minute)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS jsonb),$10)
                    RETURNING {_PROFILE_COLUMNS}""",
                    tenant_id,
                    profile_id,
                    payload.alias,
                    payload.provider_model,
                    payload.endpoint_url,
                    payload.secret_ref,
                    payload.data_classification,
                    payload.region,
                    json.dumps(payload.fallback_aliases),
                    payload.requests_per_minute,
                )
                assert row is not None
                result = record_to_dict(row)
                await insert_audit(
                    connection,
                    principal,
                    "model_profile.create",
                    "ALLOW",
                    target_type="model_profile",
                    target_id=str(profile_id),
                    tenant_id=tenant_id,
                )
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="model_profile.create",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="model profile alias already exists"
            ) from error
        response.headers["ETag"] = '"1"'
        return result

    @router.get("", response_model=ModelProfileList, responses=error_responses(400, 401, 403, 422))
    async def list_profiles(
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
            "model_profile.list",
            target_type="model_profile",
        )
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT {_PROFILE_COLUMNS} FROM tenant.model_profile
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
        "/{alias}",
        response_model=ModelProfileResponse,
        responses={**error_responses(401, 403, 404, 422), 200: {"headers": ETAG_HEADER}},
    )
    async def get_profile(
        tenant_id: UUID,
        alias: str,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "model_profile.get",
            target_type="model_profile",
            target_id=alias,
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                f"""SELECT {_PROFILE_COLUMNS} FROM tenant.model_profile
                WHERE tenant_id=$1 AND alias=$2""",
                tenant_id,
                alias,
            )
        if row is None:
            raise HTTPException(status_code=404, detail="model profile not found")
        result = record_to_dict(row)
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @router.patch(
        "/{alias}",
        response_model=ModelProfileResponse,
        responses={
            **error_responses(400, 401, 403, 404, 409, 412, 422),
            200: {"headers": ETAG_HEADER},
        },
    )
    async def update_profile(
        tenant_id: UUID,
        alias: str,
        payload: ModelProfileUpdate,
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
            "model_profile.update",
            target_type="model_profile",
            target_id=alias,
        )
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        if payload.secret_ref is not None:
            _validate_secret_tenant(payload.secret_ref, tenant_id)
        if payload.fallback_aliases is not None:
            _validate_fallbacks(alias, payload.fallback_aliases)
        request_payload = {
            "tenant_id": str(tenant_id),
            "alias": alias,
            "expected_version": expected_version,
            **payload.model_dump(mode="json", exclude_unset=True),
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="model_profile.update",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                response.headers["ETag"] = f'"{replayed["version"]}"'
                return replayed
            row = await connection.fetchrow(
                f"""UPDATE tenant.model_profile SET
                provider_model=COALESCE($3,provider_model),endpoint_url=COALESCE($4,endpoint_url),
                secret_ref=COALESCE($5,secret_ref),data_classification=COALESCE($6,data_classification),
                region=COALESCE($7,region),
                fallback_aliases=COALESCE(CAST($8 AS jsonb),fallback_aliases),
                requests_per_minute=COALESCE($9,requests_per_minute),version=version+1,updated_at=now()
                WHERE tenant_id=$1 AND alias=$2 AND version=$10
                RETURNING {_PROFILE_COLUMNS}""",
                tenant_id,
                alias,
                payload.provider_model,
                payload.endpoint_url,
                payload.secret_ref,
                payload.data_classification,
                payload.region,
                json.dumps(payload.fallback_aliases)
                if payload.fallback_aliases is not None
                else None,
                payload.requests_per_minute,
                expected_version,
            )
            if row is None:
                exists = await connection.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM tenant.model_profile
                    WHERE tenant_id=$1 AND alias=$2)""",
                    tenant_id,
                    alias,
                )
                if exists:
                    raise HTTPException(status_code=412, detail="model profile version changed")
                raise HTTPException(status_code=404, detail="model profile not found")
            result = record_to_dict(row)
            await insert_audit(
                connection,
                principal,
                "model_profile.update",
                "ALLOW",
                target_type="model_profile",
                target_id=str(result["id"]),
                tenant_id=tenant_id,
                details={"version": result["version"]},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="model_profile.update",
                payload=request_payload,
                response=result,
            )
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @router.delete(
        "/{alias}",
        status_code=204,
        response_class=Response,
        response_model=None,
        responses=error_responses(400, 401, 403, 404, 409, 412, 422),
    )
    async def delete_profile(
        tenant_id: UUID,
        alias: str,
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
            "model_profile.delete",
            target_type="model_profile",
            target_id=alias,
        )
        try:
            expected_version = parse_if_match(if_match)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        request_payload = {
            "tenant_id": str(tenant_id),
            "alias": alias,
            "expected_version": expected_version,
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="model_profile.delete",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                return
            deleted = await connection.fetchval(
                """DELETE FROM tenant.model_profile WHERE tenant_id=$1 AND alias=$2 AND version=$3
                RETURNING id""",
                tenant_id,
                alias,
                expected_version,
            )
            if deleted is None:
                exists = await connection.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM tenant.model_profile
                    WHERE tenant_id=$1 AND alias=$2)""",
                    tenant_id,
                    alias,
                )
                if exists:
                    raise HTTPException(status_code=412, detail="model profile version changed")
                raise HTTPException(status_code=404, detail="model profile not found")
            await insert_audit(
                connection,
                principal,
                "model_profile.delete",
                "ALLOW",
                target_type="model_profile",
                target_id=str(deleted),
                tenant_id=tenant_id,
                details={"version": expected_version},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="model_profile.delete",
                payload=request_payload,
                response={"deleted": True},
            )

    return router
