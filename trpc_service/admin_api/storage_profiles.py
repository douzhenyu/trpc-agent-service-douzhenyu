"""Tenant Storage Profile configuration at the Admin API boundary."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response
from sqlalchemy.exc import IntegrityError

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.http_contract import ETAG_HEADER, error_responses
from trpc_service.admin_api.idempotency import remember, replay_for
from trpc_service.admin_api.schemas import (
    StorageProfileCreate,
    StorageProfileList,
    StorageProfileResponse,
)
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.governance import DataClassification
from trpc_service.ids import uuid7
from trpc_service.storage import StorageProfile, StorageProfileError, StorageRouter

_COLUMNS = (
    "id,tenant_id,alias,classification,worker_pool,encryption_key_ref,backends,active,"
    "version,created_at,updated_at"
)


def _result(row: Any) -> dict[str, Any]:
    result = record_to_dict(row)
    if isinstance(result["backends"], str):
        result["backends"] = json.loads(result["backends"])
    return result


def _validate_profile(tenant_id: UUID, payload: StorageProfileCreate) -> None:
    try:
        prefix = f"vault://tenant/{tenant_id}/"
        if not payload.encryption_key_ref.startswith(prefix) or any(
            not backend.secret_ref.startswith(prefix) for backend in payload.backends
        ):
            raise StorageProfileError("STORAGE_SECRET_REF_SCOPE_INVALID")
        StorageRouter(
            StorageProfile(
                tenant_id=str(tenant_id),
                alias=payload.alias,
                classification=DataClassification(payload.classification),
                worker_pool=payload.worker_pool,
                encryption_key_ref=payload.encryption_key_ref,
                backends=tuple(payload.backends),
            )
        )
    except (StorageProfileError, ValueError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


async def _claim_dedicated_resources(
    connection: Any, tenant_id: UUID, payload: StorageProfileCreate
) -> None:
    """Claim endpoint and worker identities so dedicated tenants cannot overlap.

    Fingerprints deliberately keep control-plane tables free of provider URLs and
    credentials; the full endpoint remains only in the tenant-scoped profile.
    """

    resources = [
        (backend.kind.value, backend.endpoint)
        for backend in payload.backends
        if backend.dedicated
    ]
    if payload.worker_pool != "shared-workers":
        resources.append(("WORKER_POOL", payload.worker_pool))
    for kind, identity in resources:
        fingerprint = sha256(identity.encode()).hexdigest()
        await connection.execute(
            """INSERT INTO platform.storage_resource_claim
            (resource_kind,resource_fingerprint,tenant_id) VALUES ($1,$2,$3)
            ON CONFLICT (resource_kind,resource_fingerprint) DO NOTHING""",
            kind,
            fingerprint,
            tenant_id,
        )
        owner = await connection.fetchval(
            """SELECT tenant_id FROM platform.storage_resource_claim
            WHERE resource_kind=$1 AND resource_fingerprint=$2""",
            kind,
            fingerprint,
        )
        if owner is None or UUID(str(owner)) != tenant_id:
            raise HTTPException(status_code=409, detail="STORAGE_RESOURCE_ALREADY_CLAIMED")


def create_storage_profile_router(database: Database) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1/tenants/{tenant_id}/storage-profiles", tags=["storage-profiles"]
    )

    @router.post(
        "",
        response_model=StorageProfileResponse,
        status_code=201,
        responses={**error_responses(401, 403, 409, 422), 201: {"headers": ETAG_HEADER}},
    )
    async def create_profile(
        tenant_id: UUID,
        payload: StorageProfileCreate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "storage_profile.create",
            target_type="storage_profile",
        )
        _validate_profile(tenant_id, payload)
        request_payload = {"tenant_id": str(tenant_id), **payload.model_dump(mode="json")}
        profile_id = uuid7()
        try:
            async with database.tenant_transaction(tenant_id) as connection:
                replayed = await replay_for(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="storage_profile.create",
                    payload=request_payload,
                )
                if replayed is not None:
                    response.headers["Idempotency-Replayed"] = "true"
                    response.headers["ETag"] = f'"{replayed["version"]}"'
                    return replayed
                if payload.activate and await connection.fetchval(
                    """SELECT EXISTS(
                    SELECT 1 FROM tenant.storage_profile WHERE tenant_id=$1 AND active
                    )""",
                    tenant_id,
                ):
                    # ADR-0015 prohibits a direct source/destination switch.
                    # A later migration workflow owns backfill, catch-up,
                    # verification, observation and rollback.
                    raise HTTPException(status_code=409, detail="STORAGE_MIGRATION_REQUIRED")
                await _claim_dedicated_resources(connection, tenant_id, payload)
                row = await connection.fetchrow(
                    f"""INSERT INTO tenant.storage_profile
                    (tenant_id,id,alias,classification,worker_pool,encryption_key_ref,backends,active)
                    VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS jsonb),$8) RETURNING {_COLUMNS}""",
                    tenant_id,
                    profile_id,
                    payload.alias,
                    payload.classification,
                    payload.worker_pool,
                    payload.encryption_key_ref,
                    json.dumps([backend.model_dump(mode="json") for backend in payload.backends]),
                    payload.activate,
                )
                assert row is not None
                result = _result(row)
                await insert_audit(
                    connection,
                    principal,
                    "storage_profile.create",
                    "ALLOW",
                    target_type="storage_profile",
                    target_id=str(profile_id),
                    tenant_id=tenant_id,
                    details={"active": payload.activate},
                )
                await remember(
                    connection,
                    actor=principal.subject,
                    key=key,
                    operation="storage_profile.create",
                    payload=request_payload,
                    response=result,
                )
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="storage profile alias already exists"
            ) from error
        response.headers["ETag"] = '"1"'
        return result

    @router.get("", response_model=StorageProfileList, responses=error_responses(401, 403, 422))
    async def list_profiles(
        tenant_id: UUID, principal: Annotated[Principal, Depends(principal_from_request)]
    ) -> dict[str, list[dict[str, Any]]]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "storage_profile.list",
            target_type="storage_profile",
        )
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"SELECT {_COLUMNS} FROM tenant.storage_profile WHERE tenant_id=$1 ORDER BY id",
                tenant_id,
            )
        return {"items": [_result(row) for row in rows]}

    @router.get(
        "/active",
        response_model=StorageProfileResponse,
        responses=error_responses(401, 403, 404, 422),
    )
    async def active_profile(
        tenant_id: UUID, principal: Annotated[Principal, Depends(principal_from_request)]
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "storage_profile.get",
            target_type="storage_profile",
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                f"SELECT {_COLUMNS} FROM tenant.storage_profile WHERE tenant_id=$1 AND active",
                tenant_id,
            )
        if row is None:
            raise HTTPException(status_code=404, detail="active storage profile not found")
        return _result(row)

    return router
