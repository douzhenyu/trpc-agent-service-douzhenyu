"""Tenant retention, Legal Hold and deletion-request administration."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.schemas import (
    DeletionRequestCreate,
    DeletionRequestResponse,
    LegalHoldCreate,
    LegalHoldResponse,
    RetentionPolicyChangeResponse,
    RetentionPolicyResponse,
    RetentionPolicyUpdate,
)
from trpc_service.admin_api.tenant_access import require_tenant_access, require_tenant_admin
from trpc_service.content_lifecycle import (
    BACKUP_ERASURE_WINDOW,
    PRIMARY_ERASURE_WINDOW,
    RetentionPolicy,
)
from trpc_service.ids import uuid7


def _retention_result(tenant_id: UUID, row: Any | None) -> dict[str, Any]:
    values = RetentionPolicy().model_dump() if row is None else record_to_dict(row)
    return {
        "tenant_id": tenant_id,
        **values,
        "version": values.get("version", 1),
        "updated_at": values.get("updated_at", datetime.now(UTC)),
    }


def _retention_change_result(row: Any) -> dict[str, Any]:
    return record_to_dict(row)


async def _deletion_result(connection: Any, tenant_id: UUID, request_id: UUID) -> dict[str, Any]:
    row = await connection.fetchrow(
        "SELECT * FROM tenant.content_deletion_request WHERE tenant_id=$1 AND id=$2",
        tenant_id,
        request_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="DELETION_REQUEST_NOT_FOUND")
    result = record_to_dict(row)
    proofs = await connection.fetch(
        "SELECT backend,deleted_count,verified,evidence_digest,completed_at "
        "FROM tenant.content_deletion_proof WHERE tenant_id=$1 AND request_id=$2 ORDER BY backend",
        tenant_id,
        request_id,
    )
    result["proofs"] = [record_to_dict(proof) for proof in proofs]
    return result


def create_content_lifecycle_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["content-lifecycle"])

    @router.get(
        "/retention-policy",
        response_model=RetentionPolicyResponse,
        responses=error_responses(401, 403),
    )
    async def get_retention_policy(
        tenant_id: UUID, principal: Annotated[Principal, Depends(principal_from_request)]
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "retention_policy.get",
            target_type="retention_policy",
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM tenant.content_retention_policy WHERE tenant_id=$1", tenant_id
            )
        return _retention_result(tenant_id, row)

    @router.put(
        "/retention-policy",
        response_model=RetentionPolicyChangeResponse,
        status_code=202,
        responses=error_responses(401, 403, 409, 422),
    )
    async def update_retention_policy(
        tenant_id: UUID,
        payload: RetentionPolicyUpdate,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "retention_policy.update",
            target_type="retention_policy",
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """INSERT INTO tenant.content_retention_change
                (tenant_id,inbound_payload_days,session_days,memory_days,artifact_days,
                 idempotency_tombstone_days,audit_days,backup_days,id,status,initiator)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'PENDING_APPROVAL',$10)
                RETURNING *""",
                tenant_id,
                payload.inbound_payload_days,
                payload.session_days,
                payload.memory_days,
                payload.artifact_days,
                payload.idempotency_tombstone_days,
                payload.audit_days,
                payload.backup_days,
                uuid7(),
                principal.subject,
            )
            assert row is not None
            await insert_audit(
                connection,
                principal,
                "retention_policy.propose",
                "ALLOW",
                tenant_id=tenant_id,
                target_type="retention_policy",
            )
        return _retention_change_result(row)

    @router.post(
        "/retention-policy/{change_id}/approve",
        response_model=RetentionPolicyResponse,
        responses=error_responses(401, 403, 404, 409),
    )
    async def approve_retention_policy(
        tenant_id: UUID,
        change_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "retention_policy.approve",
            target_type="retention_policy_change",
            target_id=str(change_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            change = await connection.fetchrow(
                "SELECT * FROM tenant.content_retention_change WHERE tenant_id=$1 AND id=$2",
                tenant_id,
                change_id,
            )
            if change is None:
                raise HTTPException(status_code=404, detail="RETENTION_CHANGE_NOT_FOUND")
            if change["initiator"] == principal.subject:
                raise HTTPException(status_code=409, detail="RETENTION_POLICY_SELF_APPROVAL")
            approved = await connection.fetchrow(
                """UPDATE tenant.content_retention_change
                SET status='APPROVED',approver=$3,approved_at=now()
                WHERE tenant_id=$1 AND id=$2 AND status='PENDING_APPROVAL' RETURNING *""",
                tenant_id,
                change_id,
                principal.subject,
            )
            if approved is None:
                raise HTTPException(status_code=409, detail="RETENTION_CHANGE_NOT_PENDING")
            policy = await connection.fetchrow(
                """INSERT INTO tenant.content_retention_policy
                (tenant_id,inbound_payload_days,session_days,memory_days,artifact_days,
                 idempotency_tombstone_days,audit_days,backup_days)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (tenant_id) DO UPDATE SET
                  inbound_payload_days=EXCLUDED.inbound_payload_days,
                  session_days=EXCLUDED.session_days,memory_days=EXCLUDED.memory_days,
                  artifact_days=EXCLUDED.artifact_days,
                  idempotency_tombstone_days=EXCLUDED.idempotency_tombstone_days,
                  audit_days=EXCLUDED.audit_days,backup_days=EXCLUDED.backup_days,
                  version=tenant.content_retention_policy.version+1,updated_at=now()
                RETURNING *""",
                tenant_id,
                change["inbound_payload_days"],
                change["session_days"],
                change["memory_days"],
                change["artifact_days"],
                change["idempotency_tombstone_days"],
                change["audit_days"],
                change["backup_days"],
            )
            assert policy is not None
            await insert_audit(
                connection,
                principal,
                "retention_policy.approve",
                "ALLOW",
                tenant_id=tenant_id,
                target_type="retention_policy_change",
                target_id=str(change_id),
                details={"initiator": change["initiator"]},
            )
        return _retention_result(tenant_id, policy)

    @router.post(
        "/legal-holds",
        response_model=LegalHoldResponse,
        status_code=201,
        responses=error_responses(401, 403, 422),
    )
    async def create_legal_hold(
        tenant_id: UUID,
        payload: LegalHoldCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database, principal, tenant_id, "legal_hold.create", target_type="legal_hold"
        )
        hold_id = uuid7()
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """INSERT INTO tenant.legal_hold (tenant_id,id,scope,reason,status,initiator)
                VALUES ($1,$2,'TENANT',$3,'PENDING_APPROVAL',$4) RETURNING *""",
                tenant_id,
                hold_id,
                payload.reason,
                principal.subject,
            )
            assert row is not None
            await insert_audit(
                connection,
                principal,
                "legal_hold.create",
                "ALLOW",
                tenant_id=tenant_id,
                target_type="legal_hold",
                target_id=str(hold_id),
            )
        return record_to_dict(row)

    @router.post(
        "/legal-holds/{hold_id}/approve",
        response_model=LegalHoldResponse,
        responses=error_responses(401, 403, 404, 409),
    )
    async def approve_legal_hold(
        tenant_id: UUID,
        hold_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "legal_hold.approve",
            target_type="legal_hold",
            target_id=str(hold_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            hold = await connection.fetchrow(
                "SELECT * FROM tenant.legal_hold WHERE tenant_id=$1 AND id=$2", tenant_id, hold_id
            )
            if hold is None:
                raise HTTPException(status_code=404, detail="LEGAL_HOLD_NOT_FOUND")
            if hold["initiator"] == principal.subject:
                raise HTTPException(status_code=409, detail="LEGAL_HOLD_SELF_APPROVAL")
            row = await connection.fetchrow(
                """UPDATE tenant.legal_hold SET status='ACTIVE',approver=$3,activated_at=now()
                WHERE tenant_id=$1 AND id=$2 AND status='PENDING_APPROVAL' RETURNING *""",
                tenant_id,
                hold_id,
                principal.subject,
            )
            if row is None:
                raise HTTPException(status_code=409, detail="LEGAL_HOLD_NOT_PENDING")
            await insert_audit(
                connection,
                principal,
                "legal_hold.approve",
                "ALLOW",
                tenant_id=tenant_id,
                target_type="legal_hold",
                target_id=str(hold_id),
                details={"initiator": hold["initiator"]},
            )
        return record_to_dict(row)

    @router.post(
        "/legal-holds/{hold_id}/release",
        response_model=LegalHoldResponse,
        responses=error_responses(401, 403, 404, 409),
    )
    async def release_legal_hold(
        tenant_id: UUID,
        hold_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "legal_hold.release",
            target_type="legal_hold",
            target_id=str(hold_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            hold = await connection.fetchrow(
                "SELECT * FROM tenant.legal_hold WHERE tenant_id=$1 AND id=$2", tenant_id, hold_id
            )
            if hold is None:
                raise HTTPException(status_code=404, detail="LEGAL_HOLD_NOT_FOUND")
            if principal.subject in {hold["initiator"], hold["approver"]}:
                raise HTTPException(status_code=409, detail="LEGAL_HOLD_SELF_RELEASE")
            row = await connection.fetchrow(
                """UPDATE tenant.legal_hold SET status='RELEASED',released_at=now()
                WHERE tenant_id=$1 AND id=$2 AND status='ACTIVE' RETURNING *""",
                tenant_id,
                hold_id,
            )
            if row is None:
                raise HTTPException(status_code=409, detail="LEGAL_HOLD_NOT_ACTIVE")
            await insert_audit(
                connection,
                principal,
                "legal_hold.release",
                "ALLOW",
                tenant_id=tenant_id,
                target_type="legal_hold",
                target_id=str(hold_id),
                details={"initiator": hold["initiator"], "approver": hold["approver"]},
            )
        return record_to_dict(row)

    @router.post(
        "/deletion-requests",
        response_model=DeletionRequestResponse,
        status_code=201,
        responses=error_responses(401, 403, 409, 422),
    )
    async def create_deletion_request(
        tenant_id: UUID,
        payload: DeletionRequestCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "content_deletion.create",
            target_type="content_deletion_request",
        )
        request_id = uuid7()
        now = datetime.now(UTC)
        async with database.tenant_transaction(tenant_id) as connection:
            if await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM tenant.legal_hold "
                "WHERE tenant_id=$1 AND status='ACTIVE')",
                tenant_id,
            ):
                raise HTTPException(status_code=409, detail="LEGAL_HOLD_ACTIVE")
            await connection.execute(
                """INSERT INTO tenant.content_deletion_request
                (tenant_id,id,requested_by,reason,status,primary_due_at,backup_due_at,next_attempt_at)
                VALUES ($1,$2,$3,$4,'PENDING',$5,$6,$7)""",
                tenant_id,
                request_id,
                principal.subject,
                payload.reason,
                now + PRIMARY_ERASURE_WINDOW,
                now + BACKUP_ERASURE_WINDOW,
                now,
            )
            await insert_audit(
                connection,
                principal,
                "content_deletion.create",
                "ALLOW",
                tenant_id=tenant_id,
                target_type="content_deletion_request",
                target_id=str(request_id),
            )
            return await _deletion_result(connection, tenant_id, request_id)

    @router.get(
        "/deletion-requests/{request_id}",
        response_model=DeletionRequestResponse,
        responses=error_responses(401, 403, 404),
    )
    async def get_deletion_request(
        tenant_id: UUID,
        request_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "content_deletion.get",
            target_type="content_deletion_request",
            target_id=str(request_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            return await _deletion_result(connection, tenant_id, request_id)

    return router
