"""Unified operations console read surface over the versioned Admin API.

The console never talks to the database directly: every query and every
mutating op (dead-letter requeue) flows through these endpoints, which are
tenant-scoped, cursor-paginated, audited and expose stable error codes.
"""

from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.tenant_access import AccessMode, require_tenant_access

_PAGE_LIMIT = 200


def _encode_keyset(occurred_at: datetime, identifier: str) -> str:
    raw = f"{occurred_at.isoformat()}|{identifier}".encode()
    return urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_keyset(cursor: str) -> tuple[datetime, str]:
    try:
        raw = urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        stamp, _, identifier = raw.partition("|")
        return datetime.fromisoformat(stamp), identifier
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="INVALID_OPS_CURSOR") from None


class OpsSessionItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    tenant_id: UUID
    application_id: UUID
    version: int
    created_at: datetime
    updated_at: datetime


class OpsMemoryItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    tenant_id: UUID
    subject_id: str
    source_session_id: str
    content_preview: str
    is_valid: bool
    invalidated_at: datetime | None
    invalidation_reason: str | None
    created_at: datetime


class OpsArtifactItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    tenant_id: UUID
    subject_id: str
    execution_id: str
    filename: str
    media_type: str
    size_bytes: int
    classification: str
    created_at: datetime
    expires_at: datetime


class OpsDeadLetterItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    delivery_id: UUID
    tenant_id: UUID
    binding_id: UUID
    execution_id: str
    external_conversation_id: str
    attempts: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class OpsOperationEvidence(BaseModel):
    model_config = ConfigDict(frozen=True)

    proofs: list[dict[str, Any]] = []
    approval_status: str | None = None
    rollback_approval_status: str | None = None
    observation_ends_at: datetime | None = None


class OpsOperationItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: str
    id: UUID
    status: str
    attempts: int
    next_action_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    evidence: OpsOperationEvidence


class OpsPage(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    items: list[Any]
    next_cursor: str | None


def create_ops_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}/ops", tags=["ops"])

    async def _authorize(
        principal: Principal, tenant_id: UUID, mode: AccessMode, action: str, target_type: str
    ) -> None:
        await require_tenant_access(
            database, principal, tenant_id, mode, action, target_type=target_type
        )

    async def _log(
        principal: Principal,
        tenant_id: UUID,
        action: str,
        *,
        target_type: str,
        target_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        async with database.tenant_transaction(tenant_id) as connection:
            await insert_audit(
                connection,
                principal,
                action,
                "ALLOW",
                target_type=target_type,
                target_id=target_id,
                tenant_id=tenant_id,
                details=details,
            )

    @router.get(
        "/sessions",
        response_model=OpsPage,
        responses={**error_responses(401, 403)},
    )
    async def list_sessions(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        application_id: UUID | None = None,
        limit: Annotated[int, Query(ge=1, le=_PAGE_LIMIT)] = 50,
        cursor: str | None = None,
    ) -> OpsPage:
        await _authorize(principal, tenant_id, "read", "ops.read", "agent_session")
        conditions = ["tenant_id=$1"]
        args: list[Any] = [tenant_id]
        if application_id is not None:
            args.append(application_id)
            conditions.append(f"application_id=${len(args)}")
        if cursor is not None:
            stamp, identifier = _decode_keyset(cursor)
            args.extend([stamp, identifier])
            conditions.append(
                "(updated_at, id) < "
                f"(CAST(${len(args) - 1} AS timestamptz), CAST(${len(args)} AS text))"
            )
        args.append(limit + 1)
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT * FROM tenant.agent_session
                WHERE {" AND ".join(conditions)}
                ORDER BY updated_at DESC, id DESC LIMIT ${len(args)}""",
                *args,
            )
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = (
            _encode_keyset(rows[-1]["updated_at"], str(rows[-1]["id"]))
            if has_more and rows
            else None
        )
        return OpsPage(
            tenant_id=tenant_id,
            items=[
                OpsSessionItem(
                    id=row["id"],
                    tenant_id=row["tenant_id"],
                    application_id=row["application_id"],
                    version=row["version"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ],
            next_cursor=next_cursor,
        )

    @router.get(
        "/memories",
        response_model=OpsPage,
        responses={**error_responses(401, 403)},
    )
    async def list_memories(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        subject_id: str | None = None,
        valid_only: bool = False,
        limit: Annotated[int, Query(ge=1, le=_PAGE_LIMIT)] = 50,
        cursor: str | None = None,
    ) -> OpsPage:
        await _authorize(principal, tenant_id, "read", "ops.read", "memory_record")
        conditions = ["tenant_id=$1"]
        args: list[Any] = [tenant_id]
        if subject_id is not None:
            args.append(subject_id)
            conditions.append(f"subject_id=${len(args)}")
        if valid_only:
            conditions.append("is_valid")
        if cursor is not None:
            stamp, identifier = _decode_keyset(cursor)
            args.extend([stamp, identifier])
            conditions.append(
                f"(created_at, id) < (CAST(${len(args) - 1} AS timestamptz),"
                f" CAST(${len(args)} AS uuid))"
            )
        args.append(limit + 1)
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT * FROM tenant.memory_record
                WHERE {" AND ".join(conditions)}
                ORDER BY created_at DESC, id DESC LIMIT ${len(args)}""",
                *args,
            )
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = (
            _encode_keyset(rows[-1]["created_at"], str(rows[-1]["id"]))
            if has_more and rows
            else None
        )
        return OpsPage(
            tenant_id=tenant_id,
            items=[
                OpsMemoryItem(
                    id=row["id"],
                    tenant_id=row["tenant_id"],
                    subject_id=row["subject_id"],
                    source_session_id=row["source_session_id"],
                    content_preview=row["content"][:280],
                    is_valid=row["is_valid"],
                    invalidated_at=row["invalidated_at"],
                    invalidation_reason=row["invalidation_reason"],
                    created_at=row["created_at"],
                )
                for row in rows
            ],
            next_cursor=next_cursor,
        )

    @router.get(
        "/artifacts",
        response_model=OpsPage,
        responses={**error_responses(401, 403)},
    )
    async def list_artifacts(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        execution_id: str | None = None,
        limit: Annotated[int, Query(ge=1, le=_PAGE_LIMIT)] = 50,
        cursor: str | None = None,
    ) -> OpsPage:
        await _authorize(principal, tenant_id, "read", "ops.read", "artifact")
        conditions = ["tenant_id=$1"]
        args: list[Any] = [tenant_id]
        if execution_id is not None:
            args.append(execution_id)
            conditions.append(f"execution_id=${len(args)}")
        if cursor is not None:
            stamp, identifier = _decode_keyset(cursor)
            args.extend([stamp, identifier])
            conditions.append(
                f"(created_at, artifact_id) < (CAST(${len(args) - 1} AS timestamptz),"
                f" CAST(${len(args)} AS uuid))"
            )
        args.append(limit + 1)
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT * FROM tenant.artifact
                WHERE {" AND ".join(conditions)}
                ORDER BY created_at DESC, artifact_id DESC LIMIT ${len(args)}""",
                *args,
            )
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = (
            _encode_keyset(rows[-1]["created_at"], str(rows[-1]["artifact_id"]))
            if has_more and rows
            else None
        )
        # Metadata only: artifact bytes stay behind signed subject-constrained
        # capabilities and never flow through the console list surface.
        return OpsPage(
            tenant_id=tenant_id,
            items=[
                OpsArtifactItem(
                    id=row["artifact_id"],
                    tenant_id=row["tenant_id"],
                    subject_id=row["subject_id"],
                    execution_id=row["execution_id"],
                    filename=row["filename"],
                    media_type=row["media_type"],
                    size_bytes=row["size_bytes"],
                    classification=row["classification"],
                    created_at=row["created_at"],
                    expires_at=row["expires_at"],
                )
                for row in rows
            ],
            next_cursor=next_cursor,
        )

    @router.get(
        "/dead-letters",
        response_model=OpsPage,
        responses={**error_responses(401, 403)},
    )
    async def list_dead_letters(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=_PAGE_LIMIT)] = 50,
        cursor: str | None = None,
    ) -> OpsPage:
        await _authorize(principal, tenant_id, "read", "ops.read", "reply_delivery")
        conditions = ["delivery.tenant_id=$1", "delivery.status='DEAD_LETTER'"]
        args: list[Any] = [tenant_id]
        if cursor is not None:
            stamp, identifier = _decode_keyset(cursor)
            args.extend([stamp, UUID(identifier)])
            conditions.append(
                "(delivery.updated_at, delivery.delivery_id) < "
                f"(CAST(${len(args) - 1} AS timestamptz), CAST(${len(args)} AS uuid))"
            )
        args.append(limit + 1)
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT delivery.*, latest.error_code AS last_error
                FROM tenant.reply_delivery delivery
                LEFT JOIN LATERAL (
                    SELECT error_code FROM tenant.reply_delivery_attempt attempt
                    WHERE attempt.tenant_id=delivery.tenant_id
                      AND attempt.delivery_id=delivery.delivery_id
                      AND error_code IS NOT NULL
                    ORDER BY attempt_no DESC LIMIT 1
                ) latest ON true
                WHERE {" AND ".join(conditions)}
                ORDER BY delivery.updated_at DESC, delivery.delivery_id DESC LIMIT ${len(args)}""",
                *args,
            )
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = (
            _encode_keyset(rows[-1]["updated_at"], str(rows[-1]["delivery_id"]))
            if has_more and rows
            else None
        )
        # Delivery content stays out of the console: only routing facts and
        # the failure code are surfaced to operators.
        return OpsPage(
            tenant_id=tenant_id,
            items=[
                OpsDeadLetterItem(
                    delivery_id=row["delivery_id"],
                    tenant_id=row["tenant_id"],
                    binding_id=row["binding_id"],
                    execution_id=row["execution_id"],
                    external_conversation_id=row["external_conversation_id"],
                    attempts=row["attempts"],
                    last_error=row["last_error"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ],
            next_cursor=next_cursor,
        )

    @router.post(
        "/dead-letters/{delivery_id}/retries",
        response_model=dict[str, str],
        responses={**error_responses(401, 403, 404, 409)},
    )
    async def requeue_dead_letter(
        tenant_id: UUID,
        delivery_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, str]:
        await _authorize(principal, tenant_id, "write", "ops.requeue", "reply_delivery")
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """UPDATE tenant.reply_delivery
                SET status='QUEUED', updated_at=now()
                WHERE tenant_id=$1 AND delivery_id=$2 AND status='DEAD_LETTER'
                RETURNING delivery_id""",
                tenant_id,
                delivery_id,
            )
            if row is None:
                exists = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM tenant.reply_delivery "
                    "WHERE tenant_id=$1 AND delivery_id=$2)",
                    tenant_id,
                    delivery_id,
                )
                if not exists:
                    raise HTTPException(status_code=404, detail="DEAD_LETTER_NOT_FOUND")
                raise HTTPException(status_code=409, detail="DEAD_LETTER_NOT_RETRYABLE")
            await insert_audit(
                connection,
                principal,
                "ops.dead_letter.requeue",
                "ALLOW",
                target_type="reply_delivery",
                target_id=str(delivery_id),
                tenant_id=tenant_id,
            )
        return {"delivery_id": str(delivery_id), "status": "QUEUED"}

    @router.get(
        "/operations",
        response_model=OpsPage,
        responses={**error_responses(401, 403)},
    )
    async def list_operations(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        kind: str | None = None,
        limit: Annotated[int, Query(ge=1, le=_PAGE_LIMIT)] = 100,
    ) -> OpsPage:
        await _authorize(principal, tenant_id, "read", "ops.read", "operation")
        items: list[OpsOperationItem] = []
        async with database.tenant_transaction(tenant_id) as connection:
            if kind is None or kind == "CONTENT_DELETION":
                requests = await connection.fetch(
                    """SELECT request.*, count(proof.backend)::int AS proof_count
                    FROM tenant.content_deletion_request request
                    LEFT JOIN tenant.content_deletion_proof proof
                      ON proof.tenant_id=request.tenant_id AND proof.request_id=request.id
                    WHERE request.tenant_id=$1
                    GROUP BY request.tenant_id, request.id
                    ORDER BY request.created_at DESC LIMIT $2""",
                    tenant_id,
                    limit,
                )
                for row in requests:
                    proofs = await connection.fetch(
                        """SELECT backend, deleted_count, verified, evidence_digest, completed_at
                        FROM tenant.content_deletion_proof
                        WHERE tenant_id=$1 AND request_id=$2 ORDER BY backend""",
                        tenant_id,
                        row["id"],
                    )
                    items.append(
                        OpsOperationItem(
                            kind="CONTENT_DELETION",
                            id=row["id"],
                            status=row["status"],
                            attempts=row["attempts"],
                            next_action_at=row["next_attempt_at"],
                            last_error=row["last_error"],
                            created_at=row["created_at"],
                            updated_at=row["completed_at"] or row["created_at"],
                            evidence=OpsOperationEvidence(
                                proofs=[
                                    {
                                        "backend": proof["backend"],
                                        "deleted_count": proof["deleted_count"],
                                        "verified": proof["verified"],
                                        "evidence_digest": proof["evidence_digest"],
                                        "completed_at": proof["completed_at"].isoformat(),
                                    }
                                    for proof in proofs
                                ]
                            ),
                        )
                    )
            if kind is None or kind == "STORAGE_MIGRATION":
                migrations = await connection.fetch(
                    """SELECT * FROM tenant.storage_migration
                    WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT $2""",
                    tenant_id,
                    limit,
                )
                for row in migrations:
                    items.append(
                        OpsOperationItem(
                            kind="STORAGE_MIGRATION",
                            id=row["id"],
                            status=row["state"],
                            attempts=row["version"],
                            next_action_at=row["observation_ends_at"],
                            last_error=None,
                            created_at=row["created_at"],
                            updated_at=row["updated_at"],
                            evidence=OpsOperationEvidence(
                                approval_status=row["approval_status"],
                                rollback_approval_status=row["rollback_approval_status"],
                                observation_ends_at=row["observation_ends_at"],
                                proofs=[
                                    {
                                        "validation": row["validation"],
                                    }
                                ],
                            ),
                        )
                    )
        items.sort(key=lambda item: item.created_at, reverse=True)
        return OpsPage(tenant_id=tenant_id, items=items[:limit], next_cursor=None)

    return router
