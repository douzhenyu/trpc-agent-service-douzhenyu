"""Audit query, export, correction and break-glass endpoints — all self-auditing.

Every read of the audit trail writes an audit event about that read, and an
emergency (break-glass) principal additionally stamps an explicit
audit.break_glass evidence event. Corrections never mutate history: they
append a chained correction event referencing the original.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.audit_chain.chain import append_to_chain, fingerprint, occurred_epoch_micros
from trpc_service.ids import uuid7


class CorrectionRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    explanation: str = Field(min_length=1, max_length=2000)


class AuditEventResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    occurred_at: datetime
    tenant_id: UUID | None
    actor: str
    auth_method: str
    action: str
    decision: str
    target_type: str | None
    target_id: str | None
    trace_id: str | None
    chain_index: int | None
    event_hash: str | None


class AuditEventList(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    events: list[AuditEventResponse]


def _worm_and_key() -> tuple[str, Any]:
    """Signing key and WORM archive from the environment (fail-closed)."""

    signing_key = os.environ.get("AUDIT_SIGNING_KEY", "")
    if not signing_key:
        raise HTTPException(status_code=503, detail="AUDIT_SIGNING_KEY is not configured")
    from trpc_service.audit_chain.worm import MemoryWormArchive, S3ObjectLockWormArchive

    endpoint = os.environ.get("WORM_S3_ENDPOINT", "")
    if endpoint:
        worm: Any = S3ObjectLockWormArchive(
            endpoint_url=endpoint,
            access_key_id=os.environ.get("WORM_S3_ACCESS_KEY", ""),
            secret_access_key=os.environ.get("WORM_S3_SECRET_KEY", ""),
            region=os.environ.get("WORM_S3_REGION", "us-east-1"),
        )
    else:
        # No object store configured: manifests stay local-only, which the
        # deployment contract treats as incomplete rather than silent.
        worm = MemoryWormArchive()
    return signing_key, worm


def create_audit_query_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["audit"])

    async def _self_audit(
        principal: Principal,
        tenant_id: UUID,
        action: str,
        *,
        target_type: str | None = None,
        target_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Every audit-surface access writes evidence about itself."""

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
            if principal.auth_method == "emergency":
                await insert_audit(
                    connection,
                    principal,
                    "audit.break_glass",
                    "ALLOW",
                    target_type=target_type or "audit_trail",
                    tenant_id=tenant_id,
                    details={"via": action},
                )

    @router.get(
        "/audit-events",
        response_model=AuditEventList,
        responses={**error_responses(401, 403)},
    )
    async def query_audit_events(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        actor: str | None = None,
        target_type: str | None = None,
        target_id: str | None = None,
        trace_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> AuditEventList:
        await require_tenant_access(
            database, principal, tenant_id, "read", "audit.query", target_type="audit_event"
        )
        conditions = ["tenant_id=$1"]
        args: list[Any] = [tenant_id]
        for field, value in (
            ("actor", actor),
            ("target_type", target_type),
            ("target_id", target_id),
            ("trace_id", trace_id),
        ):
            if value is not None:
                args.append(value)
                conditions.append(f"{field}=${len(args)}")
        if since is not None:
            args.append(since)
            conditions.append(f"occurred_at >= ${len(args)}")
        if until is not None:
            args.append(until)
            conditions.append(f"occurred_at <= ${len(args)}")
        args.append(limit)
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT * FROM platform.audit_event
                WHERE {" AND ".join(conditions)}
                ORDER BY occurred_at DESC LIMIT ${len(args)}""",
                *args,
            )
        await _self_audit(
            principal,
            tenant_id,
            "audit.query",
            details={"filters": {"actor": actor, "trace_id": trace_id}, "returned": len(rows)},
        )
        return AuditEventList(
            tenant_id=tenant_id,
            events=[
                AuditEventResponse(
                    id=row["id"],
                    occurred_at=row["occurred_at"],
                    tenant_id=row["tenant_id"],
                    actor=row["actor"],
                    auth_method=row["auth_method"],
                    action=row["action"],
                    decision=row["decision"],
                    target_type=row["target_type"],
                    target_id=row["target_id"],
                    trace_id=row["trace_id"],
                    chain_index=row["chain_index"],
                    event_hash=row["event_hash"],
                )
                for row in rows
            ],
        )

    @router.post(
        "/audit-events/{event_id}/corrections",
        response_model=dict[str, str],
        responses={**error_responses(401, 403, 404)},
    )
    async def correct_audit_event(
        tenant_id: UUID,
        event_id: UUID,
        payload: CorrectionRequest,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, str]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "audit.correct",
            target_type="audit_event",
            target_id=str(event_id),
        )
        correction_id = str(uuid7())
        occurred = datetime.now(UTC)
        occurred_epoch = occurred_epoch_micros(occurred)
        async with database.tenant_transaction(tenant_id) as connection:
            original = await connection.fetchrow(
                "SELECT * FROM platform.audit_event WHERE id=$1 AND tenant_id=$2",
                event_id,
                tenant_id,
            )
            if original is None:
                raise HTTPException(status_code=404, detail="AUDIT_EVENT_NOT_FOUND")
            fp = fingerprint(
                event_id=correction_id,
                occurred_at=occurred_epoch,
                actor=principal.subject,
                auth_method=principal.auth_method,
                action="audit.correction",
                decision="ALLOW",
                target_type="audit_event",
                target_id=str(event_id),
                details={"explanation": payload.explanation},
            )
            chain_index, event_hash_value, prev_hash = await append_to_chain(
                connection, tenant_id=str(tenant_id), event=fp
            )
            await connection.execute(
                """INSERT INTO platform.audit_event
                    (id,tenant_id,occurred_at,actor,auth_method,action,decision,
                     target_type,target_id,details,trace_id,chain_index,
                     event_hash,prev_event_hash)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS jsonb),$11,$12,$13,$14)""",
                correction_id,
                tenant_id,
                occurred,
                principal.subject,
                principal.auth_method,
                "audit.correction",
                "ALLOW",
                "audit_event",
                str(event_id),
                json.dumps({"explanation": payload.explanation}),
                None,
                chain_index,
                event_hash_value,
                prev_hash,
            )
        await _self_audit(
            principal,
            tenant_id,
            "audit.correct",
            target_type="audit_event",
            target_id=str(event_id),
        )
        return {"correction_id": correction_id, "corrected_event_id": str(event_id)}

    @router.post(
        "/audit-manifests",
        response_model=dict[str, Any],
        responses={**error_responses(401, 403)},
    )
    async def create_audit_manifest(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database, principal, tenant_id, "write", "audit.export", target_type="audit_manifest"
        )
        from trpc_service.audit_chain.manifest import AuditManifestService

        signing_key, worm = _worm_and_key()
        service = AuditManifestService(database, signing_key=signing_key, worm=worm)
        record = await service.build_and_archive(str(tenant_id))
        await _self_audit(
            principal,
            tenant_id,
            "audit.export",
            details={"manifest_index": record.manifest_index if record else None},
        )
        if record is None:
            return {"status": "NOTHING_TO_ARCHIVE"}
        return {
            "manifest_index": record.manifest_index,
            "first_chain_index": record.first_chain_index,
            "last_chain_index": record.last_chain_index,
            "event_count": record.event_count,
            "chain_head": record.chain_head_hash,
            "worm_location": record.worm_location,
        }

    @router.get(
        "/audit-manifests/verification",
        response_model=dict[str, Any],
        responses={**error_responses(401, 403)},
    )
    async def verify_audit_manifests(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database, principal, tenant_id, "read", "audit.verify", target_type="audit_manifest"
        )
        from trpc_service.audit_chain.manifest import AuditManifestService

        signing_key, worm = _worm_and_key()
        service = AuditManifestService(database, signing_key=signing_key, worm=worm)
        report = await service.verify_tenant_evidence(str(tenant_id))
        await _self_audit(
            principal,
            tenant_id,
            "audit.verify",
            details={"tamper_detected": report["tamper_detected"]},
        )
        return report

    return router
