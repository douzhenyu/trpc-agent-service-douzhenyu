"""Append-only audit writes shared by Admin API domain routes.

Tenant-scoped events join the per-tenant hash chain inside the caller's
transaction, so the audit row and the chain head advance atomically with the
business write.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from trpc_service.admin_api.auth import Principal
from trpc_service.admin_api.database import Connection, Database
from trpc_service.audit_chain.chain import append_to_chain, fingerprint, occurred_epoch_micros
from trpc_service.ids import uuid7


def _fingerprint(
    *,
    event_id: str,
    occurred_at: int,
    principal: Principal | None,
    action: str,
    decision: str,
    target_type: str | None,
    target_id: str | None,
    details: dict[str, Any],
) -> dict[str, Any]:
    return fingerprint(
        event_id=event_id,
        occurred_at=occurred_at,
        actor=principal.subject if principal else "anonymous",
        auth_method=principal.auth_method if principal else "anonymous",
        action=action,
        decision=decision,
        target_type=target_type,
        target_id=target_id,
        details=details,
    )


async def insert_audit(
    connection: Connection,
    principal: Principal | None,
    action: str,
    decision: str,
    *,
    target_type: str | None = None,
    target_id: str | None = None,
    tenant_id: UUID | None = None,
    details: dict[str, Any] | None = None,
    trace_id: str | None = None,
) -> None:
    """Append one audit event in the caller's transaction, hash-chained."""

    from datetime import UTC, datetime

    event_id = str(uuid7())
    occurred_at = datetime.now(UTC)
    payload = details or {}
    chain_index: int | None = None
    event_hash_value: str | None = None
    prev_hash: str | None = None
    if tenant_id is not None:
        fp = _fingerprint(
            event_id=event_id,
            occurred_at=occurred_epoch_micros(occurred_at),
            principal=principal,
            action=action,
            decision=decision,
            target_type=target_type,
            target_id=target_id,
            details=payload,
        )
        chain_index, event_hash_value, prev_hash = await append_to_chain(
            connection, tenant_id=str(tenant_id), event=fp
        )
    await connection.execute(
        """INSERT INTO platform.audit_event
            (id,tenant_id,occurred_at,actor,auth_method,action,decision,
             target_type,target_id,details,trace_id,chain_index,event_hash,prev_event_hash)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS jsonb),$11,$12,$13,$14)""",
        event_id,
        tenant_id,
        occurred_at,
        principal.subject if principal else "anonymous",
        principal.auth_method if principal else "anonymous",
        action,
        decision,
        target_type,
        target_id,
        json.dumps(payload),
        trace_id,
        chain_index,
        event_hash_value,
        prev_hash,
    )


async def write_audit(
    database: Database,
    principal: Principal | None,
    action: str,
    decision: str,
    **fields: Any,
) -> None:
    async with database.transaction() as connection:
        await insert_audit(connection, principal, action, decision, **fields)
