"""Audit Outbox: business transactions enqueue audit intents atomically.

The dispatcher materializes pending outbox rows into the hash-chained online
audit trail inside one transaction per tenant, so evidence appears exactly
once and in chain order even under concurrent producers.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

from trpc_service.admin_api.database import Connection, Database
from trpc_service.audit_chain.chain import append_to_chain, fingerprint, occurred_epoch_micros
from trpc_service.ids import uuid7


async def enqueue_audit(
    connection: Connection,
    *,
    tenant_id: str,
    actor: str,
    auth_method: str,
    action: str,
    decision: str,
    target_type: str | None = None,
    target_id: str | None = None,
    details: dict[str, Any] | None = None,
    trace_id: str | None = None,
) -> str:
    """Enqueue one audit intent inside the caller's business transaction."""

    outbox_id = str(uuid4())
    await connection.execute(
        """INSERT INTO platform.audit_outbox
            (id,tenant_id,actor,auth_method,action,decision,target_type,target_id,
             details,trace_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS jsonb),$10)""",
        outbox_id,
        UUID(tenant_id),
        actor,
        auth_method,
        action,
        decision,
        target_type,
        target_id,
        json.dumps(details or {}),
        trace_id,
    )
    return outbox_id


class AuditOutboxDispatcher:
    """Drains pending audit intents into the hash-chained audit trail."""

    def __init__(self, database: Database, *, batch_size: int = 200) -> None:
        self._database = database
        self._batch_size = batch_size

    async def dispatch_pending(self, tenant_id: str) -> int:
        """Materialize every pending intent for one tenant; returns the count."""

        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                """SELECT * FROM platform.audit_outbox
                WHERE tenant_id=$1 AND status='PENDING'
                ORDER BY id LIMIT $2 FOR UPDATE""",
                UUID(tenant_id),
                self._batch_size,
            )
            dispatched = 0
            for row in rows:
                details = row["details"] or {}
                event_id = str(uuid7())
                occurred = row["occurred_at"]
                fp = fingerprint(
                    event_id=event_id,
                    occurred_at=occurred_epoch_micros(occurred)
                    if hasattr(occurred, "timestamp")
                    else occurred,
                    actor=str(row["actor"]),
                    auth_method=str(row["auth_method"]),
                    action=str(row["action"]),
                    decision=str(row["decision"]),
                    target_type=row["target_type"],
                    target_id=row["target_id"],
                    details=details,
                )
                chain_index, event_hash_value, prev_hash = await append_to_chain(
                    connection, tenant_id=tenant_id, event=fp
                )
                await connection.execute(
                    """INSERT INTO platform.audit_event
                        (id,tenant_id,occurred_at,actor,auth_method,action,decision,
                         target_type,target_id,details,trace_id,chain_index,
                         event_hash,prev_event_hash)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS jsonb),$11,$12,$13,$14)""",
                    event_id,
                    UUID(tenant_id),
                    row["occurred_at"],
                    row["actor"],
                    row["auth_method"],
                    row["action"],
                    row["decision"],
                    row["target_type"],
                    row["target_id"],
                    json.dumps(details),
                    row["trace_id"],
                    chain_index,
                    event_hash_value,
                    prev_hash,
                )
                await connection.execute(
                    "UPDATE platform.audit_outbox SET status='DISPATCHED', "
                    "dispatched_at=now() WHERE id=$1",
                    row["id"],
                )
                dispatched += 1
            return dispatched
