"""Per-tenant audit hash chain: append-only evidence with tamper detection.

Every tenant-scoped audit event carries its chain index, its own canonical
event hash and the previous event's hash. The chain head lives in
`platform.audit_chain_state` and is advanced inside the same transaction that
inserts the event, so concurrent appends serialize and any later mutation of
an event row breaks the recomputable chain.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from typing import Any
from uuid import UUID

from trpc_service.admin_api.database import Connection

GENESIS_HASH = "0" * 64

FINGERPRINT_FIELDS = (
    "id",
    "occurred_at",
    "actor",
    "auth_method",
    "action",
    "decision",
    "target_type",
    "target_id",
    "details",
)


def occurred_epoch_micros(occurred_at: datetime) -> int:
    """Timezone-independent timestamp encoding used in the hashed fingerprint."""

    return int(occurred_at.timestamp() * 1_000_000)


def fingerprint(
    *,
    event_id: str,
    occurred_at: int,
    actor: str,
    auth_method: str,
    action: str,
    decision: str,
    target_type: str | None,
    target_id: str | None,
    details: dict[str, Any],
) -> dict[str, Any]:
    """The exact field set hashed into the chain; shared by append and verify."""

    return {
        "id": event_id,
        "occurred_at": occurred_at,
        "actor": actor,
        "auth_method": auth_method,
        "action": action,
        "decision": decision,
        "target_type": target_type,
        "target_id": target_id,
        "details": details,
    }


def canonical_event_json(event: dict[str, Any]) -> str:
    """Stable, key-ordered serialization used for hashing."""

    return json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def event_hash(event: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_event_json(event).encode("utf-8")).hexdigest()


def chain_hash(previous_hash: str, current_event_hash: str) -> str:
    return hashlib.sha256(f"{previous_hash}:{current_event_hash}".encode()).hexdigest()


def random_event_id() -> str:
    return str(UUID(bytes=os.urandom(16), version=4))


async def append_to_chain(
    connection: Connection,
    *,
    tenant_id: str,
    event: dict[str, Any],
) -> tuple[int, str, str]:
    """Reserve the next chain slot and compute the chained hash.

    Locks the tenant's chain state row so concurrent appends serialize; the
    caller inserts the audit event with the returned (index, hash, prev) in
    the same transaction.
    """

    state = await connection.fetchrow(
        "SELECT last_index, last_hash FROM platform.audit_chain_state "
        "WHERE tenant_id=$1 FOR UPDATE",
        UUID(tenant_id),
    )
    if state is None:
        await connection.execute(
            "INSERT INTO platform.audit_chain_state (tenant_id,last_index,last_hash) "
            "VALUES ($1,0,$2) ON CONFLICT (tenant_id) DO NOTHING",
            tenant_id,
            GENESIS_HASH,
        )
        state = await connection.fetchrow(
            "SELECT last_index, last_hash FROM platform.audit_chain_state "
            "WHERE tenant_id=$1 FOR UPDATE",
            UUID(tenant_id),
        )
    assert state is not None  # the upsert above guarantees the row
    previous_index = int(state["last_index"])
    previous_chain_head = str(state["last_hash"])
    current = event_hash(event)
    new_index = previous_index + 1
    new_chain_head = chain_hash(previous_chain_head, current)
    await connection.execute(
        "UPDATE platform.audit_chain_state SET last_index=$2, last_hash=$3, "
        "updated_at=now() WHERE tenant_id=$1",
        tenant_id,
        new_index,
        new_chain_head,
    )
    # The event row stores its own hash plus the chain head it extends.
    return new_index, current, previous_chain_head


async def verify_tenant_chain(connection: Connection, tenant_id: str) -> dict[str, Any]:
    """Recompute the whole tenant chain from stored rows; detect any mutation."""

    rows = await connection.fetch(
        """SELECT id, occurred_at, actor, auth_method, action, decision,
                  target_type, target_id, details, chain_index, event_hash,
                  prev_event_hash
        FROM platform.audit_event
        WHERE tenant_id=$1 AND chain_index IS NOT NULL
        ORDER BY chain_index""",
        UUID(tenant_id),
    )
    previous = GENESIS_HASH
    count = 0
    for row in rows:
        count += 1
        occurred = row["occurred_at"]
        stored_fingerprint = {
            "id": str(row["id"]),
            "occurred_at": occurred_epoch_micros(occurred)
            if hasattr(occurred, "timestamp")
            else occurred,
            "actor": row["actor"],
            "auth_method": row["auth_method"],
            "action": row["action"],
            "decision": row["decision"],
            "target_type": row["target_type"],
            "target_id": row["target_id"],
            "details": row["details"] or {},
        }
        recomputed = event_hash(stored_fingerprint)
        if recomputed != str(row["event_hash"]) or previous != str(row["prev_event_hash"]):
            return {
                "valid": False,
                "broken_at_index": int(row["chain_index"]),
                "expected_prev": previous,
                "stored_prev": row["prev_event_hash"],
            }
        previous = chain_hash(previous, recomputed)
    return {"valid": True, "events": count, "chain_head": previous}
