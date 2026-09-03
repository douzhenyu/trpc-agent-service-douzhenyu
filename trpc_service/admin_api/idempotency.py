"""Transactional Idempotency-Key ledger for Admin API commands."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from trpc_service.admin_api.database import Connection


class IdempotencyConflictError(Exception):
    """The same actor reused a key for a different command payload."""


def request_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def replay_for(
    connection: Connection,
    *,
    actor: str,
    key: str,
    operation: str,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    """Serialize a key and return its prior response when the request matches."""
    await connection.fetchval(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", f"{actor}:{key}"
    )
    row = await connection.fetchrow(
        """SELECT operation,request_hash,response
        FROM platform.idempotency_record WHERE actor=$1 AND key=$2""",
        actor,
        key,
    )
    if row is None:
        return None
    if row["operation"] != operation or row["request_hash"] != request_hash(payload):
        raise IdempotencyConflictError
    response = row["response"]
    return json.loads(response) if isinstance(response, str) else dict(response)


async def remember(
    connection: Connection,
    *,
    actor: str,
    key: str,
    operation: str,
    payload: dict[str, Any],
    response: dict[str, Any],
) -> None:
    await connection.execute(
        """INSERT INTO platform.idempotency_record
        (actor,key,operation,request_hash,response)
        VALUES ($1,$2,$3,$4,CAST($5 AS jsonb))""",
        actor,
        key,
        operation,
        request_hash(payload),
        json.dumps(response, default=str),
    )
