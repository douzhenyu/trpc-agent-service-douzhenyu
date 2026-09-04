"""Session lease, fencing tokens and the authoritative Session Event commit.

The authoritative commit runs in a single PostgreSQL transaction: it validates
the fencing token and the expected Session version, appends the immutable
Session Events, advances the Session State version and writes an Outbox record
for downstream projections. A Worker that lost its lease or hit a version
conflict cannot commit.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from trpc_service.admin_api.database import Connection, Database
from trpc_service.execution_bus import (
    SESSION_EVENTS_COMMITTED_EVENT,
    WORKER_SOURCE,
    session_partition_key,
)
from trpc_service.ids import uuid7

DEFAULT_LEASE_TTL_SECONDS = 30.0
MAX_SESSION_ID_LENGTH = 512
MAX_OWNER_ID_LENGTH = 256


class SessionLeaseError(RuntimeError):
    """The caller does not hold the valid current lease for the Session."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class SessionVersionConflictError(RuntimeError):
    """The Session State moved past the expected version; the result is stale."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class LeaseGrant:
    fencing_token: int
    session_version: int


@dataclass(frozen=True)
class SessionEvent:
    kind: str
    payload: dict[str, Any]


async def create_session_if_missing(
    connection: Connection, tenant_id: UUID, application_id: UUID, session_id: str
) -> None:
    if not 1 <= len(session_id) <= MAX_SESSION_ID_LENGTH:
        raise ValueError("session_id length is out of range")
    await connection.execute(
        """INSERT INTO tenant.agent_session (tenant_id,application_id,id)
        VALUES ($1,$2,$3) ON CONFLICT (tenant_id,id) DO NOTHING""",
        tenant_id,
        application_id,
        session_id,
    )


class SessionLeaseManager:
    """Issue short leases with monotonic fencing tokens per Session.

    A lease is acquired per execution: the fencing token always increments on
    reacquisition, and a lease actively held by another owner is never stolen —
    rebalancing waits for expiry, and stale holders are fenced at commit time.
    """

    def __init__(self, database: Database, lease_ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS):
        self._database = database
        self._lease_ttl_seconds = lease_ttl_seconds

    async def acquire(self, tenant_id: UUID, session_id: str, owner_id: str) -> LeaseGrant:
        if not 1 <= len(owner_id) <= MAX_OWNER_ID_LENGTH:
            raise ValueError("owner_id length is out of range")
        async with self._database.tenant_transaction(tenant_id) as connection:
            token = await connection.fetchval(
                """INSERT INTO tenant.session_lease
                (tenant_id,session_id,owner_id,fencing_token,expires_at)
                VALUES ($1,$2,$3,1,now() + make_interval(secs => $4))
                ON CONFLICT (tenant_id,session_id) DO UPDATE SET
                  owner_id=$3,
                  fencing_token=tenant.session_lease.fencing_token + 1,
                  expires_at=now() + make_interval(secs => $4),
                  renewed_at=now()
                WHERE tenant.session_lease.expires_at <= now()
                  OR tenant.session_lease.owner_id=$3
                RETURNING fencing_token""",
                tenant_id,
                session_id,
                owner_id,
                self._lease_ttl_seconds,
            )
            if token is None:
                holder = await connection.fetchval(
                    """SELECT owner_id FROM tenant.session_lease
                    WHERE tenant_id=$1 AND session_id=$2""",
                    tenant_id,
                    session_id,
                )
                raise SessionLeaseError(f"SESSION_LEASE_HELD:{holder}")
            version = await connection.fetchval(
                """SELECT version FROM tenant.agent_session
                WHERE tenant_id=$1 AND id=$2 FOR UPDATE""",
                tenant_id,
                session_id,
            )
            if version is None:
                raise SessionLeaseError("SESSION_NOT_FOUND")
            return LeaseGrant(fencing_token=int(token), session_version=int(version))


async def commit_session_events(
    connection: Connection,
    *,
    tenant_id: UUID,
    session_id: str,
    owner_id: str,
    fencing_token: int,
    expected_version: int,
    execution_id: UUID,
    events: list[SessionEvent],
) -> int:
    """Append authoritative Session Events or fail closed without any write.

    Runs inside the caller's transaction: validates lease and expected version,
    appends events at the next sequence numbers, advances the Session version
    and enqueues the Outbox record that drives downstream projections.
    """

    if not events:
        raise ValueError("a session commit requires at least one event")
    lease = await connection.fetchrow(
        """SELECT owner_id,fencing_token,expires_at FROM tenant.session_lease
        WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE""",
        tenant_id,
        session_id,
    )
    if (
        lease is None
        or str(lease["owner_id"]) != owner_id
        or int(lease["fencing_token"]) != fencing_token
        or lease["expires_at"] <= datetime.now(UTC)
    ):
        raise SessionLeaseError("SESSION_LEASE_INVALID")
    version = await connection.fetchval(
        """SELECT version FROM tenant.agent_session
        WHERE tenant_id=$1 AND id=$2 FOR UPDATE""",
        tenant_id,
        session_id,
    )
    if version is None:
        raise SessionLeaseError("SESSION_NOT_FOUND")
    if int(version) != expected_version:
        raise SessionVersionConflictError("SESSION_VERSION_CONFLICT")
    await connection.executemany(
        """INSERT INTO tenant.session_event
        (tenant_id,session_id,sequence,execution_id,kind,payload)
        VALUES ($1,$2,$3,$4,$5,CAST($6 AS jsonb))""",
        [
            (
                tenant_id,
                session_id,
                expected_version + offset,
                execution_id,
                event.kind,
                json.dumps(event.payload),
            )
            for offset, event in enumerate(events, start=1)
        ],
    )
    new_version = expected_version + len(events)
    await connection.execute(
        """UPDATE tenant.agent_session SET version=$3,updated_at=now()
        WHERE tenant_id=$1 AND id=$2""",
        tenant_id,
        session_id,
        new_version,
    )
    await connection.execute(
        """INSERT INTO platform.outbox_record
        (tenant_id,id,message_id,source,event_type,partition_key,payload)
        VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS jsonb))""",
        tenant_id,
        uuid7(),
        str(uuid7()),
        WORKER_SOURCE,
        SESSION_EVENTS_COMMITTED_EVENT,
        session_partition_key(str(tenant_id), session_id),
        json.dumps(
            {
                "tenant_id": str(tenant_id),
                "session_id": session_id,
                "execution_id": str(execution_id),
                "from_version": expected_version,
                "to_version": new_version,
                "event_kinds": [event.kind for event in events],
            }
        ),
    )
    return new_version
