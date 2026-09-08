"""Global Failover Lease: one region is primary under a monotonic fencing
token; warm-standby regions stay fenced off from ingress and consumption
until they win the lease. Promotion fences the old primary and creates the
new primary atomically, so two primaries can never coexist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from trpc_service.admin_api.database import Database

RPO_LIMIT_SECONDS = 5 * 60
RTO_LIMIT_SECONDS = 60 * 60
_LEASE_LOCK = 0x4641494C  # hashtext('failover-lease')


class FailoverRole(StrEnum):
    PRIMARY = "PRIMARY"
    STANDBY = "STANDBY"
    FAILING_OVER = "FAILING_OVER"
    FAILING_BACK = "FAILING_BACK"
    FENCED = "FENCED"


class FailoverError(RuntimeError):
    """Stable failover failure; callers must not assume any role change."""


@dataclass(frozen=True)
class LeaseState:
    region: str
    role: FailoverRole
    fencing_token: int
    acquired_at: datetime
    expires_at: datetime


def _lease_state(row: Any) -> LeaseState:
    return LeaseState(
        region=str(row["region"]),
        role=FailoverRole(str(row["role"])),
        fencing_token=int(row["fencing_token"]),
        acquired_at=row["acquired_at"],
        expires_at=row["expires_at"],
    )


class FailoverLeaseManager:
    """Owns the platform.failover_lease contract."""

    def __init__(self, database: Database, *, lease_ttl_seconds: int = 60) -> None:
        self._database = database
        self._lease_ttl = timedelta(seconds=lease_ttl_seconds)

    async def register_standby(self, region: str, operator: str) -> LeaseState:
        """Register a warm-standby region. Ingress and consumption stay off."""
        async with self._database.transaction() as connection:
            row = await connection.fetchrow(
                """INSERT INTO platform.failover_lease
                (id,region,role,fencing_token,expires_at,operator)
                VALUES (gen_random_uuid(),$1,'STANDBY',0,$2,$3)
                RETURNING region,role,fencing_token,acquired_at,expires_at""",
                region,
                datetime.now(UTC) + self._lease_ttl,
                operator,
            )
            assert row is not None
            return _lease_state(row)

    async def promote(self, region: str, operator: str) -> LeaseState:
        """Atomically fence the current primary and promote ``region``.

        The advisory lock serializes promotions; the partial unique index
        makes a second live PRIMARY row impossible even under races.
        """
        async with self._database.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock($1)", _LEASE_LOCK)
            primary = await connection.fetchrow(
                """SELECT region,role,fencing_token,acquired_at,expires_at
                FROM platform.failover_lease
                WHERE role='PRIMARY' AND released_at IS NULL FOR UPDATE"""
            )
            if primary is not None and str(primary["region"]) == region:
                raise FailoverError("FAILOVER_ALREADY_PRIMARY")
            await connection.execute(
                """UPDATE platform.failover_lease
                SET role='FENCED',released_at=now()
                WHERE role='PRIMARY' AND released_at IS NULL""",
            )
            token_row = await connection.fetchval(
                "SELECT coalesce(max(fencing_token),0)+1 FROM platform.failover_lease"
            )
            row = await connection.fetchrow(
                """INSERT INTO platform.failover_lease
                (id,region,role,fencing_token,expires_at,operator)
                VALUES (gen_random_uuid(),$1,'PRIMARY',$2,$3,$4)
                RETURNING region,role,fencing_token,acquired_at,expires_at""",
                region,
                int(token_row),
                datetime.now(UTC) + self._lease_ttl,
                operator,
            )
            assert row is not None
            return _lease_state(row)

    async def current_primary(self) -> LeaseState | None:
        async with self._database.transaction() as connection:
            row = await connection.fetchrow(
                """SELECT region,role,fencing_token,acquired_at,expires_at
                FROM platform.failover_lease
                WHERE role='PRIMARY' AND released_at IS NULL"""
            )
            return _lease_state(row) if row is not None else None
