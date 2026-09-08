"""Retention defaults and verifiable multi-backend content erasure."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator

from trpc_service.admin_api.database import Database

PRIMARY_ERASURE_WINDOW = timedelta(hours=24)
BACKUP_ERASURE_WINDOW = timedelta(days=35)


class ContentBackend(StrEnum):
    SQL = "SQL"
    REDIS = "REDIS"
    VECTOR = "VECTOR"
    OBJECT = "OBJECT"
    DERIVED = "DERIVED"
    BACKUP = "BACKUP"


class ContentLifecycleError(RuntimeError):
    """Stable lifecycle failure that can be stored without content."""


class RetentionPolicy(BaseModel):
    """Tenant-overridable policy constrained by the platform compliance floor."""

    model_config = ConfigDict(frozen=True)

    inbound_payload_days: int = 7
    session_days: int = 90
    memory_days: int = 365
    artifact_days: int = 30
    idempotency_tombstone_days: int = 365
    audit_days: int = 365
    backup_days: int = 35

    @model_validator(mode="after")
    def validates_compliance_bounds(self) -> RetentionPolicy:
        if not 1 <= self.inbound_payload_days <= 30:
            raise ValueError("RETENTION_INBOUND_PAYLOAD_DAYS_INVALID")
        if not 30 <= self.session_days <= 365:
            raise ValueError("RETENTION_SESSION_DAYS_INVALID")
        if not 30 <= self.memory_days <= 730:
            raise ValueError("RETENTION_MEMORY_DAYS_INVALID")
        if not 1 <= self.artifact_days <= 365:
            raise ValueError("RETENTION_ARTIFACT_DAYS_INVALID")
        if not 30 <= self.idempotency_tombstone_days <= 730:
            raise ValueError("RETENTION_IDEMPOTENCY_DAYS_INVALID")
        if not 90 <= self.audit_days <= 365 * 7:
            raise ValueError("RETENTION_AUDIT_DAYS_INVALID")
        if not 1 <= self.backup_days <= BACKUP_ERASURE_WINDOW.days:
            raise ValueError("RETENTION_BACKUP_DAYS_INVALID")
        return self


class ErasableContentBackend(Protocol):
    """One tenant namespace in a primary or derived content backend."""

    async def erase(self, tenant_id: str) -> int: ...

    async def verify_erased(self, tenant_id: str) -> bool: ...


@dataclass(frozen=True)
class DeletionProof:
    backend: ContentBackend
    deleted_count: int
    verified: bool
    completed_at: datetime


@dataclass(frozen=True)
class DeletionOutcome:
    primary_due_at: datetime
    backup_due_at: datetime
    proofs: tuple[DeletionProof, ...]
    retryable: bool
    error_code: str | None = None
    next_attempt_at: datetime | None = None


class DeletionExecutor:
    """Erase every content plane and emit proof data suitable for durable audit."""

    def __init__(self, backends: dict[ContentBackend, ErasableContentBackend]) -> None:
        missing = set(ContentBackend).difference(backends)
        if missing:
            raise ContentLifecycleError("DELETION_BACKENDS_INCOMPLETE")
        self._backends = backends

    async def execute(
        self, tenant_id: str, *, legal_hold: bool = False, now: datetime | None = None
    ) -> DeletionOutcome:
        if legal_hold:
            raise ContentLifecycleError("DELETION_LEGAL_HOLD_ACTIVE")
        started_at = now or datetime.now(UTC)
        proofs: list[DeletionProof] = []
        try:
            for backend, adapter in self._backends.items():
                deleted_count = await adapter.erase(tenant_id)
                verified = await adapter.verify_erased(tenant_id)
                proofs.append(
                    DeletionProof(
                        backend=backend,
                        deleted_count=deleted_count,
                        verified=verified,
                        completed_at=started_at,
                    )
                )
                if not verified:
                    raise ContentLifecycleError("DELETION_BACKEND_UNVERIFIED")
        except ContentLifecycleError:
            raise
        except Exception:
            return DeletionOutcome(
                primary_due_at=started_at + PRIMARY_ERASURE_WINDOW,
                backup_due_at=started_at + BACKUP_ERASURE_WINDOW,
                proofs=tuple(proofs),
                retryable=True,
                error_code="DELETION_BACKEND_RETRYABLE",
                next_attempt_at=started_at + timedelta(minutes=5),
            )
        return DeletionOutcome(
            primary_due_at=started_at + PRIMARY_ERASURE_WINDOW,
            backup_due_at=started_at + BACKUP_ERASURE_WINDOW,
            proofs=tuple(proofs),
            retryable=False,
        )


class ContentDeletionWorker:
    """Durably run due erasure requests and store content-free proof receipts.

    Backends are injected by the runtime because their credentials and network
    clients never belong in the control plane.  The worker owns state changes,
    retry scheduling and reconciliation; adapters only erase a tenant namespace.
    """

    _RETRY_DELAY = timedelta(minutes=5)
    _PROCESSING_LEASE = timedelta(minutes=15)

    def __init__(self, database: Database, executor: DeletionExecutor) -> None:
        self._database = database
        self._executor = executor

    async def run_once(self, *, now: datetime | None = None) -> int:
        current = now or datetime.now(UTC)
        async with self._database.transaction() as connection:
            tenants = await connection.fetch("SELECT id FROM platform.tenant")
        completed = 0
        for tenant in tenants:
            tenant_id = UUID(str(tenant["id"]))
            async with self._database.tenant_transaction(tenant_id) as connection:
                requests = await connection.fetch(
                    """SELECT id FROM tenant.content_deletion_request
                    WHERE (status IN ('PENDING','RETRYABLE') AND next_attempt_at <= $2)
                       OR (status='PROCESSING' AND processing_at <= $3)
                       OR status='BLOCKED_LEGAL_HOLD'
                    ORDER BY created_at FOR UPDATE SKIP LOCKED""",
                    tenant_id,
                    current,
                    current - self._PROCESSING_LEASE,
                )
            for request in requests:
                if await self.process(tenant_id, UUID(str(request["id"])), now=current):
                    completed += 1
        return completed

    async def process(
        self, tenant_id: UUID, request_id: UUID, *, now: datetime | None = None
    ) -> bool:
        """Process one request once; ``True`` means every required backend verified."""

        current = now or datetime.now(UTC)
        async with self._database.tenant_transaction(tenant_id) as connection:
            active_hold = bool(
                await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM tenant.legal_hold "
                    "WHERE tenant_id=$1 AND status='ACTIVE')",
                    tenant_id,
                )
            )
            if active_hold:
                await connection.execute(
                    """UPDATE tenant.content_deletion_request
                    SET status='BLOCKED_LEGAL_HOLD',next_attempt_at=NULL,
                      last_error='DELETION_LEGAL_HOLD_ACTIVE'
                    WHERE tenant_id=$1 AND id=$2""",
                    tenant_id,
                    request_id,
                )
                return False
            claimed = await connection.fetchrow(
                """UPDATE tenant.content_deletion_request
                SET status='PROCESSING',processing_at=$3
                WHERE tenant_id=$1 AND id=$2
                  AND (status IN ('PENDING','RETRYABLE','BLOCKED_LEGAL_HOLD')
                    OR (status='PROCESSING' AND processing_at <= $4))
                RETURNING id""",
                tenant_id,
                request_id,
                current,
                current - self._PROCESSING_LEASE,
            )
        if claimed is None:
            return False

        try:
            outcome = await self._executor.execute(str(tenant_id), now=current)
        except ContentLifecycleError as error:
            outcome = DeletionOutcome(
                primary_due_at=current + PRIMARY_ERASURE_WINDOW,
                backup_due_at=current + BACKUP_ERASURE_WINDOW,
                proofs=(),
                retryable=True,
                error_code=str(error),
                next_attempt_at=current + self._RETRY_DELAY,
            )

        async with self._database.tenant_transaction(tenant_id) as connection:
            for proof in outcome.proofs:
                digest = sha256(
                    f"{request_id}:{proof.backend}:{proof.deleted_count}:{proof.verified}:"
                    f"{proof.completed_at.isoformat()}".encode()
                ).hexdigest()
                await connection.execute(
                    """INSERT INTO tenant.content_deletion_proof
                    (tenant_id,request_id,backend,deleted_count,verified,evidence_digest,completed_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (tenant_id,request_id,backend) DO UPDATE SET
                      deleted_count=EXCLUDED.deleted_count,verified=EXCLUDED.verified,
                      evidence_digest=EXCLUDED.evidence_digest,completed_at=EXCLUDED.completed_at""",
                    tenant_id,
                    request_id,
                    str(proof.backend),
                    proof.deleted_count,
                    proof.verified,
                    digest,
                    proof.completed_at,
                )
            if outcome.retryable:
                await connection.execute(
                    """UPDATE tenant.content_deletion_request
                    SET status='RETRYABLE',attempts=attempts+1,next_attempt_at=$3,
                      last_error=$4,processing_at=NULL
                    WHERE tenant_id=$1 AND id=$2""",
                    tenant_id,
                    request_id,
                    outcome.next_attempt_at or current + self._RETRY_DELAY,
                    outcome.error_code or "DELETION_BACKEND_RETRYABLE",
                )
                return False
            await connection.execute(
                """UPDATE tenant.content_deletion_request
                SET status='COMPLETED',attempts=attempts+1,next_attempt_at=NULL,last_error=NULL,
                  processing_at=NULL,completed_at=$3
                WHERE tenant_id=$1 AND id=$2""",
                tenant_id,
                request_id,
                current,
            )
        return True


class RetentionSweep:
    """Remove expired primary content using approved per-tenant limits.

    A Legal Hold wins over every ordinary expiry rule.  Metadata-only
    tombstones and audit evidence are intentionally outside this sweep.
    """

    def __init__(self, database: Database) -> None:
        self._database = database

    async def run_once(self, *, now: datetime | None = None) -> int:
        current = now or datetime.now(UTC)
        async with self._database.transaction() as connection:
            tenants = await connection.fetch("SELECT id FROM platform.tenant")
        deleted = 0
        for tenant in tenants:
            tenant_id = UUID(str(tenant["id"]))
            async with self._database.tenant_transaction(tenant_id) as connection:
                held = bool(
                    await connection.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM tenant.legal_hold "
                        "WHERE tenant_id=$1 AND status='ACTIVE')",
                        tenant_id,
                    )
                )
                if held:
                    continue
                row = await connection.fetchrow(
                    "SELECT * FROM tenant.content_retention_policy WHERE tenant_id=$1", tenant_id
                )
                policy = (
                    RetentionPolicy.model_validate(dict(row))
                    if row is not None
                    else RetentionPolicy()
                )
                deleted += await connection.execute(
                    "DELETE FROM tenant.inbound_message WHERE tenant_id=$1 AND occurred_at < $2",
                    tenant_id,
                    current - timedelta(days=policy.idempotency_tombstone_days),
                )
                deleted += await connection.execute(
                    """UPDATE tenant.reply_delivery SET content=''
                    WHERE tenant_id=$1 AND created_at < $2 AND content <> ''""",
                    tenant_id,
                    current - timedelta(days=policy.inbound_payload_days),
                )
                deleted += await connection.execute(
                    """UPDATE tenant.session_event event SET payload='{}'::jsonb
                    FROM tenant.agent_session session
                    WHERE event.tenant_id=$1 AND event.tenant_id=session.tenant_id
                      AND event.session_id=session.id AND session.updated_at < $2
                      AND event.payload <> '{}'::jsonb""",
                    tenant_id,
                    current - timedelta(days=policy.session_days),
                )
                deleted += await connection.execute(
                    """UPDATE tenant.session_summary summary SET content='[erased]'
                    FROM tenant.agent_session session
                    WHERE summary.tenant_id=$1 AND summary.tenant_id=session.tenant_id
                      AND summary.session_id=session.id AND session.updated_at < $2
                      AND summary.content <> '[erased]'""",
                    tenant_id,
                    current - timedelta(days=policy.session_days),
                )
                deleted += await connection.execute(
                    """UPDATE tenant.memory_record
                    SET content='[erased]',is_valid=false,invalidated_at=$3,
                      invalidation_reason='RETENTION_EXPIRED'
                    WHERE tenant_id=$1 AND is_valid AND last_used_at < $2""",
                    tenant_id,
                    current - timedelta(days=policy.memory_days),
                    current,
                )
        return deleted
