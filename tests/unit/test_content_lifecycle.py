from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from trpc_service.content_lifecycle import (
    BACKUP_ERASURE_WINDOW,
    PRIMARY_ERASURE_WINDOW,
    ContentBackend,
    ContentLifecycleError,
    DeletionExecutor,
    RetentionPolicy,
)


class _Backend:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.erased: list[str] = []

    async def erase(self, tenant_id: str) -> int:
        if self.fail:
            raise RuntimeError("temporary backend outage")
        self.erased.append(tenant_id)
        return 1

    async def verify_erased(self, tenant_id: str) -> bool:
        return tenant_id in self.erased


def test_retention_defaults_and_tenant_bounds_are_compliance_safe() -> None:
    policy = RetentionPolicy()

    assert policy.inbound_payload_days == 7
    assert policy.session_days == 90
    assert policy.memory_days == 365
    assert policy.artifact_days == 30
    assert policy.idempotency_tombstone_days == 365
    assert policy.audit_days == 365
    assert policy.backup_days == 35
    with pytest.raises(ValueError, match="RETENTION_AUDIT_DAYS_INVALID"):
        RetentionPolicy(audit_days=89)
    with pytest.raises(ValueError, match="RETENTION_BACKUP_DAYS_INVALID"):
        RetentionPolicy(backup_days=36)


def test_deletion_execution_covers_every_backend_and_records_backup_deadline() -> None:
    async def exercise() -> None:
        now = datetime(2026, 9, 7, tzinfo=UTC)
        backends = {kind: _Backend() for kind in ContentBackend}
        outcome = await DeletionExecutor(backends).execute("tenant-a", now=now)

        assert outcome.primary_due_at == now + PRIMARY_ERASURE_WINDOW
        assert outcome.backup_due_at == now + BACKUP_ERASURE_WINDOW
        assert outcome.retryable is False
        assert {proof.backend for proof in outcome.proofs} == set(ContentBackend)
        assert all(proof.verified for proof in outcome.proofs)

    asyncio.run(exercise())


def test_legal_hold_blocks_erasure_and_backend_failure_is_retryable() -> None:
    async def exercise() -> None:
        backends = {kind: _Backend() for kind in ContentBackend}
        executor = DeletionExecutor(backends)
        with pytest.raises(ContentLifecycleError, match="DELETION_LEGAL_HOLD_ACTIVE"):
            await executor.execute("tenant-a", legal_hold=True)

        backends[ContentBackend.VECTOR] = _Backend(fail=True)
        outcome = await executor.execute("tenant-a")
        assert outcome.retryable is True
        assert outcome.error_code == "DELETION_BACKEND_RETRYABLE"
        assert outcome.next_attempt_at is not None
        assert outcome.next_attempt_at <= datetime.now(UTC) + timedelta(minutes=6)

    asyncio.run(exercise())
