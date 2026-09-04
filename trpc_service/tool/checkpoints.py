"""Waiting checkpoints for tool approvals: park the execution, resume it later.

Parking writes one durable checkpoint and immediately releases the worker's
session lease, so a waiting execution occupies no Agent Worker. Resuming
consumes the approval and re-acquires the session lease with a fresh fencing
token, re-validating the immutable tool-call binding before the run continues.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
from uuid import uuid4

from trpc_service.sessions import LeaseGrant
from trpc_service.tool.approvals import ApprovalService


class CheckpointStatus(StrEnum):
    WAITING_APPROVAL = "WAITING_APPROVAL"
    RESUMED = "RESUMED"
    ABANDONED = "ABANDONED"


class CheckpointError(RuntimeError):
    """Stable checkpoint failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ExecutionCheckpoint:
    """One parked execution waiting for its approval decision."""

    checkpoint_id: str
    tenant_id: str
    execution_id: str
    session_id: str
    release_id: str
    approval_id: str
    tool_name: str
    tool_version: int
    params_hash: str
    requested_by: str
    parked_by: str
    status: CheckpointStatus = CheckpointStatus.WAITING_APPROVAL
    created_at: datetime | None = None
    resumed_by: str | None = None
    resumed_at: datetime | None = None


class CheckpointStore(Protocol):
    async def upsert(self, checkpoint: ExecutionCheckpoint) -> None: ...
    async def get(self, tenant_id: str, checkpoint_id: str) -> ExecutionCheckpoint | None: ...


class MemoryCheckpointStore:
    def __init__(self) -> None:
        self._checkpoints: dict[str, ExecutionCheckpoint] = {}

    async def upsert(self, checkpoint: ExecutionCheckpoint) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    async def get(self, tenant_id: str, checkpoint_id: str) -> ExecutionCheckpoint | None:
        checkpoint = self._checkpoints.get(checkpoint_id)
        if checkpoint is None or checkpoint.tenant_id != tenant_id:
            return None
        return checkpoint


class WorkerLeaseManager(Protocol):
    """The subset of SessionLeaseManager the checkpoint lifecycle needs."""

    async def acquire(self, tenant_id: str, session_id: str, owner_id: str) -> LeaseGrant: ...
    async def release(self, tenant_id: str, session_id: str, owner_id: str) -> None: ...


class CheckpointService:
    """Parks executions awaiting approval and resumes them on a fresh lease."""

    def __init__(self, store: CheckpointStore) -> None:
        self.store = store

    @classmethod
    def in_memory(cls) -> CheckpointService:
        return cls(MemoryCheckpointStore())

    async def park(
        self,
        *,
        tenant_id: str,
        execution_id: str,
        session_id: str,
        release_id: str,
        approval_id: str,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        requested_by: str,
        parked_by: str,
        lease_manager: WorkerLeaseManager,
    ) -> ExecutionCheckpoint:
        """Persist the wait state and give up the worker's session lease."""

        await lease_manager.release(tenant_id, session_id, parked_by)
        checkpoint = ExecutionCheckpoint(
            checkpoint_id=str(uuid4()),
            tenant_id=tenant_id,
            execution_id=execution_id,
            session_id=session_id,
            release_id=release_id,
            approval_id=approval_id,
            tool_name=tool_name,
            tool_version=tool_version,
            params_hash=params_hash,
            requested_by=requested_by,
            parked_by=parked_by,
            created_at=datetime.now(UTC),
        )
        await self.store.upsert(checkpoint)
        return checkpoint

    async def resume(
        self,
        tenant_id: str,
        checkpoint_id: str,
        *,
        approvals: ApprovalService,
        lease_manager: WorkerLeaseManager,
        resumed_by: str,
    ) -> LeaseGrant:
        """Consume the approval, re-acquire the session lease, mark resumed."""

        checkpoint = await self.store.get(tenant_id, checkpoint_id)
        if checkpoint is None:
            raise CheckpointError("CHECKPOINT_NOT_FOUND")
        if checkpoint.status != CheckpointStatus.WAITING_APPROVAL:
            raise CheckpointError("CHECKPOINT_NOT_WAITING")
        try:
            await approvals.consume(
                tenant_id,
                checkpoint.approval_id,
                tool_name=checkpoint.tool_name,
                tool_version=checkpoint.tool_version,
                params_hash=checkpoint.params_hash,
                requested_by=checkpoint.requested_by,
            )
        except Exception as error:
            raise CheckpointError("CHECKPOINT_APPROVAL_NOT_GRANTED") from error
        grant = await lease_manager.acquire(tenant_id, checkpoint.session_id, resumed_by)
        resumed = replace(
            checkpoint,
            status=CheckpointStatus.RESUMED,
            resumed_by=resumed_by,
            resumed_at=datetime.now(UTC),
        )
        await self.store.upsert(resumed)
        return grant

    async def get(self, tenant_id: str, checkpoint_id: str) -> ExecutionCheckpoint:
        checkpoint = await self.store.get(tenant_id, checkpoint_id)
        if checkpoint is None:
            raise CheckpointError("CHECKPOINT_NOT_FOUND")
        return checkpoint
