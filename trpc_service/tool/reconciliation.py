"""Reconciliation of OUTCOME_UNKNOWN tool calls.

A non-idempotent write whose result is uncertain is never blindly retried; it
lands in a reconcilable state. Operators close the uncertainty with a
one-time, append-only resolution (executed or not executed) that is kept
alongside the immutable call record.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from trpc_service.tool.executor import ToolInvocationRecord, ToolInvocationStatus


class ReconciliationDecision(StrEnum):
    CONFIRMED_EXECUTED = "CONFIRMED_EXECUTED"
    CONFIRMED_NOT_EXECUTED = "CONFIRMED_NOT_EXECUTED"


class ReconciliationError(RuntimeError):
    """Stable reconciliation failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class Reconciliation:
    call_id: str
    tenant_id: str
    decision: ReconciliationDecision
    resolved_by: str
    resolved_at: datetime
    note: str | None = None


class ReconciliationStore(Protocol):
    async def upsert(self, reconciliation: Reconciliation) -> None: ...
    async def get(self, tenant_id: str, call_id: str) -> Reconciliation | None: ...
    async def all_for(self, tenant_id: str) -> list[Reconciliation]: ...


class UnknownCallSource(Protocol):
    """Reads unknown-outcome calls; typically the tool-call store."""

    async def list_unknown(self, tenant_id: str) -> list[ToolInvocationRecord]: ...
    async def get_call(self, tenant_id: str, call_id: str) -> ToolInvocationRecord | None: ...


class MemoryReconciliationStore:
    def __init__(self) -> None:
        self._reconciliations: dict[str, Reconciliation] = {}

    async def upsert(self, reconciliation: Reconciliation) -> None:
        self._reconciliations[reconciliation.call_id] = reconciliation

    async def get(self, tenant_id: str, call_id: str) -> Reconciliation | None:
        reconciliation = self._reconciliations.get(call_id)
        if reconciliation is None or reconciliation.tenant_id != tenant_id:
            return None
        return reconciliation

    async def all_for(self, tenant_id: str) -> list[Reconciliation]:
        return [
            reconciliation
            for reconciliation in self._reconciliations.values()
            if reconciliation.tenant_id == tenant_id
        ]


class ReconciliationService:
    """Lists open OUTCOME_UNKNOWN calls and closes each exactly once."""

    def __init__(self, store: ReconciliationStore, calls: UnknownCallSource) -> None:
        self.store = store
        self._calls = calls

    @classmethod
    def in_memory(cls, calls: UnknownCallSource) -> ReconciliationService:
        return cls(MemoryReconciliationStore(), calls)

    async def open_calls(self, tenant_id: str) -> list[ToolInvocationRecord]:
        """Unknown-outcome calls that have no resolution yet."""

        resolved = {
            reconciliation.call_id for reconciliation in await self.store.all_for(tenant_id)
        }
        return [
            record
            for record in await self._calls.list_unknown(tenant_id)
            if record.call_id not in resolved
        ]

    async def resolve(
        self,
        tenant_id: str,
        call_id: str,
        decision: ReconciliationDecision,
        *,
        resolved_by: str,
        note: str | None = None,
    ) -> Reconciliation:
        call = await self._calls.get_call(tenant_id, call_id)
        if call is None:
            raise ReconciliationError("RECONCILIATION_CALL_NOT_FOUND")
        if call.status != ToolInvocationStatus.OUTCOME_UNKNOWN:
            raise ReconciliationError("RECONCILIATION_CALL_NOT_UNKNOWN")
        if await self.store.get(tenant_id, call_id) is not None:
            raise ReconciliationError("RECONCILIATION_ALREADY_RESOLVED")
        reconciliation = Reconciliation(
            call_id=call_id,
            tenant_id=tenant_id,
            decision=decision,
            resolved_by=resolved_by,
            resolved_at=datetime.now(UTC),
            note=note,
        )
        await self.store.upsert(reconciliation)
        return reconciliation
