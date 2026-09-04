"""Governed tool invocation: side-effect retry contracts, blocks and audit records.

Every invocation resolves the tenant-scoped versioned definition, enforces the
auto-execution policy (conversations may only run READ_ONLY and
IDEMPOTENT_WRITE tools), checks scopes and params against the declaration,
honours idempotency replay, applies the side-effect retry contract, and writes
one persistent, auditable record per invocation.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from trpc_service.governance import DataClassification
from trpc_service.tool.registry import (
    AUTO_EXECUTABLE_SIDE_EFFECTS,
    ToolDefinition,
    ToolInvocationError,
    ToolRegistry,
    ToolSideEffect,
    canonical_params_hash,
    validate_params,
)


class ToolInvocationStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class ToolBackendResult:
    ok: bool
    result: dict[str, Any] | None
    error_code: str | None
    transient: bool = False


@dataclass(frozen=True)
class ToolInvocationRecord:
    """One persistent, auditable tool invocation with its final state."""

    call_id: str
    tenant_id: str
    tool_name: str
    tool_version: int | None
    side_effect: str | None
    params: dict[str, Any]
    params_hash: str
    idempotency_key: str | None
    status: ToolInvocationStatus
    attempts: int
    cost_micros: int
    requested_by: str
    execution_id: str | None = None
    session_id: str | None = None
    release_id: str | None = None
    error_code: str | None = None
    result: dict[str, Any] | None = None
    data_classification: DataClassification | None = None


class ToolBackend(Protocol):
    """Pluggable tool transport; production backends replace the Fake."""

    async def execute(
        self, definition: ToolDefinition, params: dict[str, Any]
    ) -> ToolBackendResult: ...


class ToolCallStore(Protocol):
    """Durable side of the tool-call audit trail."""

    async def find_replay(
        self, tenant_id: str, idempotency_key: str
    ) -> ToolInvocationRecord | None: ...

    async def record(self, record: ToolInvocationRecord) -> None: ...


@dataclass(frozen=True)
class ToolInvocationResult:
    status: ToolInvocationStatus
    record: ToolInvocationRecord
    replayed: bool = False
    error_code: str | None = field(default=None)


_CONVERSATION = "conversation"
_DIRECT = "direct"


class ToolInvocationService:
    """Applies the platform tool-governance contract to every invocation."""

    def __init__(
        self,
        registry: ToolRegistry,
        backend: ToolBackend,
        store: ToolCallStore,
        *,
        max_read_attempts: int = 3,
        retry_backoff_seconds: float = 0.2,
    ) -> None:
        self._registry = registry
        self._backend = backend
        self._store = store
        self._max_read_attempts = max(1, max_read_attempts)
        self._retry_backoff_seconds = retry_backoff_seconds

    async def invoke(
        self,
        *,
        tenant_id: str,
        tool_name: str,
        params: dict[str, Any],
        scopes: frozenset[str] | None,
        requested_by: str,
        idempotency_key: str | None = None,
        mode: str = _CONVERSATION,
        version: int | None = None,
        execution_id: str | None = None,
        session_id: str | None = None,
        release_id: str | None = None,
    ) -> ToolInvocationResult:
        definition = self._registry.resolve(tenant_id, tool_name, version=version)
        if inspect.isawaitable(definition):
            definition = await definition
        if definition is None:
            # Registries may optionally expose cross-tenant name knowledge; the
            # DB-backed one cannot (RLS), so unknown tenants report NOT_FOUND.
            has_name = getattr(self._registry, "has_name", None)
            tenant_denied = has_name(tool_name) if has_name is not None else False
            if inspect.isawaitable(tenant_denied):
                tenant_denied = await tenant_denied
            blocked = await self._record_blocked(
                tenant_id=tenant_id,
                tool_name=tool_name,
                params=params,
                idempotency_key=None,
                requested_by=requested_by,
                error_code=("TOOL_TENANT_DENIED" if tenant_denied else "TOOL_NOT_FOUND"),
                execution_id=execution_id,
                session_id=session_id,
                release_id=release_id,
            )
            raise ToolInvocationError(blocked.error_code or "TOOL_NOT_FOUND")

        if mode == _CONVERSATION and definition.side_effect not in AUTO_EXECUTABLE_SIDE_EFFECTS:
            await self._record_blocked(
                tenant_id=tenant_id,
                tool_name=tool_name,
                params=params,
                idempotency_key=None,
                requested_by=requested_by,
                error_code="TOOL_AUTO_EXECUTION_BLOCKED",
                tool_version=definition.version,
                side_effect=definition.side_effect,
                execution_id=execution_id,
                session_id=session_id,
                release_id=release_id,
            )
            raise ToolInvocationError("TOOL_AUTO_EXECUTION_BLOCKED")

        effective_scopes = frozenset(definition.scopes if scopes is None else scopes)
        if not frozenset(definition.scopes).issubset(effective_scopes):
            await self._record_blocked(
                tenant_id=tenant_id,
                tool_name=tool_name,
                params=params,
                idempotency_key=None,
                requested_by=requested_by,
                error_code="TOOL_SCOPE_DENIED",
                tool_version=definition.version,
                side_effect=definition.side_effect,
                execution_id=execution_id,
                session_id=session_id,
                release_id=release_id,
            )
            raise ToolInvocationError("TOOL_SCOPE_DENIED")

        try:
            normalized = validate_params(definition, dict(params))
        except ToolInvocationError:
            await self._record_blocked(
                tenant_id=tenant_id,
                tool_name=tool_name,
                params=dict(params),
                idempotency_key=None,
                requested_by=requested_by,
                error_code="TOOL_PARAMS_INVALID",
                tool_version=definition.version,
                side_effect=definition.side_effect,
                execution_id=execution_id,
                session_id=session_id,
                release_id=release_id,
            )
            raise

        params_hash = canonical_params_hash(normalized)
        if idempotency_key is not None:
            replayed = await self._store.find_replay(tenant_id, idempotency_key)
            if replayed is not None:
                if replayed.params_hash != params_hash:
                    await self._record_blocked(
                        tenant_id=tenant_id,
                        tool_name=tool_name,
                        params=normalized,
                        idempotency_key=None,
                        requested_by=requested_by,
                        error_code="TOOL_IDEMPOTENCY_CONFLICT",
                        tool_version=definition.version,
                        side_effect=definition.side_effect,
                        execution_id=execution_id,
                        session_id=session_id,
                        release_id=release_id,
                    )
                    raise ToolInvocationError("TOOL_IDEMPOTENCY_CONFLICT")
                return ToolInvocationResult(
                    status=replayed.status,
                    record=replayed,
                    replayed=True,
                    error_code=replayed.error_code,
                )

        status, error_code, result, attempts = await self._execute_with_contract(
            definition, normalized, idempotency_key
        )
        record = ToolInvocationRecord(
            call_id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            tool_name=definition.name,
            tool_version=definition.version,
            side_effect=str(definition.side_effect),
            params=normalized,
            params_hash=params_hash,
            idempotency_key=idempotency_key,
            status=status,
            attempts=attempts,
            cost_micros=definition.cost_per_call_micros,
            requested_by=requested_by,
            execution_id=execution_id,
            session_id=session_id,
            release_id=release_id,
            error_code=error_code,
            result=result,
            data_classification=definition.data_classification,
        )
        await self._store.record(record)
        return ToolInvocationResult(status=status, record=record, error_code=error_code)

    async def _execute_with_contract(
        self,
        definition: ToolDefinition,
        params: dict[str, Any],
        idempotency_key: str | None,
    ) -> tuple[ToolInvocationStatus, str | None, dict[str, Any] | None, int]:
        """READ_ONLY backs off and retries; IDEMPOTENT_WRITE retries only when
        the downstream supports idempotency keys; NON_IDEMPOTENT_WRITE never
        retries and reports OUTCOME_UNKNOWN when the result is uncertain."""

        can_retry = definition.side_effect == ToolSideEffect.READ_ONLY or (
            definition.side_effect == ToolSideEffect.IDEMPOTENT_WRITE
            and definition.supports_idempotency
            and idempotency_key is not None
        )
        max_attempts = self._max_read_attempts if can_retry else 1
        attempts = 0
        last_error: str | None = None
        while attempts < max_attempts:
            if attempts > 0:
                await asyncio.sleep(self._retry_backoff_seconds)
            attempts += 1
            try:
                outcome = await asyncio.wait_for(
                    self._backend.execute(definition, params),
                    timeout=definition.timeout_seconds,
                )
            except TimeoutError:
                last_error = "TOOL_TIMEOUT"
                if can_retry and attempts < max_attempts:
                    continue
                return _write_outcome(definition.side_effect, last_error, attempts)
            except Exception as error:  # transport-level uncertainty
                last_error = "TOOL_BACKEND_UNAVAILABLE"
                if can_retry and attempts < max_attempts:
                    continue
                if _is_uncertain(error) and definition.side_effect != ToolSideEffect.READ_ONLY:
                    return (ToolInvocationStatus.OUTCOME_UNKNOWN, last_error, None, attempts)
                return (ToolInvocationStatus.FAILED, last_error, None, attempts)
            if outcome.ok:
                return (ToolInvocationStatus.SUCCEEDED, None, outcome.result, attempts)
            last_error = outcome.error_code or "TOOL_FAILED"
            if outcome.transient and can_retry and attempts < max_attempts:
                continue
            if outcome.transient and not can_retry:
                return (ToolInvocationStatus.OUTCOME_UNKNOWN, last_error, None, attempts)
            return (ToolInvocationStatus.FAILED, last_error, None, attempts)
        return (ToolInvocationStatus.FAILED, last_error, None, attempts)

    async def _record_blocked(
        self,
        *,
        tenant_id: str,
        tool_name: str,
        params: dict[str, Any],
        idempotency_key: str | None,
        requested_by: str,
        error_code: str,
        tool_version: int | None = None,
        side_effect: ToolSideEffect | None = None,
        execution_id: str | None = None,
        session_id: str | None = None,
        release_id: str | None = None,
    ) -> ToolInvocationRecord:
        record = ToolInvocationRecord(
            call_id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            tool_name=tool_name,
            tool_version=tool_version,
            side_effect=str(side_effect) if side_effect else None,
            params=params,
            params_hash=canonical_params_hash(params),
            idempotency_key=idempotency_key,
            status=ToolInvocationStatus.BLOCKED,
            attempts=0,
            cost_micros=0,
            requested_by=requested_by,
            execution_id=execution_id,
            session_id=session_id,
            release_id=release_id,
            error_code=error_code,
        )
        await self._store.record(record)
        return record


def _write_outcome(
    side_effect: ToolSideEffect, error_code: str, attempts: int
) -> tuple[ToolInvocationStatus, str | None, dict[str, Any] | None, int]:
    if side_effect in (ToolSideEffect.IDEMPOTENT_WRITE, ToolSideEffect.NON_IDEMPOTENT_WRITE):
        return (ToolInvocationStatus.OUTCOME_UNKNOWN, error_code, None, attempts)
    return (ToolInvocationStatus.FAILED, error_code, None, attempts)


def _is_uncertain(error: Exception) -> bool:
    """Whether an exception leaves the call outcome genuinely unknown."""

    return isinstance(error, ConnectionError | asyncio.IncompleteReadError)
