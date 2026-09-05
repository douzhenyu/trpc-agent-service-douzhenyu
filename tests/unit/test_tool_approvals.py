"""Unit tests for durable tool approvals, separation of duties and recovery checkpoints."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.unit.test_tool_governance import (
    MemoryStore,
    ScriptedBackend,
    _definition,
    store_records,
)
from trpc_service.sessions import LeaseGrant, SessionLeaseError
from trpc_service.tool.approvals import (
    APPROVER_ROLES,
    ApprovalDecision,
    ApprovalError,
    ApprovalService,
    ApprovalStatus,
)
from trpc_service.tool.checkpoints import CheckpointError, CheckpointService
from trpc_service.tool.executor import (
    ToolBackendResult,
    ToolInvocationError,
    ToolInvocationService,
)
from trpc_service.tool.reconciliation import (
    ReconciliationDecision,
    ReconciliationError,
    ReconciliationService,
)
from trpc_service.tool.registry import ToolRegistry, ToolSideEffect


def _write_definition(
    tenant_id: str = "t-1",
    name: str = "charge_card",
    side_effect: ToolSideEffect = ToolSideEffect.HIGH_RISK,
) -> Any:
    return _definition(tenant_id=tenant_id, name=name, side_effect=side_effect, scopes=())


def _service(
    approvals: ApprovalService | None = None,
    backend: Any | None = None,
    side_effect: ToolSideEffect = ToolSideEffect.HIGH_RISK,
) -> ToolInvocationService:
    registry = ToolRegistry.in_memory()
    registry.register(_write_definition(side_effect=side_effect))
    return ToolInvocationService(
        registry,
        backend if backend is not None else ScriptedBackend([]),
        MemoryStore(),
        approvals=approvals,
        retry_backoff_seconds=0,
    )


def _invoke(service: ToolInvocationService, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = dict(
        tenant_id="t-1",
        tool_name="charge_card",
        params={"account_id": "a-1"},
        scopes=frozenset(),
        requested_by="dev-1",
        release_id="r-1",
        policy_version="policy-v7",
        mode="conversation",
    )
    kwargs.update(overrides)
    return asyncio.run(service.invoke(**kwargs))


def _create(
    service: ApprovalService,
    *,
    requested_by: str = "dev-1",
    requester_role: str = "AGENT_DEVELOPER",
    side_effect: ToolSideEffect = ToolSideEffect.HIGH_RISK,
    tool_name: str = "charge_card",
) -> Any:
    return asyncio.run(
        service.create(
            tenant_id="t-1",
            release_id="r-1",
            tool_name=tool_name,
            tool_version=1,
            params_hash="a" * 64,
            params={"account_id": "a-1"},
            side_effect=side_effect,
            requested_by=requested_by,
            requester_role=requester_role,
            policy_version="policy-v7",
        )
    )


def test_approval_request_binds_release_tool_params_subject_policy_and_expiry() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    assert request.release_id == "r-1"
    assert request.tool_name == "charge_card"
    assert request.tool_version == 1
    assert request.params_hash == "a" * 64
    assert request.requested_by == "dev-1"
    assert request.policy_version == "policy-v7"
    assert request.status == ApprovalStatus.PENDING
    assert (request.expires_at - request.requested_at).total_seconds() == 900


def test_high_risk_cannot_be_approved_by_the_requester() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals, requested_by="admin-1", requester_role="TENANT_ADMIN")
    with pytest.raises(ApprovalError, match="APPROVAL_SELF_DENIED"):
        asyncio.run(
            approvals.decide(
                "t-1",
                request.approval_id,
                ApprovalDecision.APPROVE,
                decided_by="admin-1",
                decided_by_role="TENANT_ADMIN",
            )
        )


def test_high_risk_requires_a_distinct_approver_role() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    with pytest.raises(ApprovalError, match="APPROVER_ROLE_REQUIRED"):
        asyncio.run(
            approvals.decide(
                "t-1",
                request.approval_id,
                ApprovalDecision.APPROVE,
                decided_by="auditor-1",
                decided_by_role="TENANT_AUDITOR",
            )
        )
    approved = asyncio.run(
        approvals.decide(
            "t-1",
            request.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="admin-2",
            decided_by_role="TENANT_ADMIN",
        )
    )
    assert approved.status == ApprovalStatus.APPROVED
    assert approved.decided_by == "admin-2"
    assert "TENANT_ADMIN" in APPROVER_ROLES


def test_non_idempotent_write_can_be_confirmed_by_the_requester() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(
        approvals,
        side_effect=ToolSideEffect.NON_IDEMPOTENT_WRITE,
    )
    confirmed = asyncio.run(
        approvals.decide(
            "t-1",
            request.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="dev-1",
            decided_by_role="AGENT_DEVELOPER",
        )
    )
    assert confirmed.status == ApprovalStatus.APPROVED


def test_approval_consumption_is_single_use_and_binding_checked() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    asyncio.run(
        approvals.decide(
            "t-1",
            request.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="admin-2",
            decided_by_role="TENANT_ADMIN",
        )
    )
    consumed = asyncio.run(
        approvals.consume(
            "t-1",
            request.approval_id,
            tool_name="charge_card",
            tool_version=1,
            params_hash="a" * 64,
            requested_by="dev-1",
            release_id="r-1",
        )
    )
    assert consumed.status == ApprovalStatus.CONSUMED
    with pytest.raises(ApprovalError, match="APPROVAL_NOT_APPROVED"):
        asyncio.run(
            approvals.consume(
                "t-1",
                request.approval_id,
                tool_name="charge_card",
                tool_version=1,
                params_hash="a" * 64,
                requested_by="dev-1",
                release_id="r-1",
            )
        )


def test_consumption_rejects_tampered_binding() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    asyncio.run(
        approvals.decide(
            "t-1",
            request.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="admin-2",
            decided_by_role="TENANT_ADMIN",
        )
    )
    for overrides in (
        {"params_hash": "f" * 64},
        {"requested_by": "someone-else"},
        {"tool_version": 2},
        {"tool_name": "other_tool"},
        {"release_id": "r-OTHER"},
    ):
        base: dict[str, Any] = dict(
            tool_name="charge_card",
            tool_version=1,
            params_hash="a" * 64,
            requested_by="dev-1",
            release_id="r-1",
        )
        base.update(overrides)
        with pytest.raises(ApprovalError, match="APPROVAL_BINDING_MISMATCH"):
            asyncio.run(approvals.consume("t-1", request.approval_id, **base))


def test_expired_approval_is_marked_expired_on_read() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    stale = request.model_copy(update={"expires_at": request.requested_at.replace(year=2020)})
    asyncio.run(approvals.store.upsert(stale))
    assert asyncio.run(approvals.get("t-1", request.approval_id)).status == (ApprovalStatus.EXPIRED)


def test_executor_creates_pending_approval_and_blocks_conversation_calls() -> None:
    approvals = ApprovalService.in_memory()
    service = _service(approvals=approvals)
    with pytest.raises(ToolInvocationError, match="TOOL_APPROVAL_REQUIRED") as excinfo:
        _invoke(service)
    assert excinfo.value.details is not None
    approval_id = excinfo.value.details["approval_id"]
    pending = asyncio.run(approvals.get("t-1", approval_id))
    assert pending.status == ApprovalStatus.PENDING
    assert pending.release_id == "r-1"
    assert pending.policy_version == "policy-v7"
    assert [record.error_code for record in store_records(service)] == ["TOOL_APPROVAL_REQUIRED"]
    with pytest.raises(ToolInvocationError, match="TOOL_APPROVAL_REQUIRED") as again:
        _invoke(service)
    assert again.value.details is not None
    assert again.value.details["approval_id"] == approval_id


def test_executor_consumes_approved_approval_then_executes_once() -> None:
    approvals = ApprovalService.in_memory()
    backend = ScriptedBackend(
        [ToolBackendResult(ok=True, result={"charged": True}, error_code=None, transient=False)]
    )
    service = _service(
        approvals=approvals,
        backend=backend,
        side_effect=ToolSideEffect.NON_IDEMPOTENT_WRITE,
    )
    with pytest.raises(ToolInvocationError, match="TOOL_APPROVAL_REQUIRED") as excinfo:
        _invoke(service, mode="direct")
    approval_id = str(excinfo.value.details["approval_id"])
    asyncio.run(
        approvals.decide(
            "t-1",
            approval_id,
            ApprovalDecision.APPROVE,
            decided_by="dev-1",
            decided_by_role="AGENT_DEVELOPER",
        )
    )
    result = _invoke(service, mode="direct")
    assert result.status.value == "SUCCEEDED"
    assert result.record.result == {"charged": True}
    consumed = asyncio.run(approvals.get("t-1", approval_id))
    assert consumed.status == ApprovalStatus.CONSUMED
    with pytest.raises(ToolInvocationError, match="TOOL_APPROVAL_REQUIRED"):
        _invoke(service, params={"account_id": "a-DIFFERENT"}, mode="direct")


class FakeLeaseManager:
    """In-memory stand-in for SessionLeaseManager with release semantics."""

    def __init__(self) -> None:
        self.held: dict[str, str] = {}
        self.acquired: list[str] = []
        self.released: list[str] = []
        self.tokens = 0

    async def acquire(self, tenant_id: str, session_id: str, owner_id: str) -> LeaseGrant:
        holder = self.held.get(session_id)
        if holder is not None and holder != owner_id:
            raise SessionLeaseError("SESSION_LEASE_HELD:other")
        self.tokens += 1
        self.held[session_id] = owner_id
        self.acquired.append(owner_id)
        return LeaseGrant(fencing_token=self.tokens, session_version=1)

    async def release(self, tenant_id: str, session_id: str, owner_id: str) -> None:
        if self.held.get(session_id) == owner_id:
            self.released.append(session_id)
            self.held.pop(session_id, None)


def _park(
    service: CheckpointService,
    approvals: ApprovalService,
    lease_manager: FakeLeaseManager,
    **overrides: Any,
) -> Any:
    request = _create(approvals)
    kwargs: dict[str, Any] = dict(
        tenant_id="t-1",
        execution_id="e-1",
        session_id="s-1",
        release_id="r-1",
        approval_id=request.approval_id,
        tool_name="charge_card",
        tool_version=1,
        params_hash="a" * 64,
        requested_by="dev-1",
        parked_by="worker-1",
        lease_manager=lease_manager,
    )
    kwargs.update(overrides)
    return asyncio.run(service.park(**kwargs))


def test_park_releases_the_worker_lease_and_persists_the_wait() -> None:
    service = CheckpointService.in_memory()
    approvals = ApprovalService.in_memory()
    lease_manager = FakeLeaseManager()
    asyncio.run(lease_manager.acquire("t-1", "s-1", "worker-1"))
    checkpoint = _park(service, approvals, lease_manager)
    assert lease_manager.released == ["s-1"]
    assert checkpoint.status.value == "WAITING_APPROVAL"
    assert checkpoint.tool_name == "charge_card"


def test_resume_requires_a_granted_approval() -> None:
    service = CheckpointService.in_memory()
    approvals = ApprovalService.in_memory()
    checkpoint = _park(service, approvals, FakeLeaseManager())
    with pytest.raises(CheckpointError, match="CHECKPOINT_APPROVAL_NOT_GRANTED"):
        asyncio.run(
            service.resume(
                "t-1",
                checkpoint.checkpoint_id,
                approvals=approvals,
                lease_manager=FakeLeaseManager(),
                resumed_by="worker-2",
            )
        )


def test_resume_consumes_approval_and_reacquires_the_session_lease() -> None:
    service = CheckpointService.in_memory()
    approvals = ApprovalService.in_memory()
    checkpoint = _park(service, approvals, FakeLeaseManager())
    asyncio.run(
        approvals.decide(
            "t-1",
            checkpoint.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="admin-2",
            decided_by_role="TENANT_ADMIN",
        )
    )
    lease_manager = FakeLeaseManager()
    grant = asyncio.run(
        service.resume(
            "t-1",
            checkpoint.checkpoint_id,
            approvals=approvals,
            lease_manager=lease_manager,
            resumed_by="worker-2",
        )
    )
    assert grant.fencing_token == 1
    assert lease_manager.acquired == ["worker-2"]
    stored = asyncio.run(service.get("t-1", checkpoint.checkpoint_id))
    assert stored.status.value == "RESUMED"
    assert stored.resumed_by == "worker-2"
    consumed = asyncio.run(approvals.get("t-1", checkpoint.approval_id))
    assert consumed.status == ApprovalStatus.CONSUMED


def test_double_resume_is_rejected() -> None:
    service = CheckpointService.in_memory()
    approvals = ApprovalService.in_memory()
    checkpoint = _park(service, approvals, FakeLeaseManager())
    asyncio.run(
        approvals.decide(
            "t-1",
            checkpoint.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="admin-2",
            decided_by_role="TENANT_ADMIN",
        )
    )
    for _ in range(2):
        asyncio.run(
            service.resume(
                "t-1",
                checkpoint.checkpoint_id,
                approvals=approvals,
                lease_manager=FakeLeaseManager(),
                resumed_by="worker-2",
            )
        )
        break
    with pytest.raises(CheckpointError, match="CHECKPOINT_NOT_WAITING"):
        asyncio.run(
            service.resume(
                "t-1",
                checkpoint.checkpoint_id,
                approvals=approvals,
                lease_manager=FakeLeaseManager(),
                resumed_by="worker-3",
            )
        )


def test_reconciliation_resolves_unknown_outcomes_once() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(_write_definition(side_effect=ToolSideEffect.NON_IDEMPOTENT_WRITE))
    call_store = MemoryStore()
    service = ToolInvocationService(
        registry,
        ScriptedBackend(
            [ToolBackendResult(ok=False, result=None, error_code="TOOL_TIMEOUT", transient=True)]
        ),
        call_store,
        retry_backoff_seconds=0,
    )
    result = asyncio.run(
        service.invoke(
            tenant_id="t-1",
            tool_name="charge_card",
            params={"account_id": "a-1"},
            scopes=frozenset(),
            mode="direct",
            requested_by="dev-1",
        )
    )
    assert result.status.value == "OUTCOME_UNKNOWN"

    recon = ReconciliationService.in_memory(call_store)
    open_calls = asyncio.run(recon.open_calls("t-1"))
    assert [call.call_id for call in open_calls] == [result.record.call_id]

    resolution = asyncio.run(
        recon.resolve(
            "t-1",
            result.record.call_id,
            ReconciliationDecision.CONFIRMED_EXECUTED,
            resolved_by="ops-1",
            note="downstream ledger shows the charge",
        )
    )
    assert resolution.decision == ReconciliationDecision.CONFIRMED_EXECUTED
    assert asyncio.run(recon.open_calls("t-1")) == []
    with pytest.raises(ReconciliationError, match="RECONCILIATION_ALREADY_RESOLVED"):
        asyncio.run(
            recon.resolve(
                "t-1",
                result.record.call_id,
                ReconciliationDecision.CONFIRMED_NOT_EXECUTED,
                resolved_by="ops-2",
            )
        )


def test_deciding_an_expired_approval_reports_expiry() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    asyncio.run(
        approvals.store.transition(
            "t-1",
            request.approval_id,
            from_statuses=(ApprovalStatus.PENDING,),
            to_status=ApprovalStatus.EXPIRED,
        )
    )
    with pytest.raises(ApprovalError, match="APPROVAL_EXPIRED"):
        asyncio.run(
            approvals.decide(
                "t-1",
                request.approval_id,
                ApprovalDecision.APPROVE,
                decided_by="admin-2",
                decided_by_role="TENANT_ADMIN",
            )
        )


def test_consume_loses_the_race_when_another_consumer_won() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    asyncio.run(
        approvals.decide(
            "t-1",
            request.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="admin-2",
            decided_by_role="TENANT_ADMIN",
        )
    )
    first = asyncio.run(
        approvals.store.transition(
            "t-1",
            request.approval_id,
            from_statuses=(ApprovalStatus.APPROVED,),
            to_status=ApprovalStatus.CONSUMED,
        )
    )
    assert first is not None
    with pytest.raises(ApprovalError, match="APPROVAL_NOT_APPROVED"):
        asyncio.run(
            approvals.consume(
                "t-1",
                request.approval_id,
                tool_name="charge_card",
                tool_version=1,
                params_hash="a" * 64,
                requested_by="dev-1",
                release_id="r-1",
            )
        )


def test_consume_rejects_a_release_mismatch() -> None:
    approvals = ApprovalService.in_memory()
    request = _create(approvals)
    asyncio.run(
        approvals.decide(
            "t-1",
            request.approval_id,
            ApprovalDecision.APPROVE,
            decided_by="admin-2",
            decided_by_role="TENANT_ADMIN",
        )
    )
    with pytest.raises(ApprovalError, match="APPROVAL_BINDING_MISMATCH"):
        asyncio.run(
            approvals.consume(
                "t-1",
                request.approval_id,
                tool_name="charge_card",
                tool_version=1,
                params_hash="a" * 64,
                requested_by="dev-1",
                release_id="release-OTHER",
            )
        )
