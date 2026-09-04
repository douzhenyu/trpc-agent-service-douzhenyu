"""Durable tool approvals with separation of duties and single-use consumption.

An approval request pins one immutable tool-call intent: the release, the
exact tool version, the normalized params hash, the requesting subject, the
governing policy version and a fifteen-minute default validity window.
NON_IDEMPOTENT_WRITE may be confirmed by the requesting subject when the role
allows it; HIGH_RISK always requires a distinct approver role. Approval is
single use: consuming it flips the request to CONSUMED.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from trpc_service.tool.registry import ToolSideEffect

DEFAULT_APPROVAL_TTL_SECONDS = 900
APPROVER_ROLES = frozenset({"TENANT_ADMIN"})
SELF_SERVICE_ROLES = frozenset({"TENANT_ADMIN", "AGENT_DEVELOPER"})


class ApprovalStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    DENIED = "DENIED"
    EXPIRED = "EXPIRED"
    CONSUMED = "CONSUMED"


class ApprovalDecision(StrEnum):
    APPROVE = "APPROVE"
    DENY = "DENY"


class ApprovalError(RuntimeError):
    """Stable approval failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ApprovalRequest(BaseModel):
    """One immutable, expiring approval for one tool-call intent."""

    model_config = ConfigDict(frozen=True)

    approval_id: str
    tenant_id: str
    release_id: str
    tool_name: str
    tool_version: int
    params_hash: str
    params: dict[str, object]
    side_effect: ToolSideEffect
    requested_by: str
    requester_role: str
    policy_version: str
    status: ApprovalStatus = ApprovalStatus.PENDING
    requested_at: datetime
    expires_at: datetime
    decided_by: str | None = None
    decided_at: datetime | None = None


class ApprovalStore(Protocol):
    async def upsert(self, request: ApprovalRequest) -> None: ...
    async def transition(
        self,
        tenant_id: str,
        approval_id: str,
        *,
        from_statuses: tuple[ApprovalStatus, ...],
        to_status: ApprovalStatus,
        decided_by: str | None = None,
        decided_at: object | None = None,
    ) -> ApprovalRequest | None:
        """Compare-and-swap one status transition; None when the state moved."""
        ...

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRequest | None: ...
    async def find_open(
        self,
        tenant_id: str,
        *,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        requested_by: str,
        statuses: tuple[ApprovalStatus, ...],
    ) -> ApprovalRequest | None: ...


class MemoryApprovalStore:
    def __init__(self) -> None:
        self._requests: dict[str, ApprovalRequest] = {}

    async def upsert(self, request: ApprovalRequest) -> None:
        self._requests[request.approval_id] = request

    async def transition(
        self,
        tenant_id: str,
        approval_id: str,
        *,
        from_statuses: tuple[ApprovalStatus, ...],
        to_status: ApprovalStatus,
        decided_by: str | None = None,
        decided_at: object | None = None,
    ) -> ApprovalRequest | None:
        request = self._requests.get(approval_id)
        if request is None or request.tenant_id != tenant_id:
            return None
        if request.status not in from_statuses:
            return None
        updates: dict[str, object] = {"status": to_status}
        if decided_by is not None:
            updates["decided_by"] = decided_by
        if decided_at is not None:
            updates["decided_at"] = decided_at
        transitioned = request.model_copy(update=updates)
        self._requests[approval_id] = transitioned
        return transitioned

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRequest | None:
        request = self._requests.get(approval_id)
        if request is None or request.tenant_id != tenant_id:
            return None
        return request

    async def find_open(
        self,
        tenant_id: str,
        *,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        requested_by: str,
        statuses: tuple[ApprovalStatus, ...],
    ) -> ApprovalRequest | None:
        for request in self._requests.values():
            if (
                request.tenant_id == tenant_id
                and request.tool_name == tool_name
                and request.tool_version == tool_version
                and request.params_hash == params_hash
                and request.requested_by == requested_by
                and request.status in statuses
            ):
                return request
        return None


class ApprovalService:
    """Creates, decides and consumes bound approval requests."""

    def __init__(
        self, store: ApprovalStore, *, default_ttl_seconds: int = DEFAULT_APPROVAL_TTL_SECONDS
    ) -> None:
        self.store = store
        self._default_ttl_seconds = default_ttl_seconds

    @classmethod
    def in_memory(cls) -> ApprovalService:
        return cls(MemoryApprovalStore())

    async def create(
        self,
        *,
        tenant_id: str,
        release_id: str,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        params: dict[str, object],
        side_effect: ToolSideEffect,
        requested_by: str,
        requester_role: str,
        policy_version: str,
    ) -> ApprovalRequest:
        now = datetime.now(UTC)
        request = ApprovalRequest(
            approval_id=str(uuid4()),
            tenant_id=tenant_id,
            release_id=release_id,
            tool_name=tool_name,
            tool_version=tool_version,
            params_hash=params_hash,
            params=dict(params),
            side_effect=side_effect,
            requested_by=requested_by,
            requester_role=requester_role,
            policy_version=policy_version,
            requested_at=now,
            expires_at=now + timedelta(seconds=self._default_ttl_seconds),
        )
        await self.store.upsert(request)
        return request

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRequest:
        request = await self.store.get(tenant_id, approval_id)
        if request is None:
            raise ApprovalError("APPROVAL_NOT_FOUND")
        if request.status in (
            ApprovalStatus.PENDING,
            ApprovalStatus.APPROVED,
        ) and request.expires_at <= datetime.now(UTC):
            expired = await self.store.transition(
                tenant_id,
                approval_id,
                from_statuses=(ApprovalStatus.PENDING, ApprovalStatus.APPROVED),
                to_status=ApprovalStatus.EXPIRED,
            )
            if expired is not None:
                return expired
        return request

    async def decide(
        self,
        tenant_id: str,
        approval_id: str,
        decision: ApprovalDecision,
        *,
        decided_by: str,
        decided_by_role: str,
    ) -> ApprovalRequest:
        """Grant or deny one pending request, enforcing separation of duties."""

        request = await self.get(tenant_id, approval_id)
        if request.status == ApprovalStatus.EXPIRED:
            raise ApprovalError("APPROVAL_EXPIRED")
        if request.status != ApprovalStatus.PENDING:
            raise ApprovalError("APPROVAL_ALREADY_DECIDED")
        if request.side_effect == ToolSideEffect.HIGH_RISK:
            if decided_by == request.requested_by:
                raise ApprovalError("APPROVAL_SELF_DENIED")
            if decided_by_role not in APPROVER_ROLES:
                raise ApprovalError("APPROVER_ROLE_REQUIRED")
        elif decided_by_role not in SELF_SERVICE_ROLES | APPROVER_ROLES:
            raise ApprovalError("APPROVER_ROLE_REQUIRED")
        decided = await self.store.transition(
            tenant_id,
            approval_id,
            from_statuses=(ApprovalStatus.PENDING,),
            to_status=(
                ApprovalStatus.APPROVED
                if decision == ApprovalDecision.APPROVE
                else ApprovalStatus.DENIED
            ),
            decided_by=decided_by,
            decided_at=datetime.now(UTC),
        )
        if decided is None:
            raise ApprovalError("APPROVAL_ALREADY_DECIDED")
        return decided

    async def consume(
        self,
        tenant_id: str,
        approval_id: str,
        *,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        requested_by: str,
        release_id: str,
    ) -> ApprovalRequest:
        """Flip one approved request to CONSUMED; exactly once via CAS."""

        request = await self.get(tenant_id, approval_id)
        if request.status == ApprovalStatus.EXPIRED:
            raise ApprovalError("APPROVAL_EXPIRED")
        if request.status != ApprovalStatus.APPROVED:
            raise ApprovalError("APPROVAL_NOT_APPROVED")
        if (
            request.tool_name != tool_name
            or request.tool_version != tool_version
            or request.params_hash != params_hash
            or request.requested_by != requested_by
            or request.release_id != release_id
        ):
            raise ApprovalError("APPROVAL_BINDING_MISMATCH")
        consumed = await self.store.transition(
            tenant_id,
            approval_id,
            from_statuses=(ApprovalStatus.APPROVED,),
            to_status=ApprovalStatus.CONSUMED,
        )
        if consumed is None:
            raise ApprovalError("APPROVAL_NOT_APPROVED")
        return consumed

    async def find_approved(
        self,
        tenant_id: str,
        *,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        requested_by: str,
    ) -> ApprovalRequest | None:
        """A consumable approval for exactly this intent, if one exists."""

        request = await self.store.find_open(
            tenant_id,
            tool_name=tool_name,
            tool_version=tool_version,
            params_hash=params_hash,
            requested_by=requested_by,
            statuses=(ApprovalStatus.APPROVED,),
        )
        if request is None:
            return None
        if request.expires_at <= datetime.now(UTC):
            expired = request.model_copy(update={"status": ApprovalStatus.EXPIRED})
            await self.store.upsert(expired)
            return None
        return request

    async def find_pending(
        self,
        tenant_id: str,
        *,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        requested_by: str,
    ) -> ApprovalRequest | None:
        """One already-open PENDING request for this intent, if any."""

        request = await self.store.find_open(
            tenant_id,
            tool_name=tool_name,
            tool_version=tool_version,
            params_hash=params_hash,
            requested_by=requested_by,
            statuses=(ApprovalStatus.PENDING,),
        )
        if request is None:
            return None
        if request.expires_at <= datetime.now(UTC):
            expired = request.model_copy(update={"status": ApprovalStatus.EXPIRED})
            await self.store.upsert(expired)
            return None
        return request
