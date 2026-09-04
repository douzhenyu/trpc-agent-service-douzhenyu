"""Tool approval decisions and OUTCOME_UNKNOWN reconciliation endpoints."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict

from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.tool.approvals import (
    APPROVER_ROLES,
    SELF_SERVICE_ROLES,
    ApprovalDecision,
    ApprovalError,
    ApprovalRequest,
    ApprovalService,
    ApprovalStatus,
)
from trpc_service.tool.reconciliation import (
    ReconciliationDecision,
    ReconciliationError,
    ReconciliationService,
)
from trpc_service.tool.store import (
    DatabaseApprovalStore,
    DatabaseReconciliationStore,
    DatabaseToolCallStore,
)


class ApprovalDecisionRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    decision: Literal["APPROVE", "DENY"]


class ApprovalResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    approval_id: UUID
    tenant_id: UUID
    release_id: str
    tool_name: str
    tool_version: int
    params_hash: str
    params: dict[str, object]
    side_effect: str
    requested_by: str
    requester_role: str
    policy_version: str
    status: ApprovalStatus
    requested_at: object
    expires_at: object
    decided_by: str | None
    decided_at: object | None


class ApprovalList(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    approvals: list[ApprovalResponse]


class ResolutionRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    decision: Literal["CONFIRMED_EXECUTED", "CONFIRMED_NOT_EXECUTED"]
    note: str | None = None


async def resolve_decider_role(database: Database, principal: Principal, tenant_id: UUID) -> str:
    """The role a principal holds for separation-of-duties checks."""

    if principal.auth_method == "emergency" or "PLATFORM_ADMIN" in principal.roles:
        return "TENANT_ADMIN"
    async with database.tenant_transaction(tenant_id) as connection:
        rows = await connection.fetch(
            """SELECT r.role FROM tenant.member m
            JOIN tenant.member_role r
              ON r.tenant_id=m.tenant_id AND r.member_id=m.id
            WHERE m.tenant_id=$1 AND m.user_id=$2""",
            tenant_id,
            _uuid_or_none(principal.subject),
        )
    roles = {str(row["role"]) for row in rows}
    for role in sorted(roles):
        if role in APPROVER_ROLES:
            return role
    for role in sorted(roles):
        if role in SELF_SERVICE_ROLES:
            return role
    # Deterministic denial: the service rejects any role outside its sets.
    return "TENANT_AUDITOR"


def _uuid_or_none(value: str) -> UUID | None:
    try:
        return UUID(value)
    except ValueError:
        return None


def _approval_response(request: ApprovalRequest) -> ApprovalResponse:
    return ApprovalResponse(
        approval_id=UUID(request.approval_id),
        tenant_id=UUID(request.tenant_id),
        release_id=request.release_id,
        tool_name=request.tool_name,
        tool_version=request.tool_version,
        params_hash=request.params_hash,
        params=request.params,
        side_effect=str(request.side_effect),
        requested_by=request.requested_by,
        requester_role=request.requester_role,
        policy_version=request.policy_version,
        status=request.status,
        requested_at=request.requested_at,
        expires_at=request.expires_at,
        decided_by=request.decided_by,
        decided_at=request.decided_at,
    )


def create_tool_approval_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["tool-approvals"])

    def _service() -> ApprovalService:
        return ApprovalService(DatabaseApprovalStore(database))

    def _reconciliation() -> ReconciliationService:
        return ReconciliationService(
            DatabaseReconciliationStore(database), DatabaseToolCallStore(database)
        )

    @router.get(
        "/tool-approvals",
        response_model=ApprovalList,
        responses={**error_responses(401, 403)},
    )
    async def list_approvals(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        status: str | None = None,
    ) -> ApprovalList:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "tool_approval.list",
            target_type="tool_approval",
        )
        rows = await DatabaseApprovalStore(database).list(str(tenant_id), status=status)
        return ApprovalList(
            tenant_id=tenant_id, approvals=[_approval_response(row) for row in rows]
        )

    @router.post(
        "/tool-approvals/{approval_id}/decisions",
        response_model=ApprovalResponse,
        responses={**error_responses(401, 403, 404, 409)},
    )
    async def decide_approval(
        tenant_id: UUID,
        approval_id: UUID,
        payload: ApprovalDecisionRequest,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> ApprovalResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "tool_approval.decide",
            target_type="tool_approval",
            target_id=str(approval_id),
        )
        role = await resolve_decider_role(database, principal, tenant_id)
        try:
            decided = await _service().decide(
                str(tenant_id),
                str(approval_id),
                ApprovalDecision(payload.decision),
                decided_by=principal.subject,
                decided_by_role=role,
            )
        except ApprovalError as error:
            code = error.code
            status_code = (
                409
                if code
                in {
                    "APPROVAL_ALREADY_DECIDED",
                    "APPROVAL_SELF_DENIED",
                    "APPROVER_ROLE_REQUIRED",
                }
                else 404
            )
            raise HTTPException(status_code=status_code, detail=code) from error
        return _approval_response(decided)

    @router.get(
        "/tool-call-reconciliation",
        response_model=list[dict[str, str]],
        responses={**error_responses(401, 403)},
    )
    async def list_open_calls(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> list[dict[str, str]]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "tool_reconciliation.list",
            target_type="tool_call",
        )
        open_calls = await _reconciliation().open_calls(str(tenant_id))
        return [
            {
                "call_id": call.call_id,
                "tool_name": call.tool_name,
                "tool_version": str(call.tool_version or ""),
                "status": str(call.status),
                "error_code": call.error_code or "",
                "requested_by": call.requested_by,
            }
            for call in open_calls
        ]

    @router.post(
        "/tool-call-reconciliation/{call_id}/resolutions",
        response_model=dict[str, str],
        responses={**error_responses(401, 403, 404, 409)},
    )
    async def resolve_call(
        tenant_id: UUID,
        call_id: UUID,
        payload: ResolutionRequest,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, str]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "tool_reconciliation.resolve",
            target_type="tool_call",
            target_id=str(call_id),
        )
        try:
            resolution = await _reconciliation().resolve(
                str(tenant_id),
                str(call_id),
                ReconciliationDecision(payload.decision),
                resolved_by=principal.subject,
                note=payload.note,
            )
        except ReconciliationError as error:
            code = error.code
            status_code = 409 if code == "RECONCILIATION_ALREADY_RESOLVED" else 404
            raise HTTPException(status_code=status_code, detail=code) from error
        return {
            "call_id": resolution.call_id,
            "decision": str(resolution.decision),
            "resolved_by": resolution.resolved_by,
        }

    return router
