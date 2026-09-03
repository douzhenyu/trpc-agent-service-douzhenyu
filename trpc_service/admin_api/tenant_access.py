"""Tenant RBAC checks shared by tenant-scoped Admin API modules."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import HTTPException

from trpc_service.admin_api.audit import write_audit
from trpc_service.admin_api.auth import Principal
from trpc_service.admin_api.database import Database

AccessMode = Literal["read", "write"]

_PLATFORM_ROLES = {
    "read": frozenset({"PLATFORM_ADMIN", "PLATFORM_AUDITOR"}),
    "write": frozenset({"PLATFORM_ADMIN"}),
}
_TENANT_ROLES = {
    "read": frozenset({"TENANT_ADMIN", "AGENT_DEVELOPER", "TENANT_AUDITOR"}),
    "write": frozenset({"TENANT_ADMIN", "AGENT_DEVELOPER"}),
}


async def require_tenant_access(
    database: Database,
    principal: Principal,
    tenant_id: UUID,
    mode: AccessMode,
    action: str,
    *,
    target_type: str,
    target_id: str | None = None,
) -> None:
    if principal.roles.intersection(_PLATFORM_ROLES[mode]):
        return

    tenant_exists = False
    roles: set[str] = set()
    async with database.tenant_transaction(tenant_id) as connection:
        tenant_exists = bool(
            await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM platform.tenant WHERE id=$1)", tenant_id
            )
        )
        if principal.auth_method == "oidc":
            try:
                user_id = UUID(principal.subject)
            except ValueError:
                user_id = None
            if user_id is not None:
                rows = await connection.fetch(
                    """SELECT r.role FROM tenant.member m
                    JOIN tenant.member_role r
                      ON r.tenant_id=m.tenant_id AND r.member_id=m.id
                    WHERE m.tenant_id=$1 AND m.user_id=$2""",
                    tenant_id,
                    user_id,
                )
                roles = {str(row["role"]) for row in rows}
    if roles.intersection(_TENANT_ROLES[mode]):
        return

    await write_audit(
        database,
        principal,
        action,
        "DENY",
        target_type=target_type,
        target_id=target_id,
        tenant_id=tenant_id if tenant_exists else None,
        details={
            "reason": "insufficient_tenant_role",
            **({"requested_tenant_id": str(tenant_id)} if not tenant_exists else {}),
        },
    )
    raise HTTPException(status_code=403, detail="insufficient tenant role")
