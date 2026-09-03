"""Append-only audit writes shared by Admin API domain routes."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from trpc_service.admin_api.auth import Principal
from trpc_service.admin_api.database import Connection, Database
from trpc_service.ids import uuid7


async def insert_audit(
    connection: Connection,
    principal: Principal | None,
    action: str,
    decision: str,
    *,
    target_type: str | None = None,
    target_id: str | None = None,
    tenant_id: UUID | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    await connection.execute(
        """INSERT INTO platform.audit_event
            (id,tenant_id,actor,auth_method,action,decision,target_type,target_id,details)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS jsonb))""",
        uuid7(),
        tenant_id,
        principal.subject if principal else "anonymous",
        principal.auth_method if principal else "anonymous",
        action,
        decision,
        target_type,
        target_id,
        json.dumps(details or {}),
    )


async def write_audit(
    database: Database,
    principal: Principal | None,
    action: str,
    decision: str,
    **fields: Any,
) -> None:
    async with database.transaction() as connection:
        await insert_audit(connection, principal, action, decision, **fields)
