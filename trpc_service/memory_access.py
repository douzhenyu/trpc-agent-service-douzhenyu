"""Memory visibility for IM conversations.

This module is the sole policy boundary for selecting subject-scoped Memory
for an IM request.  Private Memory is available in a direct conversation only.
Group and topic conversations deliberately return no member Memory, even when
the member has verified associations, preventing a group message from acting as
an injection vector.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from trpc_service.admin_api.database import Connection, Database

IM_DIRECT_MEMORY_POLICY = "im-subject-direct-v1"
IM_GROUP_MEMORY_POLICY = "im-group-isolated-v1"


def memory_policy_for_session_scope(session_key: str) -> str:
    """Map an adapter-owned session scope to its non-bypassable visibility policy."""

    if session_key.startswith("direct:"):
        return IM_DIRECT_MEMORY_POLICY
    if session_key.startswith(("group:", "thread:")):
        return IM_GROUP_MEMORY_POLICY
    raise ValueError("SESSION_SCOPE_INVALID")


@dataclass(frozen=True)
class VisibleMemory:
    id: UUID
    subject_id: str
    content: str
    policy_version: str


class SubjectMemoryReader:
    """Read valid Memory after applying tenant and IM conversation boundaries."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def list_visible(
        self,
        *,
        tenant_id: UUID,
        subject_id: str,
        memory_policy_version: str,
        limit: int = 20,
    ) -> list[VisibleMemory]:
        if not 1 <= limit <= 100:
            raise ValueError("MEMORY_LIMIT_INVALID")
        if memory_policy_version != IM_DIRECT_MEMORY_POLICY:
            return []
        async with self._database.tenant_transaction(tenant_id) as connection:
            return await _list_direct_memory(connection, tenant_id, subject_id, limit)


async def _list_direct_memory(
    connection: Connection, tenant_id: UUID, subject_id: str, limit: int
) -> list[VisibleMemory]:
    rows: list[Any] = await connection.fetch(
        """SELECT record.id,record.subject_id,record.content,record.policy_version
        FROM tenant.memory_record record
        WHERE record.tenant_id=$1 AND record.is_valid
          AND record.policy_version=$3
          AND (record.subject_id=$2 OR EXISTS (
            SELECT 1 FROM tenant.im_subject_association association
            WHERE association.tenant_id=record.tenant_id
              AND ((association.subject_id=$2 AND association.related_subject_id=record.subject_id)
                OR (association.related_subject_id=$2 AND association.subject_id=record.subject_id))
          ))
        ORDER BY record.created_at DESC,record.id DESC LIMIT $4""",
        tenant_id,
        subject_id,
        IM_DIRECT_MEMORY_POLICY,
        limit,
    )
    return [
        VisibleMemory(
            id=UUID(str(row["id"])),
            subject_id=str(row["subject_id"]),
            content=str(row["content"]),
            policy_version=str(row["policy_version"]),
        )
        for row in rows
    ]
