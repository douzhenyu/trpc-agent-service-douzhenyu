"""IM Memory visibility policy tests."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from uuid import UUID

from trpc_service.memory_access import (
    IM_DIRECT_MEMORY_POLICY,
    IM_GROUP_MEMORY_POLICY,
    SubjectMemoryReader,
    memory_policy_for_session_scope,
)


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, str]]:
        self.calls.append((query, args))
        return [
            {
                "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "subject_id": "im:FEISHU:binding-2:alice",
                "content": "verified cross-channel context",
                "policy_version": "policy:7",
            }
        ]

    async def executemany(self, query: str, args: list[tuple[object, ...]]) -> None:
        self.calls.append((query, tuple(args)))


class RecordingDatabase:
    def __init__(self) -> None:
        self.connection = RecordingConnection()
        self.transactions = 0

    @asynccontextmanager
    async def tenant_transaction(self, _tenant_id: UUID):
        self.transactions += 1
        yield self.connection


def test_session_scope_selects_direct_or_group_memory_policy() -> None:
    assert memory_policy_for_session_scope("direct:user-1") == IM_DIRECT_MEMORY_POLICY
    assert memory_policy_for_session_scope("group:room-1") == IM_GROUP_MEMORY_POLICY
    assert memory_policy_for_session_scope("thread:room-1:topic-1") == IM_GROUP_MEMORY_POLICY


def test_group_and_topic_requests_never_query_private_member_memory() -> None:
    database = RecordingDatabase()
    reader = SubjectMemoryReader(database)  # type: ignore[arg-type]

    visible = asyncio.run(
        reader.list_visible(
            tenant_id=UUID("11111111-1111-1111-1111-111111111111"),
            subject_id="im:WECOM:binding-1:alice",
            memory_policy_version=IM_GROUP_MEMORY_POLICY,
        )
    )

    assert visible == []
    assert database.transactions == 0


def test_direct_request_can_query_only_same_tenant_verified_associations() -> None:
    database = RecordingDatabase()
    reader = SubjectMemoryReader(database)  # type: ignore[arg-type]
    tenant_id = UUID("11111111-1111-1111-1111-111111111111")

    visible = asyncio.run(
        reader.list_visible(
            tenant_id=tenant_id,
            subject_id="im:WECOM:binding-1:alice",
            memory_policy_version=IM_DIRECT_MEMORY_POLICY,
        )
    )

    assert [memory.subject_id for memory in visible] == ["im:FEISHU:binding-2:alice"]
    assert database.transactions == 1
    query, args = database.connection.calls[0]
    assert "tenant.im_subject_association" in query
    assert "association.tenant_id=record.tenant_id" in query
    assert args == (tenant_id, "im:WECOM:binding-1:alice", IM_DIRECT_MEMORY_POLICY, 20)
    update_query, update_args = database.connection.calls[1]
    assert "SET last_used_at=now()" in update_query
    assert update_args == ((tenant_id, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),)


def test_group_projection_never_writes_member_private_memory() -> None:
    from trpc_service.job_worker import SessionEventRange, _insert_memory_projection

    class Connection:
        called = False

        async def fetchval(self, *_args: object) -> object:
            self.called = True
            return None

    connection = Connection()
    asyncio.run(
        _insert_memory_projection(
            connection,  # type: ignore[arg-type]
            UUID("11111111-1111-1111-1111-111111111111"),
            "session:opaque-group",
            SessionEventRange(from_version=0, to_version=1),
            {
                "subject_id": "im:WECOM:binding-1:alice",
                "memory_policy_version": IM_GROUP_MEMORY_POLICY,
            },
            [],
        )
    )

    assert connection.called is False
