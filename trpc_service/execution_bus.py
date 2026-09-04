"""Kafka-compatible execution bus contract with partition routing and at-least-once delivery.

Production wires a Kafka (Redpanda) transport behind :class:`ExecutionBusPublisher`;
the in-memory transport exists for unit tests and local runs only. Envelopes follow
the CloudEvents 1.0 JSON format so bus payloads never serialize runtime objects.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from trpc_service.admin_api.database import Database

ENVELOPE_SPECVERSION = "1.0"
GATEWAY_SOURCE = "trpc-agent-platform://agent-gateway"
WORKER_SOURCE = "trpc-agent-platform://agent-worker"
EXECUTION_REQUESTED_EVENT = "platform.agent-execution.requested.v1"
SESSION_EVENTS_COMMITTED_EVENT = "platform.session.events.committed.v1"

_REQUIRED_ENVELOPE_FIELDS = ("id", "source", "type", "time", "partitionkey", "data")


@dataclass(frozen=True)
class ExecutionEnvelope:
    """A versioned domain event on the execution bus (CloudEvents 1.0 style)."""

    message_id: str
    source: str
    event_type: str
    partition_key: str
    time: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "specversion": ENVELOPE_SPECVERSION,
            "id": self.message_id,
            "source": self.source,
            "type": self.event_type,
            "time": self.time,
            "partitionkey": self.partition_key,
            "datacontenttype": "application/json",
            "data": self.data,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ExecutionEnvelope:
        if payload.get("specversion") != ENVELOPE_SPECVERSION:
            raise ValueError("envelope specversion must be 1.0")
        missing = [field for field in _REQUIRED_ENVELOPE_FIELDS if field not in payload]
        if missing:
            raise ValueError(f"envelope is missing required fields: {', '.join(missing)}")
        if not isinstance(payload["data"], dict):
            raise ValueError("envelope data must be a JSON object")
        return cls(
            message_id=str(payload["id"]),
            source=str(payload["source"]),
            event_type=str(payload["type"]),
            partition_key=str(payload["partitionkey"]),
            time=str(payload["time"]),
            data=dict(payload["data"]),
        )


def session_partition_key(tenant_id: str, session_id: str) -> str:
    return f"{tenant_id}:{session_id}"


def partition_for(tenant_id: str, session_id: str, partition_count: int) -> int:
    """Deterministically map one tenant Session to one bus partition."""
    if partition_count < 0:
        raise ValueError("partition_count must not be negative")
    digest = hashlib.sha256(f"{tenant_id}\x1f{session_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % max(partition_count, 1)


def partition_of(envelope: ExecutionEnvelope, partition_count: int) -> int:
    tenant_id = envelope.data.get("tenant_id")
    session_id = envelope.data.get("session_id")
    if isinstance(tenant_id, str) and isinstance(session_id, str):
        return partition_for(tenant_id, session_id, partition_count)
    digest = hashlib.sha256(envelope.partition_key.encode()).digest()
    return int.from_bytes(digest[:8], "big") % max(partition_count, 1)


class ExecutionBusPublisher(Protocol):
    async def publish(self, envelope: ExecutionEnvelope) -> None: ...


ExecutionConsumer = Callable[[ExecutionEnvelope], Awaitable[None]]


class InMemoryExecutionBus:
    """Kafka-shaped unit-test transport: ordered partitions, at-least-once redelivery.

    A failing consumer leaves its envelope at the partition head, so the next
    delivery attempt redelivers it — the same visibility contract a Kafka
    consumer group provides before the offset is committed.
    """

    def __init__(self, partition_count: int = 8) -> None:
        self._partitions: list[list[ExecutionEnvelope]] = [
            [] for _ in range(max(partition_count, 1))
        ]
        self.deliveries: dict[str, int] = {}
        self.published: list[ExecutionEnvelope] = []

    async def publish(self, envelope: ExecutionEnvelope) -> None:
        self.published.append(envelope)
        partition = self._partitions[partition_of(envelope, len(self._partitions))]
        partition.append(envelope)

    def pending_count(self) -> int:
        return sum(len(partition) for partition in self._partitions)

    async def deliver_once(self, consumer: ExecutionConsumer) -> bool:
        """Deliver the head of the first non-empty partition; redeliver on failure.

        A consumer error leaves the envelope at the partition head and returns
        False — the at-least-once contract: nothing is acknowledged before the
        consumer succeeded.
        """
        for partition in self._partitions:
            if not partition:
                continue
            envelope = partition[0]
            self.deliveries[envelope.message_id] = self.deliveries.get(envelope.message_id, 0) + 1
            try:
                await consumer(envelope)
            except Exception:
                return False
            partition.pop(0)
            return True
        return False

    async def drain(self, consumer: ExecutionConsumer) -> int:
        delivered = 0
        while await self.deliver_once(consumer):
            delivered += 1
        return delivered

    def partition(self, index: int) -> Sequence[ExecutionEnvelope]:
        return tuple(self._partitions[index])


class OutboxDispatcher:
    """Publish transactional Outbox records at-least-once, marking only after publish.

    A crash between publish and mark republishes the record; consumers dedupe on
    the message id, so at-least-once delivery never becomes duplicate execution.
    """

    def __init__(self, database: Database, bus: ExecutionBusPublisher, batch_size: int = 100):
        self._database = database
        self._bus = bus
        self._batch_size = batch_size

    async def dispatch_pending(self) -> int:
        async with self._database.transaction() as connection:
            rows = await connection.fetch(
                """SELECT id,tenant_id,message_id,source,event_type,partition_key,payload,created_at
                FROM platform.outbox_record WHERE status='PENDING'
                ORDER BY created_at,id LIMIT $1""",
                self._batch_size,
            )
        published = 0
        for row in rows:
            envelope = ExecutionEnvelope(
                message_id=str(row["message_id"]),
                source=str(row["source"]),
                event_type=str(row["event_type"]),
                partition_key=str(row["partition_key"]),
                time=row["created_at"].isoformat(),
                data=dict(row["payload"]),
            )
            await self._bus.publish(envelope)
            async with self._database.transaction() as connection:
                await connection.execute(
                    """UPDATE platform.outbox_record
                    SET status='PUBLISHED',published_at=now(),attempts=attempts+1
                    WHERE id=$1 AND status='PENDING'""",
                    row["id"],
                )
            published += 1
        return published
