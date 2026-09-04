"""Kafka-compatible execution bus contract with partition routing and at-least-once delivery.

Production wires a Kafka (Redpanda) transport behind :class:`ExecutionBusPublisher`;
the in-memory transport exists for unit tests and local runs only. Envelopes follow
the CloudEvents 1.0 JSON format and carry the tenant, causation, correlation, trace,
schema and data-classification metadata required by ADR-0041, so bus payloads never
serialize runtime objects.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.admin_api.database import Connection, Database
from trpc_service.ids import uuid7

ENVELOPE_SPECVERSION = "1.0"
GATEWAY_SOURCE = "trpc-agent-platform://agent-gateway"
WORKER_SOURCE = "trpc-agent-platform://agent-worker"
EXECUTION_REQUESTED_EVENT = "platform.agent-execution.requested.v1"
SESSION_EVENTS_COMMITTED_EVENT = "platform.session.events.committed.v1"

_REQUIRED_ENVELOPE_FIELDS = (
    "id",
    "source",
    "type",
    "time",
    "partitionkey",
    "tenantid",
    "dataschema",
    "data",
)


class ExecutionRequestedData(BaseModel):
    """Typed payload contract of `platform.agent-execution.requested.v1`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    application_id: str
    execution_id: str
    release_id: str
    environment: str
    session_id: str
    messages: list[dict[str, str]] = Field(min_length=1, max_length=200)


@dataclass(frozen=True)
class ExecutionEnvelope:
    """A versioned domain event on the execution bus (CloudEvents 1.0 style)."""

    message_id: str
    source: str
    event_type: str
    partition_key: str
    time: str
    tenant_id: str
    data_schema: str
    data: dict[str, Any]
    causation_id: str | None = None
    correlation_id: str | None = None
    data_classification: str | None = None
    trace_parent: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "specversion": ENVELOPE_SPECVERSION,
            "id": self.message_id,
            "source": self.source,
            "type": self.event_type,
            "time": self.time,
            "partitionkey": self.partition_key,
            "tenantid": self.tenant_id,
            "dataschema": self.data_schema,
            "datacontenttype": "application/json",
            "data": self.data,
        }
        if self.causation_id is not None:
            payload["causationid"] = self.causation_id
        if self.correlation_id is not None:
            payload["correlationid"] = self.correlation_id
        if self.data_classification is not None:
            payload["dataclassification"] = self.data_classification
        if self.trace_parent is not None:
            payload["traceparent"] = self.trace_parent
        return payload

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
            tenant_id=str(payload["tenantid"]),
            data_schema=str(payload["dataschema"]),
            data=dict(payload["data"]),
            causation_id=_optional_str(payload, "causationid"),
            correlation_id=_optional_str(payload, "correlationid"),
            data_classification=_optional_str(payload, "dataclassification"),
            trace_parent=_optional_str(payload, "traceparent"),
        )


def _optional_str(payload: Mapping[str, Any], field: str) -> str | None:
    value = payload.get(field)
    return str(value) if value is not None else None


def session_partition_key(tenant_id: str, session_id: str) -> str:
    return f"{tenant_id}:{session_id}"


def _stable_bucket(key: str, partition_count: int) -> int:
    if partition_count < 0:
        raise ValueError("partition_count must not be negative")
    digest = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(digest[:8], "big") % max(partition_count, 1)


def partition_for(tenant_id: str, session_id: str, partition_count: int) -> int:
    """Deterministically map one tenant Session to one bus partition."""
    return _stable_bucket(f"{tenant_id}\x1f{session_id}", partition_count)


def partition_of(envelope: ExecutionEnvelope, partition_count: int) -> int:
    tenant_id = envelope.data.get("tenant_id")
    session_id = envelope.data.get("session_id")
    if isinstance(tenant_id, str) and isinstance(session_id, str):
        return partition_for(tenant_id, session_id, partition_count)
    return _stable_bucket(envelope.partition_key, partition_count)


class ExecutionBusPublisher(Protocol):
    async def publish(self, envelope: ExecutionEnvelope) -> None: ...


ExecutionConsumer = Callable[[ExecutionEnvelope], Awaitable[None]]


async def insert_outbox_record(
    connection: Connection,
    *,
    tenant_id: str,
    message_id: str,
    source: str,
    event_type: str,
    partition_key: str,
    payload_json: str,
    causation_id: str | None = None,
    correlation_id: str | None = None,
    data_classification: str | None = None,
) -> None:
    """Append one Outbox record; must run inside the caller's business transaction."""

    await connection.execute(
        """INSERT INTO platform.outbox_record
        (tenant_id,id,message_id,source,event_type,partition_key,causation_id,correlation_id,
        data_classification,payload)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS jsonb))""",
        UUID(tenant_id),
        uuid7(),
        message_id,
        source,
        event_type,
        partition_key,
        causation_id,
        correlation_id,
        data_classification,
        payload_json,
    )


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
    """Publish transactional Outbox records at-least-once, marking only in the same tx.

    Rows are selected `FOR UPDATE SKIP LOCKED` and published while the locking
    transaction is open, so concurrent dispatchers never double-publish one
    record; a crash rolls the transaction back and the record is redelivered —
    consumers dedupe on the message id.
    """

    def __init__(self, database: Database, bus: ExecutionBusPublisher, batch_size: int = 100):
        self._database = database
        self._bus = bus
        self._batch_size = batch_size

    async def dispatch_pending(self) -> int:
        async with self._database.transaction() as connection:
            rows = await connection.fetch(
                """SELECT id,tenant_id,message_id,source,event_type,partition_key,causation_id,
                correlation_id,data_classification,payload,created_at
                FROM platform.outbox_record WHERE status='PENDING'
                ORDER BY created_at,id LIMIT $1 FOR UPDATE SKIP LOCKED""",
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
                    tenant_id=str(row["tenant_id"]),
                    data_schema=f"{row['event_type']}.schema.json",
                    data=dict(row["payload"]),
                    causation_id=(
                        str(row["causation_id"]) if row["causation_id"] is not None else None
                    ),
                    correlation_id=(
                        str(row["correlation_id"]) if row["correlation_id"] is not None else None
                    ),
                    data_classification=(
                        str(row["data_classification"])
                        if row["data_classification"] is not None
                        else None
                    ),
                )
                await self._bus.publish(envelope)
                await connection.execute(
                    """UPDATE platform.outbox_record
                    SET status='PUBLISHED',published_at=now(),attempts=attempts+1
                    WHERE id=$1 AND status='PENDING'""",
                    row["id"],
                )
                published += 1
            return published
