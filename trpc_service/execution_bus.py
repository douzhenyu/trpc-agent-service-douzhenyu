"""Kafka-compatible execution bus contract with partition routing and at-least-once delivery.

Production wires a Kafka (Redpanda) transport behind :class:`ExecutionBusPublisher`;
the in-memory transport exists for unit tests and local runs only. Envelopes follow
the CloudEvents 1.0 JSON format and carry the tenant, causation, correlation, trace,
schema and data-classification metadata required by ADR-0041, so bus payloads never
serialize runtime objects.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.admin_api.database import Connection, Database
from trpc_service.ids import uuid7

ENVELOPE_SPECVERSION = "1.0"
GATEWAY_SOURCE = "trpc-agent-platform://agent-gateway"
WORKER_SOURCE = "trpc-agent-platform://agent-worker"
JOB_WORKER_SOURCE = "trpc-agent-platform://job-worker"
EXECUTION_REQUESTED_EVENT = "platform.agent-execution.requested.v1"
EXECUTION_COMPLETED_EVENT = "platform.agent-execution.completed.v1"
SESSION_EVENTS_COMMITTED_EVENT = "platform.session.events.committed.v1"
MEMORY_INVALIDATED_EVENT = "platform.memory.invalidated.v1"

_LOGGER = logging.getLogger(__name__)

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
    channel_context: dict[str, str] | None = None
    trace_parent: str | None = Field(default=None, min_length=1, max_length=128)


class ExecutionCompletedData(BaseModel):
    """Worker result consumed by Channel Gateway for asynchronous IM replies."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    execution_id: str
    release_id: str
    session_id: str
    completion: dict[str, Any]
    channel_context: dict[str, str] | None = None
    trace_parent: str | None = Field(default=None, min_length=1, max_length=128)


class SessionEventsCommittedData(BaseModel):
    """Committed Session Event range consumed only by asynchronous projections."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    session_id: str
    execution_id: str
    from_version: int = Field(ge=0)
    to_version: int = Field(ge=1)
    event_kinds: list[str] = Field(min_length=1)


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


class ExecutionBusConsumer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def run_forever(
        self,
        handler: ExecutionConsumer,
        *,
        retry_delay_seconds: float = 1.0,
    ) -> None: ...


class KafkaProducerClient(Protocol):
    """Small injectable surface around ``AIOKafkaProducer``."""

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send_and_wait(
        self, topic: str, value: bytes, *, key: bytes | None = None
    ) -> object: ...


class KafkaConsumerRecord(Protocol):
    offset: int
    value: bytes | None


class KafkaConsumerClient(Protocol):
    """Small injectable surface around ``AIOKafkaConsumer``."""

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def getmany(
        self, *, timeout_ms: int, max_records: int
    ) -> Mapping[object, Sequence[KafkaConsumerRecord]]: ...

    async def commit(self, offsets: Mapping[object, object]) -> None: ...

    def seek(self, partition: object, offset: int) -> None: ...


def _new_kafka_producer(bootstrap_servers: str) -> KafkaProducerClient:
    from aiokafka import AIOKafkaProducer

    return cast(
        KafkaProducerClient,
        AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        ),
    )


def _new_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str) -> KafkaConsumerClient:
    from aiokafka import AIOKafkaConsumer

    return cast(
        KafkaConsumerClient,
        AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        ),
    )


class KafkaExecutionBus:
    """Production execution-bus publisher backed by Kafka or Redpanda.

    The CloudEvent is encoded as canonical UTF-8 JSON. The tenant/session
    partition key is sent as the Kafka record key, preserving per-Session
    ordering while allowing independent Sessions to spread across partitions.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        *,
        event_topics: Mapping[str, str] | None = None,
        producer: KafkaProducerClient | None = None,
    ) -> None:
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers must not be empty")
        if not topic:
            raise ValueError("topic must not be empty")
        self._event_topics = {
            EXECUTION_REQUESTED_EVENT: topic,
            **dict(event_topics or {}),
        }
        self._producer = producer or _new_kafka_producer(bootstrap_servers)

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, envelope: ExecutionEnvelope) -> None:
        value = json.dumps(
            envelope.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        await self._producer.send_and_wait(
            self._event_topics.get(envelope.event_type, envelope.event_type),
            value,
            key=envelope.partition_key.encode(),
        )


class KafkaExecutionConsumer:
    """Manual-commit Kafka consumer with at-least-once delivery semantics.

    Only the exact partition offset whose handler completed is committed. A
    decoding or handler failure leaves the offset uncommitted and retries it.
    When a dead-letter topic is configured, poison records are committed only
    after their original bytes and source coordinates are durably published to
    that topic. The domain processor collapses ordinary redelivery by message id.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        *,
        consumer: KafkaConsumerClient | None = None,
        dead_letter_topic: str = "",
        max_delivery_attempts: int = 5,
        dead_letter_producer: KafkaProducerClient | None = None,
    ) -> None:
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers must not be empty")
        if not topic:
            raise ValueError("topic must not be empty")
        if not group_id:
            raise ValueError("group_id must not be empty")
        if max_delivery_attempts < 1:
            raise ValueError("max_delivery_attempts must be positive")
        if dead_letter_producer is not None and not dead_letter_topic:
            raise ValueError("dead_letter_topic is required with a dead-letter producer")
        self._topic = topic
        self._consumer = consumer or _new_kafka_consumer(bootstrap_servers, topic, group_id)
        self._dead_letter_topic = dead_letter_topic
        self._max_delivery_attempts = max_delivery_attempts
        self._dead_letter_producer = dead_letter_producer or (
            _new_kafka_producer(bootstrap_servers) if dead_letter_topic else None
        )
        self._failure_attempts: dict[tuple[str, int, int], int] = {}

    async def start(self) -> None:
        if self._dead_letter_producer is not None:
            await self._dead_letter_producer.start()
        try:
            await self._consumer.start()
        except BaseException:
            if self._dead_letter_producer is not None:
                await self._dead_letter_producer.stop()
            raise

    async def stop(self) -> None:
        try:
            await self._consumer.stop()
        finally:
            if self._dead_letter_producer is not None:
                await self._dead_letter_producer.stop()

    async def process_once(self, handler: ExecutionConsumer, *, timeout_ms: int = 1000) -> bool:
        batches = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=1)
        for topic_partition, records in batches.items():
            if not records:
                continue
            record = records[0]
            try:
                envelope = self._decode(record.value)
                await handler(envelope)
            except asyncio.CancelledError:
                self._consumer.seek(topic_partition, record.offset)
                raise
            except Exception as error:
                failure_key = self._failure_key(topic_partition, record.offset)
                attempts = self._failure_attempts.get(failure_key, 0) + 1
                self._failure_attempts[failure_key] = attempts
                if (
                    self._dead_letter_producer is not None
                    and attempts >= self._max_delivery_attempts
                ):
                    try:
                        await self._publish_dead_letter(
                            topic_partition,
                            record,
                            attempts=attempts,
                            error=error,
                        )
                    except Exception:
                        self._consumer.seek(topic_partition, record.offset)
                        raise
                    await self._commit(topic_partition, record.offset)
                    self._failure_attempts.pop(failure_key, None)
                    _LOGGER.error(
                        "execution_bus_record_dead_lettered",
                        extra={
                            "topic": failure_key[0],
                            "partition": failure_key[1],
                            "offset": failure_key[2],
                            "attempts": attempts,
                            "error_type": type(error).__name__,
                        },
                    )
                    return True
                self._consumer.seek(topic_partition, record.offset)
                raise

            await self._commit(topic_partition, record.offset)
            self._failure_attempts.pop(self._failure_key(topic_partition, record.offset), None)
            return True
        return False

    async def _commit(self, topic_partition: object, offset: int) -> None:
        from aiokafka.structs import OffsetAndMetadata

        await self._consumer.commit({topic_partition: OffsetAndMetadata(offset + 1, "")})

    def _failure_key(self, topic_partition: object, offset: int) -> tuple[str, int, int]:
        topic = str(getattr(topic_partition, "topic", self._topic))
        partition = int(getattr(topic_partition, "partition", -1))
        return topic, partition, offset

    async def _publish_dead_letter(
        self,
        topic_partition: object,
        record: KafkaConsumerRecord,
        *,
        attempts: int,
        error: Exception,
    ) -> None:
        if self._dead_letter_producer is None:
            raise RuntimeError("dead-letter producer is not configured")
        topic, partition, offset = self._failure_key(topic_partition, record.offset)
        payload = json.dumps(
            {
                "schema_version": 1,
                "source_topic": topic,
                "source_partition": partition,
                "source_offset": offset,
                "attempts": attempts,
                "error_type": type(error).__name__,
                "payload_base64": base64.b64encode(record.value or b"").decode("ascii"),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        await self._dead_letter_producer.send_and_wait(
            self._dead_letter_topic,
            payload,
            key=f"{topic}:{partition}:{offset}".encode(),
        )

    async def run_forever(
        self,
        handler: ExecutionConsumer,
        *,
        retry_delay_seconds: float = 1.0,
    ) -> None:
        while True:
            try:
                await self.process_once(handler)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("execution_bus_consume_failed")
                await asyncio.sleep(retry_delay_seconds)

    @staticmethod
    def _decode(value: bytes | None) -> ExecutionEnvelope:
        if value is None:
            raise ValueError("execution bus record value must not be null")
        payload = json.loads(value)
        if not isinstance(payload, dict):
            raise ValueError("execution bus record must be a JSON object")
        return ExecutionEnvelope.from_dict(payload)


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
                    trace_parent=(
                        str(row["payload"]["trace_parent"])
                        if isinstance(row["payload"], dict)
                        and isinstance(row["payload"].get("trace_parent"), str)
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
