from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass

import pytest
from aiokafka.structs import OffsetAndMetadata

from trpc_service.execution_bus import (
    EXECUTION_REQUESTED_EVENT,
    ExecutionEnvelope,
    InMemoryExecutionBus,
    KafkaExecutionBus,
    KafkaExecutionConsumer,
    partition_for,
    session_partition_key,
)


def _envelope(
    message_id: str = "m-1",
    tenant_id: str = "0197c5a5-0000-7000-8000-000000000001",
    session_id: str = "session-1",
    data: dict[str, object] | None = None,
) -> ExecutionEnvelope:
    return ExecutionEnvelope(
        message_id=message_id,
        source="trpc-agent-platform://agent-gateway",
        event_type=EXECUTION_REQUESTED_EVENT,
        partition_key=session_partition_key(tenant_id, session_id),
        time="2026-09-04T09:00:00+00:00",
        tenant_id=tenant_id,
        data_schema=f"{EXECUTION_REQUESTED_EVENT}.schema.json",
        data=data
        if data is not None
        else {"tenant_id": tenant_id, "session_id": session_id, "messages": []},
        correlation_id="0197c5a5-0000-7000-8000-0000000000ff",
        data_classification="CONFIDENTIAL",
    )


def test_execution_envelope_round_trips_cloudevents_fields() -> None:
    envelope = _envelope()
    restored = ExecutionEnvelope.from_dict(envelope.to_dict())
    assert restored == envelope
    payload = envelope.to_dict()
    assert payload["specversion"] == "1.0"
    assert payload["id"] == "m-1"
    assert payload["tenantid"] == envelope.tenant_id
    assert payload["dataschema"] == envelope.data_schema
    assert payload["datacontenttype"] == "application/json"
    assert payload["partitionkey"] == envelope.partition_key
    assert payload["correlationid"] == envelope.correlation_id
    assert payload["dataclassification"] == "CONFIDENTIAL"


def test_execution_envelope_omits_and_restores_optional_fields() -> None:
    envelope = _envelope()
    minimal = ExecutionEnvelope(
        message_id=envelope.message_id,
        source=envelope.source,
        event_type=envelope.event_type,
        partition_key=envelope.partition_key,
        time=envelope.time,
        tenant_id=envelope.tenant_id,
        data_schema=envelope.data_schema,
        data=envelope.data,
    )
    encoded = minimal.to_dict()
    assert "causationid" not in encoded
    assert "traceparent" not in encoded
    assert ExecutionEnvelope.from_dict(encoded) == minimal


def test_execution_envelope_rejects_foreign_or_incomplete_payloads() -> None:
    envelope = _envelope()
    payload = envelope.to_dict()
    with pytest.raises(ValueError, match="specversion"):
        ExecutionEnvelope.from_dict({**payload, "specversion": "0.3"})
    with pytest.raises(ValueError, match="missing"):
        ExecutionEnvelope.from_dict({key: value for key, value in payload.items() if key != "id"})
    with pytest.raises(ValueError, match="data"):
        ExecutionEnvelope.from_dict({**payload, "data": "not-an-object"})


def test_partition_for_is_deterministic_and_stable_per_tenant_session() -> None:
    tenant_id = "0197c5a5-0000-7000-8000-000000000001"
    assert partition_for(tenant_id, "session-1", 8) == partition_for(tenant_id, "session-1", 8)
    spread = {partition_for(tenant_id, f"session-{index}", 8) for index in range(64)}
    assert len(spread) > 1
    for bucket in spread:
        assert 0 <= bucket < 8
    assert partition_for(tenant_id, "session-1", 0) == 0
    assert partition_for(tenant_id, "session-1", 1) == 0
    with pytest.raises(ValueError):
        partition_for(tenant_id, "session-1", -1)


async def test_in_memory_bus_routes_one_session_to_one_ordered_partition() -> None:
    bus = InMemoryExecutionBus(partition_count=4)
    tenant_id = "0197c5a5-0000-7000-8000-000000000001"
    envelopes = [_envelope(message_id=f"m-{index}", tenant_id=tenant_id) for index in range(5)]
    other = _envelope(
        message_id="other",
        tenant_id=tenant_id,
        session_id="session-2",
        data={"tenant_id": tenant_id, "session_id": "session-2"},
    )
    for envelope in [*envelopes, other]:
        await bus.publish(envelope)
    assert bus.pending_count() == 6
    delivered: list[str] = []

    async def handler(envelope: ExecutionEnvelope) -> None:
        delivered.append(envelope.message_id)

    await bus.drain(handler)
    assert delivered[:5] == [envelope.message_id for envelope in envelopes]


async def test_in_memory_bus_redelivers_until_the_handler_succeeds() -> None:
    bus = InMemoryExecutionBus(partition_count=2)
    envelope = _envelope()
    await bus.publish(envelope)
    attempts = 0

    async def flaky_handler(incoming: ExecutionEnvelope) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("consumer crashed mid-processing")

    assert await bus.deliver_once(flaky_handler) is False
    assert bus.pending_count() == 1
    assert await bus.deliver_once(flaky_handler) is False
    assert bus.pending_count() == 1
    assert await bus.deliver_once(flaky_handler) is True
    assert bus.pending_count() == 0
    assert bus.deliveries[envelope.message_id] == 3
    assert await bus.deliver_once(flaky_handler) is False


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.sent: list[tuple[str, bytes, bytes | None]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes | None = None) -> object:
        self.sent.append((topic, value, key))
        return object()


class FailingKafkaProducer(FakeKafkaProducer):
    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes | None = None) -> object:
        del topic, value, key
        raise RuntimeError("dead-letter broker unavailable")


@dataclass
class FakeKafkaRecord:
    offset: int
    value: bytes | None


class FakeKafkaConsumer:
    def __init__(self, records: list[FakeKafkaRecord]) -> None:
        self.records = records
        self.started = False
        self.stopped = False
        self.commits: list[dict[object, object]] = []
        self.seeks: list[tuple[object, int]] = []
        self.partition = object()

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(
        self, *, timeout_ms: int, max_records: int
    ) -> dict[object, list[FakeKafkaRecord]]:
        del timeout_ms
        if not self.records:
            return {}
        return {self.partition: self.records[:max_records]}

    async def commit(self, offsets: Mapping[object, object]) -> None:
        self.commits.append(dict(offsets))
        self.records.pop(0)

    def seek(self, partition: object, offset: int) -> None:
        self.seeks.append((partition, offset))


async def test_kafka_bus_publishes_canonical_cloudevent_with_session_key() -> None:
    producer = FakeKafkaProducer()
    bus = KafkaExecutionBus("redpanda:9092", "execution-requested", producer=producer)

    await bus.start()
    await bus.publish(_envelope())
    await bus.stop()

    assert producer.started is True
    assert producer.stopped is True
    assert len(producer.sent) == 1
    topic, value, key = producer.sent[0]
    assert topic == "execution-requested"
    assert key == _envelope().partition_key.encode()
    assert ExecutionEnvelope.from_dict(json.loads(value)) == _envelope()


async def test_kafka_bus_routes_event_types_to_dedicated_topics() -> None:
    producer = FakeKafkaProducer()
    bus = KafkaExecutionBus(
        "redpanda:9092",
        "execution-requested",
        event_topics={"platform.agent-execution.completed.v1": "execution-completed"},
        producer=producer,
    )
    completed = _envelope()
    completed = ExecutionEnvelope(
        **{
            **completed.__dict__,
            "event_type": "platform.agent-execution.completed.v1",
        }
    )

    await bus.publish(completed)

    assert producer.sent[0][0] == "execution-completed"


async def test_kafka_consumer_commits_only_after_handler_succeeds() -> None:
    client = FakeKafkaConsumer(
        [FakeKafkaRecord(offset=41, value=json.dumps(_envelope().to_dict()).encode())]
    )
    consumer = KafkaExecutionConsumer(
        "redpanda:9092", "execution-requested", "agent-worker", consumer=client
    )
    handled: list[str] = []

    async def handler(envelope: ExecutionEnvelope) -> None:
        handled.append(envelope.message_id)

    await consumer.start()
    assert await consumer.process_once(handler) is True
    await consumer.stop()

    assert handled == ["m-1"]
    assert client.started is True
    assert client.stopped is True
    assert len(client.commits) == 1
    committed_offset = next(iter(client.commits[0].values()))
    assert committed_offset == OffsetAndMetadata(42, "")


async def test_kafka_consumer_leaves_failed_or_invalid_records_uncommitted() -> None:
    valid_client = FakeKafkaConsumer(
        [FakeKafkaRecord(offset=7, value=json.dumps(_envelope().to_dict()).encode())]
    )
    consumer = KafkaExecutionConsumer(
        "redpanda:9092", "execution-requested", "agent-worker", consumer=valid_client
    )

    async def fail(_envelope: ExecutionEnvelope) -> None:
        raise RuntimeError("worker failed")

    with pytest.raises(RuntimeError, match="worker failed"):
        await consumer.process_once(fail)
    assert valid_client.commits == []
    assert valid_client.seeks == [(valid_client.partition, 7)]

    invalid_client = FakeKafkaConsumer([FakeKafkaRecord(offset=8, value=b"[]")])
    invalid = KafkaExecutionConsumer(
        "redpanda:9092", "execution-requested", "agent-worker", consumer=invalid_client
    )
    with pytest.raises(ValueError, match="JSON object"):
        await invalid.process_once(fail)
    assert invalid_client.commits == []
    assert invalid_client.seeks == [(invalid_client.partition, 8)]


async def test_kafka_consumer_dead_letters_poison_record_before_committing_offset() -> None:
    raw_value = json.dumps(_envelope().to_dict()).encode()
    client = FakeKafkaConsumer([FakeKafkaRecord(offset=9, value=raw_value)])
    dead_letter = FakeKafkaProducer()
    consumer = KafkaExecutionConsumer(
        "redpanda:9092",
        "execution-requested",
        "agent-worker",
        consumer=client,
        dead_letter_topic="execution-requested.dlq",
        max_delivery_attempts=2,
        dead_letter_producer=dead_letter,
    )

    async def fail(_envelope: ExecutionEnvelope) -> None:
        raise RuntimeError("poison execution")

    await consumer.start()
    with pytest.raises(RuntimeError, match="poison execution"):
        await consumer.process_once(fail)
    assert await consumer.process_once(fail) is True
    await consumer.stop()

    assert dead_letter.started is True
    assert dead_letter.stopped is True
    assert len(client.commits) == 1
    assert len(dead_letter.sent) == 1
    topic, value, key = dead_letter.sent[0]
    assert topic == "execution-requested.dlq"
    assert key == b"execution-requested:-1:9"
    payload = json.loads(value)
    assert payload == {
        "attempts": 2,
        "error_type": "RuntimeError",
        "payload_base64": base64.b64encode(raw_value).decode("ascii"),
        "schema_version": 1,
        "source_offset": 9,
        "source_partition": -1,
        "source_topic": "execution-requested",
    }


async def test_kafka_consumer_never_commits_when_dead_letter_publish_fails() -> None:
    client = FakeKafkaConsumer([FakeKafkaRecord(offset=10, value=b"[]")])
    consumer = KafkaExecutionConsumer(
        "redpanda:9092",
        "execution-requested",
        "agent-worker",
        consumer=client,
        dead_letter_topic="execution-requested.dlq",
        max_delivery_attempts=1,
        dead_letter_producer=FailingKafkaProducer(),
    )

    async def handler(_envelope: ExecutionEnvelope) -> None:
        raise AssertionError("invalid JSON must not reach the handler")

    with pytest.raises(RuntimeError, match="dead-letter broker unavailable"):
        await consumer.process_once(handler)

    assert client.commits == []
    assert client.seeks == [(client.partition, 10)]


def test_kafka_consumer_rejects_invalid_dead_letter_configuration() -> None:
    with pytest.raises(ValueError, match="positive"):
        KafkaExecutionConsumer(
            "redpanda:9092",
            "execution-requested",
            "agent-worker",
            max_delivery_attempts=0,
        )
    with pytest.raises(ValueError, match="dead_letter_topic"):
        KafkaExecutionConsumer(
            "redpanda:9092",
            "execution-requested",
            "agent-worker",
            dead_letter_producer=FakeKafkaProducer(),
        )
