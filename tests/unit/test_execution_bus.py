from __future__ import annotations

import pytest

from trpc_service.execution_bus import (
    EXECUTION_REQUESTED_EVENT,
    ExecutionEnvelope,
    InMemoryExecutionBus,
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
        data=data
        if data is not None
        else {"tenant_id": tenant_id, "session_id": session_id, "messages": []},
    )


def test_execution_envelope_round_trips_cloudevents_fields() -> None:
    envelope = _envelope()
    restored = ExecutionEnvelope.from_dict(envelope.to_dict())
    assert restored == envelope
    payload = envelope.to_dict()
    assert payload["specversion"] == "1.0"
    assert payload["id"] == "m-1"
    assert payload["datacontenttype"] == "application/json"
    assert payload["partitionkey"] == envelope.partition_key


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
