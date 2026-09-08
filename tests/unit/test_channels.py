"""Unit tests for channel bindings, inbound idempotency and reply delivery."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import pytest

from trpc_service.channels.bindings import (
    ChannelBinding,
    ChannelBindingRegistry,
    SecretRefRejected,
)
from trpc_service.channels.delivery import (
    ChannelTransportOutcome,
    DeliveryState,
    FakeChannelTransport,
    ReplyDelivery,
    ReplyDeliveryService,
)
from trpc_service.channels.inbound import (
    ChannelInboundService,
    InboundError,
    MemoryInboundStore,
)

EXECUTION = "55555555-5555-5555-5555-555555555555"
RELEASE = "66666666-6666-6666-6666-666666666666"
TENANT = "11111111-1111-1111-1111-111111111111"
TENANT_OTHER = "33333333-3333-3333-3333-333333333333"
APP = "22222222-2222-2222-2222-222222222222"
APP_OTHER = "44444444-4444-4444-4444-444444444444"


def _signed(message_key: str = "msg-1", text: str = "hello", user: str = "user-9") -> str:
    from trpc_service.channels.inbound import fake_channel_signature

    return fake_channel_signature("fake-signing-material", message_key, text, user)


def _binding(
    tenant_id: str = TENANT,
    application_id: str = APP,
    external_bot_id: str = "bot-77",
    secret_ref: str = "vault://tenant/11111111/channels/fake/bot-77#signing",
) -> ChannelBinding:
    return ChannelBinding(
        tenant_id=tenant_id,
        binding_id="binding-1",
        channel_type="FAKE",
        external_bot_id=external_bot_id,
        application_id=application_id,
        environment="PRODUCTION",
        secret_ref=secret_ref,
    )


def test_binding_stores_only_a_secret_reference_and_rejects_secrets() -> None:
    binding = _binding()
    assert binding.secret_ref.startswith("vault://")
    with pytest.raises(SecretRefRejected):
        _binding(secret_ref="sk-plaintext-secret-value-1234567890")
    with pytest.raises(SecretRefRejected):
        _binding(secret_ref="password=hunter2")


def test_registry_resolves_one_binding_per_external_bot() -> None:
    registry = ChannelBindingRegistry.in_memory()
    asyncio.run(registry.register(_binding()))
    resolved = asyncio.run(
        registry.resolve(tenant_id=TENANT, channel_type="FAKE", external_bot_id="bot-77")
    )
    assert resolved is not None and resolved.application_id == APP
    # The same external bot cannot bind a second application in one tenant.
    conflict = registry.register(
        _binding(
            application_id=APP_OTHER,
            secret_ref="vault://tenant/11111111/channels/fake/bot-77#other",
        )
    )
    with pytest.raises(SecretRefRejected, match="conflict"):
        asyncio.run(conflict)
    # Another tenant may bind the same external bot id.
    other = asyncio.run(
        registry.resolve(tenant_id=TENANT_OTHER, channel_type="FAKE", external_bot_id="bot-77")
    )
    assert other is None
    assert (
        asyncio.run(
            registry.resolve(tenant_id=TENANT, channel_type="WECOM", external_bot_id="bot-77")
        )
        is None
    )


class StaticSecretResolver:
    """Fake channel signing material resolved from the reference, never stored."""

    def __init__(self, secrets: dict[str, str] | None = None) -> None:
        self._secrets = secrets or {}

    def resolve(self, secret_ref: str) -> str:
        return self._secrets.get(secret_ref, "fake-signing-material")


@dataclass
class RecordedSubmission:
    message_id: str
    payload_hash: str
    application_id: str
    session_id: str
    memory_policy_version: str


class StaticSubmitter:
    """Stand-in for AgentExecutionSubmitter with dedup by message id."""

    def __init__(self) -> None:
        self.submissions: dict[str, RecordedSubmission] = {}
        self.calls = 0

    async def submit(self, submission: Any) -> Any:
        from trpc_service.agent_gateway import AgentExecutionAccepted

        self.calls += 1
        payload_hash = hashlib.sha256(
            json.dumps(submission.messages, sort_keys=True).encode()
        ).hexdigest()
        existing = self.submissions.get(submission.message_id)
        if existing is not None:
            if existing.payload_hash != payload_hash:
                from trpc_service.agent_gateway import AgentGatewayError

                raise AgentGatewayError("MESSAGE_PAYLOAD_CONFLICT")
            return AgentExecutionAccepted(
                execution_id=UUID(EXECUTION),
                release_id=UUID(RELEASE),
                session_id=existing.session_id,
                deduplicated=True,
            )
        self.submissions[submission.message_id] = RecordedSubmission(
            message_id=submission.message_id,
            payload_hash=payload_hash,
            session_id=submission.session_id,
            application_id=str(submission.application_id),
            memory_policy_version=submission.memory_policy_version,
        )
        return AgentExecutionAccepted(
            execution_id=UUID(EXECUTION),
            release_id=UUID(RELEASE),
            session_id=submission.session_id,
        )


def _inbound_service(
    submitter: StaticSubmitter | None = None,
    store: MemoryInboundStore | None = None,
) -> ChannelInboundService:
    registry = ChannelBindingRegistry.in_memory()

    async def _register() -> None:
        await registry.register(_binding())

    asyncio.run(_register())
    return ChannelInboundService(
        registry=registry,
        secrets=StaticSecretResolver(),
        submitter=submitter or StaticSubmitter(),
        store=store or MemoryInboundStore(),
    )


def _event(
    message_key: str = "msg-1",
    text: str = "hello",
    bot_id: str = "bot-77",
    signature: str | None = None,
    external_user: str = "user-9",
) -> dict[str, str]:
    return {
        "channel_type": "FAKE",
        "external_bot_id": bot_id,
        "message_key": message_key,
        "text": text,
        "external_user_id": external_user,
        "signature": signature
        if signature is not None
        else _signed(message_key, text, external_user),
    }


def test_ingest_verifies_signature_and_resolves_one_binding() -> None:
    service = _inbound_service()
    accepted = asyncio.run(service.ingest(tenant_id=TENANT, event=_event()))
    assert accepted.deduplicated is False
    assert str(accepted.execution_id) == EXECUTION
    # An unregistered bot fails closed without any execution.
    with pytest.raises(InboundError, match="BINDING_NOT_FOUND"):
        asyncio.run(service.ingest(tenant_id=TENANT, event=_event(bot_id="bot-unknown")))


def test_ingest_bad_signature_is_rejected_before_any_state_change() -> None:
    service = _inbound_service()
    with pytest.raises(InboundError, match="SIGNATURE_INVALID"):
        asyncio.run(service.ingest(tenant_id=TENANT, event=_event(signature="wrong")))


def test_duplicate_inbound_event_reuses_the_original_execution() -> None:
    submitter = StaticSubmitter()
    service = _inbound_service(submitter=submitter)
    first = asyncio.run(service.ingest(tenant_id=TENANT, event=_event()))
    duplicate = asyncio.run(service.ingest(tenant_id=TENANT, event=_event()))
    assert duplicate.deduplicated is True
    assert duplicate.execution_id == first.execution_id
    assert submitter.calls == 1


def test_same_key_different_payload_is_isolated_not_routed() -> None:
    submitter = StaticSubmitter()
    store = MemoryInboundStore()
    service = _inbound_service(submitter=submitter, store=store)
    first = asyncio.run(service.ingest(tenant_id=TENANT, event=_event()))
    # A differently-signed payload under the same key is isolated (the
    # channel re-issued its own signature for the new text).
    tampered = _event(message_key="msg-1", text="tampered payload")
    with pytest.raises(InboundError, match="INBOUND_PAYLOAD_CONFLICT"):
        asyncio.run(service.ingest(tenant_id=TENANT, event=tampered))
    # Tampering the text without re-signing never reaches the ledger at all.
    unsigned_tamper = dict(_event(), text="tampered payload")
    with pytest.raises(InboundError, match="SIGNATURE_INVALID"):
        asyncio.run(service.ingest(tenant_id=TENANT, event=unsigned_tamper))
    # The original execution is untouched and remains the only one.
    assert submitter.calls == 1
    assert store.messages[(TENANT, "binding-1", "msg-1")].message_key == "msg-1"
    assert first.deduplicated is False


def test_inbound_subject_is_scoped_to_tenant_and_binding() -> None:
    service = _inbound_service()
    subject = service.subject_for(binding=_binding(), external_user_id="user-9")
    assert subject == "im:FAKE:binding-1:user-9"


def test_session_ids_are_opaque_and_isolated_by_scope_binding_and_tenant() -> None:
    service = _inbound_service()
    direct = {"external_user_id": "user-9", "session_key": "direct:user-9"}
    group = {"external_user_id": "user-9", "session_key": "group:room-1"}
    topic = {"external_user_id": "user-9", "session_key": "thread:room-1:topic-1"}
    other_binding = _binding(external_bot_id="bot-78")
    other_binding = other_binding.model_copy(update={"binding_id": "binding-2"})
    other_tenant = _binding(tenant_id=TENANT_OTHER)

    direct_session = service.session_for(binding=_binding(), event=direct)
    assert direct_session.startswith("session:")
    assert "user-9" not in direct_session
    assert "binding-1" not in direct_session
    assert direct_session == service.session_for(binding=_binding(), event=direct)
    assert direct_session != service.session_for(binding=_binding(), event=group)
    assert direct_session != service.session_for(binding=_binding(), event=topic)
    assert direct_session != service.session_for(binding=other_binding, event=direct)
    assert direct_session != service.session_for(binding=other_tenant, event=direct)


def test_group_session_without_stable_chat_id_is_rejected() -> None:
    service = _inbound_service()
    with pytest.raises(InboundError, match="GROUP_SESSION_ID_REQUIRED"):
        service.session_for(
            binding=_binding(),
            event={"external_user_id": "user-9", "session_key": "group:"},
        )


def test_group_submissions_use_the_no_private_memory_policy() -> None:
    submitter = StaticSubmitter()
    service = _inbound_service(submitter=submitter)
    event = _event()
    event["session_key"] = "group:room-1"
    from trpc_service.channels.inbound import fake_channel_signature

    event["signature"] = fake_channel_signature(
        "fake-signing-material", "msg-1", "hello", "user-9", "group:room-1"
    )

    asyncio.run(service.ingest(tenant_id=TENANT, event=event))

    # The submitter sees the execution contract, including the visibility gate.
    submission = next(iter(submitter.submissions.values()))
    assert submission.session_id.startswith("session:")
    assert submission.memory_policy_version == "im-group-isolated-v1"


def test_same_message_key_cannot_move_between_session_scopes() -> None:
    service = _inbound_service()
    direct = _event()
    group = _event()
    group["session_key"] = "group:room-1"
    from trpc_service.channels.inbound import fake_channel_signature

    group["signature"] = fake_channel_signature(
        "fake-signing-material", "msg-1", "hello", "user-9", "group:room-1"
    )

    asyncio.run(service.ingest(tenant_id=TENANT, event=direct))
    with pytest.raises(InboundError, match="INBOUND_PAYLOAD_CONFLICT"):
        asyncio.run(service.ingest(tenant_id=TENANT, event=group))


def test_pre_scope_direct_redelivery_remains_deduplicated_after_upgrade() -> None:
    from trpc_service.channels.inbound import InboundMessage

    store = MemoryInboundStore()
    legacy_canonical = json.dumps(
        {"text": "hello", "external_user_id": "user-9"},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    legacy_hash = hashlib.sha256(legacy_canonical.encode()).hexdigest()
    asyncio.run(
        store.insert(
            InboundMessage(
                tenant_id=TENANT,
                binding_id="binding-1",
                message_key="msg-1",
                payload_hash=legacy_hash,
                external_user_id="user-9",
                execution_id=EXECUTION,
                release_id=RELEASE,
                occurred_at="2026-01-01T00:00:00+00:00",
            )
        )
    )
    legacy_signature = (
        "fake:"
        + hmac.new(b"fake-signing-material", b"msg-1\nhello\nuser-9", hashlib.sha256).hexdigest()
    )

    accepted = asyncio.run(
        _inbound_service(store=store).ingest(
            tenant_id=TENANT, event=_event(signature=legacy_signature)
        )
    )

    assert accepted.deduplicated is True
    assert accepted.session_id.startswith("session:")


# --- reply delivery state machine ---


@dataclass
class FakeDeliveryStore:
    deliveries: dict[str, Any] = field(default_factory=dict)
    attempts: list[Any] = field(default_factory=list)
    claims: list[tuple[str, int]] = field(default_factory=list)

    async def save_delivery(self, delivery: Any) -> None:
        self.deliveries[delivery.delivery_id] = delivery

    async def get_delivery(self, tenant_id: str, delivery_id: str) -> Any | None:
        return self.deliveries.get(delivery_id)

    async def save_attempt(self, attempt: Any) -> None:
        self.attempts.append(attempt)

    async def list_dead_letters(self, tenant_id: str) -> list[Any]:
        return [
            d
            for d in self.deliveries.values()
            if d.tenant_id == tenant_id and d.status == DeliveryState.DEAD_LETTER
        ]

    async def claim_attempt(
        self, tenant_id: str, delivery_id: str, from_status: DeliveryState, attempt_no: int
    ) -> Any | None:
        delivery = self.deliveries.get(delivery_id)
        if delivery is None or delivery.status != from_status:
            return None
        self.claims.append((delivery_id, attempt_no))
        claimed = delivery.model_copy(
            update={"status": DeliveryState.IN_FLIGHT, "attempts": attempt_no}
        )
        self.deliveries[delivery_id] = claimed
        return claimed


def _outcome(
    ok: bool = False,
    rate_limited: bool = False,
    unknown: bool = False,
    error_code: str | None = None,
) -> ChannelTransportOutcome:
    return ChannelTransportOutcome(
        delivered=ok, rate_limited=rate_limited, outcome_unknown=unknown, error_code=error_code
    )


def test_delivery_uses_stable_delivery_id_with_per_attempt_ids() -> None:
    transport = FakeChannelTransport(
        [
            _outcome(rate_limited=True, error_code="CHANNEL_RATE_LIMITED"),
            _outcome(ok=True),
        ]
    )
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.DELIVERED
    assert result.delivery_id == delivery.delivery_id
    attempt_ids = [attempt.attempt_id for attempt in store.attempts]
    assert len(attempt_ids) == 2
    assert len(set(attempt_ids)) == 2
    assert all(attempt.delivery_id == delivery.delivery_id for attempt in store.attempts)
    assert [attempt.attempt_no for attempt in store.attempts] == [1, 2]


def test_rate_limited_backs_off_between_attempts() -> None:
    transport = FakeChannelTransport(
        [
            _outcome(rate_limited=True, error_code="CHANNEL_RATE_LIMITED"),
            _outcome(rate_limited=True, error_code="CHANNEL_RATE_LIMITED"),
            _outcome(ok=True),
        ]
    )
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.DELIVERED
    assert result.attempts == 3


def test_unknown_outcome_reconciles_before_retrying() -> None:
    transport = FakeChannelTransport(
        [
            _outcome(unknown=True, error_code="CHANNEL_TIMEOUT"),
        ]
    )
    # The channel ledger confirms the first attempt actually landed.
    transport.reconcile_results = {1: "delivered"}
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.DELIVERED
    # No retry attempt was made after reconciliation confirmed delivery.
    assert len(store.attempts) == 1


def test_unknown_outcome_retries_only_after_ledger_denies_delivery() -> None:
    transport = FakeChannelTransport(
        [
            _outcome(unknown=True, error_code="CHANNEL_TIMEOUT"),
            _outcome(ok=True),
        ]
    )
    transport.reconcile_results = {1: "not_delivered"}
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.DELIVERED
    assert len(store.attempts) == 2


def test_unresolvable_unknown_stays_pending_reconciliation() -> None:
    transport = FakeChannelTransport([_outcome(unknown=True, error_code="CHANNEL_TIMEOUT")])
    transport.reconcile_results = {1: "unknown"}
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.OUTCOME_UNKNOWN
    assert len(store.attempts) == 1


def test_permanent_failure_dead_letters_and_replays_with_original_id() -> None:
    transport = FakeChannelTransport([_outcome(error_code="CHANNEL_REJECTED")] * 10)
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    dead = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert dead.status is DeliveryState.DEAD_LETTER

    listed = asyncio.run(store.list_dead_letters(TENANT))
    assert [item.delivery_id for item in listed] == [delivery.delivery_id]

    transport.outcomes = [_outcome(ok=True)]
    replayed = asyncio.run(service.replay(delivery.delivery_id, tenant_id=TENANT))
    assert replayed.delivery_id == delivery.delivery_id
    assert replayed.status is DeliveryState.DELIVERED
    # The replay continues the attempt numbering on the same delivery.
    assert store.attempts[-1].attempt_no == dead.attempts + 1


def test_backoff_delay_is_exponential_and_capped() -> None:
    from trpc_service.channels.delivery import backoff_delay

    assert backoff_delay(1, 0.2) == 0.2
    assert backoff_delay(2, 0.2) == 0.4
    assert backoff_delay(3, 0.2) == 0.8
    assert backoff_delay(20, 0.2) == 60.0


def test_parked_unknown_reconciles_before_resend_on_reentry() -> None:
    transport = FakeChannelTransport([_outcome(unknown=True, error_code="CHANNEL_TIMEOUT")])
    transport.reconcile_results = {1: "unknown"}
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    parked = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert parked.status is DeliveryState.OUTCOME_UNKNOWN
    assert len(store.attempts) == 1

    # The ledger now confirms delivery: re-entry must close it without resend.
    transport.reconcile_results = {1: "delivered"}
    resumed = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert resumed.status is DeliveryState.DELIVERED
    assert len(store.attempts) == 1

    # A still-unknown parked delivery stays parked on re-entry.
    transport2 = FakeChannelTransport([_outcome(unknown=True)])
    transport2.reconcile_results = {1: "unknown"}
    store2 = FakeDeliveryStore()
    service2 = ReplyDeliveryService(store=store2, transport=transport2, backoff_seconds=0)
    d2 = asyncio.run(
        service2.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    asyncio.run(service2.run(d2.delivery_id, tenant_id=TENANT))
    still = asyncio.run(service2.run(d2.delivery_id, tenant_id=TENANT))
    assert still.status is DeliveryState.OUTCOME_UNKNOWN
    # No resend happened — reconcile-first held on both entries.
    assert transport2.sent_attempt_nos == [1]


def test_concurrent_run_claims_are_exclusive() -> None:
    store = FakeDeliveryStore()
    delivery = ReplyDelivery(
        tenant_id=TENANT,
        delivery_id="d-1",
        binding_id="binding-1",
        execution_id="exec-1",
        external_conversation_id="conv-1",
        content="answer",
        status=DeliveryState.QUEUED,
        attempts=0,
        created_at="2026-09-05T00:00:00+00:00",
    )
    asyncio.run(store.save_delivery(delivery))
    first = asyncio.run(store.claim_attempt(TENANT, "d-1", DeliveryState.QUEUED, 1))
    second = asyncio.run(store.claim_attempt(TENANT, "d-1", DeliveryState.QUEUED, 1))
    assert first is not None and first.status is DeliveryState.IN_FLIGHT
    assert second is None  # the second worker loses the claim


def test_send_crash_parks_the_delivery_as_unknown() -> None:
    class ExplodingTransport(FakeChannelTransport):
        async def send(self, delivery, attempt_no):
            raise ConnectionError("channel dropped")

    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=ExplodingTransport([]), backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    crashed = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert crashed.status is DeliveryState.OUTCOME_UNKNOWN
    assert store.attempts[-1].outcome == "OUTCOME_UNKNOWN"
    # A later run reconciles instead of resending.
    assert crashed.attempts == 1


def test_ledger_attach_heals_the_crash_window_on_redelivery() -> None:
    submitter = StaticSubmitter()
    store = MemoryInboundStore()
    service = _inbound_service(submitter=submitter, store=store)
    asyncio.run(service.ingest(tenant_id=TENANT, event=_event()))
    # Simulate the crash window: the ledger lost its execution pointer.
    key = (TENANT, "binding-1", "msg-1")
    store.messages[key] = store.messages[key].model_copy(
        update={"execution_id": None, "release_id": None}
    )
    healed = asyncio.run(service.ingest(tenant_id=TENANT, event=_event()))
    assert healed.deduplicated is True  # gateway dedup carried the reuse
    assert store.messages[key].execution_id is not None  # and the ledger healed


def test_run_returns_when_another_worker_wins_the_claim() -> None:
    transport = FakeChannelTransport([_outcome(ok=True)])

    @dataclass
    class ContendedStore(FakeDeliveryStore):
        def __init__(self) -> None:
            super().__init__()

        async def claim_attempt(
            self, tenant_id: str, delivery_id: str, from_status: DeliveryState, attempt_no: int
        ) -> Any | None:
            # The other worker's CAS landed first; this claim loses.
            return None

    store = ContendedStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.QUEUED  # untouched by the loser
    assert transport.sent_attempt_nos == []  # no double send


def test_interrupted_in_flight_delivery_resumes_on_the_next_attempt() -> None:
    transport = FakeChannelTransport([_outcome(ok=True)])
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    # A worker crashed mid-send: IN_FLIGHT persisted, no attempt row counted.
    asyncio.run(store.claim_attempt(TENANT, delivery.delivery_id, DeliveryState.QUEUED, 1))
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.DELIVERED
    assert result.attempts == 2  # the interrupted attempt plus the resume


def test_memory_delivery_store_backs_the_service_end_to_end() -> None:
    from trpc_service.channels.delivery import MemoryDeliveryStore

    transport = FakeChannelTransport(
        [_outcome(rate_limited=True, error_code="RL"), _outcome(ok=True)]
    )
    store = MemoryDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    assert result.status is DeliveryState.DELIVERED
    assert [attempt.attempt_no for attempt in store.attempts] == [1, 2]
    assert await_dead_letters(store, TENANT) == []


def await_dead_letters(store: object, tenant_id: str) -> list:
    import asyncio as _asyncio

    return _asyncio.run(store.list_dead_letters(tenant_id))  # type: ignore[attr-defined]


def test_rate_limiting_alone_never_dead_letters() -> None:
    transport = FakeChannelTransport(
        [_outcome(rate_limited=True, error_code="RL")] * 12 + [_outcome(ok=True)]
    )
    store = FakeDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-1",
            execution_id="exec-1",
            external_conversation_id="conv-1",
            content="answer",
        )
    )
    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))
    # Thirteen rate-limited sends stayed within the failure budget.
    assert result.status is DeliveryState.DELIVERED
    assert result.attempts == 13
