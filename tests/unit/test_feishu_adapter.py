"""Protocol-replay tests for the Feishu Channel Adapter."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import pytest

from trpc_service.agent_gateway import AgentExecutionAccepted
from trpc_service.channels.bindings import ChannelBinding, ChannelBindingRegistry
from trpc_service.channels.delivery import (
    DeliveryState,
    MemoryDeliveryStore,
    ReplyDelivery,
    ReplyDeliveryService,
)
from trpc_service.channels.feishu import (
    FeishuCardResponse,
    FeishuCardTransport,
    FeishuChannelAdapter,
    LongConnectionLeaseStore,
)
from trpc_service.channels.inbound import ChannelInboundService, MemoryInboundStore

TENANT = "11111111-1111-1111-1111-111111111111"
APPLICATION = "22222222-2222-2222-2222-222222222222"
EXECUTION = "33333333-3333-3333-3333-333333333333"
RELEASE = "44444444-4444-4444-4444-444444444444"


class StaticSecrets:
    def resolve(self, secret_ref: str) -> str:
        assert secret_ref.endswith("#verification-token")
        return "verification-token"


@dataclass
class RecordingSubmitter:
    submissions: list[Any] = field(default_factory=list)

    async def submit(self, submission: Any) -> AgentExecutionAccepted:
        self.submissions.append(submission)
        return AgentExecutionAccepted(
            execution_id=UUID(EXECUTION),
            release_id=UUID(RELEASE),
            session_id=submission.session_id,
            deduplicated=False,
        )


@dataclass
class ScriptedCardClient:
    responses: list[FeishuCardResponse]
    requests: list[dict[str, object]] = field(default_factory=list)
    reconciliation: str = "unknown"

    async def upsert_card(
        self, *, conversation_id: str, card: dict[str, object], idempotency_key: str
    ) -> FeishuCardResponse:
        self.requests.append(
            {
                "conversation_id": conversation_id,
                "card": card,
                "idempotency_key": idempotency_key,
            }
        )
        return self.responses.pop(0)

    def reconcile_card(self, idempotency_key: str) -> str:
        assert idempotency_key
        return self.reconciliation


def _webhook_body(*, chat_type: str = "p2p", root_id: str | None = None) -> bytes:
    message: dict[str, str] = {
        "message_id": "om_message_1",
        "chat_id": "oc_chat_1",
        "chat_type": chat_type,
        "message_type": "text",
        "content": json.dumps({"text": "hello Feishu"}),
    }
    if root_id is not None:
        message["root_id"] = root_id
    return json.dumps(
        {
            "header": {
                "event_id": "evt_1",
                "event_type": "im.message.receive_v1",
                "app_id": "cli_feishu_bot",
            },
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_1"}},
                "message": message,
            },
        },
        separators=(",", ":"),
    ).encode()


def _headers(body: bytes) -> dict[str, str]:
    timestamp, nonce = "1710000000", "nonce-1"
    signature = hmac.new(
        b"verification-token", timestamp.encode() + nonce.encode() + body, hashlib.sha256
    ).hexdigest()
    return {
        "X-Lark-Request-Timestamp": timestamp,
        "X-Lark-Request-Nonce": nonce,
        "X-Lark-Signature": signature,
    }


def _adapter(
    *, lease_store: LongConnectionLeaseStore | None = None, owner_id: str = "gateway-a"
) -> tuple[FeishuChannelAdapter, RecordingSubmitter]:
    registry = ChannelBindingRegistry.in_memory()
    asyncio.run(
        registry.register(
            ChannelBinding(
                tenant_id=TENANT,
                binding_id="binding-feishu-1",
                channel_type="FEISHU",
                external_bot_id="cli_feishu_bot",
                application_id=APPLICATION,
                environment="PRODUCTION",
                secret_ref=(
                    "vault://tenant/11111111/channels/feishu/cli_feishu_bot#verification-token"
                ),
            )
        )
    )
    submitter = RecordingSubmitter()
    inbound = ChannelInboundService(
        registry=registry,
        secrets=StaticSecrets(),
        submitter=submitter,
        store=MemoryInboundStore(),
    )
    return (
        FeishuChannelAdapter(
            tenant_id=TENANT,
            registry=registry,
            secrets=StaticSecrets(),
            inbound=inbound,
            lease_store=lease_store,
            owner_id=owner_id,
        ),
        submitter,
    )


def test_signed_webhook_normalizes_a_direct_message_and_submits_once() -> None:
    adapter, submitter = _adapter()
    body = _webhook_body()

    accepted = asyncio.run(adapter.receive_webhook(_headers(body), body))

    assert accepted.execution_id == UUID(EXECUTION)
    assert accepted.release_id == UUID(RELEASE)
    assert accepted.session_id == "channel:binding-feishu-1:direct:ou_user_1"
    assert len(submitter.submissions) == 1
    submission = submitter.submissions[0]
    assert submission.messages == [{"role": "user", "content": "hello Feishu"}]


def test_replayed_webhook_reuses_the_original_direct_session() -> None:
    adapter, submitter = _adapter()
    body = _webhook_body()

    first = asyncio.run(adapter.receive_webhook(_headers(body), body))
    replay = asyncio.run(adapter.receive_webhook(_headers(body), body))

    assert replay.deduplicated is True
    assert replay.execution_id == first.execution_id
    assert replay.session_id == "channel:binding-feishu-1:direct:ou_user_1"
    assert len(submitter.submissions) == 1


def test_tampered_webhook_is_rejected_before_it_reaches_the_inbound_ledger() -> None:
    from trpc_service.channels.feishu import FeishuAdapterError

    adapter, submitter = _adapter()
    body = _webhook_body()
    headers = _headers(body)
    headers["X-Lark-Signature"] = "tampered"

    with pytest.raises(FeishuAdapterError, match="FEISHU_SIGNATURE_INVALID"):
        asyncio.run(adapter.receive_webhook(headers, body))

    assert submitter.submissions == []


def test_signed_webhook_isolates_group_and_topic_sessions() -> None:
    group_adapter, _ = _adapter()
    group_body = _webhook_body(chat_type="group")
    group = asyncio.run(group_adapter.receive_webhook(_headers(group_body), group_body))

    topic_adapter, _ = _adapter()
    topic_body = _webhook_body(chat_type="group", root_id="om_root_1")
    topic = asyncio.run(topic_adapter.receive_webhook(_headers(topic_body), topic_body))

    assert group.session_id == "channel:binding-feishu-1:group:oc_chat_1"
    assert topic.session_id == "channel:binding-feishu-1:thread:oc_chat_1:om_root_1"


def test_long_connection_lease_prevents_another_gateway_from_consuming() -> None:
    from trpc_service.channels.feishu import (
        FeishuAdapterError,
        MemoryLongConnectionLeaseStore,
    )

    leases = MemoryLongConnectionLeaseStore()
    first, first_submitter = _adapter(lease_store=leases, owner_id="gateway-a")
    second, second_submitter = _adapter(lease_store=leases, owner_id="gateway-b")

    first_lease = asyncio.run(first.acquire_long_connection("cli_feishu_bot"))
    with pytest.raises(FeishuAdapterError, match="LEASE_HELD"):
        asyncio.run(second.acquire_long_connection("cli_feishu_bot"))

    accepted = asyncio.run(first.receive_long_connection_event(json.loads(_webhook_body())))
    assert accepted.execution_id == UUID(EXECUTION)
    assert first_lease.fencing_token == 1
    assert len(first_submitter.submissions) == 1
    assert second_submitter.submissions == []


def test_card_transport_updates_one_card_with_a_stable_delivery_key_and_throttles() -> None:
    clock = [0.0]
    client = ScriptedCardClient(
        [FeishuCardResponse(delivered=True), FeishuCardResponse(delivered=True)]
    )
    transport = FeishuCardTransport(
        client=client, min_update_interval_seconds=1.0, clock=lambda: clock[0]
    )
    first = ReplyDelivery(
        tenant_id=TENANT,
        delivery_id="delivery-1",
        binding_id="binding-feishu-1",
        execution_id=EXECUTION,
        external_conversation_id="oc_chat_1",
        content="处理中",
        created_at="2026-01-01T00:00:00Z",
    )

    assert asyncio.run(transport.send(first, 1)).delivered is True
    assert asyncio.run(
        transport.send(first.model_copy(update={"content": "最终答案"}), 2)
    ).rate_limited
    clock[0] = 1.0
    assert asyncio.run(
        transport.send(first.model_copy(update={"content": "最终答案"}), 3)
    ).delivered
    assert [request["idempotency_key"] for request in client.requests] == [
        "delivery-1",
        "delivery-1",
    ]
    second_card = client.requests[1]["card"]
    assert second_card == {
        "config": {"wide_screen_mode": True},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "最终答案"}}],
    }


def test_timed_out_card_delivery_is_reconciled_without_a_blind_resend() -> None:
    client = ScriptedCardClient(
        [FeishuCardResponse(outcome_unknown=True, error_code="TIMEOUT")], reconciliation="delivered"
    )
    transport = FeishuCardTransport(client=client)
    store = MemoryDeliveryStore()
    service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
    delivery = asyncio.run(
        service.enqueue(
            tenant_id=TENANT,
            binding_id="binding-feishu-1",
            execution_id=EXECUTION,
            external_conversation_id="oc_chat_1",
            content="最终答案",
        )
    )

    result = asyncio.run(service.run(delivery.delivery_id, tenant_id=TENANT))

    assert result.status is DeliveryState.DELIVERED
    assert len(client.requests) == 1
