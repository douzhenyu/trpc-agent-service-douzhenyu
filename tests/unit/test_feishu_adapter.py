"""Protocol-replay tests for the Feishu Channel Adapter."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient, MockTransport, Response

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
    FeishuLongConnectionSupervisor,
    HttpFeishuCardClient,
    LarkChannelLongConnectionSource,
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
    responses: list[FeishuCardResponse | TimeoutError]
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
        response = self.responses.pop(0)
        if isinstance(response, TimeoutError):
            raise response
        return response

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
    assert accepted.session_id == "session:2fc01fae-08d1-50c6-9594-930d7b2c902f"
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
    assert replay.session_id == "session:2fc01fae-08d1-50c6-9594-930d7b2c902f"
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

    assert group.session_id == "session:8b886b5a-22db-5be6-a483-f780e360a660"
    assert topic.session_id == "session:e79ee687-ba2b-565f-a0b2-cd68524c538b"


def test_group_and_topic_runner_sessions_are_shared_without_raw_chat_ids() -> None:
    from trpc_service.channel_gateway import _runner_session_user_id

    group_owner = _runner_session_user_id(
        "group:oc_chat_1", "session:8b886b5a-22db-5be6-a483-f780e360a660"
    )
    topic_owner = _runner_session_user_id(
        "thread:oc_chat_1:om_root_1", "session:e79ee687-ba2b-565f-a0b2-cd68524c538b"
    )

    assert group_owner == "conversation:session:8b886b5a-22db-5be6-a483-f780e360a660"
    assert topic_owner == "conversation:session:e79ee687-ba2b-565f-a0b2-cd68524c538b"
    assert "oc_chat_1" not in group_owner


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


def test_long_connection_supervisor_owns_and_releases_the_sdk_event_stream() -> None:
    from trpc_service.channels.feishu import MemoryLongConnectionLeaseStore

    class Source:
        def __init__(self) -> None:
            self.apps: list[str] = []
            self.closed = False

        async def consume(self, app_id: str, on_event: Any) -> None:
            self.apps.append(app_id)
            await on_event(json.loads(_webhook_body()))
            await on_event(json.loads(_webhook_body()))

        async def close(self) -> None:
            self.closed = True

    leases = MemoryLongConnectionLeaseStore()
    adapter, submitter = _adapter(lease_store=leases)
    source = Source()
    accepted: list[AgentExecutionAccepted] = []

    async def record(result: AgentExecutionAccepted, payload: Mapping[str, object]) -> None:
        assert "event" in payload
        accepted.append(result)

    asyncio.run(
        FeishuLongConnectionSupervisor(
            adapter=adapter,
            source=source,
            app_id="cli_feishu_bot",
            on_accepted=record,
            renew_interval_seconds=3600,
        ).run()
    )

    assert source.apps == ["cli_feishu_bot"]
    assert not source.closed
    assert len(accepted) == len(submitter.submissions) == 1
    other, _ = _adapter(lease_store=leases, owner_id="gateway-b")
    assert asyncio.run(other.acquire_long_connection("cli_feishu_bot")).fencing_token == 2


def test_gateway_supervisor_retries_a_transient_provider_connection_failure() -> None:
    from trpc_service.channel_gateway import _run_supervisor
    from trpc_service.channels.feishu import MemoryLongConnectionLeaseStore

    class Source:
        def __init__(self) -> None:
            self.attempts = 0
            self.retried = asyncio.Event()
            self.stop = asyncio.Event()

        async def consume(self, app_id: str, on_event: Any) -> None:
            del app_id, on_event
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("temporary Feishu handshake failure")
            self.retried.set()
            await self.stop.wait()

        async def close(self) -> None:
            self.stop.set()

    adapter, _ = _adapter(lease_store=MemoryLongConnectionLeaseStore())

    async def exercise() -> None:
        source = Source()

        async def accepted(result: AgentExecutionAccepted, payload: Mapping[str, object]) -> None:
            del result, payload

        task = asyncio.create_task(
            _run_supervisor(
                FeishuLongConnectionSupervisor(
                    adapter=adapter,
                    source=source,
                    app_id="cli_feishu_bot",
                    on_accepted=accepted,
                    renew_interval_seconds=3600,
                ),
                retry_delay_seconds=0,
            )
        )
        await asyncio.wait_for(source.retried.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert source.attempts == 2

    asyncio.run(exercise())


def test_official_lark_source_bridges_raw_events_to_the_gateway_loop_and_closes() -> None:
    class Channel:
        def __init__(self) -> None:
            self.subscription: tuple[str, Any] | None = None
            self.stop = asyncio.Event()
            self.disconnected = False

        def on_raw_event(self, event_type: str, handler: Any) -> None:
            self.subscription = (event_type, handler)

        async def connect(self) -> None:
            await self.stop.wait()

        async def disconnect(self) -> None:
            self.disconnected = True
            self.stop.set()

    channels: list[tuple[str, str, Channel]] = []

    def factory(app_id: str, app_secret: str) -> Channel:
        channel = Channel()
        channels.append((app_id, app_secret, channel))
        return channel

    async def exercise() -> None:
        secrets_resolved = 0

        async def app_secret() -> str:
            nonlocal secrets_resolved
            secrets_resolved += 1
            return "vault-app-secret"

        received: list[dict[str, object]] = []

        async def on_event(payload: Mapping[str, object]) -> None:
            received.append(dict(payload))

        source = LarkChannelLongConnectionSource(
            app_secret=app_secret,
            channel_factory=factory,
        )
        consume = asyncio.create_task(source.consume("cli_feishu_bot", on_event))
        await asyncio.sleep(0)
        assert len(channels) == 1
        assert channels[0][:2] == ("cli_feishu_bot", "vault-app-secret")
        channel = channels[0][2]
        assert channel.subscription is not None
        event_type, receive = channel.subscription
        assert event_type == "im.message.receive_v1"
        receive({"header": {"event_id": "evt_1"}, "event": {"message": {}}})
        for _ in range(2):
            await asyncio.sleep(0)
        assert received == [
            {
                "header": {"event_id": "evt_1", "app_id": "cli_feishu_bot"},
                "event": {"message": {}},
            }
        ]
        await source.close()
        await consume
        assert channel.disconnected
        assert secrets_resolved == 1

    asyncio.run(exercise())


def test_gateway_builds_official_sources_from_non_secret_connection_declarations() -> None:
    from trpc_service.channel_gateway import (
        FeishuLongConnectionSettings,
        _configured_feishu_long_connections,
    )

    class Secrets:
        async def resolve(self, tenant_id: str, secret_ref: str) -> str:
            assert tenant_id == TENANT
            assert secret_ref.endswith("#app-secret")
            return "vault-app-secret"

    async def exercise() -> None:
        registry = ChannelBindingRegistry.in_memory()
        await registry.register(
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
        connections = await _configured_feishu_long_connections(
            registry=registry,
            declarations=[FeishuLongConnectionSettings(tenant_id=TENANT, app_id="cli_feishu_bot")],
            secrets=Secrets(),
        )
        assert len(connections) == 1
        assert connections[0].tenant_id == TENANT
        assert connections[0].app_id == "cli_feishu_bot"
        assert isinstance(connections[0].source, LarkChannelLongConnectionSource)

    asyncio.run(exercise())


def test_gateway_uses_the_pod_identity_as_the_long_connection_lease_owner() -> None:
    from trpc_service.channel_gateway import ChannelGatewaySettings, _gateway_owner_id

    assert (
        _gateway_owner_id(
            ChannelGatewaySettings(
                database_url="postgresql://example.test/platform",
                gateway_instance_id="pod-uid-a",
            )
        )
        == "pod-uid-a"
    )
    assert (
        _gateway_owner_id(
            ChannelGatewaySettings(
                database_url="postgresql://example.test/platform",
                gateway_instance_id="pod-uid-b",
            )
        )
        == "pod-uid-b"
    )


def test_declared_long_connections_do_not_block_gateway_health_while_database_recovers() -> None:
    from trpc_service.admin_api.database import Database
    from trpc_service.channel_gateway import (
        ChannelGatewaySettings,
        FeishuLongConnectionSettings,
        create_app,
    )

    class DelayedDatabase:
        async def open(self) -> None:
            await asyncio.Event().wait()

        async def close(self) -> None:
            return None

    class Secrets:
        async def resolve(self, tenant_id: str, secret_ref: str) -> str:
            del tenant_id, secret_ref
            return "unused"

    @dataclass
    class Runner:
        async def close(self) -> None:
            return None

    app = create_app(
        ChannelGatewaySettings(
            database_url="postgresql://example.test/platform",
            feishu_long_connections=[
                FeishuLongConnectionSettings(tenant_id=TENANT, app_id="cli_feishu_bot")
            ],
        ),
        database=cast(Database, DelayedDatabase()),
        runner=cast(Any, Runner()),
        feishu_secrets=Secrets(),
    )

    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200


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
    client = ScriptedCardClient([TimeoutError()], reconciliation="delivered")
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


def test_failed_card_delivery_retries_with_the_same_delivery_key() -> None:
    client = ScriptedCardClient(
        [
            FeishuCardResponse(delivered=False, error_code="FEISHU_CARD_FAILED"),
            FeishuCardResponse(delivered=True),
        ]
    )
    store = MemoryDeliveryStore()
    service = ReplyDeliveryService(
        store=store,
        transport=FeishuCardTransport(client=client, min_update_interval_seconds=0),
        backoff_seconds=0,
    )
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
    assert result.attempts == 2
    assert [request["idempotency_key"] for request in client.requests] == [
        delivery.delivery_id,
        delivery.delivery_id,
    ]


def test_rate_limited_card_delivery_retries_through_the_delivery_state_machine() -> None:
    client = ScriptedCardClient(
        [
            FeishuCardResponse(rate_limited=True, error_code="FEISHU_CARD_RATE_LIMITED"),
            FeishuCardResponse(delivered=True),
        ]
    )
    store = MemoryDeliveryStore()
    service = ReplyDeliveryService(
        store=store,
        transport=FeishuCardTransport(client=client, min_update_interval_seconds=0),
        backoff_seconds=0,
    )
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
    assert result.attempts == 2
    assert len(client.requests) == 2


def test_http_card_client_creates_then_updates_one_stable_feishu_message() -> None:
    requests: list[dict[str, object]] = []

    def handler(request: Any) -> Response:
        requests.append(
            {
                "method": request.method,
                "path": request.url.path,
                "params": dict(request.url.params),
                "body": json.loads(request.content),
                "authorization": request.headers["Authorization"],
            }
        )
        if request.method == "POST":
            return Response(200, json={"code": 0, "data": {"message_id": "om_reply_1"}})
        return Response(200, json={"code": 0, "data": {}})

    client = HttpFeishuCardClient(
        AsyncClient(transport=MockTransport(handler)),
        tenant_access_token="tenant-token",
        api_base_url="https://feishu.test",
    )

    first = asyncio.run(
        client.upsert_card(
            conversation_id="chat_id:oc_chat_1",
            card={"elements": []},
            idempotency_key="delivery-1",
        )
    )
    second = asyncio.run(
        client.upsert_card(
            conversation_id="chat_id:oc_chat_1",
            card={"elements": [{"tag": "div"}]},
            idempotency_key="delivery-1",
        )
    )

    assert first.delivered and second.delivered
    assert requests == [
        {
            "method": "POST",
            "path": "/open-apis/im/v1/messages",
            "params": {"receive_id_type": "chat_id"},
            "body": {
                "receive_id": "oc_chat_1",
                "msg_type": "interactive",
                "content": '{"elements":[]}',
                "uuid": "delivery-1",
            },
            "authorization": "Bearer tenant-token",
        },
        {
            "method": "PATCH",
            "path": "/open-apis/im/v1/messages/om_reply_1",
            "params": {},
            "body": {
                "msg_type": "interactive",
                "content": '{"elements":[{"tag":"div"}]}',
            },
            "authorization": "Bearer tenant-token",
        },
    ]


def test_channel_gateway_registers_the_feishu_webhook_entry() -> None:
    from trpc_service.channel_gateway import ChannelGatewaySettings, create_app

    app = create_app(ChannelGatewaySettings(database_url="postgresql://example.test/platform"))
    routes = {
        (str(path), frozenset(methods))
        for route in app.routes
        if isinstance((path := getattr(route, "path", None)), str)
        and isinstance((methods := getattr(route, "methods", None)), set)
    }

    assert (
        "/internal/v1/feishu/callback/{tenant_id}/{bot_id}",
        frozenset({"POST"}),
    ) in routes


def test_channel_gateway_executes_a_verified_feishu_webhook_and_delivers_a_card() -> None:
    from trpc_service.admin_api.database import Database
    from trpc_service.agent.runner import RunnerExecutionCommand, RunnerExecutionReply
    from trpc_service.channel_gateway import ChannelGatewaySettings, create_app

    class OpenDatabase:
        async def open(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class Secrets:
        def resolve(self, tenant_id: str, secret_ref: str) -> str:
            assert tenant_id == TENANT
            if secret_ref.endswith("#verification-token"):
                return "verification-token"
            assert secret_ref.endswith("#tenant-access-token")
            return "tenant-access-token"

    @dataclass
    class Runner:
        commands: list[RunnerExecutionCommand] = field(default_factory=list)

        async def complete(self, command: RunnerExecutionCommand) -> RunnerExecutionReply:
            self.commands.append(command)
            return RunnerExecutionReply(
                tenant_id=TENANT,
                execution_id=EXECUTION,
                session_id=command.session_id,
                release_id=RELEASE,
                model_alias="test",
                invocation_id="invoke-1",
                sdk_version="test",
                platform_version="test",
                content="飞书回复",
            )

        async def close(self) -> None:
            return None

    registry = ChannelBindingRegistry.in_memory()
    binding = ChannelBinding(
        tenant_id=TENANT,
        binding_id="binding-feishu-1",
        channel_type="FEISHU",
        external_bot_id="cli_feishu_bot",
        application_id=APPLICATION,
        environment="PRODUCTION",
        secret_ref="vault://tenant/11111111/channels/feishu/cli_feishu_bot#verification-token",
    )
    asyncio.run(registry.register(binding))
    submitter = RecordingSubmitter()
    inbound = ChannelInboundService(
        registry=registry,
        secrets=StaticSecrets(),
        submitter=submitter,
        store=MemoryInboundStore(),
    )
    card_requests: list[dict[str, object]] = []

    def card_handler(request: Any) -> Response:
        card_requests.append(
            {
                "url": str(request.url),
                "authorization": request.headers["Authorization"],
                "body": json.loads(request.content),
            }
        )
        return Response(200, json={"code": 0, "data": {"message_id": "om_reply_1"}})

    runner = Runner()
    app = create_app(
        ChannelGatewaySettings(database_url="postgresql://example.test/platform"),
        database=cast(Database, OpenDatabase()),
        inbound=inbound,
        runner=cast(Any, runner),
        feishu_secrets=Secrets(),
        feishu_http=AsyncClient(transport=MockTransport(card_handler)),
        feishu_delivery_store=MemoryDeliveryStore(),
    )
    body = _webhook_body()
    material = b"verification-token"
    timestamp, nonce = "1710000000", "nonce-1"
    signature = hmac.new(
        material, timestamp.encode() + nonce.encode() + body, hashlib.sha256
    ).hexdigest()

    with TestClient(app) as client:
        app.state.registry = registry
        wrong_route = client.post(
            f"/internal/v1/feishu/callback/{TENANT}/another-bot",
            content=body,
            headers={
                "X-Lark-Request-Timestamp": timestamp,
                "X-Lark-Request-Nonce": nonce,
                "X-Lark-Signature": signature,
            },
        )
        assert wrong_route.status_code == 400
        assert submitter.submissions == []
        response = client.post(
            f"/internal/v1/feishu/callback/{TENANT}/cli_feishu_bot",
            content=body,
            headers={
                "X-Lark-Request-Timestamp": timestamp,
                "X-Lark-Request-Nonce": nonce,
                "X-Lark-Signature": signature,
            },
        )

    assert response.status_code == 200
    assert len(submitter.submissions) == 1
    assert runner.commands[0].session_id == "session:2fc01fae-08d1-50c6-9594-930d7b2c902f"
    assert len(card_requests) == 1
    assert card_requests[0]["url"] == (
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id"
    )
    assert card_requests[0]["authorization"] == "Bearer tenant-access-token"
    card_body = cast(dict[str, object], card_requests[0]["body"])
    assert {key: value for key, value in card_body.items() if key != "uuid"} == {
        "receive_id": "ou_user_1",
        "msg_type": "interactive",
        "content": (
            '{"config":{"wide_screen_mode":true},"elements":['
            '{"tag":"div","text":{"tag":"lark_md","content":"飞书回复"}}]}'
        ),
    }
    assert isinstance(card_body["uuid"], str)
