"""Integration tests for the 企业微信 Channel Gateway end-to-end flow."""

from __future__ import annotations

import asyncio
import json
import os
import threading
from http.server import ThreadingHTTPServer
from uuid import uuid4

import asyncpg
import httpx
import pytest
from fastapi.testclient import TestClient

from dev.fake_external.server import FakeExternalHandler
from trpc_service.admin_api.database import Database
from trpc_service.channel_gateway import ChannelGatewaySettings, create_app
from trpc_service.channels.bindings import ChannelBinding
from trpc_service.channels.delivery import (
    ChannelTransportOutcome,
    DeliveryState,
    MemoryDeliveryStore,
    ReplyDeliveryService,
)
from trpc_service.channels.store import DatabaseBindingStore
from trpc_service.channels.wecom import (
    WeComCrypto,
    WeComRateLimiter,
    WeComReplyTransport,
    wecom_message_signature,
)
from trpc_service.database_migrations import apply_migrations

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)
TOKEN = "wecom-smoke-token"
AES_KEY = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"
RECEIVE_ID = "wecom-smoke-corpus"

pytestmark = pytest.mark.integration


def _start_fake_llm() -> tuple[ThreadingHTTPServer, int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeExternalHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def _reset_fake_im(port: int) -> None:
    httpx.post(f"http://127.0.0.1:{port}/control/v1/reset", json={})


def _recorded_im_messages(port: int) -> list[dict]:
    response = httpx.get(f"http://127.0.0.1:{port}/im/v1/messages")
    return list(response.json()["messages"])


def load_fixture(name: str) -> dict:
    with open(f"tests/fixtures/wecom/{name}.json") as handle:
        return json.load(handle)


def _encrypted_body(fixture: dict, crypto: WeComCrypto, *, response_url: str) -> tuple[str, dict]:
    plaintext = dict(fixture["plaintext"])
    plaintext["response_url"] = response_url
    encrypted = crypto.encrypt(json.dumps(plaintext, ensure_ascii=False))
    signature = wecom_message_signature(
        fixture["test_material"]["token"],
        fixture["query"]["timestamp"],
        fixture["query"]["nonce"],
        encrypted,
    )
    return signature, {"encrypt": encrypted}


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.policy_bundle, tenant.cost_ledger, tenant.budget_alert, "
            "tenant.budget_period_state, tenant.budget, tenant.model_price, "
            "tenant.session_event, tenant.session_lease, tenant.agent_execution, "
            "tenant.agent_session, tenant.channel_binding, platform.outbox_record, "
            "tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _seed_wecom_binding(
    database: Database, tenant_id: str, application_id: str, bot_id: str
) -> str:
    binding_id = uuid4()
    store = DatabaseBindingStore(database)
    await store.insert(
        ChannelBinding(
            tenant_id=tenant_id,
            binding_id=str(binding_id),
            channel_type="WECOM",
            external_bot_id=bot_id,
            application_id=application_id,
            environment="PRODUCTION",
            secret_ref=f"vault://tenant/{tenant_id}/channels/wecom#callback",
        ),
        created_by="seed",
    )
    return str(binding_id)


async def _seed_release_stack(llm_endpoint: str) -> tuple[str, str]:
    tenant_id, application_id, release_id, deployment_id = uuid4(), uuid4(), uuid4(), uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            "WeCom Tenant",
        )
        await connection.execute(
            "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) VALUES ($1,$2,$3,$4)",
            tenant_id,
            application_id,
            f"app-{application_id.hex[:8]}",
            "WeCom App",
        )
        model_profiles = [
            {
                "tenant_id": str(tenant_id),
                "alias": "primary-alias",
                "provider_model": "gpt-test",
                "endpoint_url": llm_endpoint,
                "secret_ref": f"vault://tenant/{tenant_id}/llm#primary",
                "data_classification": "CONFIDENTIAL",
                "region": "cn-test",
                "fallback_aliases": [],
                "requests_per_minute": 60,
            }
        ]
        await connection.execute(
            """INSERT INTO tenant.agent_release
            (tenant_id,id,application_id,model_alias,data_classification,region,
            fallback_aliases,model_profiles,release_version,draft_snapshot)
            VALUES ($1,$2,$3,'primary-alias','CONFIDENTIAL','cn-test','[]'::jsonb,$4::jsonb,1,
            $5::jsonb)""",
            tenant_id,
            release_id,
            application_id,
            json.dumps(model_profiles),
            json.dumps({"instructions": "Answer in one short sentence."}),
        )
        await connection.execute(
            """INSERT INTO tenant.agent_deployment
            (tenant_id,id,application_id,environment,release_id,rollout_percentage,status,
            initiator,version,activated_at)
            VALUES ($1,$2,$3,'PRODUCTION',$4,100,'ACTIVE','seed',1,now())""",
            tenant_id,
            deployment_id,
            application_id,
            release_id,
        )
    finally:
        await connection.close()
    return str(tenant_id), str(application_id)


async def _execution_count() -> int:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        return int(await connection.fetchval("SELECT count(*) FROM tenant.agent_execution"))
    finally:
        await connection.close()


def _post_callback(
    client: TestClient, tenant_id: str, fixture: dict, crypto: WeComCrypto, response_url: str
) -> TestClient.__class__:  # type: ignore[name-defined]
    signature, body = _encrypted_body(fixture, crypto, response_url=response_url)
    return client.post(
        f"/internal/v1/wecom/callback/{tenant_id}/wecom-bot-1",
        params={
            "msg_signature": signature,
            "timestamp": fixture["query"]["timestamp"],
            "nonce": fixture["query"]["nonce"],
        },
        json=body,
    )


def test_wecom_gateway_flow_end_to_end() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        server, port = _start_fake_llm()
        try:
            tenant_id, application_id = await _seed_release_stack(
                f"http://127.0.0.1:{port}/llm/v1/chat/completions"
            )
            response_url = f"http://127.0.0.1:{port}/im/v1/messages"
            _reset_fake_im(port)
            database = Database(APP_URL)
            await database.open()
            await _seed_wecom_binding(database, tenant_id, application_id, "wecom-bot-1")
            app = create_app(
                ChannelGatewaySettings(
                    database_url=APP_URL,
                    wecom_token=TOKEN,
                    wecom_encoding_aes_key=AES_KEY,
                    llm_gateway_access_key="test-key",
                    stream_min_chars=4096,
                )
            )
            crypto = WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID)
            with TestClient(app) as client:
                # Official-style URL verification: the echoed string arrives
                # encrypted; the gateway decrypts and returns the plaintext.
                echo = "echo-verification-6789"
                encrypted_echo = crypto.encrypt(echo)
                verify_query = {
                    "msg_signature": wecom_message_signature(
                        TOKEN, "1756900000", "nonce-1", encrypted_echo
                    ),
                    "timestamp": "1756900000",
                    "nonce": "nonce-1",
                    "echostr": encrypted_echo,
                }
                verified = client.get(
                    f"/internal/v1/wecom/callback/{tenant_id}/wecom-bot-1",
                    params=verify_query,
                )
                assert verified.status_code == 200
                assert verified.text == echo

                single = load_fixture("single-chat-text")
                sent = _post_callback(client, tenant_id, single, crypto, response_url)
                assert sent.status_code == 200, sent.text

                duplicate = _post_callback(client, tenant_id, single, crypto, response_url)
                assert duplicate.status_code == 200

                revoke = load_fixture("revoke-event")
                revoked = _post_callback(client, tenant_id, revoke, crypto, response_url)
                assert revoked.status_code == 200
                assert revoked.text == ""

                group = load_fixture("group-chat-text")
                grouped = _post_callback(client, tenant_id, group, crypto, response_url)
                assert grouped.status_code == 200, grouped.text

                assert await _execution_count() == 2
            recorded = _recorded_im_messages(port)
            contents = [str(item.get("text", {}).get("content", "")) for item in recorded]
            # Single chat: one merged-increment stream call plus one final
            # tracked reply. Group chat: processing notice then final reply.
            assert contents.count("fake reply") == 2
            assert contents.count("处理中,请稍候") == 1
            streams = [item for item in recorded if item.get("msgtype") == "stream"]
            assert len(streams) == 1
            assert streams[0]["stream"]["content"] == "fake reply"
        finally:
            server.shutdown()
            server.server_close()

    asyncio.run(scenario())


def test_wecom_gateway_rejects_forged_callbacks() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        server, port = _start_fake_llm()
        try:
            tenant_id, application_id = await _seed_release_stack(
                f"http://127.0.0.1:{port}/llm/v1/chat/completions"
            )
            database = Database(APP_URL)
            await database.open()
            await _seed_wecom_binding(database, tenant_id, application_id, "wecom-bot-1")
            app = create_app(
                ChannelGatewaySettings(
                    database_url=APP_URL,
                    wecom_token=TOKEN,
                    wecom_encoding_aes_key=AES_KEY,
                    llm_gateway_access_key="test-key",
                )
            )
            fixture = load_fixture("single-chat-text")
            with TestClient(app) as client:
                signature, body = _encrypted_body(
                    fixture,
                    WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID),
                    response_url=f"http://127.0.0.1:{port}/im/v1/messages",
                )
                forged = client.post(
                    f"/internal/v1/wecom/callback/{tenant_id}/wecom-bot-1",
                    params={
                        "msg_signature": "0" * 40,
                        "timestamp": fixture["query"]["timestamp"],
                        "nonce": fixture["query"]["nonce"],
                    },
                    json=body,
                )
                assert forged.status_code == 400
                assert forged.text == "WECOM_SIGNATURE_INVALID"
            assert await _execution_count() == 0
            await database.close()
        finally:
            server.shutdown()
            server.server_close()

    asyncio.run(scenario())


def test_reply_delivery_rate_limits_without_losing_messages() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        store = MemoryDeliveryStore()
        deliveries = ReplyDeliveryService(
            store=store,
            transport=WeComReplyTransport(
                httpx.AsyncClient(
                    transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
                ),
                limiter=WeComRateLimiter(capacity=1, refill_per_second=50.0),
            ),
            backoff_seconds=0.01,
            max_attempts=5,
        )
        first = await deliveries.enqueue(
            tenant_id="t-1",
            binding_id="b-1",
            execution_id="e-1",
            external_conversation_id="https://im.test/response",
            content="first reply",
        )
        first = await deliveries.run(first.delivery_id, tenant_id="t-1")
        assert first.status is DeliveryState.DELIVERED
        second = await deliveries.enqueue(
            tenant_id="t-1",
            binding_id="b-1",
            execution_id="e-2",
            external_conversation_id="https://im.test/response",
            content="second reply",
        )
        # The rate limiter backs the delivery off without losing it: once a
        # token refills, the retry delivers.
        second = await deliveries.run(second.delivery_id, tenant_id="t-1")
        assert second.status is DeliveryState.DELIVERED
        assert second.attempts >= 2

    asyncio.run(scenario())


def test_reply_delivery_outcome_unknown_parks_then_reconciles() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        store = MemoryDeliveryStore()
        transport = _RecordingTransport(
            [ChannelTransportOutcome(False, outcome_unknown=True)],
            verdicts=["unknown", "delivered"],
        )
        deliveries = ReplyDeliveryService(store=store, transport=transport)
        delivery = await deliveries.enqueue(
            tenant_id="t-1",
            binding_id="b-1",
            execution_id="e-1",
            external_conversation_id="https://im.test/response",
            content="uncertain reply",
        )
        delivery = await deliveries.run(delivery.delivery_id, tenant_id="t-1")
        assert delivery.status is DeliveryState.OUTCOME_UNKNOWN
        delivery = await deliveries.run(delivery.delivery_id, tenant_id="t-1")
        assert delivery.status is DeliveryState.DELIVERED

    asyncio.run(scenario())


def test_out_of_order_messages_produce_distinct_executions() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        server, port = _start_fake_llm()
        try:
            tenant_id, application_id = await _seed_release_stack(
                f"http://127.0.0.1:{port}/llm/v1/chat/completions"
            )
            response_url = f"http://127.0.0.1:{port}/im/v1/messages"
            _reset_fake_im(port)
            database = Database(APP_URL)
            await database.open()
            await _seed_wecom_binding(database, tenant_id, application_id, "wecom-bot-1")
            app = create_app(
                ChannelGatewaySettings(
                    database_url=APP_URL,
                    wecom_token=TOKEN,
                    wecom_encoding_aes_key=AES_KEY,
                    llm_gateway_access_key="test-key",
                )
            )
            crypto = WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=RECEIVE_ID)
            with TestClient(app) as client:
                first = _post_callback(
                    client, tenant_id, load_fixture("single-chat-text"), crypto, response_url
                )
                assert first.status_code == 200
                second = _post_callback(
                    client, tenant_id, load_fixture("group-chat-text"), crypto, response_url
                )
                assert second.status_code == 200
            assert await _execution_count() == 2
            recorded = _recorded_im_messages(port)
            assert len([m for m in recorded if m.get("msgtype") == "text"]) >= 2
            await database.close()
        finally:
            server.shutdown()
            server.server_close()

    asyncio.run(scenario())


class _RecordingTransport:
    def __init__(
        self,
        outcomes: list[ChannelTransportOutcome],
        verdicts: list[str] | None = None,
    ) -> None:
        self._outcomes = outcomes
        self._verdicts = verdicts or []

    async def send(self, delivery, attempt_no: int) -> ChannelTransportOutcome:
        return self._outcomes.pop(0)

    def reconcile(self, delivery, attempt_no: int) -> str:
        return self._verdicts.pop(0) if self._verdicts else "unknown"
