"""Integration tests: binding API, inbound ledger and reply delivery on PostgreSQL."""

from __future__ import annotations

import asyncio
import os
import uuid as uuid_module

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.channels.bindings import ChannelBindingRegistry
from trpc_service.channels.delivery import (
    ChannelTransportOutcome,
    FakeChannelTransport,
    ReplyDeliveryService,
)
from trpc_service.channels.feishu import DatabaseLongConnectionLeaseStore
from trpc_service.channels.inbound import ChannelInboundService, InboundError
from trpc_service.channels.store import (
    DatabaseBindingStore,
    DatabaseDeliveryStore,
    DatabaseInboundStore,
)
from trpc_service.database_migrations import apply_migrations

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)
PASSWORD_HASH = (
    "$argon2id$v=19$m=65536,t=3,p=4$MRV7DB8RCvU73jcYXzxkUA$"
    "z7yjdKaXuCwuYoWzAqb25/+4f8tW5j3cxFm/pComAo4"
)


def _signed(message_key: str, text: str, user: str = "user-9") -> str:
    from trpc_service.channels.inbound import fake_channel_signature

    return fake_channel_signature("fake-signing-material", message_key, text, user)


pytestmark = pytest.mark.integration


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.channel_connection_lease, tenant.reply_delivery_attempt, "
            "tenant.reply_delivery, "
            "tenant.inbound_conflict, tenant.inbound_message, tenant.channel_binding, "
            "tenant.tool_call_reconciliation, tenant.execution_checkpoint, "
            "tenant.tool_approval, tenant.tool_call, tenant.tool_definition, "
            "tenant.policy_bundle, tenant.cost_ledger, tenant.budget_alert, "
            "tenant.budget_period_state, tenant.budget, tenant.model_price, "
            "tenant.session_event, tenant.session_lease, tenant.agent_execution, "
            "tenant.agent_session, platform.outbox_record, platform.audit_outbox, "
            "platform.audit_manifest, platform.audit_chain_state, platform.audit_event, "
            "tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, platform.idempotency_record, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _seed_tenant_with_application() -> tuple[str, str]:
    tenant_id = uuid_module.uuid4()
    application_id = uuid_module.uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            "Channel Tenant",
        )
        await connection.execute(
            "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) VALUES ($1,$2,$3,$4)",
            tenant_id,
            application_id,
            f"app-{application_id.hex[:8]}",
            "Channel App",
        )
    finally:
        await connection.close()
    return str(tenant_id), str(application_id)


def _settings() -> AdminSettings:
    return AdminSettings(
        database_url=APP_URL,
        session_signing_key="test-session-key-that-is-long-enough-for-hs256",
        emergency_admin_username="break-glass",
        emergency_admin_password_hash=PASSWORD_HASH,
        session_cookie_secure=False,
        oidc_enabled=False,
    )


def _login(client: TestClient) -> None:
    login = client.post(
        "/api/v1/auth/emergency/session",
        json={"username": "break-glass", "password": "correct-horse"},
    )
    assert login.status_code == 200


def test_channel_binding_api_round_trip_and_conflicts() -> None:
    asyncio.run(_prepare_database())
    tenant_id, application_id = asyncio.run(_seed_tenant_with_application())
    app = create_app(_settings())
    with TestClient(app) as client:
        _login(client)
        payload = {
            "channel_type": "FAKE",
            "external_bot_id": "bot-77",
            "application_id": application_id,
            "environment": "PRODUCTION",
            "secret_ref": f"vault://tenant/{tenant_id}/channels/fake/bot-77#signing",
        }
        created = client.put(f"/api/v1/tenants/{tenant_id}/channel-bindings", json=payload)
        assert created.status_code == 200, created.text
        assert created.json()["status"] == "ACTIVE"
        binding_id = created.json()["binding_id"]

        # Same bot bound to another application conflicts.
        other_application = str(uuid_module.uuid4())
        asyncio.run(_seed_extra_application(tenant_id, other_application))
        conflict = client.put(
            f"/api/v1/tenants/{tenant_id}/channel-bindings",
            json={**payload, "application_id": other_application},
        )
        assert conflict.status_code == 409, conflict.text
        assert conflict.json()["error"]["code"] == "CHANNEL_BINDING_CONFLICT"

        # Plaintext secrets are rejected outright.
        rejected = client.put(
            f"/api/v1/tenants/{tenant_id}/channel-bindings",
            json={**payload, "external_bot_id": "bot-78", "secret_ref": "sk-leaked-key"},
        )
        assert rejected.status_code == 422

        resolved = client.get(
            f"/api/v1/tenants/{tenant_id}/channel-bindings/resolve",
            params={"channel_type": "FAKE", "external_bot_id": "bot-77"},
        )
        assert resolved.status_code == 200
        assert resolved.json()["binding_id"] == binding_id

        listing = client.get(f"/api/v1/tenants/{tenant_id}/channel-bindings")
        assert listing.status_code == 200
        assert [item["external_bot_id"] for item in listing.json()["bindings"]] == ["bot-77"]


async def _seed_extra_application(tenant_id: str, application_id: str) -> None:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) VALUES ($1,$2,$3,$4)",
            uuid_module.UUID(tenant_id),
            uuid_module.UUID(application_id),
            f"app-{application_id[:8]}",
            "Extra App",
        )
    finally:
        await connection.close()


class StaticSecrets:
    def resolve(self, secret_ref: str) -> str:
        return "fake-signing-material"


class StaticSubmitter:
    """Minimal execution submitter with message-id dedup semantics."""

    def __init__(self) -> None:
        self.calls = 0

    async def submit(self, submission) -> object:
        from trpc_service.agent_gateway import AgentExecutionAccepted

        self.calls += 1
        return AgentExecutionAccepted(
            execution_id=uuid_module.uuid4(),
            release_id=uuid_module.uuid4(),
            session_id=submission.session_id,
            deduplicated=self.calls > 1,
        )


def _event(message_key: str = "msg-1", text: str = "hello") -> dict[str, str]:
    return {
        "channel_type": "FAKE",
        "external_bot_id": "bot-77",
        "message_key": message_key,
        "text": text,
        "external_user_id": "user-9",
        "signature": _signed(message_key, text),
    }


def test_inbound_ledger_deduplicates_and_isolates_on_postgres() -> None:
    asyncio.run(_prepare_database())
    tenant_id, application_id = asyncio.run(_seed_tenant_with_application())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            bindings = DatabaseBindingStore(database)
            from trpc_service.channels.bindings import ChannelBinding

            await bindings.insert(
                ChannelBinding(
                    tenant_id=tenant_id,
                    binding_id=str(uuid_module.uuid4()),
                    channel_type="FAKE",
                    external_bot_id="bot-77",
                    application_id=application_id,
                    environment="PRODUCTION",
                    secret_ref=f"vault://tenant/{tenant_id[:8]}/channels/fake/bot#s",
                ),
                created_by="tester",
            )
            submitter = StaticSubmitter()
            service = ChannelInboundService(
                registry=ChannelBindingRegistry(bindings),
                secrets=StaticSecrets(),
                submitter=submitter,
                store=DatabaseInboundStore(database),
            )
            first = await service.ingest(tenant_id=tenant_id, event=_event())
            duplicate = await service.ingest(tenant_id=tenant_id, event=_event())
            assert duplicate.deduplicated is True
            assert duplicate.execution_id == first.execution_id
            assert submitter.calls == 1

            # A brand-new service (restart) still deduplicates from the ledger.
            restarted = ChannelInboundService(
                registry=ChannelBindingRegistry(bindings),
                secrets=StaticSecrets(),
                submitter=submitter,
                store=DatabaseInboundStore(database),
            )
            after_restart = await restarted.ingest(tenant_id=tenant_id, event=_event())
            assert after_restart.execution_id == first.execution_id
            assert submitter.calls == 1

            tampered = _event(message_key="msg-1", text="tampered")
            try:
                await service.ingest(tenant_id=tenant_id, event=tampered)
                raise AssertionError("conflicting payload must be isolated")
            except InboundError as error:
                assert error.code == "INBOUND_PAYLOAD_CONFLICT"
            assert submitter.calls == 1

            inbound = DatabaseInboundStore(database)
            conflicts = await inbound.list_conflicts(tenant_id)
            assert len(conflicts) == 1
            assert conflicts[0].received_hash != conflicts[0].recorded_hash
        finally:
            await database.close()

    asyncio.run(scenario())


def test_reply_delivery_state_machine_persists_on_postgres() -> None:
    asyncio.run(_prepare_database())
    tenant_id, application_id = asyncio.run(_seed_tenant_with_application())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            bindings = DatabaseBindingStore(database)
            from trpc_service.channels.bindings import ChannelBinding

            inserted_binding = await bindings.insert(
                ChannelBinding(
                    tenant_id=tenant_id,
                    binding_id=str(uuid_module.uuid4()),
                    channel_type="FAKE",
                    external_bot_id="bot-77",
                    application_id=application_id,
                    environment="PRODUCTION",
                    secret_ref=f"vault://tenant/{tenant_id[:8]}/channels/fake/bot#s",
                ),
                created_by="tester",
            )
            assert inserted_binding.binding_id
            store = DatabaseDeliveryStore(database)
            transport = FakeChannelTransport(
                [
                    ChannelTransportOutcome(
                        delivered=False, outcome_unknown=True, error_code="CHANNEL_TIMEOUT"
                    ),
                ]
            )
            transport.reconcile_results = {1: "delivered"}
            service = ReplyDeliveryService(store=store, transport=transport, backoff_seconds=0)
            delivery = await service.enqueue(
                tenant_id=tenant_id,
                binding_id=inserted_binding.binding_id,
                execution_id=str(uuid_module.uuid4()),
                external_conversation_id="conv-1",
                content="the answer",
            )
            result = await service.run(delivery.delivery_id, tenant_id=tenant_id)
            assert result.status.value == "DELIVERED"

            # A fresh service (new process) reads the same durable state.
            fresh_store = DatabaseDeliveryStore(database)
            reloaded = await fresh_store.get_delivery(tenant_id, delivery.delivery_id)
            assert reloaded is not None and reloaded.status.value == "DELIVERED"
            assert reloaded.attempts == 1
            async with database.tenant_transaction(uuid_module.UUID(tenant_id)) as conn:
                attempt_rows = await conn.fetch(
                    "SELECT * FROM tenant.reply_delivery_attempt WHERE delivery_id=$1",
                    uuid_module.UUID(delivery.delivery_id),
                )
            assert len(attempt_rows) == 1
            assert attempt_rows[0]["outcome"] == "RECONCILED_DELIVERED"
        finally:
            await database.close()

    asyncio.run(scenario())


def test_channel_tables_enforce_tenant_isolation() -> None:
    asyncio.run(_prepare_database())
    tenant_id, application_id = asyncio.run(_seed_tenant_with_application())
    other_tenant, _ = asyncio.run(_seed_tenant_with_application())

    async def scenario() -> None:
        binding_id = uuid_module.uuid4()
        admin = await asyncpg.connect(ADMIN_URL)
        try:
            await admin.execute(
                """INSERT INTO tenant.channel_binding
                    (tenant_id,binding_id,channel_type,external_bot_id,application_id,
                     environment,secret_ref,created_by)
                    VALUES ($1,$2,'FAKE','bot-77',$3,'PRODUCTION',$4,'seed')""",
                uuid_module.UUID(tenant_id),
                binding_id,
                uuid_module.UUID(application_id),
                f"vault://tenant/{tenant_id[:8]}/channels/fake/bot#s",
            )
            await admin.execute(
                """INSERT INTO tenant.inbound_message
                    (tenant_id,binding_id,message_key,payload_hash,external_user_id)
                    VALUES ($1,$2,'msg-1',$3,'user-9')""",
                uuid_module.UUID(tenant_id),
                binding_id,
                "a" * 64,
            )
        finally:
            await admin.close()

        other = await asyncpg.connect(APP_URL)
        try:
            async with other.transaction():
                await other.execute("SELECT set_config('app.tenant_id', $1, true)", other_tenant)
                assert await other.fetchval("SELECT count(*) FROM tenant.channel_binding") == 0
                assert await other.fetchval("SELECT count(*) FROM tenant.inbound_message") == 0
                assert await other.fetchval("SELECT count(*) FROM tenant.reply_delivery") == 0
        finally:
            await other.close()

    asyncio.run(scenario())


def test_feishu_long_connection_lease_fences_gateway_instances_on_postgres() -> None:
    asyncio.run(_prepare_database())
    tenant_id, _ = asyncio.run(_seed_tenant_with_application())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            first = DatabaseLongConnectionLeaseStore(database, tenant_id)
            second = DatabaseLongConnectionLeaseStore(database, tenant_id)
            acquired = await first.acquire(
                "feishu:cli_feishu_bot", "gateway-a", now=100.0, ttl_seconds=30.0
            )
            assert acquired is not None and acquired.fencing_token == 1
            assert (
                await second.acquire(
                    "feishu:cli_feishu_bot", "gateway-b", now=100.0, ttl_seconds=30.0
                )
                is None
            )
            await first.release(acquired)
            takeover = await second.acquire(
                "feishu:cli_feishu_bot", "gateway-b", now=101.0, ttl_seconds=30.0
            )
            assert takeover is not None and takeover.fencing_token == 2
            assert await first.renew(acquired, now=101.0, ttl_seconds=30.0) is None
        finally:
            await database.close()

    asyncio.run(scenario())
