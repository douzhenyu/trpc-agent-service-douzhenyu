from __future__ import annotations

import asyncio
import json
import os
from uuid import UUID, uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.database import Database
from trpc_service.agent_gateway import (
    AgentExecutionSubmission,
    AgentExecutionSubmitter,
    AgentGatewaySettings,
    create_app,
)
from trpc_service.agent_worker import (
    AgentExecutionProcessor,
    AgentWorker,
    DatabaseDeploymentRouteResolver,
    DatabaseReleaseRouteResolver,
)
from trpc_service.database_migrations import apply_migrations
from trpc_service.execution_bus import (
    EXECUTION_REQUESTED_EVENT,
    ExecutionEnvelope,
    InMemoryExecutionBus,
    OutboxDispatcher,
)
from trpc_service.llm_gateway import GatewayRequest, GatewayResult
from trpc_service.sessions import (
    SessionEvent,
    SessionLeaseError,
    SessionLeaseManager,
    SessionVersionConflictError,
    commit_session_events,
)

ADMIN_URL = os.getenv(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)

pytestmark = pytest.mark.integration


class ScriptedGateway:
    """Deterministic LLM Gateway stand-in for pipeline integration tests."""

    def __init__(self) -> None:
        self.requests: list[GatewayRequest] = []

    async def complete(self, request: GatewayRequest) -> GatewayResult:
        self.requests.append(request)
        return GatewayResult(
            model_alias="primary-alias",
            fallback_used=False,
            completion={"role": "assistant", "content": "gateway-reply"},
        )


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.session_event, tenant.session_lease, tenant.agent_execution, "
            "tenant.agent_session, platform.outbox_record, "
            "tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _seed_release_stack() -> tuple[str, str, str]:
    tenant_id, application_id, release_id, deployment_id = uuid4(), uuid4(), uuid4(), uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            "Execution Bus Tenant",
        )
        await connection.execute(
            "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) VALUES ($1,$2,$3,$4)",
            tenant_id,
            application_id,
            f"app-{application_id.hex[:8]}",
            "Execution Bus App",
        )
        model_profiles = [
            {
                "tenant_id": str(tenant_id),
                "alias": "primary-alias",
                "provider_model": "gpt-test",
                "endpoint_url": "http://fake-external:8090/v1/chat/completions",
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
            fallback_aliases,model_profiles,release_version)
            VALUES ($1,$2,$3,'primary-alias','CONFIDENTIAL','cn-test','[]'::jsonb,$4::jsonb,1)""",
            tenant_id,
            release_id,
            application_id,
            json.dumps(model_profiles),
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
    return str(tenant_id), str(application_id), str(release_id)


async def _open_database() -> Database:
    database = Database(APP_URL)
    await database.open()
    return database


async def _authority_state(tenant_id: str, session_id: str) -> dict[str, int]:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        return {
            "executions": int(
                await connection.fetchval(
                    "SELECT count(*) FROM tenant.agent_execution WHERE tenant_id=$1",
                    UUID(tenant_id),
                )
            ),
            "events": int(
                await connection.fetchval(
                    """SELECT count(*) FROM tenant.session_event
                    WHERE tenant_id=$1 AND session_id=$2""",
                    UUID(tenant_id),
                    session_id,
                )
            ),
            "version": int(
                await connection.fetchval(
                    "SELECT version FROM tenant.agent_session WHERE tenant_id=$1 AND id=$2",
                    UUID(tenant_id),
                    session_id,
                )
                or 0
            ),
            "outbox_committed": int(
                await connection.fetchval(
                    """SELECT count(*) FROM platform.outbox_record
                    WHERE tenant_id=$1 AND event_type='platform.session.events.committed.v1'""",
                    UUID(tenant_id),
                )
            ),
        }
    finally:
        await connection.close()


def _submission(
    tenant_id: str,
    application_id: str,
    session_id: str,
    message_id: str | None = None,
    content: str = "hello session",
) -> AgentExecutionSubmission:
    return AgentExecutionSubmission(
        tenant_id=UUID(tenant_id),
        application_id=UUID(application_id),
        environment="PRODUCTION",
        session_id=session_id,
        messages=[{"role": "user", "content": content}],
        message_id=message_id,
    )


def _processor(
    database: Database, owner_id: str
) -> tuple[AgentExecutionProcessor, ScriptedGateway]:
    gateway = ScriptedGateway()
    worker = AgentWorker(gateway, DatabaseReleaseRouteResolver(database))
    processor = AgentExecutionProcessor(
        worker,
        database,
        DatabaseReleaseRouteResolver(database),
        SessionLeaseManager(database),
        owner_id,
    )
    return processor, gateway


async def _expire_lease(tenant_id: str, session_id: str) -> None:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "UPDATE tenant.session_lease SET expires_at=now() - interval '1 second' "
            "WHERE tenant_id=$1 AND session_id=$2",
            UUID(tenant_id),
            session_id,
        )
    finally:
        await connection.close()


def test_gateway_commits_execution_and_outbox_in_one_transaction() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        tenant_id, application_id, release_id = await _seed_release_stack()
        bus = InMemoryExecutionBus(partition_count=4)
        app = create_app(
            AgentGatewaySettings(database_url=APP_URL, dispatch_interval_seconds=0.0),
            bus=bus,
        )
        with TestClient(app) as client:
            payload = {
                "tenant_id": tenant_id,
                "application_id": application_id,
                "environment": "PRODUCTION",
                "session_id": "session-tx",
                "messages": [{"role": "user", "content": "hello session"}],
                "message_id": "fixed-message-1",
            }
            submitted = client.post("/internal/v1/agent-executions", json=payload)
            assert submitted.status_code == 202
            body = submitted.json()
            assert body["release_id"] == release_id
            assert body["deduplicated"] is False
            duplicate = client.post("/internal/v1/agent-executions", json=payload)
            assert duplicate.status_code == 200
            assert duplicate.json()["execution_id"] == body["execution_id"]
            assert duplicate.json()["deduplicated"] is True
            conflicting = client.post(
                "/internal/v1/agent-executions",
                json={**payload, "messages": [{"role": "user", "content": "different payload"}]},
            )
            assert conflicting.status_code == 409
            assert conflicting.json()["detail"] == "MESSAGE_PAYLOAD_CONFLICT"
            unknown_app = client.post(
                "/internal/v1/agent-executions",
                json={**payload, "application_id": str(uuid4()), "message_id": "lost-message"},
            )
            assert unknown_app.status_code == 409
            assert unknown_app.json()["detail"] == "DEPLOYMENT_NOT_FOUND"
        connection = await asyncpg.connect(ADMIN_URL)
        try:
            executions = await connection.fetch(
                "SELECT status,application_id,payload_hash FROM tenant.agent_execution "
                "WHERE tenant_id=$1",
                UUID(tenant_id),
            )
            assert len(executions) == 1
            assert executions[0]["status"] == "PENDING"
            assert str(executions[0]["application_id"]) == application_id
            assert len(str(executions[0]["payload_hash"])) == 64
            outbox = await connection.fetch(
                "SELECT status,event_type,partition_key,correlation_id,data_classification "
                "FROM platform.outbox_record WHERE tenant_id=$1",
                UUID(tenant_id),
            )
            assert len(outbox) == 1
            assert outbox[0]["status"] == "PENDING"
            assert outbox[0]["event_type"] == EXECUTION_REQUESTED_EVENT
            assert outbox[0]["partition_key"] == f"{tenant_id}:session-tx"
            assert outbox[0]["correlation_id"] == body["execution_id"]
            assert outbox[0]["data_classification"] == "CONFIDENTIAL"
            sessions = await connection.fetchval(
                "SELECT count(*) FROM tenant.agent_session WHERE tenant_id=$1", UUID(tenant_id)
            )
            assert int(sessions) == 1
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_dispatcher_publishes_pending_records_and_marks_them_published() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            tenant_id, application_id, _release_id = await _seed_release_stack()
            bus = InMemoryExecutionBus(partition_count=4)
            submitter = AgentExecutionSubmitter(database, DatabaseDeploymentRouteResolver(database))
            await submitter.submit(_submission(tenant_id, application_id, "session-dispatch"))
            dispatcher = OutboxDispatcher(database, bus)
            assert await dispatcher.dispatch_pending() == 1
            assert await dispatcher.dispatch_pending() == 0
            assert len(bus.published) == 1
            envelope = bus.published[0]
            assert envelope.tenant_id == tenant_id
            assert envelope.data_schema == f"{EXECUTION_REQUESTED_EVENT}.schema.json"
            assert envelope.data["session_id"] == "session-dispatch"
            assert envelope.data["tenant_id"] == tenant_id
            assert envelope.correlation_id is not None
            assert envelope.to_dict()["specversion"] == "1.0"
            connection = await asyncpg.connect(ADMIN_URL)
            try:
                row = await connection.fetchrow(
                    "SELECT status,attempts,published_at FROM platform.outbox_record "
                    "WHERE tenant_id=$1",
                    UUID(tenant_id),
                )
                assert row is not None
                assert row["status"] == "PUBLISHED"
                assert int(row["attempts"]) == 1
                assert row["published_at"] is not None
            finally:
                await connection.close()
        finally:
            await database.close()

    asyncio.run(scenario())


def test_duplicate_message_yields_one_execution_and_one_authoritative_event_set() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            tenant_id, application_id, release_id = await _seed_release_stack()
            bus = InMemoryExecutionBus(partition_count=4)
            submitter = AgentExecutionSubmitter(database, DatabaseDeploymentRouteResolver(database))
            submission = _submission(
                tenant_id, application_id, "session-dedup", message_id="fixed-message-1"
            )
            first = await submitter.submit(submission)
            second = await submitter.submit(submission)
            assert first.execution_id == second.execution_id
            assert second.deduplicated is True
            dispatcher = OutboxDispatcher(database, bus)
            assert await dispatcher.dispatch_pending() == 1
            processor, gateway = _processor(database, "worker-a")
            envelope = bus.published[0]
            assert envelope.data["execution_id"] == str(first.execution_id)
            assert await bus.deliver_once(processor.handle) is True
            await bus.publish(envelope)
            assert await bus.deliver_once(processor.handle) is True
            assert len(gateway.requests) == 1
            assert gateway.requests[0].release_id == release_id
            state = await _authority_state(tenant_id, "session-dedup")
            assert state["executions"] == 1
            assert state["events"] == 2
            assert state["version"] == 2
            connection = await asyncpg.connect(ADMIN_URL)
            try:
                events = await connection.fetch(
                    """SELECT sequence,kind,payload FROM tenant.session_event
                    WHERE tenant_id=$1 AND session_id=$2 ORDER BY sequence""",
                    UUID(tenant_id),
                    "session-dedup",
                )
                assert [event["kind"] for event in events] == ["USER_MESSAGE", "AGENT_REPLY"]
                assert json.loads(events[0]["payload"])["content"] == "hello session"
                assert json.loads(events[1]["payload"])["model_alias"] == "primary-alias"
                committed = await connection.fetchrow(
                    """SELECT causation_id,correlation_id,data_classification
                    FROM platform.outbox_record
                    WHERE tenant_id=$1
                      AND event_type='platform.session.events.committed.v1'""",
                    UUID(tenant_id),
                )
                assert committed is not None
                assert committed["causation_id"] == "fixed-message-1"
                assert committed["correlation_id"] == str(first.execution_id)
                assert committed["data_classification"] == "CONFIDENTIAL"
                status = await connection.fetchval(
                    "SELECT status FROM tenant.agent_execution WHERE tenant_id=$1",
                    UUID(tenant_id),
                )
                assert status == "SUCCEEDED"
            finally:
                await connection.close()
        finally:
            await database.close()

    asyncio.run(scenario())


def test_worker_rejects_a_message_without_a_known_execution() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            tenant_id, application_id, release_id = await _seed_release_stack()
            bus = InMemoryExecutionBus(partition_count=2)
            fabricated = ExecutionEnvelope(
                message_id="unknown-message",
                source="trpc-agent-platform://agent-gateway",
                event_type=EXECUTION_REQUESTED_EVENT,
                partition_key=f"{tenant_id}:session-ghost",
                time="2026-09-04T09:00:00+00:00",
                tenant_id=tenant_id,
                data_schema=f"{EXECUTION_REQUESTED_EVENT}.schema.json",
                data={
                    "tenant_id": tenant_id,
                    "application_id": application_id,
                    "execution_id": str(uuid4()),
                    "release_id": release_id,
                    "environment": "PRODUCTION",
                    "session_id": "session-ghost",
                    "messages": [{"role": "user", "content": "ghost"}],
                },
            )
            await bus.publish(fabricated)
            processor, gateway = _processor(database, "worker-a")
            assert await bus.deliver_once(processor.handle) is False
            assert len(gateway.requests) == 0
            state = await _authority_state(tenant_id, "session-ghost")
            assert state["events"] == 0
        finally:
            await database.close()

    asyncio.run(scenario())


def test_worker_that_lost_its_lease_cannot_commit() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            tenant_id, application_id, _release_id = await _seed_release_stack()
            submitter = AgentExecutionSubmitter(database, DatabaseDeploymentRouteResolver(database))
            submission = _submission(
                tenant_id, application_id, "session-fence", message_id="fence-message-1"
            )
            await submitter.submit(submission)
            leases = SessionLeaseManager(database)
            grant_a = await leases.acquire(UUID(tenant_id), "session-fence", "worker-a")
            with pytest.raises(SessionLeaseError, match="SESSION_LEASE_HELD"):
                await leases.acquire(UUID(tenant_id), "session-fence", "worker-b")
            await _expire_lease(tenant_id, "session-fence")
            grant_b = await leases.acquire(UUID(tenant_id), "session-fence", "worker-b")
            assert grant_b.fencing_token == grant_a.fencing_token + 1
            events = [SessionEvent(kind="AGENT_REPLY", payload={"content": "stale"})]
            async with database.tenant_transaction(UUID(tenant_id)) as tx:
                with pytest.raises(SessionLeaseError, match="SESSION_LEASE_INVALID"):
                    await commit_session_events(
                        tx,
                        tenant_id=UUID(tenant_id),
                        session_id="session-fence",
                        owner_id="worker-a",
                        fencing_token=grant_a.fencing_token,
                        expected_version=grant_a.session_version,
                        execution_id=uuid4(),
                        idempotency_key="fence-message-1",
                        events=events,
                    )
            state = await _authority_state(tenant_id, "session-fence")
            assert state["events"] == 0
            assert state["version"] == 0
            async with database.tenant_transaction(UUID(tenant_id)) as tx:
                new_version = await commit_session_events(
                    tx,
                    tenant_id=UUID(tenant_id),
                    session_id="session-fence",
                    owner_id="worker-b",
                    fencing_token=grant_b.fencing_token,
                    expected_version=grant_b.session_version,
                    execution_id=uuid4(),
                    idempotency_key="fence-message-1",
                    events=events,
                )
            assert new_version == grant_b.session_version + 1
            state = await _authority_state(tenant_id, "session-fence")
            assert state["events"] == 1
            assert state["version"] == 1
        finally:
            await database.close()

    asyncio.run(scenario())


def test_version_conflict_blocks_a_stale_worker_result() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            tenant_id, application_id, _release_id = await _seed_release_stack()
            submitter = AgentExecutionSubmitter(database, DatabaseDeploymentRouteResolver(database))
            await submitter.submit(
                _submission(
                    tenant_id,
                    application_id,
                    "session-version",
                    message_id="version-message-1",
                )
            )
            leases = SessionLeaseManager(database)
            grant = await leases.acquire(UUID(tenant_id), "session-version", "worker-a")
            await leases.renew(UUID(tenant_id), "session-version", "worker-a", grant.fencing_token)
            with pytest.raises(SessionLeaseError, match="SESSION_LEASE_INVALID"):
                await leases.renew(UUID(tenant_id), "session-version", "worker-a", 99)
            events = [SessionEvent(kind="AGENT_REPLY", payload={"content": "stale"})]
            async with database.tenant_transaction(UUID(tenant_id)) as tx:
                with pytest.raises(SessionVersionConflictError, match="SESSION_VERSION_CONFLICT"):
                    await commit_session_events(
                        tx,
                        tenant_id=UUID(tenant_id),
                        session_id="session-version",
                        owner_id="worker-a",
                        fencing_token=grant.fencing_token,
                        expected_version=grant.session_version + 5,
                        execution_id=uuid4(),
                        idempotency_key="version-message-1",
                        events=events,
                    )
            state = await _authority_state(tenant_id, "session-version")
            assert state["events"] == 0
            assert state["version"] == 0
            async with database.tenant_transaction(UUID(tenant_id)) as tx:
                new_version = await commit_session_events(
                    tx,
                    tenant_id=UUID(tenant_id),
                    session_id="session-version",
                    owner_id="worker-a",
                    fencing_token=grant.fencing_token,
                    expected_version=grant.session_version,
                    execution_id=uuid4(),
                    idempotency_key="version-message-1",
                    events=events,
                )
            assert new_version == grant.session_version + 1
            state = await _authority_state(tenant_id, "session-version")
            assert state["events"] == 1
            assert state["version"] == 1
        finally:
            await database.close()

    asyncio.run(scenario())
