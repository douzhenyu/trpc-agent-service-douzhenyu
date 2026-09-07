"""Integration coverage for asynchronous Summary and Memory projections."""

from __future__ import annotations

import asyncio
import json
import os
from uuid import UUID, uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.admin_api.database import Database
from trpc_service.agent_gateway import AgentExecutionSubmission, AgentExecutionSubmitter
from trpc_service.agent_worker import (
    AgentExecutionProcessor,
    AgentWorker,
    DatabaseDeploymentRouteResolver,
    DatabaseReleaseRouteResolver,
)
from trpc_service.database_migrations import apply_migrations
from trpc_service.execution_bus import InMemoryExecutionBus, OutboxDispatcher
from trpc_service.job_worker import (
    JobWorkerSettings,
    SessionProjectionConsumer,
    SummaryMemoryJobWorker,
)
from trpc_service.job_worker import (
    create_app as create_job_worker_app,
)
from trpc_service.llm_gateway import GatewayRequest, GatewayResult
from trpc_service.sessions import SessionLeaseManager

ADMIN_URL = os.getenv(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)

pytestmark = pytest.mark.integration


class ScriptedGateway:
    async def complete(self, _request: GatewayRequest) -> GatewayResult:
        return GatewayResult(
            model_alias="primary-alias",
            fallback_used=False,
            completion={"choices": [{"message": {"content": "async reply"}}]},
        )


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE platform.outbox_record, platform.audit_event, "
            "platform.audit_chain_state, platform.tenant CASCADE"
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
            f"projection-{tenant_id.hex[:8]}",
            "Projection Tenant",
        )
        await connection.execute(
            "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) VALUES ($1,$2,$3,$4)",
            tenant_id,
            application_id,
            f"projection-{application_id.hex[:8]}",
            "Projection App",
        )
        profiles = [
            {
                "tenant_id": str(tenant_id),
                "alias": "primary-alias",
                "provider_model": "test-model",
                "endpoint_url": "http://test.invalid",
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
            VALUES ($1,$2,$3,'primary-alias','CONFIDENTIAL','cn-test',
            '[]'::jsonb,$4::jsonb,1,'{}'::jsonb)""",
            tenant_id,
            release_id,
            application_id,
            json.dumps(profiles),
        )
        await connection.execute(
            """INSERT INTO tenant.agent_deployment
            (tenant_id,id,application_id,environment,release_id,rollout_percentage,status,
             initiator,version,activated_at)
            VALUES ($1,$2,$3,'PRODUCTION',$4,100,'ACTIVE','test',1,now())""",
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


def _submission(
    tenant_id: str, application_id: str, content: str, message_id: str
) -> AgentExecutionSubmission:
    return AgentExecutionSubmission(
        tenant_id=UUID(tenant_id),
        application_id=UUID(application_id),
        environment="PRODUCTION",
        session_id="session-summary-memory",
        subject_id="im:FEISHU:binding-1:alice",
        memory_policy_version="policy:7",
        messages=[{"role": "user", "content": content}],
        message_id=message_id,
    )


def test_job_worker_projects_committed_events_without_blocking_replies() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        tenant_id, application_id, _release_id = await _seed_release_stack()
        database = await _open_database()
        try:
            bus = InMemoryExecutionBus()
            submitter = AgentExecutionSubmitter(database, DatabaseDeploymentRouteResolver(database))
            processor = AgentExecutionProcessor(
                AgentWorker(ScriptedGateway(), DatabaseReleaseRouteResolver(database)),
                database,
                DatabaseReleaseRouteResolver(database),
                SessionLeaseManager(database),
                "projection-worker",
            )
            dispatcher = OutboxDispatcher(database, bus)
            first = await submitter.submit(
                _submission(tenant_id, application_id, "remember tea", "m-1")
            )
            assert await dispatcher.dispatch_pending() == 1
            execution_envelope = bus.published[-1]
            await processor.handle(execution_envelope)

            connection = await asyncpg.connect(ADMIN_URL)
            try:
                status = await connection.fetchval(
                    "SELECT status FROM tenant.agent_execution WHERE tenant_id=$1 AND id=$2",
                    UUID(tenant_id),
                    first.execution_id,
                )
                assert status == "SUCCEEDED"
            finally:
                await connection.close()

            assert await dispatcher.dispatch_pending() == 1
            older_projection = bus.published[-1]
            projections = SummaryMemoryJobWorker(database)
            consumer = SessionProjectionConsumer(database, projections)
            assert await consumer.run_once() is True
            metrics = await consumer.metrics()
            assert metrics.pending_count == 0
            assert metrics.p99_visibility_seconds <= metrics.target_seconds
            assert projections.monitor.p99_visibility_seconds <= projections.monitor.target_seconds
            assert projections.monitor.backlog_alerts == []

            second = await submitter.submit(
                _submission(tenant_id, application_id, "remember coffee", "m-2")
            )
            assert await dispatcher.dispatch_pending() == 2
            execution_envelope = next(
                envelope
                for envelope in reversed(bus.published)
                if envelope.data.get("execution_id") == str(second.execution_id)
            )
            await processor.handle(execution_envelope)
            assert await dispatcher.dispatch_pending() == 1
            assert await consumer.run_once() is True
            await projections.handle(older_projection)

            connection = await asyncpg.connect(ADMIN_URL)
            try:
                summary = await connection.fetchrow(
                    """SELECT source_from_version,source_version,content
                    FROM tenant.session_summary WHERE tenant_id=$1 AND session_id=$2""",
                    UUID(tenant_id),
                    "session-summary-memory",
                )
                assert summary is not None
                assert (summary["source_from_version"], summary["source_version"]) == (1, 4)
                assert "tea" in summary["content"]
                assert "coffee" in summary["content"]
                memories = await connection.fetch(
                    """SELECT id,subject_id,source_session_id,source_from_version,source_to_version,
                    policy_version,is_valid FROM tenant.memory_record
                    WHERE tenant_id=$1 ORDER BY source_to_version""",
                    UUID(tenant_id),
                )
                source_ranges = [
                    (row["source_from_version"], row["source_to_version"]) for row in memories
                ]
                assert source_ranges == [
                    (1, 2),
                    (3, 4),
                ]
                assert all(row["subject_id"] == "im:FEISHU:binding-1:alice" for row in memories)
                assert all(row["source_session_id"] == "session-summary-memory" for row in memories)
                assert all(row["policy_version"] == "policy:7" for row in memories)
                memory_id, second_memory_id = memories[0]["id"], memories[1]["id"]
            finally:
                await connection.close()

            app = create_job_worker_app(
                JobWorkerSettings(database_url=APP_URL, operator_token="memory-operator")
            )
            with TestClient(app) as client:
                protected_metrics = client.get("/internal/v1/projection-metrics")
                assert protected_metrics.status_code == 403
                metrics_response = client.get(
                    "/internal/v1/projection-metrics",
                    headers={"X-Job-Worker-Operator-Token": "memory-operator"},
                )
                assert metrics_response.status_code == 200, metrics_response.text
                corrected = client.post(
                    f"/internal/v1/tenants/{tenant_id}/memories/{memory_id}/corrections",
                    headers={"X-Job-Worker-Operator-Token": "memory-operator"},
                    json={"actor": "memory-admin", "reason": "source was corrected"},
                )
                assert corrected.status_code == 200, corrected.text
                deleted = client.post(
                    f"/internal/v1/tenants/{tenant_id}/memories/{second_memory_id}/deletions",
                    headers={"X-Job-Worker-Operator-Token": "memory-operator"},
                    json={"actor": "memory-admin", "reason": "retention request"},
                )
                assert deleted.status_code == 200, deleted.text
            connection = await asyncpg.connect(ADMIN_URL)
            try:
                valid = await connection.fetchval(
                    "SELECT is_valid FROM tenant.memory_record WHERE tenant_id=$1 AND id=$2",
                    UUID(tenant_id),
                    memory_id,
                )
                audit_action = await connection.fetchval(
                    """SELECT action FROM platform.audit_event WHERE tenant_id=$1
                    AND target_id=$2 ORDER BY occurred_at DESC LIMIT 1""",
                    UUID(tenant_id),
                    str(memory_id),
                )
                invalidations = await connection.fetchval(
                    """SELECT count(*) FROM platform.outbox_record WHERE tenant_id=$1
                    AND event_type='platform.memory.invalidated.v1'""",
                    UUID(tenant_id),
                )
                assert valid is False
                assert audit_action == "memory.corrected"
                deleted_count = await connection.fetchval(
                    "SELECT count(*) FROM tenant.memory_record WHERE tenant_id=$1 AND id=$2",
                    UUID(tenant_id),
                    second_memory_id,
                )
                deleted_audit = await connection.fetchval(
                    """SELECT action FROM platform.audit_event WHERE tenant_id=$1 AND target_id=$2
                    ORDER BY occurred_at DESC LIMIT 1""",
                    UUID(tenant_id),
                    str(second_memory_id),
                )
                assert int(invalidations) >= 4
                assert int(deleted_count) == 0
                assert deleted_audit == "memory.deleted"
                await connection.execute(
                    """UPDATE platform.session_projection_delivery SET status='DEAD_LETTER',
                    completed_at=NULL WHERE outbox_id=(SELECT id FROM platform.outbox_record
                    WHERE tenant_id=$1 AND event_type='platform.session.events.committed.v1'
                    ORDER BY created_at DESC LIMIT 1)""",
                    UUID(tenant_id),
                )
            finally:
                await connection.close()
            degraded = await consumer.metrics()
            assert degraded.dead_letter_count == 1
            assert degraded.alerting is True
            assert degraded.degraded is True
        finally:
            await database.close()

    asyncio.run(scenario())
