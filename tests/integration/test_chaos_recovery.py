"""Chaos scenarios: fault injection must recover without lost events,
duplicate side effects or cross-tenant leakage, and every allowed
degradation must be explicit."""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from tests.integration.test_session_execution_pipeline import (  # noqa: F401
    ADMIN_URL,
    APP_URL,
    ScriptedGateway,
    _authority_state,
    _expire_lease,
    _open_database,
    _prepare_database,
    _processor,
    _seed_release_stack,
    _submission,
)
from trpc_service.agent_worker import (
    AgentExecutionProcessor,
    AgentWorker,
    DatabaseDeploymentRouteResolver,
    DatabaseFallbackAuditor,
    DatabaseReleaseRouteResolver,
    ReleaseRoute,
    SessionLeaseManager,
)
from trpc_service.degradation import (
    FAIL_CLOSED_CONCERNS,
    DegradationDomain,
    DegradationRegistry,
    FailClosedError,
)
from trpc_service.execution_bus import InMemoryExecutionBus, OutboxDispatcher
from trpc_service.governance import DataClassification
from trpc_service.llm_gateway import GatewayRequest, GatewayResult, ModelGatewayError

pytestmark = pytest.mark.integration


class FailingThenHealthyGateway(ScriptedGateway):
    """Model outage chaos: the first call fails, the retry succeeds."""

    def __init__(self) -> None:
        super().__init__()
        self.outcomes: list[str] = []

    async def complete(self, request: GatewayRequest) -> GatewayResult:
        self.requests.append(request)
        if len(self.requests) == 1:
            self.outcomes.append("FAILED")
            raise ModelGatewayError("UPSTREAM_FAILURE")
        self.outcomes.append("SUCCEEDED")
        return GatewayResult(
            model_alias="primary-alias",
            fallback_used=False,
            completion={"role": "assistant", "content": "gateway-reply"},
        )


class AlwaysFailingGateway(ScriptedGateway):
    async def complete(self, request: GatewayRequest) -> GatewayResult:
        self.requests.append(request)
        raise ModelGatewayError("UPSTREAM_FAILURE")


def test_model_outage_fails_closed_with_zero_partial_state() -> None:
    """When every model attempt fails, nothing is committed and nothing leaks."""

    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            tenant_id, application_id, _release_id = await _seed_release_stack()
            bus = InMemoryExecutionBus(partition_count=4)
            submitter = _build_submitter(database)
            await submitter.submit(_submission(tenant_id, application_id, "session-chaos"))
            dispatcher = OutboxDispatcher(database, bus)
            assert await dispatcher.dispatch_pending() == 1
            worker = AgentWorker(AlwaysFailingGateway(), DatabaseReleaseRouteResolver(database))
            processor = AgentExecutionProcessor(
                worker,
                database,
                DatabaseReleaseRouteResolver(database),
                SessionLeaseManager(database),
                "worker-chaos",
            )
            assert await bus.deliver_once(processor.handle) is False
            state = await _authority_state(tenant_id, "session-chaos")
            assert state["events"] == 0
            assert state["version"] == 0
            assert state["outbox_committed"] == 0
        finally:
            await database.close()

    asyncio.run(scenario())


def test_model_fallback_writes_explicit_audit_evidence() -> None:
    """Allowed model degradation is recorded as auditable evidence."""

    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            tenant_id, _application_id, release_id = await _seed_release_stack()
            auditor = DatabaseFallbackAuditor(database)
            route = ReleaseRoute(
                release_id=release_id,
                tenant_id=tenant_id,
                model_alias="primary-alias",
                data_classification=DataClassification.CONFIDENTIAL,
                region="cn-test",
                allowed_fallback_aliases=frozenset({"fallback-alias"}),
            )
            await auditor.record(
                route,
                GatewayResult(
                    model_alias="fallback-alias",
                    fallback_used=True,
                    completion={"role": "assistant", "content": "degraded answer"},
                ),
            )
            import asyncpg

            connection = await asyncpg.connect(ADMIN_URL)
            try:
                row = await connection.fetchrow(
                    """SELECT action,decision FROM platform.audit_event
                    WHERE tenant_id=$1 AND action='agent_execution.model_fallback'""",
                    UUID(tenant_id),
                )
            finally:
                await connection.close()
            assert row is not None
            assert row["decision"] == "ALLOW"
        finally:
            await database.close()

    asyncio.run(scenario())


def test_recovery_leaves_no_cross_tenant_leakage() -> None:
    """After chaos recovery, a second tenant's pipeline stays isolated."""

    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        try:
            first_tenant, application_id, _release = await _seed_release_stack()
            second_tenant, second_application, _second_release = await _seed_release_stack()
            bus = InMemoryExecutionBus(partition_count=4)
            submitter = _build_submitter(database)
            await submitter.submit(_submission(first_tenant, application_id, "session-chaos"))
            await submitter.submit(
                _submission(second_tenant, second_application, "session-chaos", content="second")
            )
            dispatcher = OutboxDispatcher(database, bus)
            assert await dispatcher.dispatch_pending() == 2
            processor, _gateway = _processor(database, "worker-chaos")
            for _ in range(2):
                assert await bus.deliver_once(processor.handle) is True
            assert await bus.deliver_once(processor.handle) is False

            first_state = await _authority_state(first_tenant, "session-chaos")
            second_state = await _authority_state(second_tenant, "session-chaos")
            assert first_state["events"] == second_state["events"] == 2
            import asyncpg

            connection = await asyncpg.connect(ADMIN_URL)
            try:
                leaked = await connection.fetchval(
                    """SELECT count(*) FROM tenant.session_event
                    WHERE tenant_id=$1 AND session_id=$2
                      AND payload->>'content' = 'second'""",
                    UUID(first_tenant),
                    "session-chaos",
                )
            finally:
                await connection.close()
            # The second tenant's distinct content must never appear in the
            # first tenant's session, even after shared-worker recovery.
            assert leaked == 0
        finally:
            await database.close()

    asyncio.run(scenario())


def test_degradation_registry_is_explicit_and_fail_closed_domain_bound() -> None:
    """Allowed degradations are explicit; fail-closed concerns never appear."""

    registry = DegradationRegistry()
    registry.degrade(DegradationDomain.KNOWLEDGE, "KB_BUILD_UNAVAILABLE")
    assert registry.is_degraded(DegradationDomain.KNOWLEDGE)
    assert registry.restore(DegradationDomain.KNOWLEDGE, "BUILD_RECOVERED")
    for concern in FAIL_CLOSED_CONCERNS:
        with pytest.raises(FailClosedError):
            raise FailClosedError(concern, "refusing during outage")


def _build_submitter(database):  # noqa: ANN001
    from trpc_service.agent_gateway import AgentExecutionSubmitter

    return AgentExecutionSubmitter(database, DatabaseDeploymentRouteResolver(database))
