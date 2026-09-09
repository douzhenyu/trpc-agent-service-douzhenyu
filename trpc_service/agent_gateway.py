"""Agent Gateway data-plane entry committing executions through the transactional Outbox.

The gateway resolves the environment Deployment to a fixed Agent Release, then
commits the Agent Execution business state and the Outbox record in a single
PostgreSQL transaction. The Outbox dispatcher publishes to the execution bus
afterwards; a duplicate submission dedupes on the message id and a resubmission
with a different payload is rejected so one message id never maps to two
business payloads.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import cast
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.admin_api.database import Connection, Database
from trpc_service.agent.protocols import ReleaseProtocolRegistry
from trpc_service.agent.runner import ReleasePinnedRunnerRuntime
from trpc_service.agent_worker import (
    DatabaseDeploymentRouteResolver,
    DatabaseReleaseRouteResolver,
)
from trpc_service.backpressure import (
    ADMISSION_DECISIONS,
    AdmissionController,
    AdmissionDenied,
    CapacityPolicy,
    shed_level,
)
from trpc_service.degradation import register_degradations_endpoint
from trpc_service.execution_bus import (
    EXECUTION_COMPLETED_EVENT,
    EXECUTION_REQUESTED_EVENT,
    GATEWAY_SOURCE,
    ExecutionBusPublisher,
    ExecutionRequestedData,
    InMemoryExecutionBus,
    KafkaExecutionBus,
    OutboxDispatcher,
    insert_outbox_record,
    session_partition_key,
)
from trpc_service.ids import uuid7
from trpc_service.policy_bundles import PolicyBundleRulesResolver, PolicyBundleService
from trpc_service.runtime_health import RuntimeHealthResponse
from trpc_service.sessions import create_session_if_missing
from trpc_service.telemetry import current_traceparent, install_telemetry
from trpc_service.version import TRPC_AGENT_VERSION, __version__


class AgentGatewayError(RuntimeError):
    """Safe, stable error for callers; never embeds storage or broker details."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class AgentGatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = ""
    dispatch_interval_seconds: float = 1.0
    partition_count: int = 8
    kafka_bootstrap_servers: str = ""
    execution_topic: str = EXECUTION_REQUESTED_EVENT
    execution_result_topic: str = EXECUTION_COMPLETED_EVENT
    llm_gateway_access_key: str = ""
    public_base_url: str = ""
    policy_signing_key: str = ""
    standby_mode: bool = False
    admission_sustained_per_second: int = 1000
    admission_burst_per_second: int = 3000
    admission_burst_seconds: int = 60
    admission_max_in_flight: int = 10_000

    def validate_runtime(self) -> None:
        missing = [
            name
            for name, value in {
                "DATABASE_URL": self.database_url,
                "PARTITION_COUNT": self.partition_count if self.partition_count >= 1 else None,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"Agent Gateway configuration is incomplete: {', '.join(missing)}")


class AgentExecutionSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: UUID
    application_id: UUID
    environment: str = Field(pattern=r"^(DEVELOPMENT|STAGING|PRODUCTION)$")
    session_id: str = Field(min_length=1, max_length=512)
    subject_id: str | None = Field(default=None, min_length=1, max_length=256)
    memory_policy_version: str = Field(default="policy:none", min_length=1, max_length=128)
    messages: list[dict[str, str]] = Field(min_length=1, max_length=200)
    message_id: str | None = Field(default=None, min_length=1, max_length=256)
    channel_context: dict[str, str] | None = None
    trace_parent: str | None = Field(default=None, min_length=1, max_length=128)


class AgentExecutionAccepted(BaseModel):
    model_config = ConfigDict(frozen=True)

    execution_id: UUID
    release_id: UUID
    session_id: str
    deduplicated: bool = False


def _payload_hash(submission: AgentExecutionSubmission) -> str:
    """Hash the business payload so one message id cannot carry two payloads."""

    canonical = json.dumps(
        {
            "tenant_id": str(submission.tenant_id),
            "application_id": str(submission.application_id),
            "environment": submission.environment,
            "session_id": submission.session_id,
            "subject_id": submission.subject_id,
            "memory_policy_version": submission.memory_policy_version,
            "messages": submission.messages,
            "channel_context": submission.channel_context,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class AgentExecutionSubmitter:
    """Commits the execution business state and its Outbox record in one transaction."""

    def __init__(
        self,
        database: Database,
        deployments: DatabaseDeploymentRouteResolver,
        capacity_policy: CapacityPolicy | None = None,
    ):
        self._database = database
        self._deployments = deployments
        self._capacity_policy = capacity_policy or CapacityPolicy()

    async def _deduplicated_submission(
        self,
        connection: Connection,
        submission: AgentExecutionSubmission,
        message_id: str,
        payload_hash: str,
        release_id: str,
    ) -> AgentExecutionAccepted | None:
        existing = await connection.fetchrow(
            """SELECT id,payload_hash FROM tenant.agent_execution
            WHERE tenant_id=$1 AND message_id=$2""",
            submission.tenant_id,
            message_id,
        )
        if existing is None:
            return None
        if str(existing["payload_hash"]) != payload_hash:
            raise AgentGatewayError("MESSAGE_PAYLOAD_CONFLICT")
        return AgentExecutionAccepted(
            execution_id=existing["id"],
            release_id=UUID(release_id),
            session_id=submission.session_id,
            deduplicated=True,
        )

    async def submit(self, submission: AgentExecutionSubmission) -> AgentExecutionAccepted:
        release_id = await self._deployments.resolve(
            str(submission.tenant_id),
            str(submission.application_id),
            submission.environment,
            submission.session_id,
        )
        if release_id is None:
            raise AgentGatewayError("DEPLOYMENT_NOT_FOUND")
        message_id = submission.message_id or str(uuid7())
        payload_hash = _payload_hash(submission)
        async with self._database.tenant_transaction(submission.tenant_id) as connection:
            classification_row = await connection.fetchrow(
                """SELECT id,data_classification FROM tenant.agent_release
                WHERE tenant_id=$1 AND id=$2""",
                submission.tenant_id,
                UUID(release_id),
            )
            if classification_row is None:
                raise AgentGatewayError("DEPLOYMENT_NOT_FOUND")
            duplicate = await self._deduplicated_submission(
                connection, submission, message_id, payload_hash, release_id
            )
            if duplicate is not None:
                return duplicate
            admission = str(
                await connection.fetchval(
                    "SELECT platform.try_admit_execution($1, $2, $3, $4)",
                    self._capacity_policy.max_in_flight,
                    self._capacity_policy.sustained_per_second,
                    self._capacity_policy.burst_per_second,
                    self._capacity_policy.burst_seconds,
                )
            )
            if admission != "ALLOWED":
                # The capacity lock can have waited for a concurrent duplicate
                # submission to commit. Re-check idempotency before shedding it.
                duplicate = await self._deduplicated_submission(
                    connection, submission, message_id, payload_hash, release_id
                )
                if duplicate is not None:
                    return duplicate
                raise AgentGatewayError(admission)
            await create_session_if_missing(
                connection, submission.tenant_id, submission.application_id, submission.session_id
            )
            execution_id = await connection.fetchval(
                """INSERT INTO tenant.agent_execution
                (tenant_id,id,application_id,release_id,environment,session_id,subject_id,
                memory_policy_version,message_id,payload_hash)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (tenant_id,message_id) DO NOTHING RETURNING id""",
                submission.tenant_id,
                uuid7(),
                submission.application_id,
                UUID(release_id),
                submission.environment,
                submission.session_id,
                submission.subject_id,
                submission.memory_policy_version,
                message_id,
                payload_hash,
            )
            if execution_id is None:
                duplicate = await self._deduplicated_submission(
                    connection, submission, message_id, payload_hash, release_id
                )
                if duplicate is not None:
                    return duplicate
                raise AgentGatewayError("MESSAGE_PAYLOAD_CONFLICT")
            data = ExecutionRequestedData(
                tenant_id=str(submission.tenant_id),
                application_id=str(submission.application_id),
                execution_id=str(execution_id),
                release_id=release_id,
                environment=submission.environment,
                session_id=submission.session_id,
                messages=submission.messages,
                channel_context=submission.channel_context,
                trace_parent=submission.trace_parent,
            )
            await insert_outbox_record(
                connection,
                tenant_id=str(submission.tenant_id),
                message_id=message_id,
                source=GATEWAY_SOURCE,
                event_type=EXECUTION_REQUESTED_EVENT,
                partition_key=session_partition_key(
                    str(submission.tenant_id), submission.session_id
                ),
                payload_json=json.dumps(data.model_dump()),
                correlation_id=str(execution_id),
                data_classification=str(classification_row["data_classification"]),
            )
            return AgentExecutionAccepted(
                execution_id=execution_id,
                release_id=UUID(release_id),
                session_id=submission.session_id,
            )


def create_app(
    settings: AgentGatewaySettings | None = None,
    *,
    bus: ExecutionBusPublisher | None = None,
) -> FastAPI:
    """Create the data-plane entry that accepts executions through the Outbox."""

    configured = settings or AgentGatewaySettings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        configured.validate_runtime()
        database = Database(configured.database_url)
        await database.open()
        application.state.database = database
        policy = CapacityPolicy(
            sustained_per_second=configured.admission_sustained_per_second,
            burst_per_second=configured.admission_burst_per_second,
            burst_seconds=configured.admission_burst_seconds,
            max_in_flight=configured.admission_max_in_flight,
        )
        application.state.submitter = AgentExecutionSubmitter(
            database,
            DatabaseDeploymentRouteResolver(database),
            policy,
        )
        application.state.admission = AdmissionController(policy)
        policy_resolver = (
            PolicyBundleRulesResolver(
                PolicyBundleService(database, signing_key=configured.policy_signing_key)
            )
            if configured.policy_signing_key
            else None
        )
        runner_runtime = ReleasePinnedRunnerRuntime(
            releases=DatabaseReleaseRouteResolver(database),
            llm_gateway_access_key=configured.llm_gateway_access_key,
            policies=policy_resolver,
        )
        ReleaseProtocolRegistry(
            app=application,
            runtime=runner_runtime,
            deployments=DatabaseDeploymentRouteResolver(database),
            public_base_url=configured.public_base_url,
        )
        managed_bus: KafkaExecutionBus | None = None
        if bus is not None:
            active_bus = bus
        elif configured.kafka_bootstrap_servers:
            managed_bus = KafkaExecutionBus(
                configured.kafka_bootstrap_servers,
                configured.execution_topic,
                event_topics={
                    EXECUTION_COMPLETED_EVENT: configured.execution_result_topic,
                },
            )
            await managed_bus.start()
            active_bus = managed_bus
        else:
            active_bus = InMemoryExecutionBus(partition_count=configured.partition_count)
        dispatcher = OutboxDispatcher(database, active_bus)
        dispatch_task: asyncio.Task[None] | None = None
        if configured.dispatch_interval_seconds > 0:

            async def dispatch_loop() -> None:
                while True:
                    await asyncio.sleep(configured.dispatch_interval_seconds)
                    await dispatcher.dispatch_pending()

            dispatch_task = asyncio.create_task(dispatch_loop())
        try:
            yield
        finally:
            if dispatch_task is not None:
                dispatch_task.cancel()
                with suppress(asyncio.CancelledError):
                    await dispatch_task
            if managed_bus is not None:
                await managed_bus.stop()
            await runner_runtime.close()
            await database.close()

    application = FastAPI(
        title="tRPC-Agent Platform agent-gateway",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    def health() -> RuntimeHealthResponse:
        return RuntimeHealthResponse(
            service="agent-gateway", version=__version__, trpc_agent_version=TRPC_AGENT_VERSION
        )

    @application.get("/health/live", response_model=RuntimeHealthResponse)
    async def live() -> RuntimeHealthResponse:
        return health()

    @application.get("/health/ready", response_model=RuntimeHealthResponse)
    async def ready() -> RuntimeHealthResponse:
        return health()

    @application.post(
        "/internal/v1/agent-executions",
        response_model=AgentExecutionAccepted,
    )
    async def submit_execution(
        submission: AgentExecutionSubmission, response: Response
    ) -> AgentExecutionAccepted:
        if configured.standby_mode:
            raise HTTPException(status_code=503, detail="STANDBY_FENCED")
        controller = cast(AdmissionController, application.state.admission)
        try:
            controller.admit()
        except AdmissionDenied as error:
            raise HTTPException(status_code=429, detail=error.reason) from error
        inbound = current_traceparent()
        if inbound is not None and submission.trace_parent is None:
            submission = submission.model_copy(update={"trace_parent": inbound})
        submitter = cast(AgentExecutionSubmitter, application.state.submitter)
        try:
            accepted = await submitter.submit(submission)
        except AgentGatewayError as error:
            if error.code in {"INFLIGHT_SATURATED", "RATE_EXCEEDED"}:
                controller = cast(AdmissionController, application.state.admission)
                ADMISSION_DECISIONS.labels(
                    service=controller.service, decision="DENIED", reason=error.code
                ).inc()
            status_code = 429 if error.code in {"INFLIGHT_SATURATED", "RATE_EXCEEDED"} else 409
            raise HTTPException(status_code=status_code, detail=error.code) from error
        response.status_code = 200 if accepted.deduplicated else 202
        return accepted

    @application.get("/internal/v1/capacity", response_model=dict[str, str | int])
    async def capacity_status() -> dict[str, str | int]:
        controller = cast(AdmissionController, application.state.admission)
        policy = controller.policy
        database = cast(Database, application.state.database)
        async with database.transaction() as connection:
            pending_executions = int(
                await connection.fetchval("SELECT platform.pending_execution_count()")
            )
            pending_outbox_records = int(
                await connection.fetchval(
                    """SELECT count(*) FROM platform.outbox_record
                    WHERE status='PENDING' AND event_type=$1""",
                    EXECUTION_REQUESTED_EVENT,
                )
            )
        return {
            "sustained_per_second": policy.sustained_per_second,
            "burst_per_second": policy.burst_per_second,
            "max_in_flight": policy.max_in_flight,
            "in_flight": pending_executions,
            "pending_executions": pending_executions,
            "pending_outbox_records": pending_outbox_records,
            "shed_level": shed_level(pending_executions, pending_outbox_records, policy),
        }

    register_degradations_endpoint(application)
    install_telemetry(application, "agent-gateway")
    return application


app = create_app()
