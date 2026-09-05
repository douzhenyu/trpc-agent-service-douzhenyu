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
from contextlib import asynccontextmanager
from typing import cast
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.admin_api.database import Database
from trpc_service.agent.protocols import ReleaseProtocolRegistry
from trpc_service.agent.runner import ReleasePinnedRunnerRuntime
from trpc_service.agent_worker import (
    DatabaseDeploymentRouteResolver,
    DatabaseReleaseRouteResolver,
)
from trpc_service.execution_bus import (
    EXECUTION_REQUESTED_EVENT,
    GATEWAY_SOURCE,
    ExecutionBusPublisher,
    ExecutionRequestedData,
    InMemoryExecutionBus,
    OutboxDispatcher,
    insert_outbox_record,
    session_partition_key,
)
from trpc_service.ids import uuid7
from trpc_service.policy_bundles import PolicyBundleRulesResolver, PolicyBundleService
from trpc_service.runtime_health import RuntimeHealthResponse
from trpc_service.sessions import create_session_if_missing
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
    llm_gateway_access_key: str = ""
    public_base_url: str = ""
    policy_signing_key: str = ""

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
    messages: list[dict[str, str]] = Field(min_length=1, max_length=200)
    message_id: str | None = Field(default=None, min_length=1, max_length=256)


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
            "messages": submission.messages,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class AgentExecutionSubmitter:
    """Commits the execution business state and its Outbox record in one transaction."""

    def __init__(self, database: Database, deployments: DatabaseDeploymentRouteResolver):
        self._database = database
        self._deployments = deployments

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
            await create_session_if_missing(
                connection, submission.tenant_id, submission.application_id, submission.session_id
            )
            execution_id = await connection.fetchval(
                """INSERT INTO tenant.agent_execution
                (tenant_id,id,application_id,release_id,environment,session_id,message_id,
                payload_hash)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (tenant_id,message_id) DO NOTHING RETURNING id""",
                submission.tenant_id,
                uuid7(),
                submission.application_id,
                UUID(release_id),
                submission.environment,
                submission.session_id,
                message_id,
                payload_hash,
            )
            if execution_id is None:
                existing = await connection.fetchrow(
                    """SELECT id,payload_hash FROM tenant.agent_execution
                    WHERE tenant_id=$1 AND message_id=$2""",
                    submission.tenant_id,
                    message_id,
                )
                if existing is None or str(existing["payload_hash"]) != payload_hash:
                    raise AgentGatewayError("MESSAGE_PAYLOAD_CONFLICT")
                return AgentExecutionAccepted(
                    execution_id=existing["id"],
                    release_id=UUID(release_id),
                    session_id=submission.session_id,
                    deduplicated=True,
                )
            data = ExecutionRequestedData(
                tenant_id=str(submission.tenant_id),
                application_id=str(submission.application_id),
                execution_id=str(execution_id),
                release_id=release_id,
                environment=submission.environment,
                session_id=submission.session_id,
                messages=submission.messages,
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
        application.state.submitter = AgentExecutionSubmitter(
            database,
            DatabaseDeploymentRouteResolver(database),
        )
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
        active_bus = bus or InMemoryExecutionBus(partition_count=configured.partition_count)
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
        submitter = cast(AgentExecutionSubmitter, application.state.submitter)
        try:
            accepted = await submitter.submit(submission)
        except AgentGatewayError as error:
            raise HTTPException(status_code=409, detail=error.code) from error
        response.status_code = 200 if accepted.deduplicated else 202
        return accepted

    return application
