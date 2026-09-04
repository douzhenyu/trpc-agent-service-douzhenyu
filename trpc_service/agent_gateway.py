"""Agent Gateway data-plane entry committing executions through the transactional Outbox.

The gateway resolves the environment Deployment to a fixed Agent Release, then
commits the Agent Execution business state and the Outbox record in a single
PostgreSQL transaction. The Outbox dispatcher publishes to the execution bus
afterwards; a duplicate submission dedupes on the message id and never produces
a second execution.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.admin_api.database import Database
from trpc_service.agent_worker import DatabaseDeploymentRouteResolver
from trpc_service.execution_bus import (
    EXECUTION_REQUESTED_EVENT,
    GATEWAY_SOURCE,
    ExecutionBusPublisher,
    InMemoryExecutionBus,
    OutboxDispatcher,
    session_partition_key,
)
from trpc_service.ids import uuid7
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


class SessionExecutionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: UUID
    application_id: UUID
    environment: str = Field(pattern=r"^(DEVELOPMENT|STAGING|PRODUCTION)$")
    session_id: str = Field(min_length=1, max_length=512)
    messages: list[dict[str, str]] = Field(min_length=1, max_length=200)
    message_id: str | None = Field(default=None, min_length=1, max_length=256)


class SessionExecutionAccepted(BaseModel):
    model_config = ConfigDict(frozen=True)

    execution_id: UUID
    release_id: UUID
    session_id: str
    deduplicated: bool = False


class SessionExecutionSubmitter:
    """Commits the execution business state and its Outbox record in one transaction."""

    def __init__(self, database: Database, deployments: DatabaseDeploymentRouteResolver):
        self._database = database
        self._deployments = deployments

    async def submit(self, payload: SessionExecutionPayload) -> SessionExecutionAccepted:
        release_id = await self._deployments.resolve(
            str(payload.tenant_id),
            str(payload.application_id),
            payload.environment,
            payload.session_id,
        )
        if release_id is None:
            raise AgentGatewayError("DEPLOYMENT_NOT_FOUND")
        message_id = payload.message_id or str(uuid7())
        async with self._database.tenant_transaction(payload.tenant_id) as connection:
            await create_session_if_missing(
                connection, payload.tenant_id, payload.application_id, payload.session_id
            )
            execution_id = await connection.fetchval(
                """INSERT INTO tenant.agent_execution
                (tenant_id,id,application_id,release_id,environment,session_id,message_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (tenant_id,message_id) DO NOTHING RETURNING id""",
                payload.tenant_id,
                uuid7(),
                payload.application_id,
                UUID(release_id),
                payload.environment,
                payload.session_id,
                message_id,
            )
            if execution_id is None:
                execution_id = await connection.fetchval(
                    """SELECT id FROM tenant.agent_execution
                    WHERE tenant_id=$1 AND message_id=$2""",
                    payload.tenant_id,
                    message_id,
                )
                assert execution_id is not None
                return SessionExecutionAccepted(
                    execution_id=execution_id,
                    release_id=UUID(release_id),
                    session_id=payload.session_id,
                    deduplicated=True,
                )
            await connection.execute(
                """INSERT INTO platform.outbox_record
                (tenant_id,id,message_id,source,event_type,partition_key,payload)
                VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS jsonb))""",
                payload.tenant_id,
                uuid7(),
                message_id,
                GATEWAY_SOURCE,
                EXECUTION_REQUESTED_EVENT,
                session_partition_key(str(payload.tenant_id), payload.session_id),
                json.dumps(
                    {
                        "tenant_id": str(payload.tenant_id),
                        "application_id": str(payload.application_id),
                        "execution_id": str(execution_id),
                        "release_id": release_id,
                        "environment": payload.environment,
                        "session_id": payload.session_id,
                        "messages": payload.messages,
                    }
                ),
            )
            return SessionExecutionAccepted(
                execution_id=execution_id,
                release_id=UUID(release_id),
                session_id=payload.session_id,
            )


def create_app(
    settings: AgentGatewaySettings | None = None,
    *,
    database: Database | None = None,
    submitter: SessionExecutionSubmitter | None = None,
    bus: ExecutionBusPublisher | None = None,
    dispatcher: OutboxDispatcher | None = None,
) -> FastAPI:
    """Create the data-plane entry that accepts executions through the Outbox."""

    configured = settings or AgentGatewaySettings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        if submitter is not None:
            application.state.submitter = submitter
            yield
            return
        configured.validate_runtime()
        active_database = database or Database(configured.database_url)
        if database is None:
            await active_database.open()
        active_submitter = SessionExecutionSubmitter(
            active_database,
            DatabaseDeploymentRouteResolver(active_database),
        )
        application.state.submitter = active_submitter
        active_bus = bus or InMemoryExecutionBus(partition_count=configured.partition_count)
        active_dispatcher = dispatcher or OutboxDispatcher(active_database, active_bus)
        dispatch_task: asyncio.Task[None] | None = None
        if configured.dispatch_interval_seconds > 0:

            async def dispatch_loop() -> None:
                while True:
                    await asyncio.sleep(configured.dispatch_interval_seconds)
                    await active_dispatcher.dispatch_pending()

            dispatch_task = asyncio.create_task(dispatch_loop())
        try:
            yield
        finally:
            if dispatch_task is not None:
                dispatch_task.cancel()
            if database is None:
                await active_database.close()

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
        "/internal/v1/session-executions",
        response_model=SessionExecutionAccepted,
    )
    async def submit_execution(
        payload: SessionExecutionPayload, response: Response
    ) -> SessionExecutionAccepted:
        active_submitter = cast(SessionExecutionSubmitter, application.state.submitter)
        try:
            accepted = await active_submitter.submit(payload)
        except AgentGatewayError as error:
            raise HTTPException(status_code=409, detail=error.code) from error
        response.status_code = 200 if accepted.deduplicated else 202
        return accepted

    return application
