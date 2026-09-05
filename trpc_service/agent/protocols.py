"""HTTP, SSE, AG-UI and A2A protocol surfaces over release-pinned Runners.

All four surfaces resolve the environment Deployment deterministically and
execute through the same release-pinned Runner runtime with one shared SDK
session cache per release. The HTTP and SSE replies carry the canonical
identifiers (execution, session, release, model, invocation, SDK version);
the AG-UI and A2A endpoints delegate to the upstream SDK implementations, so
standard protocol clients attach unmodified. Statuses of recent executions
are queryable by execution id.
"""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from ag_ui.core.types import RunAgentInput
from ag_ui.encoder import EventEncoder
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.responses import Response as FastAPIResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.requests import Request
from trpc_agent_sdk.server.a2a import AgentCardBuilder, TrpcA2aAgentService
from trpc_agent_sdk.server.ag_ui import AgUiAgent

from trpc_service.agent.runner import (
    AgentRunnerError,
    ReleasePinnedRunnerRuntime,
    RunnerExecutionCommand,
    RunnerExecutionReply,
)
from trpc_service.agent_worker import DeploymentRouteResolver, ReleaseRoute
from trpc_service.ids import uuid7

DEFAULT_PROTOCOL_USER = "protocol-caller"
AGENT_CARD_SUFFIX = "/.well-known/agent-card.json"
_STATUS_CACHE_LIMIT = 512


class AgentRunnerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: UUID
    application_id: UUID
    environment: str = Field(pattern=r"^(DEVELOPMENT|STAGING|PRODUCTION)$")
    session_id: str = Field(min_length=1, max_length=512)
    user_id: str = Field(default=DEFAULT_PROTOCOL_USER, min_length=1, max_length=256)
    message: str = Field(min_length=1, max_length=20000)


class AgentRunnerReplyModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    execution_id: UUID
    session_id: str
    release_id: UUID
    model_alias: str
    trace_id: str
    sdk_version: str
    platform_version: str
    content: str


def _reply_model(reply: RunnerExecutionReply) -> AgentRunnerReplyModel:
    return AgentRunnerReplyModel(
        tenant_id=UUID(reply.tenant_id),
        execution_id=UUID(reply.execution_id),
        session_id=reply.session_id,
        release_id=UUID(reply.release_id),
        model_alias=reply.model_alias,
        trace_id=reply.trace_id,
        sdk_version=reply.sdk_version,
        platform_version=reply.platform_version,
        content=reply.content,
    )


def _user_id_extractor(input_data: Any) -> str:
    forwarded = getattr(input_data, "forwarded_props", None)
    if isinstance(forwarded, dict):
        user_id = forwarded.get("user_id")
        if isinstance(user_id, str) and user_id:
            return user_id
    return DEFAULT_PROTOCOL_USER


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


class ReleaseProtocolRegistry:
    """Expose the four protocol surfaces for release-pinned executions.

    All routes are static (path parameters carry tenant and release), so
    direct AG-UI and A2A clients work without any warm-up and concurrent
    first requests share one set of caches guarded by an asyncio lock.
    """

    def __init__(
        self,
        *,
        app: FastAPI,
        runtime: ReleasePinnedRunnerRuntime,
        deployments: DeploymentRouteResolver,
        prefix: str = "/internal/v1/agent-runner",
        public_base_url: str = "",
    ) -> None:
        self._app = app
        self._runtime = runtime
        self._deployments = deployments
        self._prefix = prefix
        self._public_base_url = public_base_url.rstrip("/")
        self._lock = asyncio.Lock()
        self._ag_ui_agents: dict[str, AgUiAgent] = {}
        self._a2a_apps: dict[str, A2AStarletteApplication] = {}
        self._statuses: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._register_routes()

    def _record_status(
        self, execution_id: str, status: str, reply: RunnerExecutionReply | None
    ) -> None:
        payload: dict[str, Any] = {"execution_id": execution_id, "status": status}
        if reply is not None:
            payload.update(_reply_model(reply).model_dump(mode="json"))
        self._statuses[execution_id] = payload
        while len(self._statuses) > _STATUS_CACHE_LIMIT:
            self._statuses.popitem(last=False)

    async def _resolve_route(self, tenant_id: UUID, release_id: str) -> ReleaseRoute:
        route = await self._runtime.resolve(str(tenant_id), release_id)
        if route is None:
            raise HTTPException(status_code=409, detail="RELEASE_NOT_FOUND")
        return route

    async def _resolve_command(
        self, request: AgentRunnerRequest
    ) -> tuple[ReleaseRoute, RunnerExecutionCommand]:
        release_id = await self._deployments.resolve(
            str(request.tenant_id),
            str(request.application_id),
            request.environment,
            request.session_id,
        )
        if release_id is None:
            raise HTTPException(status_code=409, detail="DEPLOYMENT_NOT_FOUND")
        route = await self._runtime.resolve(str(request.tenant_id), release_id)
        if route is None:
            raise HTTPException(status_code=409, detail="RELEASE_NOT_FOUND")
        command = RunnerExecutionCommand(
            tenant_id=str(request.tenant_id),
            application_id=str(request.application_id),
            execution_id=str(uuid7()),
            release_id=release_id,
            session_id=request.session_id,
            user_id=request.user_id,
            message=request.message,
        )
        return route, command

    async def _ag_ui_agent_for(self, route: ReleaseRoute) -> AgUiAgent:
        async with self._lock:
            agent = self._ag_ui_agents.get(route.release_id)
            if agent is None:
                agent = AgUiAgent(
                    self._runtime.agent_for(route, await self._runtime.tools_for(route)),
                    app_name=self._runtime.app_name,
                    session_service=self._runtime.session_service_for(route),
                    user_id_extractor=_user_id_extractor,
                )
                self._ag_ui_agents[route.release_id] = agent
            return agent

    async def _a2a_app_for(self, route: ReleaseRoute) -> A2AStarletteApplication:
        async with self._lock:
            a2a_app = self._a2a_apps.get(route.release_id)
            if a2a_app is None:
                agent = self._runtime.agent_for(route, await self._runtime.tools_for(route))
                rpc_url = (
                    f"{self._public_base_url}{self._prefix}"
                    f"/a2a/{route.tenant_id}/{route.release_id}"
                )
                card = await AgentCardBuilder(agent=agent, rpc_url=rpc_url).build()
                service = TrpcA2aAgentService(
                    service_name=self._runtime.app_name,
                    agent=agent,
                    app_name=self._runtime.app_name,
                    agent_card=card,
                    session_service=self._runtime.session_service_for(route),
                )
                # The upstream sync initialize() cannot run inside a live event
                # loop; calling the async initializer wires the session service
                # and card capabilities without spawning a nested loop.
                await service._initialize()
                handler = DefaultRequestHandler(
                    agent_executor=service, task_store=InMemoryTaskStore()
                )
                a2a_app = A2AStarletteApplication(agent_card=card, http_handler=handler)
                self._a2a_apps[route.release_id] = a2a_app
            return a2a_app

    def _register_routes(self) -> None:
        prefix = self._prefix

        @self._app.post(f"{prefix}/completions", response_model=AgentRunnerReplyModel)
        async def runner_completions(
            request: AgentRunnerRequest, response: FastAPIResponse
        ) -> AgentRunnerReplyModel:
            _route, command = await self._resolve_command(request)
            self._record_status(command.execution_id, "RUNNING", None)
            try:
                reply = await self._runtime.complete(command)
            except AgentRunnerError as error:
                raise HTTPException(status_code=409, detail=error.code) from error
            self._record_status(command.execution_id, "SUCCEEDED", reply)
            response.status_code = 200
            return _reply_model(reply)

        @self._app.post(f"{prefix}/stream")
        async def runner_stream(request: AgentRunnerRequest) -> StreamingResponse:
            _route, command = await self._resolve_command(request)
            self._record_status(command.execution_id, "RUNNING", None)

            async def event_stream() -> AsyncIterator[str]:
                try:
                    async for chunk in self._runtime.stream(command, streaming=True):
                        if chunk.kind == "delta":
                            yield _sse({"type": "delta", "text": chunk.delta})
                        elif chunk.reply is not None:
                            self._record_status(command.execution_id, "SUCCEEDED", chunk.reply)
                            yield _sse(
                                {
                                    "type": "result",
                                    **_reply_model(chunk.reply).model_dump(mode="json"),
                                }
                            )
                except AgentRunnerError as error:
                    yield _sse({"type": "error", "code": error.code})

            return StreamingResponse(event_stream(), media_type="text/event-stream")

        @self._app.get(f"{prefix}/statuses/{{execution_id}}")
        async def runner_status(execution_id: UUID) -> dict[str, Any]:
            status = self._statuses.get(str(execution_id))
            if status is None:
                raise HTTPException(status_code=404, detail="EXECUTION_NOT_FOUND")
            return status

        @self._app.post(f"{prefix}/ag-ui/{{tenant_id}}/{{release_id}}")
        async def runner_ag_ui(
            tenant_id: UUID, release_id: UUID, input_data: RunAgentInput, request: Request
        ) -> StreamingResponse:
            route = await self._resolve_route(tenant_id, str(release_id))
            agent = await self._ag_ui_agent_for(route)
            encoder = EventEncoder(accept=request.headers.get("accept") or "")

            async def event_generator() -> AsyncIterator[str]:
                async for event in agent.run(input_data, http_request=request):
                    yield encoder.encode(event)

            return StreamingResponse(event_generator(), media_type=encoder.get_content_type())

        @self._app.get(f"{prefix}/a2a/{{tenant_id}}/{{release_id}}{AGENT_CARD_SUFFIX}")
        async def runner_a2a_card(tenant_id: UUID, release_id: UUID) -> JSONResponse:
            route = await self._resolve_route(tenant_id, str(release_id))
            a2a_app = await self._a2a_app_for(route)
            card = a2a_app.agent_card
            assert card is not None
            return JSONResponse(card.model_dump(exclude_none=True, mode="json"))

        @self._app.post(f"{prefix}/a2a/{{tenant_id}}/{{release_id}}")
        async def runner_a2a_rpc(tenant_id: UUID, release_id: UUID, request: Request) -> Any:
            route = await self._resolve_route(tenant_id, str(release_id))
            a2a_app = await self._a2a_app_for(route)
            # Delegate to the upstream JSON-RPC dispatcher; it parses the body
            # and returns JSON or SSE responses per the A2A specification.
            return await a2a_app._handle_requests(request)
