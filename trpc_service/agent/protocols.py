"""HTTP, SSE, AG-UI and A2A protocol surfaces over release-pinned Runners.

All four surfaces execute through the same deterministic Deployment routing
and the same release-pinned Runner runtime. The HTTP and SSE replies carry the
canonical identifiers (execution, session, release, model, trace, SDK
version); the AG-UI and A2A surfaces are the upstream SDK implementations, so
standard protocol clients can attach unmodified.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from trpc_agent_sdk.server.a2a import AgentCardBuilder, TrpcA2aAgentService
from trpc_agent_sdk.server.ag_ui import AgUiAgent, AgUiService
from trpc_agent_sdk.sessions import InMemorySessionService

from trpc_service.agent.runner import (
    AgentRunnerError,
    ReleasePinnedRunnerRuntime,
    RunnerExecutionReply,
)
from trpc_service.agent_worker import DeploymentRouteResolver, ReleaseRoute
from trpc_service.ids import uuid7

DEFAULT_USER_ID = "platform-user"


class AgentRunnerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: UUID
    application_id: UUID
    environment: str = Field(pattern=r"^(DEVELOPMENT|STAGING|PRODUCTION)$")
    session_id: str = Field(min_length=1, max_length=512)
    user_id: str = Field(default=DEFAULT_USER_ID, min_length=1, max_length=256)
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


def _reply_model(request: AgentRunnerRequest, reply: RunnerExecutionReply) -> AgentRunnerReplyModel:
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


class ReleaseProtocolRegistry:
    """Expose the four protocol surfaces per resolved Agent Release."""

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
        self._ensured: set[str] = set()
        self._ag_ui_service = AgUiService(service_name="agent-gateway", app=app)
        self._register_static_routes()

    async def ensure_release(self, route: ReleaseRoute) -> None:
        """Register the AG-UI endpoint and A2A mount once per release."""

        if route.release_id in self._ensured:
            return
        agent = self._runtime.agent_for(route)
        self._ag_ui_service.add_agent(
            f"{self._prefix}/ag-ui/{route.release_id}",
            AgUiAgent(
                agent,
                app_name=self._runtime_app_name,
                user_id_extractor=_user_id_extractor,
            ),
        )
        card = await AgentCardBuilder(
            agent=agent,
            rpc_url=f"{self._public_base_url}{self._prefix}/a2a/{route.release_id}",
        ).build()
        service = TrpcA2aAgentService(
            service_name="agent-gateway",
            agent=agent,
            app_name=self._runtime_app_name,
            agent_card=card,
            session_service=InMemorySessionService(),
        )
        # The upstream sync initialize() cannot run inside a live event loop;
        # calling the async initializer wires the session service and card
        # capabilities without spawning a nested loop.
        await service._initialize()
        handler = DefaultRequestHandler(agent_executor=service, task_store=InMemoryTaskStore())
        a2a_app = A2AStarletteApplication(agent_card=service.agent_card, http_handler=handler)
        self._app.mount(f"{self._prefix}/a2a/{route.release_id}", a2a_app.build())
        self._ensured.add(route.release_id)

    @property
    def _runtime_app_name(self) -> str:
        return self._runtime._app_name

    async def _resolve_release(self, request: AgentRunnerRequest) -> tuple[str, UUID]:
        release_id = await self._deployments.resolve(
            str(request.tenant_id),
            str(request.application_id),
            request.environment,
            request.session_id,
        )
        if release_id is None:
            raise HTTPException(status_code=409, detail="DEPLOYMENT_NOT_FOUND")
        return release_id, uuid7()

    def _register_static_routes(self) -> None:
        @self._app.post(f"{self._prefix}/completions", response_model=AgentRunnerReplyModel)
        async def runner_completions(
            request: AgentRunnerRequest, response: Response
        ) -> AgentRunnerReplyModel:
            release_id, execution_id = await self._resolve_release(request)
            route = await self._runtime.resolve(str(request.tenant_id), release_id)
            if route is None:
                raise HTTPException(status_code=409, detail="RELEASE_NOT_FOUND")
            await self.ensure_release(route)
            try:
                reply = await self._runtime.complete(
                    tenant_id=str(request.tenant_id),
                    execution_id=str(execution_id),
                    release_id=release_id,
                    session_id=request.session_id,
                    user_id=request.user_id,
                    message=request.message,
                )
            except AgentRunnerError as error:
                raise HTTPException(status_code=409, detail=error.code) from error
            response.status_code = 200
            return _reply_model(request, reply)

        @self._app.post(f"{self._prefix}/stream")
        async def runner_stream(request: AgentRunnerRequest) -> StreamingResponse:
            release_id, execution_id = await self._resolve_release(request)
            route = await self._runtime.resolve(str(request.tenant_id), release_id)
            if route is None:
                raise HTTPException(status_code=409, detail="RELEASE_NOT_FOUND")
            await self.ensure_release(route)

            async def event_stream() -> AsyncIterator[str]:
                try:
                    async for chunk in self._runtime.stream(
                        tenant_id=str(request.tenant_id),
                        execution_id=str(execution_id),
                        release_id=release_id,
                        session_id=request.session_id,
                        user_id=request.user_id,
                        message=request.message,
                        streaming=True,
                    ):
                        if chunk.kind == "delta":
                            yield _sse({"type": "delta", "text": chunk.delta})
                        elif chunk.reply is not None:
                            yield _sse(
                                {
                                    "type": "result",
                                    **_reply_model(request, chunk.reply).model_dump(mode="json"),
                                }
                            )
                except AgentRunnerError as error:
                    yield _sse({"type": "error", "code": error.code})

            return StreamingResponse(event_stream(), media_type="text/event-stream")

    @property
    def ensured_releases(self) -> frozenset[str]:
        return frozenset(self._ensured)


def _user_id_extractor(input_data: Any) -> str:
    forwarded = getattr(input_data, "forwarded_props", None)
    if isinstance(forwarded, dict):
        user_id = forwarded.get("user_id")
        if isinstance(user_id, str) and user_id:
            return user_id
    return DEFAULT_USER_ID


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
