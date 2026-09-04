"""Release-pinned tRPC-Agent Runner runtime for the Agent Gateway data plane.

Every execution is driven by the immutable Agent Release snapshot: the model
comes from the release's model profile snapshot, the instruction from the
release's draft snapshot. The runtime fails construction unless the installed
`trpc-agent-py` matches the locked baseline, and reports the canonical
execution identifiers (execution, session, release, model, invocation, SDK
version) with every reply.

Session state for these protocol executions lives in the SDK's in-memory
session service (process-local cache, shared by all four protocol surfaces).
The platform's authoritative Session Event chain (PostgreSQL) is written by
the bus-driven Agent Worker path (issue #13); routing protocol executions
through that chain is a documented follow-up, not done silently here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any, Literal, cast

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent_worker import ReleaseRoute, ReleaseRouteResolver
from trpc_service.version import __version__, require_pinned_trpc_agent_version

OPENAI_COMPLETIONS_SUFFIX = "/chat/completions"

RunnerStreamKind = Literal["delta", "final"]


class AgentRunnerError(RuntimeError):
    """Safe, stable error for callers; never embeds SDK or provider details."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def openai_base_url(endpoint_url: str) -> str:
    """Normalize a stored provider endpoint into an OpenAI client base URL."""

    if endpoint_url.endswith(OPENAI_COMPLETIONS_SUFFIX):
        return endpoint_url[: -len(OPENAI_COMPLETIONS_SUFFIX)]
    return endpoint_url


@dataclass(frozen=True)
class RunnerExecutionCommand:
    """Everything one release-pinned execution needs; no more, no less."""

    tenant_id: str
    application_id: str
    execution_id: str
    release_id: str
    session_id: str
    user_id: str
    message: str


@dataclass(frozen=True)
class RunnerExecutionReply:
    """Canonical identifiers and content of one release-pinned execution."""

    tenant_id: str
    execution_id: str
    session_id: str
    release_id: str
    model_alias: str
    invocation_id: str
    sdk_version: str
    platform_version: str
    content: str

    @property
    def trace_id(self) -> str:
        """The Runner invocation id anchors cross-component tracing (spec 条目9)."""

        return self.invocation_id


@dataclass(frozen=True)
class RunnerStreamChunk:
    """One SSE frame of a streaming execution: a text delta or the final reply."""

    kind: RunnerStreamKind
    delta: str = ""
    reply: RunnerExecutionReply | None = None


def _event_text(event: Any) -> str:
    content = event.content
    if content is None or content.parts is None:
        return ""
    return "".join(str(part.text or "") for part in content.parts)


class ReleasePinnedRunnerRuntime:
    """Build and drive tRPC-Agent Runners from immutable Release snapshots."""

    def __init__(
        self,
        *,
        releases: ReleaseRouteResolver,
        llm_gateway_access_key: str = "",
        app_name: str = "agent-gateway",
        installed_version: str | None = None,
    ) -> None:
        self.sdk_version = require_pinned_trpc_agent_version(
            installed_version if installed_version is not None else version("trpc-agent-py")
        )
        self.platform_version = __version__
        self._releases = releases
        # Dev/test: a dummy key for the Fake LLM. Production: the platform
        # access token the gateway presents to the LLM Gateway's OpenAI-
        # compatible surface, which resolves tenant secret_refs internally.
        # This process never holds provider credentials.
        self._llm_gateway_access_key = llm_gateway_access_key
        self._app_name = app_name
        self._agents: dict[str, LlmAgent] = {}
        self._runners: dict[str, Runner] = {}

    @property
    def app_name(self) -> str:
        return self._app_name

    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None:
        return await self._releases.resolve(tenant_id, release_id)

    def agent_for(self, route: ReleaseRoute) -> LlmAgent:
        """Build (once) the release-pinned LlmAgent for a resolved route."""

        cached = self._agents.get(route.release_id)
        if cached is not None:
            return cached
        profile = route.profile_snapshots[0]
        agent = LlmAgent(
            name=f"release-{route.release_id[:8]}",
            model=OpenAIModel(
                profile.provider_model,
                api_key=self._llm_gateway_access_key,
                base_url=openai_base_url(profile.endpoint_url),
            ),
            instruction=route.instructions,
        )
        self._agents[route.release_id] = agent
        return agent

    def runner_for(self, route: ReleaseRoute) -> Runner:
        """Build (once) the Runner around the release agent; SDK state stays in memory."""

        cached = self._runners.get(route.release_id)
        if cached is not None:
            return cached
        runner = Runner(
            app_name=self._app_name,
            agent=self.agent_for(route),
            session_service=InMemorySessionService(),
        )
        self._runners[route.release_id] = runner
        return runner

    def session_service_for(self, route: ReleaseRoute) -> InMemorySessionService:
        """The shared SDK session cache for a release, reused by all protocol surfaces."""

        return cast(InMemorySessionService, self.runner_for(route).session_service)

    async def complete(self, command: RunnerExecutionCommand) -> RunnerExecutionReply:
        """Execute one release-pinned run and return the full reply."""

        async for chunk in self.stream(command, streaming=False):
            if chunk.reply is not None:
                return chunk.reply
        raise AgentRunnerError("RUNNER_EMPTY_REPLY")

    async def stream(
        self, command: RunnerExecutionCommand, *, streaming: bool = True
    ) -> AsyncIterator[RunnerStreamChunk]:
        """Yield text deltas and the final canonical reply for one execution."""

        route = await self._releases.resolve(command.tenant_id, command.release_id)
        if route is None:
            raise AgentRunnerError("RELEASE_NOT_FOUND")
        runner = self.runner_for(route)
        await self._ensure_sdk_session(runner, command.user_id, command.session_id)
        new_message = Content(role="user", parts=[Part(text=command.message)])
        emitted = ""
        last_text = ""
        invocation_id = ""
        async for event in runner.run_async(
            user_id=command.user_id,
            session_id=command.session_id,
            new_message=new_message,
            run_config=RunConfig(streaming=streaming),
        ):
            invocation_id = str(event.invocation_id) or invocation_id
            text = _event_text(event)
            if text:
                # Consume the whole run: multi-step executions (tools, rounds)
                # emit several text events and the reply is the last one.
                last_text = text
            if event.partial:
                if text.startswith(emitted):
                    delta = text[len(emitted) :]
                    if delta:
                        emitted = text
                        yield RunnerStreamChunk(kind="delta", delta=delta)
                continue
        if last_text:
            yield RunnerStreamChunk(
                kind="final",
                reply=self._reply(
                    route=route,
                    command=command,
                    invocation_id=invocation_id,
                    content=last_text,
                ),
            )
            return
        raise AgentRunnerError("RUNNER_EMPTY_REPLY")

    async def close(self) -> None:
        for runner in self._runners.values():
            await runner.close()
        self._runners.clear()
        self._agents.clear()

    def _reply(
        self,
        *,
        route: ReleaseRoute,
        command: RunnerExecutionCommand,
        invocation_id: str,
        content: str,
    ) -> RunnerExecutionReply:
        return RunnerExecutionReply(
            tenant_id=route.tenant_id,
            execution_id=command.execution_id,
            session_id=command.session_id,
            release_id=route.release_id,
            model_alias=route.model_alias,
            invocation_id=invocation_id or command.execution_id,
            sdk_version=self.sdk_version,
            platform_version=self.platform_version,
            content=content,
        )

    async def _ensure_sdk_session(self, runner: Runner, user_id: str, session_id: str) -> None:
        service = cast(InMemorySessionService, runner.session_service)
        existing = await service.get_session(
            app_name=self._app_name, user_id=user_id, session_id=session_id
        )
        if existing is None:
            await service.create_session(
                app_name=self._app_name, user_id=user_id, session_id=session_id, state={}
            )
