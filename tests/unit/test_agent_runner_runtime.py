from __future__ import annotations

import asyncio
import threading
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from dev.fake_external.server import FakeExternalHandler
from trpc_service.agent.runner import (
    AgentRunnerError,
    ReleasePinnedRunnerRuntime,
    RunnerExecutionCommand,
    RunnerExecutionReply,
    openai_base_url,
)
from trpc_service.agent_worker import ReleaseRoute
from trpc_service.llm_gateway import DataClassification, ModelProfile
from trpc_service.version import PINNED_TRPC_AGENT_VERSION


def _start_fake_llm() -> tuple[ThreadingHTTPServer, int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeExternalHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def _route(tenant_id: str, release_id: str, endpoint_url: str) -> ReleaseRoute:
    profile = ModelProfile(
        tenant_id=tenant_id,
        alias="primary-alias",
        provider_model="gpt-test",
        endpoint_url=endpoint_url,
        secret_ref=f"vault://tenant/{tenant_id}/llm#primary",
        data_classification=DataClassification.CONFIDENTIAL,
        region="cn-test",
        fallback_aliases=(),
        requests_per_minute=60,
    )
    return ReleaseRoute(
        release_id=release_id,
        tenant_id=tenant_id,
        model_alias="primary-alias",
        data_classification=DataClassification.CONFIDENTIAL,
        region="cn-test",
        allowed_fallback_aliases=frozenset(),
        profile_snapshots=(profile,),
        instructions="Answer in one short sentence.",
    )


class StaticReleaseResolver:
    """Release resolver serving one fixed route, for unit tests."""

    def __init__(self, route: ReleaseRoute | None) -> None:
        self._route = route

    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None:
        if self._route is None or release_id != self._route.release_id:
            return None
        return self._route


def test_openai_base_url_strips_completions_suffix() -> None:
    assert (
        openai_base_url("http://fake-external:8090/v1/chat/completions")
        == "http://fake-external:8090/v1"
    )
    assert openai_base_url("http://fake-external:8090/v1") == "http://fake-external:8090/v1"


def test_runtime_blocks_unreviewed_sdk_versions() -> None:
    with pytest.raises(RuntimeError, match="unsupported trpc-agent-py runtime"):
        ReleasePinnedRunnerRuntime(releases=StaticReleaseResolver(None), installed_version="1.2.0")


def test_runtime_reports_pinned_sdk_version() -> None:
    runtime = ReleasePinnedRunnerRuntime(
        releases=StaticReleaseResolver(None), installed_version=PINNED_TRPC_AGENT_VERSION
    )
    assert runtime.sdk_version == PINNED_TRPC_AGENT_VERSION
    assert runtime.platform_version


def test_runtime_completes_fake_llm_agent_with_canonical_identifiers() -> None:
    server, port = _start_fake_llm()
    try:
        tenant_id, release_id = str(uuid4()), str(uuid4())
        runtime = ReleasePinnedRunnerRuntime(
            releases=StaticReleaseResolver(
                _route(tenant_id, release_id, f"http://127.0.0.1:{port}/llm/v1/chat/completions")
            ),
            llm_gateway_access_key="fake-key",
            installed_version=PINNED_TRPC_AGENT_VERSION,
        )
        execution_id = str(uuid4())
        command = RunnerExecutionCommand(
            tenant_id=tenant_id,
            application_id=str(uuid4()),
            execution_id=execution_id,
            release_id=release_id,
            session_id="session-runner-1",
            user_id="protocol-caller",
            message="Say hi in one short sentence.",
        )

        async def run() -> RunnerExecutionReply:
            return await runtime.complete(command)

        reply = asyncio.run(run())
        server.shutdown()
        assert isinstance(reply, RunnerExecutionReply)
        assert reply.content == "fake reply"
        assert reply.execution_id == execution_id
        assert reply.session_id == "session-runner-1"
        assert reply.release_id == release_id
        assert reply.tenant_id == tenant_id
        assert reply.model_alias == "primary-alias"
        assert reply.invocation_id.startswith("e-")
        assert reply.trace_id == reply.invocation_id
        assert reply.sdk_version == PINNED_TRPC_AGENT_VERSION
    finally:
        server.server_close()


def test_runtime_streams_deltas_then_final_reply() -> None:
    server, port = _start_fake_llm()
    try:
        tenant_id, release_id = str(uuid4()), str(uuid4())
        runtime = ReleasePinnedRunnerRuntime(
            releases=StaticReleaseResolver(
                _route(tenant_id, release_id, f"http://127.0.0.1:{port}/llm/v1/chat/completions")
            ),
            llm_gateway_access_key="fake-key",
            installed_version=PINNED_TRPC_AGENT_VERSION,
        )
        command = RunnerExecutionCommand(
            tenant_id=tenant_id,
            application_id=str(uuid4()),
            execution_id=str(uuid4()),
            release_id=release_id,
            session_id="session-runner-2",
            user_id="protocol-caller",
            message="Say hi in one short sentence.",
        )

        async def run() -> list[str]:
            kinds: list[str] = []
            async for chunk in runtime.stream(command, streaming=True):
                kinds.append(chunk.kind)
                if chunk.kind == "final":
                    assert chunk.reply is not None
                    assert chunk.reply.content == "fake reply"
            return kinds

        kinds = asyncio.run(run())
        server.shutdown()
        assert kinds[0] == "delta"
        assert kinds[-1] == "final"
    finally:
        server.server_close()


def test_runtime_fails_closed_for_unknown_release() -> None:
    runtime = ReleasePinnedRunnerRuntime(
        releases=StaticReleaseResolver(None), installed_version=PINNED_TRPC_AGENT_VERSION
    )

    async def run() -> None:
        await runtime.complete(
            RunnerExecutionCommand(
                tenant_id=str(uuid4()),
                application_id=str(uuid4()),
                execution_id=str(uuid4()),
                release_id=str(uuid4()),
                session_id="session-runner-3",
                user_id="protocol-caller",
                message="hello",
            )
        )

    with pytest.raises(AgentRunnerError, match="RELEASE_NOT_FOUND"):
        asyncio.run(run())
