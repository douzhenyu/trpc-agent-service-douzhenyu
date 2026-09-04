"""Unit tests for versioned tool registration, MCP capabilities and side-effect governance."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from tests.unit.test_agent_runner_runtime import (
    PINNED_TRPC_AGENT_VERSION,
    ReleasePinnedRunnerRuntime,
    RunnerExecutionCommand,
    StaticReleaseResolver,
    _route,
    _start_fake_llm,
)
from trpc_service.governance import DataClassification
from trpc_service.tool.executor import (
    ToolBackendResult,
    ToolInvocationError,
    ToolInvocationRecord,
    ToolInvocationService,
    ToolInvocationStatus,
    canonical_params_hash,
)
from trpc_service.tool.registry import (
    ToolDefinition,
    ToolRegistry,
    ToolSideEffect,
    ToolSource,
    validate_params,
)


def _definition(
    tenant_id: str = "t-1",
    name: str = "crm_lookup",
    version: int = 1,
    side_effect: ToolSideEffect = ToolSideEffect.READ_ONLY,
    scopes: tuple[str, ...] = ("crm:read",),
    supports_idempotency: bool = False,
    classification: DataClassification = DataClassification.INTERNAL,
) -> ToolDefinition:
    return ToolDefinition(
        tenant_id=tenant_id,
        name=name,
        version=version,
        description="Look up a CRM account.",
        side_effect=side_effect,
        input_schema={
            "type": "object",
            "properties": {"account_id": {"type": "string"}},
            "required": ["account_id"],
        },
        output_schema={"type": "object"},
        scopes=scopes,
        timeout_seconds=5,
        cost_per_call_micros=120,
        data_classification=classification,
        supports_idempotency=supports_idempotency,
    )


class ScriptedBackend:
    """Fake tool backend returning scripted outcomes per attempt."""

    def __init__(self, outcomes: list[ToolBackendResult]) -> None:
        self._outcomes = outcomes
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(
        self, definition: ToolDefinition, params: dict[str, Any]
    ) -> ToolBackendResult:
        self.calls.append((definition.name, dict(params)))
        if len(self.calls) > len(self._outcomes):
            raise AssertionError("backend invoked more often than scripted")
        return self._outcomes[len(self.calls) - 1]


class MemoryStore:
    def __init__(self) -> None:
        self.records: list[ToolInvocationRecord] = []

    async def find_replay(
        self, tenant_id: str, idempotency_key: str
    ) -> ToolInvocationRecord | None:
        for record in self.records:
            if (
                record.tenant_id == tenant_id
                and record.idempotency_key == idempotency_key
                and record.status == ToolInvocationStatus.SUCCEEDED
            ):
                return record
        return None

    async def record(self, record: ToolInvocationRecord) -> None:
        self.records.append(record)


def _service(backend: ScriptedBackend, store: MemoryStore | None = None) -> ToolInvocationService:
    registry = ToolRegistry.in_memory()
    registry.register(_definition())
    return ToolInvocationService(registry, backend, store or MemoryStore(), retry_backoff_seconds=0)


def test_definition_requires_supported_side_effect_and_versions() -> None:
    with pytest.raises(ValueError, match="version"):
        _definition(version=0)
    with pytest.raises(ValueError, match="side_effect"):
        ToolDefinition(
            tenant_id="t-1",
            name="bad",
            version=1,
            description="x",
            side_effect="SOMETIMES",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            data_classification=DataClassification.INTERNAL,
        )


def test_validate_params_blocks_unknown_missing_and_mistyped() -> None:
    definition = _definition()
    assert validate_params(definition, {"account_id": "a-1"}) == {"account_id": "a-1"}
    with pytest.raises(ToolInvocationError, match="TOOL_PARAMS_INVALID"):
        validate_params(definition, {"account_id": "a-1", "extra": True})
    with pytest.raises(ToolInvocationError, match="TOOL_PARAMS_INVALID"):
        validate_params(definition, {})
    with pytest.raises(ToolInvocationError, match="TOOL_PARAMS_INVALID"):
        validate_params(definition, {"account_id": 7})


def test_canonical_params_hash_is_key_order_insensitive() -> None:
    assert canonical_params_hash({"a": 1, "b": 2}) == canonical_params_hash({"b": 2, "a": 1})


def test_read_only_retries_transient_failures_then_succeeds() -> None:
    backend = ScriptedBackend(
        [
            ToolBackendResult(ok=False, result=None, error_code="TOOL_TIMEOUT", transient=True),
            ToolBackendResult(ok=False, result=None, error_code="UPSTREAM_503", transient=True),
            ToolBackendResult(ok=True, result={"name": "Acme"}, error_code=None, transient=False),
        ]
    )
    result = asyncio.run(
        _service(backend).invoke(
            tenant_id="t-1",
            tool_name="crm_lookup",
            params={"account_id": "a-1"},
            scopes=frozenset({"crm:read"}),
            execution_id="e-1",
            requested_by="subject-1",
        )
    )
    assert result.status == ToolInvocationStatus.SUCCEEDED
    assert len(backend.calls) == 3
    assert result.record.attempts == 3
    assert result.record.cost_micros == 120


def test_read_only_permanent_failure_fails_without_retry() -> None:
    backend = ScriptedBackend(
        [ToolBackendResult(ok=False, result=None, error_code="TOOL_DENIED", transient=False)]
    )
    result = asyncio.run(
        _service(backend).invoke(
            tenant_id="t-1",
            tool_name="crm_lookup",
            params={"account_id": "a-1"},
            scopes=frozenset({"crm:read"}),
            requested_by="subject-1",
        )
    )
    assert result.status == ToolInvocationStatus.FAILED
    assert result.record.error_code == "TOOL_DENIED"
    assert len(backend.calls) == 1


def test_idempotent_write_retries_only_with_downstream_idempotency_key() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(
        _definition(
            name="crm_tag",
            side_effect=ToolSideEffect.IDEMPOTENT_WRITE,
            scopes=(),
            supports_idempotency=True,
        )
    )
    backend = ScriptedBackend(
        [
            ToolBackendResult(ok=False, result=None, error_code="TOOL_TIMEOUT", transient=True),
            ToolBackendResult(ok=True, result={"tagged": True}, error_code=None, transient=False),
        ]
    )
    store = MemoryStore()
    service = ToolInvocationService(registry, backend, store, retry_backoff_seconds=0)
    result = asyncio.run(
        service.invoke(
            tenant_id="t-1",
            tool_name="crm_tag",
            params={"account_id": "a-1"},
            scopes=frozenset(),
            idempotency_key="key-1",
            requested_by="subject-1",
        )
    )
    assert result.status == ToolInvocationStatus.SUCCEEDED
    assert len(backend.calls) == 2

    without_key_backend = ScriptedBackend(
        [ToolBackendResult(ok=False, result=None, error_code="TOOL_TIMEOUT", transient=True)]
    )
    service_without_key = ToolInvocationService(
        registry, without_key_backend, MemoryStore(), retry_backoff_seconds=0
    )
    uncertain = asyncio.run(
        service_without_key.invoke(
            tenant_id="t-1",
            tool_name="crm_tag",
            params={"account_id": "a-1"},
            scopes=frozenset(),
            requested_by="subject-1",
        )
    )
    assert uncertain.status == ToolInvocationStatus.OUTCOME_UNKNOWN
    assert len(without_key_backend.calls) == 1


def test_non_idempotent_write_never_blindly_retries() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(
        _definition(
            name="charge_card",
            side_effect=ToolSideEffect.NON_IDEMPOTENT_WRITE,
            scopes=(),
        )
    )
    backend = ScriptedBackend(
        [ToolBackendResult(ok=False, result=None, error_code="TOOL_TIMEOUT", transient=True)]
    )
    result = asyncio.run(
        ToolInvocationService(registry, backend, MemoryStore(), retry_backoff_seconds=0).invoke(
            tenant_id="t-1",
            tool_name="charge_card",
            params={"account_id": "a-1"},
            scopes=frozenset(),
            idempotency_key="key-2",
            mode="direct",
            requested_by="subject-1",
        )
    )
    assert result.status == ToolInvocationStatus.OUTCOME_UNKNOWN
    assert len(backend.calls) == 1


def test_conversation_mode_blocks_writes_and_high_risk() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(
        _definition(name="charge_card", side_effect=ToolSideEffect.NON_IDEMPOTENT_WRITE, scopes=())
    )
    registry.register(
        _definition(name="wipe_data", side_effect=ToolSideEffect.HIGH_RISK, scopes=())
    )
    backend = ScriptedBackend([])
    service = ToolInvocationService(registry, backend, MemoryStore())
    for name in ("charge_card", "wipe_data"):
        with pytest.raises(ToolInvocationError, match="TOOL_AUTO_EXECUTION_BLOCKED"):
            asyncio.run(
                service.invoke(
                    tenant_id="t-1",
                    tool_name=name,
                    params={"account_id": "a-1"},
                    scopes=frozenset(),
                    requested_by="subject-1",
                )
            )
    assert [record.status for record in store_records(service)] == [
        ToolInvocationStatus.BLOCKED,
        ToolInvocationStatus.BLOCKED,
    ]


def store_records(service: ToolInvocationService) -> list[ToolInvocationRecord]:
    store = service._store
    assert isinstance(store, MemoryStore)
    return store.records


def test_missing_scope_and_cross_tenant_calls_are_blocked() -> None:
    backend = ScriptedBackend([])
    service = _service(backend)
    with pytest.raises(ToolInvocationError, match="TOOL_SCOPE_DENIED"):
        asyncio.run(
            service.invoke(
                tenant_id="t-1",
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset(),
                requested_by="subject-1",
            )
        )
    with pytest.raises(ToolInvocationError, match="TOOL_TENANT_DENIED"):
        asyncio.run(
            service.invoke(
                tenant_id="t-2",
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset({"crm:read"}),
                requested_by="subject-1",
            )
        )
    assert all(record.status == ToolInvocationStatus.BLOCKED for record in store_records(service))


def test_idempotent_replay_returns_recorded_result_and_conflict_is_blocked() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(
        _definition(
            name="crm_tag",
            side_effect=ToolSideEffect.IDEMPOTENT_WRITE,
            scopes=(),
            supports_idempotency=True,
        )
    )
    backend = ScriptedBackend(
        [ToolBackendResult(ok=True, result={"tagged": True}, error_code=None, transient=False)]
    )
    store = MemoryStore()
    service = ToolInvocationService(registry, backend, store)
    first = asyncio.run(
        service.invoke(
            tenant_id="t-1",
            tool_name="crm_tag",
            params={"account_id": "a-1"},
            scopes=frozenset(),
            idempotency_key="dup-1",
            requested_by="subject-1",
        )
    )
    replay = asyncio.run(
        service.invoke(
            tenant_id="t-1",
            tool_name="crm_tag",
            params={"account_id": "a-1"},
            scopes=frozenset(),
            idempotency_key="dup-1",
            requested_by="subject-1",
        )
    )
    assert replay.status == ToolInvocationStatus.SUCCEEDED
    assert replay.replayed is True
    assert replay.record.result == first.record.result
    assert len(backend.calls) == 1

    with pytest.raises(ToolInvocationError, match="TOOL_IDEMPOTENCY_CONFLICT"):
        asyncio.run(
            service.invoke(
                tenant_id="t-1",
                tool_name="crm_tag",
                params={"account_id": "a-OTHER"},
                scopes=frozenset(),
                idempotency_key="dup-1",
                requested_by="subject-1",
            )
        )


def test_registry_resolves_latest_and_pins_versions_per_tenant() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(_definition(version=1))
    registry.register(_definition(version=2, classification=DataClassification.CONFIDENTIAL))
    latest = registry.resolve("t-1", "crm_lookup")
    assert latest is not None and latest.version == 2
    pinned = registry.resolve("t-1", "crm_lookup", version=1)
    assert pinned is not None and pinned.data_classification == DataClassification.INTERNAL
    assert registry.resolve("t-1", "unknown") is None
    assert registry.resolve("t-2", "crm_lookup") is None


def test_mcp_registration_expands_into_versioned_tool_definitions() -> None:
    registry = ToolRegistry.in_memory()
    registered = registry.register_mcp_server(
        tenant_id="t-1",
        server_name="jira-mcp",
        server_version=1,
        tools=[
            _definition(name="jira_search", scopes=(), side_effect=ToolSideEffect.READ_ONLY),
            _definition(
                name="jira_comment",
                scopes=(),
                side_effect=ToolSideEffect.IDEMPOTENT_WRITE,
                supports_idempotency=True,
            ),
        ],
    )
    assert registered == 2
    for name in ("jira_search", "jira_comment"):
        resolved = registry.resolve("t-1", name)
        assert resolved is not None
        assert resolved.source == ToolSource.MCP
        assert resolved.mcp_server == "jira-mcp"
    with pytest.raises(ToolInvocationError, match="TOOL_PARAMS_INVALID"):
        asyncio.run(
            ToolInvocationService(registry, ScriptedBackend([]), MemoryStore()).invoke(
                tenant_id="t-1",
                tool_name="jira_search",
                params={"wrong": "shape"},
                scopes=frozenset(),
                requested_by="subject-1",
            )
        )


def test_mcp_registration_rejects_duplicate_tool_names() -> None:
    registry = ToolRegistry.in_memory()
    with pytest.raises(ValueError, match="duplicate"):
        registry.register_mcp_server(
            tenant_id="t-1",
            server_name="jira-mcp",
            server_version=1,
            tools=[_definition(name="same_name"), _definition(name="same_name")],
        )


def test_governed_tool_executes_through_service_with_execution_context() -> None:
    from trpc_service.tool.sdk_bridge import (
        GovernedTool,
        ToolInvocationContext,
        reset_invocation_context,
        set_invocation_context,
    )

    backend = ScriptedBackend(
        [ToolBackendResult(ok=True, result={"name": "Acme"}, error_code=None, transient=False)]
    )
    service = _service(backend)
    tool = GovernedTool(_definition(), service)
    token = set_invocation_context(
        ToolInvocationContext(
            tenant_id="t-1",
            requested_by="user-1",
            execution_id="e-9",
            session_id="s-9",
            release_id="r-9",
        )
    )
    try:
        response = asyncio.run(tool.governed_invoke({"account_id": "a-1"}))
    finally:
        reset_invocation_context(token)
    assert response == {"status": "SUCCEEDED", "result": {"name": "Acme"}}
    record = store_records(service)[0]
    assert record.execution_id == "e-9"
    assert record.session_id == "s-9"
    assert record.requested_by == "user-1"


def test_governed_tool_reports_blocked_writes_without_raising() -> None:
    from trpc_service.tool.sdk_bridge import GovernedTool

    registry = ToolRegistry.in_memory()
    definition = _definition(
        name="charge_card", side_effect=ToolSideEffect.NON_IDEMPOTENT_WRITE, scopes=()
    )
    registry.register(definition)
    tool = GovernedTool(
        definition, ToolInvocationService(registry, ScriptedBackend([]), MemoryStore())
    )
    response = asyncio.run(tool.governed_invoke({"account_id": "a-1"}))
    assert response == {"status": "BLOCKED", "error": "TOOL_AUTO_EXECUTION_BLOCKED"}


def test_governed_tool_declaration_matches_declared_schema() -> None:
    from trpc_service.tool.sdk_bridge import GovernedTool

    tool = GovernedTool(_definition(), _service(ScriptedBackend([])))
    declaration = tool._get_declaration()
    assert declaration is not None
    assert declaration.name == "crm_lookup"
    assert declaration.parameters is not None


def test_runtime_attaches_governed_tools_and_resolves_them_per_execution() -> None:
    from trpc_service.tool.sdk_bridge import GovernedTool

    class StaticToolResolver:
        def __init__(self) -> None:
            self.resolved: list[str] = []

        async def resolve_tools(self, route):
            self.resolved.append(route.release_id)
            return (_definition(),)

    server, port = _start_fake_llm()
    try:
        tenant_id, release_id = str(uuid4()), str(uuid4())
        route = _route(tenant_id, release_id, f"http://127.0.0.1:{port}/llm/v1/chat/completions")
        backend = ScriptedBackend([])
        registry = ToolRegistry.in_memory()
        registry.register(_definition(tenant_id=tenant_id))
        service = ToolInvocationService(registry, backend, MemoryStore())
        resolver = StaticToolResolver()
        runtime = ReleasePinnedRunnerRuntime(
            releases=StaticReleaseResolver(route),
            llm_gateway_access_key="fake-key",
            installed_version=PINNED_TRPC_AGENT_VERSION,
            tool_invoker=service,
            tool_resolver=resolver,
        )
        agent = runtime.agent_for(route, tools=(_definition(tenant_id=tenant_id),))
        assert any(isinstance(tool, GovernedTool) for tool in getattr(agent, "tools", ()))

        async def run() -> str:
            chunks = []
            async for chunk in runtime.stream(
                RunnerExecutionCommand(
                    tenant_id=tenant_id,
                    application_id=str(uuid4()),
                    execution_id=str(uuid4()),
                    release_id=release_id,
                    session_id="session-runner-tools",
                    user_id="protocol-caller",
                    message="Say hi in one short sentence.",
                ),
                streaming=False,
            ):
                chunks.append(chunk)
            return chunks[-1].reply.content

        reply = asyncio.run(run())
        server.shutdown()
        assert reply == "fake reply"
        assert resolver.resolved == [release_id]
    finally:
        server.server_close()


def test_registry_rejects_bad_schemas_and_version_conflicts() -> None:
    registry = ToolRegistry.in_memory()
    with pytest.raises(ValueError, match="top-level object"):
        registry.register(_definition().model_copy(update={"input_schema": {"type": "string"}}))
    with pytest.raises(ValueError, match="conflict"):
        registry.register(_definition())
        registry.register(_definition().model_copy(update={"cost_per_call_micros": 999}))


def test_registry_latest_definitions_per_tenant() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(_definition(name="aaa", version=1))
    registry.register(_definition(name="aaa", version=2))
    registry.register(_definition(tenant_id="t-2", name="bbb"))
    names = [d.name for d in registry.latest_definitions("t-1")]
    assert names == ["aaa"]
    assert registry.latest_definitions("t-1")[0].version == 2


def test_validate_params_rejects_non_mapping_and_bool_typed_values() -> None:
    definition = _definition()
    with pytest.raises(ToolInvocationError, match="TOOL_PARAMS_INVALID"):
        validate_params(definition, "not-a-dict")  # type: ignore[arg-type]
    with pytest.raises(ToolInvocationError, match="TOOL_PARAMS_INVALID"):
        validate_params(definition, {"account_id": True})


class ExplodingBackend:
    """Backend raising scripted exceptions per attempt."""

    def __init__(self, errors: list[Exception]) -> None:
        self._errors = errors
        self.calls = 0

    async def execute(
        self, definition: ToolDefinition, params: dict[str, Any]
    ) -> ToolBackendResult:
        self.calls += 1
        raise self._errors[min(self.calls - 1, len(self._errors) - 1)]


def test_read_only_exhausts_retries_on_repeated_transport_errors() -> None:
    backend = ExplodingBackend([ConnectionError("reset")])
    result = asyncio.run(
        _service(backend).invoke(
            tenant_id="t-1",
            tool_name="crm_lookup",
            params={"account_id": "a-1"},
            scopes=frozenset({"crm:read"}),
            requested_by="subject-1",
        )
    )
    assert result.status == ToolInvocationStatus.FAILED
    assert result.error_code == "TOOL_BACKEND_UNAVAILABLE"
    assert result.record.attempts == 3


def test_non_idempotent_transport_error_reports_failure_not_unknown() -> None:
    registry = ToolRegistry.in_memory()
    registry.register(
        _definition(name="charge_card", side_effect=ToolSideEffect.NON_IDEMPOTENT_WRITE, scopes=())
    )
    backend = ExplodingBackend([ValueError("bad request")])
    result = asyncio.run(
        ToolInvocationService(registry, backend, MemoryStore(), retry_backoff_seconds=0).invoke(
            tenant_id="t-1",
            tool_name="charge_card",
            params={"account_id": "a-1"},
            scopes=frozenset(),
            mode="direct",
            requested_by="subject-1",
        )
    )
    assert result.status == ToolInvocationStatus.FAILED


def test_async_registry_is_supported_for_database_backends() -> None:
    class AsyncRegistry(ToolRegistry):
        async def resolve(self, tenant_id, name, *, version=None):
            return super().resolve(tenant_id, name, version=version)

        async def has_name(self, name):
            return super().has_name(name)

    registry = AsyncRegistry.in_memory()
    registry.register(_definition())
    service = ToolInvocationService(registry, ScriptedBackend([]), MemoryStore())
    with pytest.raises(ToolInvocationError, match="TOOL_TENANT_DENIED"):
        asyncio.run(
            service.invoke(
                tenant_id="t-other",
                tool_name="crm_lookup",
                params={"account_id": "a-1"},
                scopes=frozenset({"crm:read"}),
                requested_by="subject-1",
            )
        )


def test_governed_tool_run_async_impl_routes_args() -> None:
    from trpc_service.tool.sdk_bridge import GovernedTool

    backend = ScriptedBackend(
        [ToolBackendResult(ok=True, result={"name": "Acme"}, error_code=None, transient=False)]
    )
    service = _service(backend)
    tool = GovernedTool(_definition(), service)
    response = asyncio.run(tool._run_async_impl(tool_context=None, args={"account_id": "a-1"}))
    assert response["status"] == "SUCCEEDED"
    assert response["result"] == {"name": "Acme"}


def test_all_runners_of_one_release_share_the_session_service() -> None:
    server, port = _start_fake_llm()
    try:
        tenant_id, release_id = str(uuid4()), str(uuid4())
        route = _route(tenant_id, release_id, f"http://127.0.0.1:{port}/llm/v1/chat/completions")
        runtime = ReleasePinnedRunnerRuntime(
            releases=StaticReleaseResolver(route),
            llm_gateway_access_key="fake-key",
            installed_version=PINNED_TRPC_AGENT_VERSION,
        )
        with_tools = runtime.runner_for(route, (_definition(tenant_id=tenant_id),))
        without_tools = runtime.runner_for(route)
        assert with_tools is not without_tools
        assert with_tools.session_service is without_tools.session_service
        assert runtime.session_service_for(route) is with_tools.session_service
    finally:
        server.server_close()
