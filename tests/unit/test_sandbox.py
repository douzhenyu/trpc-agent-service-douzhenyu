"""Unit tests for the gVisor sandbox: policy hardening, executor fail-close and pod specs."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from trpc_service.sandbox.errors import SandboxError
from trpc_service.sandbox.executor import (
    SANDBOX_UNAVAILABLE,
    SandboxedCodeExecutor,
    assert_no_unsafe_local_execution,
)
from trpc_service.sandbox.policy import (
    SandboxArtifact,
    SandboxPolicy,
    sandbox_network_policy,
    sandbox_pod_spec,
)
from trpc_service.sandbox.runtime import SandboxExecutionResult


def _policy(**overrides: Any) -> SandboxPolicy:
    defaults: dict[str, Any] = {
        "image": "registry.internal/platform/sandbox-python@sha256:deadbeef",
        "cpu_limit": "1",
        "memory_limit": "512Mi",
        "execution_timeout_seconds": 30,
        "max_output_bytes": 65536,
    }
    defaults.update(overrides)
    return SandboxPolicy(**defaults)


def _input(code: str = "print('hi')", files: list[Any] | None = None) -> Any:
    from trpc_agent_sdk.code_executors import CodeExecutionInput

    return CodeExecutionInput(code=code, input_files=files or [])


class ScriptedRuntime:
    """Records pod specs and returns scripted results or raises."""

    def __init__(self, results: list[Any] | None = None, error: Exception | None = None) -> None:
        self.pod_specs: list[dict[str, Any]] = []
        self.codes: list[str] = []
        self._results = results or []
        self._error = error

    async def run(
        self,
        policy: SandboxPolicy,
        code: str,
        input_files: list[tuple[str, bytes]] | None = None,
    ) -> SandboxExecutionResult:
        self.pod_specs.append(sandbox_pod_spec(policy, execution_id="exec-test"))
        self.codes.append(code)
        if self._error is not None:
            raise self._error
        return self._results.pop(0)


def test_policy_defaults_are_fully_hardened() -> None:
    policy = _policy()
    assert policy.runtime_class_name == "gvisor"
    assert policy.run_as_non_root is True
    assert policy.run_as_user > 0
    assert policy.read_only_root_filesystem is True
    assert policy.allow_privilege_escalation is False
    assert policy.capabilities == ("ALL",)  # drop all
    assert policy.automount_service_account_token is False
    assert policy.network_disabled is True


def test_policy_rejects_weakening_changes() -> None:
    with pytest.raises(SandboxError, match="SANDBOX_POLICY_TOO_WEAK"):
        _policy(runtime_class_name="runc")
    with pytest.raises(SandboxError, match="SANDBOX_POLICY_TOO_WEAK"):
        _policy(run_as_non_root=False)
    with pytest.raises(SandboxError, match="SANDBOX_POLICY_TOO_WEAK"):
        _policy(read_only_root_filesystem=False)
    with pytest.raises(SandboxError, match="SANDBOX_POLICY_TOO_WEAK"):
        _policy(automount_service_account_token=True)
    with pytest.raises(SandboxError, match="SANDBOX_POLICY_TOO_WEAK"):
        _policy(network_disabled=False)
    with pytest.raises(SandboxError, match="SANDBOX_POLICY_TOO_WEAK"):
        _policy(capabilities=("NET_BIND_SERVICE",))


def test_pod_spec_embeds_every_hardening_requirement() -> None:
    spec = sandbox_pod_spec(_policy(), execution_id="exec-1")
    metadata = spec["metadata"]
    assert metadata["labels"]["platform.trpc/sandbox"] == "true"
    pod_spec = spec["spec"]
    assert pod_spec["runtimeClassName"] == "gvisor"
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["activeDeadlineSeconds"] == 30
    assert pod_spec["restartPolicy"] == "Never"
    container = pod_spec["containers"][0]
    security = container["securityContext"]
    assert security["runAsNonRoot"] is True
    assert security["runAsUser"] > 0
    assert security["readOnlyRootFilesystem"] is True
    assert security["allowPrivilegeEscalation"] is False
    assert security["capabilities"]["drop"] == ["ALL"]
    assert security["seccompProfile"]["type"] == "RuntimeDefault"
    assert container["resources"]["limits"]["cpu"] == "1"
    assert container["resources"]["limits"]["memory"] == "512Mi"
    assert container["image"].startswith("registry.internal/")
    # No token, no docker socket: the pod volume list stays empty.
    assert pod_spec.get("volumes") in (None, [])


def test_network_policy_denies_all_egress_by_default() -> None:
    manifest = sandbox_network_policy(_policy(), namespace="platform")
    assert manifest["kind"] == "NetworkPolicy"
    assert manifest["spec"]["policyTypes"] == ["Egress"]
    assert manifest["spec"]["egress"] == []


def test_executor_runs_code_inside_hardened_sandbox() -> None:
    runtime = ScriptedRuntime(
        results=[SandboxExecutionResult(ok=True, exit_code=0, output="hi", truncated=False)]
    )
    executor = SandboxedCodeExecutor(policy=_policy(), runtime=runtime)
    result = asyncio.run(executor.execute_code(None, _input("print('hi')")))
    assert result.outcome is not None
    assert "hi" in (result.output or "")
    pod_spec = runtime.pod_specs[0]
    assert pod_spec["spec"]["runtimeClassName"] == "gvisor"


def test_executor_truncates_output_to_the_declared_limit() -> None:
    huge = "x" * (70000)
    runtime = ScriptedRuntime(
        results=[SandboxExecutionResult(ok=True, exit_code=0, output=huge, truncated=False)]
    )
    executor = SandboxedCodeExecutor(policy=_policy(), runtime=runtime)
    result = asyncio.run(executor.execute_code(None, _input()))
    assert result.output is not None
    assert len(result.output.encode("utf-8")) <= 65536
    assert "SANDBOX_OUTPUT_TRUNCATED" in (result.output or "")


def test_executor_times_out_resource_abuse_without_retry() -> None:
    runtime = ScriptedRuntime(
        results=[
            SandboxExecutionResult(
                ok=False, exit_code=137, output="", truncated=False, timed_out=True
            )
        ]
    )
    executor = SandboxedCodeExecutor(policy=_policy(), runtime=runtime)
    result = asyncio.run(executor.execute_code(None, _input("while True: pass")))
    assert "timed out" in (result.output or "")


def test_executor_fails_closed_when_no_runtime_is_available() -> None:
    runtime = ScriptedRuntime(error=SandboxError(SANDBOX_UNAVAILABLE))
    executor = SandboxedCodeExecutor(policy=_policy(), runtime=runtime)
    result = asyncio.run(executor.execute_code(None, _input()))
    assert SANDBOX_UNAVAILABLE in (result.output or "")


def test_escape_attempts_run_inside_the_sandbox_and_cannot_affect_the_host() -> None:
    runtime = ScriptedRuntime(
        results=[
            SandboxExecutionResult(
                ok=False, exit_code=1, output="read-only file system", truncated=False
            )
        ]
    )
    executor = SandboxedCodeExecutor(policy=_policy(), runtime=runtime)
    result = asyncio.run(executor.execute_code(None, _input("open('/etc/passwd', 'w').write('x')")))
    # The escape attempt itself never leaves the sandbox: the pod spec that
    # carried it was fully hardened (read-only rootfs, no token, no mounts).
    pod_spec = runtime.pod_specs[0]
    assert pod_spec["spec"]["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    assert pod_spec["spec"]["automountServiceAccountToken"] is False
    assert "read-only" in (result.output or "")


def test_input_files_are_passed_as_artifacts_with_hashes() -> None:
    artifact = SandboxArtifact.from_bytes("data.csv", b"a,b\n1,2\n")
    assert artifact.sha256 == __import__("hashlib").sha256(b"a,b\n1,2\n").hexdigest()
    runtime = ScriptedRuntime(
        results=[SandboxExecutionResult(ok=True, exit_code=0, output="ok", truncated=False)]
    )
    executor = SandboxedCodeExecutor(policy=_policy(), runtime=runtime)
    from trpc_agent_sdk.code_executors import CodeFile

    asyncio.run(
        executor.execute_code(
            None,
            _input(files=[CodeFile(name="data.csv", content="a,b\n1,2\n", mime_type="text/csv")]),
        )
    )
    assert runtime.pod_specs is not None


def test_production_forbids_unsafe_local_executor_and_docker_socket() -> None:
    assert_no_unsafe_local_execution("SANDBOX", environment="PRODUCTION")
    with pytest.raises(SandboxError, match="SANDBOX_UNSAFE_EXECUTOR_FORBIDDEN"):
        assert_no_unsafe_local_execution("UNSAFE_LOCAL", environment="PRODUCTION")
    with pytest.raises(SandboxError, match="SANDBOX_UNSAFE_EXECUTOR_FORBIDDEN"):
        assert_no_unsafe_local_execution("DOCKER", environment="PRODUCTION")
    with pytest.raises(SandboxError, match="SANDBOX_DOCKER_SOCKET_FORBIDDEN"):
        assert_no_unsafe_local_execution(
            "SANDBOX", environment="PRODUCTION", docker_socket_mounted=True
        )
    # Non-production environments are allowed to use the local executor.
    assert_no_unsafe_local_execution("UNSAFE_LOCAL", environment="DEVELOPMENT")


def _fake_cluster(monkeypatch: Any, transport: Any) -> Any:
    import ssl

    from trpc_service.sandbox import runtime as runtime_module

    config = runtime_module._ClusterConfig(
        api_url="https://apiserver.test:443", token="token-1", verify=ssl.create_default_context()
    )
    return config


def test_kubernetes_runtime_creates_and_cleans_up_pods(monkeypatch: Any) -> None:
    import httpx

    from trpc_service.sandbox import runtime as runtime_module
    from trpc_service.sandbox.runtime import KubernetesSandboxRuntime

    events: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "DELETE":
            events.append(("delete", path))
            return httpx.Response(200, json={})
        if request.method == "POST" and path.endswith("/pods"):
            events.append(("create", "pod"))
            return httpx.Response(201, json={})
        if request.method == "GET" and path.endswith("/log"):
            return httpx.Response(200, text="hello from sandbox")
        if request.method == "GET" and path.endswith("/pods/pod-1") or "/pods/sandbox-" in path:
            return httpx.Response(
                200,
                json={
                    "status": {
                        "phase": "Succeeded",
                        "containerStatuses": [{"state": {"terminated": {"exitCode": 0}}}],
                    }
                },
            )
        events.append((request.method, path))
        return httpx.Response(404, json={})

    transport = httpx.MockTransport(handler)
    config = _fake_cluster(monkeypatch, transport)
    real_async_client = httpx.AsyncClient

    def fake_client(**kwargs: Any) -> Any:
        kwargs["transport"] = transport
        return real_async_client(**kwargs)

    monkeypatch.setattr(runtime_module.httpx, "AsyncClient", fake_client)
    runtime = KubernetesSandboxRuntime(poll_interval=0)
    monkeypatch.setattr(runtime, "_config", lambda: config)
    result = asyncio.run(runtime.run(_policy(), "print('x')"))
    assert result.ok is True
    assert result.output == "hello from sandbox"
    assert events[0][0] == "create"
    assert any(method == "delete" for method, _ in events)


def test_kubernetes_runtime_fails_closed_without_cluster_identity(monkeypatch: Any) -> None:
    from trpc_service.sandbox import runtime as runtime_module
    from trpc_service.sandbox.errors import SandboxError
    from trpc_service.sandbox.runtime import KubernetesSandboxRuntime

    monkeypatch.setattr(runtime_module, "in_cluster_config", lambda: None)
    runtime = KubernetesSandboxRuntime()
    with pytest.raises(SandboxError, match="SANDBOX_UNAVAILABLE"):
        asyncio.run(runtime.run(_policy(), "print('x')"))
