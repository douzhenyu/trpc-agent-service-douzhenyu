"""Sandboxed code executor: tRPC-Agent BaseCodeExecutor over the gVisor sandbox.

Untrusted model-generated code only ever runs inside a fully hardened sandbox
pod. Any failure of the isolation chain — no runtime, weakened policy, output
overflow — fails closed with a stable error instead of degrading to a weaker
executor (spec 禁项8: no UnsafeLocalCodeExecutor or Docker socket in production).
"""

from __future__ import annotations

from typing import Any

from trpc_agent_sdk.code_executors import (
    BaseCodeExecutor,
    CodeExecutionInput,
    CodeExecutionResult,
    create_code_execution_result,
)

from trpc_service.sandbox.errors import SandboxError
from trpc_service.sandbox.policy import SandboxPolicy
from trpc_service.sandbox.runtime import SandboxRuntime

SANDBOX_UNAVAILABLE = "SANDBOX_UNAVAILABLE"
SANDBOX_OUTPUT_TRUNCATED = "SANDBOX_OUTPUT_TRUNCATED"
SANDBOX_UNSAFE_EXECUTOR_FORBIDDEN = "SANDBOX_UNSAFE_EXECUTOR_FORBIDDEN"
SANDBOX_DOCKER_SOCKET_FORBIDDEN = "SANDBOX_DOCKER_SOCKET_FORBIDDEN"


def assert_no_unsafe_local_execution(
    executor_kind: str,
    *,
    environment: str,
    docker_socket_mounted: bool = False,
) -> None:
    """Production guard: only the sandboxed executor is acceptable."""

    if environment != "PRODUCTION":
        return
    if executor_kind.upper() in {"UNSAFE_LOCAL", "DOCKER"}:
        raise SandboxError(SANDBOX_UNSAFE_EXECUTOR_FORBIDDEN)
    if docker_socket_mounted:
        raise SandboxError(SANDBOX_DOCKER_SOCKET_FORBIDDEN)


class SandboxedCodeExecutor(BaseCodeExecutor):  # type: ignore[misc]  # trpc_agent_sdk ships no stubs
    """Runs untrusted code in one isolated gVisor sandbox pod per execution."""

    def __init__(self, *, policy: SandboxPolicy, runtime: SandboxRuntime, **data: Any) -> None:
        super().__init__(**data)
        object.__setattr__(self, "_policy", policy)
        object.__setattr__(self, "_runtime", runtime)

    @property
    def policy(self) -> SandboxPolicy:
        return object.__getattribute__(self, "_policy")  # type: ignore[no-any-return]

    async def execute_code(
        self,
        invocation_context: Any,
        code_execution_input: CodeExecutionInput,
    ) -> CodeExecutionResult:
        """Submit the code to the sandbox; every failure is a safe result."""

        policy = self.policy
        code = code_execution_input.code or "\n".join(
            block.code for block in code_execution_input.code_blocks
        )
        input_files = [
            (file.name, str(file.content or "").encode("utf-8"))
            for file in code_execution_input.input_files
        ]
        try:
            outcome = await self._runtime.run(policy, code, input_files)
        except SandboxError as error:
            return create_code_execution_result(stderr=f"{error.code}\n")
        output = outcome.output or ""
        # Reserve headroom for the SDK result delimiters so the delivered
        # output stays within the declared limit.
        output_budget = max(policy.max_output_bytes - 128, 1024)
        if len(output.encode("utf-8")) > output_budget:
            encoded = output.encode("utf-8")[:output_budget]
            output = encoded.decode("utf-8", errors="ignore") + f"\n{SANDBOX_OUTPUT_TRUNCATED}\n"
        if outcome.timed_out:
            return create_code_execution_result(
                stdout="", stderr="Code execution timed out\n", is_timed_out=True
            )
        if outcome.ok:
            return create_code_execution_result(stdout=output)
        return create_code_execution_result(stderr=output or f"exit {outcome.exit_code}\n")
