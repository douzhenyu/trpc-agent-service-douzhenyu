"""Bridge between the governed tool executor and the tRPC-Agent SDK tool surface.

The SDK model only ever sees a declaration bound to one immutable tool
version; every call is routed through the ToolInvocationService, so tenant
boundaries, scopes, params, retry contracts and audit records cannot be
bypassed by model-generated arguments.

Per-execution audit context (execution, session, subject) is carried in a
ContextVar set by the runtime around each run, because cached release agents
are shared across executions.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from trpc_agent_sdk.tools import BaseTool
from trpc_agent_sdk.types import FunctionDeclaration, JSONSchema, Schema

from trpc_service.tool.executor import ToolInvocationService
from trpc_service.tool.registry import ToolDefinition, ToolInvocationError


@dataclass(frozen=True)
class ToolInvocationContext:
    """Who and which execution is asking for the tool right now."""

    tenant_id: str
    requested_by: str = "agent-runner"
    execution_id: str | None = None
    session_id: str | None = None
    release_id: str | None = None


_CURRENT_CONTEXT: ContextVar[ToolInvocationContext | None] = ContextVar(
    "tool_invocation_context", default=None
)


def set_invocation_context(
    context: ToolInvocationContext,
) -> object:
    """Bind the current execution's tool-audit context; returns the token."""

    return _CURRENT_CONTEXT.set(context)


def reset_invocation_context(token: object) -> None:
    _CURRENT_CONTEXT.reset(token)  # type: ignore[arg-type]


class GovernedTool(BaseTool):  # type: ignore[misc]  # trpc_agent_sdk ships no stubs
    """One SDK tool backed by the governed invocation service."""

    def __init__(
        self,
        definition: ToolDefinition,
        service: ToolInvocationService,
        *,
        scopes: frozenset[str] | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        super().__init__(name=definition.name, description=definition.description)
        self._definition = definition
        self._service = service
        self._scopes = scopes
        self._idempotency_key = idempotency_key

    @property
    def definition(self) -> ToolDefinition:
        return self._definition

    def _get_declaration(self) -> FunctionDeclaration | None:
        return FunctionDeclaration(
            name=self.name,
            description=self.description,
            parameters=Schema.from_json_schema(
                json_schema=JSONSchema.model_validate(self._definition.input_schema),
                raise_error_on_unsupported_field=False,
            ),
        )

    async def governed_invoke(self, params: dict[str, Any]) -> dict[str, Any]:
        """Invoke the tool under governance and return a model-safe response."""

        context = _CURRENT_CONTEXT.get()
        tenant_id = context.tenant_id if context is not None else self._definition.tenant_id
        try:
            result = await self._service.invoke(
                tenant_id=tenant_id,
                tool_name=self._definition.name,
                version=self._definition.version,
                params=params,
                scopes=self._scopes,
                requested_by=context.requested_by if context else "agent-runner",
                idempotency_key=self._idempotency_key,
                mode="conversation",
                execution_id=context.execution_id if context else None,
                session_id=context.session_id if context else None,
                release_id=context.release_id if context else None,
            )
        except ToolInvocationError as error:
            return {"status": "BLOCKED", "error": error.code}
        return {
            "status": str(result.status),
            "result": result.record.result or {},
            **({"error": result.error_code} if result.error_code else {}),
        }

    async def _run_async_impl(self, *, tool_context: Any, args: dict[str, Any]) -> Any:
        return await self.governed_invoke(dict(args))
