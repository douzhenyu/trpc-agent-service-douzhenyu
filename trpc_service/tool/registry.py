"""Versioned tool and MCP capability registry with side-effect governance metadata.

A Tool declares its version, JSON Schemas, required scopes, side-effect level,
timeout, per-call cost and data classification. MCP servers register as
capabilities that expand into versioned tool definitions, so governance and
audit treat MCP tools exactly like declared ones.
"""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from trpc_service.governance import DataClassification

_TOOL_NAME_PATTERN = r"^[a-z][a-z0-9_]{1,63}$"
_JSON_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "number": (int, float),
    "integer": (int,),
    "boolean": (bool,),
    "array": (list,),
    "object": (dict,),
}


class ToolSideEffect(StrEnum):
    READ_ONLY = "READ_ONLY"
    IDEMPOTENT_WRITE = "IDEMPOTENT_WRITE"
    NON_IDEMPOTENT_WRITE = "NON_IDEMPOTENT_WRITE"
    HIGH_RISK = "HIGH_RISK"


class ToolSource(StrEnum):
    DECLARED = "DECLARED"
    MCP = "MCP"


AUTO_EXECUTABLE_SIDE_EFFECTS = frozenset(
    {ToolSideEffect.READ_ONLY, ToolSideEffect.IDEMPOTENT_WRITE}
)


class ToolDefinition(BaseModel):
    """One immutable version of a tenant-registered tool capability."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    name: str = Field(pattern=_TOOL_NAME_PATTERN)
    version: int = Field(ge=1)
    description: str = Field(min_length=1, max_length=512)
    side_effect: ToolSideEffect
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    scopes: tuple[str, ...] = ()
    timeout_seconds: int = Field(default=30, ge=1, le=600)
    cost_per_call_micros: int = Field(default=0, ge=0)
    data_classification: DataClassification
    supports_idempotency: bool = False
    source: ToolSource = ToolSource.DECLARED
    mcp_server: str | None = None


def canonical_params_json(params: dict[str, Any]) -> str:
    return json.dumps(
        params, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=repr
    )


def canonical_params_hash(params: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_params_json(params).encode("utf-8")).hexdigest()


def check_schema_shape(schema: dict[str, Any]) -> None:
    if schema.get("type") != "object" or not isinstance(schema.get("properties", {}), dict):
        raise ValueError("tool schemas must describe a top-level object")


class ToolDefinitionConflict(ValueError):
    """Raised when a re-registration diverges from an immutable stored version."""


class ToolInvocationError(RuntimeError):
    """Stable, tenant-safe tool governance failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def validate_params(definition: ToolDefinition, params: dict[str, Any]) -> dict[str, Any]:
    """Normalize params against the declared schema subset; reject tampering."""

    schema = definition.input_schema
    if not isinstance(params, dict):
        raise ToolInvocationError("TOOL_PARAMS_INVALID")
    properties = schema.get("properties", {})
    allowed = set(properties)
    if set(params) - allowed:
        raise ToolInvocationError("TOOL_PARAMS_INVALID")
    for key in schema.get("required", []):
        if key not in params:
            raise ToolInvocationError("TOOL_PARAMS_INVALID")
    for key, value in params.items():
        expected = properties.get(key, {}).get("type")
        if expected is None:
            continue
        if isinstance(value, bool) and expected != "boolean":
            # bool is an int subclass; only accept it where the schema says boolean.
            raise ToolInvocationError("TOOL_PARAMS_INVALID")
        if not isinstance(value, _JSON_TYPES.get(expected, (object,))):
            raise ToolInvocationError("TOOL_PARAMS_INVALID")
    return params


class ToolRegistry:
    """Tenant-scoped registry of versioned tool definitions."""

    def __init__(self) -> None:
        self._definitions: dict[tuple[str, str, int], ToolDefinition] = {}

    @classmethod
    def in_memory(cls) -> ToolRegistry:
        return cls()

    def register(self, definition: ToolDefinition) -> ToolDefinition:
        try:
            ToolDefinition.model_validate(definition)
        except ValidationError as error:
            raise ValueError(f"invalid tool definition: {error}") from error
        check_schema_shape(definition.input_schema)
        check_schema_shape(definition.output_schema)
        key = (definition.tenant_id, definition.name, definition.version)
        existing = self._definitions.get(key)
        if existing is not None:
            if existing != definition:
                raise ToolDefinitionConflict(
                    f"tool version conflict for {definition.name} v{definition.version}"
                )
            return existing
        self._definitions[key] = definition
        return definition

    def register_mcp_server(
        self,
        *,
        tenant_id: str,
        server_name: str,
        server_version: int,
        tools: list[ToolDefinition],
    ) -> int:
        """Expand one MCP capability registration into versioned tool definitions."""

        names = [tool.name for tool in tools]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate tool names in MCP server {server_name}")
        registered = 0
        for tool in tools:
            replacement = tool.model_copy(
                update={
                    "tenant_id": tenant_id,
                    "version": server_version,
                    "source": ToolSource.MCP,
                    "mcp_server": server_name,
                }
            )
            self.register(replacement)
            registered += 1
        return registered

    def has_name(self, name: str) -> bool:
        """Whether any tenant registered this tool name (tenant-denial vs not-found)."""

        return any(candidate_name == name for _, candidate_name, _ in self._definitions)

    def resolve(
        self, tenant_id: str, name: str, *, version: int | None = None
    ) -> ToolDefinition | None:
        """Resolve one tenant's tool at a pinned version, or its latest version."""

        if version is not None:
            return self._definitions.get((tenant_id, name, version))
        candidates = [
            definition
            for (
                candidate_tenant,
                candidate_name,
                candidate_version,
            ), definition in self._definitions.items()
            if candidate_tenant == tenant_id and candidate_name == name
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda definition: definition.version)

    def latest_definitions(self, tenant_id: str) -> tuple[ToolDefinition, ...]:
        """The newest version of every tool registered by one tenant."""

        latest: dict[str, ToolDefinition] = {}
        for (
            candidate_tenant,
            candidate_name,
            _candidate_version,
        ), definition in self._definitions.items():
            if candidate_tenant != tenant_id:
                continue
            current = latest.get(candidate_name)
            if current is None or definition.version > current.version:
                latest[candidate_name] = definition
        return tuple(latest[name] for name in sorted(latest))
