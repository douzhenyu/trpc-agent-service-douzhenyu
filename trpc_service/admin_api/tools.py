"""Tool and MCP capability registration for tenants."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.governance import DataClassification
from trpc_service.tool.registry import (
    ToolDefinition,
    ToolDefinitionConflict,
    ToolSideEffect,
    ToolSource,
)
from trpc_service.tool.store import DatabaseToolRegistry


class ToolDefinitionUpsert(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(pattern=r"^[a-z][a-z0-9_]{1,63}$")
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


class McpServerRegistration(BaseModel):
    model_config = ConfigDict(frozen=True)

    server_name: str = Field(min_length=1, max_length=64)
    server_version: int = Field(ge=1)
    tools: list[ToolDefinitionUpsert] = Field(min_length=1)


class ToolDefinitionResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    name: str
    version: int
    description: str
    side_effect: ToolSideEffect
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    scopes: list[str]
    timeout_seconds: int
    cost_per_call_micros: int
    data_classification: DataClassification
    supports_idempotency: bool
    source: ToolSource
    mcp_server: str | None


class ToolDefinitionList(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    tools: list[ToolDefinitionResponse]


def _upsert_to_definition(tenant_id: str, payload: ToolDefinitionUpsert) -> ToolDefinition:
    return ToolDefinition(
        tenant_id=tenant_id,
        name=payload.name,
        version=payload.version,
        description=payload.description,
        side_effect=payload.side_effect,
        input_schema=payload.input_schema,
        output_schema=payload.output_schema,
        scopes=payload.scopes,
        timeout_seconds=payload.timeout_seconds,
        cost_per_call_micros=payload.cost_per_call_micros,
        data_classification=payload.data_classification,
        supports_idempotency=payload.supports_idempotency,
    )


def _definition_response(definition: ToolDefinition) -> ToolDefinitionResponse:
    return ToolDefinitionResponse(
        tenant_id=UUID(definition.tenant_id),
        name=definition.name,
        version=definition.version,
        description=definition.description,
        side_effect=definition.side_effect,
        input_schema=definition.input_schema,
        output_schema=definition.output_schema,
        scopes=list(definition.scopes),
        timeout_seconds=definition.timeout_seconds,
        cost_per_call_micros=definition.cost_per_call_micros,
        data_classification=definition.data_classification,
        supports_idempotency=definition.supports_idempotency,
        source=definition.source,
        mcp_server=definition.mcp_server,
    )


def create_tool_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["tools"])

    def _registry() -> DatabaseToolRegistry:
        return DatabaseToolRegistry(database)

    @router.put(
        "/tools",
        response_model=ToolDefinitionResponse,
        responses={**error_responses(401, 403, 409, 422)},
    )
    async def register_tool(
        tenant_id: UUID,
        payload: ToolDefinitionUpsert,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> ToolDefinitionResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "tool.register",
            target_type="tool",
            target_id=payload.name,
        )
        try:
            definition = _upsert_to_definition(str(tenant_id), payload)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        try:
            stored = await _registry().register(definition, created_by=principal.subject)
        except ToolDefinitionConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        async with database.tenant_transaction(tenant_id) as connection:
            await insert_audit(
                connection,
                principal,
                "tool.register",
                "ALLOW",
                target_type="tool",
                target_id=f"{definition.name}:v{definition.version}",
                tenant_id=tenant_id,
                details={"side_effect": str(definition.side_effect)},
            )
        return _definition_response(stored)

    @router.put(
        "/mcp-servers",
        response_model=ToolDefinitionList,
        responses={**error_responses(401, 403, 409, 422)},
    )
    async def register_mcp_server(
        tenant_id: UUID,
        payload: McpServerRegistration,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> ToolDefinitionList:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "mcp_server.register",
            target_type="mcp_server",
            target_id=payload.server_name,
        )
        names = [tool.name for tool in payload.tools]
        if len(names) != len(set(names)):
            raise HTTPException(status_code=422, detail="duplicate tool names in MCP server")
        definitions = [_upsert_to_definition(str(tenant_id), tool) for tool in payload.tools]
        try:
            await _registry().register_mcp_server(
                tenant_id=str(tenant_id),
                server_name=payload.server_name,
                server_version=payload.server_version,
                tools=definitions,
                created_by=principal.subject,
            )
        except ToolDefinitionConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        async with database.tenant_transaction(tenant_id) as connection:
            await insert_audit(
                connection,
                principal,
                "mcp_server.register",
                "ALLOW",
                target_type="mcp_server",
                target_id=f"{payload.server_name}:v{payload.server_version}",
                tenant_id=tenant_id,
                details={"tools": names},
            )
        resolved = [
            _definition_response(definition)
            for definition in await _registry().latest_definitions(str(tenant_id))
            if definition.mcp_server == payload.server_name
        ]
        return ToolDefinitionList(tenant_id=tenant_id, tools=resolved)

    @router.get(
        "/tools",
        response_model=ToolDefinitionList,
        responses={**error_responses(401, 403)},
    )
    async def list_tools(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> ToolDefinitionList:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "tool.list",
            target_type="tool",
        )
        definitions = await _registry().latest_definitions(str(tenant_id))
        return ToolDefinitionList(
            tenant_id=tenant_id, tools=[_definition_response(d) for d in definitions]
        )

    return router
