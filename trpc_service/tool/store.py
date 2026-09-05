"""PostgreSQL-backed tool registry and immutable tool-call records.

Definitions are tenant-scoped control-plane metadata; tool-call rows are
append-only runtime data protected by RLS and written once per invocation,
so retries cannot mutate history.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from trpc_service.admin_api.database import Connection, Database
from trpc_service.governance import DataClassification
from trpc_service.tool.executor import ToolInvocationRecord, ToolInvocationStatus
from trpc_service.tool.registry import (
    ToolDefinition,
    ToolDefinitionConflict,
    ToolSideEffect,
    ToolSource,
    check_schema_shape,
)

_DEFINITION_FIELDS = (
    "tenant_id",
    "name",
    "version",
    "description",
    "side_effect",
    "input_schema",
    "output_schema",
    "scopes",
    "timeout_seconds",
    "cost_per_call_micros",
    "data_classification",
    "supports_idempotency",
    "source",
    "mcp_server",
    "created_by",
    "created_at",
)


def _definition_from_row(row: Any) -> ToolDefinition:
    return ToolDefinition(
        tenant_id=str(row["tenant_id"]),
        name=str(row["name"]),
        version=int(row["version"]),
        description=str(row["description"]),
        side_effect=ToolSideEffect(str(row["side_effect"])),
        input_schema=row["input_schema"],
        output_schema=row["output_schema"],
        scopes=tuple(row["scopes"] or ()),
        timeout_seconds=int(row["timeout_seconds"]),
        cost_per_call_micros=int(row["cost_per_call_micros"]),
        data_classification=DataClassification(str(row["data_classification"])),
        supports_idempotency=bool(row["supports_idempotency"]),
        source=ToolSource(str(row["source"])),
        mcp_server=row["mcp_server"],
    )


class DatabaseToolRegistry:
    """Tenant-scoped versioned tool registry backed by `tenant.tool_definition`."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def register(self, definition: ToolDefinition, *, created_by: str) -> ToolDefinition:
        """Insert one definition version; identical re-registration is a no-op."""

        check_schema_shape(definition.input_schema)
        check_schema_shape(definition.output_schema)
        async with self._database.tenant_transaction(UUID(definition.tenant_id)) as connection:
            await self._insert_definition(connection, definition, created_by)
            stored = await connection.fetchrow(
                "SELECT * FROM tenant.tool_definition WHERE name=$1 AND version=$2",
                definition.name,
                definition.version,
            )
        stored_definition = _definition_from_row(stored)
        if stored_definition != definition:
            raise ToolDefinitionConflict(
                f"tool version conflict for {definition.name} v{definition.version}"
            )
        return stored_definition

    async def register_mcp_server(
        self,
        *,
        tenant_id: str,
        server_name: str,
        server_version: int,
        tools: list[ToolDefinition],
        created_by: str,
    ) -> int:
        registered = 0
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            for tool in tools:
                replacement = tool.model_copy(
                    update={
                        "tenant_id": tenant_id,
                        "version": server_version,
                        "source": ToolSource.MCP,
                        "mcp_server": server_name,
                    }
                )
                check_schema_shape(replacement.input_schema)
                check_schema_shape(replacement.output_schema)
                await self._insert_definition(connection, replacement, created_by)
                registered += 1
        return registered

    async def _insert_definition(
        self, connection: Connection, definition: ToolDefinition, created_by: str
    ) -> None:
        await connection.execute(
            """INSERT INTO tenant.tool_definition
                (tenant_id,name,version,description,side_effect,input_schema,output_schema,
                 scopes,timeout_seconds,cost_per_call_micros,data_classification,
                 supports_idempotency,source,mcp_server,created_by)
                VALUES ($1,$2,$3,$4,$5,CAST($6 AS jsonb),CAST($7 AS jsonb),$8,$9,$10,$11,
                        $12,$13,$14,$15)
                ON CONFLICT (tenant_id,name,version) DO NOTHING""",
            definition.tenant_id,
            definition.name,
            definition.version,
            definition.description,
            str(definition.side_effect),
            json.dumps(definition.input_schema, ensure_ascii=False),
            json.dumps(definition.output_schema, ensure_ascii=False),
            list(definition.scopes),
            definition.timeout_seconds,
            definition.cost_per_call_micros,
            str(definition.data_classification),
            definition.supports_idempotency,
            str(definition.source),
            definition.mcp_server,
            created_by,
        )

    async def resolve(
        self, tenant_id: str, name: str, *, version: int | None = None
    ) -> ToolDefinition | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            if version is None:
                row = await connection.fetchrow(
                    """SELECT * FROM tenant.tool_definition
                    WHERE name=$1 ORDER BY version DESC LIMIT 1""",
                    name,
                )
            else:
                row = await connection.fetchrow(
                    "SELECT * FROM tenant.tool_definition WHERE name=$1 AND version=$2",
                    name,
                    version,
                )
        return _definition_from_row(row) if row is not None else None

    async def latest_definitions(self, tenant_id: str) -> tuple[ToolDefinition, ...]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                """SELECT DISTINCT ON (name) * FROM tenant.tool_definition
                WHERE tenant_id=$1 ORDER BY name, version DESC""",
                tenant_id,
            )
        return tuple(_definition_from_row(row) for row in rows)


def _record_from_row(row: Any) -> ToolInvocationRecord:
    return ToolInvocationRecord(
        call_id=str(row["call_id"]),
        tenant_id=str(row["tenant_id"]),
        tool_name=str(row["tool_name"]),
        tool_version=int(row["tool_version"]) if row["tool_version"] is not None else None,
        side_effect=row["side_effect"],
        params=row["params"] or {},
        params_hash=str(row["params_hash"]),
        idempotency_key=row["idempotency_key"],
        status=ToolInvocationStatus(str(row["status"])),
        attempts=int(row["attempts"]),
        cost_micros=int(row["cost_micros"]),
        requested_by=str(row["requested_by"]),
        execution_id=row["execution_id"],
        session_id=row["session_id"],
        release_id=row["release_id"],
        error_code=row["error_code"],
        result=row["result"],
        data_classification=(
            DataClassification(str(row["data_classification"]))
            if row["data_classification"]
            else None
        ),
    )


class DatabaseToolCallStore:
    """Append-only `tenant.tool_call` records; the row is written once."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def find_replay(
        self, tenant_id: str, idempotency_key: str
    ) -> ToolInvocationRecord | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                """SELECT * FROM tenant.tool_call
                WHERE idempotency_key=$1 AND status='SUCCEEDED'""",
                idempotency_key,
            )
        return _record_from_row(row) if row is not None else None

    async def record(self, record: ToolInvocationRecord) -> None:
        async with self._database.tenant_transaction(UUID(record.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.tool_call
                    (tenant_id,call_id,execution_id,session_id,release_id,tool_name,
                     tool_version,side_effect,params,params_hash,idempotency_key,status,
                     error_code,result,attempts,cost_micros,requested_by,data_classification)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS jsonb),$10,$11,$12,$13,
                            CAST($14 AS jsonb),$15,$16,$17,$18)
                    ON CONFLICT (tenant_id,call_id) DO NOTHING""",
                record.tenant_id,
                record.call_id,
                record.execution_id,
                record.session_id,
                record.release_id,
                record.tool_name,
                record.tool_version,
                record.side_effect,
                json.dumps(record.params, ensure_ascii=False),
                record.params_hash,
                record.idempotency_key,
                str(record.status),
                record.error_code,
                json.dumps(record.result, ensure_ascii=False) if record.result else None,
                record.attempts,
                record.cost_micros,
                record.requested_by,
                str(record.data_classification) if record.data_classification else None,
            )
