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
from trpc_service.tool.approvals import ApprovalRequest, ApprovalStatus
from trpc_service.tool.checkpoints import CheckpointStatus, ExecutionCheckpoint
from trpc_service.tool.executor import ToolInvocationRecord, ToolInvocationStatus
from trpc_service.tool.reconciliation import Reconciliation, ReconciliationDecision
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

    async def list_unknown(self, tenant_id: str) -> list[ToolInvocationRecord]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                "SELECT * FROM tenant.tool_call WHERE tenant_id=$1 AND status='OUTCOME_UNKNOWN'",
                UUID(tenant_id),
            )
        return [_record_from_row(row) for row in rows]

    async def get_call(self, tenant_id: str, call_id: str) -> ToolInvocationRecord | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM tenant.tool_call WHERE call_id=$1", call_id
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


def _approval_from_row(row: Any) -> ApprovalRequest:
    return ApprovalRequest(
        approval_id=str(row["approval_id"]),
        tenant_id=str(row["tenant_id"]),
        release_id=str(row["release_id"]),
        tool_name=str(row["tool_name"]),
        tool_version=int(row["tool_version"]),
        params_hash=str(row["params_hash"]),
        params=row["params"] or {},
        side_effect=ToolSideEffect(str(row["side_effect"])),
        requested_by=str(row["requested_by"]),
        requester_role=str(row["requester_role"]),
        policy_version=str(row["policy_version"]),
        status=ApprovalStatus(str(row["status"])),
        requested_at=row["requested_at"],
        expires_at=row["expires_at"],
        decided_by=row["decided_by"],
        decided_at=row["decided_at"],
    )


class DatabaseApprovalStore:
    """`tenant.tool_approval` persistence for the approval service."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def transition(
        self,
        tenant_id: str,
        approval_id: str,
        *,
        from_statuses: tuple[ApprovalStatus, ...],
        to_status: ApprovalStatus,
        decided_by: str | None = None,
        decided_at: object | None = None,
    ) -> ApprovalRequest | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                """UPDATE tenant.tool_approval
                SET status=$3,
                    decided_by=COALESCE($4, decided_by),
                    decided_at=COALESCE($5, decided_at)
                WHERE approval_id=$2 AND status=ANY($1)
                RETURNING *""",
                list(from_statuses),
                approval_id,
                str(to_status),
                decided_by,
                decided_at,
            )
        return _approval_from_row(row) if row is not None else None

    async def upsert(self, request: ApprovalRequest) -> None:
        async with self._database.tenant_transaction(UUID(request.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.tool_approval
                    (tenant_id,approval_id,release_id,tool_name,tool_version,params_hash,
                     params,side_effect,requested_by,requester_role,policy_version,status,
                     requested_at,expires_at,decided_by,decided_at)
                    VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS jsonb),$8,$9,$10,$11,$12,$13,$14,$15,$16)
                    ON CONFLICT (tenant_id,approval_id) DO UPDATE SET
                      status=EXCLUDED.status,
                      decided_by=EXCLUDED.decided_by,
                      decided_at=EXCLUDED.decided_at""",
                request.tenant_id,
                request.approval_id,
                request.release_id,
                request.tool_name,
                request.tool_version,
                request.params_hash,
                json.dumps(request.params, ensure_ascii=False),
                str(request.side_effect),
                request.requested_by,
                request.requester_role,
                request.policy_version,
                str(request.status),
                request.requested_at,
                request.expires_at,
                request.decided_by,
                request.decided_at,
            )

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRequest | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM tenant.tool_approval WHERE approval_id=$1", approval_id
            )
        return _approval_from_row(row) if row is not None else None

    async def list(
        self, tenant_id: str, *, status: str | None = None, limit: int = 100
    ) -> list[ApprovalRequest]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            if status is None:
                rows = await connection.fetch(
                    """SELECT * FROM tenant.tool_approval WHERE tenant_id=$1
                    ORDER BY requested_at DESC LIMIT $2""",
                    UUID(tenant_id),
                    limit,
                )
            else:
                rows = await connection.fetch(
                    """SELECT * FROM tenant.tool_approval WHERE tenant_id=$1 AND status=$2
                    ORDER BY requested_at DESC LIMIT $3""",
                    UUID(tenant_id),
                    status,
                    limit,
                )
        return [_approval_from_row(row) for row in rows]

    async def find_open(
        self,
        tenant_id: str,
        *,
        tool_name: str,
        tool_version: int,
        params_hash: str,
        requested_by: str,
        statuses: tuple[ApprovalStatus, ...],
    ) -> ApprovalRequest | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                """SELECT * FROM tenant.tool_approval
                WHERE tool_name=$1 AND tool_version=$2 AND params_hash=$3
                  AND requested_by=$4 AND status=ANY($5)
                ORDER BY requested_at DESC LIMIT 1""",
                tool_name,
                tool_version,
                params_hash,
                requested_by,
                list(statuses),
            )
        return _approval_from_row(row) if row is not None else None


def _checkpoint_from_row(row: Any) -> ExecutionCheckpoint:
    return ExecutionCheckpoint(
        checkpoint_id=str(row["checkpoint_id"]),
        tenant_id=str(row["tenant_id"]),
        execution_id=str(row["execution_id"]),
        session_id=str(row["session_id"]),
        release_id=str(row["release_id"]),
        approval_id=str(row["approval_id"]),
        tool_name=str(row["tool_name"]),
        tool_version=int(row["tool_version"]),
        params_hash=str(row["params_hash"]),
        requested_by=str(row["requested_by"]),
        parked_by=str(row["parked_by"]),
        status=CheckpointStatus(str(row["status"])),
        created_at=row["created_at"],
        resumed_by=row["resumed_by"],
        resumed_at=row["resumed_at"],
    )


class DatabaseCheckpointStore:
    """`tenant.execution_checkpoint` persistence for parked executions."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def upsert(self, checkpoint: ExecutionCheckpoint) -> None:
        async with self._database.tenant_transaction(UUID(checkpoint.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.execution_checkpoint
                    (tenant_id,checkpoint_id,execution_id,session_id,release_id,approval_id,
                     tool_name,tool_version,params_hash,requested_by,parked_by,status,
                     created_at,resumed_by,resumed_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                    ON CONFLICT (tenant_id,checkpoint_id) DO UPDATE SET
                      status=EXCLUDED.status,
                      resumed_by=EXCLUDED.resumed_by,
                      resumed_at=EXCLUDED.resumed_at""",
                checkpoint.tenant_id,
                checkpoint.checkpoint_id,
                checkpoint.execution_id,
                checkpoint.session_id,
                checkpoint.release_id,
                checkpoint.approval_id,
                checkpoint.tool_name,
                checkpoint.tool_version,
                checkpoint.params_hash,
                checkpoint.requested_by,
                checkpoint.parked_by,
                str(checkpoint.status),
                checkpoint.created_at,
                checkpoint.resumed_by,
                checkpoint.resumed_at,
            )

    async def get(self, tenant_id: str, checkpoint_id: str) -> ExecutionCheckpoint | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM tenant.execution_checkpoint WHERE checkpoint_id=$1",
                checkpoint_id,
            )
        return _checkpoint_from_row(row) if row is not None else None


class DatabaseReconciliationStore:
    """Append-only `tenant.tool_call_reconciliation` persistence."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def upsert(self, reconciliation: Reconciliation) -> None:
        async with self._database.tenant_transaction(UUID(reconciliation.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.tool_call_reconciliation
                    (tenant_id,call_id,decision,resolved_by,note,resolved_at)
                    VALUES ($1,$2,$3,$4,$5,$6)
                    ON CONFLICT (tenant_id,call_id) DO NOTHING""",
                reconciliation.tenant_id,
                reconciliation.call_id,
                str(reconciliation.decision),
                reconciliation.resolved_by,
                reconciliation.note,
                reconciliation.resolved_at,
            )

    async def get(self, tenant_id: str, call_id: str) -> Reconciliation | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM tenant.tool_call_reconciliation WHERE call_id=$1", call_id
            )
        return (
            Reconciliation(
                call_id=str(row["call_id"]),
                tenant_id=str(row["tenant_id"]),
                decision=ReconciliationDecision(str(row["decision"])),
                resolved_by=str(row["resolved_by"]),
                resolved_at=row["resolved_at"],
                note=row["note"],
            )
            if row is not None
            else None
        )

    async def all_for(self, tenant_id: str) -> list[Reconciliation]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                "SELECT * FROM tenant.tool_call_reconciliation WHERE tenant_id=$1",
                UUID(tenant_id),
            )
        return [
            Reconciliation(
                call_id=str(row["call_id"]),
                tenant_id=str(row["tenant_id"]),
                decision=ReconciliationDecision(str(row["decision"])),
                resolved_by=str(row["resolved_by"]),
                resolved_at=row["resolved_at"],
                note=row["note"],
            )
            for row in rows
        ]
