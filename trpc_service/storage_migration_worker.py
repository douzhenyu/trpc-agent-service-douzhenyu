"""Privileged, resumable executor for online Storage Profile migrations."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.database import Database
from trpc_service.storage_migration import (
    MigrationOperation,
    StorageMigrationError,
    StorageMigrationExecutor,
    StorageMigrationState,
    StorageMigrationStateMachine,
    ValidationEvidence,
)


class StorageMigrationWorker:
    """Advance only one durable phase per call, making retries safe and observable."""

    def __init__(self, database: Database, executor: StorageMigrationExecutor) -> None:
        self._database = database
        self._executor = executor

    async def run_once(self, tenant_id: UUID) -> bool:
        async with self._database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """SELECT * FROM tenant.storage_migration WHERE tenant_id=$1
                AND state IN ('BACKFILLING','CATCHING_UP','VALIDATING','ROLLING_BACK',
                'ROLLBACK_CATCHING_UP','ROLLBACK_VALIDATING')
                ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED""",
                tenant_id,
            )
            if row is None:
                return False
            try:
                await self._execute_phase(connection, tenant_id, row)
            except StorageMigrationError as error:
                await self._record_failure(connection, tenant_id, row, error.code)
            return True

    async def _execute_phase(self, connection: Any, tenant_id: UUID, row: Any) -> None:
        migration_id = row["id"]
        source_id = str(row["source_profile_id"])
        target_id = str(row["target_profile_id"])
        state = StorageMigrationState(str(row["state"]))
        checkpoint = await connection.fetchrow(
            """SELECT forward_watermarks,rollback_watermarks
            FROM tenant.storage_migration_checkpoint
            WHERE tenant_id=$1 AND migration_id=$2 FOR UPDATE""",
            tenant_id,
            migration_id,
        )
        if state is StorageMigrationState.BACKFILLING:
            watermarks = await self._executor.backfill(
                tenant_id=str(tenant_id), source_profile_id=source_id, target_profile_id=target_id
            )
            await self._upsert_checkpoint(connection, tenant_id, migration_id, forward=watermarks)
            await self._advance(
                connection, tenant_id, row, MigrationOperation.BACKFILL_COMPLETE, "backfill"
            )
            return
        if checkpoint is None:
            raise StorageMigrationError("STORAGE_MIGRATION_CHECKPOINT_MISSING")
        if state is StorageMigrationState.CATCHING_UP:
            watermarks = await self._executor.catch_up(
                tenant_id=str(tenant_id),
                source_profile_id=source_id,
                target_profile_id=target_id,
                watermarks=dict(checkpoint["forward_watermarks"]),
            )
            await self._upsert_checkpoint(connection, tenant_id, migration_id, forward=watermarks)
            await self._advance(
                connection, tenant_id, row, MigrationOperation.CATCH_UP_COMPLETE, "catch_up"
            )
            return
        if state is StorageMigrationState.VALIDATING:
            evidence = await self._executor.validate(
                tenant_id=str(tenant_id), source_profile_id=source_id, target_profile_id=target_id
            )
            if not evidence.is_valid():
                raise StorageMigrationError("STORAGE_MIGRATION_VALIDATION_FAILED")
            await self._advance(
                connection,
                tenant_id,
                row,
                MigrationOperation.VALIDATE,
                "validate",
                evidence=evidence.model_dump(),
            )
            return
        if state is StorageMigrationState.ROLLING_BACK:
            watermarks = await self._executor.backfill(
                tenant_id=str(tenant_id), source_profile_id=target_id, target_profile_id=source_id
            )
            await self._upsert_checkpoint(connection, tenant_id, migration_id, rollback=watermarks)
            await self._advance(
                connection,
                tenant_id,
                row,
                MigrationOperation.ROLLBACK_BACKFILL_COMPLETE,
                "rollback_backfill",
            )
            return
        if state is StorageMigrationState.ROLLBACK_CATCHING_UP:
            watermarks = await self._executor.catch_up(
                tenant_id=str(tenant_id),
                source_profile_id=target_id,
                target_profile_id=source_id,
                watermarks=dict(checkpoint["rollback_watermarks"] or {}),
            )
            await self._upsert_checkpoint(connection, tenant_id, migration_id, rollback=watermarks)
            await self._advance(
                connection,
                tenant_id,
                row,
                MigrationOperation.ROLLBACK_CATCH_UP_COMPLETE,
                "rollback_catch_up",
            )
            return
        assert state is StorageMigrationState.ROLLBACK_VALIDATING
        evidence = await self._executor.validate(
            tenant_id=str(tenant_id), source_profile_id=target_id, target_profile_id=source_id
        )
        if not evidence.is_valid():
            raise StorageMigrationError("STORAGE_MIGRATION_ROLLBACK_VALIDATION_FAILED")
        await connection.execute(
            "UPDATE tenant.storage_profile SET active=false,version=version+1,updated_at=now() "
            "WHERE tenant_id=$1 AND id=$2",
            tenant_id,
            row["target_profile_id"],
        )
        await connection.execute(
            "UPDATE tenant.storage_profile SET active=true,version=version+1,updated_at=now() "
            "WHERE tenant_id=$1 AND id=$2",
            tenant_id,
            row["source_profile_id"],
        )
        await self._advance(
            connection,
            tenant_id,
            row,
            MigrationOperation.ROLLBACK_VALIDATE,
            "rollback_switch",
            evidence=evidence.model_dump(),
        )

    async def _upsert_checkpoint(
        self,
        connection: Any,
        tenant_id: UUID,
        migration_id: UUID,
        *,
        forward: dict[str, int] | None = None,
        rollback: dict[str, int] | None = None,
    ) -> None:
        await connection.execute(
            """INSERT INTO tenant.storage_migration_checkpoint
            (tenant_id,migration_id,forward_watermarks,rollback_watermarks)
            VALUES ($1,$2,COALESCE(CAST($3 AS jsonb),'{}'::jsonb),
            COALESCE(CAST($4 AS jsonb),'{}'::jsonb))
            ON CONFLICT (tenant_id,migration_id) DO UPDATE SET
            forward_watermarks=COALESCE(CAST($3 AS jsonb),
              tenant.storage_migration_checkpoint.forward_watermarks),
            rollback_watermarks=COALESCE(CAST($4 AS jsonb),
              tenant.storage_migration_checkpoint.rollback_watermarks),
            updated_at=now()""",
            tenant_id,
            migration_id,
            json.dumps(forward) if forward is not None else None,
            json.dumps(rollback) if rollback is not None else None,
        )

    async def _advance(
        self,
        connection: Any,
        tenant_id: UUID,
        row: Any,
        operation: MigrationOperation,
        action: str,
        *,
        evidence: dict[str, Any] | None = None,
    ) -> None:
        machine = StorageMigrationStateMachine(
            str(row["source_profile_id"]),
            str(row["target_profile_id"]),
            state=StorageMigrationState(row["state"]),
        )
        state = machine.advance(
            operation,
            validation=ValidationEvidence.model_validate(evidence)
            if operation in {MigrationOperation.VALIDATE, MigrationOperation.ROLLBACK_VALIDATE}
            and evidence is not None
            else None,
        )
        await connection.execute(
            """UPDATE tenant.storage_migration SET state=$3,
            validation=COALESCE(CAST($4 AS jsonb),validation), version=version+1,
            updated_at=now() WHERE tenant_id=$1 AND id=$2""",
            tenant_id,
            row["id"],
            state.value,
            json.dumps(evidence) if evidence is not None else None,
        )
        await insert_audit(
            connection,
            None,
            f"storage_migration.worker.{action}",
            "ALLOW",
            target_type="storage_migration",
            target_id=str(row["id"]),
            tenant_id=tenant_id,
            details={"state": state.value, "operation": operation.value},
        )

    async def _record_failure(self, connection: Any, tenant_id: UUID, row: Any, code: str) -> None:
        state = StorageMigrationState(str(row["state"]))
        if state in {
            StorageMigrationState.ROLLING_BACK,
            StorageMigrationState.ROLLBACK_CATCHING_UP,
            StorageMigrationState.ROLLBACK_VALIDATING,
        }:
            # Target remains fenced as the sole writer; an operator can retry
            # instead of silently selecting a stale source.
            outcome = "RETRY_REQUIRED"
        else:
            outcome = StorageMigrationState.FAILED.value
            await connection.execute(
                "UPDATE tenant.storage_migration SET state='FAILED',version=version+1,"
                "updated_at=now() "
                "WHERE tenant_id=$1 AND id=$2",
                tenant_id,
                row["id"],
            )
        await insert_audit(
            connection,
            None,
            "storage_migration.worker.failure",
            "DENY",
            target_type="storage_migration",
            target_id=str(row["id"]),
            tenant_id=tenant_id,
            details={"error": code, "outcome": outcome},
        )
