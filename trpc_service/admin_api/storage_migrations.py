"""Approved, auditable online Storage Profile migration API."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.schemas import (
    StorageMigrationAdvance,
    StorageMigrationApproval,
    StorageMigrationCreate,
    StorageMigrationList,
    StorageMigrationResponse,
    StorageMigrationRollbackApproval,
)
from trpc_service.admin_api.tenant_access import require_tenant_admin
from trpc_service.ids import uuid7
from trpc_service.storage_migration import (
    MigrationOperation,
    StorageMigrationError,
    StorageMigrationState,
    StorageMigrationStateMachine,
)

_COLUMNS = (
    "id,tenant_id,source_profile_id,target_profile_id,state,approval_status,requested_by,"
    "approved_by,approved_at,rollback_approval_status,validation,observation_seconds,"
    "rollback_requested_by,rollback_approved_by,rollback_approved_at,observation_ends_at,"
    "version,created_at,updated_at"
)


def _result(row: Any) -> dict[str, Any]:
    result = record_to_dict(row)
    if isinstance(result["validation"], str):
        result["validation"] = json.loads(result["validation"])
    return result


def _machine(row: Any) -> StorageMigrationStateMachine:
    return StorageMigrationStateMachine(
        str(row["source_profile_id"]),
        str(row["target_profile_id"]),
        state=StorageMigrationState(str(row["state"])),
    )


async def _migration(connection: Any, migration_id: UUID, *, for_update: bool = False) -> Any:
    lock = " FOR UPDATE" if for_update else ""
    row = await connection.fetchrow(
        f"SELECT {_COLUMNS} FROM tenant.storage_migration WHERE id=$1{lock}", migration_id
    )
    if row is None:
        raise HTTPException(status_code=404, detail="STORAGE_MIGRATION_NOT_FOUND")
    return row


def _transition_error(error: StorageMigrationError) -> HTTPException:
    return HTTPException(status_code=409, detail=error.code)


def create_storage_migration_router(database: Database) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1/tenants/{tenant_id}/storage-migrations", tags=["storage-migrations"]
    )

    @router.post(
        "",
        response_model=StorageMigrationResponse,
        status_code=201,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def create_migration(
        tenant_id: UUID,
        payload: StorageMigrationCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.request",
            target_type="storage_migration",
        )
        migration_id = uuid7()
        async with database.tenant_transaction(tenant_id) as connection:
            source_id = await connection.fetchval(
                "SELECT id FROM tenant.storage_profile WHERE tenant_id=$1 AND active", tenant_id
            )
            target = await connection.fetchrow(
                "SELECT id,active FROM tenant.storage_profile WHERE tenant_id=$1 AND id=$2",
                tenant_id,
                payload.target_profile_id,
            )
            if source_id is None or target is None:
                raise HTTPException(status_code=404, detail="STORAGE_PROFILE_NOT_FOUND")
            if UUID(str(source_id)) == payload.target_profile_id or bool(target["active"]):
                raise HTTPException(status_code=409, detail="STORAGE_MIGRATION_INVALID_PROFILES")
            row = await connection.fetchrow(
                f"""INSERT INTO tenant.storage_migration
                (tenant_id,id,source_profile_id,target_profile_id,state,approval_status,requested_by,
                observation_seconds) VALUES ($1,$2,$3,$4,'PREPARED','PENDING',$5,$6)
                RETURNING {_COLUMNS}""",
                tenant_id,
                migration_id,
                source_id,
                payload.target_profile_id,
                principal.subject,
                payload.observation_seconds,
            )
            assert row is not None
            await insert_audit(
                connection,
                principal,
                "storage_migration.request",
                "ALLOW",
                target_type="storage_migration",
                target_id=str(migration_id),
                tenant_id=tenant_id,
                details={
                    "source_profile_id": str(source_id),
                    "target_profile_id": str(payload.target_profile_id),
                },
            )
        return _result(row)

    @router.get("", response_model=StorageMigrationList, responses=error_responses(401, 403))
    async def list_migrations(
        tenant_id: UUID, principal: Annotated[Principal, Depends(principal_from_request)]
    ) -> dict[str, list[dict[str, Any]]]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.list",
            target_type="storage_migration",
        )
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"SELECT {_COLUMNS} FROM tenant.storage_migration "
                "WHERE tenant_id=$1 ORDER BY created_at DESC",
                tenant_id,
            )
        return {"items": [_result(row) for row in rows]}

    @router.get(
        "/{migration_id}",
        response_model=StorageMigrationResponse,
        responses=error_responses(401, 403, 404),
    )
    async def get_migration(
        tenant_id: UUID,
        migration_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.get",
            target_type="storage_migration",
            target_id=str(migration_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            return _result(await _migration(connection, migration_id))

    @router.post(
        "/{migration_id}/approvals",
        response_model=StorageMigrationResponse,
        responses=error_responses(401, 403, 404, 409),
    )
    async def decide_migration_approval(
        tenant_id: UUID,
        migration_id: UUID,
        payload: StorageMigrationApproval,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.approve",
            target_type="storage_migration",
            target_id=str(migration_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await _migration(connection, migration_id, for_update=True)
            if str(row["approval_status"]) != "PENDING":
                raise HTTPException(
                    status_code=409, detail="STORAGE_MIGRATION_APPROVAL_ALREADY_DECIDED"
                )
            if principal.subject == str(row["requested_by"]):
                raise HTTPException(
                    status_code=409, detail="STORAGE_MIGRATION_SELF_APPROVAL_DENIED"
                )
            status = "APPROVED" if payload.decision == "APPROVE" else "DENIED"
            state = "PREPARED" if status == "APPROVED" else "FAILED"
            updated = await connection.fetchrow(
                f"""UPDATE tenant.storage_migration SET state=$3,approval_status=$4,approved_by=$5,
                approved_at=now(),version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2
                RETURNING {_COLUMNS}""",
                tenant_id,
                migration_id,
                state,
                status,
                principal.subject,
            )
            assert updated is not None
            await insert_audit(
                connection,
                principal,
                "storage_migration.approve",
                "ALLOW",
                target_type="storage_migration",
                target_id=str(migration_id),
                tenant_id=tenant_id,
                details={"decision": status, "state": state},
            )
        return _result(updated)

    @router.post(
        "/{migration_id}/start",
        response_model=StorageMigrationResponse,
        responses=error_responses(401, 403, 404, 409),
    )
    async def start_migration(
        tenant_id: UUID,
        migration_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        """Queue a worker-owned snapshot; no client can mark it complete."""

        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.start",
            target_type="storage_migration",
            target_id=str(migration_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await _migration(connection, migration_id, for_update=True)
            if str(row["approval_status"]) != "APPROVED":
                raise HTTPException(status_code=409, detail="STORAGE_MIGRATION_APPROVAL_REQUIRED")
            try:
                state = _machine(row).advance(MigrationOperation.START_BACKFILL)
            except StorageMigrationError as error:
                raise _transition_error(error) from error
            updated = await connection.fetchrow(
                f"""UPDATE tenant.storage_migration SET state=$3,version=version+1,updated_at=now()
                WHERE tenant_id=$1 AND id=$2 RETURNING {_COLUMNS}""",
                tenant_id,
                migration_id,
                state.value,
            )
            assert updated is not None
            await insert_audit(
                connection,
                principal,
                "storage_migration.start",
                "ALLOW",
                target_type="storage_migration",
                target_id=str(migration_id),
                tenant_id=tenant_id,
                details={"state": state.value},
            )
        return _result(updated)

    @router.post(
        "/{migration_id}/advance",
        response_model=StorageMigrationResponse,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def advance_migration(
        tenant_id: UUID,
        migration_id: UUID,
        payload: StorageMigrationAdvance,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.advance",
            target_type="storage_migration",
            target_id=str(migration_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await _migration(connection, migration_id, for_update=True)
            if str(row["approval_status"]) != "APPROVED":
                raise HTTPException(status_code=409, detail="STORAGE_MIGRATION_APPROVAL_REQUIRED")
            operation = MigrationOperation(payload.operation)
            if operation is MigrationOperation.COMPLETE:
                ends_at = row["observation_ends_at"]
                if ends_at is None or ends_at > datetime.now(UTC):
                    raise HTTPException(
                        status_code=409, detail="STORAGE_MIGRATION_OBSERVATION_REQUIRED"
                    )
            machine = _machine(row)
            try:
                state = machine.advance(operation)
            except StorageMigrationError as error:
                raise _transition_error(error) from error
            observation_ends_at = (
                datetime.now(UTC) + timedelta(seconds=int(row["observation_seconds"]))
                if operation is MigrationOperation.SWITCH
                else row["observation_ends_at"]
            )
            if operation is MigrationOperation.SWITCH:
                await connection.execute(
                    "UPDATE tenant.storage_profile SET active=false,version=version+1,"
                    "updated_at=now() WHERE tenant_id=$1 AND id=$2",
                    tenant_id,
                    row["source_profile_id"],
                )
                await connection.execute(
                    "UPDATE tenant.storage_profile SET active=true,version=version+1,"
                    "updated_at=now() WHERE tenant_id=$1 AND id=$2",
                    tenant_id,
                    row["target_profile_id"],
                )
            updated = await connection.fetchrow(
                f"""UPDATE tenant.storage_migration SET state=$3,validation=CAST($4 AS jsonb),
                observation_ends_at=$5,version=version+1,updated_at=now()
                WHERE tenant_id=$1 AND id=$2
                RETURNING {_COLUMNS}""",
                tenant_id,
                migration_id,
                state.value,
                json.dumps(_result(row)["validation"]),
                observation_ends_at,
            )
            assert updated is not None
            await insert_audit(
                connection,
                principal,
                "storage_migration.advance",
                "ALLOW",
                target_type="storage_migration",
                target_id=str(migration_id),
                tenant_id=tenant_id,
                details={"operation": operation.value, "state": state.value},
            )
        return _result(updated)

    @router.post(
        "/{migration_id}/rollback-approvals",
        response_model=StorageMigrationResponse,
        responses=error_responses(401, 403, 404, 409),
    )
    async def decide_rollback_approval(
        tenant_id: UUID,
        migration_id: UUID,
        payload: StorageMigrationRollbackApproval,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.rollback_approval",
            target_type="storage_migration",
            target_id=str(migration_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await _migration(connection, migration_id, for_update=True)
            if str(row["state"]) != StorageMigrationState.OBSERVING.value:
                raise HTTPException(
                    status_code=409, detail="STORAGE_MIGRATION_ROLLBACK_UNAVAILABLE"
                )
            current = str(row["rollback_approval_status"])
            values: tuple[str, str | None, str | None, datetime | None]
            if payload.decision == "REQUEST":
                if current != "NONE":
                    raise HTTPException(
                        status_code=409, detail="STORAGE_MIGRATION_ROLLBACK_ALREADY_REQUESTED"
                    )
                values = ("PENDING", principal.subject, None, None)
            else:
                if current != "PENDING" or principal.subject == str(row["rollback_requested_by"]):
                    raise HTTPException(
                        status_code=409, detail="STORAGE_MIGRATION_ROLLBACK_APPROVAL_DENIED"
                    )
                values = (
                    "APPROVED" if payload.decision == "APPROVE" else "DENIED",
                    None,
                    principal.subject,
                    datetime.now(UTC),
                )
            updated = await connection.fetchrow(
                f"""UPDATE tenant.storage_migration SET rollback_approval_status=$3,
                rollback_requested_by=COALESCE($4,rollback_requested_by),rollback_approved_by=$5,
                rollback_approved_at=$6,version=version+1,updated_at=now()
                WHERE tenant_id=$1 AND id=$2
                RETURNING {_COLUMNS}""",
                tenant_id,
                migration_id,
                *values,
            )
            assert updated is not None
            await insert_audit(
                connection,
                principal,
                "storage_migration.rollback_approval",
                "ALLOW",
                target_type="storage_migration",
                target_id=str(migration_id),
                tenant_id=tenant_id,
                details={"decision": payload.decision},
            )
        return _result(updated)

    @router.post(
        "/{migration_id}/rollback",
        response_model=StorageMigrationResponse,
        responses=error_responses(401, 403, 404, 409),
    )
    async def rollback_migration(
        tenant_id: UUID,
        migration_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, Any]:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "storage_migration.rollback",
            target_type="storage_migration",
            target_id=str(migration_id),
        )
        async with database.tenant_transaction(tenant_id) as connection:
            row = await _migration(connection, migration_id, for_update=True)
            if str(row["rollback_approval_status"]) != "APPROVED":
                raise HTTPException(
                    status_code=409, detail="STORAGE_MIGRATION_ROLLBACK_APPROVAL_REQUIRED"
                )
            try:
                state = _machine(row).advance(MigrationOperation.START_ROLLBACK)
            except StorageMigrationError as error:
                raise _transition_error(error) from error
            updated = await connection.fetchrow(
                f"""UPDATE tenant.storage_migration SET state=$3,version=version+1,updated_at=now()
                WHERE tenant_id=$1 AND id=$2 RETURNING {_COLUMNS}""",
                tenant_id,
                migration_id,
                state.value,
            )
            assert updated is not None
            await insert_audit(
                connection,
                principal,
                "storage_migration.rollback",
                "ALLOW",
                target_type="storage_migration",
                target_id=str(migration_id),
                tenant_id=tenant_id,
                details={"state": state.value, "fenced_profile_id": str(row["target_profile_id"])},
            )
        return _result(updated)

    return router
