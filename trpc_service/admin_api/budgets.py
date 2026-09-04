"""Tenant budget configuration, cost ledger attribution and alert surfaces."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.http_contract import ETAG_HEADER, error_responses
from trpc_service.admin_api.idempotency import remember, replay_for
from trpc_service.admin_api.pagination import decode_cursor, encode_cursor
from trpc_service.admin_api.schemas import (
    BudgetAdjustmentRequest,
    BudgetAlertList,
    BudgetAlertResponse,
    BudgetConfigUpsert,
    BudgetList,
    BudgetResponse,
    LedgerEntryResponse,
    LedgerList,
    ModelPriceSet,
    ModelPriceVersionResponse,
)
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.budgets import BudgetService
from trpc_service.ids import uuid7

_BUDGET_COLUMNS = """id,tenant_id,application_id,scope,limit_micros,contingency_micros,
unknown_policy,version,created_at,updated_at"""
_LEDGER_COLUMNS = """id,budget_id,period_key,application_id,execution_id,entry_type,
amount_micros,reserve_id,contingency,reason,actor,price_version,created_at"""
_SCOPE_COALESCE = "coalesce(application_id, '00000000-0000-0000-0000-000000000000'::uuid)"


def _budget_response(row: Any, period_state: dict[str, Any] | None) -> BudgetResponse:
    payload = record_to_dict(row)
    if period_state is not None:
        payload.update(
            {
                "period_key": period_state["period_key"],
                "reserved_micros": int(period_state["reserved_micros"]),
                "consumed_micros": int(period_state["consumed_micros"]),
                "allowance_micros": int(period_state["allowance_micros"]),
                "contingency_reserved_micros": int(period_state["contingency_reserved_micros"]),
            }
        )
    return BudgetResponse(**payload)


def create_budget_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["budgets"])
    service = BudgetService(database)

    @router.put(
        "/budgets",
        response_model=BudgetResponse,
        responses={**error_responses(401, 403, 404, 409, 422), 200: {"headers": ETAG_HEADER}},
    )
    async def upsert_budget(
        tenant_id: UUID,
        payload: BudgetConfigUpsert,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> BudgetResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "budget.upsert",
            target_type="budget",
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            **payload.model_dump(mode="json"),
        }
        budget_id = uuid7()
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="budget.upsert",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                response.headers["ETag"] = f'"{replayed["version"]}"'
                return BudgetResponse(**replayed)
            if payload.application_id is not None:
                application = await connection.fetchval(
                    """SELECT id FROM tenant.agent_application
                    WHERE tenant_id=$1 AND id=$2""",
                    tenant_id,
                    payload.application_id,
                )
                if application is None:
                    raise HTTPException(status_code=404, detail="Agent application not found")
            inserted = await connection.fetchrow(
                f"""INSERT INTO tenant.budget
                (tenant_id,id,application_id,scope,limit_micros,contingency_micros,
                unknown_policy)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (tenant_id,scope,{_SCOPE_COALESCE}) DO NOTHING
                RETURNING {_BUDGET_COLUMNS}""",
                tenant_id,
                budget_id,
                payload.application_id,
                payload.scope,
                payload.limit_micros,
                payload.contingency_micros,
                payload.unknown_policy,
            )
            if inserted is None:
                updated = await connection.fetchrow(
                    f"""UPDATE tenant.budget SET
                    limit_micros=$4,contingency_micros=$5,unknown_policy=$6,
                    version=version+1,updated_at=now()
                    WHERE tenant_id=$1 AND scope=$2
                      AND application_id IS NOT DISTINCT FROM $3
                    RETURNING {_BUDGET_COLUMNS}""",
                    tenant_id,
                    payload.scope,
                    payload.application_id,
                    payload.limit_micros,
                    payload.contingency_micros,
                    payload.unknown_policy,
                )
                assert updated is not None
                inserted = updated
            result = record_to_dict(inserted)
            await insert_audit(
                connection,
                principal,
                "budget.upsert",
                "ALLOW",
                target_type="budget",
                target_id=str(result["id"]),
                tenant_id=tenant_id,
                details={"scope": payload.scope, "limit_micros": payload.limit_micros},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="budget.upsert",
                payload=request_payload,
                response=result,
            )
        response.headers["ETag"] = f'"{result["version"]}"'
        return BudgetResponse(**result)

    @router.get("/budgets", response_model=BudgetList)
    async def list_budgets(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query()] = None,
    ) -> BudgetList:
        await require_tenant_access(
            database, principal, tenant_id, "read", "budget.list", target_type="budget"
        )
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT {_BUDGET_COLUMNS} FROM tenant.budget
                WHERE tenant_id=$1 AND (CAST($2 AS uuid) IS NULL OR id > $2)
                ORDER BY id LIMIT $3""",
                tenant_id,
                cursor_id,
                limit + 1,
            )
            states = {
                str(row["budget_id"]): row
                for row in await connection.fetch(
                    """SELECT budget_id,period_key,reserved_micros,consumed_micros,
                    allowance_micros,contingency_reserved_micros
                    FROM tenant.budget_period_state WHERE tenant_id=$1""",
                    tenant_id,
                )
            }
        page = rows[:limit]
        items = [_budget_response(row, states.get(str(row["id"]))) for row in page]
        return BudgetList(
            items=items,
            next_cursor=encode_cursor(page[-1]["id"]) if len(rows) > limit else None,
        )

    @router.post(
        "/budgets/{budget_id}/adjustments",
        response_model=BudgetResponse,
        responses={**error_responses(401, 403, 404, 422), 200: {"headers": ETAG_HEADER}},
    )
    async def adjust_budget(
        tenant_id: UUID,
        budget_id: UUID,
        payload: BudgetAdjustmentRequest,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> BudgetResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "budget.adjust",
            target_type="budget",
            target_id=str(budget_id),
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            "budget_id": str(budget_id),
            **payload.model_dump(mode="json"),
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="budget.adjust",
                payload=request_payload,
            )
        if replayed is not None:
            response.headers["Idempotency-Replayed"] = "true"
            response.headers["ETag"] = f'"{replayed["version"]}"'
            return BudgetResponse(**replayed)
        try:
            await service.adjust(
                tenant_id=str(tenant_id),
                budget_id=str(budget_id),
                delta_micros=payload.delta_micros,
                reason=payload.reason,
                actor=principal.subject,
            )
        except LookupError as error:
            raise HTTPException(status_code=404, detail="budget not found") from error
        async with database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                f"SELECT {_BUDGET_COLUMNS} FROM tenant.budget WHERE tenant_id=$1 AND id=$2",
                tenant_id,
                budget_id,
            )
            assert row is not None
            result = record_to_dict(row)
        response.headers["ETag"] = f'"{result["version"]}"'
        return BudgetResponse(**result)

    @router.get("/cost-ledger", response_model=LedgerList)
    async def list_cost_ledger(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        execution_id: Annotated[str | None, Query()] = None,
        budget_id: Annotated[UUID | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
        cursor: Annotated[str | None, Query()] = None,
    ) -> LedgerList:
        await require_tenant_access(
            database, principal, tenant_id, "read", "cost_ledger.list", target_type="budget"
        )
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                f"""SELECT {_LEDGER_COLUMNS} FROM tenant.cost_ledger
                WHERE tenant_id=$1
                  AND (CAST($2 AS uuid) IS NULL OR id > $2)
                  AND (CAST($3 AS text) IS NULL OR execution_id=$3)
                  AND (CAST($4 AS uuid) IS NULL OR budget_id=$4)
                ORDER BY created_at,id LIMIT $5""",
                tenant_id,
                cursor_id,
                execution_id,
                budget_id,
                limit + 1,
            )
        page = rows[:limit]
        items = [LedgerEntryResponse(**record_to_dict(row)) for row in page]
        return LedgerList(
            items=items,
            next_cursor=encode_cursor(page[-1]["id"]) if len(rows) > limit else None,
        )

    @router.get("/budget-alerts", response_model=BudgetAlertList)
    async def list_budget_alerts(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
        cursor: Annotated[str | None, Query()] = None,
    ) -> BudgetAlertList:
        await require_tenant_access(
            database, principal, tenant_id, "read", "budget_alerts.list", target_type="budget"
        )
        try:
            cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                """SELECT budget_id,period_key,level,used_micros,limit_micros,created_at
                FROM tenant.budget_alert
                WHERE tenant_id=$1 AND (CAST($2 AS uuid) IS NULL OR budget_id > $2)
                ORDER BY budget_id,period_key,level LIMIT $3""",
                tenant_id,
                cursor_id,
                limit + 1,
            )
        page = rows[:limit]
        items = [BudgetAlertResponse(**record_to_dict(row)) for row in page]
        return BudgetAlertList(
            items=items,
            next_cursor=encode_cursor(page[-1]["budget_id"]) if len(rows) > limit else None,
        )

    @router.put("/model-prices", response_model=ModelPriceVersionResponse)
    async def set_model_prices(
        tenant_id: UUID,
        payload: ModelPriceSet,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> ModelPriceVersionResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "model_price.set",
            target_type="model_price",
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            **payload.model_dump(mode="json"),
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="model_price.set",
                payload=request_payload,
            )
            if replayed is not None:
                return ModelPriceVersionResponse(**replayed)
            version = await connection.fetchval(
                "SELECT coalesce(max(version),0)+1 FROM tenant.model_price WHERE tenant_id=$1",
                tenant_id,
            )
            for entry in payload.prices:
                await connection.execute(
                    """INSERT INTO tenant.model_price
                    (tenant_id,version,model_alias,input_micros_per_1k,output_micros_per_1k)
                    VALUES ($1,$2,$3,$4,$5)""",
                    tenant_id,
                    version,
                    entry.model_alias,
                    entry.input_micros_per_1k,
                    entry.output_micros_per_1k,
                )
            await insert_audit(
                connection,
                principal,
                "model_price.set",
                "ALLOW",
                target_type="model_price",
                target_id=f"version-{version}",
                tenant_id=tenant_id,
                details={"version": int(version), "entries": len(payload.prices)},
            )
            result = {
                "tenant_id": str(tenant_id),
                "version": int(version),
                "prices": [entry.model_dump(mode="json") for entry in payload.prices],
            }
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="model_price.set",
                payload=request_payload,
                response=result,
            )
        return ModelPriceVersionResponse.model_validate(result)

    return router
