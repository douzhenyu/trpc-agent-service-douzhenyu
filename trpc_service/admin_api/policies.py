"""Governance Policy Bundle administration: create, activate, roll back, list."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.http_contract import ETAG_HEADER, error_responses
from trpc_service.admin_api.idempotency import remember, replay_for
from trpc_service.admin_api.schemas import (
    PolicyBundleActivate,
    PolicyBundleList,
    PolicyBundleResponse,
    PolicyBundleRulesUpsert,
)
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.governance import GovernanceRules, verify_bundle
from trpc_service.policy_bundles import PolicyBundleError, PolicyBundleService

_BUNDLE_FIELDS = (
    "version",
    "rules",
    "bundle",
    "signature",
    "status",
    "canary_percentage",
    "created_by",
    "created_at",
    "activated_at",
)


def _bundle_response(tenant_id: str, row: dict[str, Any]) -> PolicyBundleResponse:
    return PolicyBundleResponse(
        tenant_id=UUID(tenant_id),
        version=int(row["version"]),
        rules=row["rules"],
        signature=str(row["signature"]),
        status=str(row["status"]),
        canary_percentage=int(row["canary_percentage"]),
        created_by=str(row["created_by"]),
        created_at=row["created_at"],
        activated_at=row["activated_at"],
    )


def create_policy_router(database: Database, *, signing_key: str) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["governance"])

    def _service() -> PolicyBundleService:
        return PolicyBundleService(database, signing_key=signing_key)

    @router.put(
        "/policy-bundles",
        response_model=PolicyBundleResponse,
        responses={**error_responses(401, 403, 409, 422), 200: {"headers": ETAG_HEADER}},
    )
    async def create_bundle(
        tenant_id: UUID,
        payload: PolicyBundleRulesUpsert,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> PolicyBundleResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "policy_bundle.create",
            target_type="policy_bundle",
        )
        rules = GovernanceRules.model_validate(payload.rules)
        request_payload = {"tenant_id": str(tenant_id), "rules": rules.model_dump(mode="json")}
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="policy_bundle.create",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                response.headers["ETag"] = f'"{replayed["version"]}"'
                return _bundle_response(str(tenant_id), replayed)
        created = await _service().create_version(str(tenant_id), rules, actor=principal.subject)
        async with database.tenant_transaction(tenant_id) as connection:
            await insert_audit(
                connection,
                principal,
                "policy_bundle.create",
                "ALLOW",
                target_type="policy_bundle",
                target_id=str(created["version"]),
                tenant_id=tenant_id,
                details={"signature": created["signature"]},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="policy_bundle.create",
                payload=request_payload,
                response={field: created.get(field) for field in _BUNDLE_FIELDS},
            )
        response.headers["ETag"] = f'"{created["version"]}"'
        return _bundle_response(str(tenant_id), created)

    @router.post(
        "/policy-bundles/{version}/activations",
        response_model=PolicyBundleResponse,
        responses={**error_responses(401, 403, 404, 409, 422), 200: {"headers": ETAG_HEADER}},
    )
    async def activate_bundle(
        tenant_id: UUID,
        version: int,
        payload: PolicyBundleActivate,
        response: Response,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> PolicyBundleResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "policy_bundle.activate",
            target_type="policy_bundle",
            target_id=str(version),
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            "version": version,
            **payload.model_dump(mode="json"),
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="policy_bundle.activate",
                payload=request_payload,
            )
            if replayed is not None:
                response.headers["Idempotency-Replayed"] = "true"
                response.headers["ETag"] = f'"{replayed["version"]}"'
                return _bundle_response(str(tenant_id), replayed)
        try:
            activated = await _service().activate(
                str(tenant_id), version, canary_percentage=payload.canary_percentage
            )
        except PolicyBundleError as error:
            status = 404 if error.code == "POLICY_BUNDLE_NOT_FOUND" else 409
            raise HTTPException(status_code=status, detail=error.code) from error
        async with database.tenant_transaction(tenant_id) as connection:
            await insert_audit(
                connection,
                principal,
                "policy_bundle.activate",
                "ALLOW",
                target_type="policy_bundle",
                target_id=str(version),
                tenant_id=tenant_id,
                details={
                    "status": activated["status"],
                    "canary_percentage": payload.canary_percentage,
                },
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="policy_bundle.activate",
                payload=request_payload,
                response={field: activated.get(field) for field in _BUNDLE_FIELDS},
            )
        response.headers["ETag"] = f'"{activated["version"]}"'
        return _bundle_response(str(tenant_id), activated)

    @router.get("/policy-bundles", response_model=PolicyBundleList)
    async def list_bundles(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query()] = None,
    ) -> PolicyBundleList:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "policy_bundle.list",
            target_type="policy_bundle",
        )
        try:
            cursor_version = int(cursor) if cursor is not None else 0
        except ValueError as error:
            raise HTTPException(status_code=400, detail="invalid cursor") from error
        versions = await _service().list_versions(str(tenant_id))
        page = [row for row in versions if int(row["version"]) > cursor_version][:limit]
        items = [_bundle_response(str(tenant_id), row) for row in page]
        return PolicyBundleList(
            items=items,
            next_cursor=str(page[-1]["version"]) if len(page) == limit else None,
        )

    @router.get("/policy-bundles/{version}/verification")
    async def verify_bundle_signature(
        tenant_id: UUID,
        version: int,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> dict[str, bool]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "policy_bundle.verify",
            target_type="policy_bundle",
            target_id=str(version),
        )
        versions = await _service().list_versions(str(tenant_id))
        row = next((item for item in versions if int(item["version"]) == version), None)
        if row is None:
            raise HTTPException(status_code=404, detail="POLICY_BUNDLE_NOT_FOUND")
        rules = GovernanceRules.model_validate(row["rules"])
        return {"valid": verify_bundle(rules, version, str(row["signature"]), signing_key)}

    return router
