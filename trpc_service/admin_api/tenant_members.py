"""Tenant member registration and least-privilege role assignment."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.idempotency import remember, replay_for
from trpc_service.admin_api.tenant_access import require_tenant_access, require_tenant_admin
from trpc_service.ids import uuid7

TenantMemberRole = Literal["TENANT_ADMIN", "AGENT_DEVELOPER", "TENANT_AUDITOR"]


class TenantMemberResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    member_id: UUID
    user_id: UUID
    display_name: str
    email: str | None
    roles: list[TenantMemberRole]
    version: int


class TenantMemberList(BaseModel):
    model_config = ConfigDict(frozen=True)

    items: list[TenantMemberResponse]


def create_tenant_member_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}/members", tags=["identity"])

    @router.get(
        "",
        response_model=TenantMemberList,
        responses=error_responses(401, 403, 404),
    )
    async def list_members(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> TenantMemberList:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "tenant_member.list",
            target_type="tenant_member",
        )
        async with database.tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                """SELECT m.tenant_id,m.id member_id,m.user_id,m.version,
                u.display_name,u.email,
                coalesce(array_agg(r.role ORDER BY r.role)
                  FILTER (WHERE r.role IS NOT NULL),'{}') roles
                FROM tenant.member m
                JOIN platform.platform_user u ON u.id=m.user_id
                LEFT JOIN tenant.member_role r
                  ON r.tenant_id=m.tenant_id AND r.member_id=m.id
                WHERE m.tenant_id=$1
                GROUP BY m.tenant_id,m.id,m.user_id,m.version,u.display_name,u.email
                ORDER BY u.display_name,m.id""",
                tenant_id,
            )
        return TenantMemberList(
            items=[
                TenantMemberResponse(
                    tenant_id=row["tenant_id"],
                    member_id=row["member_id"],
                    user_id=row["user_id"],
                    display_name=row["display_name"],
                    email=row["email"],
                    roles=list(row["roles"]),
                    version=row["version"],
                )
                for row in rows
            ]
        )

    @router.put(
        "/{user_id}/roles/{role}",
        response_model=TenantMemberResponse,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def assign_member_role(
        tenant_id: UUID,
        user_id: UUID,
        role: TenantMemberRole,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> TenantMemberResponse:
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "tenant_member.role.assign",
            target_type="platform_user",
            target_id=str(user_id),
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            "user_id": str(user_id),
            "role": role,
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="tenant_member.role.assign",
                payload=request_payload,
            )
            if replayed is not None:
                return TenantMemberResponse.model_validate(replayed)

            user = await connection.fetchrow(
                "SELECT id,display_name,email FROM platform.platform_user WHERE id=$1",
                user_id,
            )
            if user is None:
                raise HTTPException(status_code=404, detail="platform user not found")

            member = await connection.fetchrow(
                """INSERT INTO tenant.member (tenant_id,id,user_id)
                VALUES ($1,$2,$3)
                ON CONFLICT (tenant_id,user_id) DO UPDATE SET user_id=EXCLUDED.user_id
                RETURNING id,version""",
                tenant_id,
                uuid7(),
                user_id,
            )
            assert member is not None
            inserted = await connection.fetchval(
                """INSERT INTO tenant.member_role (tenant_id,id,member_id,role)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT (tenant_id,member_id,role) DO NOTHING
                RETURNING id""",
                tenant_id,
                uuid7(),
                member["id"],
                role,
            )
            version = int(member["version"])
            if inserted is not None:
                version = int(
                    await connection.fetchval(
                        """UPDATE tenant.member SET version=version+1
                        WHERE tenant_id=$1 AND id=$2 RETURNING version""",
                        tenant_id,
                        member["id"],
                    )
                )
                await insert_audit(
                    connection,
                    principal,
                    "tenant_member.role.assign",
                    "ALLOW",
                    target_type="tenant_member",
                    target_id=str(member["id"]),
                    tenant_id=tenant_id,
                    details={"user_id": str(user_id), "role": role},
                )

            role_rows = await connection.fetch(
                """SELECT role FROM tenant.member_role
                WHERE tenant_id=$1 AND member_id=$2 ORDER BY role""",
                tenant_id,
                member["id"],
            )
            response = TenantMemberResponse(
                tenant_id=tenant_id,
                member_id=member["id"],
                user_id=user_id,
                display_name=user["display_name"],
                email=user["email"],
                roles=[row["role"] for row in role_rows],
                version=version,
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="tenant_member.role.assign",
                payload=request_payload,
                response=response.model_dump(mode="json"),
            )
        return response

    return router
