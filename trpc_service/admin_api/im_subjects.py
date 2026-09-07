"""Administrative verification of same-tenant IM subject associations."""

from __future__ import annotations

from hashlib import sha256
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, model_validator

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Connection, Database
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.tenant_access import require_tenant_admin

_IM_SUBJECT_PATTERN = r"^im:(FAKE|WECOM|FEISHU):[^:]{1,64}:.{1,128}$"


class SubjectAssociationRequest(BaseModel):
    """A human-verified link, never an inference from equal external IDs."""

    model_config = ConfigDict(frozen=True)

    subject_id: str = Field(pattern=_IM_SUBJECT_PATTERN, max_length=256)
    related_subject_id: str = Field(pattern=_IM_SUBJECT_PATTERN, max_length=256)
    verification_reference: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def _different_subjects(self) -> SubjectAssociationRequest:
        if self.subject_id == self.related_subject_id:
            raise ValueError("IM_SUBJECT_ASSOCIATION_INVALID")
        return self


class SubjectAssociationResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    subject_id: str
    related_subject_id: str
    verification_reference: str
    verified_by: str
    verified_at: str


def _canonical_pair(subject_id: str, related_subject_id: str) -> tuple[str, str]:
    return tuple(sorted((subject_id, related_subject_id)))  # type: ignore[return-value]


def _association_audit_id(subject_id: str, related_subject_id: str) -> str:
    first, second = _canonical_pair(subject_id, related_subject_id)
    return sha256(f"{first}\n{second}".encode()).hexdigest()


def _binding_from_subject(subject_id: str) -> tuple[str, UUID]:
    _prefix, channel_type, binding_id, _external_user_id = subject_id.split(":", 3)
    try:
        return channel_type, UUID(binding_id)
    except ValueError as error:
        raise HTTPException(status_code=422, detail="IM_SUBJECT_ASSOCIATION_INVALID") from error


async def _verify_subject_bindings(
    connection: Connection, tenant_id: UUID, subject_ids: tuple[str, str]
) -> None:
    for subject_id in subject_ids:
        channel_type, binding_id = _binding_from_subject(subject_id)
        exists = await connection.fetchval(
            """SELECT EXISTS(SELECT 1 FROM tenant.channel_binding
            WHERE tenant_id=$1 AND binding_id=$2 AND channel_type=$3)""",
            tenant_id,
            binding_id,
            channel_type,
        )
        if not exists:
            raise HTTPException(status_code=422, detail="IM_SUBJECT_ASSOCIATION_INVALID")


def create_im_subject_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["im-subjects"])

    @router.put(
        "/im-subject-associations",
        response_model=SubjectAssociationResponse,
        responses={**error_responses(401, 403, 422)},
    )
    async def verify_association(
        tenant_id: UUID,
        payload: SubjectAssociationRequest,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> SubjectAssociationResponse:
        audit_id = _association_audit_id(payload.subject_id, payload.related_subject_id)
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "im_subject.associate",
            target_type="im_subject_association",
            target_id=audit_id,
        )
        subject_id, related_subject_id = _canonical_pair(
            payload.subject_id, payload.related_subject_id
        )
        async with database.tenant_transaction(tenant_id) as connection:
            await _verify_subject_bindings(connection, tenant_id, (subject_id, related_subject_id))
            row = await connection.fetchrow(
                """INSERT INTO tenant.im_subject_association
                (tenant_id,subject_id,related_subject_id,verification_reference,verified_by)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (tenant_id,subject_id,related_subject_id) DO UPDATE SET
                  verification_reference=EXCLUDED.verification_reference,
                  verified_by=EXCLUDED.verified_by,verified_at=now()
                RETURNING subject_id,related_subject_id,verification_reference,
                  verified_by,verified_at""",
                tenant_id,
                subject_id,
                related_subject_id,
                payload.verification_reference,
                principal.subject,
            )
            assert row is not None
            await insert_audit(
                connection,
                principal,
                "im_subject.associate",
                "ALLOW",
                target_type="im_subject_association",
                target_id=audit_id,
                tenant_id=tenant_id,
                details={
                    "subject_id": subject_id,
                    "related_subject_id": related_subject_id,
                    "verification_reference": payload.verification_reference,
                },
            )
        return SubjectAssociationResponse(
            tenant_id=tenant_id,
            subject_id=str(row["subject_id"]),
            related_subject_id=str(row["related_subject_id"]),
            verification_reference=str(row["verification_reference"]),
            verified_by=str(row["verified_by"]),
            verified_at=row["verified_at"].isoformat(),
        )

    @router.delete(
        "/im-subject-associations",
        status_code=204,
        response_model=None,
        responses={**error_responses(401, 403, 404, 422)},
    )
    async def revoke_association(
        tenant_id: UUID,
        subject_id: Annotated[str, Query(pattern=_IM_SUBJECT_PATTERN, max_length=256)],
        related_subject_id: Annotated[str, Query(pattern=_IM_SUBJECT_PATTERN, max_length=256)],
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> None:
        if subject_id == related_subject_id:
            raise HTTPException(status_code=422, detail="IM_SUBJECT_ASSOCIATION_INVALID")
        audit_id = _association_audit_id(subject_id, related_subject_id)
        await require_tenant_admin(
            database,
            principal,
            tenant_id,
            "im_subject.revoke",
            target_type="im_subject_association",
            target_id=audit_id,
        )
        first, second = _canonical_pair(subject_id, related_subject_id)
        async with database.tenant_transaction(tenant_id) as connection:
            removed = await connection.fetchval(
                """DELETE FROM tenant.im_subject_association
                WHERE tenant_id=$1 AND subject_id=$2 AND related_subject_id=$3
                RETURNING 1""",
                tenant_id,
                first,
                second,
            )
            if removed is None:
                raise HTTPException(status_code=404, detail="IM_SUBJECT_ASSOCIATION_NOT_FOUND")
            await insert_audit(
                connection,
                principal,
                "im_subject.revoke",
                "ALLOW",
                target_type="im_subject_association",
                target_id=audit_id,
                tenant_id=tenant_id,
                details={"subject_id": first, "related_subject_id": second},
            )

    return router
