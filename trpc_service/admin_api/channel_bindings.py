"""Channel binding administration: register and resolve bot-to-application routes."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.channels.bindings import (
    ChannelBinding,
    ChannelBindingConflict,
    ChannelBindingRegistry,
    ChannelBindingStatus,
    SecretRefRejected,
)
from trpc_service.channels.store import DatabaseBindingStore


class ChannelBindingUpsert(BaseModel):
    model_config = ConfigDict(frozen=True)

    channel_type: str = Field(pattern=r"^(FAKE|WECOM|FEISHU)$")
    external_bot_id: str = Field(min_length=1, max_length=128)
    application_id: UUID
    environment: str = Field(pattern=r"^(DEVELOPMENT|STAGING|PRODUCTION)$")
    secret_ref: str = Field(min_length=1, max_length=512)


class ChannelBindingResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    binding_id: UUID
    tenant_id: UUID
    channel_type: str
    external_bot_id: str
    application_id: UUID
    environment: str
    secret_ref: str
    status: str


class BindingStatusChange(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: ChannelBindingStatus


class ChannelBindingList(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    bindings: list[ChannelBindingResponse]


def _response(binding: ChannelBinding) -> ChannelBindingResponse:
    return ChannelBindingResponse(
        binding_id=UUID(binding.binding_id),
        tenant_id=UUID(binding.tenant_id),
        channel_type=binding.channel_type,
        external_bot_id=binding.external_bot_id,
        application_id=UUID(binding.application_id),
        environment=binding.environment,
        secret_ref=binding.secret_ref,
        status=str(binding.status),
    )


def create_channel_binding_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}", tags=["channels"])

    def _store() -> DatabaseBindingStore:
        return DatabaseBindingStore(database)

    @router.put(
        "/channel-bindings",
        response_model=ChannelBindingResponse,
        responses={**error_responses(401, 403, 409, 422)},
    )
    async def register_binding(
        tenant_id: UUID,
        payload: ChannelBindingUpsert,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> ChannelBindingResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "channel_binding.register",
            target_type="channel_binding",
            target_id=payload.external_bot_id,
        )
        try:
            binding = ChannelBinding(
                tenant_id=str(tenant_id),
                binding_id=str(uuid4()),
                channel_type=payload.channel_type,
                external_bot_id=payload.external_bot_id,
                application_id=str(payload.application_id),
                environment=payload.environment,
                secret_ref=payload.secret_ref,
            )
        except SecretRefRejected as error:
            raise HTTPException(status_code=422, detail="SECRET_REF_REJECTED") from error
        try:
            stored = await _store().insert(binding, created_by=principal.subject)
        except ChannelBindingConflict as error:
            raise HTTPException(status_code=409, detail="CHANNEL_BINDING_CONFLICT") from error
        async with database.tenant_transaction(tenant_id) as connection:
            await insert_audit(
                connection,
                principal,
                "channel_binding.register",
                "ALLOW",
                target_type="channel_binding",
                target_id=stored.binding_id,
                tenant_id=tenant_id,
                details={"channel_type": stored.channel_type},
            )
        return _response(stored)

    @router.post(
        "/channel-bindings/{binding_id}/status-changes",
        response_model=ChannelBindingResponse,
        responses={**error_responses(401, 403, 404, 422)},
    )
    async def change_status(
        tenant_id: UUID,
        binding_id: UUID,
        payload: BindingStatusChange,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> ChannelBindingResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "channel_binding.status",
            target_type="channel_binding",
            target_id=str(binding_id),
        )
        updated = await _store().set_status(str(tenant_id), str(binding_id), payload.status)
        if updated is None:
            raise HTTPException(status_code=404, detail="CHANNEL_BINDING_NOT_FOUND")
        async with database.tenant_transaction(tenant_id) as connection:
            await insert_audit(
                connection,
                principal,
                "channel_binding.status",
                "ALLOW",
                target_type="channel_binding",
                target_id=str(binding_id),
                tenant_id=tenant_id,
                details={"status": payload.status.value},
            )
        return _response(updated)

    @router.get(
        "/channel-bindings",
        response_model=ChannelBindingList,
        responses={**error_responses(401, 403)},
    )
    async def list_bindings(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
    ) -> ChannelBindingList:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "channel_binding.list",
            target_type="channel_binding",
        )
        bindings = await _store().list(str(tenant_id))
        return ChannelBindingList(
            tenant_id=tenant_id, bindings=[_response(binding) for binding in bindings]
        )

    @router.get(
        "/channel-bindings/resolve",
        response_model=ChannelBindingResponse,
        responses={**error_responses(401, 403, 404)},
    )
    async def resolve_binding(
        tenant_id: UUID,
        principal: Annotated[Principal, Depends(principal_from_request)],
        channel_type: str,
        external_bot_id: str,
    ) -> ChannelBindingResponse:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "read",
            "channel_binding.resolve",
            target_type="channel_binding",
            target_id=external_bot_id,
        )
        registry = ChannelBindingRegistry(_store())
        binding = await registry.resolve(
            tenant_id=str(tenant_id),
            channel_type=channel_type,
            external_bot_id=external_bot_id,
        )
        if binding is None:
            raise HTTPException(status_code=404, detail="CHANNEL_BINDING_NOT_FOUND")
        return _response(binding)

    return router
