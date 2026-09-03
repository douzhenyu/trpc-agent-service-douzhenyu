"""Public Admin API response models."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    """Stable public health contract consumed by the Web Console."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    service: Literal["admin-api"] = "admin-api"
    version: str
    trpc_agent_version: str


class EmergencyLoginRequest(BaseModel):
    username: str
    password: str


class SessionResponse(BaseModel):
    subject: str
    auth_method: Literal["oidc", "emergency"]
    roles: list[str]


class TenantCreate(BaseModel):
    slug: str
    name: str


class TenantResponse(BaseModel):
    id: UUID
    slug: str
    name: str
    status: str
    version: int
    created_at: datetime
    updated_at: datetime


class TenantList(BaseModel):
    items: list[TenantResponse]
    next_cursor: str | None = None


class TenantGroupCreate(BaseModel):
    name: str
    tenant_ids: list[UUID] = Field(default_factory=list)


class TenantGroupResponse(BaseModel):
    id: UUID
    name: str
    version: int
    tenant_ids: list[UUID]


class TenantGroupList(BaseModel):
    items: list[TenantGroupResponse]
    next_cursor: str | None = None


class PlatformUserCreate(BaseModel):
    issuer: str
    subject: str
    email: str | None = None
    display_name: str


class PlatformUserResponse(BaseModel):
    id: UUID
    issuer: str
    subject: str
    email: str | None
    display_name: str
    version: int
    roles: list[str]


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class PlatformUserList(BaseModel):
    items: list[PlatformUserResponse]
    next_cursor: str | None = None


class AuditEventResponse(BaseModel):
    id: UUID
    occurred_at: datetime
    tenant_id: UUID | None
    actor: str
    auth_method: str
    action: str
    decision: str
    target_type: str | None
    target_id: str | None
    details: dict[str, Any]


class AuditEventList(BaseModel):
    items: list[AuditEventResponse]
    next_cursor: str | None = None
