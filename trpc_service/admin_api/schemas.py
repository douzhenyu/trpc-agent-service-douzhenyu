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


class AgentApplicationCreate(BaseModel):
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)


class AgentApplicationUpdate(BaseModel):
    slug: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class AgentApplicationResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    slug: str
    name: str
    description: str
    version: int
    created_at: datetime
    updated_at: datetime


class AgentApplicationList(BaseModel):
    items: list[AgentApplicationResponse]
    next_cursor: str | None = None


class AgentDraftCreate(BaseModel):
    instructions: str = Field(default="", max_length=100_000)
    model_alias: str = Field(default="", max_length=128)
    tool_aliases: list[str] = Field(default_factory=list, max_length=100)
    knowledge_refs: list[str] = Field(default_factory=list, max_length=100)
    governance_policy_ref: str | None = Field(default=None, max_length=128)


class AgentDraftUpdate(BaseModel):
    instructions: str | None = Field(default=None, max_length=100_000)
    model_alias: str | None = Field(default=None, max_length=128)
    tool_aliases: list[str] | None = Field(default=None, max_length=100)
    knowledge_refs: list[str] | None = Field(default=None, max_length=100)
    governance_policy_ref: str | None = Field(default=None, max_length=128)


class AgentDraftResponse(BaseModel):
    tenant_id: UUID
    application_id: UUID
    instructions: str
    model_alias: str
    tool_aliases: list[str]
    knowledge_refs: list[str]
    governance_policy_ref: str | None
    lifecycle: Literal["DRAFT"] = "DRAFT"
    serves_production_traffic: Literal[False] = False
    version: int
    created_at: datetime
    updated_at: datetime


DraftIssueCode = Literal[
    "DRAFT_INSTRUCTIONS_REQUIRED",
    "DRAFT_MODEL_ALIAS_INVALID",
    "DRAFT_DUPLICATE_TOOL_ALIAS",
    "DRAFT_DUPLICATE_KNOWLEDGE_REF",
    "DRAFT_GOVERNANCE_POLICY_REF_INVALID",
]


class DraftValidationIssue(BaseModel):
    code: DraftIssueCode
    path: str
    message: str


class DraftValidationResponse(BaseModel):
    valid: bool
    draft_version: int
    issues: list[DraftValidationIssue]


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


ErrorCode = Literal[
    "INVALID_REQUEST",
    "INVALID_CREDENTIALS",
    "IDENTITY_PROVIDER_UNAVAILABLE",
    "UNAUTHENTICATED",
    "FORBIDDEN",
    "NOT_FOUND",
    "CONFLICT",
    "IDEMPOTENCY_CONFLICT",
    "VERSION_MISMATCH",
    "VALIDATION_ERROR",
    "INTERNAL_ERROR",
    "REQUEST_FAILED",
]


class ErrorDetail(BaseModel):
    code: ErrorCode
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
