"""Public Admin API response models."""

from datetime import datetime
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field


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


DraftReference = Annotated[str, Field(max_length=128)]
ModelAlias = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")]
ModelFallbackAlias = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")]
SecretReference = Annotated[
    str,
    Field(
        pattern=r"^vault://tenant/[0-9a-f-]{36}/[a-z0-9][a-z0-9/_-]{0,255}#[A-Za-z0-9_.-]{1,64}$",
        max_length=384,
    ),
]


def _endpoint_without_credentials(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("endpoint_url must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("endpoint_url must not contain query or fragment data")
    return value


ModelEndpoint = Annotated[
    str,
    Field(pattern=r"^https?://[^\s]{1,480}$", max_length=512),
    AfterValidator(_endpoint_without_credentials),
]
ModelRegion = Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{1,62}$", max_length=63)]
DataClassification = Literal["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"]


class ModelProfileCreate(BaseModel):
    alias: ModelAlias
    provider_model: str = Field(min_length=1, max_length=256)
    endpoint_url: ModelEndpoint
    secret_ref: SecretReference
    data_classification: DataClassification
    region: ModelRegion
    fallback_aliases: list[ModelFallbackAlias] = Field(default_factory=list, max_length=10)
    requests_per_minute: int = Field(default=60, ge=1, le=100_000)


class ModelProfileUpdate(BaseModel):
    provider_model: str | None = Field(default=None, min_length=1, max_length=256)
    endpoint_url: ModelEndpoint | None = None
    secret_ref: SecretReference | None = None
    data_classification: DataClassification | None = None
    region: ModelRegion | None = None
    fallback_aliases: list[ModelFallbackAlias] | None = Field(default=None, max_length=10)
    requests_per_minute: int | None = Field(default=None, ge=1, le=100_000)


class ModelProfileResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    alias: str
    provider_model: str
    endpoint_url: str
    secret_ref: str
    data_classification: DataClassification
    region: str
    fallback_aliases: list[str]
    requests_per_minute: int
    version: int
    created_at: datetime
    updated_at: datetime


class ModelProfileList(BaseModel):
    items: list[ModelProfileResponse]
    next_cursor: str | None = None


class AgentDraftCreate(BaseModel):
    instructions: str = Field(default="", max_length=100_000)
    model_alias: str = Field(default="", max_length=128)
    tool_aliases: list[DraftReference] = Field(default_factory=list, max_length=100)
    knowledge_refs: list[DraftReference] = Field(default_factory=list, max_length=100)
    governance_policy_ref: str | None = Field(default=None, max_length=128)


class AgentDraftUpdate(BaseModel):
    instructions: str | None = Field(default=None, max_length=100_000)
    model_alias: str | None = Field(default=None, max_length=128)
    tool_aliases: list[DraftReference] | None = Field(default=None, max_length=100)
    knowledge_refs: list[DraftReference] | None = Field(default=None, max_length=100)
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


class AgentReleaseCreate(BaseModel):
    data_classification: DataClassification
    region: ModelRegion
    fallback_aliases: list[ModelFallbackAlias] = Field(default_factory=list, max_length=10)


class AgentReleaseSource(BaseModel):
    draft_version: int | None = Field(default=None, ge=1)
    actor: str = Field(min_length=1, max_length=255)
    kind: Literal["DRAFT", "LEGACY"]


class AgentReleaseResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    application_id: UUID
    version: int = Field(ge=1)
    model_alias: str
    data_classification: DataClassification
    region: str
    fallback_aliases: list[str]
    model_profiles: list[dict[str, Any]]
    source: AgentReleaseSource
    draft_snapshot: dict[str, Any]
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime


class AgentReleaseList(BaseModel):
    items: list[AgentReleaseResponse]
    next_cursor: str | None = None


DeploymentEnvironment = Literal["DEVELOPMENT", "STAGING", "PRODUCTION"]
DeploymentStatus = Literal["PENDING_APPROVAL", "ACTIVE"]


class AgentDeploymentCreate(BaseModel):
    environment: DeploymentEnvironment
    release_id: UUID
    rollout_percentage: int = Field(ge=1, le=100)


class AgentDeploymentRollback(BaseModel):
    release_id: UUID


class AgentDeploymentResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    application_id: UUID
    environment: DeploymentEnvironment
    release_id: UUID
    previous_release_id: UUID | None
    rollout_percentage: int = Field(ge=1, le=100)
    status: DeploymentStatus
    initiator: str
    approver: str | None
    version: int = Field(ge=1)
    created_at: datetime
    activated_at: datetime | None


class AgentDeploymentList(BaseModel):
    items: list[AgentDeploymentResponse]
    next_cursor: str | None = None


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


class BudgetConfigUpsert(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scope: Literal["TENANT_MONTHLY", "AGENT_DAILY", "EXECUTION"]
    application_id: UUID | None = None
    limit_micros: int = Field(ge=0)
    contingency_micros: int = Field(default=0, ge=0)
    unknown_policy: Literal["FAIL_CLOSED", "CONTINGENCY"] = "FAIL_CLOSED"


class BudgetResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    tenant_id: UUID
    application_id: UUID | None
    scope: str
    limit_micros: int
    contingency_micros: int
    unknown_policy: str
    version: int
    period_key: str | None = None
    reserved_micros: int | None = None
    consumed_micros: int | None = None
    allowance_micros: int | None = None
    contingency_reserved_micros: int | None = None


class BudgetList(BaseModel):
    items: list[BudgetResponse]
    next_cursor: str | None = None


class BudgetAdjustmentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delta_micros: int
    reason: str = Field(min_length=1, max_length=500)


class LedgerEntryResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    budget_id: UUID
    period_key: str
    application_id: UUID | None
    execution_id: str | None
    entry_type: str
    amount_micros: int
    reserve_id: UUID | None
    contingency: bool
    reason: str
    actor: str
    price_version: int | None
    created_at: datetime


class LedgerList(BaseModel):
    items: list[LedgerEntryResponse]
    next_cursor: str | None = None


class BudgetAlertResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    budget_id: UUID
    period_key: str
    level: str
    used_micros: int
    limit_micros: int
    created_at: datetime


class BudgetAlertList(BaseModel):
    items: list[BudgetAlertResponse]
    next_cursor: str | None = None


class ModelPriceEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_alias: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    input_micros_per_1k: int = Field(ge=0)
    output_micros_per_1k: int = Field(ge=0)


class ModelPriceSet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prices: list[ModelPriceEntry] = Field(min_length=1, max_length=100)


class ModelPriceVersionResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    version: int
    prices: list[ModelPriceEntry]


class PolicyBundleRulesUpsert(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rules: dict[str, Any] = Field(min_length=1)


class PolicyBundleActivate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    canary_percentage: int = Field(default=0, ge=0, le=100)


class PolicyBundleResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    version: int
    rules: dict[str, Any]
    signature: str
    status: str
    canary_percentage: int
    created_by: str
    created_at: datetime
    activated_at: datetime | None


class PolicyBundleList(BaseModel):
    items: list[PolicyBundleResponse]
    next_cursor: str | None = None
