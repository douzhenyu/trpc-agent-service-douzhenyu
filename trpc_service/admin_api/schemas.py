"""Public Admin API response models."""

from typing import Literal

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    """Stable public health contract consumed by the Web Console."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    service: Literal["admin-api"] = "admin-api"
    version: str
    trpc_agent_version: str
