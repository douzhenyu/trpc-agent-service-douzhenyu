"""Health-only process boundary for production units implemented in later slices."""

from __future__ import annotations

import os
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict

from trpc_service.version import TRPC_AGENT_VERSION, __version__

RuntimeUnit = Literal["agent-gateway", "channel-gateway", "agent-worker", "job-worker"]
RUNTIME_UNITS: tuple[RuntimeUnit, ...] = (
    "agent-gateway",
    "channel-gateway",
    "agent-worker",
    "job-worker",
)


class RuntimeHealthResponse(BaseModel):
    """Stable health response observed by Kubernetes and rollout analysis."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    service: RuntimeUnit
    version: str
    trpc_agent_version: str


def _runtime_unit() -> RuntimeUnit:
    value = os.environ.get("PLATFORM_UNIT", "agent-gateway")
    if value not in RUNTIME_UNITS:
        raise RuntimeError(f"unsupported PLATFORM_UNIT: {value}")
    return value  # type: ignore[return-value]


def create_app(unit: RuntimeUnit | None = None) -> FastAPI:
    """Create the health boundary shared by the four data-plane skeletons."""
    service = unit or _runtime_unit()
    application = FastAPI(
        title=f"tRPC-Agent Platform {service}",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    def response() -> RuntimeHealthResponse:
        return RuntimeHealthResponse(
            service=service,
            version=__version__,
            trpc_agent_version=TRPC_AGENT_VERSION,
        )

    @application.get("/health/live", response_model=RuntimeHealthResponse)
    async def live() -> RuntimeHealthResponse:
        return response()

    @application.get("/health/ready", response_model=RuntimeHealthResponse)
    async def ready() -> RuntimeHealthResponse:
        return response()

    return application


app = create_app()
