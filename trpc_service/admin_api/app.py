"""FastAPI application for the Admin API deployment unit."""

from fastapi import FastAPI

from trpc_service.admin_api.schemas import HealthResponse
from trpc_service.version import TRPC_AGENT_VERSION, __version__


def create_app() -> FastAPI:
    application = FastAPI(
        title="tRPC-Agent Platform Admin API",
        version=__version__,
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )

    @application.get(
        "/api/v1/health",
        response_model=HealthResponse,
        tags=["platform"],
        summary="Report the public Admin API health state",
    )
    async def health() -> HealthResponse:
        return HealthResponse(
            version=__version__,
            trpc_agent_version=TRPC_AGENT_VERSION,
        )

    return application


app = create_app()
