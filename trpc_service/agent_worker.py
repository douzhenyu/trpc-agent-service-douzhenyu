"""Agent Worker runtime module that routes released executions through LLM Gateway."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from trpc_service.llm_gateway import (
    DataClassification,
    GatewayModel,
    GatewayResult,
    LLMGateway,
    ModelGatewayError,
)


@dataclass(frozen=True)
class ReleaseRoute:
    release_id: str
    tenant_id: str
    model_alias: str
    data_classification: DataClassification
    region: str
    allowed_fallback_aliases: frozenset[str]


@dataclass(frozen=True)
class AgentExecutionRequest:
    tenant_id: str
    release_id: str
    messages: list[dict[str, str]]


class ReleaseRouteResolver(Protocol):
    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None: ...


class InMemoryReleaseRouteResolver:
    def __init__(self, routes: list[ReleaseRoute]) -> None:
        self._routes = {(route.tenant_id, route.release_id): route for route in routes}

    async def resolve(self, tenant_id: str, release_id: str) -> ReleaseRoute | None:
        return self._routes.get((tenant_id, release_id))


class AgentWorker:
    """Deep runtime module: callers supply only a released execution and messages."""

    def __init__(self, gateway: LLMGateway, releases: ReleaseRouteResolver) -> None:
        self._gateway, self._releases = gateway, releases

    async def complete(self, request: AgentExecutionRequest) -> GatewayResult:
        route = await self._releases.resolve(request.tenant_id, request.release_id)
        if route is None:
            raise ModelGatewayError("RELEASE_NOT_FOUND")
        return await GatewayModel(
            gateway=self._gateway,
            tenant_id=route.tenant_id,
            model_alias=route.model_alias,
            data_classification=route.data_classification,
            region=route.region,
            allowed_fallback_aliases=route.allowed_fallback_aliases,
        ).complete(request.messages)
