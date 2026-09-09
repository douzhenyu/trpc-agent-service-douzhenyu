"""Telemetry primitives: W3C trace context, attribute allowlist, metrics, SLO."""

from __future__ import annotations

import re
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from fastapi import Response

_W3C_TRACEPARENT = re.compile(
    r"^(?P<version>[0-9a-f]{2})-(?P<trace_id>[0-9a-f]{32})"
    r"-(?P<span_id>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})$"
)
_ZERO_TRACE_ID = "0" * 32
_ZERO_SPAN_ID = "0" * 16

BAGGAGE_PUBLIC_ALLOWLIST = frozenset({"public.tenant", "public.environment"})
_INTERNAL_BAGGAGE_KEY = re.compile(r"^(?!public\.)[A-Za-z0-9_\-*/@.]+$")

ATTRIBUTE_ALLOWLIST = frozenset(
    {
        "service.name",
        "http.request.method",
        "http.route",
        "http.response.status_code",
        "url.path",
        "tenant.id",
        "session.id",
        "execution.id",
        "application.id",
        "channel",
        "subject.id",
        "messaging.system",
        "messaging.destination.name",
        "error.type",
        "outcome",
        "kind",
        "status",
    }
)


def parse_traceparent(value: str | None) -> tuple[str, str, str] | None:
    """Parse a W3C traceparent into (trace_id, span_id, flags) or ``None``."""
    if value is None:
        return None
    match = _W3C_TRACEPARENT.match(value.strip().lower())
    if match is None:
        return None
    if match.group("version") == "ff":
        return None
    if match.group("trace_id") == _ZERO_TRACE_ID or match.group("span_id") == _ZERO_SPAN_ID:
        return None
    return match.group("trace_id"), match.group("span_id"), match.group("flags")


def child_traceparent(value: str | None) -> str | None:
    """Derive the outbound traceparent: same trace, fresh sampled child span."""
    parsed = parse_traceparent(value)
    if parsed is None:
        return None
    trace_id, _, flags = parsed
    span_id = secrets.token_hex(8)
    return f"00-{trace_id}-{span_id}-{'01' if flags != '00' else '00'}"


def _sampled_root_traceparent() -> str:
    """Create a valid sampled W3C root context for an untraced request."""
    trace_id = secrets.token_hex(16)
    while trace_id == _ZERO_TRACE_ID:
        trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    while span_id == _ZERO_SPAN_ID:
        span_id = secrets.token_hex(8)
    return f"00-{trace_id}-{span_id}-01"


def strip_internal_baggage(value: str | None) -> str:
    """Drop every baggage member except the explicitly public allowlist."""
    if not value:
        return ""
    kept: list[str] = []
    for member in value.split(","):
        stripped = member.strip()
        if not stripped:
            continue
        key = stripped.partition("=")[0].strip()
        if key in BAGGAGE_PUBLIC_ALLOWLIST:
            kept.append(stripped)
    return ",".join(kept)


def filter_attributes(attributes: dict[str, object]) -> dict[str, object]:
    """Keep only allowlisted span/log attributes and coerce values to primitives."""
    kept: dict[str, object] = {}
    for key, value in attributes.items():
        if key not in ATTRIBUTE_ALLOWLIST:
            continue
        if isinstance(value, bool | int | float | str) or value is None:
            kept[key] = value
        else:
            kept[key] = str(value)
    return kept


SLO_OBJECTIVES: dict[str, float] = {
    "data_plane": 99.95,
    "control_plane": 99.9,
}


def burn_rate(errors: int, total: int, objective: str) -> float | None:
    """Observed error-budget consumption rate; 1.0 means budget used exactly."""
    if total <= 0:
        return None
    target = SLO_OBJECTIVES[objective] / 100.0
    observed_success = (total - errors) / total
    budget = 1.0 - target
    return (1.0 - observed_success) / budget


# --- OpenTelemetry spans -------------------------------------------------
# The SDK stays optional: with no OTLP endpoint configured, every helper
# degrades to a no-op so units run lean without losing call-site shape.

_current_traceparent: ContextVar[str | None] = ContextVar("current_traceparent", default=None)

_OTLP_ENV_KEYS = ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")
_configured = False


def current_traceparent() -> str | None:
    """The active trace context of the request currently being served."""
    return _current_traceparent.get()


def _span_traceparent(span: object) -> str | None:
    """Serialize a valid OpenTelemetry span context as W3C traceparent."""
    get_context = getattr(span, "get_span_context", None)
    if get_context is None:
        return None
    context = get_context()
    if not getattr(context, "is_valid", False):
        return None
    return f"00-{context.trace_id:032x}-{context.span_id:016x}-{int(context.trace_flags):02x}"


def _tracer() -> Any:
    global _configured
    import os

    from opentelemetry import trace

    if not _configured:
        endpoint = next((os.environ[key] for key in _OTLP_ENV_KEYS if os.environ.get(key)), "")
        if endpoint:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            provider = TracerProvider(resource=Resource.create({}))
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
            trace.set_tracer_provider(provider)
        _configured = True
    return trace.get_tracer("trpc-agent-platform")


@contextmanager
def linked_span(
    name: str,
    trace_parent: str | None,
    attributes: dict[str, object] | None = None,
) -> Iterator[object]:
    """Start a span linked to an async parent described by a W3C traceparent.

    Asynchronous workers never run inside the caller's span context; the
    W3C spec answer is a Span Link, which keeps the request trace queryable
    from the worker side without nesting unrelated work.
    """
    parsed = parse_traceparent(trace_parent)
    tracer = _tracer()
    from opentelemetry.context import Context
    from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, TraceFlags

    link_context = None
    parent_context: Context | None = None
    if parsed is not None:
        trace_id, span_id, flags = parsed
        span_context = SpanContext(
            trace_id=int(trace_id, 16),
            span_id=int(span_id, 16),
            is_remote=True,
            trace_flags=TraceFlags(int(flags, 16)),
        )
        parent_context = Context({})

        from opentelemetry.trace import set_span_in_context

        parent_context = set_span_in_context(NonRecordingSpan(span_context), parent_context)
        link_context = span_context
    links = [Link(link_context)] if link_context is not None else None
    with tracer.start_as_current_span(
        name, context=parent_context, links=links, attributes=filter_attributes(attributes or {})
    ) as span:
        active = (
            _span_traceparent(span)
            or child_traceparent(trace_parent)
            or _sampled_root_traceparent()
        )
        token = _current_traceparent.set(active)
        try:
            yield span
        finally:
            _current_traceparent.reset(token)


# --- Prometheus metrics ---------------------------------------------------

from prometheus_client import (  # noqa: E402
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)

HTTP_REQUESTS = Counter(
    "platform_http_requests_total",
    "HTTP requests served by platform units",
    ["service", "method", "route", "status"],
)
HTTP_LATENCY = Histogram(
    "platform_http_request_seconds",
    "HTTP request latency in seconds",
    ["service", "route"],
)
EXECUTION_OUTCOMES = Counter(
    "platform_execution_outcomes_total",
    "Execution pipeline outcomes",
    ["service", "outcome"],
)


def metrics_response() -> Response:
    """Render the Prometheus exposition body for /metrics endpoints."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


def install_telemetry(app: object, service_name: str) -> None:
    """Attach trace propagation, request metrics and /metrics to a FastAPI app."""
    import time

    from fastapi import Request
    from starlette.middleware.base import BaseHTTPMiddleware

    from trpc_service.log import configure_json_logging

    configure_json_logging()

    class TelemetryMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next: Any) -> Response:
            inbound = request.headers.get("traceparent")
            active = (
                inbound if parse_traceparent(inbound) is not None else _sampled_root_traceparent()
            )
            token = _current_traceparent.set(active)
            outbound = child_traceparent(active)
            started = time.perf_counter()
            try:
                response: Response = await call_next(request)
            finally:
                _current_traceparent.reset(token)
            if outbound is not None and response is not None:
                response.headers["traceparent"] = outbound
            route = request.scope.get("route")
            route_label = getattr(route, "path", None) or request.url.path
            status = getattr(response, "status_code", 500)
            route_only = getattr(route, "path", None) is not None
            if route_only:
                HTTP_LATENCY.labels(service=service_name, route=route_label).observe(
                    time.perf_counter() - started
                )
            HTTP_REQUESTS.labels(
                service=service_name,
                method=request.method,
                route=route_label,
                status=str(status),
            ).inc()
            return response

    app.add_middleware(TelemetryMiddleware)  # type: ignore[attr-defined]

    @app.get("/metrics", include_in_schema=False)  # type: ignore[attr-defined, misc]
    async def prometheus_metrics() -> Response:
        return metrics_response()
