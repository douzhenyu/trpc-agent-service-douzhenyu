"""Unit tests for W3C trace context handling, baggage stripping and metrics."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trpc_service.telemetry import (
    ATTRIBUTE_ALLOWLIST,
    burn_rate,
    child_traceparent,
    current_traceparent,
    filter_attributes,
    install_telemetry,
    linked_span,
    parse_traceparent,
    strip_internal_baggage,
)


def test_traceparent_roundtrip_and_child_keeps_trace() -> None:
    parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    assert parse_traceparent(parent) == (
        "4bf92f3577b34da6a3ce929d0e0e4736",
        "00f067aa0ba902b7",
        "01",
    )
    child = child_traceparent(parent)
    assert child is not None
    parsed = parse_traceparent(child)
    assert parsed is not None
    assert parsed[0] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert parsed[1] != "00f067aa0ba902b7"
    assert parsed[2] == "01"


def test_traceparent_rejects_invalid_context() -> None:
    assert parse_traceparent(None) is None
    assert parse_traceparent("not-a-traceparent") is None
    assert parse_traceparent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") is None
    assert parse_traceparent("00-" + "0" * 32 + "-00f067aa0ba902b7-01") is None
    assert parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-" + "0" * 16 + "-01") is None
    assert child_traceparent(None) is None


def test_outbound_baggage_only_keeps_public_allowlist() -> None:
    stripped = strip_internal_baggage(
        "public.tenant=acme,session.id=s-1,public.environment=prod,internal.route=x"
    )
    assert stripped == "public.tenant=acme,public.environment=prod"
    assert strip_internal_baggage(None) == ""
    assert strip_internal_baggage("session.id=s-1,internal.flag=1") == ""


def test_attribute_allowlist_filters_and_coerces() -> None:
    filtered = filter_attributes(
        {
            "http.request.method": "POST",
            "http.route": "/api/v1/tenants",
            "http.response.status_code": 201,
            "tenant.id": "t-1",
            "raw.payload": {"secret": "value"},
            "user.email": "user@example.test",
        }
    )
    assert set(filtered) == {
        "http.request.method",
        "http.route",
        "http.response.status_code",
        "tenant.id",
    }
    assert "user.email" not in ATTRIBUTE_ALLOWLIST


def test_linked_span_records_link_to_async_parent() -> None:
    from opentelemetry import trace as otel_trace
    from opentelemetry.sdk.trace import Span, TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

    parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    captured: list[Span] = []

    class Collector(SpanExporter):
        def export(self, spans):  # type: ignore[no-untyped-def]
            captured.extend(spans)
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            return None

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(Collector()))
    otel_trace.set_tracer_provider(provider)

    with linked_span(
        "agent_worker.execute",
        parent,
        {"tenant.id": "t-1", "raw.secret": "x"},
    ) as span:
        assert span is not None
        active = parse_traceparent(current_traceparent())
        assert active is not None
        assert active[0] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert current_traceparent() is None
    assert len(captured) == 1
    links = list(captured[0].links)
    assert links, "async work must link the original trace"
    assert links[0].context.trace_id == int("4bf92f3577b34da6a3ce929d0e0e4736", 16)
    attributes = dict(captured[0].attributes or {})
    assert attributes.get("tenant.id") == "t-1"
    assert "raw.secret" not in attributes

    with linked_span("unlinked", None) as span:
        assert span is not None


def test_install_telemetry_propagates_trace_and_counts() -> None:
    app = FastAPI()
    install_telemetry(app, "test-unit")

    @app.get("/echo")
    async def echo() -> dict[str, str]:
        return {"inbound": current_traceparent() or ""}

    with TestClient(app) as client:
        missing = client.get("/echo")
        assert missing.status_code == 200
        missing_active = parse_traceparent(missing.json()["inbound"])
        missing_response = parse_traceparent(missing.headers["traceparent"])
        assert missing_active is not None
        assert missing_response is not None
        assert missing_active[0] == missing_response[0]
        assert missing_active[1] != missing_response[1]
        assert missing_active[2] == "01"

        invalid = client.get("/echo", headers={"traceparent": "not-a-traceparent"})
        invalid_active = parse_traceparent(invalid.json()["inbound"])
        invalid_response = parse_traceparent(invalid.headers["traceparent"])
        assert invalid_active is not None
        assert invalid_response is not None
        assert invalid_active[0] == invalid_response[0]
        assert invalid_active[1] != invalid_response[1]
        assert invalid_active[2] == "01"

        request = client.get(
            "/echo",
            headers={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
        )
        assert request.json()["inbound"].startswith("00-4bf92f3577b34da6a3ce929d0e0e4736")
        assert request.headers["traceparent"].startswith("00-4bf92f3577b34da6a3ce929d0e0e4736")

        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert "platform_http_requests_total" in metrics.text


def test_slo_targets_and_burn_rate() -> None:
    assert burn_rate(0, 1000, "data_plane") == 0.0
    assert burn_rate(1000, 1000, "data_plane") is not None
    # 0.2% errors against a 0.05% budget: 4x burn.
    assert burn_rate(2, 1000, "data_plane") == pytest.approx(4.0)
    # 1% errors against a 0.1% budget: 10x burn.
    assert burn_rate(10, 1000, "control_plane") == pytest.approx(10.0)
    assert burn_rate(1, 0, "data_plane") is None
