"""Structured logs must carry the active trace context for Loki correlation."""

from __future__ import annotations

import json
import logging

from trpc_service.log import TraceContextFormatter
from trpc_service.telemetry import _current_traceparent


def test_formatter_embeds_trace_id_from_context() -> None:
    token = _current_traceparent.set("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    try:
        record = logging.LogRecord("trpc", logging.INFO, __file__, 1, "handled execution", (), None)
        payload = json.loads(TraceContextFormatter().format(record))
        assert payload["message"] == "handled execution"
        assert payload["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    finally:
        _current_traceparent.reset(token)


def test_formatter_omits_trace_id_without_context() -> None:
    record = logging.LogRecord("trpc", logging.INFO, __file__, 1, "no trace", (), None)
    payload = json.loads(TraceContextFormatter().format(record))
    assert "trace_id" not in payload
