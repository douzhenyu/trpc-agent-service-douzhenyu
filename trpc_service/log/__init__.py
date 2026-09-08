"""Structured logging that carries the active W3C trace context into Loki."""

from __future__ import annotations

import json
import logging
from typing import Any

from trpc_service.telemetry import current_traceparent


class TraceContextFormatter(logging.Formatter):
    """Format records as JSON with trace_id/span_id extracted from the traceparent."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        trace_parent = current_traceparent()
        if trace_parent:
            payload["trace_id"] = trace_parent.split("-")[1]
        for field in ("tenant_id", "session_id", "execution_id"):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = str(value)
        return json.dumps(payload, ensure_ascii=False)


def configure_json_logging() -> None:
    """Install the trace-aware formatter on the root logger exactly once."""
    root = logging.getLogger()
    if any(isinstance(handler.formatter, TraceContextFormatter) for handler in root.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(TraceContextFormatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)
