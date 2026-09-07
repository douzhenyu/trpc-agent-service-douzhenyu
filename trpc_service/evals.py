"""Deterministic interpretation of tRPC-Agent Evaluation evidence."""

from __future__ import annotations

from typing import Any


async def production_eval_gate_error(
    connection: Any, *, tenant_id: Any, application_id: Any, release_id: Any
) -> str | None:
    """Return the fail-closed Production promotion error for a Release, if any.

    The most recent Production run is authoritative.  A later failed run must
    supersede an older pass; otherwise a release could be promoted after its
    safety or regression evidence has been invalidated.
    """

    row = await connection.fetchrow(
        """SELECT status FROM tenant.eval_run
        WHERE tenant_id=$1 AND application_id=$2 AND release_id=$3 AND environment='PRODUCTION'
        ORDER BY created_at DESC, id DESC LIMIT 1""",
        tenant_id,
        application_id,
        release_id,
    )
    if row is None:
        return "EVAL_RUN_REQUIRED"
    return None if row["status"] == "PASSED" else "EVAL_RUN_FAILED"


def evaluate_evidence(
    *, thresholds: dict[str, Any], deterministic_assertions: list[str], evidence: dict[str, Any]
) -> dict[str, Any]:
    """Make safety checks independent from non-deterministic model judges.

    The Job Worker may obtain quality evidence through ``trpc_agent_sdk.evaluation``.
    This boundary turns that versioned evidence into the platform's fail-closed
    promotion decision: missing deterministic assertions fail, never pass.
    """

    assertions = evidence.get("assertions")
    metrics = evidence.get("metrics")
    assertion_values = assertions if isinstance(assertions, dict) else {}
    metric_values = metrics if isinstance(metrics, dict) else {}
    failed_assertions = [
        assertion
        for assertion in deterministic_assertions
        if assertion_values.get(assertion) is not True
    ]
    quality = _number(metric_values.get("quality"))
    latency_ms = _number(metric_values.get("latency_ms"))
    cost_micros = _number(metric_values.get("cost_micros"))
    threshold_failures: list[str] = []
    if quality is None or quality < _threshold(thresholds.get("quality_min")):
        threshold_failures.append("QUALITY_THRESHOLD")
    if latency_ms is None or latency_ms > _threshold(thresholds.get("latency_ms_max")):
        threshold_failures.append("LATENCY_THRESHOLD")
    if cost_micros is None or cost_micros > _threshold(thresholds.get("cost_micros_max")):
        threshold_failures.append("COST_THRESHOLD")
    return {
        "failed_assertions": failed_assertions,
        "failed_thresholds": threshold_failures,
        "metrics": {"quality": quality, "latency_ms": latency_ms, "cost_micros": cost_micros},
        "status": "PASSED" if not failed_assertions and not threshold_failures else "FAILED",
    }


def _threshold(value: Any) -> float:
    number = _number(value)
    return number if number is not None and number >= 0 else 0.0


def _number(value: Any) -> float | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    return float(value)
