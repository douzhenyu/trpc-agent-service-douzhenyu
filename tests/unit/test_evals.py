from trpc_service.evals import evaluate_evidence


def test_evaluation_evidence_fails_closed_for_missing_safety_assertion() -> None:
    result = evaluate_evidence(
        thresholds={"quality_min": 0.8, "latency_ms_max": 500, "cost_micros_max": 1000},
        deterministic_assertions=["NO_SECRET_LEAK", "NO_DISABLED_TOOL"],
        evidence={
            "assertions": {"NO_SECRET_LEAK": True},
            "metrics": {"quality": 0.9, "latency_ms": 200, "cost_micros": 500},
        },
    )

    assert result["status"] == "FAILED"
    assert result["failed_assertions"] == ["NO_DISABLED_TOOL"]


def test_evaluation_evidence_flags_canary_metric_regressions() -> None:
    result = evaluate_evidence(
        thresholds={"quality_min": 0.8, "latency_ms_max": 500, "cost_micros_max": 1000},
        deterministic_assertions=[],
        evidence={"metrics": {"quality": 0.7, "latency_ms": 600, "cost_micros": 1001}},
    )

    assert result["status"] == "FAILED"
    assert result["failed_thresholds"] == [
        "QUALITY_THRESHOLD",
        "LATENCY_THRESHOLD",
        "COST_THRESHOLD",
    ]
