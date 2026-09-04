from datetime import UTC, datetime

import pytest

from trpc_service.budgets import (
    CRITICAL_90,
    WARNING_70,
    daily_period_key,
    estimate_cost_micros,
    estimate_tokens,
    evaluate_level,
    monthly_period_key,
    period_key_for,
)


def test_period_keys_are_utc_calendar_buckets() -> None:
    moment = datetime(2026, 9, 4, 23, 30, tzinfo=UTC)
    assert monthly_period_key(moment) == "2026-09"
    assert daily_period_key(moment) == "2026-09-04"
    assert period_key_for("TENANT_MONTHLY", moment, None) == "2026-09"
    assert period_key_for("AGENT_DAILY", moment, None) == "2026-09-04"
    assert period_key_for("EXECUTION", moment, "exec-1") == "exec-1"
    with pytest.raises(ValueError, match="execution id"):
        period_key_for("EXECUTION", moment, None)
    with pytest.raises(ValueError, match="unknown budget scope"):
        period_key_for("WEEKLY", moment, None)


def test_token_estimation_is_deterministic_and_ceilings() -> None:
    tokens = estimate_tokens([{"role": "user", "content": "12345678"}], max_output_tokens=128)
    assert tokens == (2, 128)
    assert estimate_tokens([{"role": "user", "content": ""}]) == (0, 512)
    assert estimate_tokens([{"role": "user", "content": "x" * 5}]) == (2, 512)


def test_cost_estimation_rounds_up_to_micros() -> None:
    cost = estimate_cost_micros(1500, 2000, input_micros_per_1k=1000, output_micros_per_1k=2000)
    assert cost == 1500 + 4000
    assert estimate_cost_micros(1, 1, input_micros_per_1k=1, output_micros_per_1k=1) == 2
    assert estimate_cost_micros(1_000_000, 0, input_micros_per_1k=0, output_micros_per_1k=0) == 0


def test_level_evaluation_matches_spec_thresholds() -> None:
    assert evaluate_level(0, 1000) is None
    assert evaluate_level(699, 1000) is None
    assert evaluate_level(700, 1000) == WARNING_70
    assert evaluate_level(899, 1000) == WARNING_70
    assert evaluate_level(900, 1000) == CRITICAL_90
    assert evaluate_level(1000, 1000) == CRITICAL_90  # exactly at limit is admissible
    assert evaluate_level(1001, 1000) == "REJECT"
    assert evaluate_level(1500, 1000) == "REJECT"
    assert evaluate_level(0, 0) is None
    assert evaluate_level(1, 0) == "REJECT"
