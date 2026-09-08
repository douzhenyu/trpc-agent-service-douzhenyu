"""Acceptance checks for the repeatable capacity load-test report."""

import asyncio

from scripts import load_test


def test_evaluate_rejects_fast_throttling_even_when_attempt_rate_is_high() -> None:
    report = {
        "requested_rate": 1000,
        "attempted_rate": 1000.0,
        "accepted_rate": 0.0,
        "p99_seconds": 0.01,
        "status_counts": {"4xx/429": 60_000},
    }

    assert load_test.evaluate(report) == ["accepted 0.0/s below 90% of 1000/s"]


def test_run_schedules_requests_concurrently_and_counts_only_acceptance(
    monkeypatch,
) -> None:
    active = 0
    peak_active = 0

    async def fake_send(_client, _url, _payload):
        nonlocal active, peak_active
        active += 1
        peak_active = max(peak_active, active)
        await asyncio.sleep(0.02)
        active -= 1
        return 0.02, 202

    monkeypatch.setitem(load_test.PROFILES, "test", {"rate": 200, "seconds": 1})
    monkeypatch.setattr(load_test, "_send_one", fake_send)

    report = asyncio.run(load_test.run("test", "http://gateway", "tenant", "application"))

    assert peak_active > 1
    assert report["attempted"] == 200
    assert 180 <= report["accepted"] < 200
    assert report["late_accepted"] > 0


def test_run_does_not_count_accepted_responses_that_miss_the_profile_window(
    monkeypatch,
) -> None:
    async def slow_send(_client, _url, _payload):
        await asyncio.sleep(1.05)
        return 1.05, 202

    monkeypatch.setitem(load_test.PROFILES, "slow", {"rate": 1, "seconds": 1})
    monkeypatch.setattr(load_test, "_send_one", slow_send)

    report = asyncio.run(load_test.run("slow", "http://gateway", "tenant", "application"))

    assert report["accepted"] == 0
    assert report["late_accepted"] == 1
    assert load_test.evaluate(report) == ["accepted 0.0/s below 90% of 1/s"]
