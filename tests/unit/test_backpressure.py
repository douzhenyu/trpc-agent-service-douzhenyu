"""Capacity policy: sustained and burst throughput, in-flight cap, rejection."""

from __future__ import annotations

from trpc_service.backpressure import (
    AdmissionController,
    AdmissionDenied,
    CapacityPolicy,
    shed_level,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_sustained_rate_admits_1000_per_second() -> None:
    clock = FakeClock()
    controller = AdmissionController(CapacityPolicy(), clock=clock)
    admitted = 0
    for _ in range(2000):
        try:
            controller.admit()
            controller.release()
            admitted += 1
        except AdmissionDenied:
            pass
        clock.advance(0.001)  # 1 ms ticks: sustained 1000/s
    assert admitted == 2000


def test_burst_absorbs_3000_per_second_for_60_seconds() -> None:
    clock = FakeClock()
    controller = AdmissionController(CapacityPolicy(), clock=clock)
    denied = 0
    for _ in range(60 * 3000):
        try:
            controller.admit()
            controller.release()
        except AdmissionDenied:
            denied += 1
        clock.advance(1 / 3000)
    # A 60s burst at 3000/s stays inside the bucket: 3000 initial capacity
    # plus 1000/s refill fully covers it.
    assert denied == 0


def test_sustained_above_limit_is_rejected_with_stable_reason() -> None:
    clock = FakeClock()
    # A small envelope so the burst bucket drains within the test: 30/s
    # attempted against a 10/s budget with a 60-token capacity.
    policy = CapacityPolicy(
        sustained_per_second=10, burst_per_second=30, burst_seconds=2, max_in_flight=100
    )
    controller = AdmissionController(policy, clock=clock)
    rejected = 0
    for _ in range(200):
        try:
            controller.admit()
            controller.release()
        except AdmissionDenied as error:
            assert error.reason == "RATE_EXCEEDED"
            rejected += 1
        clock.advance(1 / 30)
    assert rejected > 0


def test_in_flight_cap_blocks_at_10000_concurrent() -> None:
    controller = AdmissionController(CapacityPolicy(), clock=FakeClock())
    for _ in range(10_000):
        controller.admit()
    try:
        controller.admit()
        raise AssertionError("expected ADMISSION_DENIED")
    except AdmissionDenied as error:
        assert error.reason == "INFLIGHT_SATURATED"
    assert controller.in_flight == 10_000
    controller.release()
    assert controller.in_flight == 9_999
    controller.admit()
    assert controller.in_flight == 10_000


def test_shed_level_classifies_saturation() -> None:
    policy = CapacityPolicy()
    assert shed_level(0, 0, policy) == "GREEN"
    assert shed_level(policy.max_in_flight // 2, 0, policy) == "YELLOW"
    assert shed_level(0, policy.burst_per_second // 2, policy) == "YELLOW"
    assert shed_level(policy.max_in_flight, 0, policy) == "RED"
    assert shed_level(0, policy.burst_per_second, policy) == "RED"
