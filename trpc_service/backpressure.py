"""Admission control and load-shedding that keep the platform inside its
verified capacity envelope: sustained 1000 msg/s, 3000 msg/s burst for 60s
and at least 10000 concurrent executions, with observable, stable rejection
beyond those bounds.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from prometheus_client import Counter

ADMISSION_DECISIONS = Counter(
    "platform_admission_decisions_total",
    "Gateway admission decisions under the capacity policy",
    ["service", "decision", "reason"],
)


@dataclass(frozen=True)
class CapacityPolicy:
    """The production capacity envelope the load tests must keep proving."""

    sustained_per_second: int = 1000
    burst_per_second: int = 3000
    burst_seconds: int = 60
    max_in_flight: int = 10_000


class AdmissionDenied(RuntimeError):
    """Stable rejection raised when the capacity envelope is exhausted."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class AdmissionController:
    """Token-bucket rate limit plus an in-flight concurrency cap.

    The bucket capacity equals the full 60-second burst allowance and refills
    at the sustained rate, so a 3000/s burst is absorbed entirely while
    long-run traffic settles at 1000/s. Every decision is observable through
    ``platform_admission_decisions_total``.
    """

    policy: CapacityPolicy = field(default_factory=CapacityPolicy)
    service: str = "agent-gateway"
    clock: Callable[[], float] = time.monotonic
    _tokens: float | None = field(default=None, init=False)
    _last_refill: float | None = field(default=None, init=False)
    _in_flight: int = field(default=0, init=False)

    def _refill(self, now: float) -> float:
        capacity = float(self.policy.burst_per_second * self.policy.burst_seconds)
        if self._tokens is None or self._last_refill is None:
            self._tokens = capacity
            self._last_refill = now
            return self._tokens
        elapsed = max(0.0, now - self._last_refill)
        self._tokens = min(capacity, self._tokens + elapsed * self.policy.sustained_per_second)
        self._last_refill = now
        return self._tokens

    def admit(self) -> None:
        """Reserve one admission slot or raise :class:`AdmissionDenied`."""
        now = self.clock()
        if self._in_flight >= self.policy.max_in_flight:
            ADMISSION_DECISIONS.labels(
                service=self.service, decision="DENIED", reason="INFLIGHT_SATURATED"
            ).inc()
            raise AdmissionDenied("INFLIGHT_SATURATED")
        tokens = self._refill(now)
        if tokens < 1.0:
            ADMISSION_DECISIONS.labels(
                service=self.service, decision="DENIED", reason="RATE_EXCEEDED"
            ).inc()
            raise AdmissionDenied("RATE_EXCEEDED")
        self._tokens = tokens - 1.0
        self._in_flight += 1
        ADMISSION_DECISIONS.labels(service=self.service, decision="ALLOWED", reason="NONE").inc()

    def release(self) -> None:
        """Return an in-flight slot after the execution has been accepted."""
        if self._in_flight > 0:
            self._in_flight -= 1

    @property
    def in_flight(self) -> int:
        return self._in_flight


def shed_level(in_flight: int, pending: int, policy: CapacityPolicy) -> str:
    """Classify saturation: GREEN normal, YELLOW queueing, RED shedding."""
    if in_flight >= policy.max_in_flight or pending >= policy.burst_per_second:
        return "RED"
    if in_flight >= policy.max_in_flight // 2 or pending >= policy.burst_per_second // 2:
        return "YELLOW"
    return "GREEN"
