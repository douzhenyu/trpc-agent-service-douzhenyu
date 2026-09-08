"""Explicit, auditable degradation for non-critical dependency failures.

Security state, secrets, leases and side-effect governance are fail closed:
they never degrade, only deny. Knowledge, Memory, Artifact and Model
availability may degrade, but the degraded service level is explicit,
queryable and audited instead of being silently absorbed.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class DegradationDomain(StrEnum):
    MODEL = "MODEL"
    KNOWLEDGE = "KNOWLEDGE"
    MEMORY = "MEMORY"
    ARTIFACT = "ARTIFACT"
    IM = "IM"


# Fail-closed concerns: an outage here must deny the operation, never serve it
# in a reduced mode. They are intentionally not DegradationDomain members.
FAIL_CLOSED_CONCERNS = frozenset(
    {"SECURITY_STATE", "SECRET_RESOLUTION", "SESSION_LEASE", "SIDE_EFFECT_GOVERNANCE"}
)


class FailClosedError(RuntimeError):
    """A fail-closed concern refused to continue; the caller must not proceed."""

    def __init__(self, concern: str, reason: str) -> None:
        super().__init__(f"{concern}:{reason}")
        self.concern = concern
        self.reason = reason


@dataclass(frozen=True)
class Degradation:
    domain: DegradationDomain
    reason: str
    entered_at: str
    detail: str | None = None


AuditHook = Callable[[str, Degradation], None]


class DegradationRegistry:
    """Process-local view of every explicitly degraded dependency.

    ``audit_hook`` receives ``entered``/``restored`` notifications so the host
    process can append them to the immutable audit chain; without a hook the
    registry still works, keeping units startable in isolation.
    """

    def __init__(self, audit_hook: AuditHook | None = None) -> None:
        self._lock = threading.Lock()
        self._active: dict[DegradationDomain, Degradation] = {}
        self._audit_hook = audit_hook

    def degrade(
        self, domain: DegradationDomain, reason: str, *, detail: str | None = None
    ) -> Degradation:
        degradation = Degradation(
            domain=domain,
            reason=reason,
            entered_at=datetime.now(UTC).isoformat(),
            detail=detail,
        )
        with self._lock:
            self._active[domain] = degradation
        self._notify("degradation.entered", degradation)
        return degradation

    def restore(self, domain: DegradationDomain, reason: str) -> bool:
        with self._lock:
            degradation = self._active.pop(domain, None)
        if degradation is None:
            return False
        self._notify(
            "degradation.restored",
            Degradation(
                domain=domain,
                reason=reason,
                entered_at=degradation.entered_at,
                detail=degradation.reason,
            ),
        )
        return True

    def is_degraded(self, domain: DegradationDomain) -> bool:
        with self._lock:
            return domain in self._active

    def status(self) -> list[Degradation]:
        with self._lock:
            return sorted(self._active.values(), key=lambda item: item.domain)

    def _notify(self, action: str, degradation: Degradation) -> None:
        if self._audit_hook is not None:
            self._audit_hook(action, degradation)


_registry = DegradationRegistry()


def degradation_registry() -> DegradationRegistry:
    """The unit-process registry shared by every degradation call site."""
    return _registry


def register_degradations_endpoint(app: object) -> None:
    """Expose ``GET /internal/v1/degradations`` for the ops console."""

    from fastapi import FastAPI

    assert isinstance(app, FastAPI)

    @app.get("/internal/v1/degradations", response_model=list[dict[str, str | None]])
    async def degradations() -> list[dict[str, str | None]]:
        return [
            {
                "domain": item.domain.value,
                "reason": item.reason,
                "entered_at": item.entered_at,
                "detail": item.detail,
            }
            for item in _registry.status()
        ]
