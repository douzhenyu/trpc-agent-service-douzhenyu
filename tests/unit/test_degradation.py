"""Explicit degradation, fail-closed concerns and chaos recovery invariants."""

from __future__ import annotations

from trpc_service.degradation import (
    FAIL_CLOSED_CONCERNS,
    DegradationDomain,
    DegradationRegistry,
    FailClosedError,
)


def test_degrade_and_restore_are_explicit_and_ordered() -> None:
    registry = DegradationRegistry()
    assert registry.status() == []
    registry.degrade(DegradationDomain.KNOWLEDGE, "KB_BUILD_UNAVAILABLE", detail="base=ops-kb")
    assert registry.is_degraded(DegradationDomain.KNOWLEDGE)
    registry.degrade(DegradationDomain.MODEL, "LLM_FALLBACK_USED")
    assert [item.domain for item in registry.status()] == [
        DegradationDomain.KNOWLEDGE,
        DegradationDomain.MODEL,
    ]
    assert registry.restore(DegradationDomain.MODEL, "PRIMARY_RECOVERED") is True
    assert not registry.is_degraded(DegradationDomain.MODEL)
    assert registry.restore(DegradationDomain.MODEL, "PRIMARY_RECOVERED") is False


def test_audit_hook_receives_entered_and_restored() -> None:
    events: list[tuple[str, str, str]] = []
    registry = DegradationRegistry(
        audit_hook=lambda action, degradation: events.append(
            (action, degradation.domain.value, degradation.reason)
        )
    )
    registry.degrade(DegradationDomain.ARTIFACT, "STORE_UNAVAILABLE")
    registry.restore(DegradationDomain.ARTIFACT, "STORE_RECOVERED")
    assert ("degradation.entered", "ARTIFACT", "STORE_UNAVAILABLE") in events
    assert ("degradation.restored", "ARTIFACT", "STORE_RECOVERED") in events


def test_fail_closed_concerns_never_degrade() -> None:
    assert (
        frozenset(
            {"SECURITY_STATE", "SECRET_RESOLUTION", "SESSION_LEASE", "SIDE_EFFECT_GOVERNANCE"}
        )
        == FAIL_CLOSED_CONCERNS
    )
    for concern in FAIL_CLOSED_CONCERNS:
        error = FailClosedError(concern, "denied during outage")
        assert error.concern == concern
        assert "denied" in str(error)
    # No fail-closed concern is representable as a degradation domain.
    assert all(
        concern not in {domain.value for domain in DegradationDomain}
        for concern in FAIL_CLOSED_CONCERNS
    )
