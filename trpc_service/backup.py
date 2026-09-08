"""Backup manifest evaluation and failover drill evidence (RPO/RTO gates).

The repository owns the contract and the acceptance math; the concrete
pgBackPatrol-style PITR pipelines, object-store versioning and replication
probes are deployment-owned and injected as callables.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.failover import RPO_LIMIT_SECONDS, RTO_LIMIT_SECONDS


class BackupManifest(BaseModel):
    """What a warm-standby region must be able to restore from."""

    model_config = ConfigDict(frozen=True)

    postgres_pitr_seconds: int = Field(ge=0, description="PITR recoverable window")
    replication_lag_seconds: int = Field(ge=0, description="WAL/replica lag")
    object_versioning_enabled: bool
    message_offsets_captured: bool
    config_refs_captured: bool
    secret_refs_captured: bool


def check_rpo(manifest: BackupManifest) -> tuple[bool, list[str]]:
    """RPO ≤ 5 minutes: lag within limit and every plane captured."""
    failures: list[str] = []
    if manifest.replication_lag_seconds > RPO_LIMIT_SECONDS:
        failures.append("REPLICATION_LAG_EXCEEDS_RPO")
    if not manifest.object_versioning_enabled:
        failures.append("OBJECT_VERSIONING_DISABLED")
    if not manifest.message_offsets_captured:
        failures.append("MESSAGE_OFFSETS_MISSING")
    if not manifest.config_refs_captured or not manifest.secret_refs_captured:
        failures.append("CONFIG_OR_SECRET_REFS_MISSING")
    return (not failures, failures)


@dataclass(frozen=True)
class RestoreProbeResult:
    pitr_restorable: bool
    object_versions_restorable: bool
    offsets_replayable: bool


@dataclass(frozen=True)
class RestoreReport:
    passed: bool
    failures: list[str] = field(default_factory=list)


async def verify_restore(
    pitr_probe: Callable[[], Awaitable[bool]],
    object_probe: Callable[[], Awaitable[bool]],
    offsets_probe: Callable[[], Awaitable[bool]],
) -> RestoreReport:
    """Run deployment-provided restore probes; every plane must restore."""
    failures: list[str] = []
    pitr = await pitr_probe()
    objects = await object_probe()
    offsets = await offsets_probe()
    if not pitr:
        failures.append("POSTGRES_PITR_NOT_RESTORABLE")
    if not objects:
        failures.append("OBJECT_VERSIONS_NOT_RESTORABLE")
    if not offsets:
        failures.append("MESSAGE_OFFSETS_NOT_REPLAYABLE")
    return RestoreReport(passed=not failures, failures=failures)


@dataclass(frozen=True)
class DrillOutcome:
    passed: bool
    rpo_seconds: int
    rto_seconds: int
    failures: list[str]


async def run_failover_drill(
    *,
    region_from: str,
    region_to: str,
    operator: str,
    measured_rpo_seconds: int,
    measured_rto_seconds: int,
    restore_probes: dict[str, Callable[[], Awaitable[RestoreReport]]] | None = None,
    record: Callable[..., Awaitable[None]] | None = None,
) -> DrillOutcome:
    """Evaluate one drill against the acceptance gates (RPO 5m, RTO 60m).

    ``restore_probes`` carry deployment-owned plane verifications (Postgres
    PITR, object versions, message offsets); the RPO/RTO values are the
    measured results of the drill run itself.
    """
    failures: list[str] = []
    if restore_probes:
        for probe in restore_probes.values():
            report = await probe()
            failures.extend(report.failures)
    if not drill_passes(measured_rpo_seconds, measured_rto_seconds):
        failures.append("RPO_OR_RTO_GATE_FAILED")

    executed_at = datetime.now(UTC)
    outcome = DrillOutcome(
        passed=not failures,
        rpo_seconds=measured_rpo_seconds,
        rto_seconds=measured_rto_seconds,
        failures=failures,
    )
    if record is not None:
        await record(
            region_from,
            region_to,
            outcome.rpo_seconds,
            outcome.rto_seconds,
            outcome.passed,
            {"operator": operator, "failures": failures, "executed_at": executed_at.isoformat()},
        )
    return outcome


def drill_passes(rpo_seconds: int, rto_seconds: int) -> bool:
    """The quarterly drill gate: RPO ≤ 5 minutes, RTO ≤ 60 minutes."""
    return rpo_seconds <= RPO_LIMIT_SECONDS and rto_seconds <= RTO_LIMIT_SECONDS


def drill_evidence_row(
    drill_id: UUID,
    region_from: str,
    region_to: str,
    rpo_seconds: int,
    rto_seconds: int,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": str(drill_id),
        "region_from": region_from,
        "region_to": region_to,
        "rpo_seconds": rpo_seconds,
        "rto_seconds": rto_seconds,
        "passed": drill_passes(rpo_seconds, rto_seconds),
        "evidence": evidence,
    }
