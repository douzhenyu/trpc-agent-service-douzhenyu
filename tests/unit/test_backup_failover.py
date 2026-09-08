"""Backup manifest gates and failover drill acceptance (RPO 5m, RTO 60m)."""

from __future__ import annotations

import pytest

from trpc_service.backup import (
    BackupManifest,
    RestoreReport,
    check_rpo,
    drill_passes,
    run_failover_drill,
    verify_restore,
)
from trpc_service.failover import RPO_LIMIT_SECONDS, RTO_LIMIT_SECONDS


def _manifest(**overrides: object) -> BackupManifest:
    base = {
        "postgres_pitr_seconds": 3600,
        "replication_lag_seconds": 120,
        "object_versioning_enabled": True,
        "message_offsets_captured": True,
        "config_refs_captured": True,
        "secret_refs_captured": True,
    }
    base.update(overrides)
    return BackupManifest(**base)  # type: ignore[arg-type]


def test_healthy_manifest_passes_the_rpo_gate() -> None:
    passed, failures = check_rpo(_manifest())
    assert passed
    assert failures == []


def test_rpo_gate_flags_each_missing_plane() -> None:
    passed, failures = check_rpo(
        _manifest(
            replication_lag_seconds=600,
            object_versioning_enabled=False,
            message_offsets_captured=False,
            config_refs_captured=False,
        )
    )
    assert not passed
    assert set(failures) == {
        "REPLICATION_LAG_EXCEEDS_RPO",
        "OBJECT_VERSIONING_DISABLED",
        "MESSAGE_OFFSETS_MISSING",
        "CONFIG_OR_SECRET_REFS_MISSING",
    }


def test_rpo_and_rto_limits_match_the_quarterly_gate() -> None:
    assert RPO_LIMIT_SECONDS == 300
    assert RTO_LIMIT_SECONDS == 3600
    assert drill_passes(299, 3599)
    assert not drill_passes(301, 100)
    assert not drill_passes(100, 3601)


@pytest.mark.asyncio
async def test_restore_verification_requires_every_plane() -> None:
    report = await verify_restore(
        lambda: _async_true(), lambda: _async_true(), lambda: _async_true()
    )
    assert report == RestoreReport(passed=True, failures=[])
    report = await verify_restore(
        lambda: _async_false(), lambda: _async_true(), lambda: _async_false()
    )
    assert not report.passed
    assert set(report.failures) == {
        "POSTGRES_PITR_NOT_RESTORABLE",
        "MESSAGE_OFFSETS_NOT_REPLAYABLE",
    }


@pytest.mark.asyncio
async def test_drill_records_pass_and_fail_evidence() -> None:
    recorded: list[tuple] = []

    async def record(*args: object) -> None:
        recorded.append(args)

    passed = await run_failover_drill(
        region_from="cn-east",
        region_to="cn-north",
        operator="drill-operator",
        measured_rpo_seconds=180,
        measured_rto_seconds=2400,
        record=record,
    )
    assert passed.passed
    failed = await run_failover_drill(
        region_from="cn-east",
        region_to="cn-north",
        operator="drill-operator",
        measured_rpo_seconds=900,
        measured_rto_seconds=100,
        restore_probes={
            "postgres": lambda: _async_report(
                RestoreReport(passed=False, failures=["POSTGRES_PITR_NOT_RESTORABLE"])
            )
        },
        record=record,
    )
    assert not failed.passed
    assert "POSTGRES_PITR_NOT_RESTORABLE" in failed.failures
    assert "RPO_OR_RTO_GATE_FAILED" in failed.failures
    assert len(recorded) == 2
    assert recorded[1][4] is False


async def _async_true() -> bool:
    return True


async def _async_false() -> bool:
    return False


async def _async_report(report: RestoreReport) -> RestoreReport:
    return report
