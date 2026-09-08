"""Public contract tests for versioned online Storage Profile migration."""

from __future__ import annotations

import pytest

from trpc_service.storage import InMemorySqlAdapter
from trpc_service.storage_migration import (
    MigrationOperation,
    StorageMigrationError,
    StorageMigrationExecutor,
    StorageMigrationState,
    StorageMigrationStateMachine,
    ValidationEvidence,
)


class _Factory:
    def __init__(self, source: InMemorySqlAdapter, target: InMemorySqlAdapter) -> None:
        self.source = source
        self.target = target

    async def adapters(self, **_kwargs: str) -> list[tuple[InMemorySqlAdapter, InMemorySqlAdapter]]:
        return [(self.source, self.target)]


def test_migration_advances_in_order_and_switches_one_business_authority() -> None:
    machine = StorageMigrationStateMachine("source-profile", "target-profile")

    assert machine.business_write_profile_id == "source-profile"
    assert machine.state is StorageMigrationState.PREPARED
    assert machine.advance(MigrationOperation.START_BACKFILL) is StorageMigrationState.BACKFILLING
    assert (
        machine.advance(MigrationOperation.BACKFILL_COMPLETE) is StorageMigrationState.CATCHING_UP
    )
    assert machine.advance(MigrationOperation.CATCH_UP_COMPLETE) is StorageMigrationState.VALIDATING
    assert (
        machine.advance(
            MigrationOperation.VALIDATE,
            validation=ValidationEvidence(
                source_count=10,
                target_count=10,
                source_digest="a" * 64,
                target_digest="a" * 64,
                source_version=42,
                target_version=42,
                vector_recall=0.99,
            ),
        )
        is StorageMigrationState.READY_TO_SWITCH
    )
    assert machine.business_write_profile_id == "source-profile"
    assert machine.advance(MigrationOperation.SWITCH) is StorageMigrationState.OBSERVING
    assert machine.business_write_profile_id == "target-profile"


@pytest.mark.parametrize(
    "validation",
    [
        ValidationEvidence(
            source_count=10,
            target_count=9,
            source_digest="a" * 64,
            target_digest="a" * 64,
            source_version=1,
            target_version=1,
            vector_recall=0.99,
        ),
        ValidationEvidence(
            source_count=10,
            target_count=10,
            source_digest="a" * 64,
            target_digest="b" * 64,
            source_version=1,
            target_version=1,
            vector_recall=0.99,
        ),
        ValidationEvidence(
            source_count=10,
            target_count=10,
            source_digest="a" * 64,
            target_digest="a" * 64,
            source_version=1,
            target_version=2,
            vector_recall=0.99,
        ),
        ValidationEvidence(
            source_count=10,
            target_count=10,
            source_digest="a" * 64,
            target_digest="a" * 64,
            source_version=1,
            target_version=1,
            vector_recall=0.94,
        ),
    ],
)
def test_validation_rejects_incomplete_or_low_quality_destination(
    validation: ValidationEvidence,
) -> None:
    machine = StorageMigrationStateMachine("source-profile", "target-profile")
    machine.advance(MigrationOperation.START_BACKFILL)
    machine.advance(MigrationOperation.BACKFILL_COMPLETE)
    machine.advance(MigrationOperation.CATCH_UP_COMPLETE)

    with pytest.raises(StorageMigrationError, match="STORAGE_MIGRATION_VALIDATION_FAILED"):
        machine.advance(MigrationOperation.VALIDATE, validation=validation)
    assert machine.state is StorageMigrationState.VALIDATING


def test_rollback_is_only_available_during_observation() -> None:
    machine = StorageMigrationStateMachine("source-profile", "target-profile")
    with pytest.raises(StorageMigrationError, match="STORAGE_MIGRATION_INVALID_TRANSITION"):
        machine.advance(MigrationOperation.START_ROLLBACK)


def test_rollback_fences_business_writes_and_failure_cannot_strand_cutover() -> None:
    machine = StorageMigrationStateMachine(
        "source-profile", "target-profile", state=StorageMigrationState.OBSERVING
    )
    with pytest.raises(StorageMigrationError, match="STORAGE_MIGRATION_INVALID_TRANSITION"):
        machine.advance(MigrationOperation.FAIL)
    assert machine.advance(MigrationOperation.START_ROLLBACK) is StorageMigrationState.ROLLING_BACK
    assert machine.business_write_profile_id is None


@pytest.mark.asyncio
async def test_worker_executor_backfills_and_replays_changes_before_validation() -> None:
    source = InMemorySqlAdapter()
    target = InMemorySqlAdapter()
    await source.put("tenant-a", "one", b"first")
    executor = StorageMigrationExecutor(_Factory(source, target))

    checkpoint = await executor.backfill(
        tenant_id="tenant-a", source_profile_id="source", target_profile_id="target"
    )
    await source.put("tenant-a", "two", b"second")
    await executor.catch_up(
        tenant_id="tenant-a",
        source_profile_id="source",
        target_profile_id="target",
        watermarks=checkpoint,
    )

    evidence = await executor.validate(
        tenant_id="tenant-a", source_profile_id="source", target_profile_id="target"
    )
    assert evidence.is_valid()
    assert await target.get("tenant-a", "two") == b"second"
