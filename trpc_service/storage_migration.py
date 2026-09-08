"""Versioned, worker-executed online Storage Profile migration primitives.

The Admin API can request and approve an operation; it cannot assert that data
was copied. A privileged worker takes snapshots, replays change feeds, persists
watermarks, and collects validation evidence.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.storage import OnlineMigrationAdapter


class StorageMigrationState(StrEnum):
    PREPARED = "PREPARED"
    BACKFILLING = "BACKFILLING"
    CATCHING_UP = "CATCHING_UP"
    VALIDATING = "VALIDATING"
    READY_TO_SWITCH = "READY_TO_SWITCH"
    OBSERVING = "OBSERVING"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLBACK_CATCHING_UP = "ROLLBACK_CATCHING_UP"
    ROLLBACK_VALIDATING = "ROLLBACK_VALIDATING"
    COMPLETED = "COMPLETED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


class MigrationOperation(StrEnum):
    START_BACKFILL = "START_BACKFILL"
    BACKFILL_COMPLETE = "BACKFILL_COMPLETE"
    CATCH_UP_COMPLETE = "CATCH_UP_COMPLETE"
    VALIDATE = "VALIDATE"
    SWITCH = "SWITCH"
    COMPLETE = "COMPLETE"
    START_ROLLBACK = "START_ROLLBACK"
    ROLLBACK_BACKFILL_COMPLETE = "ROLLBACK_BACKFILL_COMPLETE"
    ROLLBACK_CATCH_UP_COMPLETE = "ROLLBACK_CATCH_UP_COMPLETE"
    ROLLBACK_VALIDATE = "ROLLBACK_VALIDATE"
    FAIL = "FAIL"


class StorageMigrationError(RuntimeError):
    """Stable error code for migration orchestration clients."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ValidationEvidence(BaseModel):
    """Evidence collected after catch-up and before an atomic profile switch."""

    model_config = ConfigDict(frozen=True)

    source_count: int = Field(ge=0)
    target_count: int = Field(ge=0)
    source_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_version: int = Field(ge=0)
    target_version: int = Field(ge=0)
    vector_recall: float = Field(ge=0, le=1)

    def is_valid(self, *, minimum_vector_recall: float = 0.95) -> bool:
        return (
            self.source_count == self.target_count
            and self.source_digest == self.target_digest
            and self.source_version == self.target_version
            and self.vector_recall >= minimum_vector_recall
        )


class StorageMigrationStateMachine:
    """Prevent skipped phases and uncoordinated source/target dual writes."""

    _TRANSITIONS = {
        (StorageMigrationState.PREPARED, MigrationOperation.START_BACKFILL): (
            StorageMigrationState.BACKFILLING
        ),
        (StorageMigrationState.BACKFILLING, MigrationOperation.BACKFILL_COMPLETE): (
            StorageMigrationState.CATCHING_UP
        ),
        (StorageMigrationState.CATCHING_UP, MigrationOperation.CATCH_UP_COMPLETE): (
            StorageMigrationState.VALIDATING
        ),
        (StorageMigrationState.VALIDATING, MigrationOperation.VALIDATE): (
            StorageMigrationState.READY_TO_SWITCH
        ),
        (StorageMigrationState.READY_TO_SWITCH, MigrationOperation.SWITCH): (
            StorageMigrationState.OBSERVING
        ),
        (StorageMigrationState.OBSERVING, MigrationOperation.COMPLETE): (
            StorageMigrationState.COMPLETED
        ),
        (StorageMigrationState.OBSERVING, MigrationOperation.START_ROLLBACK): (
            StorageMigrationState.ROLLING_BACK
        ),
        (StorageMigrationState.ROLLING_BACK, MigrationOperation.ROLLBACK_BACKFILL_COMPLETE): (
            StorageMigrationState.ROLLBACK_CATCHING_UP
        ),
        (
            StorageMigrationState.ROLLBACK_CATCHING_UP,
            MigrationOperation.ROLLBACK_CATCH_UP_COMPLETE,
        ): (StorageMigrationState.ROLLBACK_VALIDATING),
        (StorageMigrationState.ROLLBACK_VALIDATING, MigrationOperation.ROLLBACK_VALIDATE): (
            StorageMigrationState.ROLLED_BACK
        ),
    }

    def __init__(
        self,
        source_profile_id: str,
        target_profile_id: str,
        *,
        state: StorageMigrationState = StorageMigrationState.PREPARED,
    ) -> None:
        if source_profile_id == target_profile_id:
            raise StorageMigrationError("STORAGE_MIGRATION_IDENTICAL_PROFILES")
        self.source_profile_id = source_profile_id
        self.target_profile_id = target_profile_id
        self.state = state

    @property
    def business_write_profile_id(self) -> str | None:
        """The sole profile application code may write at the current phase."""

        if self.state in {
            StorageMigrationState.ROLLING_BACK,
            StorageMigrationState.ROLLBACK_CATCHING_UP,
            StorageMigrationState.ROLLBACK_VALIDATING,
        }:
            return None
        if self.state in {
            StorageMigrationState.OBSERVING,
            StorageMigrationState.COMPLETED,
        }:
            return self.target_profile_id
        return self.source_profile_id

    @property
    def source_is_authoritative(self) -> bool:
        return self.business_write_profile_id == self.source_profile_id

    def advance(
        self,
        operation: MigrationOperation,
        *,
        validation: ValidationEvidence | None = None,
    ) -> StorageMigrationState:
        if operation is MigrationOperation.FAIL and self.state in {
            StorageMigrationState.PREPARED,
            StorageMigrationState.BACKFILLING,
            StorageMigrationState.CATCHING_UP,
            StorageMigrationState.VALIDATING,
            StorageMigrationState.READY_TO_SWITCH,
        }:
            self.state = StorageMigrationState.FAILED
            return self.state
        next_state = self._TRANSITIONS.get((self.state, operation))
        if next_state is None:
            raise StorageMigrationError("STORAGE_MIGRATION_INVALID_TRANSITION")
        if operation is MigrationOperation.VALIDATE and (
            validation is None or not validation.is_valid()
        ):
            raise StorageMigrationError("STORAGE_MIGRATION_VALIDATION_FAILED")
        self.state = next_state
        return self.state


class StorageMigrationAdapterFactory(Protocol):
    """Resolve worker-only adapter pairs for one profile pair."""

    async def adapters(
        self,
        *,
        tenant_id: str,
        source_profile_id: str,
        target_profile_id: str,
    ) -> list[tuple[OnlineMigrationAdapter, OnlineMigrationAdapter]]: ...


class StorageMigrationExecutor:
    """Copies and catches up all adapter pairs; business requests never use it."""

    def __init__(self, factory: StorageMigrationAdapterFactory) -> None:
        self._factory = factory

    async def backfill(
        self, *, tenant_id: str, source_profile_id: str, target_profile_id: str
    ) -> dict[str, int]:
        pairs = await self._factory.adapters(
            tenant_id=tenant_id,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
        )
        if not pairs:
            raise StorageMigrationError("STORAGE_MIGRATION_ADAPTERS_UNAVAILABLE")
        watermarks: dict[str, int] = {}
        for index, (source, target) in enumerate(pairs):
            watermark, snapshot = await source.snapshot_tenant(tenant_id)
            await target.apply_changes(tenant_id, list(snapshot.items()))
            watermarks[str(index)] = watermark
        return watermarks

    async def catch_up(
        self,
        *,
        tenant_id: str,
        source_profile_id: str,
        target_profile_id: str,
        watermarks: dict[str, int],
    ) -> dict[str, int]:
        pairs = await self._factory.adapters(
            tenant_id=tenant_id,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
        )
        if len(pairs) != len(watermarks):
            raise StorageMigrationError("STORAGE_MIGRATION_CHECKPOINT_INVALID")
        updated: dict[str, int] = {}
        for index, (source, target) in enumerate(pairs):
            watermark, changes = await source.changes_since(tenant_id, watermarks[str(index)])
            await target.apply_changes(tenant_id, changes)
            updated[str(index)] = watermark
        return updated

    async def validate(
        self, *, tenant_id: str, source_profile_id: str, target_profile_id: str
    ) -> ValidationEvidence:
        pairs = await self._factory.adapters(
            tenant_id=tenant_id,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
        )
        if not pairs:
            raise StorageMigrationError("STORAGE_MIGRATION_ADAPTERS_UNAVAILABLE")
        source_counts: list[int] = []
        target_counts: list[int] = []
        source_digests: list[str] = []
        target_digests: list[str] = []
        source_versions: list[int] = []
        target_versions: list[int] = []
        recalls: list[float] = []
        for source, target in pairs:
            source_count, source_digest, source_version = await source.tenant_fingerprint(tenant_id)
            target_count, target_digest, target_version = await target.tenant_fingerprint(tenant_id)
            source_counts.append(source_count)
            target_counts.append(target_count)
            source_digests.append(source_digest)
            target_digests.append(target_digest)
            source_versions.append(source_version)
            # The target's native sequence is not comparable across backend
            # implementations. The source watermark is the replay checkpoint
            # and becomes the target version only after equal content proof.
            target_versions.append(
                source_version
                if source_count == target_count and source_digest == target_digest
                else target_version
            )
            recalls.append(await target.vector_recall_against(source, tenant_id))

        import hashlib

        def folded(values: list[str]) -> str:
            return hashlib.sha256("".join(values).encode()).hexdigest()

        return ValidationEvidence(
            source_count=sum(source_counts),
            target_count=sum(target_counts),
            source_digest=folded(source_digests),
            target_digest=folded(target_digests),
            source_version=max(source_versions),
            target_version=max(target_versions),
            vector_recall=min(recalls),
        )
