"""Asynchronous, version-safe Summary and Memory projections.

The Job Worker consumes only the transactional `Session Events Committed`
contract. It reads the authoritative Session Event range after that commit,
therefore a delayed or failed projection never delays an Agent reply.
"""

from __future__ import annotations

import asyncio
import hmac
import importlib
import json
import logging
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Self
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal
from trpc_service.admin_api.database import Connection, Database
from trpc_service.artifacts import ArtifactLifecycleWorker
from trpc_service.content_lifecycle import ContentDeletionWorker, DeletionExecutor, RetentionSweep
from trpc_service.execution_bus import (
    JOB_WORKER_SOURCE,
    MEMORY_INVALIDATED_EVENT,
    SESSION_EVENTS_COMMITTED_EVENT,
    ExecutionEnvelope,
    SessionEventsCommittedData,
    insert_outbox_record,
    session_partition_key,
)
from trpc_service.ids import uuid7
from trpc_service.memory_access import IM_GROUP_MEMORY_POLICY
from trpc_service.storage_migration import StorageMigrationAdapterFactory, StorageMigrationExecutor
from trpc_service.storage_migration_worker import StorageMigrationWorker
from trpc_service.version import TRPC_AGENT_VERSION, __version__

LOGGER = logging.getLogger(__name__)
MEMORY_VISIBILITY_P99_TARGET_SECONDS = 5.0
MAX_PROJECTION_ATTEMPTS = 8


@dataclass(frozen=True)
class ProjectionLagAlert:
    tenant_id: str
    session_id: str
    source_version: int
    visibility_seconds: float


@dataclass(frozen=True)
class SessionEventRange:
    """An inclusive, committed Session Event range used by one projection."""

    from_version: int
    to_version: int

    @classmethod
    def from_event(cls, data: SessionEventsCommittedData) -> Self:
        if data.from_version >= data.to_version:
            raise ValueError("SESSION_EVENT_RANGE_INVALID")
        return cls(from_version=data.from_version, to_version=data.to_version)

    @property
    def first_sequence(self) -> int:
        return self.from_version + 1

    @property
    def event_count(self) -> int:
        return self.to_version - self.from_version


@dataclass(frozen=True)
class ProjectionMetrics:
    p99_visibility_seconds: float
    target_seconds: float
    pending_count: int
    dead_letter_count: int
    oldest_pending_seconds: float
    alerting: bool
    degraded: bool


class MemoryInvalidationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    actor: str = Field(min_length=1, max_length=256)
    reason: str = Field(min_length=1, max_length=512)


@dataclass
class ProjectionMonitor:
    """Small observability seam; production can adapt this to metrics/alerts."""

    target_seconds: float = MEMORY_VISIBILITY_P99_TARGET_SECONDS
    visibility_seconds: list[float] = field(default_factory=list)
    backlog_alerts: list[ProjectionLagAlert] = field(default_factory=list)

    def record(
        self, *, tenant_id: str, session_id: str, source_version: int, committed_at: str
    ) -> None:
        committed = datetime.fromisoformat(committed_at.replace("Z", "+00:00"))
        elapsed = max((datetime.now(UTC) - committed).total_seconds(), 0.0)
        self.visibility_seconds.append(elapsed)
        if elapsed > self.target_seconds:
            alert = ProjectionLagAlert(tenant_id, session_id, source_version, elapsed)
            self.backlog_alerts.append(alert)
            LOGGER.warning(
                "memory projection backlog exceeds target",
                extra={
                    "tenant_id": tenant_id,
                    "session_id": session_id,
                    "source_version": source_version,
                    "visibility_seconds": elapsed,
                },
            )

    @property
    def p99_visibility_seconds(self) -> float:
        if not self.visibility_seconds:
            return 0.0
        ordered = sorted(self.visibility_seconds)
        return ordered[math.ceil(len(ordered) * 0.99) - 1]


class SummaryMemoryJobWorker:
    """Project committed ranges with source-range idempotency and version gating."""

    def __init__(self, database: Database, monitor: ProjectionMonitor | None = None) -> None:
        self._database = database
        self.monitor = monitor or ProjectionMonitor()

    async def handle(self, envelope: ExecutionEnvelope) -> None:
        if envelope.event_type != SESSION_EVENTS_COMMITTED_EVENT:
            raise ValueError("JOB_WORKER_UNSUPPORTED_EVENT")
        data = SessionEventsCommittedData.model_validate(envelope.data)
        event_range = SessionEventRange.from_event(data)
        tenant_id = UUID(data.tenant_id)
        async with self._database.tenant_transaction(tenant_id) as connection:
            execution = await _projection_execution(connection, tenant_id, data)
            summary_events = await _source_events(
                connection, tenant_id, data.session_id, 1, data.to_version
            )
            _assert_complete(summary_events, expected_count=data.to_version, first_sequence=1)
            await _upsert_summary(
                connection, tenant_id, data.session_id, event_range, summary_events
            )
            range_events = await _source_events(
                connection,
                tenant_id,
                data.session_id,
                event_range.first_sequence,
                event_range.to_version,
            )
            _assert_complete(
                range_events,
                expected_count=event_range.event_count,
                first_sequence=event_range.first_sequence,
            )
            await _insert_memory_projection(
                connection, tenant_id, data.session_id, event_range, execution, range_events
            )
        self.monitor.record(
            tenant_id=data.tenant_id,
            session_id=data.session_id,
            source_version=data.to_version,
            committed_at=envelope.time,
        )

    async def correct_memory(
        self, tenant_id: str, memory_id: UUID, *, actor: str, reason: str
    ) -> bool:
        return await self._invalidate_memory(
            tenant_id, memory_id, actor=actor, reason=reason, action="memory.corrected"
        )

    async def _invalidate_memory(
        self, tenant_id: str, memory_id: UUID, *, actor: str, reason: str, action: str
    ) -> bool:
        """Invalidate a derived Memory and append correction evidence; events stay immutable."""

        if not 1 <= len(actor) <= 256 or not 1 <= len(reason) <= 512:
            raise ValueError("MEMORY_CORRECTION_INVALID")
        parsed_tenant_id = UUID(tenant_id)
        async with self._database.tenant_transaction(parsed_tenant_id) as connection:
            invalidated = await connection.fetchrow(
                """UPDATE tenant.memory_record SET is_valid=false,invalidated_at=now(),
                invalidation_reason=$3 WHERE tenant_id=$1 AND id=$2 AND is_valid=true
                RETURNING source_session_id""",
                parsed_tenant_id,
                memory_id,
                reason,
            )
            if invalidated is None:
                return False
            await _publish_memory_invalidation(
                connection,
                tenant_id=parsed_tenant_id,
                memory_id=str(memory_id),
                session_id=str(invalidated["source_session_id"]),
                reason=action.removeprefix("memory.").upper(),
            )
            await insert_audit(
                connection,
                Principal(subject=actor, auth_method="oidc", roles=frozenset()),
                action,
                "ALLOW",
                target_type="memory",
                target_id=str(memory_id),
                tenant_id=parsed_tenant_id,
                details={"reason": reason},
            )
        return True


async def _source_events(
    connection: Connection, tenant_id: UUID, session_id: str, start: int, end: int
) -> list[Any]:
    return await connection.fetch(
        """SELECT sequence,kind,payload FROM tenant.session_event
        WHERE tenant_id=$1 AND session_id=$2 AND sequence BETWEEN $3 AND $4
        ORDER BY sequence""",
        tenant_id,
        session_id,
        start,
        end,
    )


async def _projection_execution(
    connection: Connection, tenant_id: UUID, data: SessionEventsCommittedData
) -> Any:
    execution = await connection.fetchrow(
        """SELECT subject_id,memory_policy_version FROM tenant.agent_execution
        WHERE tenant_id=$1 AND id=$2 AND session_id=$3""",
        tenant_id,
        UUID(data.execution_id),
        data.session_id,
    )
    if execution is None:
        raise ValueError("SESSION_PROJECTION_EXECUTION_NOT_FOUND")
    return execution


def _assert_complete(events: list[Any], *, expected_count: int, first_sequence: int) -> None:
    if len(events) != expected_count or int(events[0]["sequence"]) != first_sequence:
        raise ValueError("SESSION_PROJECTION_SOURCE_INCOMPLETE")


async def _upsert_summary(
    connection: Connection,
    tenant_id: UUID,
    session_id: str,
    event_range: SessionEventRange,
    events: list[Any],
) -> None:
    source_from_version, content = _summary_projection(events)
    await connection.execute(
        """INSERT INTO tenant.session_summary
        (tenant_id,session_id,source_from_version,source_version,content)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (tenant_id,session_id) DO UPDATE SET
          source_from_version=EXCLUDED.source_from_version,
          source_version=EXCLUDED.source_version,
          content=EXCLUDED.content,
          updated_at=now()
        WHERE tenant.session_summary.source_version < EXCLUDED.source_version""",
        tenant_id,
        session_id,
        source_from_version,
        event_range.to_version,
        content,
    )


async def _insert_memory_projection(
    connection: Connection,
    tenant_id: UUID,
    session_id: str,
    event_range: SessionEventRange,
    execution: Any,
    events: list[Any],
) -> None:
    subject_id = execution["subject_id"]
    if subject_id is None or execution["memory_policy_version"] == IM_GROUP_MEMORY_POLICY:
        return
    memory_id = await connection.fetchval(
        """INSERT INTO tenant.memory_record
        (tenant_id,id,subject_id,source_session_id,source_from_version,source_to_version,
        policy_version,content)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (tenant_id,subject_id,source_session_id,source_from_version,
        source_to_version) DO NOTHING RETURNING id""",
        tenant_id,
        uuid7(),
        str(subject_id),
        session_id,
        event_range.first_sequence,
        event_range.to_version,
        str(execution["memory_policy_version"]),
        _projection_content(events),
    )
    if memory_id is not None:
        await _publish_memory_invalidation(
            connection,
            tenant_id=tenant_id,
            memory_id=str(memory_id),
            session_id=session_id,
            reason="CREATED",
        )


def _projection_content(events: list[Any]) -> str:
    parts = [content for event in events if (content := _event_content(event)) is not None]
    return "\n".join(parts)[:16000] or "Session Event projection contained no textual content."


def _event_content(event: Any) -> str | None:
    payload = event["payload"]
    value = payload if isinstance(payload, dict) else json.loads(str(payload))
    content = value.get("content")
    if content is None:
        choices = value.get("completion", {}).get("choices", [])
        if choices and isinstance(choices[0], dict):
            content = choices[0].get("message", {}).get("content")
    return f"{event['kind']}: {content}" if content else None


def _summary_projection(events: list[Any]) -> tuple[int, str]:
    """Keep the newest complete Event range that fits in Summary storage."""

    fragments: list[tuple[int, str]] = []
    for event in events:
        content = _event_content(event)
        if content is not None:
            fragments.append((int(event["sequence"]), content))
    if not fragments:
        return int(events[0]["sequence"]), "Session Event projection contained no textual content."

    selected: list[tuple[int, str]] = []
    remaining = 16000
    for sequence, content in reversed(fragments):
        separator = 1 if selected else 0
        available = remaining - separator
        if available <= 0:
            break
        if len(content) > available:
            selected.append((sequence, content[-available:]))
            break
        selected.append((sequence, content))
        remaining -= len(content) + separator
    selected.reverse()
    return selected[0][0], "\n".join(content for _, content in selected)


async def _publish_memory_invalidation(
    connection: Connection,
    *,
    tenant_id: UUID,
    memory_id: str,
    session_id: str,
    reason: str,
) -> None:
    await insert_outbox_record(
        connection,
        tenant_id=str(tenant_id),
        message_id=str(uuid7()),
        source=JOB_WORKER_SOURCE,
        event_type=MEMORY_INVALIDATED_EVENT,
        partition_key=session_partition_key(str(tenant_id), session_id),
        payload_json=json.dumps(
            {"tenant_id": str(tenant_id), "memory_id": memory_id, "reason": reason}
        ),
        correlation_id=memory_id,
    )


class SessionProjectionConsumer:
    """Durably acknowledges published committed-event records for Job Worker replicas.

    The current execution-bus adapter persists its publish state in Outbox. This
    consumer claims those published records with `SKIP LOCKED`; the projection
    is committed before the acknowledgement, so a crash is replay-safe.
    """

    def __init__(
        self,
        database: Database,
        projections: SummaryMemoryJobWorker,
        *,
        max_attempts: int = MAX_PROJECTION_ATTEMPTS,
    ) -> None:
        self._database = database
        self._projections = projections
        self._max_attempts = max_attempts

    async def run_once(self) -> bool:
        async with self._database.transaction() as connection:
            row = await connection.fetchrow(
                """SELECT o.id,o.tenant_id,o.message_id,o.source,o.event_type,o.partition_key,
                o.causation_id,o.correlation_id,o.data_classification,o.payload,o.created_at,
                COALESCE(d.attempts,0) AS attempts
                FROM platform.outbox_record o
                LEFT JOIN platform.session_projection_delivery d ON d.outbox_id=o.id
                WHERE o.status='PUBLISHED' AND o.event_type=$1
                  AND (d.status IS NULL OR d.status='PENDING')
                  AND COALESCE(d.attempts,0) < $2
                ORDER BY o.created_at,o.id LIMIT 1 FOR UPDATE OF o SKIP LOCKED""",
                SESSION_EVENTS_COMMITTED_EVENT,
                self._max_attempts,
            )
            if row is None:
                return False
            envelope = _outbox_envelope(row)
            try:
                await self._projections.handle(envelope)
            except Exception as error:
                await self._record_delivery_failure(connection, row, str(error))
                return False
            await connection.execute(
                """INSERT INTO platform.session_projection_delivery
                (outbox_id,tenant_id,status,attempts,completed_at)
                VALUES ($1,$2,'SUCCEEDED',$3,now())
                ON CONFLICT (outbox_id) DO UPDATE SET status='SUCCEEDED',attempts=$3,
                last_error=NULL,completed_at=now(),updated_at=now()""",
                row["id"],
                row["tenant_id"],
                int(row["attempts"]) + 1,
            )
            return True

    async def metrics(self) -> ProjectionMetrics:
        async with self._database.transaction() as connection:
            p99 = await connection.fetchval(
                """SELECT COALESCE(percentile_cont(0.99) WITHIN GROUP
                (ORDER BY EXTRACT(EPOCH FROM d.completed_at-o.created_at)),0)
                FROM platform.session_projection_delivery d
                JOIN platform.outbox_record o ON o.id=d.outbox_id
                WHERE d.status='SUCCEEDED' AND o.event_type=$1""",
                SESSION_EVENTS_COMMITTED_EVENT,
            )
            pending = await connection.fetchrow(
                """SELECT count(*) AS count,
                count(*) FILTER (WHERE d.status='DEAD_LETTER') AS dead_letters,
                COALESCE(EXTRACT(EPOCH FROM now()-min(o.created_at)),0) AS oldest
                FROM platform.outbox_record o
                LEFT JOIN platform.session_projection_delivery d ON d.outbox_id=o.id
                WHERE o.event_type=$1 AND (
                  o.status='PENDING' OR d.status IS NULL OR d.status IN ('PENDING','DEAD_LETTER')
                )""",
                SESSION_EVENTS_COMMITTED_EVENT,
            )
        assert pending is not None
        p99_seconds = float(p99)
        oldest_seconds = float(pending["oldest"])
        pending_count = int(pending["count"])
        dead_letter_count = int(pending["dead_letters"])
        alerting = (
            p99_seconds > MEMORY_VISIBILITY_P99_TARGET_SECONDS
            or oldest_seconds > MEMORY_VISIBILITY_P99_TARGET_SECONDS
            or dead_letter_count > 0
        )
        return ProjectionMetrics(
            p99_visibility_seconds=p99_seconds,
            target_seconds=MEMORY_VISIBILITY_P99_TARGET_SECONDS,
            pending_count=pending_count,
            dead_letter_count=dead_letter_count,
            oldest_pending_seconds=oldest_seconds,
            alerting=alerting,
            degraded=alerting,
        )

    async def _record_delivery_failure(self, connection: Connection, row: Any, error: str) -> None:
        attempts = int(row["attempts"]) + 1
        status = "DEAD_LETTER" if attempts >= self._max_attempts else "PENDING"
        await connection.execute(
            """INSERT INTO platform.session_projection_delivery
            (outbox_id,tenant_id,status,attempts,last_error)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (outbox_id) DO UPDATE SET status=$3,attempts=$4,last_error=$5,
            updated_at=now()""",
            row["id"],
            row["tenant_id"],
            status,
            attempts,
            error[:512],
        )


def _outbox_envelope(row: Any) -> ExecutionEnvelope:
    return ExecutionEnvelope(
        message_id=str(row["message_id"]),
        source=str(row["source"]),
        event_type=str(row["event_type"]),
        partition_key=str(row["partition_key"]),
        time=row["created_at"].isoformat(),
        tenant_id=str(row["tenant_id"]),
        data_schema=f"{row['event_type']}.schema.json",
        data=dict(row["payload"]),
        causation_id=str(row["causation_id"]) if row["causation_id"] is not None else None,
        correlation_id=(str(row["correlation_id"]) if row["correlation_id"] is not None else None),
        data_classification=(
            str(row["data_classification"]) if row["data_classification"] is not None else None
        ),
    )


class JobWorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = ""
    projection_poll_interval_seconds: float = 0.5
    artifact_lifecycle_interval_seconds: float = 3600
    artifact_access_key: str = ""
    content_deletion_enabled: bool = False
    content_deletion_executor_factory: str = ""
    operator_token: str = ""

    def validate_runtime(self) -> None:
        if not self.database_url:
            raise RuntimeError("Job Worker configuration is incomplete: DATABASE_URL")
        if self.projection_poll_interval_seconds <= 0:
            raise RuntimeError(
                "Job Worker configuration is invalid: PROJECTION_POLL_INTERVAL_SECONDS"
            )
        if self.artifact_lifecycle_interval_seconds <= 0:
            raise RuntimeError(
                "Job Worker configuration is invalid: ARTIFACT_LIFECYCLE_INTERVAL_SECONDS"
            )
        if self.content_deletion_enabled and not self.content_deletion_executor_factory:
            raise RuntimeError(
                "Job Worker configuration is incomplete: CONTENT_DELETION_EXECUTOR_FACTORY"
            )


def create_app(
    settings: JobWorkerSettings | None = None,
    *,
    deletion_executor: DeletionExecutor | None = None,
    storage_migration_factory: StorageMigrationAdapterFactory | None = None,
) -> FastAPI:
    configured = settings or JobWorkerSettings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        configured.validate_runtime()
        resolved_deletion_executor = deletion_executor
        if resolved_deletion_executor is None and configured.content_deletion_enabled:
            resolved_deletion_executor = _load_deletion_executor(
                configured.content_deletion_executor_factory, configured.database_url
            )
        database = Database(configured.database_url)
        await database.open()
        projections = SummaryMemoryJobWorker(database)
        consumer = SessionProjectionConsumer(database, projections)
        application.state.projections = projections
        application.state.consumer = consumer
        lifecycle = (
            ArtifactLifecycleWorker(database, access_key=configured.artifact_access_key.encode())
            if configured.artifact_access_key
            else None
        )
        deletion_lifecycle = (
            ContentDeletionWorker(database, resolved_deletion_executor)
            if resolved_deletion_executor is not None
            else None
        )
        retention_lifecycle = RetentionSweep(database)
        migration_worker = (
            StorageMigrationWorker(database, StorageMigrationExecutor(storage_migration_factory))
            if storage_migration_factory is not None
            else None
        )
        application.state.storage_migration_worker = migration_worker

        async def consume() -> None:
            while True:
                try:
                    delivered = await consumer.run_once()
                except Exception:
                    LOGGER.exception("session projection consumer failed")
                    delivered = False
                if not delivered:
                    await asyncio.sleep(configured.projection_poll_interval_seconds)

        consume_task = asyncio.create_task(consume())

        async def purge_artifacts() -> None:
            assert lifecycle is not None
            while True:
                try:
                    await lifecycle.run_once()
                except Exception:
                    LOGGER.exception("artifact lifecycle worker failed")
                await asyncio.sleep(configured.artifact_lifecycle_interval_seconds)

        lifecycle_task = asyncio.create_task(purge_artifacts()) if lifecycle is not None else None

        async def erase_requested_content() -> None:
            assert deletion_lifecycle is not None
            while True:
                try:
                    await deletion_lifecycle.run_once()
                except Exception:
                    LOGGER.exception("content deletion lifecycle worker failed")
                await asyncio.sleep(configured.artifact_lifecycle_interval_seconds)

        deletion_task = (
            asyncio.create_task(erase_requested_content())
            if deletion_lifecycle is not None
            else None
        )

        async def purge_expired_content() -> None:
            while True:
                try:
                    await retention_lifecycle.run_once()
                except Exception:
                    LOGGER.exception("content retention lifecycle worker failed")
                await asyncio.sleep(configured.artifact_lifecycle_interval_seconds)

        retention_task = asyncio.create_task(purge_expired_content())

        async def migrate_storage() -> None:
            assert migration_worker is not None
            while True:
                progressed = False
                try:
                    async with database.transaction() as connection:
                        tenants = await connection.fetch(
                            "SELECT id FROM platform.tenant WHERE status='ACTIVE' ORDER BY id"
                        )
                    for tenant in tenants:
                        progressed = await migration_worker.run_once(tenant["id"]) or progressed
                except Exception:
                    LOGGER.exception("storage migration worker failed")
                if not progressed:
                    await asyncio.sleep(configured.projection_poll_interval_seconds)

        migration_task = (
            asyncio.create_task(migrate_storage()) if migration_worker is not None else None
        )
        try:
            yield
        finally:
            consume_task.cancel()
            if lifecycle_task is not None:
                lifecycle_task.cancel()
            if deletion_task is not None:
                deletion_task.cancel()
            retention_task.cancel()
            if migration_task is not None:
                migration_task.cancel()
            await database.close()

    application = FastAPI(title="tRPC-Agent Platform job-worker", lifespan=lifespan)

    def require_operator(operation_token: str | None) -> None:
        if not configured.operator_token or operation_token is None:
            raise HTTPException(status_code=403, detail="MEMORY_OPERATION_FORBIDDEN")
        if not hmac.compare_digest(operation_token, configured.operator_token):
            raise HTTPException(status_code=403, detail="MEMORY_OPERATION_FORBIDDEN")

    @application.get("/health/live")
    @application.get("/health/ready")
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "service": "job-worker",
            "version": __version__,
            "trpc_agent_version": TRPC_AGENT_VERSION,
        }

    @application.get("/internal/v1/projection-metrics", response_model=ProjectionMetrics)
    async def projection_metrics(
        operation_token: str | None = Header(default=None, alias="X-Job-Worker-Operator-Token"),
    ) -> ProjectionMetrics:
        require_operator(operation_token)
        consumer = application.state.consumer
        assert isinstance(consumer, SessionProjectionConsumer)
        return await consumer.metrics()

    async def correct_memory(
        tenant_id: UUID,
        memory_id: UUID,
        payload: MemoryInvalidationRequest,
        operation_token: str | None = Header(default=None, alias="X-Job-Worker-Operator-Token"),
    ) -> dict[str, bool]:
        require_operator(operation_token)
        projections = application.state.projections
        assert isinstance(projections, SummaryMemoryJobWorker)
        invalidated = await projections.correct_memory(
            str(tenant_id), memory_id, actor=payload.actor, reason=payload.reason
        )
        if not invalidated:
            raise HTTPException(status_code=404, detail="MEMORY_NOT_FOUND_OR_INVALID")
        return {"invalidated": True}

    @application.post("/internal/v1/tenants/{tenant_id}/memories/{memory_id}/corrections")
    async def correct_memory_endpoint(
        tenant_id: UUID,
        memory_id: UUID,
        payload: MemoryInvalidationRequest,
        operation_token: str | None = Header(default=None, alias="X-Job-Worker-Operator-Token"),
    ) -> dict[str, bool]:
        return await correct_memory(tenant_id, memory_id, payload, operation_token)

    return application


def _load_deletion_executor(factory_path: str, database_url: str) -> DeletionExecutor:
    """Load the deployment-owned backend binding before the worker starts.

    This is deliberately mandatory in production: starting without all six
    erasure adapters would otherwise accept requests that can never complete.
    """

    module_name, separator, attribute = factory_path.partition(":")
    if not separator or not module_name or not attribute:
        raise RuntimeError("CONTENT_DELETION_EXECUTOR_FACTORY_INVALID")
    factory = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(factory):
        raise RuntimeError("CONTENT_DELETION_EXECUTOR_FACTORY_INVALID")
    executor = factory(database_url)
    if not isinstance(executor, DeletionExecutor):
        raise RuntimeError("CONTENT_DELETION_EXECUTOR_FACTORY_INVALID")
    return executor


app = create_app()
