"""PostgreSQL persistence for channel bindings, the inbound ledger and deliveries."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from trpc_service.admin_api.database import Database
from trpc_service.channels.bindings import (
    ChannelBinding,
    ChannelBindingConflict,
    ChannelBindingStatus,
)
from trpc_service.channels.delivery import DeliveryAttempt, DeliveryState, ReplyDelivery
from trpc_service.channels.inbound import InboundConflict, InboundMessage, InboundStatus


def _binding_from_row(row: Any) -> ChannelBinding:
    return ChannelBinding(
        tenant_id=str(row["tenant_id"]),
        binding_id=str(row["binding_id"]),
        channel_type=str(row["channel_type"]),
        external_bot_id=str(row["external_bot_id"]),
        application_id=str(row["application_id"]),
        environment=str(row["environment"]),
        secret_ref=str(row["secret_ref"]),
        status=ChannelBindingStatus(str(row["status"])),
    )


def _inbound_from_row(row: Any) -> InboundMessage:
    occurred = row["occurred_at"]
    return InboundMessage(
        tenant_id=str(row["tenant_id"]),
        binding_id=str(row["binding_id"]),
        message_key=str(row["message_key"]),
        payload_hash=str(row["payload_hash"]),
        external_user_id=str(row["external_user_id"]),
        execution_id=str(row["execution_id"]) if row["execution_id"] else None,
        release_id=str(row["release_id"]) if row["release_id"] else None,
        status=InboundStatus(str(row["status"])),
        occurred_at=occurred.isoformat() if hasattr(occurred, "isoformat") else str(occurred),
    )


class DatabaseBindingStore:
    """`tenant.channel_binding` persistence; conflicts are detected at insert."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def insert(self, binding: ChannelBinding, *, created_by: str = "") -> ChannelBinding:
        async with self._database.tenant_transaction(UUID(binding.tenant_id)) as connection:
            inserted = await connection.fetchrow(
                """INSERT INTO tenant.channel_binding
                    (tenant_id,binding_id,channel_type,external_bot_id,application_id,
                     environment,secret_ref,status,created_by)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (tenant_id,channel_type,external_bot_id) DO NOTHING
                    RETURNING *""",
                UUID(binding.tenant_id),
                UUID(binding.binding_id),
                binding.channel_type,
                binding.external_bot_id,
                UUID(binding.application_id),
                binding.environment,
                binding.secret_ref,
                str(binding.status),
                created_by,
            )
            if inserted is None:
                stored = await connection.fetchrow(
                    """SELECT * FROM tenant.channel_binding
                    WHERE tenant_id=$1 AND channel_type=$2 AND external_bot_id=$3""",
                    UUID(binding.tenant_id),
                    binding.channel_type,
                    binding.external_bot_id,
                )
                stored_binding = _binding_from_row(stored)
                if stored_binding.model_dump(exclude={"binding_id"}) != binding.model_dump(
                    exclude={"binding_id"}
                ):
                    raise ChannelBindingConflict("channel binding conflict: bot already bound")
                return stored_binding
            return _binding_from_row(inserted)

    async def resolve(
        self, tenant_id: str, channel_type: str, external_bot_id: str
    ) -> ChannelBinding | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                """SELECT * FROM tenant.channel_binding
                WHERE tenant_id=$1 AND channel_type=$2 AND external_bot_id=$3
                  AND status='ACTIVE'""",
                UUID(tenant_id),
                channel_type,
                external_bot_id,
            )
        return _binding_from_row(row) if row is not None else None

    async def set_status(
        self, tenant_id: str, binding_id: str, status: ChannelBindingStatus
    ) -> ChannelBinding | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                """UPDATE tenant.channel_binding SET status=$3
                WHERE tenant_id=$1 AND binding_id=$2 RETURNING *""",
                UUID(tenant_id),
                UUID(binding_id),
                str(status),
            )
        return _binding_from_row(row) if row is not None else None

    async def list(self, tenant_id: str) -> list[ChannelBinding]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                "SELECT * FROM tenant.channel_binding WHERE tenant_id=$1 ORDER BY created_at",
                UUID(tenant_id),
            )
        return [_binding_from_row(row) for row in rows]


class DatabaseInboundStore:
    """`tenant.inbound_message` ledger plus conflict evidence."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def insert(self, message: InboundMessage) -> tuple[InboundMessage, bool]:
        async with self._database.tenant_transaction(UUID(message.tenant_id)) as connection:
            inserted = await connection.fetchrow(
                """INSERT INTO tenant.inbound_message
                    (tenant_id,binding_id,message_key,payload_hash,external_user_id,
                     execution_id,release_id,status)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (tenant_id,binding_id,message_key) DO NOTHING
                    RETURNING *""",
                UUID(message.tenant_id),
                UUID(message.binding_id),
                message.message_key,
                message.payload_hash,
                message.external_user_id,
                UUID(message.execution_id) if message.execution_id else None,
                UUID(message.release_id) if message.release_id else None,
                str(message.status),
            )
            if inserted is not None:
                return _inbound_from_row(inserted), True
            existing = await connection.fetchrow(
                """SELECT * FROM tenant.inbound_message
                WHERE tenant_id=$1 AND binding_id=$2 AND message_key=$3""",
                UUID(message.tenant_id),
                UUID(message.binding_id),
                message.message_key,
            )
            return _inbound_from_row(existing), False

    async def record_conflict(self, conflict: InboundConflict) -> None:
        async with self._database.tenant_transaction(UUID(conflict.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.inbound_conflict
                    (tenant_id,conflict_id,binding_id,message_key,recorded_hash,received_hash)
                    VALUES ($1,$2,$3,$4,$5,$6)""",
                UUID(conflict.tenant_id),
                uuid4(),
                UUID(conflict.binding_id),
                conflict.message_key,
                conflict.recorded_hash,
                conflict.received_hash,
            )

    async def attach_execution(
        self,
        tenant_id: str,
        binding_id: str,
        message_key: str,
        execution_id: str,
        release_id: str,
    ) -> None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            await connection.execute(
                """UPDATE tenant.inbound_message
                SET execution_id=$3, release_id=$4
                WHERE tenant_id=$1 AND binding_id=$2 AND message_key=$5""",
                UUID(tenant_id),
                UUID(binding_id),
                UUID(execution_id),
                UUID(release_id),
                message_key,
            )

    async def list_conflicts(self, tenant_id: str) -> list[InboundConflict]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                """SELECT * FROM tenant.inbound_conflict
                WHERE tenant_id=$1 ORDER BY detected_at DESC LIMIT 100""",
                UUID(tenant_id),
            )
        return [
            InboundConflict(
                tenant_id=str(row["tenant_id"]),
                binding_id=str(row["binding_id"]),
                message_key=str(row["message_key"]),
                recorded_hash=str(row["recorded_hash"]),
                received_hash=str(row["received_hash"]),
                detected_at=row["detected_at"].isoformat(),
            )
            for row in rows
        ]


def _delivery_from_row(row: Any) -> ReplyDelivery:
    created = row["created_at"]
    return ReplyDelivery(
        tenant_id=str(row["tenant_id"]),
        delivery_id=str(row["delivery_id"]),
        binding_id=str(row["binding_id"]),
        execution_id=str(row["execution_id"]),
        external_conversation_id=str(row["external_conversation_id"]),
        content=row["content"],
        status=DeliveryState(str(row["status"])),
        attempts=int(row["attempts"]),
        created_at=created.isoformat() if hasattr(created, "isoformat") else str(created),
    )


class DatabaseDeliveryStore:
    """`tenant.reply_delivery` and attempt persistence."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def save_delivery(self, delivery: ReplyDelivery) -> None:
        async with self._database.tenant_transaction(UUID(delivery.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.reply_delivery
                    (tenant_id,delivery_id,binding_id,execution_id,
                     external_conversation_id,content,status,attempts)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (tenant_id,delivery_id) DO UPDATE SET
                      content=EXCLUDED.content,
                      status=EXCLUDED.status,
                      attempts=EXCLUDED.attempts,
                      updated_at=now()""",
                UUID(delivery.tenant_id),
                UUID(delivery.delivery_id),
                UUID(delivery.binding_id),
                delivery.execution_id,
                delivery.external_conversation_id,
                delivery.content,
                str(delivery.status),
                delivery.attempts,
            )

    async def get_delivery(self, tenant_id: str, delivery_id: str) -> ReplyDelivery | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM tenant.reply_delivery WHERE tenant_id=$1 AND delivery_id=$2",
                UUID(tenant_id),
                UUID(delivery_id),
            )
        return _delivery_from_row(row) if row is not None else None

    async def save_attempt(self, attempt: DeliveryAttempt) -> None:
        from datetime import datetime as _dt

        def _as_datetime(value: str | None) -> _dt | None:
            return _dt.fromisoformat(value) if value else None

        async with self._database.tenant_transaction(UUID(attempt.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.reply_delivery_attempt
                    (tenant_id,attempt_id,delivery_id,attempt_no,outcome,error_code,
                     started_at,finished_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (tenant_id,attempt_id) DO NOTHING""",
                UUID(attempt.tenant_id),
                UUID(attempt.attempt_id),
                UUID(attempt.delivery_id),
                attempt.attempt_no,
                attempt.outcome,
                attempt.error_code,
                _as_datetime(attempt.started_at),
                _as_datetime(attempt.finished_at),
            )

    async def claim_attempt(
        self, tenant_id: str, delivery_id: str, from_status: DeliveryState, attempt_no: int
    ) -> ReplyDelivery | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                """UPDATE tenant.reply_delivery
                SET status='IN_FLIGHT', attempts=$3, updated_at=now()
                WHERE tenant_id=$1 AND delivery_id=$2 AND status=$4
                RETURNING *""",
                UUID(tenant_id),
                UUID(delivery_id),
                attempt_no,
                str(from_status),
            )
        return _delivery_from_row(row) if row is not None else None

    async def list_dead_letters(self, tenant_id: str) -> list[ReplyDelivery]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                """SELECT * FROM tenant.reply_delivery
                WHERE tenant_id=$1 AND status='DEAD_LETTER' ORDER BY created_at""",
                UUID(tenant_id),
            )
        return [_delivery_from_row(row) for row in rows]
