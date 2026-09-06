"""Reply delivery state machine: tracked, at-least-once, reconcilable.

One delivery has a stable delivery id and carries numbered attempts, each
with its own attempt id. The transport outcome drives the state machine:
delivered closes it, rate limits back off and retry, and an unknown outcome
reconciles against the channel ledger first — a confirmed delivery is never
resent, a confirmed miss retries, an unresolvable outcome parks in
OUTCOME_UNKNOWN. Exhausted deliveries dead-letter; replaying a dead letter
keeps the original delivery id and continues the attempt numbering.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

MAX_BACKOFF_SECONDS = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 1.0


class DeliveryState(StrEnum):
    QUEUED = "QUEUED"
    IN_FLIGHT = "IN_FLIGHT"
    DELIVERED = "DELIVERED"
    RATE_LIMITED = "RATE_LIMITED"
    FAILED = "FAILED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    DEAD_LETTER = "DEAD_LETTER"


TERMINAL_STATES = frozenset({DeliveryState.DELIVERED, DeliveryState.DEAD_LETTER})


class ReplyDeliveryError(RuntimeError):
    """Stable delivery failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ReplyDelivery(BaseModel):
    """One tracked reply delivery with a stable id and attempt counter."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    delivery_id: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    execution_id: str = Field(min_length=1)
    external_conversation_id: str = Field(min_length=1, max_length=256)
    content: str = Field(min_length=1)
    status: DeliveryState = DeliveryState.QUEUED
    attempts: int = 0
    created_at: str = Field(min_length=1)


class DeliveryAttempt(BaseModel):
    """One send attempt; ids are per-attempt and never reused."""

    model_config = ConfigDict(frozen=True)

    attempt_id: str
    tenant_id: str
    delivery_id: str
    attempt_no: int = Field(ge=1)
    outcome: str
    error_code: str | None = None
    started_at: str
    finished_at: str | None = None


@dataclass(frozen=True)
class ChannelTransportOutcome:
    delivered: bool
    rate_limited: bool = False
    outcome_unknown: bool = False
    error_code: str | None = None


ReconcileVerdict = str  # "delivered" | "not_delivered" | "unknown"


class ReplyTransport(Protocol):
    async def send(self, delivery: ReplyDelivery, attempt_no: int) -> ChannelTransportOutcome: ...

    def reconcile(self, delivery: ReplyDelivery, attempt_no: int) -> ReconcileVerdict: ...


class MemoryDeliveryStore:
    def __init__(self) -> None:
        self.deliveries: dict[str, ReplyDelivery] = {}
        self.attempts: list[DeliveryAttempt] = []

    async def save_delivery(self, delivery: ReplyDelivery) -> None:
        self.deliveries[delivery.delivery_id] = delivery

    async def get_delivery(self, tenant_id: str, delivery_id: str) -> ReplyDelivery | None:
        delivery = self.deliveries.get(delivery_id)
        if delivery is None or delivery.tenant_id != tenant_id:
            return None
        return delivery

    async def save_attempt(self, attempt: DeliveryAttempt) -> None:
        self.attempts.append(attempt)

    async def list_dead_letters(self, tenant_id: str) -> list[ReplyDelivery]:
        return [
            delivery
            for delivery in self.deliveries.values()
            if delivery.tenant_id == tenant_id and delivery.status is DeliveryState.DEAD_LETTER
        ]

    async def claim_attempt(
        self, tenant_id: str, delivery_id: str, from_status: DeliveryState, attempt_no: int
    ) -> ReplyDelivery | None:
        delivery = self.deliveries.get(delivery_id)
        if delivery is None or delivery.tenant_id != tenant_id:
            return None
        if delivery.status is not from_status:
            return None
        claimed = delivery.model_copy(
            update={"status": DeliveryState.IN_FLIGHT, "attempts": attempt_no}
        )
        self.deliveries[delivery_id] = claimed
        return claimed


class FakeChannelTransport:
    """Scriptable channel backend for success/duplicate/rate-limit/unknown drills."""

    def __init__(self, outcomes: list[ChannelTransportOutcome]) -> None:
        self.outcomes = list(outcomes)
        self.reconcile_results: dict[int, ReconcileVerdict] = {}
        self.sent_attempt_nos: list[int] = []

    async def send(self, delivery: ReplyDelivery, attempt_no: int) -> ChannelTransportOutcome:
        self.sent_attempt_nos.append(attempt_no)
        if not self.outcomes:
            return ChannelTransportOutcome(delivered=True)
        return self.outcomes.pop(0)

    def reconcile(self, delivery: ReplyDelivery, attempt_no: int) -> ReconcileVerdict:
        return self.reconcile_results.get(attempt_no, "unknown")


class DeliveryStore(Protocol):
    async def save_delivery(self, delivery: ReplyDelivery) -> None: ...
    async def get_delivery(self, tenant_id: str, delivery_id: str) -> ReplyDelivery | None: ...
    async def save_attempt(self, attempt: DeliveryAttempt) -> None: ...
    async def list_dead_letters(self, tenant_id: str) -> list[ReplyDelivery]: ...
    async def claim_attempt(
        self, tenant_id: str, delivery_id: str, from_status: DeliveryState, attempt_no: int
    ) -> ReplyDelivery | None:
        """Compare-and-swap into IN_FLIGHT; None when another worker holds it."""
        ...


def backoff_delay(attempt_no: int, base_seconds: float) -> float:
    """Exponential backoff capped at MAX_BACKOFF_SECONDS."""

    delay = base_seconds * float(2 ** max(0, attempt_no - 1))
    return min(delay, MAX_BACKOFF_SECONDS)


class ReplyDeliveryService:
    """Drives reply deliveries to a terminal or reconcilable state."""

    def __init__(
        self,
        *,
        store: DeliveryStore,
        transport: ReplyTransport,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self._store = store
        self._transport = transport
        self._backoff_seconds = backoff_seconds
        self._max_attempts = max_attempts

    async def enqueue(
        self,
        *,
        tenant_id: str,
        binding_id: str,
        execution_id: str,
        external_conversation_id: str,
        content: str,
    ) -> ReplyDelivery:
        delivery = ReplyDelivery(
            tenant_id=tenant_id,
            delivery_id=str(uuid4()),
            binding_id=binding_id,
            execution_id=execution_id,
            external_conversation_id=external_conversation_id,
            content=content,
            created_at=datetime.now(UTC).isoformat(),
        )
        await self._store.save_delivery(delivery)
        return delivery

    async def run(
        self, delivery_id: str, *, tenant_id: str, max_attempts: int | None = None
    ) -> ReplyDelivery:
        """Send until delivered, parked-unknown or dead-lettered.

        Re-entering an OUTCOME_UNKNOWN delivery reconciles the parked attempt
        before any resend; re-entering an interrupted IN_FLIGHT delivery
        continues from the next attempt number.
        """

        budget = self._max_attempts if max_attempts is None else max_attempts
        delivery = await self._load(tenant_id, delivery_id)
        if delivery.status in TERMINAL_STATES:
            return delivery
        if delivery.status is DeliveryState.OUTCOME_UNKNOWN:
            delivery = await self._resume_reconciliation(delivery)
            if delivery.status in TERMINAL_STATES | {DeliveryState.OUTCOME_UNKNOWN}:
                return delivery
        while True:
            if delivery.attempts >= budget and delivery.status is not DeliveryState.RATE_LIMITED:
                # Rate limits back off without consuming the failure budget;
                # only exhausted failures dead-letter.
                delivery = await self._transition(delivery, DeliveryState.DEAD_LETTER)
                return delivery
            if delivery.attempts > 0:
                await asyncio.sleep(backoff_delay(delivery.attempts, self._backoff_seconds))
            attempt_no = delivery.attempts + 1
            claimed = await self._store.claim_attempt(
                tenant_id, delivery_id, delivery.status, attempt_no
            )
            if claimed is None:
                # Another worker owns this delivery right now.
                return delivery
            delivery = claimed
            started = datetime.now(UTC).isoformat()
            try:
                outcome = await self._transport.send(delivery, attempt_no)
            except Exception:
                # A crashed send leaves the outcome genuinely unknown.
                await self._record_attempt(
                    delivery, attempt_no, "OUTCOME_UNKNOWN", "CHANNEL_SEND_FAILED", started
                )
                return await self._transition(delivery, DeliveryState.OUTCOME_UNKNOWN)

            if outcome.delivered:
                await self._record_attempt(delivery, attempt_no, "DELIVERED", None, started)
                return await self._transition(
                    delivery, DeliveryState.DELIVERED, attempts=attempt_no
                )

            if outcome.outcome_unknown:
                verdict = self._transport.reconcile(delivery, attempt_no)
                if verdict == "delivered":
                    await self._record_attempt(
                        delivery, attempt_no, "RECONCILED_DELIVERED", outcome.error_code, started
                    )
                    return await self._transition(
                        delivery, DeliveryState.DELIVERED, attempts=attempt_no
                    )
                if verdict == "unknown":
                    await self._record_attempt(
                        delivery, attempt_no, "OUTCOME_UNKNOWN", outcome.error_code, started
                    )
                    # Park for reconciliation; never blindly resend.
                    return await self._transition(delivery, DeliveryState.OUTCOME_UNKNOWN)
                await self._record_attempt(
                    delivery, attempt_no, "RECONCILED_NOT_DELIVERED", outcome.error_code, started
                )
                delivery = await self._transition(
                    delivery, DeliveryState.FAILED, attempts=attempt_no
                )
                continue

            state = DeliveryState.RATE_LIMITED if outcome.rate_limited else DeliveryState.FAILED
            await self._record_attempt(
                delivery,
                attempt_no,
                str(state),
                outcome.error_code,
                started,
            )
            delivery = await self._transition(delivery, state, attempts=attempt_no)

    async def _resume_reconciliation(self, delivery: ReplyDelivery) -> ReplyDelivery:
        """Reconcile the parked attempt before considering any resend."""

        verdict = self._transport.reconcile(delivery, delivery.attempts)
        if verdict == "delivered":
            return await self._transition(delivery, DeliveryState.DELIVERED)
        if verdict == "unknown":
            # Still unresolvable: stay parked rather than resend.
            return delivery
        return await self._transition(delivery, DeliveryState.FAILED)

    async def replay(self, delivery_id: str, *, tenant_id: str) -> ReplyDelivery:
        """Requeue a dead letter under its original delivery id and run once more."""

        delivery = await self._load(tenant_id, delivery_id)
        if delivery.status != DeliveryState.DEAD_LETTER:
            raise ReplyDeliveryError("DELIVERY_NOT_DEAD_LETTER")
        delivery = await self._transition(delivery, DeliveryState.QUEUED)
        return await self.run(delivery_id, tenant_id=tenant_id, max_attempts=delivery.attempts + 1)

    async def _load(self, tenant_id: str, delivery_id: str) -> ReplyDelivery:
        delivery = await self._store.get_delivery(tenant_id, delivery_id)
        if delivery is None:
            raise ReplyDeliveryError("DELIVERY_NOT_FOUND")
        return delivery

    async def _transition(
        self,
        delivery: ReplyDelivery,
        status: DeliveryState,
        *,
        attempts: int | None = None,
    ) -> ReplyDelivery:
        updated = delivery.model_copy(
            update={
                "status": status,
                "attempts": delivery.attempts if attempts is None else attempts,
            }
        )
        await self._store.save_delivery(updated)
        return updated

    async def _record_attempt(
        self,
        delivery: ReplyDelivery,
        attempt_no: int,
        outcome: str,
        error_code: str | None,
        started_at: str,
    ) -> None:
        await self._store.save_attempt(
            DeliveryAttempt(
                attempt_id=str(uuid4()),
                tenant_id=delivery.tenant_id,
                delivery_id=delivery.delivery_id,
                attempt_no=attempt_no,
                outcome=outcome,
                error_code=error_code,
                started_at=started_at,
                finished_at=datetime.now(UTC).isoformat(),
            )
        )
