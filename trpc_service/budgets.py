"""Hard budget enforcement and the immutable cost ledger.

Budgets apply at three levels (tenant monthly, agent application daily, single
execution). Before a billable call the service reserves the estimated cost
atomically: the conditional `UPDATE ... WHERE used + amount <= limit` makes
concurrent reservations unable to overdraw a budget. Settle, release, reject
and adjust append immutable cost ledger entries; crossing 70%/90% raises a
one-time alert per level and period. When a reservation outcome cannot be
confirmed the configured policy applies: fail closed, or draw from the
budget's separate contingency allowance.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.exc import SQLAlchemyError

from trpc_service.admin_api.database import Connection, Database
from trpc_service.ids import uuid7

CHARS_PER_TOKEN = 4
DEFAULT_OUTPUT_TOKENS = 512
WARNING_70 = "WARNING_70"
CRITICAL_90 = "CRITICAL_90"
TENANT_MONTHLY = "TENANT_MONTHLY"
AGENT_DAILY = "AGENT_DAILY"
EXECUTION = "EXECUTION"
FAIL_CLOSED = "FAIL_CLOSED"
CONTINGENCY = "CONTINGENCY"


class BudgetExceeded(RuntimeError):
    """A level's budget cannot cover the estimated cost; new reserves are rejected."""

    def __init__(self, code: str = "BUDGET_EXCEEDED") -> None:
        super().__init__(code)
        self.code = code


class BudgetStateUnknown(RuntimeError):
    """The reservation outcome is unknowable; the caller must fail closed."""

    def __init__(self, code: str = "BUDGET_STATE_UNKNOWN") -> None:
        super().__init__(code)
        self.code = code


def monthly_period_key(moment: datetime) -> str:
    return f"{moment.astimezone(UTC):%Y-%m}"


def daily_period_key(moment: datetime) -> str:
    return f"{moment.astimezone(UTC):%Y-%m-%d}"


def period_key_for(scope: str, moment: datetime, execution_id: str | None) -> str:
    match scope:
        case "TENANT_MONTHLY":
            return monthly_period_key(moment)
        case "AGENT_DAILY":
            return daily_period_key(moment)
        case "EXECUTION":
            if not execution_id:
                raise ValueError("execution budgets require an execution id")
            return execution_id
        case _:
            raise ValueError(f"unknown budget scope: {scope}")


def estimate_tokens(
    messages: Sequence[dict[str, Any]],
    *,
    max_output_tokens: int = DEFAULT_OUTPUT_TOKENS,
) -> tuple[int, int]:
    """Deterministic char-based token estimation for a completion request."""

    input_chars = sum(len(str(message.get("content", ""))) for message in messages)
    input_tokens = math.ceil(input_chars / CHARS_PER_TOKEN)
    return input_tokens, max_output_tokens


def estimate_cost_micros(
    input_tokens: int,
    output_tokens: int,
    *,
    input_micros_per_1k: int,
    output_micros_per_1k: int,
) -> int:
    """Price a completion in micros; unknown prices default to zero."""

    input_cost = math.ceil(input_tokens * input_micros_per_1k / 1000)
    output_cost = math.ceil(output_tokens * output_micros_per_1k / 1000)
    return input_cost + output_cost


def evaluate_level(used_after_micros: int, limit_micros: int) -> str | None:
    """Pure admission decision for one level.

    Returns None or the crossed alert level (WARNING_70 / CRITICAL_90) for an
    admissible reserve, and "REJECT" when the reserve would exceed the limit.
    Ratios refer to the post-reserve usage; landing exactly on the limit is
    still admissible — only usage beyond it is rejected (100% hard limit).
    """

    if used_after_micros > limit_micros:
        return "REJECT"
    if limit_micros <= 0:
        return None
    ratio = used_after_micros / limit_micros
    if ratio >= 0.9:
        return CRITICAL_90
    if ratio >= 0.7:
        return WARNING_70
    return None


@dataclass(frozen=True)
class BudgetCommand:
    tenant_id: str
    application_id: str | None
    execution_id: str | None
    estimated_micros: int


@dataclass(frozen=True)
class BudgetReservation:
    budget_id: str
    scope: str
    period_key: str
    amount_micros: int
    ledger_entry_id: str
    contingency: bool = False


@dataclass(frozen=True)
class BudgetReservationBundle:
    command: BudgetCommand
    entries: tuple[BudgetReservation, ...] = ()
    alerts: tuple[str, ...] = ()


@dataclass(frozen=True)
class _Level:
    budget_id: str
    scope: str
    period_key: str
    limit_micros: int
    contingency_micros: int
    unknown_policy: str


class BudgetService:
    """Transactional budget reservation, settlement and ledger bookkeeping."""

    def __init__(
        self,
        database: Database,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._clock = clock or (lambda: datetime.now(UTC))

    async def reserve(self, command: BudgetCommand) -> BudgetReservationBundle:
        """Reserve the estimated cost on every applicable level, or fail closed.

        A rejected attempt commits its own evidence — the compensating RELEASE
        entries for already-secured levels and the REJECT entry — and then
        raises outside the transaction.
        """

        tenant_id = UUID(command.tenant_id)
        rejected = False
        try:
            async with self._database.tenant_transaction(tenant_id) as connection:
                levels = await self._applicable_levels(connection, command)
                secured: list[BudgetReservation] = []
                alerts: list[str] = []
                for level in levels:
                    reservation, level_alerts = await self._reserve_level(
                        connection, level, command
                    )
                    if reservation is None:
                        rejected = True
                        break
                    secured.append(reservation)
                    alerts.extend(level_alerts)
                if rejected:
                    for entry in secured:
                        await self._compensate_level(
                            connection, command, entry, "rejected downstream"
                        )
                    await self._append_ledger(
                        connection,
                        command,
                        levels[len(secured)].budget_id,
                        levels[len(secured)].period_key,
                        "REJECT",
                        command.estimated_micros,
                        reserve_id=uuid4(),
                        reason="limit exhausted",
                    )
                    bundle = BudgetReservationBundle(command=command)
                else:
                    bundle = BudgetReservationBundle(
                        command=command,
                        entries=tuple(secured),
                        alerts=tuple(alerts),
                    )
        except SQLAlchemyError as error:
            outcome = await self._apply_unknown_policy(tenant_id, command, error)
            if isinstance(outcome, BudgetReservationBundle):
                return outcome
            raise outcome from error
        if rejected:
            # Raise only after the rejection evidence has committed.
            raise BudgetExceeded()
        return bundle

    async def settle(
        self, bundle: BudgetReservationBundle, *, actual_micros: int | None = None
    ) -> None:
        """Charge the actual cost; the estimate leaves the reserved bucket."""

        actual = actual_micros
        async with self._database.tenant_transaction(UUID(bundle.command.tenant_id)) as connection:
            for entry in bundle.entries:
                charged = actual if actual is not None else entry.amount_micros
                await connection.execute(
                    """UPDATE tenant.budget_period_state
                    SET reserved_micros=reserved_micros-$4,
                        consumed_micros=consumed_micros+$5,
                        version=version+1
                    WHERE tenant_id=$1 AND budget_id=$2 AND period_key=$3""",
                    UUID(bundle.command.tenant_id),
                    UUID(entry.budget_id),
                    entry.period_key,
                    entry.amount_micros,
                    charged,
                )
                await self._append_ledger(
                    connection,
                    bundle.command,
                    entry.budget_id,
                    entry.period_key,
                    "SETTLE",
                    charged,
                    reserve_id=UUID(entry.ledger_entry_id),
                    contingency=entry.contingency,
                )

    async def release(self, bundle: BudgetReservationBundle, *, reason: str) -> None:
        """Return a failed execution's reservation; nothing is consumed."""

        async with self._database.tenant_transaction(UUID(bundle.command.tenant_id)) as connection:
            for entry in bundle.entries:
                await connection.execute(
                    """UPDATE tenant.budget_period_state
                    SET reserved_micros=reserved_micros-$4,version=version+1
                    WHERE tenant_id=$1 AND budget_id=$2 AND period_key=$3""",
                    UUID(bundle.command.tenant_id),
                    UUID(entry.budget_id),
                    entry.period_key,
                    entry.amount_micros,
                )
                await self._append_ledger(
                    connection,
                    bundle.command,
                    entry.budget_id,
                    entry.period_key,
                    "RELEASE",
                    entry.amount_micros,
                    reserve_id=UUID(entry.ledger_entry_id),
                    contingency=entry.contingency,
                    reason=reason,
                )

    async def adjust(
        self,
        *,
        tenant_id: str,
        budget_id: str,
        delta_micros: int,
        reason: str,
        actor: str,
    ) -> None:
        """Apply an administrator allowance adjustment to the current period."""

        moment = self._clock()
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            budget = await connection.fetchrow(
                """SELECT id,scope,application_id FROM tenant.budget
                WHERE tenant_id=$1 AND id=$2""",
                UUID(tenant_id),
                UUID(budget_id),
            )
            if budget is None:
                raise LookupError("budget not found")
            period_key = period_key_for(
                str(budget["scope"]),
                moment,
                None if budget["application_id"] is None else str(budget["application_id"]),
            )
            await self._ensure_state_row(connection, tenant_id, budget_id, period_key)
            await connection.execute(
                """UPDATE tenant.budget_period_state
                SET allowance_micros=allowance_micros+$4,version=version+1
                WHERE tenant_id=$1 AND budget_id=$2 AND period_key=$3""",
                UUID(tenant_id),
                UUID(budget_id),
                period_key,
                delta_micros,
            )
            await self._append_ledger(
                connection,
                BudgetCommand(
                    tenant_id=tenant_id,
                    application_id=(
                        str(budget["application_id"])
                        if budget["application_id"] is not None
                        else None
                    ),
                    execution_id=None,
                    estimated_micros=0,
                ),
                budget_id,
                period_key,
                "ADJUST",
                max(delta_micros, 0),
                reserve_id=uuid4(),
                reason=f"{reason} (delta {delta_micros})",
                actor=actor,
            )

    async def _applicable_levels(
        self, connection: Connection, command: BudgetCommand
    ) -> list[_Level]:
        moment = self._clock()
        rows = await connection.fetch(
            """SELECT id,scope,application_id,limit_micros,contingency_micros,unknown_policy
            FROM tenant.budget WHERE tenant_id=$1 ORDER BY created_at,id""",
            UUID(command.tenant_id),
        )
        levels: list[_Level] = []
        for row in rows:
            scope = str(row["scope"])
            application_id = (
                str(row["application_id"]) if row["application_id"] is not None else None
            )
            if scope == AGENT_DAILY and application_id != command.application_id:
                continue
            if scope == EXECUTION and (
                command.execution_id is None
                or (application_id is not None and application_id != command.application_id)
            ):
                continue
            levels.append(
                _Level(
                    budget_id=str(row["id"]),
                    scope=scope,
                    period_key=period_key_for(scope, moment, command.execution_id),
                    limit_micros=int(row["limit_micros"]),
                    contingency_micros=int(row["contingency_micros"]),
                    unknown_policy=str(row["unknown_policy"]),
                )
            )
        return levels

    async def _reserve_level(
        self, connection: Connection, level: _Level, command: BudgetCommand
    ) -> tuple[BudgetReservation | None, list[str]]:
        await self._ensure_state_row(
            connection, command.tenant_id, level.budget_id, level.period_key
        )
        state = await connection.fetchrow(
            """SELECT reserved_micros,consumed_micros,allowance_micros
            FROM tenant.budget_period_state
            WHERE tenant_id=$1 AND budget_id=$2 AND period_key=$3 FOR UPDATE""",
            UUID(command.tenant_id),
            UUID(level.budget_id),
            level.period_key,
        )
        assert state is not None
        effective_limit = level.limit_micros + int(state["allowance_micros"])
        used_after = (
            int(state["reserved_micros"]) + int(state["consumed_micros"]) + command.estimated_micros
        )
        decision = evaluate_level(used_after, effective_limit)
        if decision == "REJECT":
            return None, []
        updated = await connection.fetchval(
            """UPDATE tenant.budget_period_state
            SET reserved_micros=reserved_micros+$4,version=version+1
            WHERE tenant_id=$1 AND budget_id=$2 AND period_key=$3
              AND reserved_micros+consumed_micros+$4 <= $5
            RETURNING version""",
            UUID(command.tenant_id),
            UUID(level.budget_id),
            level.period_key,
            command.estimated_micros,
            effective_limit,
        )
        if updated is None:
            return None, []
        entry_id = str(uuid7())
        await self._append_ledger(
            connection,
            command,
            level.budget_id,
            level.period_key,
            "RESERVE",
            command.estimated_micros,
            reserve_id=UUID(entry_id),
        )
        alerts: list[str] = []
        if decision is not None:
            inserted = await connection.fetchval(
                """INSERT INTO tenant.budget_alert
                (tenant_id,budget_id,period_key,level,used_micros,limit_micros)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (tenant_id,budget_id,period_key,level) DO NOTHING
                RETURNING level""",
                UUID(command.tenant_id),
                UUID(level.budget_id),
                level.period_key,
                decision,
                used_after,
                level.limit_micros,
            )
            if inserted is not None:
                alerts.append(f"{level.scope}:{decision}")
        return (
            BudgetReservation(
                budget_id=level.budget_id,
                scope=level.scope,
                period_key=level.period_key,
                amount_micros=command.estimated_micros,
                ledger_entry_id=entry_id,
            ),
            alerts,
        )

    async def _compensate_level(
        self, connection: Connection, command: BudgetCommand, entry: BudgetReservation, reason: str
    ) -> None:
        """Undo one secured level inside the same transaction; ledger RELEASE."""

        await connection.execute(
            """UPDATE tenant.budget_period_state
            SET reserved_micros=reserved_micros-$4,version=version+1
            WHERE tenant_id=$1 AND budget_id=$2 AND period_key=$3""",
            UUID(command.tenant_id),
            UUID(entry.budget_id),
            entry.period_key,
            entry.amount_micros,
        )
        await self._append_ledger(
            connection,
            command,
            entry.budget_id,
            entry.period_key,
            "RELEASE",
            entry.amount_micros,
            reserve_id=UUID(entry.ledger_entry_id),
            contingency=entry.contingency,
            reason=reason,
        )

    async def _apply_unknown_policy(
        self, tenant_id: UUID, command: BudgetCommand, error: SQLAlchemyError
    ) -> Exception | BudgetReservationBundle:
        """Resolve an unknowable reservation outcome: fail closed or contingency."""

        try:
            async with self._database.tenant_transaction(tenant_id) as connection:
                levels = await self._applicable_levels(connection, command)
                contingent = [
                    level
                    for level in levels
                    if level.unknown_policy == CONTINGENCY and level.contingency_micros > 0
                ]
                if not contingent:
                    return BudgetStateUnknown()
                entries: list[BudgetReservation] = []
                for level in contingent:
                    amount = min(command.estimated_micros, level.contingency_micros)
                    reserved = await connection.fetchval(
                        """UPDATE tenant.budget_period_state
                        SET contingency_reserved_micros=contingency_reserved_micros+$4,
                            version=version+1
                        WHERE tenant_id=$1 AND budget_id=$2 AND period_key=$3
                          AND contingency_reserved_micros+$4 <= $5
                        RETURNING version""",
                        tenant_id,
                        UUID(level.budget_id),
                        level.period_key,
                        amount,
                        level.contingency_micros,
                    )
                    if reserved is None:
                        return BudgetStateUnknown()
                    entry_id = str(uuid7())
                    await self._append_ledger(
                        connection,
                        command,
                        level.budget_id,
                        level.period_key,
                        "RESERVE",
                        amount,
                        reserve_id=UUID(entry_id),
                        contingency=True,
                        reason="contingency allowance after unknown budget state",
                    )
                    entries.append(
                        BudgetReservation(
                            budget_id=level.budget_id,
                            scope=level.scope,
                            period_key=level.period_key,
                            amount_micros=amount,
                            ledger_entry_id=entry_id,
                            contingency=True,
                        )
                    )
                return BudgetReservationBundle(command=command, entries=tuple(entries))
        except SQLAlchemyError:
            return BudgetStateUnknown()

    async def _ensure_state_row(
        self, connection: Connection, tenant_id: str, budget_id: str, period_key: str
    ) -> None:
        await connection.execute(
            """INSERT INTO tenant.budget_period_state
            (tenant_id,budget_id,period_key) VALUES ($1,$2,$3)
            ON CONFLICT (tenant_id,budget_id,period_key) DO NOTHING""",
            UUID(tenant_id),
            UUID(budget_id),
            period_key,
        )

    async def _append_ledger(
        self,
        connection: Connection,
        command: BudgetCommand,
        budget_id: str,
        period_key: str,
        entry_type: str,
        amount_micros: int,
        *,
        reserve_id: UUID,
        contingency: bool = False,
        reason: str = "",
        actor: str = "system",
        price_version: int | None = None,
    ) -> None:
        await connection.execute(
            """INSERT INTO tenant.cost_ledger
            (tenant_id,id,budget_id,period_key,application_id,execution_id,entry_type,
            amount_micros,reserve_id,contingency,reason,actor,price_version)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (tenant_id,budget_id,period_key,reserve_id,entry_type) DO NOTHING""",
            UUID(command.tenant_id),
            uuid7(),
            UUID(budget_id),
            period_key,
            UUID(command.application_id) if command.application_id else None,
            command.execution_id,
            entry_type,
            amount_micros,
            reserve_id,
            contingency,
            reason,
            actor,
            price_version,
        )
