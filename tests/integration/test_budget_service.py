from __future__ import annotations

import asyncio
import contextlib
import os
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg
import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from trpc_service.admin_api.app import create_app
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.settings import AdminSettings
from trpc_service.budgets import (
    AGENT_DAILY,
    EXECUTION,
    TENANT_MONTHLY,
    BudgetCommand,
    BudgetExceeded,
    BudgetService,
    BudgetStateUnknown,
)
from trpc_service.database_migrations import apply_migrations
from trpc_service.llm_gateway import (
    DatabaseBudgetGuard,
    DataClassification,
    GatewayRequest,
    InMemoryModelProfileResolver,
    LLMGateway,
    ModelProfile,
)

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)
PASSWORD_HASH = (
    "$argon2id$v=19$m=65536,t=3,p=4$MRV7DB8RCvU73jcYXzxkUA$"
    "z7yjdKaXuCwuYoWzAqb25/+4f8tW5j3cxFm/pComAo4"
)

pytestmark = pytest.mark.integration

FIXED_NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


class FlakyCommitDatabase:
    """Delegates to the real database but fails the first N transaction commits."""

    def __init__(self, database: Database, failures: int = 1) -> None:
        self._database = database
        self.remaining_failures = failures

    @contextlib.asynccontextmanager
    async def tenant_transaction(self, tenant_id):
        async with self._database.tenant_transaction(tenant_id) as connection:
            # The body runs and commits for real; the caller cannot know.
            yield connection
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise SQLAlchemyError("simulated uncertain commit")


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "TRUNCATE tenant.cost_ledger, tenant.budget_alert, tenant.budget_period_state, "
            "tenant.budget, tenant.model_price, tenant.session_event, tenant.session_lease, "
            "tenant.agent_execution, tenant.agent_session, platform.outbox_record, "
            "tenant.model_profile, tenant.agent_release, tenant.agent_draft, "
            "tenant.agent_application, platform.idempotency_record, platform.audit_event, "
            "platform.platform_role_assignment, platform.platform_user, "
            "platform.tenant_group_member, platform.tenant_group, "
            "tenant.member_role, tenant.member, platform.tenant CASCADE"
        )
    finally:
        await connection.close()


async def _seed_tenant_with_application() -> tuple[str, str]:
    tenant_id, application_id = uuid4(), uuid4()
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            "Budget Tenant",
        )
        await connection.execute(
            "INSERT INTO tenant.agent_application (tenant_id,id,slug,name) VALUES ($1,$2,$3,$4)",
            tenant_id,
            application_id,
            f"app-{application_id.hex[:8]}",
            "Budget App",
        )
    finally:
        await connection.close()
    return str(tenant_id), str(application_id)


async def _create_budget(
    tenant_id: str,
    *,
    scope: str,
    application_id: str | None,
    limit_micros: int,
    contingency_micros: int = 0,
    unknown_policy: str = "FAIL_CLOSED",
) -> str:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        budget_id = uuid4()
        await connection.execute(
            """INSERT INTO tenant.budget
            (tenant_id,id,application_id,scope,limit_micros,contingency_micros,unknown_policy)
            VALUES ($1,$2,$3,$4,$5,$6,$7)""",
            tenant_id,
            budget_id,
            application_id,
            scope,
            limit_micros,
            contingency_micros,
            unknown_policy,
        )
    finally:
        await connection.close()
    return str(budget_id)


async def _state_row(tenant_id: str, budget_id: str) -> dict[str, int]:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        row = await connection.fetchrow(
            """SELECT reserved_micros,consumed_micros,contingency_reserved_micros
            FROM tenant.budget_period_state WHERE tenant_id=$1 AND budget_id=$2""",
            tenant_id,
            budget_id,
        )
        assert row is not None
        return {
            "reserved": int(row["reserved_micros"]),
            "consumed": int(row["consumed_micros"]),
            "contingency_reserved": int(row["contingency_reserved_micros"]),
        }
    finally:
        await connection.close()


async def _ledger_entries(tenant_id: str, entry_type: str | None = None) -> list[dict]:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        return [
            dict(row)
            for row in await connection.fetch(
                """SELECT id,budget_id,period_key,entry_type,amount_micros,reserve_id,
                contingency,reason,actor,price_version
                FROM tenant.cost_ledger WHERE tenant_id=$1
                AND ($2::text IS NULL OR entry_type=$2) ORDER BY created_at,id""",
                tenant_id,
                entry_type,
            )
        ]
    finally:
        await connection.close()


async def _alerts(tenant_id: str) -> list[dict]:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        return [
            dict(row)
            for row in await connection.fetch(
                "SELECT budget_id,level FROM tenant.budget_alert WHERE tenant_id=$1 ORDER BY level",
                tenant_id,
            )
        ]
    finally:
        await connection.close()


def _service(database: Database) -> BudgetService:
    return BudgetService(database, clock=lambda: FIXED_NOW)


def test_reserve_settle_round_trip_writes_append_only_ledger() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            budget_id = await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=1_000_000
            )
            service = _service(database)
            bundle = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-1", 300_000)
            )
            assert len(bundle.entries) == 1
            assert bundle.entries[0].scope == TENANT_MONTHLY
            assert bundle.entries[0].period_key == "2026-09"
            state = await _state_row(tenant_id, budget_id)
            assert state["reserved"] == 300_000
            await service.settle(bundle, actual_micros=250_000)
            state = await _state_row(tenant_id, budget_id)
            assert state == {"reserved": 0, "consumed": 250_000, "contingency_reserved": 0}
            entries = await _ledger_entries(tenant_id)
            assert [entry["entry_type"] for entry in entries] == ["RESERVE", "SETTLE"]
            assert entries[1]["amount_micros"] == 250_000
            # A replayed settle is a no-op: the ledger row already exists.
            await service.settle(bundle, actual_micros=250_000)
            state = await _state_row(tenant_id, budget_id)
            assert state["consumed"] == 250_000
            assert len(await _ledger_entries(tenant_id)) == 2
        finally:
            await database.close()

    asyncio.run(scenario())


def test_alert_thresholds_and_hard_rejection_are_verifiable() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            budget_id = await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=1_000_000
            )
            service = _service(database)
            first = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-1", 700_000)
            )
            assert first.alerts == (f"{TENANT_MONTHLY}:WARNING_70",)
            second = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-2", 200_000)
            )
            assert second.alerts == (f"{TENANT_MONTHLY}:CRITICAL_90",)
            with pytest.raises(BudgetExceeded):
                await service.reserve(BudgetCommand(tenant_id, application_id, "exec-3", 200_000))
            alerts = await _alerts(tenant_id)
            assert sorted(alert["level"] for alert in alerts) == ["CRITICAL_90", "WARNING_70"]
            rejects = await _ledger_entries(tenant_id, "REJECT")
            assert len(rejects) == 1
            assert rejects[0]["amount_micros"] == 200_000
            state = await _state_row(tenant_id, budget_id)
            assert state["reserved"] == 900_000
        finally:
            await database.close()

    asyncio.run(scenario())


def test_concurrent_reservations_never_overdraw() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            budget_id = await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=1_000_000
            )
            service = _service(database)

            async def reserve(index: int) -> bool:
                try:
                    await service.reserve(
                        BudgetCommand(tenant_id, application_id, f"exec-{index}", 300_000)
                    )
                except BudgetExceeded:
                    return False
                return True

            outcomes = await asyncio.gather(*(reserve(index) for index in range(10)))
            assert outcomes.count(True) == 3
            assert outcomes.count(False) == 7
            state = await _state_row(tenant_id, budget_id)
            assert state["reserved"] == 900_000
            rejects = await _ledger_entries(tenant_id, "REJECT")
            assert len(rejects) == 7
        finally:
            await database.close()

    asyncio.run(scenario())


def test_release_returns_a_failed_reservation() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            budget_id = await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=500_000
            )
            service = _service(database)
            bundle = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-1", 400_000)
            )
            await service.release(bundle, reason="upstream failure")
            state = await _state_row(tenant_id, budget_id)
            assert state["reserved"] == 0
            releases = await _ledger_entries(tenant_id, "RELEASE")
            assert len(releases) == 1
            assert releases[0]["reason"] == "upstream failure"
            # Budget is usable again after the release.
            retry = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-2", 400_000)
            )
            assert retry.entries
        finally:
            await database.close()

    asyncio.run(scenario())


def test_admin_adjustment_extends_the_current_period() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            budget_id = await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=500_000
            )
            service = _service(database)
            with pytest.raises(BudgetExceeded):
                await service.reserve(BudgetCommand(tenant_id, application_id, "exec-1", 600_000))
            await service.adjust(
                tenant_id=tenant_id,
                budget_id=budget_id,
                delta_micros=200_000,
                reason="finance-approved overage",
                actor="finance-admin",
            )
            bundle = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-2", 600_000)
            )
            assert bundle.entries
            adjusts = await _ledger_entries(tenant_id, "ADJUST")
            assert len(adjusts) == 1
            assert adjusts[0]["actor"] == "finance-admin"
        finally:
            await database.close()

    asyncio.run(scenario())


def test_unknown_budget_state_fails_closed_by_default() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=500_000
            )
            service = _service(FlakyCommitDatabase(database))
            with pytest.raises(BudgetStateUnknown):
                await service.reserve(BudgetCommand(tenant_id, application_id, "exec-1", 100_000))
        finally:
            await database.close()

    asyncio.run(scenario())


def test_contingency_policy_draws_the_separate_allowance() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            budget_id = await _create_budget(
                tenant_id,
                scope=TENANT_MONTHLY,
                application_id=None,
                limit_micros=500_000,
                contingency_micros=300_000,
                unknown_policy="CONTINGENCY",
            )
            service = _service(FlakyCommitDatabase(database, failures=1))
            bundle = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-1", 400_000)
            )
            assert bundle.entries
            assert bundle.entries[0].contingency is True
            # The draw is capped at the contingency allowance, not the request.
            state = await _state_row(tenant_id, budget_id)
            assert state["contingency_reserved"] == 300_000
            reserves = await _ledger_entries(tenant_id, "RESERVE")
            contingency_draws = [entry for entry in reserves if entry["contingency"]]
            assert len(contingency_draws) == 1
            assert contingency_draws[0]["amount_micros"] == 300_000
            # The contingency allowance is bounded as well.
            exhausted = _service(FlakyCommitDatabase(database, failures=1))
            with pytest.raises(BudgetStateUnknown):
                await exhausted.reserve(BudgetCommand(tenant_id, application_id, "exec-2", 100_000))
        finally:
            await database.close()

    asyncio.run(scenario())


def test_multi_level_reserves_roll_back_on_higher_level_rejection() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            tenant_budget = await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=500_000
            )
            await _create_budget(
                tenant_id, scope=AGENT_DAILY, application_id=application_id, limit_micros=100_000
            )
            service = _service(database)
            with pytest.raises(BudgetExceeded):
                await service.reserve(BudgetCommand(tenant_id, application_id, "exec-1", 400_000))
            state = await _state_row(tenant_id, tenant_budget)
            assert state["reserved"] == 0
            types = sorted(entry["entry_type"] for entry in await _ledger_entries(tenant_id))
            assert types == ["REJECT", "RELEASE", "RESERVE"]
        finally:
            await database.close()

    asyncio.run(scenario())


def test_execution_budget_without_execution_id_fails_closed() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            await _create_budget(
                tenant_id, scope=EXECUTION, application_id=application_id, limit_micros=100_000
            )
            service = _service(database)
            with pytest.raises(BudgetStateUnknown, match="EXECUTION_BUDGET_UNEVALUABLE"):
                await service.reserve(BudgetCommand(tenant_id, application_id, None, 50_000))
        finally:
            await database.close()

    asyncio.run(scenario())


def test_adjust_rejects_execution_scope_budgets() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            budget_id = await _create_budget(
                tenant_id, scope=EXECUTION, application_id=application_id, limit_micros=100_000
            )
            service = _service(database)
            with pytest.raises(ValueError, match="reconfiguring the limit"):
                await service.adjust(
                    tenant_id=tenant_id,
                    budget_id=budget_id,
                    delta_micros=50_000,
                    reason="not applicable",
                    actor="finance-admin",
                )
        finally:
            await database.close()

    asyncio.run(scenario())


def test_execution_scope_budget_applies_per_execution() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            await _create_budget(
                tenant_id, scope=EXECUTION, application_id=application_id, limit_micros=100_000
            )
            service = _service(database)
            bundle = await service.reserve(
                BudgetCommand(tenant_id, application_id, "exec-1", 100_000)
            )
            assert bundle.entries[0].scope == EXECUTION
            with pytest.raises(BudgetExceeded):
                await service.reserve(BudgetCommand(tenant_id, application_id, "exec-1", 1))
        finally:
            await database.close()

    asyncio.run(scenario())


class _AllowAllPolicy:
    async def allows(
        self, request: GatewayRequest, profile: ModelProfile, effective: DataClassification
    ) -> bool:
        return True


class _FixedSecrets:
    async def resolve(self, tenant_id: str, secret_ref: str) -> str:
        return "fake-credential"


class _StubProvider:
    def __init__(self, responder) -> None:
        self._responder = responder

    def transport(self) -> httpx.AsyncBaseTransport:
        return httpx.MockTransport(self._responder)


def _openai_response() -> dict:
    return {
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "model": "gpt-test",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "priced reply"}}],
    }


def test_llm_gateway_guard_reserves_and_settles() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=10_000_000
            )
            connection = await asyncpg.connect(ADMIN_URL)
            try:
                await connection.execute(
                    """INSERT INTO tenant.model_price
                    (tenant_id,version,model_alias,input_micros_per_1k,output_micros_per_1k)
                    VALUES ($1,1,'primary-alias',1000,2000)""",
                    tenant_id,
                )
            finally:
                await connection.close()
            profile = ModelProfile(
                tenant_id=tenant_id,
                alias="primary-alias",
                provider_model="gpt-test",
                endpoint_url="https://provider.test/v1/chat/completions",
                secret_ref=f"vault://tenant/{tenant_id}/llm#primary",
                data_classification=DataClassification.CONFIDENTIAL,
                region="cn-test",
                fallback_aliases=(),
                requests_per_minute=60,
            )
            gateway = LLMGateway(
                InMemoryModelProfileResolver([profile]),
                _FixedSecrets(),
                httpx.AsyncClient(
                    transport=_StubProvider(
                        lambda request, **_: httpx.Response(200, json=_openai_response())
                    ).transport()
                ),
                policy=_AllowAllPolicy(),
                budget=DatabaseBudgetGuard(_service(database)),
            )
            request = GatewayRequest(
                tenant_id=tenant_id,
                model_alias="primary-alias",
                messages=[{"role": "user", "content": "hello"}],
                data_classification=DataClassification.CONFIDENTIAL,
                region="cn-test",
                application_id=application_id,
                execution_id="exec-gateway-1",
            )
            result = await gateway.complete(request)
            assert result.completion["choices"][0]["message"]["content"] == "priced reply"
            budget_id = await _tenant_budget_id(tenant_id)
            state = await _state_row(tenant_id, budget_id)
            # tokens: input ceil(5/4)=2, output 512 → cost = ceil(2*1000/1000) + ceil(512*2000/1000)
            assert state["consumed"] == 2 + 1024
            entries = await _ledger_entries(tenant_id)
            assert {entry["price_version"] for entry in entries} == {1}
        finally:
            await database.close()

    asyncio.run(scenario())


async def _tenant_budget_id(tenant_id: str) -> str:
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        budget_id = await connection.fetchval(
            "SELECT id FROM tenant.budget WHERE tenant_id=$1", tenant_id
        )
        assert budget_id is not None
        return str(budget_id)
    finally:
        await connection.close()


def test_llm_gateway_guard_fails_closed_for_unpriced_models() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, _application_id = await _seed_tenant_with_application()
            await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=10_000_000
            )
            profile = ModelProfile(
                tenant_id=tenant_id,
                alias="primary-alias",
                provider_model="gpt-test",
                endpoint_url="https://provider.test/v1/chat/completions",
                secret_ref=f"vault://tenant/{tenant_id}/llm#primary",
                data_classification=DataClassification.CONFIDENTIAL,
                region="cn-test",
                fallback_aliases=(),
                requests_per_minute=60,
            )
            gateway = LLMGateway(
                InMemoryModelProfileResolver([profile]),
                _FixedSecrets(),
                httpx.AsyncClient(
                    transport=_StubProvider(
                        lambda request: httpx.Response(200, json=_openai_response())
                    ).transport()
                ),
                policy=_AllowAllPolicy(),
                budget=DatabaseBudgetGuard(_service(database)),
            )
            from trpc_service.llm_gateway import ModelGatewayError

            with pytest.raises(ModelGatewayError, match="MODEL_UNPRICED"):
                await gateway.complete(
                    GatewayRequest(
                        tenant_id=tenant_id,
                        model_alias="primary-alias",
                        messages=[{"role": "user", "content": "hello"}],
                        data_classification=DataClassification.CONFIDENTIAL,
                        region="cn-test",
                    )
                )
        finally:
            await database.close()

    asyncio.run(scenario())


def test_llm_gateway_guard_rejects_exhausted_budget() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, _application_id = await _seed_tenant_with_application()
            await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=1
            )
            connection = await asyncpg.connect(ADMIN_URL)
            try:
                await connection.execute(
                    """INSERT INTO tenant.model_price
                    (tenant_id,version,model_alias,input_micros_per_1k,output_micros_per_1k)
                    VALUES ($1,1,'primary-alias',1000,2000)""",
                    tenant_id,
                )
            finally:
                await connection.close()
            profile = ModelProfile(
                tenant_id=tenant_id,
                alias="primary-alias",
                provider_model="gpt-test",
                endpoint_url="https://provider.test/v1/chat/completions",
                secret_ref=f"vault://tenant/{tenant_id}/llm#primary",
                data_classification=DataClassification.CONFIDENTIAL,
                region="cn-test",
                fallback_aliases=(),
                requests_per_minute=60,
            )
            gateway = LLMGateway(
                InMemoryModelProfileResolver([profile]),
                _FixedSecrets(),
                httpx.AsyncClient(
                    transport=_StubProvider(
                        lambda request, **_: httpx.Response(200, json=_openai_response())
                    ).transport()
                ),
                policy=_AllowAllPolicy(),
                budget=DatabaseBudgetGuard(_service(database)),
            )
            from trpc_service.llm_gateway import ModelGatewayError

            with pytest.raises(ModelGatewayError, match="BUDGET_EXCEEDED"):
                await gateway.complete(
                    GatewayRequest(
                        tenant_id=tenant_id,
                        model_alias="primary-alias",
                        messages=[{"role": "user", "content": "hello"}],
                        data_classification=DataClassification.CONFIDENTIAL,
                        region="cn-test",
                    )
                )
            rejects = await _ledger_entries(tenant_id, "REJECT")
            assert len(rejects) == 1
        finally:
            await database.close()

    asyncio.run(scenario())


def _admin_settings() -> AdminSettings:
    return AdminSettings(
        database_url=APP_URL,
        session_signing_key="test-session-key-that-is-long-enough-for-hs256",
        emergency_admin_username="break-glass",
        emergency_admin_password_hash=PASSWORD_HASH,
        session_cookie_secure=False,
        oidc_enabled=False,
    )


def test_budget_admin_api_surfaces_configuration_ledger_and_alerts() -> None:
    asyncio.run(_prepare_database())

    async def seed() -> str:
        tenant_id, _application_id = await _seed_tenant_with_application()
        return tenant_id

    tenant_id = asyncio.run(seed())
    app = create_app(_admin_settings())
    with TestClient(app) as client:
        login = client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        assert login.status_code == 200
        created = client.put(
            f"/api/v1/tenants/{tenant_id}/budgets",
            headers={"Idempotency-Key": str(uuid4())},
            json={"scope": "TENANT_MONTHLY", "limit_micros": 5_000_000},
        )
        assert created.status_code == 200, created.text
        budget = created.json()
        assert budget["scope"] == "TENANT_MONTHLY"
        assert budget["limit_micros"] == 5_000_000

        replay = client.put(
            f"/api/v1/tenants/{tenant_id}/budgets",
            headers={"Idempotency-Key": str(uuid4())},
            json={"scope": "TENANT_MONTHLY", "limit_micros": 5_000_000},
        )
        assert replay.status_code == 200

        prices = client.put(
            f"/api/v1/tenants/{tenant_id}/model-prices",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "prices": [
                    {
                        "model_alias": "primary-alias",
                        "input_micros_per_1k": 1000,
                        "output_micros_per_1k": 2000,
                    }
                ]
            },
        )
        assert prices.status_code == 200, prices.text
        assert prices.json()["version"] == 1

        listing = client.get(f"/api/v1/tenants/{tenant_id}/budgets")
        assert listing.status_code == 200
        assert len(listing.json()["items"]) == 1

        ledger = client.get(f"/api/v1/tenants/{tenant_id}/cost-ledger")
        assert ledger.status_code == 200
        assert ledger.json()["items"] == []

        alerts = client.get(f"/api/v1/tenants/{tenant_id}/budget-alerts")
        assert alerts.status_code == 200
        assert alerts.json()["items"] == []


def test_budget_admin_api_reports_ledger_and_alerts_after_usage() -> None:
    asyncio.run(_prepare_database())

    async def seed() -> str:
        database = Database(APP_URL)
        await database.open()
        try:
            tenant_id, application_id = await _seed_tenant_with_application()
            await _create_budget(
                tenant_id, scope=TENANT_MONTHLY, application_id=None, limit_micros=1_000_000
            )
            service = _service(database)
            await service.reserve(BudgetCommand(tenant_id, application_id, "exec-1", 950_000))
            return tenant_id
        finally:
            await database.close()

    tenant_id = asyncio.run(seed())
    app = create_app(_admin_settings())
    with TestClient(app) as client:
        login = client.post(
            "/api/v1/auth/emergency/session",
            json={"username": "break-glass", "password": "correct-horse"},
        )
        assert login.status_code == 200
        ledger = client.get(
            f"/api/v1/tenants/{tenant_id}/cost-ledger", params={"execution_id": "exec-1"}
        )
        assert ledger.status_code == 200
        entries = ledger.json()["items"]
        assert [entry["entry_type"] for entry in entries] == ["RESERVE"]
        assert entries[0]["amount_micros"] == 950_000
        alerts = client.get(f"/api/v1/tenants/{tenant_id}/budget-alerts")
        assert alerts.status_code == 200
        levels = [alert["level"] for alert in alerts.json()["items"]]
        assert levels == ["CRITICAL_90"]
