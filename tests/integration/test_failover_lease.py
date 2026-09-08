"""Failover lease contract: fencing, dual-primary prevention, standby ingress."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient

from trpc_service.agent_gateway import AgentGatewaySettings, create_app
from trpc_service.database_migrations import apply_migrations
from trpc_service.execution_bus import InMemoryExecutionBus
from trpc_service.failover import FailoverError, FailoverLeaseManager

pytestmark = pytest.mark.integration

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform",
)


async def _prepare_database() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    connection = await asyncpg.connect(ADMIN_URL)
    try:
        await connection.execute("TRUNCATE platform.failover_lease, platform.failover_drill")
    finally:
        await connection.close()


def test_promotion_fences_primary_and_forbids_dual_primary() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        database = await _open_database()
        manager = FailoverLeaseManager(database)
        await manager.register_standby("cn-north", "operator-a")
        assert await manager.current_primary() is None

        first = await manager.promote("cn-east", "operator-a")
        assert first.fencing_token == 1
        current = await manager.current_primary()
        assert current is not None and current.region == "cn-east"

        # Promoting the current primary again is a no-op rejection.
        with pytest.raises(FailoverError, match="FAILOVER_ALREADY_PRIMARY"):
            await manager.promote("cn-east", "operator-a")

        # Failing over fences cn-east and promotes cn-north with a new token.
        second = await manager.promote("cn-north", "operator-b")
        assert second.fencing_token > first.fencing_token
        current = await manager.current_primary()
        assert current is not None and current.region == "cn-north"

        connection = await asyncpg.connect(ADMIN_URL)
        try:
            primaries = await connection.fetchval(
                "SELECT count(*) FROM platform.failover_lease "
                "WHERE role='PRIMARY' AND released_at IS NULL"
            )
            fenced = await connection.fetchval(
                "SELECT count(*) FROM platform.failover_lease "
                "WHERE role='FENCED' AND region='cn-east'"
            )
        finally:
            await connection.close()
        assert primaries == 1
        assert fenced == 1

    asyncio.run(scenario())


def test_standby_region_rejects_ingress_with_stable_error() -> None:
    asyncio.run(_prepare_database())

    async def scenario() -> None:
        bus = InMemoryExecutionBus(partition_count=4)
        app = create_app(
            AgentGatewaySettings(
                database_url=APP_URL,
                dispatch_interval_seconds=0.0,
                standby_mode=True,
            ),
            bus=bus,
        )
        with TestClient(app) as client:
            rejected = client.post(
                "/internal/v1/agent-executions",
                json={
                    "tenant_id": str(uuid4()),
                    "application_id": str(uuid4()),
                    "environment": "PRODUCTION",
                    "session_id": "session-standby",
                    "messages": [{"role": "user", "content": "hello"}],
                    "message_id": "standby-1",
                },
            )
            assert rejected.status_code == 503
            assert rejected.json()["detail"] == "STANDBY_FENCED"

    asyncio.run(scenario())


async def _open_database():
    from trpc_service.admin_api.database import Database

    database = Database(APP_URL)
    await database.open()
    return database
