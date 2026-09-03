"""Database boundary for platform administration and tenant-scoped work."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg


class Database:
    def __init__(self, url: str) -> None:
        self._url = url
        self._pool: asyncpg.Pool[asyncpg.Record] | None = None

    async def open(self) -> None:
        self._pool = await asyncpg.create_pool(self._url, min_size=1, max_size=10)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    @property
    def pool(self) -> asyncpg.Pool[asyncpg.Record]:
        if self._pool is None:
            raise RuntimeError("database pool is not open")
        return self._pool

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection[asyncpg.Record]]:
        async with self.pool.acquire() as connection, connection.transaction():
            yield connection

    @asynccontextmanager
    async def tenant_transaction(
        self, tenant_id: UUID
    ) -> AsyncIterator[asyncpg.Connection[asyncpg.Record]]:
        async with self.transaction() as connection:
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", str(tenant_id))
            yield connection


def record_to_dict(record: asyncpg.Record) -> dict[str, Any]:
    value = dict(record)
    if isinstance(value.get("details"), str):
        value["details"] = json.loads(value["details"])
    return value
