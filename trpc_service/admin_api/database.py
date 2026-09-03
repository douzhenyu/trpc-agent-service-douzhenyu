"""Database boundary for platform administration and tenant-scoped work."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


def sqlalchemy_url(url: str) -> str:
    """Normalize operator-friendly PostgreSQL URLs for SQLAlchemy's async driver."""
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


class Connection:
    """Small SQLAlchemy Core boundary used by control-plane repositories."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._connection = connection

    @staticmethod
    def _statement(sql: str, args: tuple[Any, ...]) -> tuple[str, dict[str, Any]]:
        parameters = {f"p{index}": value for index, value in enumerate(args, start=1)}
        statement = re.sub(r"\$(\d+)", lambda match: f":p{match.group(1)}", sql)
        return statement, parameters

    async def execute(self, sql: str, *args: Any) -> int:
        statement, parameters = self._statement(sql, args)
        result = await self._connection.execute(text(statement), parameters)
        return result.rowcount

    async def fetchrow(self, sql: str, *args: Any) -> Any | None:
        statement, parameters = self._statement(sql, args)
        result = await self._connection.execute(text(statement), parameters)
        return result.mappings().first()

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        statement, parameters = self._statement(sql, args)
        result = await self._connection.execute(text(statement), parameters)
        return list(result.mappings().all())

    async def fetchval(self, sql: str, *args: Any) -> Any:
        statement, parameters = self._statement(sql, args)
        result = await self._connection.execute(text(statement), parameters)
        return result.scalar()

    async def executemany(self, sql: str, args: list[tuple[Any, ...]]) -> None:
        if not args:
            return
        statement, _ = self._statement(sql, args[0])
        parameters = [self._statement(sql, item)[1] for item in args]
        await self._connection.execute(text(statement), parameters)


class Database:
    def __init__(self, url: str) -> None:
        self._url = url
        self._engine: AsyncEngine | None = None

    async def open(self) -> None:
        self._engine = create_async_engine(sqlalchemy_url(self._url), pool_size=10)
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("database engine is not open")
        return self._engine

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Connection]:
        async with self.engine.begin() as connection:
            yield Connection(connection)

    @asynccontextmanager
    async def tenant_transaction(self, tenant_id: UUID) -> AsyncIterator[Connection]:
        async with self.transaction() as connection:
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", str(tenant_id))
            yield connection


def record_to_dict(record: Any) -> dict[str, Any]:
    return dict(record)
