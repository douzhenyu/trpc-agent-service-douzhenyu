from __future__ import annotations

import asyncio
import importlib
import runpy
from contextlib import asynccontextmanager
from typing import Any

import pytest
from alembic import context

from trpc_service import database_migrations


def test_migration_statements_keep_the_role_block_intact() -> None:
    revision = importlib.import_module("trpc_service.migrations.versions.0001_platform_identity")
    statements = revision.migration_statements()
    assert statements[0].lstrip().startswith("DO $$ BEGIN")
    assert statements[0].endswith("END $$;")
    assert any("CREATE TABLE IF NOT EXISTS platform.tenant" in item for item in statements)
    assert any("CREATE POLICY tenant_isolation" in item for item in statements)


def test_migration_entrypoint_requires_admin_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_ADMIN_URL", raising=False)
    with pytest.raises(SystemExit, match="DATABASE_ADMIN_URL is required"):
        database_migrations.main()


def test_migration_entrypoint_applies_head_and_reports_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    called: list[tuple[str, str | None]] = []

    async def fake_apply(database_url: str, password: str | None) -> None:
        called.append((database_url, password))

    monkeypatch.setenv("DATABASE_ADMIN_URL", "postgresql://admin:secret@database/platform")
    monkeypatch.setenv("DATABASE_APP_PASSWORD", "app-password")
    monkeypatch.setattr(database_migrations, "apply_migrations", fake_apply)

    database_migrations.main()

    assert called == [("postgresql://admin:secret@database/platform", "app-password")]
    assert capsys.readouterr().out.strip() == '{"status": "ok", "applied_revisions": 1}'


class _FakeMigrationConnection:
    def __init__(self, scalar_results: list[int] | None = None) -> None:
        self.statements: list[str] = []
        self.scalar_statements: list[str] = []
        self.scalar_results = scalar_results or []

    async def exec_driver_sql(self, statement: str) -> None:
        self.statements.append(statement)

    async def scalar(self, _statement: object, _parameters: object = None) -> int:
        self.scalar_statements.append(str(_statement))
        return self.scalar_results.pop(0) if self.scalar_results else 0


class _FakeMigrationEngine:
    def __init__(self, scalar_results: list[int] | None = None) -> None:
        self.connection = _FakeMigrationConnection(scalar_results)
        self.disposed = False

    @asynccontextmanager
    async def begin(self) -> Any:
        yield self.connection

    async def dispose(self) -> None:
        self.disposed = True


def test_apply_migrations_accepts_url_encoded_password_and_hardens_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrades: list[tuple[str, str]] = []
    engine = _FakeMigrationEngine()

    def fake_upgrade(config: Any, revision: str) -> None:
        upgrades.append((config.get_main_option("sqlalchemy.url"), revision))

    monkeypatch.setattr(database_migrations.command, "upgrade", fake_upgrade)
    monkeypatch.setattr(database_migrations, "create_async_engine", lambda _url: engine)
    database_url = "postgresql://admin:p%25ss%40word@database/platform"

    asyncio.run(database_migrations.apply_migrations(database_url))

    assert upgrades == [("postgresql+asyncpg://admin:p%25ss%40word@database/platform", "head")]
    assert any(
        "ALTER ROLE trpc_platform_app" in statement for statement in engine.connection.statements
    )
    ownership_query = "\n".join(engine.connection.scalar_statements)
    for catalog in ("pg_namespace", "pg_database", "pg_proc", "pg_type"):
        assert catalog in ownership_query
    assert engine.disposed


@pytest.mark.parametrize(
    ("scalar_results", "message"),
    [
        ([1], "must not inherit or assume"),
        ([0, 1], "must not own"),
    ],
)
def test_apply_migrations_blocks_unsafe_role_relationships(
    monkeypatch: pytest.MonkeyPatch,
    scalar_results: list[int],
    message: str,
) -> None:
    engine = _FakeMigrationEngine(scalar_results)
    upgrades: list[str] = []
    monkeypatch.setattr(
        database_migrations.command,
        "upgrade",
        lambda _config, revision: upgrades.append(revision),
    )
    monkeypatch.setattr(database_migrations, "create_async_engine", lambda _url: engine)

    with pytest.raises(RuntimeError, match=message):
        asyncio.run(
            database_migrations.apply_migrations("postgresql://admin:secret@database/platform")
        )
    assert upgrades == []
    assert "NOLOGIN" in engine.connection.statements[0]
    assert engine.disposed


def test_revision_executes_every_statement_and_is_irreversible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revision = importlib.import_module("trpc_service.migrations.versions.0001_platform_identity")
    executed: list[str] = []
    monkeypatch.setattr(revision, "migration_statements", lambda: ["FIRST", "SECOND"])
    monkeypatch.setattr(revision.op, "execute", executed.append)

    revision.upgrade()

    assert executed == ["FIRST", "SECOND"]
    with pytest.raises(RuntimeError, match="intentionally irreversible"):
        revision.downgrade()


def test_alembic_environment_rejects_offline_migrations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(context, "is_offline_mode", lambda: True)
    with pytest.raises(RuntimeError, match="offline migrations are not supported"):
        runpy.run_module("trpc_service.migrations.env", run_name="__offline_test__")
