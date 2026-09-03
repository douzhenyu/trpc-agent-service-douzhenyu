from __future__ import annotations

import asyncio

import pytest

from trpc_service import database_migrations


def test_migration_statements_keep_the_role_block_intact() -> None:
    statements = database_migrations.migration_statements()
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


def test_apply_migrations_without_app_password_skips_role_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrades: list[str] = []

    def fake_upgrade(_config: object, revision: str) -> None:
        upgrades.append(revision)

    monkeypatch.setattr(database_migrations.command, "upgrade", fake_upgrade)
    asyncio.run(database_migrations.apply_migrations("postgresql://admin:secret@database/platform"))
    assert upgrades == ["head"]
