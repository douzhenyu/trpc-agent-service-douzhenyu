"""Idempotent control-plane schema migration entry point."""

from __future__ import annotations

import json
import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from trpc_service.admin_api.database import sqlalchemy_url


async def _disable_and_harden_app_role(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            """DO $$ BEGIN
            IF EXISTS (SELECT FROM pg_roles WHERE rolname='trpc_platform_app') THEN
              EXECUTE 'ALTER ROLE trpc_platform_app NOLOGIN NOINHERIT NOSUPERUSER' ||
                ' NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS';
              PERFORM pg_terminate_backend(pid) FROM pg_stat_activity
                WHERE usename='trpc_platform_app' AND pid <> pg_backend_pid();
            END IF;
            END $$;"""
        )


async def _assert_safe_app_role(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        unsafe_memberships = await connection.scalar(
            text(
                """SELECT count(*) FROM pg_auth_members memberships
                JOIN pg_roles member ON member.oid=memberships.member
                WHERE member.rolname='trpc_platform_app'"""
            )
        )
        if unsafe_memberships:
            raise RuntimeError("trpc_platform_app must not inherit or assume another role")
        owned_objects = await connection.scalar(
            text(
                """SELECT count(*) FROM (
                  SELECT namespace.oid FROM pg_namespace namespace
                    JOIN pg_roles owner ON owner.oid=namespace.nspowner
                    WHERE owner.rolname='trpc_platform_app'
                      AND namespace.nspname IN ('platform','tenant')
                  UNION ALL
                  SELECT objects.oid FROM pg_class objects
                    JOIN pg_roles owner ON owner.oid=objects.relowner
                    JOIN pg_namespace namespace ON namespace.oid=objects.relnamespace
                    WHERE owner.rolname='trpc_platform_app'
                      AND namespace.nspname IN ('platform','tenant')
                  UNION ALL
                  SELECT database.oid FROM pg_database database
                    JOIN pg_roles owner ON owner.oid=database.datdba
                    WHERE owner.rolname='trpc_platform_app'
                      AND database.datname=current_database()
                  UNION ALL
                  SELECT procedure.oid FROM pg_proc procedure
                    JOIN pg_roles owner ON owner.oid=procedure.proowner
                    JOIN pg_namespace namespace ON namespace.oid=procedure.pronamespace
                    WHERE owner.rolname='trpc_platform_app'
                      AND namespace.nspname IN ('platform','tenant')
                  UNION ALL
                  SELECT type.oid FROM pg_type type
                    JOIN pg_roles owner ON owner.oid=type.typowner
                    JOIN pg_namespace namespace ON namespace.oid=type.typnamespace
                    WHERE owner.rolname='trpc_platform_app'
                      AND namespace.nspname IN ('platform','tenant')
                  UNION ALL
                  SELECT defaults.oid FROM pg_default_acl defaults
                    JOIN pg_roles owner ON owner.oid=defaults.defaclrole
                    WHERE owner.rolname='trpc_platform_app'
                ) unsafe"""
            )
        )
        if owned_objects:
            raise RuntimeError("trpc_platform_app must not own database or schema objects")


async def apply_migrations(database_url: str, app_role_password: str | None = None) -> None:
    import asyncio

    config = Config()
    config.set_main_option("script_location", str(Path(__file__).with_name("migrations")))
    config.set_main_option("sqlalchemy.url", sqlalchemy_url(database_url).replace("%", "%%"))
    engine = create_async_engine(sqlalchemy_url(database_url))
    try:
        await _disable_and_harden_app_role(engine)
        await _assert_safe_app_role(engine)
        await asyncio.to_thread(command.upgrade, config, "head")
        await _disable_and_harden_app_role(engine)
        await _assert_safe_app_role(engine)
        if app_role_password:
            async with engine.begin() as connection:
                quoted = await connection.scalar(
                    text("SELECT quote_literal(:password)"), {"password": app_role_password}
                )
                await connection.exec_driver_sql(
                    f"ALTER ROLE trpc_platform_app LOGIN PASSWORD {quoted}"
                )
    finally:
        await engine.dispose()


def main() -> None:
    """Apply the schema using a privileged, deployment-only connection."""
    import asyncio

    database_url = os.environ.get("DATABASE_ADMIN_URL")
    if not database_url:
        raise SystemExit("DATABASE_ADMIN_URL is required")
    asyncio.run(apply_migrations(database_url, os.environ.get("DATABASE_APP_PASSWORD")))
    print(json.dumps({"status": "ok", "applied_revisions": 1}))


if __name__ == "__main__":
    main()
