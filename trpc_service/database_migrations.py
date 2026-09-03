"""Idempotent control-plane schema migration entry point."""

from __future__ import annotations

import json
import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.admin_api.database import sqlalchemy_url

MIGRATION = r"""
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trpc_platform_app') THEN
    CREATE ROLE trpc_platform_app NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
      NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS platform;
CREATE SCHEMA IF NOT EXISTS tenant;

CREATE TABLE IF NOT EXISTS platform.tenant (
  id uuid PRIMARY KEY,
  slug text NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED')),
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS platform.tenant_group (
  id uuid PRIMARY KEY,
  name text NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 200),
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS platform.tenant_group_member (
  group_id uuid NOT NULL REFERENCES platform.tenant_group(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  PRIMARY KEY (group_id, tenant_id)
);

CREATE TABLE IF NOT EXISTS platform.platform_user (
  id uuid PRIMARY KEY,
  issuer text NOT NULL,
  subject text NOT NULL,
  email text,
  display_name text NOT NULL,
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (issuer, subject)
);

CREATE TABLE IF NOT EXISTS platform.platform_role_assignment (
  user_id uuid NOT NULL REFERENCES platform.platform_user(id) ON DELETE CASCADE,
  role text NOT NULL CHECK (role IN ('PLATFORM_ADMIN', 'PLATFORM_AUDITOR')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, role)
);

CREATE TABLE IF NOT EXISTS platform.audit_event (
  id uuid PRIMARY KEY,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  tenant_id uuid REFERENCES platform.tenant(id) ON DELETE SET NULL,
  actor text NOT NULL,
  auth_method text NOT NULL CHECK (auth_method IN ('oidc', 'emergency', 'anonymous')),
  action text NOT NULL,
  decision text NOT NULL CHECK (decision IN ('ALLOW', 'DENY')),
  target_type text,
  target_id text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS platform.idempotency_record (
  actor text NOT NULL,
  key text NOT NULL,
  operation text NOT NULL,
  request_hash text NOT NULL,
  response jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (actor, key)
);

CREATE TABLE IF NOT EXISTS tenant.member (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  user_id uuid NOT NULL REFERENCES platform.platform_user(id) ON DELETE CASCADE,
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS tenant.member_role (
  tenant_id uuid NOT NULL,
  id uuid NOT NULL,
  member_id uuid NOT NULL,
  role text NOT NULL CHECK (role IN ('TENANT_ADMIN', 'AGENT_DEVELOPER', 'TENANT_AUDITOR')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, member_id, role),
  FOREIGN KEY (tenant_id, member_id)
    REFERENCES tenant.member(tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE tenant.member ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.member FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.member_role ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.member_role FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON tenant.member;
CREATE POLICY tenant_isolation ON tenant.member
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
DROP POLICY IF EXISTS tenant_isolation ON tenant.member_role;
CREATE POLICY tenant_isolation ON tenant.member_role
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

REVOKE ALL ON SCHEMA platform, tenant FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA platform, tenant FROM PUBLIC;
GRANT USAGE ON SCHEMA platform, tenant TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON
  platform.tenant, platform.tenant_group, platform.tenant_group_member,
  platform.platform_user, platform.platform_role_assignment,
  platform.idempotency_record,
  tenant.member, tenant.member_role TO trpc_platform_app;
REVOKE UPDATE, DELETE ON platform.audit_event FROM trpc_platform_app;
GRANT SELECT, INSERT ON platform.audit_event TO trpc_platform_app;
"""


def migration_statements() -> list[str]:
    """Split the initial revision without breaking the role-creation DO block."""
    block_end = "END $$;"
    role_block, remaining = MIGRATION.split(block_end, maxsplit=1)
    statements = [f"{role_block}{block_end}"]
    statements.extend(statement for item in remaining.split(";") if (statement := item.strip()))
    return statements


async def apply_migrations(database_url: str, app_role_password: str | None = None) -> None:
    import asyncio

    config = Config()
    config.set_main_option("script_location", str(Path(__file__).with_name("migrations")))
    config.set_main_option("sqlalchemy.url", sqlalchemy_url(database_url).replace("%", "%%"))
    await asyncio.to_thread(command.upgrade, config, "head")
    engine = create_async_engine(sqlalchemy_url(database_url))
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                """ALTER ROLE trpc_platform_app NOINHERIT NOSUPERUSER NOCREATEDB
                NOCREATEROLE NOREPLICATION NOBYPASSRLS"""
            )
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
                    """SELECT count(*) FROM pg_class objects
                    JOIN pg_roles owner ON owner.oid=objects.relowner
                    JOIN pg_namespace namespace ON namespace.oid=objects.relnamespace
                    WHERE owner.rolname='trpc_platform_app'
                      AND namespace.nspname IN ('platform','tenant')"""
                )
            )
            if owned_objects:
                raise RuntimeError("trpc_platform_app must not own tenant or platform objects")
            if app_role_password:
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
