from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest

from trpc_service.database_migrations import apply_migrations

pytestmark = pytest.mark.integration
ADMIN_URL = os.getenv(
    "TEST_DATABASE_ADMIN_URL", "postgresql://postgres:postgres@127.0.0.1:55432/trpc_platform"
)
APP_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql://trpc_platform_app:app-password@127.0.0.1:55432/trpc_platform"
)


async def _exercise_isolation() -> None:
    await apply_migrations(ADMIN_URL, "app-password")
    admin = await asyncpg.connect(ADMIN_URL)
    tenant_a, tenant_b, user_id, member_id = uuid4(), uuid4(), uuid4(), uuid4()
    try:
        await admin.execute(
            "TRUNCATE tenant.member_role, tenant.member, platform.platform_user, "
            "platform.audit_event, platform.tenant CASCADE"
        )
        await admin.executemany(
            "INSERT INTO platform.tenant (id,slug,name) VALUES ($1,$2,$3)",
            [(tenant_a, "tenant-a", "Tenant A"), (tenant_b, "tenant-b", "Tenant B")],
        )
        await admin.execute(
            """INSERT INTO platform.platform_user (id,issuer,subject,display_name)
            VALUES ($1,'issuer','subject','User')""",
            user_id,
        )
        role = await admin.fetchrow(
            "SELECT rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls "
            "FROM pg_roles WHERE rolname='trpc_platform_app'"
        )
        assert dict(role) == {
            "rolsuper": False,
            "rolinherit": False,
            "rolcreaterole": False,
            "rolcreatedb": False,
            "rolreplication": False,
            "rolbypassrls": False,
        }
        tenant_tables = await admin.fetch(
            """SELECT class.relname, class.relrowsecurity, class.relforcerowsecurity,
            roles.rolname AS owner
            FROM pg_class class
            JOIN pg_namespace namespace ON namespace.oid=class.relnamespace
            JOIN pg_roles roles ON roles.oid=class.relowner
            WHERE namespace.nspname='tenant' AND class.relkind='r'
            ORDER BY class.relname"""
        )
        assert [row["relname"] for row in tenant_tables] == ["member", "member_role"]
        assert all(row["relrowsecurity"] and row["relforcerowsecurity"] for row in tenant_tables)
        assert all(row["owner"] != "trpc_platform_app" for row in tenant_tables)
    finally:
        await admin.close()

    app = await asyncpg.connect(APP_URL)
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await app.execute(
                "INSERT INTO tenant.member (tenant_id,id,user_id) VALUES ($1,$2,$3)",
                tenant_a,
                member_id,
                user_id,
            )

        async with app.transaction():
            await app.execute("SELECT set_config('app.tenant_id',$1,true)", str(tenant_a))
            await app.execute(
                "INSERT INTO tenant.member (tenant_id,id,user_id) VALUES ($1,$2,$3)",
                tenant_a,
                member_id,
                user_id,
            )
            await app.execute(
                "INSERT INTO tenant.member_role (tenant_id,id,member_id,role) "
                "VALUES ($1,$2,$3,'TENANT_ADMIN')",
                tenant_a,
                uuid4(),
                member_id,
            )

        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with app.transaction():
                await app.execute("SELECT set_config('app.tenant_id',$1,true)", str(tenant_a))
                await app.execute(
                    "INSERT INTO tenant.member (tenant_id,id,user_id) VALUES ($1,$2,$3)",
                    tenant_b,
                    uuid4(),
                    user_id,
                )

        async with app.transaction():
            await app.execute("SELECT set_config('app.tenant_id',$1,true)", str(tenant_b))
            assert await app.fetchval("SELECT count(*) FROM tenant.member") == 0
            assert await app.fetchval("SELECT count(*) FROM tenant.member_role") == 0
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await app.execute(
                    "INSERT INTO tenant.member_role (tenant_id,id,member_id,role) "
                    "VALUES ($1,$2,$3,'TENANT_AUDITOR')",
                    tenant_a,
                    uuid4(),
                    member_id,
                )

        async with app.transaction():
            await app.execute("SELECT set_config('app.tenant_id',$1,true)", str(tenant_b))
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await app.execute(
                    "INSERT INTO tenant.member_role (tenant_id,id,member_id,role) "
                    "VALUES ($1,$2,$3,'TENANT_ADMIN')",
                    tenant_b,
                    uuid4(),
                    member_id,
                )

        assert await app.fetchval("SELECT count(*) FROM tenant.member") == 0
        assert await app.fetchval("SELECT count(*) FROM tenant.member_role") == 0
    finally:
        await app.close()


def test_rls_context_and_composite_foreign_keys_block_cross_tenant_access() -> None:
    asyncio.run(_exercise_isolation())


async def _exercise_role_drift_repair() -> None:
    admin = await asyncpg.connect(ADMIN_URL)
    try:
        await admin.execute("ALTER ROLE trpc_platform_app BYPASSRLS INHERIT CREATEDB CREATEROLE")
    finally:
        await admin.close()

    await apply_migrations(ADMIN_URL, "app-password")

    admin = await asyncpg.connect(ADMIN_URL)
    try:
        role = await admin.fetchrow(
            "SELECT rolinherit,rolcreatedb,rolcreaterole,rolbypassrls "
            "FROM pg_roles WHERE rolname='trpc_platform_app'"
        )
        assert dict(role) == {
            "rolinherit": False,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolbypassrls": False,
        }
    finally:
        await admin.close()


def test_migration_repairs_drifted_application_role_privileges() -> None:
    asyncio.run(_exercise_role_drift_repair())


async def _exercise_unsafe_membership_fail_closed() -> None:
    admin = await asyncpg.connect(ADMIN_URL)
    try:
        await admin.execute("DROP ROLE IF EXISTS trpc_platform_smoke_parent")
        await admin.execute("CREATE ROLE trpc_platform_smoke_parent NOLOGIN")
        await admin.execute("GRANT trpc_platform_smoke_parent TO trpc_platform_app")
        with pytest.raises(RuntimeError, match="must not inherit or assume"):
            await apply_migrations(ADMIN_URL, "app-password")
        assert not await admin.fetchval(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname='trpc_platform_app'"
        )
    finally:
        await admin.execute("REVOKE trpc_platform_smoke_parent FROM trpc_platform_app")
        await admin.execute("DROP ROLE IF EXISTS trpc_platform_smoke_parent")
        await admin.close()
    await apply_migrations(ADMIN_URL, "app-password")


def test_migration_disables_login_before_rejecting_unsafe_role_membership() -> None:
    asyncio.run(_exercise_unsafe_membership_fail_closed())
