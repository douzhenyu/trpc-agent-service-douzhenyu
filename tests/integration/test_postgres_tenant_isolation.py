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
        assert [row["relname"] for row in tenant_tables] == [
            "agent_application",
            "agent_deployment",
            "agent_draft",
            "agent_execution",
            "agent_release",
            "agent_session",
            "budget",
            "budget_alert",
            "budget_period_state",
            "cost_ledger",
            "execution_checkpoint",
            "member",
            "member_role",
            "model_price",
            "model_profile",
            "policy_bundle",
            "session_event",
            "session_lease",
            "tool_approval",
            "tool_call",
            "tool_call_reconciliation",
            "tool_definition",
        ]
        assert all(row["relrowsecurity"] and row["relforcerowsecurity"] for row in tenant_tables)
        assert all(row["owner"] != "trpc_platform_app" for row in tenant_tables)
    finally:
        await admin.close()

    app = await asyncpg.connect(APP_URL)
    application_id = uuid4()
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
            await app.execute(
                """INSERT INTO tenant.agent_application (tenant_id,id,slug,name)
                VALUES ($1,$2,'isolated-agent','Isolated Agent')""",
                tenant_a,
                application_id,
            )
            await app.execute(
                """INSERT INTO tenant.agent_draft
                (tenant_id,application_id,instructions,model_alias)
                VALUES ($1,$2,'Answer safely.','balanced')""",
                tenant_a,
                application_id,
            )
            await app.execute(
                """INSERT INTO tenant.model_profile
                (tenant_id,id,alias,provider_model,endpoint_url,secret_ref,data_classification,region)
                VALUES ($1,$2,'balanced','fake-balanced','https://fake-llm.test/v1',
                $3,'INTERNAL','cn-north-1')""",
                tenant_a,
                uuid4(),
                f"vault://tenant/{tenant_a}/llm/balanced#api_key",
            )
            await app.execute(
                """INSERT INTO tenant.agent_release
                (tenant_id,id,application_id,model_alias,data_classification,region,release_version)
                VALUES ($1,$2,$3,'balanced','INTERNAL','cn-north-1',1)""",
                tenant_a,
                uuid4(),
                application_id,
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
            assert await app.fetchval("SELECT count(*) FROM tenant.agent_application") == 0
            assert await app.fetchval("SELECT count(*) FROM tenant.agent_deployment") == 0
            assert await app.fetchval("SELECT count(*) FROM tenant.agent_draft") == 0
            assert await app.fetchval("SELECT count(*) FROM tenant.agent_release") == 0
            assert await app.fetchval("SELECT count(*) FROM tenant.model_profile") == 0
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await app.execute(
                    "INSERT INTO tenant.member_role (tenant_id,id,member_id,role) "
                    "VALUES ($1,$2,$3,'TENANT_AUDITOR')",
                    tenant_a,
                    uuid4(),
                    member_id,
                )

        async with app.transaction():
            await app.execute("SELECT set_config('app.user_id',$1,true)", str(user_id))
            visible = await app.fetch("SELECT tenant_id FROM tenant.member ORDER BY tenant_id")
            assert [row["tenant_id"] for row in visible] == [tenant_a]
            assert (
                await app.execute(
                    "UPDATE tenant.member SET version=version+1 WHERE tenant_id=$1 AND id=$2",
                    tenant_a,
                    member_id,
                )
                == "UPDATE 0"
            )
            assert (
                await app.execute(
                    "DELETE FROM tenant.member WHERE tenant_id=$1 AND id=$2",
                    tenant_a,
                    member_id,
                )
                == "DELETE 0"
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await app.execute(
                    "INSERT INTO tenant.member (tenant_id,id,user_id) VALUES ($1,$2,$3)",
                    tenant_a,
                    uuid4(),
                    user_id,
                )

        async with app.transaction():
            await app.execute("SELECT set_config('app.user_id',$1,true)", str(uuid4()))
            assert await app.fetchval("SELECT count(*) FROM tenant.member") == 0

        async with app.transaction():
            await app.execute("SELECT set_config('app.tenant_id',$1,true)", str(tenant_b))
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await app.execute(
                    """INSERT INTO tenant.agent_application (tenant_id,id,slug,name)
                    VALUES ($1,$2,'cross-tenant','Cross Tenant')""",
                    tenant_a,
                    uuid4(),
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

        async with app.transaction():
            await app.execute("SELECT set_config('app.tenant_id',$1,true)", str(tenant_b))
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await app.execute(
                    """INSERT INTO tenant.agent_draft
                    (tenant_id,application_id,instructions,model_alias)
                    VALUES ($1,$2,'Cross tenant.','balanced')""",
                    tenant_b,
                    application_id,
                )

        async with app.transaction():
            await app.execute("SELECT set_config('app.tenant_id',$1,true)", str(tenant_b))
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await app.execute(
                    """INSERT INTO tenant.model_profile
                    (tenant_id,id,alias,provider_model,endpoint_url,secret_ref,data_classification,region)
                    VALUES ($1,$2,'cross-tenant','fake-cross','https://fake-llm.test/v1',
                    $3,'INTERNAL','cn-north-1')""",
                    tenant_a,
                    uuid4(),
                    f"vault://tenant/{tenant_a}/llm/cross#api_key",
                )

        assert await app.fetchval("SELECT count(*) FROM tenant.member") == 0
        assert await app.fetchval("SELECT count(*) FROM tenant.member_role") == 0
        assert await app.fetchval("SELECT count(*) FROM tenant.agent_application") == 0
        assert await app.fetchval("SELECT count(*) FROM tenant.agent_deployment") == 0
        assert await app.fetchval("SELECT count(*) FROM tenant.agent_draft") == 0
        assert await app.fetchval("SELECT count(*) FROM tenant.model_profile") == 0
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
