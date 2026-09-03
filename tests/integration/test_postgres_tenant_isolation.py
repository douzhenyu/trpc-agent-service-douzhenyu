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
            "SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname='trpc_platform_app'"
        )
        assert dict(role) == {"rolsuper": False, "rolbypassrls": False}
        assert await admin.fetchval(
            "SELECT tableowner <> 'trpc_platform_app' FROM pg_tables "
            "WHERE schemaname='tenant' AND tablename='member'"
        )
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
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await app.execute(
                    "INSERT INTO tenant.member_role (tenant_id,id,member_id,role) "
                    "VALUES ($1,$2,$3,'TENANT_ADMIN')",
                    tenant_b,
                    uuid4(),
                    member_id,
                )

        assert await app.fetchval("SELECT count(*) FROM tenant.member") == 0
    finally:
        await app.close()


def test_rls_context_and_composite_foreign_keys_block_cross_tenant_access() -> None:
    asyncio.run(_exercise_isolation())
