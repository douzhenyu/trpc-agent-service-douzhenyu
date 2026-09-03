"""Create platform identity, tenancy, audit, and RLS structures."""

from alembic import op

from trpc_service.database_migrations import migration_statements

revision = "0001_platform_identity"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("the initial platform schema is intentionally irreversible")
