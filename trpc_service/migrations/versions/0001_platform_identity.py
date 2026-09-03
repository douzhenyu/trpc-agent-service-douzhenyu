"""Create platform identity, tenancy, audit, and RLS structures."""

from pathlib import Path

from alembic import op

revision = "0001_platform_identity"
down_revision = None
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    """Load SQL owned immutably by this revision and preserve its DO block."""
    migration = Path(__file__).with_suffix(".sql").read_text()
    block_end = "END $$;"
    role_block, remaining = migration.split(block_end, maxsplit=1)
    statements = [f"{role_block}{block_end}"]
    statements.extend(statement for item in remaining.split(";") if (statement := item.strip()))
    return statements


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("the initial platform schema is intentionally irreversible")
