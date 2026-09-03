"""Allow authenticated users to discover only their own tenant memberships."""

from pathlib import Path

from alembic import op

revision = "0003_visible_tenant_memberships"
down_revision = "0002_agent_drafts"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("tenant membership discovery downgrade is intentionally unsupported")
