"""Create tenant-isolated Agent application and Agent Draft resources."""

from pathlib import Path

from alembic import op

revision = "0002_agent_drafts"
down_revision = "0001_platform_identity"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Agent application schema downgrade is intentionally unsupported")
