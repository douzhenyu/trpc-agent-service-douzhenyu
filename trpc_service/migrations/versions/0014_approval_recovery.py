"""Create tool approvals, execution checkpoints and reconciliation records."""

from pathlib import Path

from alembic import op

revision = "0014_approval_recovery"
down_revision = "0013_tool_governance"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("approval recovery schema downgrade is intentionally unsupported")
