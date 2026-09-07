"""Persist versioned Eval Suites, Eval Runs and canary gate evidence."""

from pathlib import Path

from alembic import op

revision = "0029_eval_suites"
down_revision = "0028_storage_rollback_catchup"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    return [
        statement.strip()
        for statement in Path(__file__).with_suffix(".sql").read_text().split(";\n")
        if statement.strip()
    ]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Eval Suite schema downgrade is intentionally unsupported")
