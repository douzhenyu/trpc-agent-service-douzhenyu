"""Persist tenant retention, Legal Holds and erasure evidence."""

from pathlib import Path

from alembic import op

revision = "0030_content_lifecycle"
down_revision = "0029_eval_suites"
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
    raise RuntimeError("content lifecycle schema downgrade is intentionally unsupported")
