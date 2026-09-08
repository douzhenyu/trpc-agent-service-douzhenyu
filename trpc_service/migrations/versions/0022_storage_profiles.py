"""Persist tenant Storage Profile declarations and active selections."""

from pathlib import Path

from alembic import op

revision = "0022_storage_profiles"
down_revision = "0021_artifacts"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    return [
        statement
        for item in Path(__file__).with_suffix(".sql").read_text().split(";")
        if (statement := item.strip())
    ]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Storage Profile migration is intentionally unsupported")
