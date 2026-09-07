"""Persist online migration replay checkpoints and rollback states."""

from pathlib import Path

from alembic import op

revision = "0027_storage_migration_cps"
down_revision = "0026_storage_migrations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Storage migration history is intentionally append-only")
