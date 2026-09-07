"""Persist verified same-tenant IM subject associations."""

from pathlib import Path

from alembic import op

revision = "0020_im_subject_associations"
down_revision = "0019_summary_memory_projections"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("IM subject association migration is intentionally unsupported")
