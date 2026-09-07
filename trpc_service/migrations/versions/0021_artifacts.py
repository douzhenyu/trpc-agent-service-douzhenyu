"""Persist tenant-scoped Artifact metadata, bytes and lifecycle records."""

from pathlib import Path

from alembic import op

revision = "0021_artifacts"
down_revision = "0020_im_subject_associations"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Artifact migration is intentionally unsupported")
