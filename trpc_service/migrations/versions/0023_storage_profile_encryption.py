"""Require a tenant-scoped encryption key reference for Storage Profiles."""

from pathlib import Path

from alembic import op

revision = "0023_storage_profile_encryption"
down_revision = "0022_storage_profiles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Storage Profile encryption migration is intentionally unsupported")
