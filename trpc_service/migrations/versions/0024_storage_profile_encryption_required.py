"""Disallow empty Storage Profile encryption key references."""

from pathlib import Path

from alembic import op

revision = "0024_storage_key_required"
down_revision = "0023_storage_profile_encryption"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Storage Profile encryption migration is intentionally unsupported")
