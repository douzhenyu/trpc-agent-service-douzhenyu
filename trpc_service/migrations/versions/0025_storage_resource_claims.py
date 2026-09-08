"""Prevent dedicated Storage Profiles from sharing a physical resource."""

from pathlib import Path

from alembic import op

revision = "0025_storage_claims"
down_revision = "0024_storage_key_required"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Dedicated storage claims are intentionally unsupported")
