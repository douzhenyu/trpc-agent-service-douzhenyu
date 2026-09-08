"""Persist the global Failover Lease and quarterly drill evidence."""

from pathlib import Path

from alembic import op

revision = "0031_failover_lease"
down_revision = "0030_content_lifecycle"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Failover lease history is intentionally append-only")
