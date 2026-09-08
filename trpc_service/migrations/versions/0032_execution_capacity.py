"""Coordinate the durable execution capacity limit across gateway replicas."""

from pathlib import Path

from alembic import op

revision = "0032_execution_capacity"
down_revision = "0031_failover_lease"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split("\n-- statement\n"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Execution capacity history is intentionally append-only")
