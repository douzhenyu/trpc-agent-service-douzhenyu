"""Persist immutable released Agent execution routes."""

from pathlib import Path

from alembic import op

revision = "0005_agent_releases"
down_revision = "0004_model_profiles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("agent release migration is intentionally unsupported")
