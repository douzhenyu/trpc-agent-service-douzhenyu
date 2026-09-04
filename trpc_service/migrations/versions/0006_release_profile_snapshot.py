"""Store model routing snapshots with immutable Agent Releases."""

from pathlib import Path

from alembic import op

revision = "0006_release_profile_snapshot"
down_revision = "0005_agent_releases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("release model profile snapshot migration is intentionally unsupported")
