"""Persist fenced ownership for Channel Gateway long connections."""

from pathlib import Path

from alembic import op

revision = "0017_feishu_connection_leases"
down_revision = "0016_channel_bindings"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    return [
        statement.strip()
        for statement in Path(__file__).with_suffix(".sql").read_text().split(";\n")
        if statement.strip()
    ]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Feishu connection lease schema downgrade is intentionally unsupported")
