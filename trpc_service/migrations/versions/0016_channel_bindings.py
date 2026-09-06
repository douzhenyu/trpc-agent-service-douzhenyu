"""Create channel bindings, the inbound ledger and reply delivery tables."""

from pathlib import Path

from alembic import op

revision = "0016_channel_bindings"
down_revision = "0015_audit_chain_worm"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("channel binding schema downgrade is intentionally unsupported")
