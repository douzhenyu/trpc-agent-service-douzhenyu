"""Create hard budget and immutable cost ledger schema."""

from pathlib import Path

from alembic import op

revision = "0011_budgets_cost_ledger"
down_revision = "0010_execution_bus_sessions"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("cost ledger schema downgrade is intentionally unsupported")
