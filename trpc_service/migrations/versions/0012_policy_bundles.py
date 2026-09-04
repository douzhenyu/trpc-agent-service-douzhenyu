"""Create signed, versioned Policy Bundle storage."""

from pathlib import Path

from alembic import op

revision = "0012_policy_bundles"
down_revision = "0011_budgets_cost_ledger"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("policy bundle schema downgrade is intentionally unsupported")
