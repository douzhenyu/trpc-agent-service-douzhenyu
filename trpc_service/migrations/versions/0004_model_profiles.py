"""Create tenant-scoped model profiles containing only secret references."""

from pathlib import Path

from alembic import op

revision = "0004_model_profiles"
down_revision = "0003_visible_tenant_memberships"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("model profile migration is intentionally unsupported")
