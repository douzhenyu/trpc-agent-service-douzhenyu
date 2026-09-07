"""Create version-gated Summary and source-traceable Memory projections."""

from pathlib import Path

from alembic import op

revision = "0019_summary_memory_projections"
down_revision = "0018_merge_feishu_knowledge"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Summary and Memory projection migration is intentionally unsupported")
