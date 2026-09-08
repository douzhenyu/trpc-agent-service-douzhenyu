"""Create immutable Knowledge Revisions and controlled retrieval storage."""

from pathlib import Path

from alembic import op

revision = "0018_knowledge_revisions"
down_revision = "0017_feishu_connection_leases"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_dollar_quote = False
    for line in Path(__file__).with_suffix(".sql").read_text().splitlines(keepends=True):
        current.append(line)
        if line.count("$$") % 2:
            in_dollar_quote = not in_dollar_quote
        if not in_dollar_quote and line.rstrip().endswith(";"):
            statements.append("".join(current).strip())
            current = []
    if current:
        raise RuntimeError("unterminated knowledge revision migration statement")
    return statements


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("knowledge revision schema downgrade is intentionally unsupported")
