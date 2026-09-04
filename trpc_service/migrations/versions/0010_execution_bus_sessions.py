"""Create the execution bus authority tables: sessions, events, leases, executions and outbox."""

from pathlib import Path

from alembic import op

revision = "0010_execution_bus_sessions"
down_revision = "0009_agent_deployments"
branch_labels = None
depends_on = None


def migration_statements() -> list[str]:
    migration = Path(__file__).with_suffix(".sql").read_text()
    return [statement for item in migration.split(";") if (statement := item.strip())]


def upgrade() -> None:
    for statement in migration_statements():
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Session authority and outbox schema downgrade is intentionally unsupported")
