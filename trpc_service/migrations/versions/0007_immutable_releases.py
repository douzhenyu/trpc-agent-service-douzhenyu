"""Prevent the application role from changing published releases."""

from alembic import op

revision = "0007_immutable_releases"
down_revision = "0006_release_profile_snapshot"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("REVOKE UPDATE, DELETE ON tenant.agent_release FROM trpc_platform_app")


def downgrade() -> None:
    raise RuntimeError("published releases must remain immutable")
