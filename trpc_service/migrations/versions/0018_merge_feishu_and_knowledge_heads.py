"""Merge Feishu connection leases and Knowledge Revisions schema heads."""

revision = "0018_merge_feishu_and_knowledge_heads"
down_revision = ("0017_feishu_connection_leases", "0017_knowledge_revisions")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join independently deployed schema branches without altering either schema."""


def downgrade() -> None:
    raise RuntimeError("merged Feishu and Knowledge schema history is intentionally irreversible")
