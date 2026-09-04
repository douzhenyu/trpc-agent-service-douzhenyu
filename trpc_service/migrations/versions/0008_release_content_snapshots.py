"""Persist versioned immutable Agent Release configuration snapshots."""

from alembic import op

revision = "0008_release_content_snapshots"
down_revision = "0007_immutable_releases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """ALTER TABLE tenant.agent_release
        ADD COLUMN release_version integer,
        ADD COLUMN source_draft_version integer,
        ADD COLUMN source_actor text NOT NULL DEFAULT 'legacy-migration',
        ADD COLUMN source_kind text NOT NULL DEFAULT 'LEGACY'
          CHECK (source_kind IN ('DRAFT','LEGACY')),
        ADD COLUMN draft_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb
          CHECK (jsonb_typeof(draft_snapshot) = 'object'),
        ADD COLUMN content_hash text NOT NULL DEFAULT repeat('0', 64)
          CHECK (content_hash ~ '^[0-9a-f]{64}$')"""
    )
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute(
        """WITH numbered AS (
          SELECT tenant_id, id,
            row_number() OVER (PARTITION BY tenant_id, application_id ORDER BY created_at, id)
              AS version
          FROM tenant.agent_release
        )
        UPDATE tenant.agent_release release
        SET release_version=numbered.version,
            draft_snapshot=jsonb_build_object(
              'legacy_release_configuration', jsonb_build_object(
                'model_alias', release.model_alias,
                'data_classification', release.data_classification,
                'region', release.region,
                'fallback_aliases', release.fallback_aliases,
                'model_profiles', release.model_profiles
              )
            ),
            content_hash=encode(digest(jsonb_build_object(
              'legacy_release_configuration', jsonb_build_object(
                'model_alias', release.model_alias,
                'data_classification', release.data_classification,
                'region', release.region,
                'fallback_aliases', release.fallback_aliases,
                'model_profiles', release.model_profiles
              )
            )::text, 'sha256'), 'hex')
        FROM numbered
        WHERE release.tenant_id=numbered.tenant_id AND release.id=numbered.id"""
    )
    op.execute("ALTER TABLE tenant.agent_release ALTER COLUMN release_version SET NOT NULL")
    op.execute(
        """ALTER TABLE tenant.agent_release
        ADD CONSTRAINT agent_release_application_version_unique
          UNIQUE (tenant_id, application_id, release_version)"""
    )
    op.execute(
        """ALTER TABLE tenant.agent_release
        ADD CONSTRAINT agent_release_source_provenance_check CHECK (
          (source_kind='DRAFT' AND source_draft_version >= 1)
          OR (source_kind='LEGACY' AND source_draft_version IS NULL)
        )"""
    )


def downgrade() -> None:
    raise RuntimeError("published release content snapshots must remain immutable")
