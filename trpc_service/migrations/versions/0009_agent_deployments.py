"""Persist environment-scoped Agent Release Deployments."""

from alembic import op

revision = "0009_agent_deployments"
down_revision = "0008_release_content_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """CREATE TABLE tenant.agent_deployment (
          tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
          id uuid NOT NULL,
          application_id uuid NOT NULL,
          environment text NOT NULL CHECK (environment IN ('DEVELOPMENT','STAGING','PRODUCTION')),
          release_id uuid NOT NULL,
          previous_release_id uuid,
          previous_deployment_id uuid,
          previous_deployment_version integer,
          rollout_percentage integer NOT NULL CHECK (rollout_percentage BETWEEN 1 AND 100),
          status text NOT NULL CHECK (status IN ('PENDING_APPROVAL','ACTIVE')),
          initiator text NOT NULL,
          approver text,
          version integer NOT NULL DEFAULT 1,
          created_at timestamptz NOT NULL DEFAULT now(),
          activated_at timestamptz,
          PRIMARY KEY (tenant_id, id),
          FOREIGN KEY (tenant_id, application_id)
            REFERENCES tenant.agent_application(tenant_id, id) ON DELETE CASCADE,
          FOREIGN KEY (tenant_id, release_id)
            REFERENCES tenant.agent_release(tenant_id, id) ON DELETE RESTRICT,
          FOREIGN KEY (tenant_id, previous_release_id)
            REFERENCES tenant.agent_release(tenant_id, id) ON DELETE RESTRICT,
          CHECK (
            (previous_deployment_id IS NULL AND previous_deployment_version IS NULL)
            OR (previous_deployment_id IS NOT NULL AND previous_deployment_version >= 1)
          )
        )"""
    )
    op.execute(
        """CREATE INDEX agent_deployment_active_resolution_idx
        ON tenant.agent_deployment
          (tenant_id, application_id, environment, activated_at DESC, id DESC)
        WHERE status = 'ACTIVE'"""
    )
    op.execute("ALTER TABLE tenant.agent_deployment ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE tenant.agent_deployment FORCE ROW LEVEL SECURITY")
    op.execute(
        """CREATE POLICY tenant_isolation ON tenant.agent_deployment
        USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
        WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)"""
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON tenant.agent_deployment TO trpc_platform_app")


def downgrade() -> None:
    raise RuntimeError("Agent Deployments are retained as release history")
