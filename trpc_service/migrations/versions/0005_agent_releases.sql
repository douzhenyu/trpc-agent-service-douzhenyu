CREATE TABLE tenant.agent_release (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  application_id uuid NOT NULL,
  model_alias text NOT NULL CHECK (model_alias ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  data_classification text NOT NULL CHECK (data_classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
  region text NOT NULL CHECK (region ~ '^[a-z][a-z0-9-]{1,62}$'),
  fallback_aliases jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(fallback_aliases) = 'array'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE tenant.agent_release ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.agent_release FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.agent_release
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON tenant.agent_release TO trpc_platform_app;
