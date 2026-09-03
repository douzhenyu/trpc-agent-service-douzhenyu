CREATE TABLE tenant.agent_application (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  slug text NOT NULL CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  description text NOT NULL DEFAULT '' CHECK (length(description) <= 2000),
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, slug)
);

CREATE TABLE tenant.agent_draft (
  tenant_id uuid NOT NULL,
  application_id uuid NOT NULL,
  instructions text NOT NULL DEFAULT '',
  model_alias text NOT NULL DEFAULT '',
  tool_aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
  knowledge_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  governance_policy_ref text,
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, application_id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE tenant.agent_application ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.agent_application FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.agent_draft ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.agent_draft FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.agent_application
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.agent_draft
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON
  tenant.agent_application, tenant.agent_draft TO trpc_platform_app;
