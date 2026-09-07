CREATE TABLE tenant.storage_profile (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  alias text NOT NULL CHECK (alias ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  classification text NOT NULL CHECK (classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
  worker_pool text NOT NULL CHECK (worker_pool ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  backends jsonb NOT NULL CHECK (jsonb_typeof(backends) = 'array'),
  active boolean NOT NULL DEFAULT false,
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, alias)
);

CREATE UNIQUE INDEX storage_profile_one_active_per_tenant
  ON tenant.storage_profile (tenant_id) WHERE active;

ALTER TABLE tenant.storage_profile ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.storage_profile FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant.storage_profile
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant.storage_profile TO trpc_platform_app;
