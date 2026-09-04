CREATE TABLE tenant.policy_bundle (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  version integer NOT NULL CHECK (version >= 1),
  rules jsonb NOT NULL CHECK (jsonb_typeof(rules) = 'object'),
  bundle jsonb NOT NULL CHECK (jsonb_typeof(bundle) = 'object'),
  signature text NOT NULL CHECK (signature ~ '^[0-9a-f]{64}$'),
  status text NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT','ACTIVE','CANARY','RETIRED')),
  canary_percentage integer NOT NULL DEFAULT 0 CHECK (canary_percentage BETWEEN 0 AND 100),
  created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  PRIMARY KEY (tenant_id, version)
);

ALTER TABLE tenant.policy_bundle ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.policy_bundle FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.policy_bundle
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.policy_bundle TO trpc_platform_app;
