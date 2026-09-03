CREATE TABLE tenant.model_profile (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  alias text NOT NULL CHECK (alias ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  provider_model text NOT NULL CHECK (length(provider_model) BETWEEN 1 AND 256),
  endpoint_url text NOT NULL CHECK (length(endpoint_url) <= 512 AND endpoint_url ~ '^https?://[^[:space:]]+$'),
  secret_ref text NOT NULL CHECK (secret_ref ~ '^vault://tenant/[0-9a-f-]{36}/[a-z0-9][a-z0-9/_-]{0,255}#[A-Za-z0-9_.-]{1,64}$'),
  data_classification text NOT NULL CHECK (data_classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
  region text NOT NULL CHECK (region ~ '^[a-z][a-z0-9-]{1,62}$'),
  fallback_aliases jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(fallback_aliases) = 'array'),
  requests_per_minute integer NOT NULL DEFAULT 60 CHECK (requests_per_minute BETWEEN 1 AND 100000),
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, alias)
);

ALTER TABLE tenant.model_profile ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.model_profile FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.model_profile
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON tenant.model_profile TO trpc_platform_app;
