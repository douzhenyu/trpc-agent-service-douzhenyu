CREATE TABLE tenant.channel_connection_lease (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  connection_key text NOT NULL CHECK (length(connection_key) BETWEEN 1 AND 512),
  owner_id text NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 256),
  fencing_token bigint NOT NULL CHECK (fencing_token >= 1),
  expires_at timestamptz NOT NULL,
  renewed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, connection_key)
);

ALTER TABLE tenant.channel_connection_lease ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.channel_connection_lease FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant.channel_connection_lease
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.channel_connection_lease TO trpc_platform_app;
