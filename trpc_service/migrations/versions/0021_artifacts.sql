-- Tenant-owned Artifact metadata and bounded payloads.  The service exposes
-- bytes only through a signed tenant-and-subject constrained capability.

CREATE TABLE tenant.artifact (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  artifact_id uuid NOT NULL,
  subject_id text NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 256),
  execution_id text NOT NULL CHECK (length(execution_id) BETWEEN 1 AND 128),
  filename text NOT NULL CHECK (length(filename) BETWEEN 1 AND 255),
  media_type text NOT NULL CHECK (length(media_type) BETWEEN 1 AND 128),
  content bytea NOT NULL CHECK (octet_length(content) <= 8388608),
  size_bytes integer NOT NULL CHECK (size_bytes >= 0 AND size_bytes <= 8388608),
  sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  classification text NOT NULL CHECK (classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, artifact_id),
  CHECK (expires_at > created_at)
);

CREATE INDEX artifact_expiry_idx ON tenant.artifact (tenant_id, expires_at);

ALTER TABLE tenant.artifact ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.artifact FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.artifact
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, DELETE ON tenant.artifact TO trpc_platform_app;
