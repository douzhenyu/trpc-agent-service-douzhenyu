CREATE TABLE platform.storage_resource_claim (
  resource_kind text NOT NULL CHECK (resource_kind IN ('SQL','REDIS','VECTOR','OBJECT','EXTERNAL_MEMORY','WORKER_POOL')),
  resource_fingerprint text NOT NULL CHECK (resource_fingerprint ~ '^[0-9a-f]{64}$'),
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (resource_kind, resource_fingerprint)
);

GRANT SELECT, INSERT, DELETE ON platform.storage_resource_claim TO trpc_platform_app;
