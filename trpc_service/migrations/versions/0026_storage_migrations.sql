CREATE TABLE tenant.storage_migration (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  source_profile_id uuid NOT NULL,
  target_profile_id uuid NOT NULL,
  state text NOT NULL CHECK (state IN ('PREPARED','BACKFILLING','CATCHING_UP','VALIDATING','READY_TO_SWITCH','OBSERVING','COMPLETED','ROLLED_BACK','FAILED')),
  approval_status text NOT NULL CHECK (approval_status IN ('PENDING','APPROVED','DENIED')),
  requested_by text NOT NULL CHECK (length(requested_by) BETWEEN 1 AND 256),
  approved_by text,
  approved_at timestamptz,
  rollback_approval_status text NOT NULL DEFAULT 'NONE' CHECK (rollback_approval_status IN ('NONE','PENDING','APPROVED','DENIED')),
  rollback_requested_by text,
  rollback_approved_by text,
  rollback_approved_at timestamptz,
  validation jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(validation) = 'object'),
  observation_seconds integer NOT NULL CHECK (observation_seconds BETWEEN 60 AND 86400),
  observation_ends_at timestamptz,
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,id),
  CONSTRAINT storage_migration_distinct_profiles CHECK (source_profile_id <> target_profile_id),
  FOREIGN KEY (tenant_id,source_profile_id) REFERENCES tenant.storage_profile(tenant_id,id),
  FOREIGN KEY (tenant_id,target_profile_id) REFERENCES tenant.storage_profile(tenant_id,id)
);

CREATE UNIQUE INDEX storage_migration_one_open_per_tenant
  ON tenant.storage_migration (tenant_id)
  WHERE state NOT IN ('COMPLETED','ROLLED_BACK','FAILED');

ALTER TABLE tenant.storage_migration ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.storage_migration FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant.storage_migration
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE ON tenant.storage_migration TO trpc_platform_app;
