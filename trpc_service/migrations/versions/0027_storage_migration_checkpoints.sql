ALTER TABLE tenant.storage_migration DROP CONSTRAINT storage_migration_state_check;
ALTER TABLE tenant.storage_migration ADD CONSTRAINT storage_migration_state_check CHECK
  (state IN ('PREPARED','BACKFILLING','CATCHING_UP','VALIDATING','READY_TO_SWITCH',
             'OBSERVING','ROLLING_BACK','ROLLBACK_VALIDATING','COMPLETED','ROLLED_BACK','FAILED'));

CREATE TABLE tenant.storage_migration_checkpoint (
  tenant_id uuid NOT NULL,
  migration_id uuid NOT NULL,
  forward_watermarks jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(forward_watermarks) = 'object'),
  rollback_watermarks jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(rollback_watermarks) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,migration_id),
  FOREIGN KEY (tenant_id,migration_id) REFERENCES tenant.storage_migration(tenant_id,id) ON DELETE CASCADE
);
ALTER TABLE tenant.storage_migration_checkpoint ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.storage_migration_checkpoint FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant.storage_migration_checkpoint
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE ON tenant.storage_migration_checkpoint TO trpc_platform_app;
