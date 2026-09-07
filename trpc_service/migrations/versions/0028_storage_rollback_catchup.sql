ALTER TABLE tenant.storage_migration DROP CONSTRAINT storage_migration_state_check;
ALTER TABLE tenant.storage_migration ADD CONSTRAINT storage_migration_state_check CHECK
  (state IN ('PREPARED','BACKFILLING','CATCHING_UP','VALIDATING','READY_TO_SWITCH',
             'OBSERVING','ROLLING_BACK','ROLLBACK_CATCHING_UP','ROLLBACK_VALIDATING',
             'COMPLETED','ROLLED_BACK','FAILED'));
