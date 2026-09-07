ALTER TABLE tenant.storage_profile
  ADD COLUMN encryption_key_ref text NOT NULL DEFAULT ''
  CHECK (length(encryption_key_ref) <= 384);
