ALTER TABLE tenant.storage_profile ALTER COLUMN encryption_key_ref DROP DEFAULT;
ALTER TABLE tenant.storage_profile
  ADD CONSTRAINT storage_profile_encryption_key_ref_nonempty CHECK (length(encryption_key_ref) > 0);
