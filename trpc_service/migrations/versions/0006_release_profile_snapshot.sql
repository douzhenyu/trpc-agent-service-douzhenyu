ALTER TABLE tenant.agent_release
  ADD COLUMN model_profiles jsonb NOT NULL DEFAULT '[]'::jsonb
  CHECK (jsonb_typeof(model_profiles) = 'array');
