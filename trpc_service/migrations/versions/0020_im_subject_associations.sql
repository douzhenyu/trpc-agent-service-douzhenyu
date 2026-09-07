-- Associations are explicit administrative attestations. They are never inferred
-- from matching external identifiers and exist only inside one tenant.
CREATE TABLE tenant.im_subject_association (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  subject_id text NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 256),
  related_subject_id text NOT NULL CHECK (length(related_subject_id) BETWEEN 1 AND 256),
  verification_reference text NOT NULL CHECK (length(verification_reference) BETWEEN 1 AND 256),
  verified_by text NOT NULL CHECK (length(verified_by) BETWEEN 1 AND 256),
  verified_at timestamptz NOT NULL DEFAULT now(),
  version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
  PRIMARY KEY (tenant_id, subject_id, related_subject_id),
  CHECK (subject_id < related_subject_id)
);

CREATE INDEX im_subject_association_related_idx
  ON tenant.im_subject_association (tenant_id, related_subject_id, subject_id);

ALTER TABLE tenant.im_subject_association ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.im_subject_association FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.im_subject_association
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON tenant.im_subject_association TO trpc_platform_app;
