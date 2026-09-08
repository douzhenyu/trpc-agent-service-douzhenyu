-- Summary and Memory are rebuildable projections of immutable Session Events.
-- They are never authoritative replacements for Session Event history.

ALTER TABLE tenant.agent_execution
  ADD COLUMN subject_id text CHECK (length(subject_id) BETWEEN 1 AND 256),
  ADD COLUMN memory_policy_version text NOT NULL DEFAULT 'policy:none'
    CHECK (length(memory_policy_version) BETWEEN 1 AND 128);

CREATE TABLE tenant.session_summary (
  tenant_id uuid NOT NULL,
  session_id text NOT NULL,
  source_from_version bigint NOT NULL CHECK (source_from_version >= 1),
  source_version bigint NOT NULL CHECK (source_version >= source_from_version),
  content text NOT NULL CHECK (length(content) BETWEEN 1 AND 16000),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id),
  FOREIGN KEY (tenant_id, session_id)
    REFERENCES tenant.agent_session(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE tenant.memory_record (
  tenant_id uuid NOT NULL,
  id uuid NOT NULL,
  subject_id text NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 256),
  source_session_id text NOT NULL,
  source_from_version bigint NOT NULL CHECK (source_from_version >= 1),
  source_to_version bigint NOT NULL CHECK (source_to_version >= source_from_version),
  policy_version text NOT NULL CHECK (length(policy_version) BETWEEN 1 AND 128),
  content text NOT NULL CHECK (length(content) BETWEEN 1 AND 16000),
  is_valid boolean NOT NULL DEFAULT true,
  invalidated_at timestamptz,
  invalidation_reason text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, subject_id, source_session_id, source_from_version, source_to_version),
  FOREIGN KEY (tenant_id, source_session_id)
    REFERENCES tenant.agent_session(tenant_id, id) ON DELETE CASCADE,
  CHECK ((is_valid AND invalidated_at IS NULL AND invalidation_reason IS NULL)
    OR (NOT is_valid AND invalidated_at IS NOT NULL
      AND length(invalidation_reason) BETWEEN 1 AND 512))
);

CREATE INDEX memory_record_subject_visible_idx
  ON tenant.memory_record (tenant_id, subject_id, created_at DESC) WHERE is_valid;

-- Durable acknowledgement state for the Job Worker consumer. A failed
-- projection is retried from the published Outbox record, and an already written
-- projection can safely be replayed because Summary and Memory are idempotent.
CREATE TABLE platform.session_projection_delivery (
  outbox_id uuid NOT NULL REFERENCES platform.outbox_record(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  status text NOT NULL CHECK (status IN ('PENDING','SUCCEEDED','DEAD_LETTER')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error text,
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (outbox_id),
  CHECK ((status = 'SUCCEEDED' AND completed_at IS NOT NULL)
    OR (status <> 'SUCCEEDED' AND completed_at IS NULL))
);

CREATE INDEX session_projection_delivery_pending_idx
  ON platform.session_projection_delivery (status, updated_at)
  WHERE status = 'PENDING';

ALTER TABLE tenant.session_summary ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.session_summary FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.memory_record ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.memory_record FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.session_summary
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.memory_record
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.session_summary TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant.memory_record TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON platform.session_projection_delivery TO trpc_platform_app;
