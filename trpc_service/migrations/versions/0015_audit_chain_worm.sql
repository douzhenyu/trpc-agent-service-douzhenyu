-- Immutable audit evidence: a per-tenant hash chain over the append-only
-- audit_event rows, an Audit Outbox for atomic business+audit commits, and
-- signed manifests that are archived to WORM object storage.

ALTER TABLE platform.audit_event
  ADD COLUMN IF NOT EXISTS trace_id text,
  ADD COLUMN IF NOT EXISTS chain_index bigint,
  ADD COLUMN IF NOT EXISTS event_hash text,
  ADD COLUMN IF NOT EXISTS prev_event_hash text;

CREATE UNIQUE INDEX IF NOT EXISTS audit_event_chain_index
  ON platform.audit_event (tenant_id, chain_index)
  WHERE tenant_id IS NOT NULL AND chain_index IS NOT NULL;

CREATE INDEX IF NOT EXISTS audit_event_query_idx
  ON platform.audit_event (tenant_id, occurred_at);
CREATE INDEX IF NOT EXISTS audit_event_trace_idx
  ON platform.audit_event (trace_id) WHERE trace_id IS NOT NULL;

-- Serialized per-tenant append state for the hash chain.
CREATE TABLE IF NOT EXISTS platform.audit_chain_state (
  tenant_id uuid PRIMARY KEY REFERENCES platform.tenant(id) ON DELETE CASCADE,
  last_index bigint NOT NULL DEFAULT 0 CHECK (last_index >= 0),
  last_hash text NOT NULL CHECK (last_hash ~ '^[0-9a-f]{64}$'),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Audit Outbox: business transactions enqueue audit intents atomically and a
-- dispatcher materializes them into the hash-chained online audit trail.
CREATE TABLE IF NOT EXISTS platform.audit_outbox (
  id uuid PRIMARY KEY,
  tenant_id uuid REFERENCES platform.tenant(id) ON DELETE CASCADE,
  actor text NOT NULL,
  auth_method text NOT NULL CHECK (auth_method IN ('oidc', 'emergency', 'anonymous')),
  action text NOT NULL,
  decision text NOT NULL CHECK (decision IN ('ALLOW', 'DENY')),
  target_type text,
  target_id text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
  trace_id text,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','DISPATCHED')),
  dispatched_at timestamptz
);

CREATE INDEX audit_outbox_pending_idx ON platform.audit_outbox (tenant_id, id)
  WHERE status = 'PENDING';

GRANT SELECT, INSERT, UPDATE ON platform.audit_outbox TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON platform.audit_chain_state TO trpc_platform_app;

-- Signed manifests: one immutable, verifiable statement per archived segment.
CREATE TABLE IF NOT EXISTS platform.audit_manifest (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  manifest_index bigint NOT NULL CHECK (manifest_index >= 1),
  first_chain_index bigint NOT NULL,
  last_chain_index bigint NOT NULL,
  event_count bigint NOT NULL CHECK (event_count >= 0),
  chain_head_hash text NOT NULL CHECK (chain_head_hash ~ '^[0-9a-f]{64}$'),
  signature text NOT NULL CHECK (signature ~ '^[0-9a-f]{64}$'),
  worm_location text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, manifest_index),
  CONSTRAINT audit_manifest_segment_ordered CHECK (last_chain_index >= first_chain_index)
);

GRANT SELECT, INSERT ON platform.audit_manifest TO trpc_platform_app;
