-- Durable approval requests: one immutable intent binding (release, tool
-- version, params hash, subject, policy version) with a validity window.

CREATE TABLE tenant.tool_approval (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  approval_id uuid NOT NULL,
  release_id text NOT NULL,
  tool_name text NOT NULL,
  tool_version integer NOT NULL CHECK (tool_version >= 1),
  params_hash text NOT NULL CHECK (params_hash ~ '^[0-9a-f]{64}$'),
  params jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(params) = 'object'),
  side_effect text NOT NULL CHECK (side_effect IN ('READ_ONLY','IDEMPOTENT_WRITE','NON_IDEMPOTENT_WRITE','HIGH_RISK')),
  requested_by text NOT NULL CHECK (length(requested_by) BETWEEN 1 AND 256),
  requester_role text NOT NULL CHECK (length(requester_role) BETWEEN 1 AND 64),
  policy_version text NOT NULL,
  status text NOT NULL CHECK (status IN ('PENDING','APPROVED','DENIED','EXPIRED','CONSUMED')),
  requested_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  decided_by text,
  decided_at timestamptz,
  PRIMARY KEY (tenant_id, approval_id)
);

CREATE INDEX tool_approval_open_idx ON tenant.tool_approval
  (tenant_id, tool_name, tool_version, requested_by) WHERE status IN ('PENDING','APPROVED');

ALTER TABLE tenant.tool_approval ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.tool_approval FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.tool_approval
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.tool_approval TO trpc_platform_app;

-- Execution checkpoints: a parked execution waiting for its approval decision.

CREATE TABLE tenant.execution_checkpoint (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  checkpoint_id uuid NOT NULL,
  execution_id text NOT NULL,
  session_id text NOT NULL,
  release_id text NOT NULL,
  approval_id uuid NOT NULL,
  tool_name text NOT NULL,
  tool_version integer NOT NULL CHECK (tool_version >= 1),
  params_hash text NOT NULL CHECK (params_hash ~ '^[0-9a-f]{64}$'),
  requested_by text NOT NULL,
  parked_by text NOT NULL,
  status text NOT NULL CHECK (status IN ('WAITING_APPROVAL','RESUMED','ABANDONED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  resumed_by text,
  resumed_at timestamptz,
  PRIMARY KEY (tenant_id, checkpoint_id)
);

CREATE INDEX execution_checkpoint_open_idx ON tenant.execution_checkpoint
  (tenant_id, session_id) WHERE status = 'WAITING_APPROVAL';

ALTER TABLE tenant.execution_checkpoint ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.execution_checkpoint FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.execution_checkpoint
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.execution_checkpoint TO trpc_platform_app;

-- Reconciliation resolutions: one append-only closure per OUTCOME_UNKNOWN call.

CREATE TABLE tenant.tool_call_reconciliation (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  call_id uuid NOT NULL,
  decision text NOT NULL CHECK (decision IN ('CONFIRMED_EXECUTED','CONFIRMED_NOT_EXECUTED')),
  resolved_by text NOT NULL CHECK (length(resolved_by) BETWEEN 1 AND 256),
  note text,
  resolved_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, call_id)
);

ALTER TABLE tenant.tool_call_reconciliation ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.tool_call_reconciliation FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.tool_call_reconciliation
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.tool_call_reconciliation TO trpc_platform_app;
