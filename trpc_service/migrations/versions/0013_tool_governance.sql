-- Versioned tool definitions: tenant control-plane metadata for declared and
-- MCP-sourced capabilities. Immutable per (tenant, name, version).

CREATE TABLE tenant.tool_definition (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  name text NOT NULL CHECK (name ~ '^[a-z][a-z0-9_]{1,63}$'),
  version integer NOT NULL CHECK (version >= 1),
  description text NOT NULL CHECK (length(description) BETWEEN 1 AND 512),
  side_effect text NOT NULL CHECK (side_effect IN ('READ_ONLY','IDEMPOTENT_WRITE','NON_IDEMPOTENT_WRITE','HIGH_RISK')),
  input_schema jsonb NOT NULL CHECK (jsonb_typeof(input_schema) = 'object'),
  output_schema jsonb NOT NULL CHECK (jsonb_typeof(output_schema) = 'object'),
  scopes text[] NOT NULL DEFAULT '{}',
  timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 600),
  cost_per_call_micros bigint NOT NULL DEFAULT 0 CHECK (cost_per_call_micros >= 0),
  data_classification text NOT NULL CHECK (data_classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
  supports_idempotency boolean NOT NULL DEFAULT false,
  source text NOT NULL DEFAULT 'DECLARED' CHECK (source IN ('DECLARED','MCP')),
  mcp_server text,
  created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, name, version)
);

ALTER TABLE tenant.tool_definition ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.tool_definition FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.tool_definition
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.tool_definition TO trpc_platform_app;

-- Tool-call records: one append-only, auditable row per governed invocation.
-- No UPDATE or DELETE is granted, so retries cannot rewrite history.

CREATE TABLE tenant.tool_call (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  call_id uuid NOT NULL,
  execution_id text,
  session_id text,
  release_id text,
  tool_name text NOT NULL,
  tool_version integer,
  side_effect text,
  params jsonb,
  params_hash text NOT NULL CHECK (params_hash ~ '^[0-9a-f]{64}$'),
  idempotency_key text,
  status text NOT NULL CHECK (status IN ('SUCCEEDED','FAILED','OUTCOME_UNKNOWN','BLOCKED')),
  error_code text,
  result jsonb CHECK (result IS NULL OR jsonb_typeof(result) = 'object'),
  attempts integer NOT NULL DEFAULT 1 CHECK (attempts >= 0),
  cost_micros bigint NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
  requested_by text NOT NULL CHECK (length(requested_by) BETWEEN 1 AND 256),
  data_classification text CHECK (data_classification IS NULL OR data_classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, call_id)
);

-- A successful call pins its idempotency key for replay detection while
-- failed or unknown outcomes may be re-attempted under the same key.
CREATE UNIQUE INDEX tool_call_idempotent_replay
  ON tenant.tool_call (tenant_id, idempotency_key) WHERE status = 'SUCCEEDED' AND idempotency_key IS NOT NULL;

CREATE INDEX tool_call_execution_idx ON tenant.tool_call (tenant_id, execution_id);

ALTER TABLE tenant.tool_call ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.tool_call FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.tool_call
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.tool_call TO trpc_platform_app;
