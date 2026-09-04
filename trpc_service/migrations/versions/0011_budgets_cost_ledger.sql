CREATE TABLE tenant.budget (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  application_id uuid,
  scope text NOT NULL CHECK (scope IN ('TENANT_MONTHLY','AGENT_DAILY','EXECUTION')),
  limit_micros bigint NOT NULL CHECK (limit_micros >= 0),
  contingency_micros bigint NOT NULL DEFAULT 0 CHECK (contingency_micros >= 0),
  unknown_policy text NOT NULL DEFAULT 'FAIL_CLOSED'
    CHECK (unknown_policy IN ('FAIL_CLOSED','CONTINGENCY')),
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX budget_scope_unique
  ON tenant.budget (tenant_id, COALESCE(application_id, '00000000-0000-0000-0000-000000000000'::uuid),
  scope);

CREATE TABLE tenant.budget_period_state (
  tenant_id uuid NOT NULL,
  budget_id uuid NOT NULL,
  period_key text NOT NULL CHECK (length(period_key) BETWEEN 1 AND 64),
  reserved_micros bigint NOT NULL DEFAULT 0 CHECK (reserved_micros >= 0),
  consumed_micros bigint NOT NULL DEFAULT 0 CHECK (consumed_micros >= 0),
  allowance_micros bigint NOT NULL DEFAULT 0,
  contingency_reserved_micros bigint NOT NULL DEFAULT 0 CHECK (contingency_reserved_micros >= 0),
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  PRIMARY KEY (tenant_id, budget_id, period_key),
  FOREIGN KEY (tenant_id, budget_id) REFERENCES tenant.budget(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE tenant.budget_alert (
  tenant_id uuid NOT NULL,
  budget_id uuid NOT NULL,
  period_key text NOT NULL,
  level text NOT NULL CHECK (level IN ('WARNING_70','CRITICAL_90')),
  used_micros bigint NOT NULL CHECK (used_micros >= 0),
  limit_micros bigint NOT NULL CHECK (limit_micros > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, budget_id, period_key, level),
  FOREIGN KEY (tenant_id, budget_id) REFERENCES tenant.budget(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE tenant.cost_ledger (
  tenant_id uuid NOT NULL,
  id uuid NOT NULL,
  budget_id uuid NOT NULL,
  period_key text NOT NULL,
  application_id uuid,
  execution_id text,
  entry_type text NOT NULL CHECK (entry_type IN ('RESERVE','SETTLE','RELEASE','REJECT','ADJUST')),
  amount_micros bigint NOT NULL CHECK (amount_micros >= 0),
  reserve_id uuid,
  contingency boolean NOT NULL DEFAULT false,
  reason text NOT NULL DEFAULT '' CHECK (length(reason) <= 500),
  actor text NOT NULL DEFAULT 'system' CHECK (length(actor) BETWEEN 1 AND 256),
  price_version integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id)
);

CREATE UNIQUE INDEX cost_ledger_entry_unique
  ON tenant.cost_ledger (tenant_id, budget_id, period_key, reserve_id, entry_type);
CREATE INDEX cost_ledger_attribution_idx
  ON tenant.cost_ledger (tenant_id, execution_id, created_at);

CREATE TABLE tenant.model_price (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  version integer NOT NULL CHECK (version >= 1),
  model_alias text NOT NULL CHECK (model_alias ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  input_micros_per_1k bigint NOT NULL CHECK (input_micros_per_1k >= 0),
  output_micros_per_1k bigint NOT NULL CHECK (output_micros_per_1k >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, version, model_alias)
);

ALTER TABLE tenant.budget ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.budget FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.budget_period_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.budget_period_state FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.budget_alert ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.budget_alert FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.cost_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.cost_ledger FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.model_price ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.model_price FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.budget
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.budget_period_state
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.budget_alert
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.cost_ledger
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.model_price
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

-- The cost ledger is append-only evidence: the application role can never
-- update or delete committed entries. Alerts fire once per level and stay.
GRANT SELECT, INSERT, UPDATE ON tenant.budget TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON tenant.budget_period_state TO trpc_platform_app;
GRANT SELECT, INSERT ON tenant.budget_alert TO trpc_platform_app;
GRANT SELECT, INSERT ON tenant.cost_ledger TO trpc_platform_app;
GRANT SELECT, INSERT ON tenant.model_price TO trpc_platform_app;
