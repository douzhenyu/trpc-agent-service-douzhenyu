ALTER TABLE tenant.agent_release
  ADD CONSTRAINT agent_release_tenant_id_application_id_key UNIQUE (tenant_id, id, application_id);

CREATE TABLE tenant.eval_suite (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  application_id uuid NOT NULL,
  slug text NOT NULL CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  suite_version integer NOT NULL CHECK (suite_version >= 1),
  dataset jsonb NOT NULL CHECK (jsonb_typeof(dataset) = 'object'),
  scorers jsonb NOT NULL CHECK (jsonb_typeof(scorers) = 'array'),
  thresholds jsonb NOT NULL CHECK (jsonb_typeof(thresholds) = 'object'),
  deterministic_assertions jsonb NOT NULL CHECK (jsonb_typeof(deterministic_assertions) = 'array'),
  content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
  created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, application_id, slug, suite_version),
  UNIQUE (tenant_id, id, application_id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE tenant.eval_run (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  application_id uuid NOT NULL,
  suite_id uuid NOT NULL,
  release_id uuid NOT NULL,
  environment text NOT NULL CHECK (environment IN ('DEVELOPMENT','STAGING','PRODUCTION')),
  sdk_version text NOT NULL CHECK (length(sdk_version) BETWEEN 1 AND 64),
  dependency_snapshot jsonb NOT NULL CHECK (jsonb_typeof(dependency_snapshot) = 'object'),
  evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
  results jsonb NOT NULL CHECK (jsonb_typeof(results) = 'object'),
  status text NOT NULL CHECK (status IN ('PASSED','FAILED')),
  content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
  created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, suite_id, application_id)
    REFERENCES tenant.eval_suite(tenant_id, id, application_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, release_id, application_id)
    REFERENCES tenant.agent_release(tenant_id, id, application_id) ON DELETE RESTRICT
);

CREATE INDEX eval_run_release_gate_idx ON tenant.eval_run
  (tenant_id, application_id, release_id, environment, created_at DESC, id DESC);

CREATE TABLE tenant.eval_canary_observation (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  deployment_id uuid NOT NULL,
  eval_run_id uuid NOT NULL,
  metrics jsonb NOT NULL CHECK (jsonb_typeof(metrics) = 'object'),
  decision text NOT NULL CHECK (decision IN ('CONTINUE','HALTED')),
  created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, deployment_id) REFERENCES tenant.agent_deployment(tenant_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, eval_run_id) REFERENCES tenant.eval_run(tenant_id, id) ON DELETE RESTRICT
);

ALTER TABLE tenant.agent_deployment
  DROP CONSTRAINT agent_deployment_status_check,
  ADD CONSTRAINT agent_deployment_status_check
    CHECK (status IN ('PENDING_APPROVAL','ACTIVE','HALTED')),
  ADD COLUMN halted_at timestamptz,
  ADD COLUMN halt_reason text;

ALTER TABLE tenant.eval_suite ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.eval_suite FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.eval_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.eval_run FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.eval_canary_observation ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.eval_canary_observation FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.eval_suite
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.eval_run
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.eval_canary_observation
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.eval_suite, tenant.eval_run, tenant.eval_canary_observation TO trpc_platform_app;
GRANT UPDATE (status, halted_at, halt_reason) ON tenant.agent_deployment TO trpc_platform_app;
