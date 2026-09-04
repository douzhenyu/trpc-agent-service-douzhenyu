CREATE TABLE tenant.agent_session (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  application_id uuid NOT NULL,
  id text NOT NULL CHECK (length(id) BETWEEN 1 AND 512),
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE tenant.session_event (
  tenant_id uuid NOT NULL,
  session_id text NOT NULL,
  sequence bigint NOT NULL CHECK (sequence >= 1),
  execution_id uuid NOT NULL,
  kind text NOT NULL CHECK (kind ~ '^[A-Z][A-Z0-9_]{0,63}$'),
  payload jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id, sequence),
  FOREIGN KEY (tenant_id, session_id)
    REFERENCES tenant.agent_session(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE tenant.session_lease (
  tenant_id uuid NOT NULL,
  session_id text NOT NULL,
  owner_id text NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 256),
  fencing_token bigint NOT NULL DEFAULT 1 CHECK (fencing_token >= 1),
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  renewed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id),
  FOREIGN KEY (tenant_id, session_id)
    REFERENCES tenant.agent_session(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE tenant.agent_execution (
  tenant_id uuid NOT NULL,
  id uuid NOT NULL,
  application_id uuid NOT NULL,
  release_id uuid NOT NULL,
  environment text NOT NULL CHECK (environment IN ('DEVELOPMENT','STAGING','PRODUCTION')),
  session_id text NOT NULL,
  message_id text NOT NULL CHECK (length(message_id) BETWEEN 1 AND 256),
  status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SUCCEEDED','FAILED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, message_id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE CASCADE,
  FOREIGN KEY (tenant_id, release_id)
    REFERENCES tenant.agent_release(tenant_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, session_id)
    REFERENCES tenant.agent_session(tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX agent_execution_session_idx
  ON tenant.agent_execution (tenant_id, session_id, created_at);

CREATE TABLE platform.outbox_record (
  tenant_id uuid NOT NULL,
  id uuid NOT NULL,
  message_id text NOT NULL CHECK (length(message_id) BETWEEN 1 AND 256),
  source text NOT NULL CHECK (length(source) BETWEEN 1 AND 256),
  event_type text NOT NULL CHECK (event_type ~ '^[a-z][a-z0-9.-]{0,127}$'),
  partition_key text NOT NULL CHECK (length(partition_key) BETWEEN 1 AND 600),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PUBLISHED')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  PRIMARY KEY (id),
  UNIQUE (tenant_id, message_id)
);

CREATE INDEX outbox_pending_idx
  ON platform.outbox_record (created_at, id) WHERE status = 'PENDING';

ALTER TABLE tenant.agent_session ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.agent_session FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.session_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.session_event FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.session_lease ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.session_lease FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.agent_execution ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.agent_execution FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.agent_session
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.session_event
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.session_lease
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.agent_execution
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

-- Session Events are append-only authoritative facts. The application role can
-- never update or delete committed history. Everything else stays mutable for
-- the data plane: leases are stolen, execution status advances, sessions
-- advance their version and the outbox marks records published.
GRANT SELECT, INSERT ON tenant.session_event TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON
  tenant.agent_session, tenant.session_lease, tenant.agent_execution
  TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON platform.outbox_record TO trpc_platform_app;
