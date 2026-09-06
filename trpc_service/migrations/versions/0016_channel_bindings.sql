-- Channel bindings: one external bot resolved to one tenant application.
-- Only 密钥引用 (opaque vault references) are storable, never secret values.

CREATE TABLE tenant.channel_binding (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  binding_id uuid NOT NULL,
  channel_type text NOT NULL CHECK (channel_type IN ('FAKE','WECOM','FEISHU')),
  external_bot_id text NOT NULL CHECK (length(external_bot_id) BETWEEN 1 AND 128),
  application_id uuid NOT NULL,
  environment text NOT NULL CHECK (environment IN ('DEVELOPMENT','STAGING','PRODUCTION')),
  secret_ref text NOT NULL CHECK (secret_ref ~ '^vault://[a-z0-9/_:-]+#[a-zA-Z0-9_-]+$'),
  status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','DISABLED')),
  created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, binding_id),
  UNIQUE (tenant_id, channel_type, external_bot_id),
  FOREIGN KEY (tenant_id, application_id)
    REFERENCES tenant.agent_application(tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE tenant.channel_binding ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.channel_binding FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.channel_binding
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.channel_binding TO trpc_platform_app;
GRANT UPDATE (status) ON tenant.channel_binding TO trpc_platform_app;

-- Inbound ledger: one immutable row per inbound message key. Redelivery with
-- the same payload reuses the recorded execution while a different payload
-- under the same key is isolated below and never routed.

CREATE TABLE tenant.inbound_message (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  binding_id uuid NOT NULL,
  message_key text NOT NULL CHECK (length(message_key) BETWEEN 1 AND 256),
  payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
  external_user_id text NOT NULL CHECK (length(external_user_id) BETWEEN 1 AND 128),
  execution_id uuid,
  release_id uuid,
  status text NOT NULL DEFAULT 'ACCEPTED' CHECK (status IN ('ACCEPTED','CONFLICTED')),
  occurred_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, binding_id, message_key),
  FOREIGN KEY (tenant_id, binding_id)
    REFERENCES tenant.channel_binding(tenant_id, binding_id) ON DELETE CASCADE
);

ALTER TABLE tenant.inbound_message ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.inbound_message FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.inbound_message
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.inbound_message TO trpc_platform_app;

-- Conflict evidence for the same key arriving with a different payload.

CREATE TABLE tenant.inbound_conflict (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  conflict_id uuid NOT NULL,
  binding_id uuid NOT NULL,
  message_key text NOT NULL,
  recorded_hash text NOT NULL CHECK (recorded_hash ~ '^[0-9a-f]{64}$'),
  received_hash text NOT NULL CHECK (received_hash ~ '^[0-9a-f]{64}$'),
  detected_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conflict_id)
);

ALTER TABLE tenant.inbound_conflict ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.inbound_conflict FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.inbound_conflict
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.inbound_conflict TO trpc_platform_app;

-- Reply deliveries: stable delivery id, per-attempt rows in the child table.

CREATE TABLE tenant.reply_delivery (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  delivery_id uuid NOT NULL,
  binding_id uuid NOT NULL,
  execution_id text NOT NULL,
  external_conversation_id text NOT NULL CHECK (length(external_conversation_id) BETWEEN 1 AND 256),
  content text NOT NULL,
  status text NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED','IN_FLIGHT','DELIVERED','RATE_LIMITED','FAILED','OUTCOME_UNKNOWN','DEAD_LETTER')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, delivery_id)
);

CREATE INDEX reply_delivery_dead_letter_idx ON tenant.reply_delivery (tenant_id)
  WHERE status = 'DEAD_LETTER';

ALTER TABLE tenant.reply_delivery ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.reply_delivery FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.reply_delivery
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.reply_delivery TO trpc_platform_app;

CREATE TABLE tenant.reply_delivery_attempt (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  attempt_id uuid NOT NULL,
  delivery_id uuid NOT NULL,
  attempt_no integer NOT NULL CHECK (attempt_no >= 1),
  outcome text NOT NULL,
  error_code text,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  PRIMARY KEY (tenant_id, attempt_id),
  UNIQUE (tenant_id, delivery_id, attempt_no)
);

ALTER TABLE tenant.reply_delivery_attempt ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.reply_delivery_attempt FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.reply_delivery_attempt
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.reply_delivery_attempt TO trpc_platform_app;
