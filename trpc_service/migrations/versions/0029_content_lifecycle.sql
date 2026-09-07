CREATE TABLE tenant.content_retention_policy (
  tenant_id uuid PRIMARY KEY REFERENCES platform.tenant(id) ON DELETE CASCADE,
  inbound_payload_days integer NOT NULL DEFAULT 7 CHECK (inbound_payload_days BETWEEN 1 AND 30),
  session_days integer NOT NULL DEFAULT 90 CHECK (session_days BETWEEN 30 AND 365),
  memory_days integer NOT NULL DEFAULT 365 CHECK (memory_days BETWEEN 30 AND 730),
  artifact_days integer NOT NULL DEFAULT 30 CHECK (artifact_days BETWEEN 1 AND 365),
  idempotency_tombstone_days integer NOT NULL DEFAULT 365 CHECK (idempotency_tombstone_days BETWEEN 30 AND 730),
  audit_days integer NOT NULL DEFAULT 365 CHECK (audit_days BETWEEN 90 AND 2555),
  backup_days integer NOT NULL DEFAULT 35 CHECK (backup_days BETWEEN 1 AND 35),
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tenant.content_retention_change (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  inbound_payload_days integer NOT NULL CHECK (inbound_payload_days BETWEEN 1 AND 30),
  session_days integer NOT NULL CHECK (session_days BETWEEN 30 AND 365),
  memory_days integer NOT NULL CHECK (memory_days BETWEEN 30 AND 730),
  artifact_days integer NOT NULL CHECK (artifact_days BETWEEN 1 AND 365),
  idempotency_tombstone_days integer NOT NULL CHECK (idempotency_tombstone_days BETWEEN 30 AND 730),
  audit_days integer NOT NULL CHECK (audit_days BETWEEN 90 AND 2555),
  backup_days integer NOT NULL CHECK (backup_days BETWEEN 1 AND 35),
  status text NOT NULL CHECK (status IN ('PENDING_APPROVAL','APPROVED')),
  initiator text NOT NULL CHECK (length(initiator) BETWEEN 1 AND 255),
  approver text CHECK (length(approver) BETWEEN 1 AND 255),
  created_at timestamptz NOT NULL DEFAULT now(),
  approved_at timestamptz,
  PRIMARY KEY (tenant_id,id),
  CHECK ((status='PENDING_APPROVAL' AND approver IS NULL AND approved_at IS NULL)
    OR (status='APPROVED' AND approver IS NOT NULL AND approved_at IS NOT NULL))
);

CREATE UNIQUE INDEX content_retention_change_pending_idx
  ON tenant.content_retention_change (tenant_id) WHERE status='PENDING_APPROVAL';

ALTER TABLE tenant.memory_record
  ADD COLUMN last_used_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE tenant.legal_hold (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  scope text NOT NULL CHECK (scope IN ('TENANT')),
  reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 1000),
  status text NOT NULL CHECK (status IN ('PENDING_APPROVAL','ACTIVE','RELEASED')),
  initiator text NOT NULL CHECK (length(initiator) BETWEEN 1 AND 255),
  approver text CHECK (length(approver) BETWEEN 1 AND 255),
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  released_at timestamptz,
  PRIMARY KEY (tenant_id,id),
  CHECK ((status='PENDING_APPROVAL' AND approver IS NULL AND activated_at IS NULL AND released_at IS NULL)
    OR (status='ACTIVE' AND approver IS NOT NULL AND activated_at IS NOT NULL AND released_at IS NULL)
    OR (status='RELEASED' AND approver IS NOT NULL AND activated_at IS NOT NULL AND released_at IS NOT NULL))
);

CREATE TABLE tenant.content_deletion_request (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  requested_by text NOT NULL CHECK (length(requested_by) BETWEEN 1 AND 255),
  reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 1000),
  status text NOT NULL CHECK (status IN ('PENDING','RETRYABLE','PROCESSING','PRIMARY_ERASED','COMPLETED','BLOCKED_LEGAL_HOLD')),
  primary_due_at timestamptz NOT NULL,
  backup_due_at timestamptz NOT NULL,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz,
  processing_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  PRIMARY KEY (tenant_id,id),
  CHECK (backup_due_at > primary_due_at)
);

CREATE TABLE tenant.content_deletion_proof (
  tenant_id uuid NOT NULL,
  request_id uuid NOT NULL,
  backend text NOT NULL CHECK (backend IN ('SQL','REDIS','VECTOR','OBJECT','DERIVED','BACKUP')),
  deleted_count bigint NOT NULL CHECK (deleted_count >= 0),
  verified boolean NOT NULL,
  evidence_digest text NOT NULL CHECK (evidence_digest ~ '^[0-9a-f]{64}$'),
  completed_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id,request_id,backend),
  FOREIGN KEY (tenant_id,request_id) REFERENCES tenant.content_deletion_request(tenant_id,id) ON DELETE CASCADE
);

CREATE INDEX content_deletion_retry_idx ON tenant.content_deletion_request (tenant_id,next_attempt_at)
  WHERE status IN ('PENDING','RETRYABLE');

ALTER TABLE tenant.content_retention_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.content_retention_policy FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.content_retention_change ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.content_retention_change FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.legal_hold ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.legal_hold FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.content_deletion_request ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.content_deletion_request FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.content_deletion_proof ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.content_deletion_proof FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.content_retention_policy USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.content_retention_change USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.legal_hold USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.content_deletion_request USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.content_deletion_proof USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT, UPDATE ON tenant.content_retention_policy TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON tenant.content_retention_change TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON tenant.legal_hold TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON tenant.content_deletion_request TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON tenant.content_deletion_proof TO trpc_platform_app;
GRANT DELETE ON tenant.inbound_message, tenant.reply_delivery, tenant.session_event,
  tenant.session_summary TO trpc_platform_app;
GRANT UPDATE ON tenant.session_event TO trpc_platform_app;
