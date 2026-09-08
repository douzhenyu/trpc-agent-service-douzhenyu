CREATE TABLE tenant.knowledge_base (
  tenant_id uuid NOT NULL REFERENCES platform.tenant(id) ON DELETE CASCADE,
  id uuid NOT NULL,
  slug text NOT NULL CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, slug),
  UNIQUE (tenant_id, id)
);

CREATE TABLE tenant.knowledge_revision (
  tenant_id uuid NOT NULL,
  id uuid NOT NULL,
  base_id uuid NOT NULL,
  revision_version integer NOT NULL CHECK (revision_version >= 1),
  source_snapshot jsonb NOT NULL CHECK (jsonb_typeof(source_snapshot) = 'array'),
  chunking jsonb NOT NULL CHECK (jsonb_typeof(chunking) = 'object'),
  embedding_model text NOT NULL CHECK (length(embedding_model) BETWEEN 1 AND 128),
  index_config jsonb NOT NULL CHECK (jsonb_typeof(index_config) = 'object'),
  content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, base_id, revision_version),
  UNIQUE (tenant_id, base_id, id),
  FOREIGN KEY (tenant_id, base_id) REFERENCES tenant.knowledge_base(tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE tenant.knowledge_revision_build (
  tenant_id uuid NOT NULL,
  revision_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('BUILDING','READY','FAILED')),
  validated_at timestamptz,
  error_code text,
  PRIMARY KEY (tenant_id, revision_id),
  FOREIGN KEY (tenant_id, revision_id) REFERENCES tenant.knowledge_revision(tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE tenant.knowledge_document (
  tenant_id uuid NOT NULL,
  revision_id uuid NOT NULL,
  id uuid NOT NULL,
  source_ref text NOT NULL CHECK (length(source_ref) BETWEEN 1 AND 512),
  content text NOT NULL CHECK (length(content) BETWEEN 1 AND 1000000),
  content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
  data_classification text NOT NULL CHECK (data_classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
  PRIMARY KEY (tenant_id, revision_id, id),
  UNIQUE (tenant_id, revision_id, source_ref),
  FOREIGN KEY (tenant_id, revision_id) REFERENCES tenant.knowledge_revision(tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE tenant.knowledge_document_acl (
  tenant_id uuid NOT NULL,
  revision_id uuid NOT NULL,
  document_id uuid NOT NULL,
  subject_id text NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 256),
  PRIMARY KEY (tenant_id, revision_id, document_id, subject_id),
  FOREIGN KEY (tenant_id, revision_id, document_id)
    REFERENCES tenant.knowledge_document(tenant_id, revision_id, id) ON DELETE RESTRICT
);

CREATE TABLE tenant.knowledge_chunk (
  tenant_id uuid NOT NULL,
  revision_id uuid NOT NULL,
  document_id uuid NOT NULL,
  chunk_index integer NOT NULL CHECK (chunk_index >= 0),
  content text NOT NULL CHECK (length(content) BETWEEN 1 AND 1000000),
  search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
  PRIMARY KEY (tenant_id, revision_id, document_id, chunk_index),
  FOREIGN KEY (tenant_id, revision_id, document_id)
    REFERENCES tenant.knowledge_document(tenant_id, revision_id, id) ON DELETE RESTRICT
);

CREATE INDEX knowledge_chunk_search_idx ON tenant.knowledge_chunk USING gin (search_vector);

ALTER TABLE tenant.agent_execution
  ADD COLUMN knowledge_revision_ids jsonb NOT NULL DEFAULT '[]'::jsonb
  CHECK (jsonb_typeof(knowledge_revision_ids) = 'array');

CREATE TABLE tenant.knowledge_deployment (
  tenant_id uuid NOT NULL,
  id uuid NOT NULL,
  base_id uuid NOT NULL,
  environment text NOT NULL CHECK (environment IN ('DEVELOPMENT','STAGING','PRODUCTION')),
  revision_id uuid NOT NULL,
  previous_revision_id uuid,
  rollout_percentage integer NOT NULL CHECK (rollout_percentage BETWEEN 1 AND 100),
  source_kind text NOT NULL CHECK (source_kind IN ('DEPLOY','ROLLBACK')),
  created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, base_id) REFERENCES tenant.knowledge_base(tenant_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, base_id, revision_id)
    REFERENCES tenant.knowledge_revision(tenant_id, base_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, base_id, previous_revision_id)
    REFERENCES tenant.knowledge_revision(tenant_id, base_id, id) ON DELETE RESTRICT
);

CREATE OR REPLACE FUNCTION tenant.require_building_knowledge_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM tenant.knowledge_revision_build
    WHERE tenant_id=NEW.tenant_id AND revision_id=NEW.revision_id AND status='BUILDING'
  ) THEN RAISE EXCEPTION 'knowledge revision is immutable'; END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER knowledge_document_only_while_building
  BEFORE INSERT ON tenant.knowledge_document
  FOR EACH ROW EXECUTE FUNCTION tenant.require_building_knowledge_revision();
CREATE TRIGGER knowledge_document_acl_only_while_building
  BEFORE INSERT ON tenant.knowledge_document_acl
  FOR EACH ROW EXECUTE FUNCTION tenant.require_building_knowledge_revision();
CREATE TRIGGER knowledge_chunk_only_while_building
  BEFORE INSERT ON tenant.knowledge_chunk
  FOR EACH ROW EXECUTE FUNCTION tenant.require_building_knowledge_revision();

CREATE OR REPLACE FUNCTION tenant.advance_knowledge_build_status()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.status <> 'BUILDING' OR NEW.status NOT IN ('READY','FAILED') THEN
    RAISE EXCEPTION 'knowledge build status is immutable';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER knowledge_build_only_advances_once
  BEFORE UPDATE ON tenant.knowledge_revision_build
  FOR EACH ROW EXECUTE FUNCTION tenant.advance_knowledge_build_status();

ALTER TABLE tenant.knowledge_base ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_base FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_revision ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_revision FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_revision_build ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_revision_build FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_document ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_document FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_document_acl ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_document_acl FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_chunk ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_chunk FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_deployment ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant.knowledge_deployment FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant.knowledge_base
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.knowledge_revision
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.knowledge_revision_build
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.knowledge_document
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.knowledge_document_acl
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.knowledge_chunk
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON tenant.knowledge_deployment
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

GRANT SELECT, INSERT ON tenant.knowledge_base, tenant.knowledge_revision,
  tenant.knowledge_document, tenant.knowledge_document_acl, tenant.knowledge_chunk,
  tenant.knowledge_deployment TO trpc_platform_app;
GRANT SELECT, INSERT, UPDATE ON tenant.knowledge_revision_build TO trpc_platform_app;
