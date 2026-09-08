-- Global Failover Lease: one region owns the primary role under a monotonic
-- fencing token. A warm-standby region keeps replicated data warm but stays
-- fenced off from ingress and business consumption until it holds the lease.

CREATE TABLE platform.failover_lease (
  id uuid NOT NULL,
  region text NOT NULL CHECK (length(region) BETWEEN 1 AND 64),
  role text NOT NULL CHECK (role IN ('PRIMARY','STANDBY','FAILING_OVER','FAILING_BACK','FENCED')),
  fencing_token bigint NOT NULL CHECK (fencing_token >= 0),
  acquired_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  released_at timestamptz,
  operator text NOT NULL CHECK (length(operator) BETWEEN 1 AND 256),
  PRIMARY KEY (id),
  CHECK (expires_at > acquired_at)
);

CREATE UNIQUE INDEX failover_lease_single_active
  ON platform.failover_lease ((role = 'PRIMARY'))
  WHERE released_at IS NULL AND role = 'PRIMARY';

CREATE TABLE platform.failover_drill (
  id uuid NOT NULL,
  region_from text NOT NULL CHECK (length(region_from) BETWEEN 1 AND 64),
  region_to text NOT NULL CHECK (length(region_to) BETWEEN 1 AND 64),
  rpo_seconds integer NOT NULL CHECK (rpo_seconds >= 0),
  rto_seconds integer NOT NULL CHECK (rto_seconds >= 0),
  passed boolean NOT NULL,
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(evidence) = 'object'),
  executed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id)
);

ALTER TABLE platform.failover_lease ENABLE ROW LEVEL SECURITY;
ALTER TABLE platform.failover_lease FORCE ROW LEVEL SECURITY;
CREATE POLICY platform_isolation ON platform.failover_lease
  USING (true)
  WITH CHECK (true);

ALTER TABLE platform.failover_drill ENABLE ROW LEVEL SECURITY;
ALTER TABLE platform.failover_drill FORCE ROW LEVEL SECURITY;
CREATE POLICY platform_isolation ON platform.failover_drill
  USING (true)
  WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE ON platform.failover_lease TO trpc_platform_app;
GRANT SELECT, INSERT ON platform.failover_drill TO trpc_platform_app;
