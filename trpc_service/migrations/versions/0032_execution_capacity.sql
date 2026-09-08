-- Durable capacity is the number of accepted executions that have not yet
-- reached a terminal state. The security-definer boundary is deliberate: the
-- gateway's tenant role must enforce one platform-wide limit without being
-- able to read another tenant's executions.

CREATE TABLE platform.execution_admission_bucket (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  tokens numeric NOT NULL CHECK (tokens >= 0),
  last_refill_at timestamptz NOT NULL
);

-- statement
INSERT INTO platform.execution_admission_bucket (id, tokens, last_refill_at)
VALUES (true, 180000, clock_timestamp());

-- statement
CREATE FUNCTION platform.pending_execution_count()
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
  SELECT count(*) FROM tenant.agent_execution WHERE status = 'PENDING'
$function$;

-- statement
CREATE FUNCTION platform.try_admit_execution(
  max_in_flight integer,
  sustained_per_second integer,
  burst_per_second integer,
  burst_seconds integer
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
  bucket_tokens numeric;
  bucket_refill_at timestamptz;
  bucket_capacity numeric;
  pending_count bigint;
BEGIN
  IF max_in_flight < 1 OR sustained_per_second < 1
    OR burst_per_second < 1 OR burst_seconds < 1 THEN
    RAISE EXCEPTION 'execution capacity settings must be positive';
  END IF;

  PERFORM pg_advisory_xact_lock(9042, 34);
  SELECT tokens, last_refill_at INTO bucket_tokens, bucket_refill_at
  FROM platform.execution_admission_bucket WHERE id = true FOR UPDATE;
  bucket_capacity := burst_per_second * burst_seconds;
  bucket_tokens := LEAST(
    bucket_capacity,
    bucket_tokens + GREATEST(
      0,
      EXTRACT(epoch FROM clock_timestamp() - bucket_refill_at) * sustained_per_second
    )
  );

  SELECT count(*) INTO pending_count
  FROM tenant.agent_execution WHERE status = 'PENDING';
  IF pending_count >= max_in_flight THEN
    UPDATE platform.execution_admission_bucket
    SET tokens = bucket_tokens, last_refill_at = clock_timestamp() WHERE id = true;
    RETURN 'INFLIGHT_SATURATED';
  END IF;
  IF bucket_tokens < 1 THEN
    UPDATE platform.execution_admission_bucket
    SET tokens = bucket_tokens, last_refill_at = clock_timestamp() WHERE id = true;
    RETURN 'RATE_EXCEEDED';
  END IF;

  UPDATE platform.execution_admission_bucket
  SET tokens = bucket_tokens - 1, last_refill_at = clock_timestamp() WHERE id = true;
  RETURN 'ALLOWED';
END;
$function$;

-- statement
REVOKE ALL ON FUNCTION platform.pending_execution_count() FROM PUBLIC;

-- statement
REVOKE ALL ON FUNCTION platform.try_admit_execution(integer, integer, integer, integer) FROM PUBLIC;

-- statement
GRANT EXECUTE ON FUNCTION platform.pending_execution_count() TO trpc_platform_app;

-- statement
GRANT EXECUTE ON FUNCTION platform.try_admit_execution(integer, integer, integer, integer) TO trpc_platform_app;
