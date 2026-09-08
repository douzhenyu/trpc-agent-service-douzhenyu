-- Durable capacity is the number of accepted executions that have not yet
-- reached a terminal state. The security-definer boundary is deliberate: the
-- gateway's tenant role must enforce one platform-wide limit without being
-- able to read another tenant's executions.

CREATE FUNCTION platform.pending_execution_count()
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
  SELECT count(*) FROM tenant.agent_execution WHERE status = 'PENDING'
$function$;

CREATE FUNCTION platform.try_admit_execution(max_in_flight integer)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
  WITH capacity_lock AS (
    SELECT pg_advisory_xact_lock(9042, 34)
  )
  SELECT count(*) < max_in_flight
  FROM tenant.agent_execution, capacity_lock
  WHERE status = 'PENDING'
$function$;

REVOKE ALL ON FUNCTION platform.pending_execution_count() FROM PUBLIC;
REVOKE ALL ON FUNCTION platform.try_admit_execution(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION platform.pending_execution_count() TO trpc_platform_app;
GRANT EXECUTE ON FUNCTION platform.try_admit_execution(integer) TO trpc_platform_app;
