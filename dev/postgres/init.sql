-- Local development only. Production credentials are provisioned by the platform secret manager.
CREATE ROLE trpc_platform_app LOGIN PASSWORD 'app-password'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
