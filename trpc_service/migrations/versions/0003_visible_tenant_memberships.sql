DROP POLICY tenant_isolation ON tenant.member;

CREATE POLICY tenant_isolation ON tenant.member
  USING (
    tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid
    OR user_id = NULLIF(current_setting('app.user_id', true), '')::uuid
  )
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
