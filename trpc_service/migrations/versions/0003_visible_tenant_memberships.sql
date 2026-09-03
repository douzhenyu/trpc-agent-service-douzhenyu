DROP POLICY tenant_isolation ON tenant.member;

CREATE POLICY tenant_isolation ON tenant.member
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

CREATE POLICY member_self_discovery ON tenant.member
  FOR SELECT
  USING (user_id = NULLIF(current_setting('app.user_id', true), '')::uuid);
