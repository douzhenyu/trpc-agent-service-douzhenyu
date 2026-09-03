import createClient from "openapi-fetch";

import type { components, paths } from "./generated/admin-api";

export type Session = components["schemas"]["SessionResponse"];
export type Tenant = components["schemas"]["TenantResponse"];
export type TenantGroup = components["schemas"]["TenantGroupResponse"];
export type PlatformUser = components["schemas"]["PlatformUserResponse"];

const client = createClient<paths>({
  baseUrl: globalThis.location.origin,
  fetch: (request) => globalThis.fetch(request),
  credentials: "same-origin",
});

function key(): string {
  return crypto.randomUUID();
}

export async function getSession(): Promise<Session | null> {
  const { data, response } = await client.GET("/api/v1/auth/session");
  if (response.status === 401) return null;
  if (!response.ok || !data) throw new Error("无法读取登录状态");
  return data;
}

export async function emergencyLogin(
  username: string,
  password: string,
): Promise<Session> {
  const { data, response } = await client.POST(
    "/api/v1/auth/emergency/session",
    {
      body: { username, password },
    },
  );
  if (!response.ok || !data) throw new Error("应急登录失败");
  return data;
}

export async function getTenants(): Promise<Tenant[]> {
  const { data, response } = await client.GET("/api/v1/tenants");
  if (!response.ok || !data) throw new Error("无法读取租户");
  return data.items;
}

export async function createTenant(
  slug: string,
  name: string,
): Promise<Tenant> {
  const { data, response } = await client.POST("/api/v1/tenants", {
    params: { header: { "Idempotency-Key": key() } },
    body: { slug, name },
  });
  if (!response.ok || !data) throw new Error("创建租户失败");
  return data;
}

export async function getGroups(): Promise<TenantGroup[]> {
  const { data, response } = await client.GET("/api/v1/tenant-groups");
  if (!response.ok || !data) throw new Error("无法读取 Tenant Group");
  return data.items;
}

export async function createGroup(
  name: string,
  tenantIds: string[],
): Promise<TenantGroup> {
  const { data, response } = await client.POST("/api/v1/tenant-groups", {
    params: { header: { "Idempotency-Key": key() } },
    body: { name, tenant_ids: tenantIds },
  });
  if (!response.ok || !data) throw new Error("创建 Tenant Group 失败");
  return data;
}

export async function getUsers(): Promise<PlatformUser[]> {
  const { data, response } = await client.GET("/api/v1/platform-users");
  if (!response.ok || !data) throw new Error("无法读取平台用户");
  return data.items;
}

export async function assignRole(
  userId: string,
  role: "PLATFORM_ADMIN" | "PLATFORM_AUDITOR",
): Promise<void> {
  const { response } = await client.PUT(
    "/api/v1/platform-users/{user_id}/roles/{role}",
    {
      params: {
        path: { user_id: userId, role },
        header: { "Idempotency-Key": key() },
      },
    },
  );
  if (!response.ok) throw new Error("分配角色失败");
}
