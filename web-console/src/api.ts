import createClient from "openapi-fetch";

import type { components, paths } from "./generated/admin-api";

export type Session = components["schemas"]["SessionResponse"];
export type Tenant = components["schemas"]["TenantResponse"];
export type TenantGroup = components["schemas"]["TenantGroupResponse"];
export type PlatformUser = components["schemas"]["PlatformUserResponse"];
export type AgentApplication =
  components["schemas"]["AgentApplicationResponse"];
export type AgentApplicationCreate =
  components["schemas"]["AgentApplicationCreate"];
export type AgentApplicationUpdate =
  components["schemas"]["AgentApplicationUpdate"];
export type AgentDraft = components["schemas"]["AgentDraftResponse"];
export type AgentDraftCreate = components["schemas"]["AgentDraftCreate"];
export type AgentDraftUpdate = components["schemas"]["AgentDraftUpdate"];
export type DraftValidation = components["schemas"]["DraftValidationResponse"];
export type AgentRelease = components["schemas"]["AgentReleaseResponse"];
export type AgentReleaseCreate = components["schemas"]["AgentReleaseCreate"];
export type AgentDeployment = components["schemas"]["AgentDeploymentResponse"];
export type AgentDeploymentCreate =
  components["schemas"]["AgentDeploymentCreate"];
export type AgentDeploymentRollback =
  components["schemas"]["AgentDeploymentRollback"];
export type ModelProfile = components["schemas"]["ModelProfileResponse"];
export type ModelProfileCreate = components["schemas"]["ModelProfileCreate"];
export type ModelProfileUpdate = components["schemas"]["ModelProfileUpdate"];

const client = createClient<paths>({
  baseUrl: globalThis.location.origin,
  fetch: (request) => globalThis.fetch(request),
  credentials: "same-origin",
});

function idempotencyKey(): string {
  return crypto.randomUUID();
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
  }
}

function apiError(
  response: Response,
  fallback: string,
  payload: unknown,
): ApiError {
  const body = payload as {
    error?: { code?: string; message?: string };
  } | null;
  return new ApiError(
    body?.error?.message ?? fallback,
    response.status,
    body?.error?.code,
  );
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

export async function getModelProfiles(
  tenantId: string,
): Promise<ModelProfile[]> {
  const { data, error, response } = await client.GET(
    "/api/v1/tenants/{tenant_id}/model-profiles",
    { params: { path: { tenant_id: tenantId } } },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法读取模型配置档", error);
  return data.items;
}

export async function createModelProfile(
  tenantId: string,
  payload: ModelProfileCreate,
): Promise<ModelProfile> {
  const { data, error, response } = await client.POST(
    "/api/v1/tenants/{tenant_id}/model-profiles",
    {
      params: {
        path: { tenant_id: tenantId },
        header: { "Idempotency-Key": idempotencyKey() },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法保存模型配置档", error);
  return data;
}

export async function createTenant(
  slug: string,
  name: string,
): Promise<Tenant> {
  const { data, response } = await client.POST("/api/v1/tenants", {
    params: { header: { "Idempotency-Key": idempotencyKey() } },
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
    params: { header: { "Idempotency-Key": idempotencyKey() } },
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
  version: number,
): Promise<void> {
  const { response } = await client.PUT(
    "/api/v1/platform-users/{user_id}/roles/{role}",
    {
      params: {
        path: { user_id: userId, role },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${version}"`,
        },
      },
    },
  );
  if (!response.ok) throw new Error("分配角色失败");
}

export async function getAgentApplications(
  tenantId: string,
): Promise<AgentApplication[]> {
  const { data, error, response } = await client.GET(
    "/api/v1/tenants/{tenant_id}/agent-applications",
    { params: { path: { tenant_id: tenantId } } },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法读取 Agent 应用", error);
  return data.items;
}

export async function createAgentApplication(
  tenantId: string,
  payload: AgentApplicationCreate,
): Promise<AgentApplication> {
  const { data, error, response } = await client.POST(
    "/api/v1/tenants/{tenant_id}/agent-applications",
    {
      params: {
        path: { tenant_id: tenantId },
        header: { "Idempotency-Key": idempotencyKey() },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法创建 Agent 应用", error);
  return data;
}

export async function updateAgentApplication(
  application: AgentApplication,
  payload: AgentApplicationUpdate,
): Promise<AgentApplication> {
  const { data, error, response } = await client.PATCH(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}",
    {
      params: {
        path: {
          tenant_id: application.tenant_id,
          application_id: application.id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${application.version}"`,
        },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法更新 Agent 应用", error);
  return data;
}

export async function deleteAgentApplication(
  application: AgentApplication,
): Promise<void> {
  const { error, response } = await client.DELETE(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}",
    {
      params: {
        path: {
          tenant_id: application.tenant_id,
          application_id: application.id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${application.version}"`,
        },
      },
    },
  );
  if (!response.ok) throw apiError(response, "无法删除 Agent 应用", error);
}

export async function getAgentDraft(
  tenantId: string,
  applicationId: string,
): Promise<AgentDraft | null> {
  const { data, error, response } = await client.GET(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/draft",
    {
      params: { path: { tenant_id: tenantId, application_id: applicationId } },
    },
  );
  if (response.status === 404) return null;
  if (!response.ok || !data)
    throw apiError(response, "无法读取 Agent Draft", error);
  return data;
}

export async function createAgentDraft(
  tenantId: string,
  applicationId: string,
  payload: AgentDraftCreate,
): Promise<AgentDraft> {
  const { data, error, response } = await client.PUT(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/draft",
    {
      params: {
        path: { tenant_id: tenantId, application_id: applicationId },
        header: { "Idempotency-Key": idempotencyKey() },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法创建 Agent Draft", error);
  return data;
}

export async function updateAgentDraft(
  draft: AgentDraft,
  payload: AgentDraftUpdate,
): Promise<AgentDraft> {
  const { data, error, response } = await client.PATCH(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/draft",
    {
      params: {
        path: {
          tenant_id: draft.tenant_id,
          application_id: draft.application_id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${draft.version}"`,
        },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法更新 Agent Draft", error);
  return data;
}

export async function deleteAgentDraft(draft: AgentDraft): Promise<void> {
  const { error, response } = await client.DELETE(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/draft",
    {
      params: {
        path: {
          tenant_id: draft.tenant_id,
          application_id: draft.application_id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${draft.version}"`,
        },
      },
    },
  );
  if (!response.ok) throw apiError(response, "无法删除 Agent Draft", error);
}

export async function validateAgentDraft(
  draft: AgentDraft,
): Promise<DraftValidation> {
  const { data, error, response } = await client.POST(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/draft/validate",
    {
      params: {
        path: {
          tenant_id: draft.tenant_id,
          application_id: draft.application_id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${draft.version}"`,
        },
      },
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法校验 Agent Draft", error);
  return data;
}

export async function getAgentReleases(
  tenantId: string,
  applicationId: string,
): Promise<AgentRelease[]> {
  const { data, error, response } = await client.GET(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/releases",
    {
      params: { path: { tenant_id: tenantId, application_id: applicationId } },
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法读取 Agent Release", error);
  return data.items;
}

export async function publishAgentRelease(
  application: AgentApplication,
  payload: AgentReleaseCreate,
): Promise<AgentRelease> {
  const { data, error, response } = await client.POST(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/releases",
    {
      params: {
        path: {
          tenant_id: application.tenant_id,
          application_id: application.id,
        },
        header: { "Idempotency-Key": idempotencyKey() },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法发布 Agent Release", error);
  return data;
}

export async function getAgentDeployments(
  tenantId: string,
  applicationId: string,
): Promise<AgentDeployment[]> {
  const { data, error, response } = await client.GET(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/deployments",
    {
      params: { path: { tenant_id: tenantId, application_id: applicationId } },
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法读取环境 Deployment", error);
  return data.items;
}

export async function createAgentDeployment(
  application: AgentApplication,
  payload: AgentDeploymentCreate,
  activeDeployment?: AgentDeployment,
): Promise<AgentDeployment> {
  const { data, error, response } = await client.POST(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/deployments",
    {
      params: {
        path: {
          tenant_id: application.tenant_id,
          application_id: application.id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          ...(activeDeployment
            ? { "If-Match": `"${activeDeployment.version}"` }
            : {}),
        },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法创建环境 Deployment", error);
  return data;
}

export async function approveAgentDeployment(
  deployment: AgentDeployment,
): Promise<AgentDeployment> {
  const { data, error, response } = await client.POST(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/deployments/{deployment_id}/approve",
    {
      params: {
        path: {
          tenant_id: deployment.tenant_id,
          application_id: deployment.application_id,
          deployment_id: deployment.id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${deployment.version}"`,
        },
      },
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法批准 Production Deployment", error);
  return data;
}

export async function rollbackAgentDeployment(
  deployment: AgentDeployment,
  payload: AgentDeploymentRollback,
): Promise<AgentDeployment> {
  const { data, error, response } = await client.POST(
    "/api/v1/tenants/{tenant_id}/agent-applications/{application_id}/deployments/{deployment_id}/rollback",
    {
      params: {
        path: {
          tenant_id: deployment.tenant_id,
          application_id: deployment.application_id,
          deployment_id: deployment.id,
        },
        header: {
          "Idempotency-Key": idempotencyKey(),
          "If-Match": `"${deployment.version}"`,
        },
      },
      body: payload,
    },
  );
  if (!response.ok || !data)
    throw apiError(response, "无法回滚环境 Deployment", error);
  return data;
}
