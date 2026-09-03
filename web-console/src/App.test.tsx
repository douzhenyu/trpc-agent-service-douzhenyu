import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";

import App from "./App";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

test("平台管理员可通过公开 API 创建租户与 Tenant Group", async () => {
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(
      json({
        subject: "admin",
        auth_method: "emergency",
        roles: ["PLATFORM_ADMIN"],
      }),
    )
    .mockResolvedValueOnce(json({ items: [] }))
    .mockResolvedValueOnce(json({ items: [] }))
    .mockResolvedValueOnce(json({ items: [] }))
    .mockResolvedValueOnce(
      json(
        {
          id: "tenant-1",
          slug: "acme",
          name: "Acme",
          status: "ACTIVE",
          version: 1,
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
        201,
      ),
    )
    .mockResolvedValueOnce(
      json(
        {
          id: "group-1",
          name: "核心客户",
          version: 1,
          tenant_ids: ["tenant-1"],
        },
        201,
      ),
    );

  render(<App />);
  expect(
    await screen.findByRole("heading", { name: "租户与权限管理" }),
  ).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("租户标识"), {
    target: { value: "acme" },
  });
  fireEvent.change(screen.getByLabelText("租户名称"), {
    target: { value: "Acme" },
  });
  fireEvent.click(screen.getByRole("button", { name: "创建租户" }));
  expect(await screen.findByText("Acme (acme)")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("Tenant Group 名称"), {
    target: { value: "核心客户" },
  });
  fireEvent.click(screen.getByRole("checkbox", { name: "Acme" }));
  fireEvent.click(screen.getByRole("button", { name: "创建 Tenant Group" }));
  expect(await screen.findByText("核心客户 · 1 个租户")).toBeInTheDocument();
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(6));
  expect(
    fetchMock.mock.calls.every(([request]) =>
      new URL(
        request instanceof Request ? request.url : String(request),
      ).pathname.startsWith("/api/v1"),
    ),
  ).toBe(true);
});

test("未登录时展示企业 OIDC 和应急管理员入口", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
    json({ error: { code: "UNAUTHENTICATED" } }, 401),
  );
  render(<App />);
  expect(
    await screen.findByRole("link", { name: "使用企业账号登录" }),
  ).toHaveAttribute("href", "/api/v1/auth/oidc/login");
  expect(screen.getByRole("button", { name: "应急登录" })).toBeInTheDocument();
});

test("本地应急管理员可登录", async () => {
  vi.spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(json({}, 401))
    .mockResolvedValueOnce(
      json({
        subject: "emergency:admin",
        auth_method: "emergency",
        roles: ["PLATFORM_ADMIN"],
      }),
    )
    .mockResolvedValueOnce(json({ items: [] }))
    .mockResolvedValueOnce(json({ items: [] }))
    .mockResolvedValueOnce(json({ items: [] }));
  render(<App />);
  await screen.findByRole("button", { name: "应急登录" });
  fireEvent.change(screen.getByLabelText("用户名"), {
    target: { value: "admin" },
  });
  fireEvent.change(screen.getByLabelText("密码"), {
    target: { value: "secret" },
  });
  fireEvent.click(screen.getByRole("button", { name: "应急登录" }));
  expect(await screen.findByText("应急管理员")).toBeInTheDocument();
});

test("平台角色按钮调用公开 API 并刷新", async () => {
  const session = {
    subject: "admin",
    auth_method: "emergency",
    roles: ["PLATFORM_ADMIN"],
  };
  const user = {
    id: "user-1",
    issuer: "issuer",
    subject: "alice",
    email: null,
    display_name: "Alice",
    version: 1,
    roles: [],
  };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(json(session))
    .mockResolvedValueOnce(json({ items: [] }))
    .mockResolvedValueOnce(json({ items: [] }))
    .mockResolvedValueOnce(json({ items: [user] }))
    .mockImplementation(() => Promise.resolve(json({ items: [] })));
  render(<App />);
  fireEvent.click(await screen.findByRole("button", { name: "授予管理员" }));
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(8));
  const roleRequest = fetchMock.mock.calls[4]?.[0];
  expect(roleRequest).toBeInstanceOf(Request);
  expect((roleRequest as Request).headers.get("if-match")).toBe('"1"');
});

test("管理 API 不可用时显示错误", async () => {
  vi.spyOn(globalThis, "fetch").mockRejectedValueOnce(new Error("offline"));
  render(<App />);
  expect(await screen.findByText("Admin API 暂时不可用")).toBeInTheDocument();
});

test("错误的应急凭据显示失败提示", async () => {
  vi.spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(json({}, 401))
    .mockResolvedValueOnce(
      json({ error: { code: "INVALID_CREDENTIALS" } }, 401),
    );
  render(<App />);
  await screen.findByRole("button", { name: "应急登录" });
  fireEvent.change(screen.getByLabelText("用户名"), {
    target: { value: "admin" },
  });
  fireEvent.change(screen.getByLabelText("密码"), {
    target: { value: "wrong" },
  });
  fireEvent.click(screen.getByRole("button", { name: "应急登录" }));
  expect(await screen.findByText("应急凭据无效")).toBeInTheDocument();
});

test("Agent 开发者通过公开 API 完成应用与 Draft 编辑校验闭环", async () => {
  const tenant = {
    id: "00000000-0000-0000-0000-000000000001",
    slug: "acme",
    name: "Acme",
    status: "ACTIVE",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const application = {
    id: "00000000-0000-0000-0000-000000000002",
    tenant_id: tenant.id,
    slug: "support-agent",
    name: "Support Agent",
    description: "",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const draft = {
    tenant_id: tenant.id,
    application_id: application.id,
    instructions: "Answer helpfully.",
    model_alias: "balanced",
    tool_aliases: ["search"],
    knowledge_refs: ["handbook"],
    governance_policy_ref: "standard-policy",
    lifecycle: "DRAFT",
    serves_production_traffic: false,
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input) => {
      const request = input as Request;
      const url = new URL(request.url);
      if (url.pathname === "/api/v1/auth/session") {
        return Promise.resolve(
          json({
            subject: "admin",
            auth_method: "emergency",
            roles: ["PLATFORM_ADMIN"],
          }),
        );
      }
      if (url.pathname === "/api/v1/tenants")
        return Promise.resolve(json({ items: [tenant] }));
      if (
        url.pathname === "/api/v1/tenant-groups" ||
        url.pathname === "/api/v1/platform-users"
      )
        return Promise.resolve(json({ items: [] }));
      if (
        url.pathname.endsWith("/agent-applications") &&
        request.method === "GET"
      )
        return Promise.resolve(json({ items: [] }));
      if (
        url.pathname.endsWith("/agent-applications") &&
        request.method === "POST"
      )
        return Promise.resolve(json(application, 201));
      if (url.pathname.endsWith("/draft") && request.method === "GET")
        return Promise.resolve(json({ error: { code: "NOT_FOUND" } }, 404));
      if (url.pathname.endsWith("/draft") && request.method === "PUT")
        return Promise.resolve(json(draft, 201));
      if (url.pathname.endsWith("/draft/validate"))
        return Promise.resolve(
          json({
            valid: false,
            draft_version: 1,
            issues: [
              {
                code: "DRAFT_MODEL_ALIAS_INVALID",
                path: "/model_alias",
                message: "Model alias must be a stable resource name.",
              },
            ],
          }),
        );
      if (request.method === "DELETE")
        return Promise.resolve(new Response(null, { status: 204 }));
      throw new Error(`Unexpected request: ${request.method} ${url.pathname}`);
    });

  render(<App />);
  await screen.findByRole("heading", { name: "租户与权限管理" });
  fireEvent.click(screen.getByRole("button", { name: "加载 Agent 应用" }));
  fireEvent.change(screen.getByLabelText("Agent 应用标识"), {
    target: { value: "support-agent" },
  });
  fireEvent.change(screen.getByLabelText("Agent 应用名称"), {
    target: { value: "Support Agent" },
  });
  fireEvent.click(screen.getByRole("button", { name: "创建 Agent 应用" }));
  expect(
    await screen.findByText("Support Agent (support-agent)"),
  ).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("Draft 指令"), {
    target: { value: "Answer helpfully." },
  });
  fireEvent.change(screen.getByLabelText("模型别名"), {
    target: { value: "balanced" },
  });
  fireEvent.change(screen.getByLabelText("工具别名"), {
    target: { value: "search" },
  });
  fireEvent.change(screen.getByLabelText("Knowledge 引用"), {
    target: { value: "handbook" },
  });
  fireEvent.change(screen.getByLabelText("治理策略引用"), {
    target: { value: "standard-policy" },
  });
  fireEvent.click(screen.getByRole("button", { name: "创建 Agent Draft" }));
  expect(
    await screen.findByText("Draft 版本 1 · 不承载生产流量"),
  ).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "校验 Agent Draft" }));
  expect(await screen.findByText("/model_alias")).toBeInTheDocument();
  expect(
    screen.getByText("Model alias must be a stable resource name."),
  ).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "删除 Agent Draft" }));
  fireEvent.click(screen.getByRole("button", { name: "删除 Agent 应用" }));
  await waitFor(() =>
    expect(screen.queryByText("Support Agent (support-agent)")).toBeNull(),
  );
  expect(
    fetchMock.mock.calls.every(([request]) =>
      new URL(
        request instanceof Request ? request.url : String(request),
      ).pathname.startsWith("/api/v1"),
    ),
  ).toBe(true);
});

test("Agent 编辑遇到并发版本冲突时提示重新加载", async () => {
  const tenant = {
    id: "00000000-0000-0000-0000-000000000001",
    slug: "acme",
    name: "Acme",
    status: "ACTIVE",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const application = {
    id: "00000000-0000-0000-0000-000000000002",
    tenant_id: tenant.id,
    slug: "support-agent",
    name: "Support Agent",
    description: "",
    version: 4,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input) => {
      const request = input as Request;
      const path = new URL(request.url).pathname;
      if (path === "/api/v1/auth/session")
        return Promise.resolve(
          json({
            subject: "admin",
            auth_method: "emergency",
            roles: ["PLATFORM_ADMIN"],
          }),
        );
      if (path === "/api/v1/tenants")
        return Promise.resolve(json({ items: [tenant] }));
      if (path === "/api/v1/tenant-groups" || path === "/api/v1/platform-users")
        return Promise.resolve(json({ items: [] }));
      if (path.endsWith("/agent-applications") && request.method === "GET")
        return Promise.resolve(json({ items: [application] }));
      if (path.endsWith("/draft") && request.method === "GET")
        return Promise.resolve(json({ error: { code: "NOT_FOUND" } }, 404));
      if (request.method === "PATCH")
        return Promise.resolve(
          json(
            {
              error: {
                code: "VERSION_MISMATCH",
                message: "Agent application version changed",
              },
            },
            412,
          ),
        );
      throw new Error(`Unexpected request: ${request.method} ${path}`);
    });

  render(<App />);
  await screen.findByRole("heading", { name: "租户与权限管理" });
  fireEvent.click(screen.getByRole("button", { name: "加载 Agent 应用" }));
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Support Agent (support-agent)",
    }),
  );
  fireEvent.change(await screen.findByLabelText("Agent 应用显示名称"), {
    target: { value: "Concurrent edit" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存 Agent 应用" }));

  expect(
    await screen.findByText("版本已变化，请重新加载后再编辑。"),
  ).toBeInTheDocument();
  const patch = fetchMock.mock.calls
    .map(([request]) => request as Request)
    .find((request) => request.method === "PATCH");
  expect(patch?.headers.get("if-match")).toBe('"4"');
});

test("现有 Agent 应用和 Draft 可继续编辑并通过校验", async () => {
  const tenant = {
    id: "00000000-0000-0000-0000-000000000001",
    slug: "acme",
    name: "Acme",
    status: "ACTIVE",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const application = {
    id: "00000000-0000-0000-0000-000000000002",
    tenant_id: tenant.id,
    slug: "support-agent",
    name: "Support Agent",
    description: "First version",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const draft = {
    tenant_id: tenant.id,
    application_id: application.id,
    instructions: "Answer helpfully.",
    model_alias: "balanced",
    tool_aliases: ["search"],
    knowledge_refs: ["handbook"],
    governance_policy_ref: null,
    lifecycle: "DRAFT",
    serves_production_traffic: false,
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input) => {
      const request = input as Request;
      const path = new URL(request.url).pathname;
      if (path === "/api/v1/auth/session")
        return Promise.resolve(
          json({
            subject: "admin",
            auth_method: "emergency",
            roles: ["PLATFORM_ADMIN"],
          }),
        );
      if (path === "/api/v1/tenants")
        return Promise.resolve(json({ items: [tenant] }));
      if (path === "/api/v1/tenant-groups" || path === "/api/v1/platform-users")
        return Promise.resolve(json({ items: [] }));
      if (path.endsWith("/agent-applications") && request.method === "GET")
        return Promise.resolve(json({ items: [application] }));
      if (
        path.endsWith("/agent-applications/" + application.id) &&
        request.method === "PATCH"
      )
        return Promise.resolve(
          json({
            ...application,
            name: "Support Copilot",
            description: "Second version",
            version: 2,
          }),
        );
      if (path.endsWith("/draft") && request.method === "GET")
        return Promise.resolve(json(draft));
      if (path.endsWith("/draft") && request.method === "PATCH")
        return Promise.resolve(
          json({ ...draft, instructions: "Answer concisely.", version: 2 }),
        );
      if (path.endsWith("/draft/validate"))
        return Promise.resolve(
          json({ valid: true, draft_version: 2, issues: [] }),
        );
      throw new Error(`Unexpected request: ${request.method} ${path}`);
    });

  render(<App />);
  await screen.findByRole("heading", { name: "租户与权限管理" });
  fireEvent.click(screen.getByRole("button", { name: "加载 Agent 应用" }));
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Support Agent (support-agent)",
    }),
  );
  expect(
    await screen.findByText("Draft 版本 1 · 不承载生产流量"),
  ).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("Agent 应用显示名称"), {
    target: { value: "Support Copilot" },
  });
  fireEvent.change(screen.getByLabelText("Agent 应用描述"), {
    target: { value: "Second version" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存 Agent 应用" }));
  expect(
    await screen.findByText("Support Copilot (support-agent)"),
  ).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("Draft 指令"), {
    target: { value: "Answer concisely." },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存 Agent Draft" }));
  expect(
    await screen.findByText("Draft 版本 2 · 不承载生产流量"),
  ).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "校验 Agent Draft" }));
  expect(await screen.findByText("Agent Draft 校验通过")).toBeInTheDocument();
  expect(fetchMock).toHaveBeenCalled();
});

test("快速切换 Agent 时不会让较慢的旧 Draft 覆盖当前选择", async () => {
  const tenant = {
    id: "00000000-0000-0000-0000-000000000001",
    slug: "acme",
    name: "Acme",
    status: "ACTIVE",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const applications = [
    {
      id: "00000000-0000-0000-0000-000000000002",
      tenant_id: tenant.id,
      slug: "slow-agent",
      name: "Slow Agent",
      description: "",
      version: 1,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    },
    {
      id: "00000000-0000-0000-0000-000000000003",
      tenant_id: tenant.id,
      slug: "fast-agent",
      name: "Fast Agent",
      description: "",
      version: 1,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    },
    {
      id: "00000000-0000-0000-0000-000000000004",
      tenant_id: tenant.id,
      slug: "failing-agent",
      name: "Failing Agent",
      description: "",
      version: 1,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    },
  ];
  const createdApplication = {
    id: "00000000-0000-0000-0000-000000000005",
    tenant_id: tenant.id,
    slug: "created-agent",
    name: "Created Agent",
    description: "",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const draft = (applicationId: string, instructions: string) => ({
    tenant_id: tenant.id,
    application_id: applicationId,
    instructions,
    model_alias: "balanced",
    tool_aliases: [],
    knowledge_refs: [],
    governance_policy_ref: null,
    lifecycle: "DRAFT",
    serves_production_traffic: false,
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  });
  let resolveSlowDraft!: (response: Response) => void;
  const slowDraft = new Promise<Response>((resolve) => {
    resolveSlowDraft = resolve;
  });
  let rejectFailingDraft!: (reason: Error) => void;
  const failingDraft = new Promise<Response>((_resolve, reject) => {
    rejectFailingDraft = reject;
  });

  vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
    const request = input as Request;
    const path = new URL(request.url).pathname;
    if (path === "/api/v1/auth/session")
      return Promise.resolve(
        json({
          subject: "admin",
          auth_method: "emergency",
          roles: ["PLATFORM_ADMIN"],
        }),
      );
    if (path === "/api/v1/tenants")
      return Promise.resolve(json({ items: [tenant] }));
    if (path === "/api/v1/tenant-groups" || path === "/api/v1/platform-users")
      return Promise.resolve(json({ items: [] }));
    if (path.endsWith("/agent-applications") && request.method === "GET")
      return Promise.resolve(json({ items: applications }));
    if (path.endsWith("/agent-applications") && request.method === "POST")
      return Promise.resolve(json(createdApplication, 201));
    if (path.includes(applications[0].id)) return slowDraft;
    if (path.includes(applications[1].id))
      return Promise.resolve(json(draft(applications[1].id, "Fast draft")));
    if (path.includes(applications[2].id)) return failingDraft;
    throw new Error(`Unexpected request: ${request.method} ${path}`);
  });

  render(<App />);
  await screen.findByRole("heading", { name: "租户与权限管理" });
  fireEvent.click(screen.getByRole("button", { name: "加载 Agent 应用" }));
  fireEvent.click(
    await screen.findByRole("button", { name: "Slow Agent (slow-agent)" }),
  );
  fireEvent.change(screen.getByLabelText("Agent 应用标识"), {
    target: { value: "created-agent" },
  });
  fireEvent.change(screen.getByLabelText("Agent 应用名称"), {
    target: { value: "Created Agent" },
  });
  fireEvent.click(screen.getByRole("button", { name: "创建 Agent 应用" }));
  expect(
    await screen.findByRole("button", {
      name: "Created Agent (created-agent)",
    }),
  ).toBeInTheDocument();
  resolveSlowDraft(json(draft(applications[0].id, "Slow draft")));
  await new Promise((resolve) => setTimeout(resolve, 0));
  await waitFor(() => {
    expect(screen.getByDisplayValue("Created Agent")).toBeInTheDocument();
    expect(screen.queryByDisplayValue("Slow draft")).toBeNull();
    expect(
      screen.getByRole("button", { name: "创建 Agent Draft" }),
    ).toBeInTheDocument();
  });

  fireEvent.click(
    screen.getByRole("button", { name: "Failing Agent (failing-agent)" }),
  );
  fireEvent.click(
    screen.getByRole("button", { name: "Fast Agent (fast-agent)" }),
  );
  expect(await screen.findByDisplayValue("Fast draft")).toBeInTheDocument();
  rejectFailingDraft(new Error("stale failure"));
  await waitFor(() => {
    expect(screen.getByDisplayValue("Fast draft")).toBeInTheDocument();
    expect(screen.queryByText("stale failure")).toBeNull();
  });
  fireEvent.click(
    screen.getByRole("button", { name: "Failing Agent (failing-agent)" }),
  );
  expect(await screen.findByText("stale failure")).toBeInTheDocument();
});
