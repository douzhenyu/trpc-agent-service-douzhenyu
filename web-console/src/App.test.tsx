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
