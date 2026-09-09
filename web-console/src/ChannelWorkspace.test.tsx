import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";

import { ChannelWorkspace } from "./ChannelWorkspace";
import type { Tenant } from "./api";

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

const tenant: Tenant = {
  id: "00000000-0000-0000-0000-000000000001",
  slug: "acme",
  name: "Acme",
  status: "ACTIVE",
  version: 1,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

const application = {
  id: "00000000-0000-0000-0000-000000000010",
  tenant_id: tenant.id,
  slug: "support",
  name: "客服助手",
  description: "",
  version: 1,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

const wecomBinding = {
  binding_id: "00000000-0000-0000-0000-000000000020",
  tenant_id: tenant.id,
  channel_type: "WECOM",
  external_bot_id: "wx-bot-1",
  application_id: application.id,
  environment: "DEVELOPMENT",
  secret_ref: `vault://tenant/${tenant.id}/channels/wecom#credential`,
  status: "ACTIVE",
};

test("可加载、创建并停用企微与飞书通道，且不回显密钥片段", async () => {
  let createdPayload: Record<string, unknown> | null = null;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const request =
      input instanceof Request ? input : new Request(String(input));
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path.endsWith("/channel-bindings"))
      return json({ tenant_id: tenant.id, bindings: [wecomBinding] });
    if (request.method === "GET" && path.endsWith("/agent-applications"))
      return json({ items: [application], next_cursor: null });
    if (request.method === "PUT" && path.endsWith("/channel-bindings")) {
      const body = (await request.json()) as Record<string, unknown>;
      createdPayload = body;
      return json(
        {
          ...body,
          binding_id: "00000000-0000-0000-0000-000000000021",
          tenant_id: tenant.id,
          status: "ACTIVE",
        },
        201,
      );
    }
    if (request.method === "POST" && path.endsWith("/status-changes"))
      return json({ ...wecomBinding, status: "DISABLED" });
    return json({ error: { code: "NOT_FOUND" } }, 404);
  });

  render(<ChannelWorkspace tenants={[tenant]} />);

  expect(await screen.findByText("wx-bot-1")).toBeInTheDocument();
  expect(screen.getByText(/#••••••$/)).toBeInTheDocument();
  expect(screen.queryByText(/#credential$/)).toBeNull();
  expect(screen.getByLabelText("通道状态摘要")).toHaveTextContent("1 已登记");
  expect(screen.getByLabelText("通道状态摘要")).toHaveTextContent("1 运行中");
  expect(
    screen.getByRole("button", { name: "复制回调 URL" }),
  ).toBeInTheDocument();
  expect(screen.getByText("#encoding-aes-key")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("radio", { name: /飞书/ }));
  expect(screen.getByText(/官方 SDK 建立长连接/)).toBeInTheDocument();
  expect(
    screen.getByRole("button", { name: "复制长连接声明" }),
  ).toBeInTheDocument();
  expect(screen.getByText("#app-secret")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("App ID"), {
    target: { value: "cli_feishu_1" },
  });
  fireEvent.change(screen.getByLabelText("通道运行环境"), {
    target: { value: "STAGING" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存通道配置" }));

  expect(await screen.findByText("cli_feishu_1")).toBeInTheDocument();
  expect(screen.getByRole("status")).toHaveTextContent("飞书通道已登记");
  expect(createdPayload).toMatchObject({
    channel_type: "FEISHU",
    application_id: application.id,
    environment: "STAGING",
  });

  fireEvent.click(screen.getAllByRole("button", { name: "停用" })[0]);
  await waitFor(() => expect(screen.getByText("已停用")).toBeInTheDocument());
});

test("无租户时给出下一步指引", () => {
  const fetchMock = vi.spyOn(globalThis, "fetch");
  render(<ChannelWorkspace tenants={[]} />);
  expect(
    screen.getByText("请先创建租户和 Agent 应用，再配置 IM 通道。"),
  ).toBeInTheDocument();
  expect(fetchMock).not.toHaveBeenCalled();
});

test("通道冲突和密钥引用错误使用可操作提示", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const request =
      input instanceof Request ? input : new Request(String(input));
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path.endsWith("/channel-bindings"))
      return json({ tenant_id: tenant.id, bindings: [] });
    if (request.method === "GET" && path.endsWith("/agent-applications"))
      return json({ items: [application], next_cursor: null });
    return json(
      {
        error: {
          code: "CHANNEL_BINDING_CONFLICT",
          message: "CHANNEL_BINDING_CONFLICT",
        },
      },
      409,
    );
  });

  render(<ChannelWorkspace tenants={[tenant]} />);
  await screen.findByRole("button", { name: "保存通道配置" });
  fireEvent.change(screen.getByLabelText(/智能机器人 Bot ID/), {
    target: { value: "duplicate-bot" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存通道配置" }));
  expect(
    await screen.findByText("该机器人已绑定，请更换机器人 ID 或先停用原绑定。"),
  ).toBeInTheDocument();
});

test("可重新启用已停用通道", async () => {
  const disabled = { ...wecomBinding, status: "DISABLED" };
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const request =
      input instanceof Request ? input : new Request(String(input));
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path.endsWith("/channel-bindings"))
      return json({ tenant_id: tenant.id, bindings: [disabled] });
    if (request.method === "GET" && path.endsWith("/agent-applications"))
      return json({ items: [application], next_cursor: null });
    if (request.method === "POST" && path.endsWith("/status-changes"))
      return json({ ...disabled, status: "ACTIVE" });
    return json({}, 404);
  });

  render(<ChannelWorkspace tenants={[tenant]} />);
  fireEvent.click(await screen.findByRole("button", { name: "启用" }));
  await waitFor(() => expect(screen.getByText("运行中")).toBeInTheDocument());
});

test("加载失败与不安全密钥引用均提供明确提示", async () => {
  const fetchMock = vi.spyOn(globalThis, "fetch");
  fetchMock.mockResolvedValueOnce(
    json(
      { error: { code: "INTERNAL_ERROR", message: "backend unavailable" } },
      500,
    ),
  );
  fetchMock.mockResolvedValueOnce(
    json({ items: [application], next_cursor: null }),
  );

  const view = render(<ChannelWorkspace tenants={[tenant]} />);
  expect(await screen.findByText("backend unavailable")).toBeInTheDocument();
  view.unmount();

  fetchMock.mockReset().mockImplementation(async (input) => {
    const request =
      input instanceof Request ? input : new Request(String(input));
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path.endsWith("/channel-bindings"))
      return json({ tenant_id: tenant.id, bindings: [] });
    if (request.method === "GET" && path.endsWith("/agent-applications"))
      return json({ items: [application], next_cursor: null });
    return json(
      {
        error: { code: "SECRET_REF_REJECTED", message: "SECRET_REF_REJECTED" },
      },
      422,
    );
  });

  render(<ChannelWorkspace tenants={[tenant]} />);
  await screen.findByRole("button", { name: "保存通道配置" });
  fireEvent.change(screen.getByLabelText(/智能机器人 Bot ID/), {
    target: { value: "unsafe-secret-bot" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存通道配置" }));
  expect(
    await screen.findByText(
      "密钥引用不符合规范，请使用 vault://tenant/... 路径。",
    ),
  ).toBeInTheDocument();
});

test("未知测试通道和未找到的 Agent 使用稳定回退显示", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const request =
      input instanceof Request ? input : new Request(String(input));
    const path = new URL(request.url).pathname;
    if (path.endsWith("/channel-bindings"))
      return json({
        tenant_id: tenant.id,
        bindings: [
          {
            ...wecomBinding,
            channel_type: "FAKE",
            application_id: "00000000-0000-0000-0000-000000000099",
          },
        ],
      });
    return json({ items: [], next_cursor: null });
  });

  render(<ChannelWorkspace tenants={[tenant]} />);
  expect(await screen.findByText("FAKE")).toBeInTheDocument();
  expect(
    screen.getByText("00000000-0000-0000-0000-000000000099"),
  ).toBeInTheDocument();
  expect(screen.getByText(/当前租户暂无 Agent 应用/)).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "保存通道配置" })).toBeDisabled();
});
