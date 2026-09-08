import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";

import { OpsConsole } from "./OpsConsole";

const tenant = {
  id: "00000000-0000-0000-0000-000000000001",
  slug: "acme",
  name: "Acme",
  status: "ACTIVE",
  version: 1,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
} as const;

const session = {
  id: "s-0",
  tenant_id: tenant.id,
  application_id: "00000000-0000-0000-0000-000000000002",
  version: 1,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T01:00:00Z",
};

const deadLetter = {
  delivery_id: "00000000-0000-0000-0000-000000000003",
  tenant_id: tenant.id,
  binding_id: "00000000-0000-0000-0000-000000000004",
  execution_id: "exec-1",
  external_conversation_id: "oc_1",
  attempts: 4,
  last_error: "PROVIDER_TIMEOUT",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T02:00:00Z",
};

const operation = {
  kind: "CONTENT_DELETION",
  id: "00000000-0000-0000-0000-000000000005",
  status: "RETRYABLE",
  attempts: 2,
  next_action_at: "2026-01-01T03:00:00Z",
  last_error: "DELETION_BACKEND_RETRYABLE",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T02:00:00Z",
  evidence: { proofs: [] },
};

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    headers: { "Content-Type": "application/json" },
  });
}

function mockFetch(postPaths: string[] = []) {
  return vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const request =
      input instanceof Request ? input : new Request(String(input));
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.method === "POST") {
      expect(postPaths).toContain(path);
      return jsonResponse({
        delivery_id: deadLetter.delivery_id,
        status: "QUEUED",
      });
    }
    const paged = { items: [], next_cursor: null };
    if (path.endsWith("/ops/sessions")) {
      return jsonResponse({ ...paged, items: [session] });
    }
    if (path.endsWith("/ops/dead-letters")) {
      if (url.searchParams.get("cursor")) {
        return jsonResponse({
          items: [
            {
              ...deadLetter,
              delivery_id: "00000000-0000-0000-0000-000000000006",
              last_error: "SECOND_PAGE",
            },
          ],
          next_cursor: null,
        });
      }
      return jsonResponse({ items: [deadLetter], next_cursor: "cursor-1" });
    }
    if (path.endsWith("/ops/operations")) {
      return jsonResponse({ ...paged, items: [operation] });
    }
    if (path.endsWith("/audit-events")) {
      return jsonResponse({ events: [] });
    }
    if (path.endsWith("/tool-approvals")) {
      return jsonResponse({ approvals: [] });
    }
    return jsonResponse(paged);
  });
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

test("运营工作台集中展示会话、死信与异步操作状态", async () => {
  mockFetch();
  render(<OpsConsole tenants={[tenant]} />);

  expect(screen.getByText("统一运营工作台")).toBeInTheDocument();
  await screen.findByText("s-0");
  await screen.findByText("PROVIDER_TIMEOUT");
  await screen.findByText("RETRYABLE");
  await screen.findByText("死信队列");
  await screen.findByText("异步操作");
  expect((await screen.findAllByText("加载更多")).length).toBeGreaterThan(0);
});

test("死信重试通过 Admin API 提交并刷新列表", async () => {
  const fetchMock = mockFetch([
    `/api/v1/tenants/${tenant.id}/ops/dead-letters/${deadLetter.delivery_id}/retries`,
  ]);
  render(<OpsConsole tenants={[tenant]} />);
  await screen.findByText("PROVIDER_TIMEOUT");

  const retryButton = screen.getByRole("button", { name: /重试/ });
  retryButton.click();

  await vi.waitFor(() => {
    const calls = fetchMock.mock.calls.filter((call) => {
      const request =
        call[0] instanceof Request ? call[0] : new Request(String(call[0]));
      return request.method === "POST";
    });
    expect(calls).toHaveLength(1);
  });
});

test("运营工作台为死信队列加载下一页并追加结果", async () => {
  const fetchMock = mockFetch();
  render(<OpsConsole tenants={[tenant]} />);
  await screen.findByText("PROVIDER_TIMEOUT");

  const deadLetterSection = screen
    .getByRole("heading", { name: "死信队列" })
    .closest("section");
  expect(deadLetterSection).not.toBeNull();
  fireEvent.click(
    within(deadLetterSection!).getByRole("button", { name: "加载更多" }),
  );

  await vi.waitFor(() => {
    const paginatedRequests = fetchMock.mock.calls.filter((call) => {
      const request =
        call[0] instanceof Request ? call[0] : new Request(String(call[0]));
      const url = new URL(request.url);
      return (
        url.pathname.endsWith("/ops/dead-letters") &&
        url.searchParams.get("cursor") === "cursor-1" &&
        url.searchParams.get("limit") === "20"
      );
    });
    expect(paginatedRequests).toHaveLength(1);
  });
  expect(await screen.findByText("SECOND_PAGE")).toBeInTheDocument();
});

test("运营工作台在没有可选租户时不发起请求", async () => {
  const fetchMock = mockFetch();
  render(<OpsConsole tenants={[]} />);

  expect(await screen.findAllByText("暂无数据")).not.toHaveLength(0);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("运营工作台显示加载请求错误", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const request =
      input instanceof Request ? input : new Request(String(input));
    if (request.method === "POST") throw new Error("重试服务不可用");
    if (new URL(request.url).pathname.endsWith("/ops/dead-letters")) {
      return jsonResponse({ items: [deadLetter], next_cursor: null });
    }
    throw new Error("加载服务不可用");
  });
  render(<OpsConsole tenants={[tenant]} />);

  expect(await screen.findByText("加载服务不可用")).toBeInTheDocument();
});
