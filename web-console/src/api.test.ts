import { afterEach, expect, test, vi } from "vitest";

import {
  getAuditEvents,
  getBudgets,
  getKnowledgeBases,
  getStorageMigrations,
  getToolApprovals,
  listOpsArtifacts,
  listOpsOperations,
  listOpsSessions,
} from "./api";

const tenantId = "00000000-0000-0000-0000-000000000001";

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function requestFromCall(call: unknown[]): Request {
  const input = call[0];
  return input instanceof Request ? input : new Request(String(input));
}

afterEach(() => {
  vi.restoreAllMocks();
});

test("运营 API 使用分页默认值，并把缺失的可选列表规范为空数组", async () => {
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async () =>
      jsonResponse({ items: [], next_cursor: null }),
    );

  await expect(listOpsSessions(tenantId)).resolves.toEqual({
    items: [],
    next_cursor: null,
  });
  await expect(listOpsOperations(tenantId)).resolves.toEqual([]);
  await expect(getToolApprovals(tenantId)).resolves.toEqual([]);
  await expect(getAuditEvents(tenantId)).resolves.toEqual([]);
  await expect(getBudgets(tenantId)).resolves.toEqual([]);
  await expect(getStorageMigrations(tenantId)).resolves.toEqual([]);
  await expect(getKnowledgeBases(tenantId)).resolves.toEqual([]);

  const request = requestFromCall(fetchMock.mock.calls[0]);
  expect(request.url).toContain(
    `/api/v1/tenants/${tenantId}/ops/sessions?limit=50`,
  );
});

test("运营 API 保留显式分页游标，并返回结构化错误", async () => {
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(jsonResponse({ items: [], next_cursor: null }))
    .mockResolvedValueOnce(
      jsonResponse(
        { error: { code: "OPS_UNAVAILABLE", message: "操作服务不可用" } },
        503,
      ),
    );

  await listOpsSessions(tenantId, { cursor: "next-page", limit: 10 });
  const paginatedRequest = requestFromCall(fetchMock.mock.calls[0]);
  expect(paginatedRequest.url).toContain("cursor=next-page");
  expect(paginatedRequest.url).toContain("limit=10");

  await expect(listOpsArtifacts(tenantId)).rejects.toEqual(
    expect.objectContaining({
      code: "OPS_UNAVAILABLE",
      message: "操作服务不可用",
      status: 503,
    }),
  );
});

test("运营 API 为每个聚合资源传播服务错误", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
    jsonResponse({ error: { message: "读取失败" } }, 503),
  );

  await expect(getAuditEvents(tenantId)).rejects.toThrow("读取失败");
  await expect(getBudgets(tenantId)).rejects.toThrow("读取失败");
  await expect(getStorageMigrations(tenantId)).rejects.toThrow("读取失败");
  await expect(getKnowledgeBases(tenantId)).rejects.toThrow("读取失败");
});
