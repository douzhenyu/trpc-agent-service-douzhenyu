import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";

import { ModelProfilesWorkspace } from "./ModelProfilesWorkspace";

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

test("租户管理员只通过公开 API 保存和查看模型 secret_ref", async () => {
  const tenant = {
    id: "00000000-0000-0000-0000-000000000001",
    slug: "acme",
    name: "Acme",
    status: "ACTIVE",
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const profile = {
    id: "00000000-0000-0000-0000-000000000002",
    tenant_id: tenant.id,
    alias: "balanced",
    provider_model: "fake-balanced",
    endpoint_url: "http://fake-llm.internal/v1/chat/completions",
    secret_ref: `vault://tenant/${tenant.id}/llm/balanced#api_key`,
    data_classification: "CONFIDENTIAL",
    region: "cn-north-1",
    fallback_aliases: [],
    requests_per_minute: 60,
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(json({ items: [], next_cursor: null }))
    .mockResolvedValueOnce(json(profile, 201));

  render(<ModelProfilesWorkspace tenants={[tenant]} />);
  fireEvent.click(screen.getByRole("button", { name: "加载模型配置档" }));
  await screen.findByText("暂无模型配置档。");
  fireEvent.change(screen.getByLabelText("模型配置档别名"), {
    target: { value: "balanced" },
  });
  fireEvent.change(screen.getByLabelText("供应商模型"), {
    target: { value: "fake-balanced" },
  });
  fireEvent.change(screen.getByLabelText("模型 Endpoint"), {
    target: { value: "http://fake-llm.internal/v1/chat/completions" },
  });
  fireEvent.change(screen.getByLabelText("密钥引用"), {
    target: { value: profile.secret_ref },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存模型配置档" }));

  expect(
    await screen.findByText("balanced → fake-balanced"),
  ).toBeInTheDocument();
  expect(screen.getByText(profile.secret_ref)).toBeInTheDocument();
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  for (const [request] of fetchMock.mock.calls) {
    expect(
      new URL(request instanceof Request ? request.url : String(request))
        .pathname,
    ).toMatch(/^\/api\/v1\/tenants\/.*\/model-profiles$/);
  }
});
