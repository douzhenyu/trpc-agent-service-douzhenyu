import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";

import { StorageProfilesWorkspace } from "./StorageProfilesWorkspace";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

test("控制台能保存专属高敏 Storage Profile 和 Worker Pool", async () => {
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
    alias: "restricted",
    classification: "RESTRICTED",
    worker_pool: "acme-restricted-workers",
    backends: [],
    active: true,
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response(JSON.stringify({ items: [] })))
    .mockResolvedValueOnce(
      new Response(JSON.stringify(profile), { status: 201 }),
    );

  render(<StorageProfilesWorkspace tenants={[tenant]} />);
  fireEvent.click(screen.getByRole("button", { name: "加载存储配置档" }));
  await screen.findByText("暂无存储配置档。");
  fireEvent.change(screen.getByLabelText("存储配置档别名"), {
    target: { value: "restricted" },
  });
  fireEvent.change(screen.getByLabelText("Worker Pool"), {
    target: { value: "acme-restricted-workers" },
  });
  fireEvent.change(screen.getByLabelText("存储隔离方式"), {
    target: { value: "DEDICATED" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存存储配置档" }));

  expect(
    await screen.findByText("restricted → acme-restricted-workers"),
  ).toBeInTheDocument();
  const request = fetchMock.mock.calls[1]?.[0];
  expect(
    new URL(request instanceof Request ? request.url : String(request))
      .pathname,
  ).toMatch(/^\/api\/v1\/tenants\/.*\/storage-profiles$/);
});
