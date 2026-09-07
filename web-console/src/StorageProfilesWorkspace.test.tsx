import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";

import { StorageProfilesWorkspace } from "./StorageProfilesWorkspace";

const tenant = {
  id: "00000000-0000-0000-0000-000000000001",
  slug: "acme",
  name: "Acme",
  status: "ACTIVE",
  version: 1,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
} as const;

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

test("控制台能保存专属高敏 Storage Profile 和 Worker Pool", async () => {
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

test("控制台可选用外部 Memory，并提交自定义后端与密钥引用", async () => {
  const profile = {
    id: "00000000-0000-0000-0000-000000000003",
    tenant_id: tenant.id,
    alias: "with-memory",
    classification: "INTERNAL",
    worker_pool: "shared-workers",
    encryption_key_ref: "vault://tenant/acme/storage#key",
    backends: [],
    active: true,
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(
      new Response(JSON.stringify(profile), { status: 201 }),
    );

  render(<StorageProfilesWorkspace tenants={[tenant]} />);
  fireEvent.click(screen.getByLabelText("启用外部 Memory"));
  fireEvent.change(screen.getByLabelText("存储配置档别名"), {
    target: { value: "with-memory" },
  });
  fireEvent.change(screen.getByLabelText("数据加密密钥引用"), {
    target: { value: "vault://tenant/acme/storage#key" },
  });
  fireEvent.change(screen.getByLabelText("SQL Endpoint"), {
    target: { value: "https://sql.acme.test" },
  });
  fireEvent.change(screen.getByLabelText("SQL 密钥引用"), {
    target: { value: "vault://tenant/acme/storage#sql" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存存储配置档" }));

  await screen.findByText("with-memory → shared-workers");
  const request = fetchMock.mock.calls[0]?.[0] as Request;
  const payload = (await request.json()) as {
    encryption_key_ref: string;
    backends: Array<{ kind: string; endpoint: string; secret_ref: string }>;
  };
  expect(payload.encryption_key_ref).toBe("vault://tenant/acme/storage#key");
  expect(payload.backends).toContainEqual({
    kind: "SQL",
    endpoint: "https://sql.acme.test",
    dedicated: false,
    secret_ref: "vault://tenant/acme/storage#sql",
  });
  expect(payload.backends.map((backend) => backend.kind)).toContain(
    "EXTERNAL_MEMORY",
  );
});

test("控制台显示读取失败和高敏隔离校验错误", async () => {
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ error: { message: "无法读取配置档" } }), {
        status: 500,
      }),
    )
    .mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          error: { code: "DEDICATED_STORAGE_REQUIRED", message: "rejected" },
        }),
        { status: 422 },
      ),
    );

  render(<StorageProfilesWorkspace tenants={[tenant]} />);
  fireEvent.click(screen.getByRole("button", { name: "加载存储配置档" }));
  await screen.findByText("无法读取配置档");
  fireEvent.change(screen.getByLabelText("存储配置档别名"), {
    target: { value: "restricted" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存存储配置档" }));
  await screen.findByText("高敏存储必须使用专属后端和专属 Worker Pool。");
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

test("没有租户时不展示配置表单", () => {
  render(<StorageProfilesWorkspace tenants={[]} />);

  expect(screen.getByText("请先创建租户。")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "保存存储配置档" })).toBeNull();
});
