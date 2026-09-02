import { expect, test } from "@playwright/test";

test("公开健康接口可通过 Web Console 查看", async ({ page, request }) => {
  const response = await request.get("/api/v1/health");

  expect(response.ok()).toBe(true);
  await expect(response.json()).resolves.toEqual({
    status: "ok",
    service: "admin-api",
    version: "0.1.0",
    trpc_agent_version: "1.1.19",
  });

  await page.goto("/");

  await expect(
    page.getByRole("heading", { name: "平台健康状态" }),
  ).toBeVisible();
  await expect(page.getByText("运行正常")).toBeVisible();
  await expect(page.getByText("tRPC-Agent 1.1.19")).toBeVisible();
});
