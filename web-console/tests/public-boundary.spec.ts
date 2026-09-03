import { expect, test } from "@playwright/test";

test("应急管理员可通过 Web Console 完成租户管理闭环", async ({
  page,
  request,
}) => {
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
    page.getByRole("heading", { name: "平台管理登录" }),
  ).toBeVisible();
  await page.getByLabel("用户名").fill("emergency-admin");
  await page.getByLabel("密码").fill("correct-horse");
  await page.getByRole("button", { name: "应急登录" }).click();
  await expect(
    page.getByRole("heading", { name: "租户与权限管理" }),
  ).toBeVisible();

  const slug = `smoke-${Date.now()}`;
  await page.getByLabel("租户标识").fill(slug);
  await page.getByLabel("租户名称").fill("Smoke Tenant");
  await page.getByRole("button", { name: "创建租户" }).click();
  await expect(page.getByText(`Smoke Tenant (${slug})`)).toBeVisible();
});

test("开发 Fake 可编排外部限流并恢复默认状态", async ({ request }) => {
  const fakeUrl = process.env.FAKE_EXTERNAL_URL ?? "http://127.0.0.1:8090";

  await request.post(`${fakeUrl}/control/v1/reset`);
  const configure = await request.post(`${fakeUrl}/control/v1/scenarios`, {
    data: { llm: "rate_limit" },
  });
  expect(configure.ok()).toBe(true);

  const limited = await request.post(`${fakeUrl}/llm/v1/chat/completions`, {
    data: { model: "fake-model", messages: [] },
  });
  expect(limited.status()).toBe(429);
  await expect(limited.json()).resolves.toEqual({ error: "rate_limit" });

  const reset = await request.post(`${fakeUrl}/control/v1/reset`);
  expect(reset.ok()).toBe(true);
});
