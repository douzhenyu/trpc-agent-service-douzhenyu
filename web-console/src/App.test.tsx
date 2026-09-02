import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";

import App from "./App";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

test("平台用户可看到 Admin API 的公开健康状态", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(
      JSON.stringify({
        status: "ok",
        service: "admin-api",
        version: "0.1.0",
        trpc_agent_version: "1.1.19",
      }),
      { status: 200, headers: { "Content-Type": "application/json" } },
    ),
  );

  render(<App />);

  expect(
    screen.getByRole("heading", { name: "平台健康状态" }),
  ).toBeInTheDocument();
  expect(await screen.findByText("运行正常")).toBeInTheDocument();
  expect(screen.getByText("Admin API")).toBeInTheDocument();
  expect(screen.getByText("tRPC-Agent 1.1.19")).toBeInTheDocument();
  expect(fetch).toHaveBeenCalledOnce();
});

test("Admin API 不健康时展示明确的不可用状态", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify({ error: "unavailable" }), {
      status: 503,
      headers: { "Content-Type": "application/json" },
    }),
  );

  render(<App />);

  expect(await screen.findByText("Admin API 暂时不可用")).toBeInTheDocument();
  expect(screen.queryByText("运行正常")).not.toBeInTheDocument();
});
