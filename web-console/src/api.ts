import createClient from "openapi-fetch";

import type { components, paths } from "./generated/admin-api";

export type HealthResponse = components["schemas"]["HealthResponse"];

const client = createClient<paths>({
  baseUrl: globalThis.location.origin,
  fetch: (request) => globalThis.fetch(request),
});

export async function getHealth(): Promise<HealthResponse> {
  const { data, error, response } = await client.GET("/api/v1/health");

  if (!response.ok || error || !data) {
    throw new Error(`Admin API health request failed with ${response.status}`);
  }

  return data;
}
