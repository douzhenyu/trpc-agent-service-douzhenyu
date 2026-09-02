export type HealthResponse = {
  status: "ok";
  service: "admin-api";
  version: string;
  trpc_agent_version: string;
};

export async function getHealth(): Promise<HealthResponse> {
  const response = await fetch("/api/v1/health", {
    headers: { Accept: "application/json" },
  });

  if (!response.ok) {
    throw new Error(`Admin API health request failed with ${response.status}`);
  }

  return (await response.json()) as HealthResponse;
}
