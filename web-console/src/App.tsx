import { useEffect, useState } from "react";

import { getHealth, type HealthResponse } from "./api";
import "./styles.css";

type ViewState =
  | { kind: "loading" }
  | { kind: "ready"; health: HealthResponse }
  | { kind: "error" };

export default function App() {
  const [state, setState] = useState<ViewState>({ kind: "loading" });

  useEffect(() => {
    let active = true;

    getHealth()
      .then((health) => {
        if (active) setState({ kind: "ready", health });
      })
      .catch(() => {
        if (active) setState({ kind: "error" });
      });

    return () => {
      active = false;
    };
  }, []);

  return (
    <main className="shell">
      <section className="health-panel" aria-live="polite">
        <p className="eyebrow">tRPC-Agent 多租户平台</p>
        <h1>平台健康状态</h1>

        {state.kind === "loading" && (
          <p className="status status--loading">正在检查…</p>
        )}

        {state.kind === "error" && (
          <p className="status status--error">Admin API 暂时不可用</p>
        )}

        {state.kind === "ready" && (
          <div className="health-card">
            <div>
              <span className="status-dot" aria-hidden="true" />
              <strong>运行正常</strong>
            </div>
            <dl>
              <div>
                <dt>服务</dt>
                <dd>Admin API</dd>
              </div>
              <div>
                <dt>平台版本</dt>
                <dd>{state.health.version}</dd>
              </div>
              <div>
                <dt>SDK</dt>
                <dd>tRPC-Agent {state.health.trpc_agent_version}</dd>
              </div>
            </dl>
          </div>
        )}
      </section>
    </main>
  );
}
