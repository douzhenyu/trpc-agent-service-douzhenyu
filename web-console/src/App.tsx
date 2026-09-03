import { useEffect, useState } from "react";

import { getHealth, type HealthResponse } from "./api";
import { t } from "./i18n";
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
        <p className="eyebrow">{t("productName")}</p>
        <h1>{t("healthTitle")}</h1>

        {state.kind === "loading" && (
          <p className="status status--loading">{t("checking")}</p>
        )}

        {state.kind === "error" && (
          <p className="status status--error">{t("unavailable")}</p>
        )}

        {state.kind === "ready" && (
          <div className="health-card">
            <div>
              <span className="status-dot" aria-hidden="true" />
              <strong>{t("healthy")}</strong>
            </div>
            <dl>
              <div>
                <dt>{t("service")}</dt>
                <dd>{t("adminApi")}</dd>
              </div>
              <div>
                <dt>{t("platformVersion")}</dt>
                <dd>{state.health.version}</dd>
              </div>
              <div>
                <dt>{t("sdk")}</dt>
                <dd>tRPC-Agent {state.health.trpc_agent_version}</dd>
              </div>
            </dl>
          </div>
        )}
      </section>
    </main>
  );
}
