import { FormEvent, useEffect, useState } from "react";

import {
  assignRole,
  createGroup,
  createTenant,
  emergencyLogin,
  getGroups,
  getSession,
  getTenants,
  getUsers,
  type PlatformUser,
  type Session,
  type Tenant,
  type TenantGroup,
} from "./api";
import { AgentWorkspace } from "./AgentWorkspace";
import "./styles.css";

type State =
  | { kind: "loading" }
  | { kind: "anonymous"; error?: string }
  | {
      kind: "ready";
      session: Session;
      tenants: Tenant[];
      groups: TenantGroup[];
      users: PlatformUser[];
    }
  | { kind: "error"; message: string };

export default function App() {
  const [state, setState] = useState<State>({ kind: "loading" });
  const [slug, setSlug] = useState("");
  const [tenantName, setTenantName] = useState("");
  const [groupName, setGroupName] = useState("");
  const [selected, setSelected] = useState<string[]>([]);

  async function load(session?: Session) {
    const active = session ?? (await getSession());
    if (!active) {
      setState({ kind: "anonymous" });
      return;
    }
    const platformReader = active.roles.some((role) =>
      ["PLATFORM_ADMIN", "PLATFORM_AUDITOR"].includes(role),
    );
    const [tenants, groups, users] = await Promise.all([
      getTenants(),
      platformReader ? getGroups() : Promise.resolve([]),
      platformReader ? getUsers() : Promise.resolve([]),
    ]);
    setState({ kind: "ready", session: active, tenants, groups, users });
  }

  useEffect(() => {
    load().catch(() =>
      setState({ kind: "error", message: "Admin API 暂时不可用" }),
    );
  }, []);

  async function onEmergency(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    try {
      await load(
        await emergencyLogin(
          String(form.get("username")),
          String(form.get("password")),
        ),
      );
    } catch {
      setState({ kind: "anonymous", error: "应急凭据无效" });
    }
  }

  async function onTenant(event: FormEvent) {
    event.preventDefault();
    if (state.kind !== "ready") return;
    const tenant = await createTenant(slug, tenantName);
    setState({ ...state, tenants: [...state.tenants, tenant] });
    setSlug("");
    setTenantName("");
  }

  async function onGroup(event: FormEvent) {
    event.preventDefault();
    if (state.kind !== "ready") return;
    const group = await createGroup(groupName, selected);
    setState({ ...state, groups: [...state.groups, group] });
    setGroupName("");
    setSelected([]);
  }

  if (state.kind === "loading")
    return (
      <main className="shell">
        <p>正在加载管理控制台…</p>
      </main>
    );
  if (state.kind === "error")
    return (
      <main className="shell">
        <p className="status status--error">{state.message}</p>
      </main>
    );
  if (state.kind === "anonymous")
    return (
      <main className="shell">
        <section className="login-card">
          <p className="eyebrow">tRPC-Agent 多租户平台</p>
          <h1>平台管理登录</h1>
          <a className="primary-link" href="/api/v1/auth/oidc/login">
            使用企业账号登录
          </a>
          <div className="divider">应急访问</div>
          <form onSubmit={onEmergency} className="stack">
            <label>
              用户名
              <input name="username" autoComplete="username" required />
            </label>
            <label>
              密码
              <input
                name="password"
                type="password"
                autoComplete="current-password"
                required
              />
            </label>
            <button type="submit">应急登录</button>
          </form>
          {state.error && <p className="status status--error">{state.error}</p>}
        </section>
      </main>
    );

  const platformReader = state.session.roles.some((role) =>
    ["PLATFORM_ADMIN", "PLATFORM_AUDITOR"].includes(role),
  );

  return (
    <main className="console-shell">
      <header>
        <div>
          <p className="eyebrow">tRPC-Agent 多租户平台</p>
          <h1>租户与权限管理</h1>
        </div>
        <span>
          {state.session.auth_method === "emergency"
            ? "应急管理员"
            : state.session.subject}
        </span>
      </header>
      <div className="management-grid">
        {platformReader && (
          <>
            <section className="panel">
              <h2>租户</h2>
              <form onSubmit={onTenant} className="stack">
                <label>
                  租户标识
                  <input
                    aria-label="租户标识"
                    value={slug}
                    onChange={(event) => setSlug(event.target.value)}
                    required
                    pattern="[a-z0-9][a-z0-9-]+"
                  />
                </label>
                <label>
                  租户名称
                  <input
                    aria-label="租户名称"
                    value={tenantName}
                    onChange={(event) => setTenantName(event.target.value)}
                    required
                  />
                </label>
                <button type="submit">创建租户</button>
              </form>
              <ul>
                {state.tenants.map((tenant) => (
                  <li key={tenant.id}>
                    {tenant.name} ({tenant.slug})
                  </li>
                ))}
              </ul>
            </section>

            <section className="panel">
              <h2>Tenant Group</h2>
              <form onSubmit={onGroup} className="stack">
                <label>
                  Tenant Group 名称
                  <input
                    aria-label="Tenant Group 名称"
                    value={groupName}
                    onChange={(event) => setGroupName(event.target.value)}
                    required
                  />
                </label>
                <fieldset>
                  <legend>成员租户</legend>
                  {state.tenants.map((tenant) => (
                    <label key={tenant.id} className="check">
                      <input
                        type="checkbox"
                        aria-label={tenant.name}
                        checked={selected.includes(tenant.id)}
                        onChange={() =>
                          setSelected(
                            selected.includes(tenant.id)
                              ? selected.filter((id) => id !== tenant.id)
                              : [...selected, tenant.id],
                          )
                        }
                      />
                      {tenant.name}
                    </label>
                  ))}
                </fieldset>
                <button type="submit">创建 Tenant Group</button>
              </form>
              <ul>
                {state.groups.map((group) => (
                  <li key={group.id}>
                    {group.name} · {group.tenant_ids.length} 个租户
                  </li>
                ))}
              </ul>
            </section>

            <section className="panel panel--wide">
              <h2>平台角色</h2>
              {state.users.length === 0 ? (
                <p className="muted">OIDC 用户登录后会出现在这里。</p>
              ) : (
                <ul>
                  {state.users.map((user) => (
                    <li key={user.id} className="role-row">
                      <span>
                        {user.display_name}{" "}
                        <small>{user.roles.join(" · ") || "无角色"}</small>
                      </span>
                      <button
                        onClick={() =>
                          assignRole(
                            user.id,
                            "PLATFORM_ADMIN",
                            user.version,
                          ).then(() => load(state.session))
                        }
                      >
                        授予管理员
                      </button>
                      <button
                        onClick={() =>
                          assignRole(
                            user.id,
                            "PLATFORM_AUDITOR",
                            user.version,
                          ).then(() => load(state.session))
                        }
                      >
                        授予审计员
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </section>
          </>
        )}
        <AgentWorkspace tenants={state.tenants} />
      </div>
    </main>
  );
}
