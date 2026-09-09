import { FormEvent, useEffect, useState } from "react";

import {
  assignRole,
  assignTenantRole,
  createGroup,
  createTenant,
  createUser,
  emergencyLogin,
  getGroups,
  getSession,
  getTenants,
  getUsers,
  logout,
  type PlatformUser,
  type Session,
  type Tenant,
  type TenantGroup,
  type TenantMemberRole,
} from "./api";
import { AgentWorkspace } from "./AgentWorkspace";
import { ChannelWorkspace } from "./ChannelWorkspace";
import { ModelProfilesWorkspace } from "./ModelProfilesWorkspace";
import { OpsConsole } from "./OpsConsole";
import { StorageProfilesWorkspace } from "./StorageProfilesWorkspace";
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

type PlatformRole = "" | "PLATFORM_ADMIN" | "PLATFORM_AUDITOR";
type InitialTenantRole = "" | TenantMemberRole;

const LOCAL_ADMIN_USERNAME = "emergency-admin";
const LOCAL_ADMIN_PASSWORD = "correct-horse";

export default function App() {
  const [state, setState] = useState<State>({ kind: "loading" });
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [slug, setSlug] = useState("");
  const [tenantName, setTenantName] = useState("");
  const [groupName, setGroupName] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [issuer, setIssuer] = useState("https://id.example.com");
  const [subject, setSubject] = useState("");
  const [email, setEmail] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [initialRole, setInitialRole] = useState<PlatformRole>("");
  const [initialTenantId, setInitialTenantId] = useState("");
  const [initialTenantRole, setInitialTenantRole] =
    useState<InitialTenantRole>("");
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const isLocalhost = ["localhost", "127.0.0.1"].includes(
    globalThis.location.hostname,
  );

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
    setBusy(true);
    try {
      await load(await emergencyLogin(username, password));
    } catch {
      setState({ kind: "anonymous", error: "应急凭据无效" });
    } finally {
      setBusy(false);
    }
  }

  async function onTenant(event: FormEvent) {
    event.preventDefault();
    if (state.kind !== "ready") return;
    setBusy(true);
    try {
      const tenant = await createTenant(slug, tenantName);
      setState({ ...state, tenants: [...state.tenants, tenant] });
      setSlug("");
      setTenantName("");
      setActionMessage(`租户“${tenant.name}”已创建。`);
    } catch (error) {
      setActionMessage(error instanceof Error ? error.message : "创建租户失败");
    } finally {
      setBusy(false);
    }
  }

  async function onGroup(event: FormEvent) {
    event.preventDefault();
    if (state.kind !== "ready") return;
    setBusy(true);
    try {
      const group = await createGroup(groupName, selected);
      setState({ ...state, groups: [...state.groups, group] });
      setGroupName("");
      setSelected([]);
      setActionMessage(`租户组“${group.name}”已创建。`);
    } catch (error) {
      setActionMessage(
        error instanceof Error ? error.message : "创建租户组失败",
      );
    } finally {
      setBusy(false);
    }
  }

  async function onUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (state.kind !== "ready") return;
    setBusy(true);
    try {
      const created = await createUser({
        issuer,
        subject,
        email: email || null,
        display_name: displayName,
      });
      if (initialRole) {
        await assignRole(created.id, initialRole, created.version);
      }
      if (initialTenantId && initialTenantRole) {
        await assignTenantRole(initialTenantId, created.id, initialTenantRole);
      }
      if (initialRole) {
        await load(state.session);
      } else {
        setState({ ...state, users: [...state.users, created] });
      }
      setSubject("");
      setEmail("");
      setDisplayName("");
      setInitialRole("");
      setInitialTenantId("");
      setInitialTenantRole("");
      setActionMessage(
        initialTenantId && initialTenantRole
          ? `用户“${created.display_name}”已登记，并已加入所选租户。`
          : `用户“${created.display_name}”已登记。`,
      );
    } catch (error) {
      setActionMessage(error instanceof Error ? error.message : "登记用户失败");
    } finally {
      setBusy(false);
    }
  }

  async function onLogout() {
    setBusy(true);
    try {
      await logout();
      setState({ kind: "anonymous" });
    } catch (error) {
      setActionMessage(error instanceof Error ? error.message : "退出登录失败");
    } finally {
      setBusy(false);
    }
  }

  if (state.kind === "loading")
    return (
      <main className="loading-shell">
        <span className="loading-mark">T</span>
        <p>正在连接管理控制台…</p>
      </main>
    );

  if (state.kind === "error")
    return (
      <main className="shell">
        <section className="error-card">
          <span className="error-code">503</span>
          <h1>控制台暂时不可用</h1>
          <p className="muted">
            <span>{state.message}</span>。请确认本机服务已启动后重试。
          </p>
          <button type="button" onClick={() => globalThis.location.reload()}>
            重新连接
          </button>
        </section>
      </main>
    );

  if (state.kind === "anonymous")
    return (
      <main className="auth-shell">
        <section className="auth-intro">
          <div className="brand brand--dark">
            <span className="brand-mark">T</span>
            <span>tRPC-Agent Platform</span>
          </div>
          <div className="auth-message">
            <p className="eyebrow eyebrow--light">
              Enterprise Agent Infrastructure
            </p>
            <h1>让 Agent 安全进入企业工作流</h1>
            <p>统一管理租户、Agent、企业 IM、模型、存储与审计边界。</p>
          </div>
          <ul className="trust-list">
            <li>
              <span>01</span>
              <div>
                <strong>企业身份认证</strong>
                <small>OIDC 单点登录，首次登录自动登记</small>
              </div>
            </li>
            <li>
              <span>02</span>
              <div>
                <strong>多租户隔离</strong>
                <small>权限、密钥、数据与审计相互隔离</small>
              </div>
            </li>
            <li>
              <span>03</span>
              <div>
                <strong>全链路审计</strong>
                <small>关键管理动作进入防篡改审计链</small>
              </div>
            </li>
          </ul>
        </section>
        <section className="auth-panel">
          <div className="login-card">
            <div className="mobile-brand">
              <span className="brand-mark">T</span>tRPC-Agent Platform
            </div>
            <p className="eyebrow">Welcome back</p>
            <h2 aria-label="平台管理登录">登录管理控制台</h2>
            <p className="muted">
              使用企业身份，或在本机开发环境使用初始管理员。
            </p>
            <a className="primary-link" href="/api/v1/auth/oidc/login">
              使用企业账号登录
            </a>
            <p className="login-help">
              首次 OIDC 登录会自动登记账号，之后由管理员分配权限。
            </p>
            <div className="divider">
              <span>本地开发</span>
            </div>
            <form onSubmit={onEmergency} className="stack">
              <label>
                用户名
                <input
                  name="username"
                  autoComplete="username"
                  value={username}
                  onChange={(event) => setUsername(event.target.value)}
                  placeholder="请输入管理员用户名"
                  required
                />
              </label>
              <label>
                密码
                <input
                  name="password"
                  type="password"
                  autoComplete="current-password"
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  placeholder="请输入密码"
                  required
                />
              </label>
              <button
                type="submit"
                className="button button--dark"
                aria-label="应急登录"
                disabled={busy}
              >
                {busy ? "正在登录…" : "使用本地管理员登录"}
              </button>
            </form>
            {isLocalhost && (
              <div className="local-credentials">
                <div>
                  <strong>本机开发凭据</strong>
                  <small>仅在 localhost / 127.0.0.1 显示</small>
                </div>
                <code>{LOCAL_ADMIN_USERNAME}</code>
                <button
                  type="button"
                  className="button button--quiet button--small"
                  onClick={() => {
                    setUsername(LOCAL_ADMIN_USERNAME);
                    setPassword(LOCAL_ADMIN_PASSWORD);
                  }}
                >
                  自动填入
                </button>
              </div>
            )}
            {state.error && (
              <p className="status status--error" role="alert">
                {state.error}
              </p>
            )}
          </div>
        </section>
      </main>
    );

  const platformReader = state.session.roles.some((role) =>
    ["PLATFORM_ADMIN", "PLATFORM_AUDITOR"].includes(role),
  );
  const platformAdmin = state.session.roles.includes("PLATFORM_ADMIN");
  const accountLabel =
    state.session.auth_method === "emergency"
      ? "应急管理员"
      : state.session.subject;

  return (
    <div className="app-frame">
      <aside className="sidebar">
        <div className="brand brand--dark">
          <span className="brand-mark">T</span>
          <span>
            tRPC-Agent
            <br />
            <small>Platform Console</small>
          </span>
        </div>
        <nav aria-label="主导航">
          <a href="#overview" className="nav-item nav-item--active">
            <span>概</span>总览
          </a>
          {platformReader && (
            <a href="#tenants" className="nav-item">
              <span>租</span>租户与成员
            </a>
          )}
          <a href="#agents" className="nav-item">
            <span>A</span>Agent 应用
          </a>
          <a href="#channels" className="nav-item">
            <span>通</span>通道接入
          </a>
          <a href="#models" className="nav-item">
            <span>模</span>模型配置
          </a>
          <a href="#storage" className="nav-item">
            <span>存</span>存储与数据
          </a>
          <a href="#operations" className="nav-item">
            <span>审</span>审计与运维
          </a>
        </nav>
        <div className="sidebar-footer">
          <div className="environment">
            <span className="health-dot" />
            本地开发环境
          </div>
          <div className="account-block">
            <span className="avatar">
              {accountLabel.slice(0, 1).toUpperCase()}
            </span>
            <div>
              <strong>{accountLabel}</strong>
              <small>{state.session.roles.join(" · ") || "租户成员"}</small>
            </div>
          </div>
        </div>
      </aside>

      <main className="main-content">
        <header className="topbar">
          <div>
            <span className="breadcrumb">平台</span>
            <span className="breadcrumb-separator">/</span>
            <strong>工作台</strong>
          </div>
          <div className="topbar-actions">
            <span className="version-chip">API v1</span>
            <button
              className="button button--quiet button--small"
              type="button"
              onClick={onLogout}
              disabled={busy}
            >
              退出登录
            </button>
          </div>
        </header>

        <div className="content-inner">
          <section className="overview" id="overview">
            <div>
              <p className="eyebrow">Platform overview</p>
              <h1>租户与权限管理</h1>
              <p>从身份和租户边界开始，完成 Agent、IM 通道与生产运行配置。</p>
            </div>
            <a href="#channels" className="button">
              配置企业 IM
            </a>
          </section>

          <section className="metric-grid" aria-label="平台摘要">
            <article>
              <span className="metric-icon metric-icon--blue">租</span>
              <div>
                <small>可访问租户</small>
                <strong>{state.tenants.length}</strong>
                <p>配置与数据按租户隔离</p>
              </div>
            </article>
            <article>
              <span className="metric-icon metric-icon--green">权</span>
              <div>
                <small>平台身份</small>
                <strong>{platformReader ? state.users.length : "—"}</strong>
                <p>OIDC 统一身份入口</p>
              </div>
            </article>
            <article>
              <span className="metric-icon metric-icon--amber">环</span>
              <div>
                <small>当前环境</small>
                <strong>LOCAL</strong>
                <p>生产部署前完成安全校验</p>
              </div>
            </article>
          </section>

          {actionMessage && (
            <p className="status status--success" role="status">
              {actionMessage}
            </p>
          )}

          {platformReader && (
            <section className="workspace" id="tenants">
              <div className="section-heading">
                <div>
                  <p className="eyebrow">Identity & tenancy</p>
                  <h2>租户、成员与权限</h2>
                  <p className="section-description">
                    管理员创建租户、预登记 OIDC 身份并按最小权限授予平台角色。
                  </p>
                </div>
              </div>
              <div className="management-grid">
                {platformAdmin && (
                  <section className="panel panel--flat">
                    <div className="panel-heading">
                      <div>
                        <h3>创建租户</h3>
                        <p>建立新的配置、权限、数据和成本边界。</p>
                      </div>
                      <span className="step-badge">01</span>
                    </div>
                    <form onSubmit={onTenant} className="stack">
                      <label>
                        租户标识
                        <input
                          aria-label="租户标识"
                          value={slug}
                          onChange={(event) => setSlug(event.target.value)}
                          placeholder="acme-cn"
                          required
                          pattern="[a-z0-9][a-z0-9-]+"
                        />
                        <small className="field-hint">
                          创建后不可修改，用于资源与密钥路径。
                        </small>
                      </label>
                      <label>
                        租户名称
                        <input
                          aria-label="租户名称"
                          value={tenantName}
                          onChange={(event) =>
                            setTenantName(event.target.value)
                          }
                          placeholder="例如：星河科技"
                          required
                        />
                      </label>
                      <button type="submit" disabled={busy}>
                        创建租户
                      </button>
                    </form>
                    <div className="compact-list">
                      {state.tenants.map((tenant) => (
                        <div key={tenant.id}>
                          <span className="list-avatar">
                            {tenant.name.slice(0, 1)}
                          </span>
                          <div>
                            <strong>
                              {tenant.name} ({tenant.slug})
                            </strong>
                            <small>租户资源边界</small>
                          </div>
                          <span className="status-chip status-chip--active">
                            {tenant.status}
                          </span>
                        </div>
                      ))}
                    </div>
                  </section>
                )}

                {platformAdmin && (
                  <section className="panel panel--flat">
                    <div className="panel-heading">
                      <div>
                        <h3>创建租户组</h3>
                        <p>批量组织租户，不改变各租户的数据边界。</p>
                      </div>
                      <span className="step-badge">02</span>
                    </div>
                    <form onSubmit={onGroup} className="stack">
                      <label>
                        租户组名称
                        <input
                          aria-label="Tenant Group 名称"
                          value={groupName}
                          onChange={(event) => setGroupName(event.target.value)}
                          placeholder="例如：华东客户"
                          required
                        />
                      </label>
                      <fieldset>
                        <legend>选择成员租户</legend>
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
                      <button type="submit" disabled={busy}>
                        创建 Tenant Group
                      </button>
                    </form>
                    <div className="compact-list">
                      {state.groups.map((group) => (
                        <div key={group.id}>
                          <span className="list-avatar list-avatar--neutral">
                            组
                          </span>
                          <div>
                            <strong>
                              {group.name} · {group.tenant_ids.length} 个租户
                            </strong>
                            <small>仅用于组织，不继承权限</small>
                          </div>
                        </div>
                      ))}
                    </div>
                  </section>
                )}

                <section className="panel panel--flat panel--wide">
                  <div className="panel-heading">
                    <div>
                      <h3>成员与平台角色</h3>
                      <p>
                        普通用户首次 OIDC
                        登录会自动登记；管理员也可提前录入身份。
                      </p>
                    </div>
                    <span className="step-badge">03</span>
                  </div>
                  {platformAdmin && (
                    <form className="member-form" onSubmit={onUser}>
                      <label>
                        显示名称
                        <input
                          aria-label="用户显示名称"
                          value={displayName}
                          onChange={(event) =>
                            setDisplayName(event.target.value)
                          }
                          placeholder="张三"
                          required
                        />
                      </label>
                      <label>
                        邮箱
                        <input
                          aria-label="用户邮箱"
                          type="email"
                          value={email}
                          onChange={(event) => setEmail(event.target.value)}
                          placeholder="user@example.com"
                        />
                      </label>
                      <label>
                        OIDC Issuer
                        <input
                          aria-label="OIDC Issuer"
                          type="url"
                          value={issuer}
                          onChange={(event) => setIssuer(event.target.value)}
                          required
                        />
                      </label>
                      <label>
                        OIDC Subject
                        <input
                          aria-label="OIDC Subject"
                          value={subject}
                          onChange={(event) => setSubject(event.target.value)}
                          placeholder="身份提供商中的唯一用户 ID"
                          required
                        />
                      </label>
                      <label>
                        初始平台角色
                        <select
                          aria-label="初始平台角色"
                          value={initialRole}
                          onChange={(event) =>
                            setInitialRole(event.target.value as PlatformRole)
                          }
                        >
                          <option value="">普通用户（无平台角色）</option>
                          <option value="PLATFORM_AUDITOR">平台审计员</option>
                          <option value="PLATFORM_ADMIN">平台管理员</option>
                        </select>
                      </label>
                      <label>
                        加入租户
                        <select
                          aria-label="加入租户"
                          value={initialTenantId}
                          onChange={(event) => {
                            setInitialTenantId(event.target.value);
                            if (!event.target.value) setInitialTenantRole("");
                          }}
                        >
                          <option value="">暂不加入租户</option>
                          {state.tenants.map((tenant) => (
                            <option key={tenant.id} value={tenant.id}>
                              {tenant.name}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label>
                        初始租户角色
                        <select
                          aria-label="初始租户角色"
                          value={initialTenantRole}
                          onChange={(event) =>
                            setInitialTenantRole(
                              event.target.value as InitialTenantRole,
                            )
                          }
                          disabled={!initialTenantId}
                          required={Boolean(initialTenantId)}
                        >
                          <option value="">请选择租户角色</option>
                          <option value="AGENT_DEVELOPER">Agent 开发者</option>
                          <option value="TENANT_AUDITOR">租户审计员</option>
                          <option value="TENANT_ADMIN">租户管理员</option>
                        </select>
                      </label>
                      <button
                        type="submit"
                        aria-label="登记用户"
                        disabled={busy}
                      >
                        登记并授权
                      </button>
                    </form>
                  )}
                  {state.users.length === 0 ? (
                    <div className="empty-state">
                      暂无已登记用户。OIDC 用户首次登录后会出现在这里。
                    </div>
                  ) : (
                    <div className="user-list">
                      {state.users.map((user) => (
                        <div key={user.id} className="user-row">
                          <span className="avatar avatar--light">
                            {user.display_name.slice(0, 1)}
                          </span>
                          <div className="user-identity">
                            <strong>{user.display_name}</strong>
                            <small>
                              {user.email ?? `${user.issuer} · ${user.subject}`}
                            </small>
                          </div>
                          <div className="role-tags">
                            {user.roles.length ? (
                              user.roles.map((role) => (
                                <span className="status-chip" key={role}>
                                  {role}
                                </span>
                              ))
                            ) : (
                              <span className="muted">普通用户</span>
                            )}
                          </div>
                          {platformAdmin && (
                            <div className="row-actions">
                              <button
                                className="button button--quiet button--small"
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
                                className="button button--quiet button--small"
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
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </section>
              </div>
            </section>
          )}

          <div id="agents">
            <AgentWorkspace tenants={state.tenants} />
          </div>
          <ChannelWorkspace tenants={state.tenants} />
          <div id="models">
            <ModelProfilesWorkspace tenants={state.tenants} />
          </div>
          <div id="storage">
            <StorageProfilesWorkspace tenants={state.tenants} />
          </div>
          <div id="operations">
            <OpsConsole tenants={state.tenants} />
          </div>
        </div>
      </main>
    </div>
  );
}
