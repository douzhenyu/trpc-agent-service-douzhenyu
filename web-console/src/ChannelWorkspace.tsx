import { FormEvent, useEffect, useMemo, useState } from "react";

import {
  ApiError,
  changeChannelBindingStatus,
  createChannelBinding,
  getAgentApplications,
  getChannelBindings,
  type AgentApplication,
  type ChannelBinding,
  type ChannelBindingUpsert,
  type Tenant,
} from "./api";

type ChannelType = "WECOM" | "FEISHU";

const channelCopy: Record<
  ChannelType,
  {
    name: string;
    mode: string;
    idLabel: string;
    idHint: string;
    secretHint: string;
    vaultFields: { name: string; description: string }[];
    checklist: string[];
  }
> = {
  WECOM: {
    name: "企业微信",
    mode: "智能机器人 · 加密回调",
    idLabel: "智能机器人 Bot ID（aibotid）",
    idHint: "企业微信智能机器人回调事件中的 aibotid，不是机器人名称",
    secretHint: "引用机器人凭据所在的租户专属密钥路径",
    vaultFields: [
      { name: "token", description: "回调签名校验 Token" },
      { name: "encoding-aes-key", description: "43 位 EncodingAESKey" },
    ],
    checklist: [
      "创建智能机器人",
      "配置可信回调域名",
      "写入 Token 与 AESKey",
      "发送回环消息",
    ],
  },
  FEISHU: {
    name: "飞书",
    mode: "自建应用 · 长连接",
    idLabel: "App ID",
    idHint: "飞书开放平台凭证与基础信息中的 App ID",
    secretHint: "引用 App Secret 所在的密钥路径",
    vaultFields: [
      { name: "app-secret", description: "长连接 SDK 建连凭据" },
      { name: "verification-token", description: "事件验签 Token" },
      { name: "tenant-access-token", description: "消息回复访问令牌" },
    ],
    checklist: [
      "创建企业自建应用",
      "开启机器人能力",
      "订阅消息事件",
      "建立长连接",
    ],
  },
};

function safeSecretRef(secretRef: string): string {
  const [path] = secretRef.split("#");
  return `${path}#••••••`;
}

function failureMessage(error: unknown): string {
  if (error instanceof ApiError && error.code === "CHANNEL_BINDING_CONFLICT")
    return "该机器人已绑定，请更换机器人 ID 或先停用原绑定。";
  if (error instanceof ApiError && error.code === "SECRET_REF_REJECTED")
    return "密钥引用不符合规范，请使用 vault://tenant/... 路径。";
  return error instanceof Error ? error.message : "通道配置操作失败";
}

export function ChannelWorkspace({ tenants }: { tenants: Tenant[] }) {
  const [tenantId, setTenantId] = useState(tenants[0]?.id ?? "");
  const [bindings, setBindings] = useState<ChannelBinding[]>([]);
  const [applications, setApplications] = useState<AgentApplication[]>([]);
  const [channelType, setChannelType] = useState<ChannelType>("WECOM");
  const [externalBotId, setExternalBotId] = useState("");
  const [applicationId, setApplicationId] = useState("");
  const [environment, setEnvironment] =
    useState<ChannelBindingUpsert["environment"]>("DEVELOPMENT");
  const [secretRef, setSecretRef] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const copy = channelCopy[channelType];
  const activeCount = bindings.filter(
    (binding) => binding.status === "ACTIVE",
  ).length;
  const callbackUrl = externalBotId
    ? `https://<channel-gateway-host>/internal/v1/wecom/callback/${tenantId}/${externalBotId}`
    : `https://<channel-gateway-host>/internal/v1/wecom/callback/${tenantId || "<tenant-id>"}/<aibotid>`;
  const feishuDeclaration = JSON.stringify([
    {
      tenant_id: tenantId || "<tenant-id>",
      app_id: externalBotId || "<app-id>",
    },
  ]);

  useEffect(() => {
    if (!tenants.some((tenant) => tenant.id === tenantId))
      setTenantId(tenants[0]?.id ?? "");
  }, [tenantId, tenants]);

  useEffect(() => {
    if (!tenantId) return;
    let cancelled = false;
    setBusy(true);
    Promise.all([getChannelBindings(tenantId), getAgentApplications(tenantId)])
      .then(([nextBindings, nextApplications]) => {
        if (cancelled) return;
        setBindings(nextBindings);
        setApplications(nextApplications);
        setApplicationId((current) => current || nextApplications[0]?.id || "");
        setMessage(null);
      })
      .catch((error) => {
        if (!cancelled) setMessage(failureMessage(error));
      })
      .finally(() => {
        if (!cancelled) setBusy(false);
      });
    return () => {
      cancelled = true;
    };
  }, [tenantId]);

  useEffect(() => {
    setSecretRef(
      tenantId
        ? `vault://tenant/${tenantId}/channels/${channelType.toLowerCase()}#${channelType === "FEISHU" ? "verification-token" : "token"}`
        : "",
    );
  }, [channelType, tenantId]);

  const applicationNames = useMemo(
    () =>
      new Map(
        applications.map((application) => [application.id, application.name]),
      ),
    [applications],
  );

  async function onCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!tenantId || !applicationId) return;
    setBusy(true);
    try {
      const created = await createChannelBinding(tenantId, {
        channel_type: channelType,
        external_bot_id: externalBotId,
        application_id: applicationId,
        environment,
        secret_ref: secretRef,
      });
      setBindings((current) => [...current, created]);
      setExternalBotId("");
      setMessage(
        `${copy.name}通道已登记。请完成右侧运行时检查后再发送生产消息。`,
      );
    } catch (error) {
      setMessage(failureMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function toggleStatus(binding: ChannelBinding) {
    setBusy(true);
    try {
      const updated = await changeChannelBindingStatus(
        binding,
        binding.status === "ACTIVE" ? "DISABLED" : "ACTIVE",
      );
      setBindings((current) =>
        current.map((item) =>
          item.binding_id === updated.binding_id ? updated : item,
        ),
      );
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function copyConfiguration(value: string, label: string) {
    try {
      await navigator.clipboard.writeText(value);
      setMessage(`${label}已复制。`);
    } catch {
      setMessage(`无法访问剪贴板，请手动复制${label}。`);
    }
  }

  return (
    <section className="workspace channel-workspace" id="channels">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Channels</p>
          <h2>企微与飞书接入</h2>
          <p className="section-description">
            为租户登记机器人路由、Agent 和密钥引用；协议凭据保留在
            Vault/OpenBao。
          </p>
        </div>
        <div className="metric-inline" aria-label="通道状态摘要">
          <span>
            <strong>{bindings.length}</strong> 已登记
          </span>
          <span>
            <strong>{activeCount}</strong> 运行中
          </span>
        </div>
      </div>

      {tenants.length === 0 ? (
        <div className="empty-state">
          请先创建租户和 Agent 应用，再配置 IM 通道。
        </div>
      ) : (
        <div className="channel-layout">
          <div className="panel panel--flat">
            <div className="field-row">
              <label>
                当前租户
                <select
                  aria-label="通道租户"
                  value={tenantId}
                  onChange={(event) => setTenantId(event.target.value)}
                >
                  {tenants.map((tenant) => (
                    <option key={tenant.id} value={tenant.id}>
                      {tenant.name}
                    </option>
                  ))}
                </select>
              </label>
              <span className="field-hint">
                通道和消息只在当前租户边界内生效
              </span>
            </div>

            <h3>1. 选择平台</h3>
            <div
              className="provider-grid"
              role="radiogroup"
              aria-label="IM 平台"
            >
              {(Object.keys(channelCopy) as ChannelType[]).map((type) => (
                <label
                  key={type}
                  className={`provider-card ${channelType === type ? "provider-card--selected" : ""}`}
                >
                  <input
                    type="radio"
                    name="channel_type"
                    value={type}
                    checked={channelType === type}
                    onChange={() => setChannelType(type)}
                  />
                  <span
                    className={`provider-logo provider-logo--${type.toLowerCase()}`}
                  >
                    {type === "WECOM" ? "企" : "飞"}
                  </span>
                  <span>
                    <strong>{channelCopy[type].name}</strong>
                    <small>{channelCopy[type].mode}</small>
                  </span>
                </label>
              ))}
            </div>

            <form className="stack channel-form" onSubmit={onCreate}>
              <h3>2. 登记路由与凭据引用</h3>
              <label>
                {copy.idLabel}
                <input
                  aria-label={copy.idLabel}
                  value={externalBotId}
                  onChange={(event) => setExternalBotId(event.target.value)}
                  placeholder={
                    channelType === "WECOM"
                      ? "例如：1000002"
                      : "例如：cli_a1b2c3d4"
                  }
                  required
                />
                <small className="field-hint">{copy.idHint}</small>
              </label>
              <label>
                绑定 Agent
                <select
                  aria-label="绑定 Agent"
                  value={applicationId}
                  onChange={(event) => setApplicationId(event.target.value)}
                  required
                >
                  <option value="">请选择 Agent 应用</option>
                  {applications.map((application) => (
                    <option key={application.id} value={application.id}>
                      {application.name}
                    </option>
                  ))}
                </select>
                {applications.length === 0 && (
                  <small className="field-hint field-hint--warning">
                    当前租户暂无 Agent 应用，请先到 Agent 应用区创建。
                  </small>
                )}
              </label>
              <div className="form-grid">
                <label>
                  运行环境
                  <select
                    aria-label="通道运行环境"
                    value={environment}
                    onChange={(event) => setEnvironment(event.target.value)}
                  >
                    <option value="DEVELOPMENT">开发</option>
                    <option value="STAGING">测试</option>
                    <option value="PRODUCTION">生产</option>
                  </select>
                </label>
                <label>
                  密钥存储引用
                  <input
                    aria-label="通道密钥引用"
                    value={secretRef}
                    onChange={(event) => setSecretRef(event.target.value)}
                    pattern="vault://tenant/.+#.+"
                    required
                  />
                </label>
              </div>
              <p className="security-note">
                <strong>只保存 Vault/OpenBao 引用</strong>
                {copy.secretHint}；控制台不会保存或回显明文 Secret。
              </p>
              <details className="parameter-guide" open>
                <summary>查看 {copy.name} 必需参数与部署值</summary>
                <div className="parameter-guide__content">
                  <div>
                    <strong>密钥字段</strong>
                    <ul className="parameter-list">
                      {copy.vaultFields.map((field) => (
                        <li key={field.name}>
                          <code>#{field.name}</code>
                          <span>{field.description}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                  {channelType === "WECOM" ? (
                    <div>
                      <strong>企业微信后台回调 URL</strong>
                      <div className="copyable-config">
                        <code className="config-value">{callbackUrl}</code>
                        <button
                          className="button button--quiet button--small"
                          type="button"
                          onClick={() =>
                            copyConfiguration(callbackUrl, "回调 URL")
                          }
                        >
                          复制回调 URL
                        </button>
                      </div>
                      <small className="field-hint">
                        当前运行时还需把同一份密钥映射为 WECOM_TOKEN 与
                        WECOM_ENCODING_AES_KEY。
                      </small>
                    </div>
                  ) : (
                    <div>
                      <strong>Channel Gateway 长连接声明</strong>
                      <div className="copyable-config">
                        <code className="config-value">
                          FEISHU_LONG_CONNECTIONS={feishuDeclaration}
                        </code>
                        <button
                          className="button button--quiet button--small"
                          type="button"
                          onClick={() =>
                            copyConfiguration(
                              `FEISHU_LONG_CONNECTIONS=${feishuDeclaration}`,
                              "长连接声明",
                            )
                          }
                        >
                          复制长连接声明
                        </button>
                      </div>
                      <small className="field-hint">
                        App ID 必须同时出现在绑定与非敏感部署声明中；App Secret
                        只从 Vault/OpenBao 读取。
                      </small>
                    </div>
                  )}
                </div>
              </details>
              <button
                type="submit"
                disabled={busy || applications.length === 0}
              >
                {busy ? "正在保存…" : "保存通道配置"}
              </button>
            </form>
          </div>

          <aside className="channel-aside">
            <div className="panel panel--flat checklist-card">
              <p className="eyebrow">Setup guide</p>
              <h3>{copy.name}上线检查</h3>
              <ol className="setup-list">
                {copy.checklist.map((item, index) => (
                  <li key={item}>
                    <span>{index + 1}</span>
                    <div>
                      <strong>{item}</strong>
                      <small>
                        {index < 2
                          ? "在 IM 平台控制台完成"
                          : "由 Channel Gateway 完成"}
                      </small>
                    </div>
                  </li>
                ))}
              </ol>
              <p className="aside-note">
                {channelType === "FEISHU"
                  ? "飞书通过官方 SDK 建立长连接，无需公网回调地址。"
                  : "企微当前使用签名校验的加密回调，需要可访问的 HTTPS 地址。"}
              </p>
            </div>
          </aside>
        </div>
      )}

      <div className="panel panel--flat binding-list">
        <div className="list-heading">
          <div>
            <h3>已登记通道</h3>
            <p className="muted">密钥仅展示路径，片段已隐藏。</p>
          </div>
          {busy && (
            <span className="status-chip status-chip--pending">同步中</span>
          )}
        </div>
        {bindings.length === 0 ? (
          <div className="empty-state">
            暂无通道配置。使用上方表单完成第一次接入。
          </div>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>平台</th>
                  <th>机器人 / App</th>
                  <th>关联 Agent</th>
                  <th>环境</th>
                  <th>密钥引用</th>
                  <th>状态</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {bindings.map((binding) => (
                  <tr key={binding.binding_id}>
                    <td>
                      <strong>
                        {binding.channel_type === "WECOM"
                          ? "企业微信"
                          : binding.channel_type === "FEISHU"
                            ? "飞书"
                            : binding.channel_type}
                      </strong>
                    </td>
                    <td className="mono">{binding.external_bot_id}</td>
                    <td>
                      {applicationNames.get(binding.application_id) ??
                        binding.application_id}
                    </td>
                    <td>{binding.environment}</td>
                    <td className="mono secret-ref">
                      {safeSecretRef(binding.secret_ref)}
                    </td>
                    <td>
                      <span
                        className={`status-chip ${binding.status === "ACTIVE" ? "status-chip--active" : ""}`}
                      >
                        {binding.status === "ACTIVE" ? "运行中" : "已停用"}
                      </span>
                    </td>
                    <td>
                      <button
                        className="button button--quiet button--small"
                        type="button"
                        onClick={() => toggleStatus(binding)}
                        disabled={busy}
                      >
                        {binding.status === "ACTIVE" ? "停用" : "启用"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
      {message && (
        <p
          className={`status ${message.includes("失败") || message.includes("不符合") || message.includes("已绑定") ? "status--error" : "status--success"}`}
          role="status"
        >
          {message}
        </p>
      )}
    </section>
  );
}
