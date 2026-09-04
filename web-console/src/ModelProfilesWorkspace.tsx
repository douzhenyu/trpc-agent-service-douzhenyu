import { FormEvent, useEffect, useState } from "react";

import {
  ApiError,
  createModelProfile,
  getModelProfiles,
  type ModelProfile,
  type ModelProfileCreate,
  type Tenant,
} from "./api";

const classifications: ModelProfileCreate["data_classification"][] = [
  "PUBLIC",
  "INTERNAL",
  "CONFIDENTIAL",
  "RESTRICTED",
];

function aliases(value: string): string[] {
  return value
    .split(",")
    .map((alias) => alias.trim())
    .filter(Boolean);
}

function failureMessage(error: unknown): string {
  if (error instanceof ApiError && error.code === "VERSION_MISMATCH")
    return "模型配置档已变化，请重新加载后再编辑。";
  return error instanceof Error ? error.message : "模型配置档操作失败";
}

export function ModelProfilesWorkspace({ tenants }: { tenants: Tenant[] }) {
  const [tenantId, setTenantId] = useState(tenants[0]?.id ?? "");
  const [profiles, setProfiles] = useState<ModelProfile[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [alias, setAlias] = useState("");
  const [providerModel, setProviderModel] = useState("");
  const [endpointUrl, setEndpointUrl] = useState("");
  const [secretRef, setSecretRef] = useState("");
  const [classification, setClassification] =
    useState<ModelProfileCreate["data_classification"]>("CONFIDENTIAL");
  const [region, setRegion] = useState("cn-north-1");
  const [fallbackAliases, setFallbackAliases] = useState("");
  const [requestsPerMinute, setRequestsPerMinute] = useState("60");

  useEffect(() => {
    if (!tenants.some((tenant) => tenant.id === tenantId)) {
      setTenantId(tenants[0]?.id ?? "");
      setProfiles([]);
    }
  }, [tenantId, tenants]);

  async function loadProfiles() {
    if (!tenantId) return;
    try {
      setProfiles(await getModelProfiles(tenantId));
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  async function onSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!tenantId) return;
    try {
      const created = await createModelProfile(tenantId, {
        alias,
        provider_model: providerModel,
        endpoint_url: endpointUrl,
        secret_ref: secretRef,
        data_classification: classification,
        region,
        fallback_aliases: aliases(fallbackAliases),
        requests_per_minute: Number(requestsPerMinute),
      });
      setProfiles((current) => [...current, created]);
      setAlias("");
      setProviderModel("");
      setEndpointUrl("");
      setSecretRef("");
      setFallbackAliases("");
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  return (
    <section className="panel panel--wide">
      <h2>模型配置档</h2>
      {tenants.length === 0 ? (
        <p className="muted">请先创建租户。</p>
      ) : (
        <>
          <div className="toolbar">
            <label>
              配置档租户
              <select
                aria-label="配置档租户"
                value={tenantId}
                onChange={(event) => {
                  setTenantId(event.target.value);
                  setProfiles([]);
                }}
              >
                {tenants.map((tenant) => (
                  <option key={tenant.id} value={tenant.id}>
                    {tenant.name}
                  </option>
                ))}
              </select>
            </label>
            <button type="button" onClick={loadProfiles}>
              加载模型配置档
            </button>
          </div>
          <form className="stack" onSubmit={onSave}>
            <label>
              模型配置档别名
              <input
                aria-label="模型配置档别名"
                value={alias}
                onChange={(event) => setAlias(event.target.value)}
                pattern="[a-z0-9][a-z0-9-]{1,62}"
                required
              />
            </label>
            <label>
              供应商模型
              <input
                aria-label="供应商模型"
                value={providerModel}
                onChange={(event) => setProviderModel(event.target.value)}
                required
              />
            </label>
            <label>
              模型 Endpoint
              <input
                aria-label="模型 Endpoint"
                value={endpointUrl}
                onChange={(event) => setEndpointUrl(event.target.value)}
                type="url"
                required
              />
            </label>
            <label>
              密钥引用
              <input
                aria-label="密钥引用"
                value={secretRef}
                onChange={(event) => setSecretRef(event.target.value)}
                placeholder={`vault://tenant/${tenantId}/llm/provider#api_key`}
                required
              />
            </label>
            <label>
              最高数据等级
              <select
                aria-label="最高数据等级"
                value={classification}
                onChange={(event) =>
                  setClassification(
                    event.target
                      .value as ModelProfileCreate["data_classification"],
                  )
                }
              >
                {classifications.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
            <label>
              区域
              <input
                aria-label="区域"
                value={region}
                onChange={(event) => setRegion(event.target.value)}
                required
              />
            </label>
            <label>
              Fallback 别名
              <input
                aria-label="Fallback 别名"
                value={fallbackAliases}
                onChange={(event) => setFallbackAliases(event.target.value)}
                placeholder="economy, premium"
              />
            </label>
            <label>
              每分钟请求数
              <input
                aria-label="每分钟请求数"
                min="1"
                max="100000"
                type="number"
                value={requestsPerMinute}
                onChange={(event) => setRequestsPerMinute(event.target.value)}
                required
              />
            </label>
            <button type="submit">保存模型配置档</button>
          </form>
          {profiles.length === 0 ? (
            <p className="muted">暂无模型配置档。</p>
          ) : (
            <ul>
              {profiles.map((profile) => (
                <li key={profile.id}>
                  <strong>
                    {profile.alias} → {profile.provider_model}
                  </strong>
                  <br />
                  <small>
                    {profile.data_classification} · {profile.region} ·{" "}
                    {profile.requests_per_minute} RPM
                  </small>
                  <br />
                  <code>{profile.secret_ref}</code>
                </li>
              ))}
            </ul>
          )}
          {message && <p className="status status--error">{message}</p>}
        </>
      )}
    </section>
  );
}
