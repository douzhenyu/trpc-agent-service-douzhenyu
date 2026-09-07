import { FormEvent, useEffect, useState } from "react";

import {
  ApiError,
  createStorageProfile,
  getStorageProfiles,
  type StorageProfile,
  type StorageProfileCreate,
  type Tenant,
} from "./api";

const kinds = ["SQL", "REDIS", "VECTOR", "OBJECT", "EXTERNAL_MEMORY"] as const;
type BackendKind = (typeof kinds)[number];
const classifications: StorageProfileCreate["classification"][] = [
  "PUBLIC",
  "INTERNAL",
  "CONFIDENTIAL",
  "RESTRICTED",
];

export function StorageProfilesWorkspace({ tenants }: { tenants: Tenant[] }) {
  const [tenantId, setTenantId] = useState(tenants[0]?.id ?? "");
  const [profiles, setProfiles] = useState<StorageProfile[]>([]);
  const [alias, setAlias] = useState("");
  const [workerPool, setWorkerPool] = useState("shared-workers");
  const [backendEndpoints, setBackendEndpoints] = useState<
    Record<BackendKind, string>
  >({
    SQL: "https://sql.example.test",
    REDIS: "https://redis.example.test",
    VECTOR: "https://vector.example.test",
    OBJECT: "https://object.example.test",
    EXTERNAL_MEMORY: "https://memory.example.test",
  });
  const [backendSecrets, setBackendSecrets] = useState<
    Record<BackendKind, string>
  >({
    SQL: "",
    REDIS: "",
    VECTOR: "",
    OBJECT: "",
    EXTERNAL_MEMORY: "",
  });
  const [encryptionKeyRef, setEncryptionKeyRef] = useState("");
  const [externalMemoryEnabled, setExternalMemoryEnabled] = useState(false);
  const [classification, setClassification] =
    useState<StorageProfileCreate["classification"]>("INTERNAL");
  const [isolation, setIsolation] = useState<"SHARED" | "DEDICATED">("SHARED");
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!tenants.some((tenant) => tenant.id === tenantId)) {
      setTenantId(tenants[0]?.id ?? "");
      setProfiles([]);
    }
  }, [tenantId, tenants]);

  async function loadProfiles() {
    if (!tenantId) return;
    try {
      setProfiles(await getStorageProfiles(tenantId));
      setMessage(null);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "无法读取存储配置档");
    }
  }

  async function onSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!tenantId) return;
    try {
      const profile = await createStorageProfile(tenantId, {
        alias,
        classification,
        worker_pool: workerPool,
        encryption_key_ref:
          encryptionKeyRef ||
          `vault://tenant/${tenantId}/storage#encryption_key`,
        activate: true,
        backends: kinds
          .filter((kind) => kind !== "EXTERNAL_MEMORY" || externalMemoryEnabled)
          .map((kind) => ({
            kind,
            endpoint: backendEndpoints[kind],
            dedicated: isolation === "DEDICATED",
            secret_ref:
              backendSecrets[kind] ||
              `vault://tenant/${tenantId}/storage/${kind.toLowerCase()}#credential`,
          })),
      });
      setProfiles((current) => [
        ...current.filter((item) => !item.active),
        profile,
      ]);
      setAlias("");
      setMessage(null);
    } catch (error) {
      setMessage(
        error instanceof ApiError && error.code === "DEDICATED_STORAGE_REQUIRED"
          ? "高敏存储必须使用专属后端和专属 Worker Pool。"
          : error instanceof Error
            ? error.message
            : "无法保存存储配置档",
      );
    }
  }

  return (
    <section className="panel panel--wide">
      <h2>存储配置档</h2>
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
                onChange={(event) => setTenantId(event.target.value)}
              >
                {tenants.map((tenant) => (
                  <option key={tenant.id} value={tenant.id}>
                    {tenant.name}
                  </option>
                ))}
              </select>
            </label>
            <button type="button" onClick={loadProfiles}>
              加载存储配置档
            </button>
          </div>
          <form className="stack" onSubmit={onSave}>
            <label>
              存储配置档别名
              <input
                aria-label="存储配置档别名"
                value={alias}
                onChange={(event) => setAlias(event.target.value)}
                pattern="[a-z0-9][a-z0-9-]{1,62}"
                required
              />
            </label>
            <label>
              存储隔离方式
              <select
                aria-label="存储隔离方式"
                value={isolation}
                onChange={(event) =>
                  setIsolation(event.target.value as "SHARED" | "DEDICATED")
                }
              >
                <option value="SHARED">共享</option>
                <option value="DEDICATED">专属</option>
              </select>
            </label>
            <label>
              最高数据等级
              <select
                aria-label="最高数据等级"
                value={classification}
                onChange={(event) =>
                  setClassification(
                    event.target
                      .value as StorageProfileCreate["classification"],
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
              Worker Pool
              <input
                aria-label="Worker Pool"
                value={workerPool}
                onChange={(event) => setWorkerPool(event.target.value)}
                required
              />
            </label>
            <label>
              数据加密密钥引用
              <input
                aria-label="数据加密密钥引用"
                value={encryptionKeyRef}
                onChange={(event) => setEncryptionKeyRef(event.target.value)}
                placeholder={`vault://tenant/${tenantId}/storage#encryption_key`}
              />
            </label>
            <label>
              <input
                aria-label="启用外部 Memory"
                type="checkbox"
                checked={externalMemoryEnabled}
                onChange={(event) =>
                  setExternalMemoryEnabled(event.target.checked)
                }
              />
              启用外部 Memory Adapter
            </label>
            {kinds
              .filter(
                (kind) => kind !== "EXTERNAL_MEMORY" || externalMemoryEnabled,
              )
              .map((kind) => (
                <fieldset key={kind}>
                  <legend>{kind} 后端</legend>
                  <label>
                    Endpoint
                    <input
                      aria-label={`${kind} Endpoint`}
                      type="url"
                      value={backendEndpoints[kind]}
                      onChange={(event) =>
                        setBackendEndpoints((current) => ({
                          ...current,
                          [kind]: event.target.value,
                        }))
                      }
                      required
                    />
                  </label>
                  <label>
                    密钥引用
                    <input
                      aria-label={`${kind} 密钥引用`}
                      value={backendSecrets[kind]}
                      onChange={(event) =>
                        setBackendSecrets((current) => ({
                          ...current,
                          [kind]: event.target.value,
                        }))
                      }
                      placeholder={`vault://tenant/${tenantId}/storage/${kind.toLowerCase()}#credential`}
                    />
                  </label>
                </fieldset>
              ))}
            <button type="submit">保存存储配置档</button>
          </form>
          {message && <p className="status status--error">{message}</p>}
          {profiles.length === 0 ? (
            <p className="muted">暂无存储配置档。</p>
          ) : (
            <ul>
              {profiles.map((profile) => (
                <li key={profile.id}>
                  <strong>
                    {profile.alias} → {profile.worker_pool}
                  </strong>
                  <br />
                  <small>
                    {profile.classification} ·{" "}
                    {profile.active ? "当前生效" : "未生效"} ·{" "}
                    {profile.backends
                      .map((backend) => backend.kind)
                      .join(" / ")}
                  </small>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </section>
  );
}
