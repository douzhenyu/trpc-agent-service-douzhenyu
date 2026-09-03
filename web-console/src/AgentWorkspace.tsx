import { FormEvent, useEffect, useRef, useState } from "react";

import {
  ApiError,
  createAgentApplication,
  createAgentDraft,
  deleteAgentApplication,
  deleteAgentDraft,
  getAgentApplications,
  getAgentDraft,
  updateAgentApplication,
  updateAgentDraft,
  validateAgentDraft,
  type AgentApplication,
  type AgentDraft,
  type DraftValidation,
  type Tenant,
} from "./api";

function references(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function failureMessage(error: unknown): string {
  if (error instanceof ApiError && error.code === "VERSION_MISMATCH")
    return "版本已变化，请重新加载后再编辑。";
  return error instanceof Error ? error.message : "Agent 管理操作失败";
}

export function AgentWorkspace({ tenants }: { tenants: Tenant[] }) {
  const selectionRequest = useRef(0);
  const [tenantId, setTenantId] = useState(tenants[0]?.id ?? "");
  const [applications, setApplications] = useState<AgentApplication[]>([]);
  const [selected, setSelected] = useState<AgentApplication | null>(null);
  const [draft, setDraft] = useState<AgentDraft | null>(null);
  const [validation, setValidation] = useState<DraftValidation | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [slug, setSlug] = useState("");
  const [createName, setCreateName] = useState("");
  const [editName, setEditName] = useState("");
  const [description, setDescription] = useState("");
  const [instructions, setInstructions] = useState("");
  const [modelAlias, setModelAlias] = useState("");
  const [toolAliases, setToolAliases] = useState("");
  const [knowledgeRefs, setKnowledgeRefs] = useState("");
  const [policyRef, setPolicyRef] = useState("");

  useEffect(() => {
    if (!tenants.some((tenant) => tenant.id === tenantId))
      setTenantId(tenants[0]?.id ?? "");
  }, [tenantId, tenants]);

  function showDraft(next: AgentDraft | null) {
    setDraft(next);
    setValidation(null);
    setInstructions(next?.instructions ?? "");
    setModelAlias(next?.model_alias ?? "");
    setToolAliases(next?.tool_aliases.join(", ") ?? "");
    setKnowledgeRefs(next?.knowledge_refs.join(", ") ?? "");
    setPolicyRef(next?.governance_policy_ref ?? "");
  }

  async function loadApplications() {
    if (!tenantId) return;
    try {
      selectionRequest.current += 1;
      setApplications(await getAgentApplications(tenantId));
      setSelected(null);
      showDraft(null);
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  async function selectApplication(application: AgentApplication) {
    const request = ++selectionRequest.current;
    try {
      setSelected(application);
      setEditName(application.name);
      setDescription(application.description);
      showDraft(null);
      const nextDraft = await getAgentDraft(
        application.tenant_id,
        application.id,
      );
      if (selectionRequest.current !== request) return;
      showDraft(nextDraft);
      setMessage(null);
    } catch (error) {
      if (selectionRequest.current !== request) return;
      setMessage(failureMessage(error));
    }
  }

  async function onCreateApplication(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    try {
      const application = await createAgentApplication(tenantId, {
        slug,
        name: createName,
        description: "",
      });
      selectionRequest.current += 1;
      setApplications((current) => [...current, application]);
      setSelected(application);
      showDraft(null);
      setSlug("");
      setCreateName("");
      setEditName(application.name);
      setDescription(application.description);
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  async function onUpdateApplication(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selected) return;
    try {
      const updated = await updateAgentApplication(selected, {
        name: editName,
        description,
      });
      setSelected(updated);
      setApplications((current) =>
        current.map((application) =>
          application.id === updated.id ? updated : application,
        ),
      );
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  async function onDeleteApplication() {
    if (!selected) return;
    try {
      await deleteAgentApplication(selected);
      selectionRequest.current += 1;
      setApplications((current) =>
        current.filter((application) => application.id !== selected.id),
      );
      setSelected(null);
      showDraft(null);
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  async function onSaveDraft(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selected) return;
    const payload = {
      instructions,
      model_alias: modelAlias,
      tool_aliases: references(toolAliases),
      knowledge_refs: references(knowledgeRefs),
      governance_policy_ref: policyRef || null,
    };
    try {
      showDraft(
        draft
          ? await updateAgentDraft(draft, payload)
          : await createAgentDraft(selected.tenant_id, selected.id, payload),
      );
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  async function onDeleteDraft() {
    if (!draft) return;
    try {
      await deleteAgentDraft(draft);
      showDraft(null);
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  async function onValidateDraft() {
    if (!draft) return;
    try {
      setValidation(await validateAgentDraft(draft));
      setMessage(null);
    } catch (error) {
      setMessage(failureMessage(error));
    }
  }

  return (
    <section className="panel panel--wide agent-workspace">
      <h2>Agent 应用与 Draft</h2>
      {tenants.length === 0 ? (
        <p className="muted">请先创建租户。</p>
      ) : (
        <>
          <div className="toolbar">
            <label>
              Agent 租户
              <select
                aria-label="Agent 租户"
                value={tenantId}
                onChange={(event) => {
                  selectionRequest.current += 1;
                  setTenantId(event.target.value);
                  setApplications([]);
                  setSelected(null);
                  showDraft(null);
                }}
              >
                {tenants.map((tenant) => (
                  <option key={tenant.id} value={tenant.id}>
                    {tenant.name}
                  </option>
                ))}
              </select>
            </label>
            <button type="button" onClick={loadApplications}>
              加载 Agent 应用
            </button>
          </div>

          <form className="stack" onSubmit={onCreateApplication}>
            <h3>创建 Agent 应用</h3>
            <label>
              Agent 应用标识
              <input
                aria-label="Agent 应用标识"
                value={slug}
                onChange={(event) => setSlug(event.target.value)}
                required
              />
            </label>
            <label>
              Agent 应用名称
              <input
                aria-label="Agent 应用名称"
                value={createName}
                onChange={(event) => setCreateName(event.target.value)}
                required
              />
            </label>
            <button type="submit">创建 Agent 应用</button>
          </form>

          <ul>
            {applications.map((application) => (
              <li key={application.id}>
                <button
                  type="button"
                  onClick={() => selectApplication(application)}
                >
                  {application.name} ({application.slug})
                </button>
              </li>
            ))}
          </ul>

          {selected && (
            <div className="agent-editor">
              <form className="stack" onSubmit={onUpdateApplication}>
                <h3>编辑 Agent 应用</h3>
                <label>
                  Agent 应用显示名称
                  <input
                    aria-label="Agent 应用显示名称"
                    value={editName}
                    onChange={(event) => setEditName(event.target.value)}
                    required
                  />
                </label>
                <label>
                  Agent 应用描述
                  <textarea
                    aria-label="Agent 应用描述"
                    value={description}
                    onChange={(event) => setDescription(event.target.value)}
                  />
                </label>
                <button type="submit">保存 Agent 应用</button>
                <button
                  type="button"
                  className="danger"
                  onClick={onDeleteApplication}
                >
                  删除 Agent 应用
                </button>
              </form>

              <form className="stack" onSubmit={onSaveDraft}>
                <h3>Agent Draft</h3>
                {draft && <p>Draft 版本 {draft.version} · 不承载生产流量</p>}
                <label>
                  Draft 指令
                  <textarea
                    aria-label="Draft 指令"
                    value={instructions}
                    onChange={(event) => setInstructions(event.target.value)}
                  />
                </label>
                <label>
                  模型别名
                  <input
                    aria-label="模型别名"
                    value={modelAlias}
                    onChange={(event) => setModelAlias(event.target.value)}
                  />
                </label>
                <label>
                  工具别名
                  <input
                    aria-label="工具别名"
                    value={toolAliases}
                    onChange={(event) => setToolAliases(event.target.value)}
                  />
                </label>
                <label>
                  Knowledge 引用
                  <input
                    aria-label="Knowledge 引用"
                    value={knowledgeRefs}
                    onChange={(event) => setKnowledgeRefs(event.target.value)}
                  />
                </label>
                <label>
                  治理策略引用
                  <input
                    aria-label="治理策略引用"
                    value={policyRef}
                    onChange={(event) => setPolicyRef(event.target.value)}
                  />
                </label>
                <button type="submit">
                  {draft ? "保存 Agent Draft" : "创建 Agent Draft"}
                </button>
                {draft && (
                  <>
                    <button type="button" onClick={onValidateDraft}>
                      校验 Agent Draft
                    </button>
                    <button
                      type="button"
                      className="danger"
                      onClick={onDeleteDraft}
                    >
                      删除 Agent Draft
                    </button>
                  </>
                )}
              </form>
            </div>
          )}

          {validation && (
            <div
              className={validation.valid ? "status" : "status status--error"}
            >
              {validation.valid ? (
                <p>Agent Draft 校验通过</p>
              ) : (
                <ul>
                  {validation.issues.map((issue) => (
                    <li key={`${issue.path}:${issue.code}`}>
                      <code>{issue.path}</code> {issue.message}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
          {message && <p className="status status--error">{message}</p>}
        </>
      )}
    </section>
  );
}
