import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";

import {
  getAuditEvents,
  getBudgets,
  getKnowledgeBases,
  getStorageMigrations,
  getToolApprovals,
  listOpsArtifacts,
  listOpsDeadLetters,
  listOpsMemories,
  listOpsOperations,
  listOpsSessions,
  requeueDeadLetter,
  type GenericRow,
  type OpsDeadLetter,
  type OpsOperation,
  type Tenant,
} from "./api";
import { t } from "./i18n";

type LoadMore = { rows: GenericRow[]; cursor: string | null };

function formatRow(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "string") {
    if (!/^\d{4}-\d{2}-\d{2}T/.test(value)) return value;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime())
      ? value
      : parsed.toLocaleString("zh-CN", { hour12: false });
  }
  return String(value);
}

function Cell({ value }: { value: unknown }) {
  return <td className="ops-cell">{formatRow(value)}</td>;
}

function Table({
  columns,
  rows,
}: {
  columns: { key: string; label: string }[];
  rows: GenericRow[];
}) {
  if (!rows || rows.length === 0) {
    return <p className="ops-empty">{t("opsEmpty")}</p>;
  }
  return (
    <table className="ops-table">
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column.key}>{column.label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={index}>
            {columns.map((column) => (
              <Cell key={column.key} value={row[column.key]} />
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Section({
  title,
  error,
  children,
}: {
  title: string;
  error?: string | null;
  children: ReactNode;
}) {
  return (
    <section className="ops-section">
      <h3>{title}</h3>
      {error ? <p className="ops-error">{error}</p> : null}
      {children}
    </section>
  );
}

function paged(
  first: GenericRow[],
  cursor: string | null,
): LoadMore {
  return { rows: first, cursor };
}

export function OpsConsole({ tenants }: { tenants: Tenant[] }) {
  const [tenantId, setTenantId] = useState(tenants[0]?.id ?? "");
  const [message, setMessage] = useState<string | null>(null);
  const [sessions, setSessions] = useState<LoadMore>({ rows: [], cursor: null });
  const [memories, setMemories] = useState<LoadMore>({ rows: [], cursor: null });
  const [artifacts, setArtifacts] = useState<LoadMore>({ rows: [], cursor: null });
  const [deadLetters, setDeadLetters] = useState<LoadMore>({ rows: [], cursor: null });
  const [operations, setOperations] = useState<OpsOperation[]>([]);
  const [approvals, setApprovals] = useState<GenericRow[]>([]);
  const [auditEvents, setAuditEvents] = useState<GenericRow[]>([]);
  const [budgets, setBudgets] = useState<GenericRow[]>([]);
  const [migrations, setMigrations] = useState<GenericRow[]>([]);
  const [knowledgeBases, setKnowledgeBases] = useState<GenericRow[]>([]);

  useEffect(() => {
    if (!tenants.some((tenant) => tenant.id === tenantId)) {
      setTenantId(tenants[0]?.id ?? "");
    }
  }, [tenantId, tenants]);

  const reset = useCallback(() => {
    setSessions({ rows: [], cursor: null });
    setMemories({ rows: [], cursor: null });
    setArtifacts({ rows: [], cursor: null });
    setDeadLetters({ rows: [], cursor: null });
    setOperations([]);
    setApprovals([]);
    setAuditEvents([]);
    setBudgets([]);
    setMigrations([]);
    setKnowledgeBases([]);
  }, []);

  const load = useCallback(async () => {
    if (!tenantId) return;
    try {
      const [sessionPage, memoryPage, artifactPage, deadLetterPage] =
        await Promise.all([
          listOpsSessions(tenantId, { limit: 20 }),
          listOpsMemories(tenantId, { limit: 20 }),
          listOpsArtifacts(tenantId, { limit: 20 }),
          listOpsDeadLetters(tenantId, { limit: 20 }),
        ]);
      setSessions(paged(sessionPage.items, sessionPage.next_cursor));
      setMemories(paged(memoryPage.items, memoryPage.next_cursor));
      setArtifacts(paged(artifactPage.items, artifactPage.next_cursor));
      setDeadLetters(paged(deadLetterPage.items, deadLetterPage.next_cursor));
      const [
        operationRows,
        approvalRows,
        auditRows,
        budgetRows,
        migrationRows,
        knowledgeRows,
      ] = await Promise.all([
        listOpsOperations(tenantId),
        getToolApprovals(tenantId),
        getAuditEvents(tenantId),
        getBudgets(tenantId),
        getStorageMigrations(tenantId),
        getKnowledgeBases(tenantId),
      ]);
      setOperations(operationRows);
      setApprovals(approvalRows);
      setAuditEvents(auditRows);
      setBudgets(budgetRows);
      setMigrations(migrationRows);
      setKnowledgeBases(knowledgeRows);
      setMessage(null);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : t("opsLoadFailed"));
    }
  }, [tenantId]);

  useEffect(() => {
    reset();
    void load();
  }, [load, reset]);

  async function loadMore(
    loader: (cursor: string) => Promise<{ items: GenericRow[]; next_cursor: string | null }>,
    current: LoadMore,
    update: (next: LoadMore) => void,
  ) {
    if (!current.cursor) return;
    try {
      const page = await loader(current.cursor);
      update({
        rows: [...current.rows, ...page.items],
        cursor: page.next_cursor,
      });
    } catch (error) {
      setMessage(error instanceof Error ? error.message : t("opsLoadFailed"));
    }
  }

  async function retryDeadLetter(deliveryId: string) {
    try {
      await requeueDeadLetter(tenantId, deliveryId);
      const page = await listOpsDeadLetters(tenantId, { limit: 20 });
      setDeadLetters(paged(page.items, page.next_cursor));
      setMessage(null);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : t("opsRetryFailed"));
    }
  }

  const memoryColumns = useMemo(
    () => [
      { key: "subject_id", label: t("opsColumnSubject") },
      { key: "source_session_id", label: t("opsColumnSession") },
      { key: "content_preview", label: t("opsColumnContent") },
      { key: "is_valid", label: t("opsColumnValid") },
      { key: "invalidation_reason", label: t("opsColumnReason") },
    ],
    [],
  );

  return (
    <section className="workspace">
      <h2>{t("opsTitle")}</h2>
      <p className="ops-hint">{t("opsHint")}</p>
      <label>
        {t("tenantLabel")}
        <select
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
      {message ? <p className="ops-error">{message}</p> : null}

      <Section title={t("opsDeadLetters")} error={null}>
        <Table
          columns={[
            { key: "delivery_id", label: t("opsColumnDelivery") },
            { key: "external_conversation_id", label: t("opsColumnConversation") },
            { key: "attempts", label: t("opsColumnAttempts") },
            { key: "last_error", label: t("opsColumnLastError") },
            { key: "updated_at", label: t("opsColumnUpdated") },
          ]}
          rows={deadLetters.rows}
        />
        <ul className="ops-actions">
          {(deadLetters.rows as unknown as OpsDeadLetter[]).map((letter) => (
            <li key={letter.delivery_id}>
              <button type="button" onClick={() => retryDeadLetter(letter.delivery_id)}>
                {t("opsRetry")} {letter.delivery_id.slice(0, 8)}
              </button>
            </li>
          ))}
        </ul>
        {deadLetters.cursor ? (
          <button
            type="button"
            onClick={() =>
              loadMore(
                (cursor) => listOpsDeadLetters(tenantId, { cursor, limit: 20 }),
                deadLetters,
                setDeadLetters,
              )
            }
          >
            {t("opsLoadMore")}
          </button>
        ) : null}
      </Section>

      <Section title={t("opsOperations")}>
        <Table
          columns={[
            { key: "kind", label: t("opsColumnKind") },
            { key: "id", label: t("opsColumnId") },
            { key: "status", label: t("opsColumnStatus") },
            { key: "attempts", label: t("opsColumnAttempts") },
            { key: "next_action_at", label: t("opsColumnNextAction") },
            { key: "last_error", label: t("opsColumnLastError") },
          ]}
          rows={operations as unknown as GenericRow[]}
        />
      </Section>

      <Section title={t("opsSessions")}>
        <Table
          columns={[
            { key: "id", label: t("opsColumnId") },
            { key: "application_id", label: t("opsColumnApplication") },
            { key: "version", label: t("opsColumnVersion") },
            { key: "updated_at", label: t("opsColumnUpdated") },
          ]}
          rows={sessions.rows}
        />
        {sessions.cursor ? (
          <button
            type="button"
            onClick={() =>
              loadMore(
                (cursor) => listOpsSessions(tenantId, { cursor, limit: 20 }),
                sessions,
                setSessions,
              )
            }
          >
            {t("opsLoadMore")}
          </button>
        ) : null}
      </Section>

      <Section title={t("opsMemories")}>
        <Table columns={memoryColumns} rows={memories.rows} />
        {memories.cursor ? (
          <button
            type="button"
            onClick={() =>
              loadMore(
                (cursor) => listOpsMemories(tenantId, { cursor, limit: 20 }),
                memories,
                setMemories,
              )
            }
          >
            {t("opsLoadMore")}
          </button>
        ) : null}
      </Section>

      <Section title={t("opsArtifacts")}>
        <Table
          columns={[
            { key: "filename", label: t("opsColumnFilename") },
            { key: "media_type", label: t("opsColumnMediaType") },
            { key: "size_bytes", label: t("opsColumnSize") },
            { key: "classification", label: t("opsColumnClassification") },
            { key: "expires_at", label: t("opsColumnExpires") },
          ]}
          rows={artifacts.rows}
        />
        {artifacts.cursor ? (
          <button
            type="button"
            onClick={() =>
              loadMore(
                (cursor) => listOpsArtifacts(tenantId, { cursor, limit: 20 }),
                artifacts,
                setArtifacts,
              )
            }
          >
            {t("opsLoadMore")}
          </button>
        ) : null}
      </Section>

      <Section title={t("opsApprovals")}>
        <Table
          columns={[
            { key: "tool_name", label: t("opsColumnTool") },
            { key: "side_effect", label: t("opsColumnSideEffect") },
            { key: "status", label: t("opsColumnStatus") },
            { key: "requested_by", label: t("opsColumnRequestedBy") },
            { key: "requested_at", label: t("opsColumnRequestedAt") },
          ]}
          rows={approvals}
        />
      </Section>

      <Section title={t("opsAudit")}>
        <Table
          columns={[
            { key: "occurred_at", label: t("opsColumnOccurred") },
            { key: "actor", label: t("opsColumnActor") },
            { key: "action", label: t("opsColumnAction") },
            { key: "decision", label: t("opsColumnDecision") },
          ]}
          rows={auditEvents}
        />
      </Section>

      <Section title={t("opsBudgets")}>
        <Table
          columns={[
            { key: "budget_type", label: t("opsColumnType") },
            { key: "limit_amount", label: t("opsColumnLimit") },
            { key: "spent_amount", label: t("opsColumnSpent") },
            { key: "status", label: t("opsColumnStatus") },
          ]}
          rows={budgets}
        />
      </Section>

      <Section title={t("opsMigrations")}>
        <Table
          columns={[
            { key: "state", label: t("opsColumnStatus") },
            { key: "approval_status", label: t("opsColumnApproval") },
            { key: "requested_by", label: t("opsColumnRequestedBy") },
            { key: "updated_at", label: t("opsColumnUpdated") },
          ]}
          rows={migrations}
        />
      </Section>

      <Section title={t("opsKnowledge")}>
        <Table
          columns={[
            { key: "slug", label: t("opsColumnSlug") },
            { key: "name", label: t("opsColumnName") },
            { key: "version", label: t("opsColumnVersion") },
            { key: "created_at", label: t("opsColumnCreated") },
          ]}
          rows={knowledgeBases}
        />
      </Section>
    </section>
  );
}
