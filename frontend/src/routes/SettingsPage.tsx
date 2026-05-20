import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckSquare, Download, Loader2, RefreshCw, Save, Search, Square, Trash2, UserX } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { AuditLogSchema, PriceHistorySchema, PriceRetryQueueSchema, SettingsResponseSchema, SyncStatusSchema, UsersResponseSchema, UsersStatsResponseSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { asRecord, compactDate, errorMessage, numberValue } from "../lib/common";

type MarkupRuleDraft = {
  marketplace: string;
  minUsd: number;
  coefficient: number;
};

type AvailabilityRuleDraft = {
  marketplace: string;
  minAvailableSuppliers: number;
  coefficientDelta: number;
  targetStock: number;
};

type SettingsTab = "prices" | "marketplaces" | "ai" | "users" | "audit" | "system";

const settingsTabs: Array<{ id: SettingsTab; label: string }> = [
  { id: "prices", label: "Цены" },
  { id: "marketplaces", label: "Маркетплейсы" },
  { id: "ai", label: "AI" },
  { id: "users", label: "Сотрудники" },
  { id: "audit", label: "Аудит" },
  { id: "system", label: "Система" },
];

const defaultAvailabilityRule: AvailabilityRuleDraft = {
  marketplace: "all",
  minAvailableSuppliers: 1,
  coefficientDelta: 0,
  targetStock: 3,
};

const codexSaleAiPreset = {
  enabled: true,
  providerId: "codexsale",
  baseUrl: "https://codex.sale/v1",
  textModel: "gpt-5.4-mini",
  imageModel: "gpt-image-2",
  imageSize: "1024x1024",
  imageQuality: "auto",
  imageFormat: "png",
};

function normalizeMarketplace(value: unknown) {
  const text = String(value || "all").toLowerCase();
  return text === "ozon" || text === "yandex" ? text : "all";
}

function settingsArray(value: unknown) {
  return Array.isArray(value) ? value.map(asRecord) : [];
}

function readMarkupRules(value: unknown): MarkupRuleDraft[] {
  return settingsArray(value).map((rule) => ({
    marketplace: normalizeMarketplace(rule.marketplace),
    minUsd: numberValue(rule.minUsd, 0),
    coefficient: numberValue(rule.coefficient, 1),
  }));
}

function readAvailabilityRules(value: unknown): AvailabilityRuleDraft[] {
  return settingsArray(value).map((rule) => ({
    marketplace: normalizeMarketplace(rule.marketplace),
    minAvailableSuppliers: Math.max(0, Math.round(numberValue(rule.minAvailableSuppliers, 1))),
    coefficientDelta: numberValue(rule.coefficientDelta, 0),
    targetStock: Math.max(0, Math.round(numberValue(rule.targetStock, 3))),
  }));
}

function settingsSavePayload(draft: Record<string, unknown>) {
  const markups = asRecord(draft.defaultMarkups);
  const markupRules = readMarkupRules(draft.markupRules)
    .filter((rule) => Number.isFinite(rule.coefficient) && rule.coefficient > 0)
    .map((rule) => ({
      marketplace: rule.marketplace,
      minUsd: Math.max(0, rule.minUsd),
      coefficient: rule.coefficient,
    }));
  const availabilityRules = readAvailabilityRules(draft.availabilityRules);
  return {
    ...draft,
    fixedUsdRate: numberValue(draft.fixedUsdRate, 95),
    defaultMarkups: {
      ...markups,
      ozon: numberValue(markups.ozon, 1.7),
      yandex: numberValue(markups.yandex, 1.6),
    },
    markupRules,
    availabilityRules: availabilityRules.length ? availabilityRules : [defaultAvailabilityRule],
  };
}

function recordNumber(record: Record<string, unknown>, key: string) {
  const value = Number(record[key] || 0);
  return Number.isFinite(value) ? value : 0;
}

function userStatusText(row: Record<string, unknown>) {
  if (row.deleted || row.hardDeleted) return "удален";
  if (row.active === false || row.disabled === true) return "выключен";
  return "активен";
}

function saveBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function UsersSettingsPanel() {
  const queryClient = useQueryClient();
  const [form, setForm] = useState({ username: "", password: "", role: "manager" });
  const [statsPeriod, setStatsPeriod] = useState("30d");
  const [selectedStatsUsers, setSelectedStatsUsers] = useState<string[]>([]);
  const [includeInactiveStats, setIncludeInactiveStats] = useState(true);
  const [includeDeletedStats, setIncludeDeletedStats] = useState(true);
  const usersQuery = useQuery({ queryKey: ["users"], queryFn: () => fetchJson("/api/users", UsersResponseSchema) });
  const statsQuery = useQuery({
    queryKey: ["user-stats", statsPeriod, selectedStatsUsers, includeInactiveStats, includeDeletedStats],
    queryFn: () => {
      const params = new URLSearchParams({
        period: statsPeriod,
        includeInactive: includeInactiveStats ? "1" : "0",
        includeDeleted: includeDeletedStats ? "1" : "0",
      });
      if (selectedStatsUsers.length) params.set("users", selectedStatsUsers.join(","));
      return fetchJson(`/api/users/stats?${params.toString()}`, UsersStatsResponseSchema);
    },
  });
  const refreshUsers = () => {
    void queryClient.invalidateQueries({ queryKey: ["users"] });
    void queryClient.invalidateQueries({ queryKey: ["user-stats"] });
  };
  const createUser = useMutation({
    mutationFn: () => fetchJson("/api/users", UsersResponseSchema, mutationBody(form)),
    onSuccess: () => {
      setForm({ username: "", password: "", role: "manager" });
      refreshUsers();
    },
  });
  const updateUser = useMutation({
    mutationFn: ({ username, patch }: { username: string; patch: Record<string, unknown> }) => fetchJson(
      `/api/users/${encodeURIComponent(username)}`,
      UsersResponseSchema,
      { method: "PUT", body: JSON.stringify(patch), headers: { "Content-Type": "application/json" } },
    ),
    onSuccess: refreshUsers,
  });
  const deleteUser = useMutation({
    mutationFn: (username: string) => fetchJson(
      `/api/users/${encodeURIComponent(username)}`,
      UsersResponseSchema,
      { method: "DELETE" },
    ),
    onSuccess: refreshUsers,
  });
  const hardDeleteUser = useMutation({
    mutationFn: (username: string) => fetchJson(
      `/api/users/${encodeURIComponent(username)}?hard=true`,
      UsersResponseSchema,
      { method: "DELETE" },
    ),
    onSuccess: refreshUsers,
  });
  const exportStats = useMutation({
    mutationFn: async () => {
      const response = await fetch("/api/users/stats/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          period: statsPeriod,
          usernames: selectedStatsUsers,
          includeInactive: includeInactiveStats,
          includeDeleted: includeDeletedStats,
        }),
      });
      if (!response.ok) throw new Error(await response.text());
      return response.blob();
    },
    onSuccess: (blob) => saveBlob(blob, `magic-vibe-user-stats-${statsPeriod}.pdf`),
  });
  const users = usersQuery.data?.users || [];
  const stats = statsQuery.data?.users || [];
  const summary = asRecord(statsQuery.data?.summary);
  const statsUsers = Array.from(new Set([
    ...users.map((user) => String(user.username || "")).filter(Boolean),
    ...stats.map((user) => String(user.username || "")).filter(Boolean),
  ])).sort((a, b) => a.localeCompare(b));
  const toggleStatsUser = (username: string) => {
    setSelectedStatsUsers((current) => (
      current.includes(username) ? current.filter((item) => item !== username) : [...current, username]
    ));
  };
  return (
    <>
    <section className="settings-panel settings-panel-wide">
      <div className="section-title"><div><span>Доступ</span><h3>Сотрудники и роли</h3></div></div>
      <div className="settings-form-row">
        <input placeholder="Логин" value={form.username} onChange={(event) => setForm({ ...form, username: event.target.value })} />
        <input placeholder="Пароль минимум 6 символов" type="password" value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} />
        <select value={form.role} onChange={(event) => setForm({ ...form, role: event.target.value })}>
          <option value="manager">manager</option>
          <option value="admin">admin</option>
        </select>
        <button className="primary-action" type="button" disabled={createUser.isPending || !form.username || form.password.length < 6} onClick={() => createUser.mutate()}>Добавить</button>
      </div>
      {usersQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю сотрудников...</div>}
      {users.map((user) => {
        const username = String(user.username || "");
        const role = String(user.role || "manager");
        const active = user.active !== false && user.disabled !== true;
        const protectedUser = Boolean(user.protected);
        return (
          <article className="job-row" key={username}>
            <div>
              <strong>{username}</strong>
              <span>{role} · {active ? "активен" : "выключен"} · {String(user.source || "local")}</span>
            </div>
            <div className="row-actions">
              <button className="secondary-action" type="button" disabled={protectedUser || updateUser.isPending} onClick={() => updateUser.mutate({ username, patch: { role: role === "admin" ? "manager" : "admin" } })}>{role === "admin" ? "Сделать manager" : "Сделать admin"}</button>
              <button className="secondary-action" type="button" disabled={protectedUser || updateUser.isPending} onClick={() => updateUser.mutate({ username, patch: { active: !active } })}>{active ? "Выключить" : "Включить"}</button>
              <button className="icon-action danger" type="button" disabled={protectedUser || deleteUser.isPending} onClick={() => deleteUser.mutate(username)} title="Удалить"><Trash2 size={15} /></button>
              <button className="secondary-action danger-action" type="button" disabled={hardDeleteUser.isPending} onClick={() => {
                if (window.confirm("Удалить " + username + " полностью из списка? История действий сохранится.")) hardDeleteUser.mutate(username);
              }}><UserX size={15} /> Удалить полностью</button>
            </div>
          </article>
        );
      })}
      {(createUser.error || updateUser.error || deleteUser.error || hardDeleteUser.error) && <div className="inline-error">{errorMessage(createUser.error || updateUser.error || deleteUser.error || hardDeleteUser.error)}</div>}
    </section>
    <section className="settings-panel settings-panel-wide">
      <div className="section-title">
        <div><span>PriceMaster</span><h3>Статистика привязок</h3></div>
        <div className="row-actions">
          <select value={statsPeriod} onChange={(event) => setStatsPeriod(event.target.value)}>
            <option value="7d">7 дней</option>
            <option value="30d">30 дней</option>
            <option value="90d">90 дней</option>
            <option value="all">Все</option>
          </select>
          <button className="secondary-action" type="button" onClick={() => statsQuery.refetch()}><RefreshCw size={16} /> Обновить</button>
        </div>
      </div>
      <div className="employee-stats-dashboard">
        <DiagnosticValue label="Сотрудников" value={recordNumber(summary, "totalUsers")} />
        <DiagnosticValue label="Действий" value={recordNumber(summary, "actionsTotal")} />
        <DiagnosticValue label="Добавлено" value={recordNumber(summary, "linksAdded")} tone="success" />
        <DiagnosticValue label="Товаров" value={recordNumber(summary, "affectedProducts")} />
      </div>
      <div className="employee-stats-controls">
        <label className="toggle-filter"><input type="checkbox" checked={includeInactiveStats} onChange={(event) => setIncludeInactiveStats(event.target.checked)} /> Выключенные</label>
        <label className="toggle-filter"><input type="checkbox" checked={includeDeletedStats} onChange={(event) => setIncludeDeletedStats(event.target.checked)} /> Удаленные</label>
        <button className="secondary-action" type="button" disabled={!selectedStatsUsers.length} onClick={() => setSelectedStatsUsers([])}>Все сотрудники</button>
        <button className="primary-action" type="button" disabled={exportStats.isPending} onClick={() => exportStats.mutate()}><Download size={16} /> Экспорт PDF</button>
      </div>
      <div className="employee-user-picker">
        {statsUsers.map((username) => {
          const selected = selectedStatsUsers.includes(username);
          return (
            <button className={selected ? "employee-user-chip is-selected" : "employee-user-chip"} type="button" key={username} onClick={() => toggleStatsUser(username)}>
              {selected ? <CheckSquare size={14} /> : <Square size={14} />} {username}
            </button>
          );
        })}
      </div>
      {exportStats.error && <div className="inline-error">{errorMessage(exportStats.error)}</div>}
      {statsQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Считаю статистику...</div>}
      {!statsQuery.isLoading && !stats.length && <div className="soft-empty">По выбранному периоду действий нет.</div>}
      {stats.map((row) => (
        <article className="job-row employee-stats-row" key={String(row.username)}>
          <div>
            <strong>{String(row.username || "system")}</strong>
            <span>{String(row.role || "-")} · {userStatusText(row)} · действий {Number(row.actionsTotal || 0)}</span>
            <small>последнее: {compactDate(String(row.lastActionAt || "")) || "нет действий за период"}</small>
          </div>
          <div className="employee-stats-grid">
            <span><b>{Number(row.currentLinksCreated || 0)}</b>создал активных</span>
            <span><b>{Number(row.currentLinksUpdated || 0)}</b>изменил активных</span>
            <span><b>{Number(row.linksAdded || 0)}</b>добавил</span>
            <span><b>{Number(row.linksUpdated || 0)}</b>обновил</span>
            <span><b>{Number(row.linksDeleted || 0)}</b>удалил</span>
            <span><b>{Number(row.affectedProducts || 0)}</b>товаров</span>
          </div>
        </article>
      ))}
      {statsQuery.error && <div className="inline-error">{errorMessage(statsQuery.error)}</div>}
    </section>
    </>
  );
}

function AuditSettingsPanel() {
  const [q, setQ] = useState("");
  const auditQuery = useQuery({
    queryKey: ["audit", q],
    queryFn: () => fetchJson(`/api/audit-log?limit=120&q=${encodeURIComponent(q)}`, AuditLogSchema),
  });
  const audit = auditQuery.data?.audit || [];
  return (
    <section className="settings-panel settings-panel-wide">
      <div className="section-title">
        <div><span>Журнал</span><h3>Аудит действий</h3></div>
        <button className="secondary-action" type="button" onClick={() => auditQuery.refetch()}><RefreshCw size={16} /> Обновить</button>
      </div>
      <div className="search-box compact-search"><Search size={16} /><input placeholder="Поиск по пользователю, SKU, действию" value={q} onChange={(event) => setQ(event.target.value)} /></div>
      {auditQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю аудит...</div>}
      {audit.map((entry, index) => (
        <article className="job-row" key={`${String(entry.id || entry.createdAt || "")}-${index}`}>
          <div>
            <strong>{String(entry.action || "action")}</strong>
            <span>{String(entry.username || entry.user || "system")} · {compactDate(String(entry.createdAt || entry.time || ""))}</span>
            <small>{String(entry.entityType || "")} {String(entry.entityId || "")}</small>
          </div>
        </article>
      ))}
      {!auditQuery.isLoading && !audit.length && <div className="soft-empty">Записей аудита не найдено.</div>}
    </section>
  );
}

function SystemSettingsPanel() {
  const queryClient = useQueryClient();
  const retryQuery = useQuery({ queryKey: ["price-retry"], queryFn: () => fetchJson("/api/warehouse/prices/retry-queue", PriceRetryQueueSchema) });
  const historyQuery = useQuery({ queryKey: ["price-history"], queryFn: () => fetchJson("/api/warehouse/prices/history?limit=80", PriceHistorySchema) });
  const syncQuery = useQuery({ queryKey: ["warehouse-sync"], queryFn: () => fetchJson("/api/warehouse/sync/status", SyncStatusSchema) });
  const dailyQuery = useQuery({ queryKey: ["daily-sync"], queryFn: () => fetchJson("/api/daily-sync", SyncStatusSchema) });
  const runSync = useMutation({
    mutationFn: () => fetchJson("/api/warehouse/sync/run", SyncStatusSchema, mutationBody({})),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["warehouse-sync"] }),
  });
  const runDaily = useMutation({
    mutationFn: () => fetchJson("/api/daily-sync/run", SyncStatusSchema, mutationBody({})),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["daily-sync"] }),
  });
  const retryPrices = useMutation({
    mutationFn: () => fetchJson("/api/warehouse/prices/retry", SyncStatusSchema, mutationBody({ confirmed: true })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["price-retry"] });
      void queryClient.invalidateQueries({ queryKey: ["price-history"] });
    },
  });
  const clearRetry = useMutation({
    mutationFn: () => fetchJson("/api/warehouse/prices/retry-queue", SyncStatusSchema, { method: "DELETE" }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["price-retry"] }),
  });
  const retryItems = retryQuery.data?.items || [];
  const historyItems = historyQuery.data?.items || [];
  return (
    <section className="settings-grid pricing-settings-grid">
      <div className="settings-panel">
        <div className="section-title"><div><span>Sync</span><h3>Синхронизация</h3></div></div>
        <DiagnosticValue label="Warehouse sync" value={String(syncQuery.data?.status || syncQuery.data?.running || "-")} />
        <DiagnosticValue label="Daily sync" value={String(dailyQuery.data?.status || dailyQuery.data?.running || "-")} />
        <div className="row-actions">
          <button className="primary-action" type="button" disabled={runSync.isPending} onClick={() => runSync.mutate()}>Запустить warehouse sync</button>
          <button className="secondary-action" type="button" disabled={runDaily.isPending} onClick={() => runDaily.mutate()}>Запустить daily sync</button>
        </div>
      </div>
      <div className="settings-panel">
        <div className="section-title"><div><span>Цены</span><h3>Retry queue</h3></div></div>
        <DiagnosticValue label="В очереди" value={retryQuery.data?.total || retryItems.length} tone={retryItems.length ? "warn" : ""} />
        <div className="row-actions">
          <button className="primary-action" type="button" disabled={!retryItems.length || retryPrices.isPending} onClick={() => retryPrices.mutate()}>Повторить цены</button>
          <button className="secondary-action" type="button" disabled={!retryItems.length || clearRetry.isPending} onClick={() => clearRetry.mutate()}>Очистить очередь</button>
        </div>
      </div>
      <div className="settings-panel settings-panel-wide">
        <div className="section-title"><div><span>История</span><h3>Последние отправки цен</h3></div><button className="secondary-action" type="button" onClick={() => historyQuery.refetch()}><RefreshCw size={16} /> Обновить</button></div>
        {historyQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю историю цен...</div>}
        {historyItems.slice(0, 40).map((item, index) => (
          <article className="job-row" key={`${String(item.id || item.offerId || "")}-${index}`}>
            <div>
              <strong>{String(item.offerId || item.productId || "-")}</strong>
              <span>{String(item.marketplace || "-")} · {String(item.status || "-")} · {compactDate(String(item.createdAt || item.sentAt || item.at || ""))}</span>
              <small>цена {String(item.requestedPrice || item.price || "-")} · {String(item.error || item.detail || "")}</small>
            </div>
          </article>
        ))}
        {!historyQuery.isLoading && !historyItems.length && <div className="soft-empty">Истории отправки цен пока нет.</div>}
      </div>
      {(runSync.error || runDaily.error || retryPrices.error || clearRetry.error) && <div className="inline-error">{errorMessage(runSync.error || runDaily.error || retryPrices.error || clearRetry.error)}</div>}
    </section>
  );
}

export function SettingsPage() {
  const queryClient = useQueryClient();
  const settingsQuery = useQuery({ queryKey: ["settings"], queryFn: () => fetchJson("/api/settings", SettingsResponseSchema) });
  const settings = settingsQuery.data?.settings || {};
  const ai = asRecord(settings.ai);
  const markups = asRecord(settings.defaultMarkups);
  const [draft, setDraft] = useState<Record<string, unknown>>({});
  const [activeTab, setActiveTab] = useState<SettingsTab>("prices");

  useEffect(() => {
    if (settingsQuery.data?.settings) setDraft(settingsQuery.data.settings);
  }, [settingsQuery.data]);

  const draftAi = asRecord(draft.ai);
  const draftMarkups = asRecord(draft.defaultMarkups);
  const markupRules = readMarkupRules(draft.markupRules);
  const availabilityRules = readAvailabilityRules(draft.availabilityRules);
  const save = useMutation({
    mutationFn: () => fetchJson("/api/settings", SettingsResponseSchema, mutationBody(settingsSavePayload(draft))),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
  const testAi = useMutation({
    mutationFn: () => fetchJson("/api/settings/ai/test", SettingsResponseSchema, mutationBody({ ai: draftAi })),
  });
  const update = (patch: Record<string, unknown>) => setDraft((current) => ({ ...current, ...patch }));
  const updateAi = (patch: Record<string, unknown>) => update({ ai: { ...draftAi, ...patch } });
  const updateMarkups = (patch: Record<string, unknown>) => update({ defaultMarkups: { ...draftMarkups, ...patch } });
  const setMarkupRules = (rules: MarkupRuleDraft[]) => update({ markupRules: rules });
  const setAvailabilityRules = (rules: AvailabilityRuleDraft[]) => update({ availabilityRules: rules });
  const updateMarkupRule = (index: number, patch: Partial<MarkupRuleDraft>) => {
    setMarkupRules(markupRules.map((rule, ruleIndex) => (ruleIndex === index ? { ...rule, ...patch } : rule)));
  };
  const updateAvailabilityRule = (index: number, patch: Partial<AvailabilityRuleDraft>) => {
    setAvailabilityRules(availabilityRules.map((rule, ruleIndex) => (ruleIndex === index ? { ...rule, ...patch } : rule)));
  };
  const saveButton = (
    <button className="primary-action" onClick={() => save.mutate()} disabled={save.isPending}>
      {save.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить
    </button>
  );

  return (
    <>
      <PageHeader title="Настройки" subtitle="Курс, наценки, правила доступности, маркетплейсы и AI-провайдер в новом интерфейсе." action={saveButton} />
      <nav className="settings-tabs" aria-label="settings sections">
        {settingsTabs.map((tab) => (
          <button key={tab.id} className={activeTab === tab.id ? "is-active" : ""} type="button" onClick={() => setActiveTab(tab.id)}>{tab.label}</button>
        ))}
      </nav>
      {settingsQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю настройки...</div>}
      {activeTab === "prices" && <section className="settings-grid pricing-settings-grid">
        <div className="settings-panel settings-panel-wide">
          <div className="settings-hint">Ozon и Yandex используют общие привязки PriceMaster в объединенной карточке, но цена считается отдельно по своему базовому коэффициенту, правилам наценки и доступности.</div>
        </div>
        <div className="settings-panel">
          <div className="section-title"><div><span>Цены</span><h3>Базовые цены</h3></div></div>
          <label>Курс USD/RUB<input type="number" min="0.0001" step="0.0001" value={String(draft.fixedUsdRate ?? settings.fixedUsdRate ?? "")} onChange={(event) => update({ fixedUsdRate: numberValue(event.target.value) })} /></label>
          <label>Базовая наценка Ozon<input type="number" min="0.0001" step="0.0001" value={String(draftMarkups.ozon ?? markups.ozon ?? "")} onChange={(event) => updateMarkups({ ozon: numberValue(event.target.value) })} /></label>
          <label>Базовая наценка Yandex Market<input type="number" min="0.0001" step="0.0001" value={String(draftMarkups.yandex ?? markups.yandex ?? "")} onChange={(event) => updateMarkups({ yandex: numberValue(event.target.value) })} /></label>
          <div className="soft-empty compact">После изменения курса или наценки backend ставит пересчет цен в очередь.</div>
        </div>

        <div className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div><span>Наценки</span><h3>Гибкие правила наценки</h3></div>
            <button className="secondary-action" type="button" onClick={() => setMarkupRules([...markupRules, { marketplace: "all", minUsd: 0, coefficient: 1 }])}>Добавить</button>
          </div>
          <p className="settings-hint">Правила применяются по цене поставщика в USD. Пустой список допустим: тогда используются базовые наценки.</p>
          <div className="settings-rule-table">
            <div className="settings-rule-head"><span>Маркетплейс</span><span>От цены, USD</span><span>Коэффициент</span><span></span></div>
            {markupRules.map((rule, index) => (
              <div className="settings-rule-row" key={`markup-${index}`}>
                <select value={rule.marketplace} onChange={(event) => updateMarkupRule(index, { marketplace: event.target.value })}>
                  <option value="all">Все</option>
                  <option value="ozon">Ozon</option>
                  <option value="yandex">Yandex Market</option>
                </select>
                <input type="number" min="0" step="0.0001" value={String(rule.minUsd)} onChange={(event) => updateMarkupRule(index, { minUsd: numberValue(event.target.value) })} />
                <input type="number" min="0.0001" step="0.0001" value={String(rule.coefficient)} onChange={(event) => updateMarkupRule(index, { coefficient: numberValue(event.target.value, 1) })} />
                <button className="icon-action danger" type="button" title="Удалить правило" onClick={() => setMarkupRules(markupRules.filter((_, ruleIndex) => ruleIndex !== index))}><Trash2 size={15} /></button>
              </div>
            ))}
            {!markupRules.length && <div className="soft-empty compact">Гибких правил нет. Будут использоваться базовые наценки Ozon/Yandex.</div>}
          </div>
        </div>

        <div className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div><span>Остатки</span><h3>Доступность и остатки</h3></div>
            <button className="secondary-action" type="button" onClick={() => setAvailabilityRules([...availabilityRules, defaultAvailabilityRule])}>Добавить</button>
          </div>
          <p className="settings-hint">Если доступных поставщиков много, правило может снизить коэффициент и поднять целевой остаток. При пустом списке при сохранении добавится безопасное правило all / 1 / 0 / 3.</p>
          <div className="settings-rule-table availability-rule-table">
            <div className="settings-rule-head"><span>Маркетплейс</span><span>Поставщиков от</span><span>Поправка</span><span>Остаток</span><span></span></div>
            {availabilityRules.map((rule, index) => (
              <div className="settings-rule-row" key={`availability-${index}`}>
                <select value={rule.marketplace} onChange={(event) => updateAvailabilityRule(index, { marketplace: event.target.value })}>
                  <option value="all">Все</option>
                  <option value="ozon">Ozon</option>
                  <option value="yandex">Yandex Market</option>
                </select>
                <input type="number" min="0" step="1" value={String(rule.minAvailableSuppliers)} onChange={(event) => updateAvailabilityRule(index, { minAvailableSuppliers: Math.round(numberValue(event.target.value, 1)) })} />
                <input type="number" step="0.0001" value={String(rule.coefficientDelta)} onChange={(event) => updateAvailabilityRule(index, { coefficientDelta: numberValue(event.target.value) })} />
                <input type="number" min="0" step="1" value={String(rule.targetStock)} onChange={(event) => updateAvailabilityRule(index, { targetStock: Math.round(numberValue(event.target.value, 3)) })} />
                <button className="icon-action danger" type="button" title="Удалить правило" onClick={() => setAvailabilityRules(availabilityRules.filter((_, ruleIndex) => ruleIndex !== index))}><Trash2 size={15} /></button>
              </div>
            ))}
            {!availabilityRules.length && <div className="soft-empty compact">При сохранении будет добавлено дефолтное правило: все площадки, от 1 поставщика, поправка 0, остаток 3.</div>}
          </div>
        </div>
      </section>}

      {activeTab === "ai" && <section className="settings-grid pricing-settings-grid">
        <div className="settings-panel">
          <div className="section-title">
            <div><span>AI</span><h3>Провайдер и модели</h3></div>
            <div className="section-actions">
              <button className="secondary-action" type="button" onClick={() => updateAi({ ...codexSaleAiPreset, apiKey: draftAi.apiKey || "" })}>Codex Sale preset</button>
              <button className="secondary-action" onClick={() => testAi.mutate()} disabled={testAi.isPending}>Тест</button>
            </div>
          </div>
          <label>Provider ID<input value={String(draftAi.providerId ?? ai.providerId ?? "")} onChange={(event) => updateAi({ providerId: event.target.value })} /></label>
          <label>Base URL<input value={String(draftAi.baseUrl ?? ai.baseUrl ?? "")} onChange={(event) => updateAi({ baseUrl: event.target.value })} /></label>
          <label>Text model<input value={String(draftAi.textModel ?? ai.textModel ?? "")} onChange={(event) => updateAi({ textModel: event.target.value })} /></label>
          <label>Image model<input value={String(draftAi.imageModel ?? ai.imageModel ?? "gpt-image-2")} onChange={(event) => updateAi({ imageModel: event.target.value })} /></label>
          <label>API key<input type="password" placeholder={ai.apiKeySet ? "ключ сохранен" : "вставьте ключ"} onChange={(event) => updateAi({ apiKey: event.target.value })} /></label>
          {testAi.error && <div className="inline-error">{errorMessage(testAi.error)}</div>}
          {testAi.isSuccess && <div className="success-strip">AI подключен.</div>}
        </div>
      </section>}

      {activeTab === "marketplaces" && <section className="settings-grid pricing-settings-grid">
        <div className="settings-panel">
          <div className="section-title"><div><span>Yandex</span><h3>Склад остатков</h3></div></div>
          <label>Warehouse ID<input value={String(asRecord(draft.yandex).warehouseId ?? asRecord(settings.yandex).warehouseId ?? "128820967")} onChange={(event) => update({ yandex: { ...asRecord(draft.yandex), warehouseId: event.target.value } })} /></label>
          <div className="soft-empty compact">Остатки должны уходить только в Magic Stick: 128820967.</div>
        </div>

        <div className="settings-panel">
          <div className="section-title"><div><span>Fallback</span><h3>Старый интерфейс</h3></div></div>
          <div className="soft-empty compact">Legacy оставлен только как аварийный fallback на время приемки нового интерфейса.</div>
          <a className="secondary-action" href="/legacy">Открыть legacy</a>
        </div>
      </section>}

      {activeTab === "users" && <UsersSettingsPanel />}
      {activeTab === "audit" && <AuditSettingsPanel />}
      {activeTab === "system" && <SystemSettingsPanel />}

      {(save.error) && <div className="inline-error">{errorMessage(save.error)}</div>}
      {save.isSuccess && (
        <div className={save.data?.priceRepriceQueueError ? "inline-error" : "success-strip"}>
          {save.data?.priceRepriceQueueError
            ? `Настройки сохранены, но пересчет цен не поставился в очередь: ${save.data.priceRepriceQueueError}`
            : save.data?.priceAffectingChanged
              ? (save.data.priceRepriceQueued ? "Настройки сохранены. Курс/наценки изменились, пересчет цен поставлен в очередь." : "Настройки сохранены. Курс/наценки изменились, но очередь пересчета не подтвердилась.")
              : "Настройки сохранены. Ценовые правила не менялись, пересчет не нужен."}
        </div>
      )}
      <div className="settings-save-footer">{saveButton}</div>
    </>
  );
}
