import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckSquare, Download, Eye, Loader2, Percent, RefreshCw, Save, Search, Square, Trash2, Upload, UserX } from "lucide-react";
import { fetchJson, mutationBody, patchBody } from "../api";
import { AuditLogSchema, PriceHistorySchema, PriceRetryQueueSchema, SettingsResponseSchema, SuppliersResponseSchema, SyncStatusSchema, UsersResponseSchema, UsersStatsResponseSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { PageAccessModal } from "../components/PageAccessModal";
import { MarketplaceAccountsPanel } from "../components/MarketplaceAccountsPanel";
import { asRecord, compactDate, errorMessage, numberValue } from "../lib/common";
import { readNotificationSoundSettings, writeNotificationSoundSettings, playNotificationSound, NotificationSoundSettings } from "../components/NotificationsBell";



function SettingsHubPanel() {
  const sections: Array<{ href: string; title: string; note: string }> = [
    { href: "/app/suppliers", title: "Поставщики", note: "Стоп-склады, активность, импорт из PriceMaster" },
    { href: "/app/prices", title: "Цены", note: "Коэффициенты, правила наценки, отправка" },
    { href: "/app/operations", title: "Операции", note: "Журнал операций и запуски" },
    { href: "/app/no-supplier", title: "Ошибки наличия", note: "Товары без доступного поставщика" },
    { href: "/app/problem-products", title: "Проблемные товары", note: "Ошибки карточек и низкие оценки" },
    { href: "/app/recovery-queue", title: "Восстановление", note: "Очередь разархива Ozon + Яндекс" },
    { href: "/app/system", title: "Система", note: "Диагностика, фоновые процессы" },
    { href: "/app/ai-drafts", title: "AI", note: "Черновики контента и AI-фото" },
    { href: "/app/finance", title: "Финансы", note: "Заказы, себестоимость, прибыль" },
  ];
  return (
    <section className="settings-panel settings-panel-wide">
      <div className="section-title"><div><span>Разделы</span><h3>Все настройки и операции</h3></div></div>
      <div className="settings-hub-grid">
        {sections.map((section) => (
          <a className="settings-hub-card" key={section.href} href={section.href}>
            <strong>{section.title}</strong>
            <small>{section.note}</small>
          </a>
        ))}
      </div>
    </section>
  );
}

function NotificationSettingsPanel() {
  const [settings, setSettings] = useState<NotificationSoundSettings>(() => readNotificationSoundSettings());
  const update = (next: NotificationSoundSettings) => {
    setSettings(next);
    writeNotificationSoundSettings(next);
  };
  const typeLabels: Array<[string, string]> = [["order", "Заказы"], ["chat", "Чаты"], ["review", "Отзывы"], ["question", "Вопросы"]];
  return (
    <section className="settings-panel">
      <div className="section-title"><div><span>Уведомления</span><h3>Звук и события</h3></div></div>
      <label className="settings-toggle">
        <input type="checkbox" checked={settings.enabled} onChange={(event) => update({ ...settings, enabled: event.target.checked })} />
        Звук и тосты включены
      </label>
      <div className="settings-form-row">
        <SelectField
          ariaLabel="Звук уведомления"
          value={settings.sound}
          onChange={(next) => update({ ...settings, sound: next as NotificationSoundSettings["sound"] })}
          options={[
            { value: "ding", label: "Динь" },
            { value: "chime", label: "Перелив" },
            { value: "pop", label: "Поп" },
          ]}
        />
        <button className="secondary-action" type="button" onClick={() => playNotificationSound(settings.sound)}>Проба звука</button>
      </div>
      <div className="settings-toggle-row">
        {typeLabels.map(([type, label]) => (
          <label className="settings-toggle" key={type}>
            <input
              type="checkbox"
              checked={settings.types[type] !== false}
              onChange={(event) => update({ ...settings, types: { ...settings.types, [type]: event.target.checked } })}
            />
            {label}
          </label>
        ))}
      </div>
      <div className="info-strip compact">Настройки хранятся в браузере: на каждом рабочем месте можно выбрать свой звук.</div>
    </section>
  );
}

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

type SettingsTab = "prices" | "marketplaces" | "ai" | "tools" | "users" | "audit" | "system";

const settingsTabs: Array<{ id: SettingsTab; label: string }> = [
  { id: "prices", label: "Цены" },
  { id: "marketplaces", label: "Маркетплейсы" },
  { id: "ai", label: "AI" },
  { id: "tools", label: "Инструменты" },
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
  return text === "ozon" || text === "yandex" || text === "avito" || text === "wb" ? text : "all";
}

const PRICING_MARKETPLACES = ["ozon", "yandex", "avito", "wb"] as const;

function marketplaceLabel(value: string) {
  if (value === "ozon") return "Ozon";
  if (value === "yandex") return "Yandex Market";
  if (value === "avito") return "Avito";
  if (value === "wb") return "Wildberries";
  return "Все маркетплейсы";
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
      avito: numberValue(markups.avito, 1.6),
      wb: numberValue(markups.wb, 1.6),
    },
    markupRules,
    availabilityRules: availabilityRules.length ? availabilityRules : [defaultAvailabilityRule],
  };
}

function recordNumber(record: Record<string, unknown>, key: string) {
  const value = Number(record[key] || 0);
  return Number.isFinite(value) ? value : 0;
}

function adjustCoefficient(value: unknown, multiplier: number) {
  const current = Number(value || 0);
  if (!Number.isFinite(current) || current <= 0) return 0;
  return Math.max(0.0001, Number((current * multiplier).toFixed(4)));
}

function adjustedRulePreview(rule: MarkupRuleDraft, marketplace: string, multiplier: number) {
  if (marketplace === "all") return [{ ...rule, nextMarketplace: rule.marketplace, nextCoefficient: adjustCoefficient(rule.coefficient, multiplier), splitOnly: false }];
  if (rule.marketplace === marketplace) return [{ ...rule, nextMarketplace: rule.marketplace, nextCoefficient: adjustCoefficient(rule.coefficient, multiplier), splitOnly: false }];
  if (rule.marketplace !== "all") return [];
  const others = PRICING_MARKETPLACES.filter((key) => key !== marketplace);
  return [
    { ...rule, nextMarketplace: marketplace, nextCoefficient: adjustCoefficient(rule.coefficient, multiplier), splitOnly: false },
    ...others.map((other) => ({ ...rule, nextMarketplace: other, nextCoefficient: rule.coefficient, splitOnly: true })),
  ];
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

function brandingMarketplace(settings: Record<string, unknown>, marketplace: "ozon" | "yandex") {
  return asRecord(asRecord(asRecord(settings.branding).marketplaces)[marketplace]);
}

function BrandingLogoPanel({
  marketplace,
  label,
  settings,
  draft,
  update,
}: {
  marketplace: "ozon" | "yandex";
  label: string;
  settings: Record<string, unknown>;
  draft: Record<string, unknown>;
  update: (patch: Record<string, unknown>) => void;
}) {
  const queryClient = useQueryClient();
  const [logoFile, setLogoFile] = useState<File | null>(null);
  const [extraCardFile, setExtraCardFile] = useState<File | null>(null);
  const draftBranding = asRecord(draft.branding);
  const draftMarketplaces = asRecord(draftBranding.marketplaces);
  const current = {
    ...brandingMarketplace(settings, marketplace),
    ...asRecord(draftMarketplaces[marketplace]),
  };
  const shopName = String(current.shopName || (marketplace === "ozon" ? "Magic Stick" : "parfumerius"));
  const logoUrl = String(current.logoUrl || "");
  const updateMarketplace = (patch: Record<string, unknown>) => {
    update({
      branding: {
        ...draftBranding,
        marketplaces: {
          ...draftMarketplaces,
          [marketplace]: {
            ...current,
            ...patch,
          },
        },
      },
    });
  };
  const uploadLogo = useMutation({
    mutationFn: async () => {
      if (!logoFile) throw new Error("Выберите PNG 258x258.");
      const body = new FormData();
      body.set("logo", logoFile);
      body.set("marketplace", marketplace);
      body.set("shopName", shopName);
      const response = await fetch("/api/settings/branding/logo", {
        method: "POST",
        body,
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(String(payload.error || `HTTP ${response.status}`));
      return SettingsResponseSchema.parse(payload);
    },
    onSuccess: (payload) => {
      if (payload.settings) update({ branding: asRecord(payload.settings.branding) });
      setLogoFile(null);
      void queryClient.invalidateQueries({ queryKey: ["settings"] });
    },
  });
  const uploadExtraCard = useMutation({
    mutationFn: async () => {
      if (!extraCardFile) throw new Error("Выберите картинку для 6/7 фото.");
      const body = new FormData();
      body.set("image", extraCardFile);
      body.set("marketplace", marketplace);
      body.set("label", `Фото ${Math.min(7, 6 + (Array.isArray(current.extraCards) ? current.extraCards.length : 0))}`);
      const response = await fetch("/api/settings/branding/extra-card", {
        method: "POST",
        body,
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(String(payload.error || `HTTP ${response.status}`));
      return SettingsResponseSchema.parse(payload);
    },
    onSuccess: (payload) => {
      if (payload.settings) update({ branding: asRecord(payload.settings.branding) });
      setExtraCardFile(null);
      void queryClient.invalidateQueries({ queryKey: ["settings"] });
    },
  });
  const extraCards = Array.isArray(current.extraCards) ? current.extraCards.map(asRecord) : [];
  return (
    <div className="branding-card">
      <div className="branding-logo-preview">
        {logoUrl ? <img src={logoUrl} alt={shopName} /> : <span>{label.slice(0, 2)}</span>}
      </div>
      <div className="branding-fields">
        <div className="section-title compact-title"><div><span>{label}</span><h3>{shopName}</h3></div></div>
        <label>Название магазина<input value={shopName} onChange={(event) => updateMarketplace({ shopName: event.target.value })} /></label>
        <label>Логотип PNG 258x258<input type="file" accept="image/png" onChange={(event) => setLogoFile(event.target.files?.[0] || null)} /></label>
        <label>Доп. фото 6/7<input type="file" accept="image/png,image/jpeg,image/webp" onChange={(event) => setExtraCardFile(event.target.files?.[0] || null)} /></label>
        <div className="draft-actions">
          <button className="secondary-action" type="button" disabled={!logoFile || uploadLogo.isPending} onClick={() => uploadLogo.mutate()}>
            {uploadLogo.isPending ? <Loader2 className="spin" size={16} /> : <Upload size={16} />} Загрузить логотип
          </button>
          <button className="secondary-action" type="button" disabled={!extraCardFile || uploadExtraCard.isPending} onClick={() => uploadExtraCard.mutate()}>
            {uploadExtraCard.isPending ? <Loader2 className="spin" size={16} /> : <Upload size={16} />} Добавить 6/7 фото
          </button>
          {logoUrl ? <span className="formula-chip">PNG 258x258</span> : <span className="formula-chip">Логотип не загружен</span>}
        </div>
        {extraCards.length ? <div className="extra-card-preview-row">
          {extraCards.map((card, index) => (
            <span className="extra-card-preview" key={String(card.id || card.url || index)}>
              {card.url ? <img src={String(card.url)} alt="" /> : null}
              <small>{String(card.label || `Фото ${index + 6}`)}</small>
            </span>
          ))}
        </div> : <div className="soft-empty compact">Дополнительные 6/7 фото для {label} пока не загружены.</div>}
        {uploadLogo.error && <div className="inline-error">{errorMessage(uploadLogo.error)}</div>}
        {uploadExtraCard.error && <div className="inline-error">{errorMessage(uploadExtraCard.error)}</div>}
        {uploadLogo.isSuccess && <div className="success-strip">Логотип сохранен.</div>}
        {uploadExtraCard.isSuccess && <div className="success-strip">Дополнительное фото сохранено.</div>}
      </div>
    </div>
  );
}

function UsersSettingsPanel() {
  const queryClient = useQueryClient();
  const [form, setForm] = useState({ username: "", password: "", role: "manager" });
  const [accessUser, setAccessUser] = useState<{ username: string; role: string; allowedPages: string[] | null } | null>(null);
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
  const saveAccess = useMutation({
    mutationFn: ({ username, pages }: { username: string; pages: string[] | null }) => fetchJson(
      `/api/users/${encodeURIComponent(username)}`,
      UsersResponseSchema,
      { method: "PUT", body: JSON.stringify({ allowedPages: pages }), headers: { "Content-Type": "application/json" } },
    ),
    onSuccess: () => {
      setAccessUser(null);
      refreshUsers();
    },
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
    <div className="settings-stack">
    <SettingsHubPanel />
    <NotificationSettingsPanel />
    <section className="settings-panel settings-panel-wide">
      <div className="section-title"><div><span>Доступ</span><h3>Сотрудники и роли</h3></div></div>
      <div className="settings-form-row">
        <input placeholder="Логин" value={form.username} onChange={(event) => setForm({ ...form, username: event.target.value })} />
        <input placeholder="Пароль минимум 6 символов" type="password" value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} />
        <SelectField
          ariaLabel="Роль"
          value={form.role}
          onChange={(next) => setForm({ ...form, role: next })}
          options={[
            { value: "manager", label: "manager" },
            { value: "admin", label: "admin" },
          ]}
        />
        <button className="primary-action" type="button" disabled={createUser.isPending || !form.username || form.password.length < 6} onClick={() => createUser.mutate()}>Добавить</button>
      </div>
      {usersQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю сотрудников...</div>}
      {users.map((user) => {
        const username = String(user.username || "");
        const role = String(user.role || "manager");
        const active = user.active !== false && user.disabled !== true;
        const protectedUser = Boolean(user.protected);
        const allowedPages = Array.isArray(user.allowedPages) ? (user.allowedPages as unknown[]).map((page) => String(page)) : null;
        const accessLabel = role === "admin" ? "все страницы" : (allowedPages?.length ? `страниц: ${allowedPages.length}` : "по умолчанию (склад + сборка)");
        return (
          <article className="job-row" key={username}>
            <div>
              <strong>{username}</strong>
              <span>{role} · {active ? "активен" : "выключен"} · {String(user.source || "local")} · доступ: {accessLabel}</span>
            </div>
            <div className="row-actions">
              <button className="secondary-action" type="button" disabled={updateUser.isPending} onClick={() => setAccessUser({ username, role, allowedPages })}><Eye size={15} /> Доступ к страницам</button>
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
      {accessUser ? (
        <PageAccessModal
          username={accessUser.username}
          role={accessUser.role}
          allowedPages={accessUser.allowedPages}
          saving={saveAccess.isPending}
          error={saveAccess.error ? errorMessage(saveAccess.error) : undefined}
          onClose={() => setAccessUser(null)}
          onSave={(pages) => saveAccess.mutate({ username: accessUser.username, pages })}
        />
      ) : null}
    </section>
    <section className="settings-panel settings-panel-wide">
      <div className="section-title">
        <div><span>PriceMaster</span><h3>Статистика привязок</h3></div>
        <div className="row-actions">
          <SelectField
            ariaLabel="Период"
            value={statsPeriod}
            onChange={setStatsPeriod}
            options={[
              { value: "7d", label: "7 дней" },
              { value: "30d", label: "30 дней" },
              { value: "90d", label: "90 дней" },
              { value: "all", label: "Все" },
            ]}
          />
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
    </div>
  );
}

function AuditSettingsPanel() {
  const [q, setQ] = useState("");
  const auditQuery = useQuery({
    queryKey: ["audit", q],
    queryFn: () => fetchJson(`/api/audit-log?limit=500&q=${encodeURIComponent(q)}`, AuditLogSchema),
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
          <button className="secondary-action" type="button" disabled={!retryItems.length || clearRetry.isPending} onClick={() => { if (!window.confirm("Очистить очередь повторных отправок? Это действие необратимо.")) return; clearRetry.mutate(); }}>Очистить очередь</button>
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

function SupplierStockModePanel() {
  const queryClient = useQueryClient();
  const [search, setSearch] = useState("");
  const suppliersQuery = useQuery({
    queryKey: ["suppliers", "stock-mode"],
    queryFn: () => fetchJson("/api/suppliers", SuppliersResponseSchema),
    staleTime: 60_000,
  });
  const updateSupplier = useMutation({
    mutationFn: ({ id, patch }: { id: string; patch: Record<string, unknown> }) =>
      fetchJson(`/api/suppliers/${encodeURIComponent(id)}`, SuppliersResponseSchema, patchBody(patch)),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });
  const suppliers = suppliersQuery.data?.suppliers || [];
  const filtered = suppliers
    .filter((supplier) => {
      const haystack = [supplier.name, supplier.partnerId, supplier.pricingMode].filter(Boolean).join(" ").toLowerCase();
      return !search.trim() || haystack.includes(search.trim().toLowerCase());
    })
    .slice(0, 80);

  return (
    <div className="settings-panel settings-panel-wide">
      <div className="section-title">
        <div>
          <span>PriceMaster</span>
          <h3>Складской источник без цены</h3>
        </div>
        <button className="secondary-action" type="button" onClick={() => suppliersQuery.refetch()} disabled={suppliersQuery.isFetching}>
          {suppliersQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
        </button>
      </div>
      <div className="settings-hint">
        Включайте для “нашего склада”: наличие участвует в продаже и восстановлении, но цена PriceMaster не берется в авторасчет. Для автокорзины задавайте доверие, дедлайн приема заказов и признак перекупщика: так система выберет лучшего поставщика именно сейчас.
      </div>
      <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Найти поставщика PriceMaster" />
      <div className="supplier-mode-list">
        {filtered.map((supplier) => {
          const id = supplier.id || supplier.partnerId || supplier.name || "";
          const stockOnly = supplier.pricingMode === "stock_only" || supplier.stockOnly === true;
          return (
            <article className={`supplier-mode-row ${stockOnly ? "is-stock-only" : ""}`} key={id}>
              <div>
                <strong>{supplier.name || supplier.partnerId || "Поставщик"}</strong>
                <span>{supplier.partnerId ? `partner ${supplier.partnerId}` : "без partnerId"} · товаров {supplier.impactProductCount || 0}</span>
              </div>
              <label className="toggle-line">
                <input
                  type="checkbox"
                  checked={stockOnly}
                  disabled={!id || updateSupplier.isPending}
                  onChange={(event) => updateSupplier.mutate({ id, patch: { pricingMode: event.target.checked ? "stock_only" : "normal" } })}
                />
                <span>складской / не брать цену</span>
              </label>
              <div className="supplier-quality-controls">
                <label>Доверие
                  <input
                    key={`${id}-trust-${supplier.trustFactor ?? 100}`}
                    type="number"
                    min={0}
                    max={100}
                    defaultValue={supplier.trustFactor ?? 100}
                    disabled={!id || updateSupplier.isPending}
                    onBlur={(event) => updateSupplier.mutate({ id, patch: { trustFactor: Number(event.target.value) || 0 } })}
                  />
                </label>
                <label>Заказы до
                  <input
                    key={`${id}-cutoff-${supplier.orderCutoffTime || ""}`}
                    defaultValue={supplier.orderCutoffTime || ""}
                    placeholder="13:00"
                    disabled={!id || updateSupplier.isPending}
                    onBlur={(event) => updateSupplier.mutate({ id, patch: { orderCutoffTime: event.target.value } })}
                  />
                </label>
                <label className="toggle-line">
                  <input
                    type="checkbox"
                    checked={supplier.reseller === true}
                    disabled={!id || updateSupplier.isPending}
                    onChange={(event) => updateSupplier.mutate({ id, patch: { reseller: event.target.checked } })}
                  />
                  <span>перекупщик</span>
                </label>
              </div>
            </article>
          );
        })}
        {suppliersQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю поставщиков...</div>}
        {!suppliersQuery.isLoading && !filtered.length && <div className="soft-empty">Поставщики не найдены.</div>}
      </div>
      {updateSupplier.error && <div className="inline-error">{errorMessage(updateSupplier.error)}</div>}
    </div>
  );
}

function ToolsSettingsPanel() {
  const tools = [
    { href: "/app/prices", title: "Цены", text: "Автоматизация цен и остатков, retry и причины пропусков." },
    { href: "/app/finance", title: "Финансы", text: "Заказы, ручные закупки, поставщики и чистая прибыль." },
    { href: "/app/operations", title: "Операции", text: "Массовые задачи, прогресс и аварийные запуски." },
    { href: "/app/recovery-queue", title: "Восстановление", text: "Очередь Ozon autoarchive и дневной лимит разархива." },
    { href: "/app/no-supplier", title: "Ошибки наличия", text: "Товары без доступного поставщика и риски остатков." },
    { href: "/app/problem-products", title: "Проблемные товары", text: "Единая диагностика SKU, фото, цен, остатков и связей." },
    { href: "/app/ai-drafts", title: "AI drafts", text: "Черновики карточек и генерация контента." },
    { href: "/app/system", title: "Система", text: "Health, очереди, кэши и технический статус." },
  ];
  return (
    <section className="settings-grid pricing-settings-grid">
      {tools.map((tool) => (
        <article className="settings-panel" key={tool.href}>
          <div className="section-title">
            <div><span>Инструмент</span><h3>{tool.title}</h3></div>
          </div>
          <p className="settings-hint">{tool.text}</p>
          <a className="secondary-action" href={tool.href}>Открыть</a>
        </article>
      ))}
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
  const isDirtyRef = useRef(false);
  const [activeTab, setActiveTab] = useState<SettingsTab>("prices");
  const [adjustMarketplace, setAdjustMarketplace] = useState("all");
  const [adjustDirection, setAdjustDirection] = useState("decrease");
  const [adjustPercent, setAdjustPercent] = useState(2);
  const [rulesTab, setRulesTab] = useState("ozon");

  const copyRulesFrom = (sourceMarketplace: string) => {
    const sourceRules = markupRules
      .filter((rule) => rule.marketplace === sourceMarketplace)
      .map((rule) => ({ ...rule, marketplace: rulesTab }));
    const otherRules = markupRules.filter((rule) => rule.marketplace !== rulesTab);
    setMarkupRules([...otherRules, ...sourceRules]);
  };

  useEffect(() => {
    if (settingsQuery.data?.settings && !isDirtyRef.current) {
      setDraft(settingsQuery.data.settings);
    }
  }, [settingsQuery.data]);

  const draftAi = asRecord(draft.ai);
  const draftMarkups = asRecord(draft.defaultMarkups);
  const markupRules = readMarkupRules(draft.markupRules);
  const availabilityRules = readAvailabilityRules(draft.availabilityRules);
  const save = useMutation({
    mutationFn: () => fetchJson("/api/settings", SettingsResponseSchema, mutationBody(settingsSavePayload(draft))),
    onSuccess: () => { isDirtyRef.current = false; queryClient.invalidateQueries({ queryKey: ["settings"] }); },
  });
  const adjustPricing = useMutation({
    mutationFn: () => fetchJson("/api/settings/pricing/adjust-percent", SettingsResponseSchema, mutationBody({
      marketplace: adjustMarketplace,
      direction: adjustDirection,
      percent: adjustPercent,
    })),
    onSuccess: (data) => {
      if (data.settings) setDraft(data.settings);
      queryClient.invalidateQueries({ queryKey: ["settings"] });
      queryClient.invalidateQueries({ queryKey: ["sales-automation-summary"] });
      queryClient.invalidateQueries({ queryKey: ["sales-automation-items"] });
    },
  });
  const testAi = useMutation({
    mutationFn: () => fetchJson("/api/settings/ai/test", SettingsResponseSchema, mutationBody({ ai: draftAi })),
  });
  const update = (patch: Record<string, unknown>) => { isDirtyRef.current = true; setDraft((current) => ({ ...current, ...patch })); };
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
  const adjustMultiplier = adjustDirection === "increase" ? 1 + numberValue(adjustPercent, 0) / 100 : 1 - numberValue(adjustPercent, 0) / 100;
  const previewMarketplaces = adjustMarketplace === "all" ? [...PRICING_MARKETPLACES] : [adjustMarketplace];
  const adjustedDefaults = previewMarketplaces.map((marketplace) => ({
    marketplace,
    current: recordNumber(draftMarkups, marketplace),
    next: adjustCoefficient(recordNumber(draftMarkups, marketplace), adjustMultiplier),
  })).filter((row) => row.current > 0 && row.next > 0);
  const adjustedRules = markupRules.flatMap((rule) => adjustedRulePreview(rule, adjustMarketplace, adjustMultiplier));
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
        <div className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div><span>Быстро</span><h3>Быстрая корректировка цен</h3></div>
            <button className="primary-action" type="button" disabled={adjustPricing.isPending || !(numberValue(adjustPercent, 0) > 0)} onClick={() => adjustPricing.mutate()}>
              {adjustPricing.isPending ? <Loader2 className="spin" size={16} /> : <Percent size={16} />} Применить ко всем
            </button>
          </div>
          <p className="settings-hint">Меняет базовые коэффициенты и гибкие правила, а не цены напрямую. Ручные наценки в карточках товаров не трогаются.</p>
          <div className="settings-rule-row">
            <SelectField
              ariaLabel="Маркетплейс"
              value={adjustMarketplace}
              onChange={setAdjustMarketplace}
              options={[
                { value: "all", label: "Ozon + Yandex + Avito + WB" },
                { value: "ozon", label: "Ozon" },
                { value: "yandex", label: "Yandex Market" },
                { value: "avito", label: "Avito" },
                { value: "wb", label: "Wildberries" },
              ]}
            />
            <SelectField
              ariaLabel="Направление"
              value={adjustDirection}
              onChange={setAdjustDirection}
              options={[
                { value: "decrease", label: "Снизить" },
                { value: "increase", label: "Поднять" },
              ]}
            />
            <input type="number" min="0.01" max="90" step="0.01" value={String(adjustPercent)} onChange={(event) => setAdjustPercent(numberValue(event.target.value, 0))} />
            <span className="settings-hint">%</span>
          </div>
          <div className="settings-rule-table">
            <div className="settings-rule-head"><span>Что изменится</span><span>Было</span><span>Станет</span><span></span></div>
            {adjustedDefaults.map((row) => (
              <div className="settings-rule-row" key={`default-${row.marketplace}`}>
                <span>Базовая наценка {marketplaceLabel(row.marketplace)}</span>
                <span>{row.current.toFixed(4)}</span>
                <strong>{row.next.toFixed(4)}</strong>
                <span></span>
              </div>
            ))}
            {adjustedRules.map((rule, index) => (
              <div className="settings-rule-row" key={`rule-preview-${index}-${rule.marketplace}-${rule.nextMarketplace}-${rule.minUsd}`}>
                <span>Правило {rule.marketplace} от ${rule.minUsd}{rule.splitOnly ? " · будет разделено" : ""}</span>
                <span>{rule.coefficient.toFixed(4)}</span>
                <strong>{rule.nextCoefficient.toFixed(4)}</strong>
                <span>{rule.nextMarketplace}</span>
              </div>
            ))}
            {!adjustedDefaults.length && !adjustedRules.length && <div className="soft-empty compact">Нет коэффициентов для предпросмотра.</div>}
          </div>
          {adjustPricing.error && <div className="inline-error">{errorMessage(adjustPricing.error)}</div>}
          {adjustPricing.isSuccess && <div className="success-strip">Настройки изменены, пересчет цен поставлен в очередь.</div>}
        </div>
        <div className="settings-panel">
          <div className="section-title"><div><span>Цены</span><h3>Базовые цены</h3></div></div>
          <label>Курс USD/RUB<input type="number" min="0.0001" step="0.0001" value={String(draft.fixedUsdRate ?? settings.fixedUsdRate ?? "")} onChange={(event) => update({ fixedUsdRate: numberValue(event.target.value) })} /></label>
          <label>Базовая наценка Ozon<input type="number" min="0.0001" step="0.0001" value={String(draftMarkups.ozon ?? markups.ozon ?? "")} onChange={(event) => updateMarkups({ ozon: numberValue(event.target.value) })} /></label>
          <label>Базовая наценка Yandex Market<input type="number" min="0.0001" step="0.0001" value={String(draftMarkups.yandex ?? markups.yandex ?? "")} onChange={(event) => updateMarkups({ yandex: numberValue(event.target.value) })} /></label>
          <label>Базовая наценка Avito<input type="number" min="0.0001" step="0.0001" value={String(draftMarkups.avito ?? markups.avito ?? "")} onChange={(event) => updateMarkups({ avito: numberValue(event.target.value) })} /></label>
          <label>Базовая наценка Wildberries<input type="number" min="0.0001" step="0.0001" value={String(draftMarkups.wb ?? markups.wb ?? "")} onChange={(event) => updateMarkups({ wb: numberValue(event.target.value) })} /></label>
          <div className="soft-empty compact">После изменения курса или наценки backend ставит пересчет цен в очередь.</div>
        </div>

        <div className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div><span>Наценки</span><h3>Гибкие правила наценки</h3></div>
            <div className="section-title-actions">
              {(() => {
                const copyOptions = [
                  { value: "ozon", label: "Ozon" },
                  { value: "yandex", label: "Yandex Market" },
                  { value: "avito", label: "Avito" },
                  { value: "wb", label: "Wildberries" },
                  { value: "all", label: "Общие" },
                ].filter((mp) => mp.value !== rulesTab && markupRules.some((r) => r.marketplace === mp.value));
                if (!copyOptions.length) return null;
                return (
                  <select
                    className="copy-from-select"
                    value=""
                    title="Скопировать правила с другого маркетплейса (заменит текущие правила вкладки)"
                    onChange={(e) => { if (e.target.value) copyRulesFrom(e.target.value); }}
                  >
                    <option value="">Скопировать с…</option>
                    {copyOptions.map((opt) => {
                      const n = markupRules.filter((r) => r.marketplace === opt.value).length;
                      return <option key={opt.value} value={opt.value}>{opt.label} ({n})</option>;
                    })}
                  </select>
                );
              })()}
              <button className="secondary-action" type="button" onClick={() => setMarkupRules([...markupRules, { marketplace: rulesTab, minUsd: 0, coefficient: 1 }])}>
                Добавить правило {rulesTab === "all" ? "для всех" : marketplaceLabel(rulesTab)}
              </button>
            </div>
          </div>
          <p className="settings-hint">Правила применяются по цене поставщика в USD и разложены по маркетплейсам — каждая вкладка отсортирована по цене «от». Пустой список допустим: тогда используются базовые наценки.</p>
          <nav className="rules-marketplace-tabs" aria-label="Маркетплейс правил">
            {[
              { value: "ozon", label: "Ozon" },
              { value: "yandex", label: "Yandex Market" },
              { value: "avito", label: "Avito" },
              { value: "wb", label: "Wildberries" },
              { value: "all", label: "Общие (все МП)" },
            ].map((tab) => {
              const count = markupRules.filter((rule) => rule.marketplace === tab.value).length;
              return (
                <button key={tab.value} type="button" className={rulesTab === tab.value ? "is-active" : ""} onClick={() => setRulesTab(tab.value)}>
                  {tab.label}
                  <span className="rules-tab-count">{count}</span>
                </button>
              );
            })}
          </nav>
          <div className="settings-rule-table markup-rule-table">
            <div className="settings-rule-head"><span>От цены, USD</span><span>Коэффициент</span><span></span></div>
            {markupRules
              .map((rule, index) => ({ rule, index }))
              .filter(({ rule }) => rule.marketplace === rulesTab)
              .sort((a, b) => a.rule.minUsd - b.rule.minUsd || a.index - b.index)
              .map(({ rule, index }) => (
                <div className="settings-rule-row" key={`markup-${index}`}>
                  <input type="number" min="0" step="0.0001" value={String(rule.minUsd)} onChange={(event) => updateMarkupRule(index, { minUsd: numberValue(event.target.value) })} />
                  <input type="number" min="0.0001" step="0.0001" value={String(rule.coefficient)} onChange={(event) => updateMarkupRule(index, { coefficient: numberValue(event.target.value, 1) })} />
                  <button className="icon-action danger" type="button" title="Удалить правило" onClick={() => setMarkupRules(markupRules.filter((_, ruleIndex) => ruleIndex !== index))}><Trash2 size={15} /></button>
                </div>
              ))}
            {!markupRules.some((rule) => rule.marketplace === rulesTab) && (
              <div className="soft-empty compact">
                {rulesTab === "all"
                  ? "Общих правил нет — действуют правила конкретных маркетплейсов и базовые наценки."
                  : `Правил для ${marketplaceLabel(rulesTab)} нет. Будет использоваться базовая наценка.`}
              </div>
            )}
          </div>
          <p className="settings-hint">Avito использует эти же правила: цена объявления = закупка у привязанного поставщика × коэффициент Avito. Дополнительная тонкая подстройка — на странице «Импорт на Avito».</p>
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
                <SelectField
                  ariaLabel="Маркетплейс правила"
                  value={rule.marketplace}
                  onChange={(next) => updateAvailabilityRule(index, { marketplace: next })}
                  options={[
                    { value: "all", label: "Все" },
                    { value: "ozon", label: "Ozon" },
                    { value: "yandex", label: "Yandex Market" },
                  ]}
                />
                <input type="number" min="0" step="1" value={String(rule.minAvailableSuppliers)} onChange={(event) => updateAvailabilityRule(index, { minAvailableSuppliers: Math.round(numberValue(event.target.value, 1)) })} />
                <input type="number" step="0.0001" value={String(rule.coefficientDelta)} onChange={(event) => updateAvailabilityRule(index, { coefficientDelta: numberValue(event.target.value) })} />
                <input type="number" min="0" step="1" value={String(rule.targetStock)} onChange={(event) => updateAvailabilityRule(index, { targetStock: Math.round(numberValue(event.target.value, 3)) })} />
                <button className="icon-action danger" type="button" title="Удалить правило" onClick={() => setAvailabilityRules(availabilityRules.filter((_, ruleIndex) => ruleIndex !== index))}><Trash2 size={15} /></button>
              </div>
            ))}
            {!availabilityRules.length && <div className="soft-empty compact">При сохранении будет добавлено дефолтное правило: все площадки, от 1 поставщика, поправка 0, остаток 3.</div>}
          </div>
        </div>

        <div className="settings-panel">
          <div className="section-title">
            <div><span>Реализация</span><h3>Ежедневный отчёт спонсору</h3></div>
          </div>
          <p className="soft-empty compact">
            Каждый день в указанный час спонсор получит в Telegram сводку: продажи за день, доля прибыли, текущий баланс.
          </p>
          <div className="settings-form-row">
            <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
              <input
                type="checkbox"
                checked={Boolean(draft.sponsorDailyReportEnabled)}
                onChange={(event) => update({ sponsorDailyReportEnabled: event.target.checked })}
              />
              Включить ежедневный отчёт
            </label>
          </div>
          <div className="settings-form-row">
            <label>
              Telegram Chat ID спонсора
              <input
                type="text"
                placeholder="-100123456789"
                value={String(draft.sponsorTelegramChatId ?? "")}
                onChange={(event) => update({ sponsorTelegramChatId: event.target.value })}
                style={{ maxWidth: 220 }}
              />
            </label>
            <label>
              Час отправки (0–23)
              <input
                type="number"
                min={0}
                max={23}
                value={String(draft.sponsorDailyReportHour ?? 20)}
                onChange={(event) => update({ sponsorDailyReportHour: Number(event.target.value) })}
                style={{ maxWidth: 90 }}
              />
            </label>
          </div>
          <div className="settings-form-row">
            <button
              className="secondary-action"
              type="button"
              onClick={() =>
                fetch("/api/consignment/sponsor-report/send", { method: "POST" })
                  .then((r) => r.json())
                  .then((d) =>
                    alert(
                      d.result?.skipped
                        ? `Пропущено: ${d.result.reason}`
                        : `Отчёт отправлен. Прибыль за сегодня: ${d.result?.todayProfit ?? 0} ₽`,
                    ),
                  )
                  .catch((e) => alert("Ошибка: " + e.message))
              }
            >
              Тестовый отчёт в Telegram
            </button>
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
        <MarketplaceAccountsPanel />
        <SupplierStockModePanel />

        <div className="settings-panel settings-panel-wide">
          <div className="section-title"><div><span>Брендинг</span><h3>Логотипы магазинов для премиум-фото</h3></div></div>
          <div className="settings-hint">PNG 258x258 с прозрачным фоном. Логотип ставится маленьким бейджем на премиальные фото и не перекрывает флакон.</div>
          <div className="branding-grid">
            <BrandingLogoPanel marketplace="ozon" label="Ozon" settings={settings} draft={draft} update={update} />
            <BrandingLogoPanel marketplace="yandex" label="Yandex" settings={settings} draft={draft} update={update} />
          </div>
        </div>

        <div className="settings-panel">
          <div className="section-title"><div><span>Yandex</span><h3>Склад остатков</h3></div></div>
          <label>Warehouse ID<input value={String(asRecord(draft.yandex).warehouseId ?? asRecord(settings.yandex).warehouseId ?? "128820967")} onChange={(event) => update({ yandex: { ...asRecord(draft.yandex), warehouseId: event.target.value } })} /></label>
          <div className="soft-empty compact">Остатки должны уходить только в Magic Stick: 128820967.</div>
        </div>

      </section>}

      {activeTab === "tools" && <ToolsSettingsPanel />}
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
