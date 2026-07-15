import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, Loader2, Pencil, PlugZap, Plus, RotateCcw, Trash2, X } from "lucide-react";
import { SelectField } from "./SelectField";

type MarketplaceAccount = {
  id: string;
  marketplace: string;
  name: string;
  clientId?: string;
  apiKey?: string;
  businessId?: string;
  campaignId?: string;
  configured?: boolean;
  source?: string;
  readOnly?: boolean;
  inheritedFromEnv?: boolean;
  syncEnabled?: boolean;
  updatedAt?: string | null;
};

type AccountsResponse = {
  accounts: MarketplaceAccount[];
  hiddenAccounts: MarketplaceAccount[];
};

type AccountDraft = {
  id: string;
  marketplace: string;
  name: string;
  clientId: string;
  apiKey: string;
  businessId: string;
  campaignId: string;
  syncEnabled: boolean;
};

const EMPTY_DRAFT: AccountDraft = { id: "", marketplace: "ozon", name: "", clientId: "", apiKey: "", businessId: "", campaignId: "", syncEnabled: true };

const MARKETPLACE_LABELS: Record<string, string> = { ozon: "Ozon", yandex: "Yandex Market", avito: "Avito", wb: "Wildberries" };

function marketplaceLabel(marketplace: string): string {
  return MARKETPLACE_LABELS[marketplace] || marketplace;
}

async function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...(init || {}),
    headers: { ...(init?.body ? { "Content-Type": "application/json" } : {}), ...(init?.headers || {}) },
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error((data as { error?: string })?.error || `HTTP ${response.status}`);
  return data as T;
}

export function MarketplaceAccountsPanel() {
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState<AccountDraft>(EMPTY_DRAFT);
  const [formOpen, setFormOpen] = useState(false);
  const [testResults, setTestResults] = useState<Record<string, { ok: boolean; message: string }>>({});

  const accountsQuery = useQuery({
    queryKey: ["marketplace-accounts"],
    queryFn: () => apiJson<AccountsResponse>("/api/marketplace-accounts"),
  });
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ["marketplace-accounts"] });

  const save = useMutation({
    mutationFn: (payload: AccountDraft) => apiJson(
      payload.id ? `/api/marketplace-accounts/${encodeURIComponent(payload.id)}` : "/api/marketplace-accounts",
      { method: payload.id ? "PATCH" : "POST", body: JSON.stringify({ ...payload, syncEnabled: payload.syncEnabled ? "true" : "false" }) },
    ),
    onSuccess: () => {
      setDraft(EMPTY_DRAFT);
      setFormOpen(false);
      invalidate();
    },
  });
  const remove = useMutation({
    mutationFn: (id: string) => apiJson(`/api/marketplace-accounts/${encodeURIComponent(id)}`, { method: "DELETE" }),
    onSuccess: invalidate,
  });
  const restore = useMutation({
    mutationFn: (id: string) => apiJson(`/api/marketplace-accounts/${encodeURIComponent(id)}/restore`, { method: "POST", body: JSON.stringify({}) }),
    onSuccess: invalidate,
  });
  const toggleSync = useMutation({
    mutationFn: (account: MarketplaceAccount) => apiJson(`/api/marketplace-accounts/${encodeURIComponent(account.id)}`, {
      method: "PATCH",
      body: JSON.stringify({ syncEnabled: account.syncEnabled === false ? "true" : "false" }),
    }),
    onSuccess: invalidate,
  });
  const test = useMutation({
    mutationFn: (id: string) => apiJson<{ ok: boolean; message?: string; error?: string }>(`/api/marketplace-accounts/${encodeURIComponent(id)}/test`, { method: "POST", body: JSON.stringify({}) }),
    onSuccess: (data, id) => setTestResults((current) => ({ ...current, [id]: { ok: data.ok !== false, message: data.message || "Подключение работает." } })),
    onError: (error, id) => setTestResults((current) => ({ ...current, [id]: { ok: false, message: (error as Error).message } })),
  });

  const startEdit = (account: MarketplaceAccount) => {
    setDraft({
      id: account.id,
      marketplace: account.marketplace,
      name: account.name || "",
      clientId: "",
      apiKey: "",
      businessId: account.businessId || "",
      campaignId: account.campaignId || "",
      syncEnabled: account.syncEnabled !== false,
    });
    setFormOpen(true);
  };
  const closeForm = () => {
    setDraft(EMPTY_DRAFT);
    setFormOpen(false);
  };

  const accounts = accountsQuery.data?.accounts || [];
  const hidden = accountsQuery.data?.hiddenAccounts || [];
  const isEditing = Boolean(draft.id);
  const isYandex = draft.marketplace === "yandex";
  const isWb = draft.marketplace === "wb";
  const clientIdLabel = draft.marketplace === "avito" ? "Client ID" : "Client-Id";
  const apiKeyLabel = draft.marketplace === "avito" ? "Client Secret" : isWb ? "API-токен продавца" : "API Key";
  const canSave = Boolean(draft.name.trim()) && (isEditing || (isYandex
    ? Boolean(draft.businessId && draft.apiKey)
    : isWb
      ? Boolean(draft.apiKey)
      : Boolean(draft.clientId && draft.apiKey)));

  return (
    <div className="settings-panel settings-panel-wide accounts-panel">
      <div className="section-title">
        <div><span>Кабинеты</span><h3>Кабинеты маркетплейсов</h3></div>
        <button className="secondary-action" type="button" onClick={() => (formOpen ? closeForm() : setFormOpen(true))}>
          {formOpen ? <X size={15} /> : <Plus size={15} />} {formOpen ? "Закрыть форму" : "Добавить кабинет"}
        </button>
      </div>
      <p className="settings-hint">Ключи Ozon, Yandex Market, Avito и Wildberries хранятся в кабинете сайта — без правки .env и перезапуска. После сохранения проверьте подключение кнопкой на карточке.</p>

      {formOpen ? (
        <div className="account-edit-form">
          <div className="settings-form-row account-form-row">
            <label>Маркетплейс
              <SelectField
                ariaLabel="Маркетплейс кабинета"
                value={draft.marketplace}
                onChange={(next) => setDraft((current) => ({ ...current, marketplace: next }))}
                options={[
                  { value: "ozon", label: "Ozon" },
                  { value: "yandex", label: "Yandex Market" },
                  { value: "avito", label: "Avito" },
                  { value: "wb", label: "Wildberries" },
                ]}
              />
            </label>
            <label>Название
              <input value={draft.name} placeholder={`Например: ${marketplaceLabel(draft.marketplace)} основной`} onChange={(event) => setDraft((current) => ({ ...current, name: event.target.value }))} />
            </label>
          </div>
          <div className="settings-form-row account-form-row">
            {!isYandex && !isWb ? (
              <label>{clientIdLabel}
                <input value={draft.clientId} autoComplete="off" placeholder={isEditing ? "оставьте пустым, чтобы не менять" : ""} onChange={(event) => setDraft((current) => ({ ...current, clientId: event.target.value }))} />
              </label>
            ) : null}
            {isYandex ? (
              <label>Business ID
                <input value={draft.businessId} inputMode="numeric" onChange={(event) => setDraft((current) => ({ ...current, businessId: event.target.value }))} />
              </label>
            ) : null}
            <label>{apiKeyLabel}
              <input type="password" value={draft.apiKey} autoComplete="new-password" placeholder={isEditing ? "оставьте пустым, чтобы не менять" : ""} onChange={(event) => setDraft((current) => ({ ...current, apiKey: event.target.value }))} />
            </label>
            {isYandex || isWb ? (
              <label>{isWb ? "ID склада FBS (для остатков)" : "Campaign ID"}
                <input value={draft.campaignId} inputMode="numeric" onChange={(event) => setDraft((current) => ({ ...current, campaignId: event.target.value }))} />
              </label>
            ) : null}
          </div>
          <div className="account-form-actions">
            <label className="settings-toggle">
              <input type="checkbox" checked={draft.syncEnabled} onChange={(event) => setDraft((current) => ({ ...current, syncEnabled: event.target.checked }))} />
              Загружать товары и цены этого кабинета
            </label>
            <button className="primary-action" type="button" disabled={!canSave || save.isPending} onClick={() => save.mutate(draft)}>
              {save.isPending ? <Loader2 className="spin" size={15} /> : <CheckCircle2 size={15} />} {isEditing ? "Сохранить изменения" : "Сохранить кабинет"}
            </button>
          </div>
          {isEditing ? <p className="settings-hint">Пустые секретные поля сохранят прежние ключи.</p> : null}
          {save.error ? <div className="inline-error">{String((save.error as Error).message)}</div> : null}
        </div>
      ) : null}

      <div className="accounts-cards">
        {accounts.map((account) => {
          const testResult = testResults[account.id];
          return (
            <div className={`account-modern-card${account.configured ? "" : " not-configured"}${account.syncEnabled === false ? " sync-disabled" : ""}`} key={account.id}>
              <div className="account-modern-head">
                <span className={`market-badge market-${account.marketplace}`}>{marketplaceLabel(account.marketplace)}</span>
                <strong>{account.name}</strong>
                <span className={`pill ${account.configured ? "ok" : "warn"}`}>{account.configured ? "ключи подключены" : "не настроен"}</span>
                {account.syncEnabled === false ? <span className="pill muted">загрузка выкл</span> : null}
              </div>
              <div className="account-modern-meta">
                {account.marketplace === "yandex"
                  ? <span>Business ID: <b>{account.businessId || "—"}</b>{account.campaignId ? <> · Campaign: <b>{account.campaignId}</b></> : null}</span>
                  : account.marketplace === "wb"
                    ? <span>Склад FBS: <b>{account.campaignId || "—"}</b></span>
                    : <span>{account.marketplace === "avito" ? "Client ID" : "Client-Id"}: <b>{account.clientId || "—"}</b></span>}
                <span>{account.marketplace === "avito" ? "Secret" : account.marketplace === "wb" ? "Токен" : "API Key"}: <b>{account.apiKey || "—"}</b></span>
                <span className="account-source">{account.readOnly ? "из .env" : account.inheritedFromEnv ? "переопределён" : "локальный"}</span>
              </div>
              {testResult ? (
                <div className={testResult.ok ? "success-strip compact" : "inline-error"}>{testResult.message}</div>
              ) : null}
              <div className="account-modern-actions">
                <button className="secondary-action" type="button" disabled={test.isPending} onClick={() => test.mutate(account.id)}>
                  {test.isPending && test.variables === account.id ? <Loader2 className="spin" size={14} /> : <PlugZap size={14} />} Проверить
                </button>
                <button className="secondary-action" type="button" disabled={toggleSync.isPending} onClick={() => toggleSync.mutate(account)}>
                  {account.syncEnabled === false ? "Включить загрузку" : "Отключить загрузку"}
                </button>
                <button className="secondary-action" type="button" onClick={() => startEdit(account)}><Pencil size={14} /> Изменить</button>
                <button className="icon-action danger" type="button" title={account.readOnly ? "Скрыть кабинет из .env" : "Удалить кабинет"} disabled={remove.isPending} onClick={() => remove.mutate(account.id)}>
                  <Trash2 size={15} />
                </button>
              </div>
            </div>
          );
        })}
        {accountsQuery.isLoading ? <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю кабинеты…</div> : null}
        {!accounts.length && !accountsQuery.isLoading ? <div className="empty-state">Кабинетов нет. Добавьте первый кабинет Ozon, Yandex Market или Avito.</div> : null}
      </div>

      {hidden.length ? (
        <>
          <div className="section-title compact-title"><div><span>Скрытые</span><h3>Скрытые кабинеты из .env</h3></div></div>
          {hidden.map((account) => (
            <div className="account-hidden-row" key={account.id}>
              <span className={`market-badge market-${account.marketplace}`}>{marketplaceLabel(account.marketplace)}</span>
              <strong>{account.name}</strong>
              <small>{account.id}</small>
              <button className="secondary-action" type="button" disabled={restore.isPending} onClick={() => restore.mutate(account.id)}>
                <RotateCcw size={14} /> Вернуть
              </button>
            </div>
          ))}
        </>
      ) : null}
      {(remove.error || restore.error || toggleSync.error) ? (
        <div className="inline-error">{String(((remove.error || restore.error || toggleSync.error) as Error).message)}</div>
      ) : null}
    </div>
  );
}
