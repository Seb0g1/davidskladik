import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, BadgeDollarSign, CheckCircle2, Loader2, RefreshCcw, Send, Zap } from "lucide-react";
import { useMemo, useState } from "react";
import { fetchJson, mutationBody } from "../api";
import { MutationProductResponseSchema, SalesAutomationItemsSchema, SalesAutomationSummarySchema } from "../types";

const text = (value: unknown) => String(value ?? "").trim();
const numberValue = (value: unknown) => Number(value || 0) || 0;
const asRecord = (value: unknown): Record<string, unknown> => (value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {});
const itemValue = (item: Record<string, unknown>, key: string) => item[key] ?? asRecord(item.raw)[key];

const money = (value: unknown) => {
  const number = Number(value || 0);
  return number > 0 ? `${Math.round(number).toLocaleString("ru-RU")} ₽` : "-";
};

const formatDate = (value: unknown) => {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
};

const reasonLabel = (reason: unknown) => {
  const value = text(reason);
  const labels: Record<string, string> = {
    ok: "готово",
    unchanged: "цена уже совпадает",
    no_supplier: "нет поставщика",
    no_price: "нет расчетной цены",
    api_error: "ошибка API",
    in_retry: "в retry",
    ozon_limit: "лимит Ozon",
    stock_only_manual_price_missing: "нужна ручная цена склада",
    no_pricemaster_link: "нет PM-привязки",
    not_ready: "поставщик не готов",
    unchanged_verified: "цена уже проверена",
    queued: "в очереди",
    api_accepted: "API принял",
    verification_pending: "ждем проверку",
    verified: "проверено",
    ozon_price_not_applied: "Ozon не применил цену",
    ozon_price_delayed: "Ozon отложил цену",
    pm_live_timeout: "PM timeout",
  };
  return labels[value] || value || "-";
};

export function PricesPage() {
  const [marketplace, setMarketplace] = useState("all");
  const [reason, setReason] = useState("all");
  const queryClient = useQueryClient();

  const summary = useQuery({
    queryKey: ["sales-automation", "summary"],
    queryFn: () => fetchJson("/api/sales-automation/summary", SalesAutomationSummarySchema),
    refetchInterval: 30_000,
  });
  const itemsQuery = useQuery({
    queryKey: ["sales-automation", "items", marketplace, reason],
    queryFn: () => {
      const params = new URLSearchParams({ marketplace, limit: "500" });
      if (reason !== "all") params.set("reason", reason);
      return fetchJson(`/api/sales-automation/items?${params.toString()}`, SalesAutomationItemsSchema);
    },
    refetchInterval: 45_000,
  });
  const run = useMutation({
    mutationFn: (payload: { marketplace: string; force?: boolean; onlyChanged?: boolean }) => fetchJson(
      "/api/sales-automation/run",
      MutationProductResponseSchema,
      mutationBody({
        marketplace: payload.marketplace,
        force: Boolean(payload.force),
        onlyChanged: payload.onlyChanged !== false,
        limit: 5000,
      }),
    ),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["sales-automation"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const reasons = summary.data?.reasons || {};
  const reasonOptions = useMemo(() => Object.entries(reasons).sort((a, b) => b[1] - a[1]), [reasons]);
  const items = itemsQuery.data?.items || [];
  const okReasons = ["ok", "unchanged", "unchanged_verified", "verified"];
  const yandexIssues = items.filter((item) => text(item.marketplace) === "yandex" && !okReasons.includes(text(item.reason))).length;
  const ozonIssues = items.filter((item) => text(item.marketplace) === "ozon" && !okReasons.includes(text(item.reason))).length;

  return (
    <section className="page-section price-control-page">
      <div className="section-title">
        <div>
          <span>Sales automation</span>
          <h2>Автоматизация цен и остатков</h2>
        </div>
        <button className="secondary-action" type="button" onClick={() => { void summary.refetch(); void itemsQuery.refetch(); }} disabled={summary.isFetching || itemsQuery.isFetching}>
          {summary.isFetching || itemsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Обновить контроль
        </button>
      </div>

      <div className="price-automation-hero">
        <div>
          <span className="eyebrow">Автоматический режим включен</span>
          <h3>Цены и остатки отправляются фоном после изменений PriceMaster, курса, наценок и привязок.</h3>
          <p>
            Эта страница не нужна для ежедневного ручного клика. Она показывает, что ушло в Ozon/Yandex, что стоит в retry, где лимит Ozon, и какие SKU требуют внимания.
          </p>
        </div>
        <div className="price-automation-badge">
          <CheckCircle2 size={22} />
          <strong>{summary.data?.autoEnabled ? "auto on" : "auto off"}</strong>
          <span>последний расчет: {formatDate(summary.data?.updatedAt)}</span>
        </div>
      </div>

      <div className="summary-grid price-summary-grid">
        <div><span>SKU под контролем</span><strong>{summary.data?.total ?? 0}</strong></div>
        <div><span>Retry цен</span><strong>{summary.data?.retryTotal ?? 0}</strong></div>
        <div><span>Ozon autoarchive</span><strong>{summary.data?.ozonUnarchiveQueued ?? 0}</strong></div>
        <div><span>Проблем Ozon / Yandex</span><strong>{ozonIssues} / {yandexIssues}</strong></div>
      </div>

      <div className="control-grid price-controls">
        <label>
          Маркетплейс
          <select value={marketplace} onChange={(event) => setMarketplace(event.target.value)}>
            <option value="all">Ozon + Yandex</option>
            <option value="ozon">Только Ozon</option>
            <option value="yandex">Только Yandex</option>
          </select>
        </label>
        <label>
          Причина
          <select value={reason} onChange={(event) => setReason(event.target.value)}>
            <option value="all">Все причины</option>
            {reasonOptions.map(([key, count]) => (
              <option value={key} key={key}>{reasonLabel(key)} · {count}</option>
            ))}
          </select>
        </label>
        <button className="primary-action danger-action" type="button" onClick={() => run.mutate({ marketplace, onlyChanged: true })} disabled={run.isPending}>
          {run.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Аварийно отправить измененные
        </button>
        <button className="secondary-action" type="button" onClick={() => run.mutate({ marketplace: "yandex", force: true, onlyChanged: false })} disabled={run.isPending}>
          <BadgeDollarSign size={16} /> Force Yandex
        </button>
      </div>

      {run.error ? <div className="inline-error">{String((run.error as Error).message || run.error)}</div> : null}
      {itemsQuery.error ? <div className="inline-error">{String((itemsQuery.error as Error).message || itemsQuery.error)}</div> : null}
      {run.data ? (
        <div className="success-strip">
          {run.data.accepted
            ? `Пересчет поставлен в очередь: ${numberValue(run.data.queued)} SKU · ${numberValue(run.data.queuedBatches)} batch · intent ${text(run.data.priceIntentId) || "new"}.`
            : `Отправлено: ${numberValue(run.data.sent)} · Ozon ${numberValue(run.data.ozonSent)} · Yandex ${numberValue(run.data.yandexSent)} · ошибок ${numberValue(run.data.failed)}`}
        </div>
      ) : null}

      <div className="price-reason-grid">
        {reasonOptions.length ? reasonOptions.slice(0, 8).map(([key, count]) => (
          <button className={reason === key ? "is-active" : ""} type="button" key={key} onClick={() => setReason(key)}>
            <AlertTriangle size={14} />
            <span>{reasonLabel(key)}</span>
            <strong>{count}</strong>
          </button>
        )) : <span className="muted-text">Причин пропуска пока нет.</span>}
      </div>

      <div className="table-panel price-table price-status-table">
        <div className="table-head">
          <span>Маркет</span><span>Артикул</span><span>Текущая</span><span>Расчет</span><span>Запрос</span><span>Verified</span><span>Apply</span><span>Причина</span><span>Обновлено</span>
        </div>
        {items.map((item) => (
          <div className="table-row" key={`${text(item.marketplace)}-${text(item.productId || item.offerId)}-${text(item.target)}`}>
            <span data-label="Маркет"><Zap size={14} /> {text(item.marketplace)}</span>
            <span data-label="Артикул">{text(item.offerId)}</span>
            <span data-label="Текущая">{money(item.currentPrice ?? item.oldPrice)}</span>
            <span data-label="Расчет"><strong>{money(item.targetPrice ?? item.price)}</strong></span>
            <span data-label="Запрос">{money(itemValue(item, "lastRequestedPrice"))}</span>
            <span data-label="Verified">{money(itemValue(item, "lastVerifiedPrice"))}</span>
            <span data-label="Apply">{reasonLabel(itemValue(item, "priceApplyStatus"))}</span>
            <span data-label="Причина">{reasonLabel(item.reason)}</span>
            <span data-label="Обновлено">{formatDate(item.updatedAt || item.lastCalculatedAt)}</span>
          </div>
        ))}
        {!items.length && !itemsQuery.isLoading ? <div className="empty-state">Сейчас нет строк по выбранному фильтру. Автоматизация продолжает работать в фоне.</div> : null}
      </div>
    </section>
  );
}
