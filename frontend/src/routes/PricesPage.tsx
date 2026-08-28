import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, BadgeDollarSign, CheckCircle2, Loader2, RefreshCcw, Send, Zap } from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { ListSkeleton } from "../components/Skeleton";
import { Stat } from "../components/Stat";
import { MutationProductResponseSchema, SalesAutomationItemsSchema, SalesAutomationSummarySchema } from "../types";

const text = (value: unknown) => String(value ?? "").trim();
const numberValue = (value: unknown) => Number(value || 0) || 0;
const asRecord = (value: unknown): Record<string, unknown> => (value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {});
const itemValue = (item: Record<string, unknown>, key: string) => item[key] ?? asRecord(item.raw)[key];
const supplierName = (item: Record<string, unknown>) => {
  const supplier = asRecord(itemValue(item, "selectedSupplier"));
  return text(supplier.partnerName || supplier.supplierName || supplier.name) || "-";
};

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
  const [applyStatus, setApplyStatus] = useState("all");
  const [runResult, setRunResult] = useState<Record<string, unknown> | null>(null);
  const runResultTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const queryClient = useQueryClient();

  const summary = useQuery({
    queryKey: ["sales-automation", "summary"],
    queryFn: () => fetchJson("/api/sales-automation/summary", SalesAutomationSummarySchema),
    refetchInterval: 45_000,
  });
  const itemsQuery = useQuery({
    queryKey: ["sales-automation", "items", marketplace, reason, applyStatus],
    queryFn: () => {
      const params = new URLSearchParams({ marketplace, limit: "500" });
      if (reason !== "all") params.set("reason", reason);
      if (applyStatus !== "all") params.set("status", applyStatus);
      return fetchJson(`/api/sales-automation/items?${params.toString()}`, SalesAutomationItemsSchema);
    },
    refetchInterval: 45_000,
  });
  const run = useMutation({
    mutationFn: (payload: { marketplace: string; force?: boolean; onlyChanged?: boolean; reason?: string }) => fetchJson(
      "/api/sales-automation/run",
      MutationProductResponseSchema,
      mutationBody({
        marketplace: payload.marketplace,
        force: Boolean(payload.force),
        onlyChanged: payload.onlyChanged !== false,
        reason: payload.reason || "sales_automation_manual",
        limit: 5000,
      }),
    ),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: ["sales-automation"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      setRunResult(data as Record<string, unknown>);
      if (runResultTimer.current) clearTimeout(runResultTimer.current);
      runResultTimer.current = setTimeout(() => setRunResult(null), 6000);
    },
  });

  const reasons = summary.data?.reasons || {};
  const reasonOptions = useMemo(() => Object.entries(reasons).sort((a, b) => b[1] - a[1]), [reasons]);
  const quickFilters = [
    { label: "в retry", reason: "in_retry", status: "all" },
    { label: "Ozon не применил", reason: "ozon_price_not_applied", status: "all" },
    { label: "PM timeout", reason: "pm_live_timeout", status: "all" },
    { label: "нет поставщика", reason: "no_supplier", status: "all" },
    { label: "нет PM-привязки", reason: "no_pricemaster_link", status: "all" },
    { label: "verified", reason: "all", status: "verified" },
  ];
  const items = itemsQuery.data?.items || [];
  const okReasons = ["ok", "unchanged", "unchanged_verified", "verified"];
  const { ozonIssues, yandexIssues } = useMemo(() => {
    let ozon = 0;
    let yandex = 0;
    for (const item of items) {
      if (okReasons.includes(text(item.reason))) continue;
      if (text(item.marketplace) === "ozon") ozon++;
      else if (text(item.marketplace) === "yandex") yandex++;
    }
    return { ozonIssues: ozon, yandexIssues: yandex };
  }, [items]);

  return (
    <section className="page-section price-control-page">
      <PageHeader
        title="Цены и остатки"
        subtitle="Автоматизация цен и остатков Ozon/Yandex: что отправлено, что в retry и что требует внимания."
        action={(
          <button className="secondary-action" type="button" onClick={() => { void summary.refetch(); void itemsQuery.refetch(); }} disabled={summary.isFetching || itemsQuery.isFetching}>
            {summary.isFetching || itemsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Обновить контроль
          </button>
        )}
      />

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

      <section className="dashboard-metrics">
        <Stat label="SKU под контролем" value={summary.data?.total ?? 0} tone="accent" icon={<BadgeDollarSign size={18} />} />
        <Stat label="Retry цен" value={summary.data?.retryTotal ?? 0} tone={summary.data?.retryTotal ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
        <Stat label="Ozon autoarchive" value={summary.data?.ozonUnarchiveQueued ?? 0} tone={summary.data?.ozonUnarchiveQueued ? "warn" : "success"} icon={<Zap size={18} />} />
        <Stat label="Проблем Ozon / Yandex" value={`${ozonIssues} / ${yandexIssues}`} tone={ozonIssues || yandexIssues ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
      </section>

      <div className="control-grid price-controls">
        <label>
          Маркетплейс
          <SelectField
            ariaLabel="Маркетплейс"
            value={marketplace}
            onChange={setMarketplace}
            options={[
              { value: "all", label: "Ozon + Yandex" },
              { value: "ozon", label: "Только Ozon" },
              { value: "yandex", label: "Только Yandex" },
            ]}
          />
        </label>
        <label>
          Причина
          <SelectField
            ariaLabel="Причина"
            value={reason}
            onChange={setReason}
            options={[
              { value: "all", label: "Все причины" },
              ...reasonOptions.map(([key, count]) => ({ value: String(key), label: `${reasonLabel(key)} · ${count}` })),
            ]}
          />
        </label>
        <label>
          Статус отправки
          <SelectField
            ariaLabel="Apply статус"
            value={applyStatus}
            onChange={setApplyStatus}
            options={[
              { value: "all", label: "Все статусы" },
              { value: "queued", label: "В очереди" },
              { value: "verification_pending", label: "Ждем проверку" },
              { value: "verified", label: "Verified Ozon" },
              { value: "api_accepted", label: "API accepted" },
              { value: "ozon_price_not_applied", label: "Ozon не применил" },
              { value: "ozon_price_delayed", label: "Ozon отложил" },
            ]}
          />
        </label>
        <button className="primary-action danger-action" type="button" onClick={() => run.mutate({ marketplace, force: true, onlyChanged: false, reason: "sales_automation_reprice_selected" })} disabled={run.isPending}>
          {run.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Пересчитать выбранное
        </button>
        <button className="primary-action danger-action" type="button" onClick={() => { if (window.confirm("Отправить цены по ВСЕМ товарам прямо сейчас? Это перезапишет цены на маркетплейсах.")) run.mutate({ marketplace: "all", force: true, onlyChanged: false, reason: "force_all_immediate" }); }} disabled={run.isPending}>
          {run.isPending ? <Loader2 className="spin" size={16} /> : <Zap size={16} />} ОТПРАВИТЬ ВСЕ СЕЙЧАС
        </button>
        <button className="secondary-action danger-action" type="button" onClick={() => run.mutate({ marketplace: "ozon", force: true, onlyChanged: false, reason: "sales_automation_force_ozon" })} disabled={run.isPending}>
          <BadgeDollarSign size={16} /> Force Ozon
        </button>
        <button className="secondary-action" type="button" onClick={() => run.mutate({ marketplace: "yandex", force: true, onlyChanged: false, reason: "sales_automation_force_yandex" })} disabled={run.isPending}>
          <BadgeDollarSign size={16} /> Force Yandex
        </button>
        <button className="secondary-action" type="button" onClick={() => run.mutate({ marketplace, force: true, onlyChanged: false, reason: "sales_automation_retry_errors" })} disabled={run.isPending}>
          <RefreshCcw size={16} /> Retry ошибки
        </button>
      </div>

      {run.error ? <div className="inline-error">{String((run.error as Error).message || run.error)}</div> : null}
      {itemsQuery.error ? <div className="inline-error">{String((itemsQuery.error as Error).message || itemsQuery.error)}</div> : null}
      {runResult ? (
        <div className="success-strip">
          {runResult.accepted
            ? `Пересчет поставлен в очередь: ${numberValue(runResult.queued)} SKU · ${numberValue(runResult.queuedBatches)} batch · intent ${text(runResult.priceIntentId) || "new"}.`
            : `Отправлено: ${numberValue(runResult.sent)} · Ozon ${numberValue(runResult.ozonSent)} · Yandex ${numberValue(runResult.yandexSent)} · ошибок ${numberValue(runResult.failed)}`}
        </div>
      ) : null}

      <div className="price-reason-grid">
        {quickFilters.map((filter) => (
          <button
            className={reason === filter.reason && applyStatus === filter.status ? "is-active" : ""}
            type="button"
            key={`${filter.reason}-${filter.status}`}
            onClick={() => {
              setReason(filter.reason);
              setApplyStatus(filter.status);
            }}
          >
            <AlertTriangle size={14} />
            <span>{filter.label}</span>
          </button>
        ))}
        {reasonOptions.length ? reasonOptions.slice(0, 8).map(([key, count]) => (
          <button className={reason === key && applyStatus === "all" ? "is-active" : ""} type="button" key={key} onClick={() => { setReason(key); setApplyStatus("all"); }}>
            <AlertTriangle size={14} />
            <span>{reasonLabel(key)}</span>
            <strong>{count}</strong>
          </button>
        )) : <span className="muted-text">Причин пропуска пока нет.</span>}
      </div>

      <div className="table-panel price-table price-status-table">
        <div className="table-head">
          <span>Маркет</span><span>Артикул</span><span>Поставщик</span><span>Закупка</span><span>Расчет</span><span>Запрос</span><span>Verified</span><span>Apply</span><span>Intent</span><span>Ошибка</span><span>Проверено</span>
        </div>
        {items.map((item) => (
          <div className="table-row" key={`${text(item.marketplace)}-${text(item.productId || item.offerId)}-${text(item.target)}`}>
            <span data-label="Маркет"><Zap size={14} /> {text(item.marketplace)}</span>
            <span data-label="Артикул">{text(item.offerId)}</span>
            <span data-label="Поставщик">{supplierName(item)}</span>
            <span data-label="Закупка">{money(itemValue(item, "supplierPurchasePrice"))}</span>
            <span data-label="Расчет"><strong>{money(item.targetPrice ?? item.price)}</strong></span>
            <span data-label="Запрос">{money(itemValue(item, "lastRequestedPrice"))}</span>
            <span data-label="Verified">{money(itemValue(item, "lastVerifiedPrice"))}</span>
            <span data-label="Apply">{reasonLabel(itemValue(item, "priceApplyStatus"))}</span>
            <span data-label="Intent">{text(itemValue(item, "priceIntentId")).slice(0, 8) || "-"}</span>
            <span data-label="Ошибка">{text(item.lastError) || reasonLabel(item.reason)}</span>
            <span data-label="Проверено">{formatDate(itemValue(item, "lastPriceVerifiedAt") || item.updatedAt || item.lastCalculatedAt)}</span>
          </div>
        ))}
        {itemsQuery.isLoading && !items.length ? <ListSkeleton rows={8} /> : null}
        {!items.length && !itemsQuery.isLoading ? <div className="empty-state">Сейчас нет строк по выбранному фильтру. Автоматизация продолжает работать в фоне.</div> : null}
      </div>
    </section>
  );
}
