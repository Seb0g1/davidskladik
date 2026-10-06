import { useVirtualizer } from "@tanstack/react-virtual";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, BadgeDollarSign, CheckCircle2, Loader2, RefreshCcw, Search, Send, Zap } from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { ListSkeleton } from "../components/Skeleton";
import { Stat } from "../components/Stat";
import { MutationProductResponseSchema, SalesAutomationItemsSchema, SalesAutomationSummarySchema } from "../types";
import { useDebounced } from "../lib/common";

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
    in_retry: "ждёт повтора",
    ozon_limit: "лимит Ozon",
    stock_only_manual_price_missing: "нужна ручная цена склада",
    no_pricemaster_link: "нет привязки PriceMaster",
    not_ready: "поставщик не готов",
    unchanged_verified: "цена уже проверена",
    queued: "в очереди",
    api_accepted: "маркетплейс принял",
    verification_pending: "ждем проверку",
    verified: "проверено",
    ozon_price_not_applied: "Ozon не применил цену",
    ozon_price_delayed: "Ozon отложил цену",
    pm_live_timeout: "PriceMaster не ответил",
  };
  // an unknown code still reads as words, not as snake_case
  return labels[value] || value.replace(/_/g, " ") || "-";
};

export function PricesPage() {
  const [marketplace, setMarketplace] = useState("all");
  const [reason, setReason] = useState("all");
  const [applyStatus, setApplyStatus] = useState("all");
  const [q, setQ] = useState("");
  const debouncedQ = useDebounced(q, 200);
  const [runResult, setRunResult] = useState<Record<string, unknown> | null>(null);
  const runResultTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const queryClient = useQueryClient();
  const tableRef = useRef<HTMLDivElement>(null);

  const summary = useQuery({
    queryKey: ["sales-automation", "summary"],
    queryFn: () => fetchJson("/api/sales-automation/summary", SalesAutomationSummarySchema),
    refetchInterval: 45_000,
  });
  const itemsQuery = useQuery({
    queryKey: ["sales-automation", "items", marketplace, reason, applyStatus],
    queryFn: () => {
      const params = new URLSearchParams({ marketplace, limit: "2000" });
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
    { label: "Ждут повтора", reason: "in_retry", status: "all" },
    { label: "Ozon не применил", reason: "ozon_price_not_applied", status: "all" },
    { label: "PriceMaster не ответил", reason: "pm_live_timeout", status: "all" },
    { label: "нет поставщика", reason: "no_supplier", status: "all" },
    { label: "Нет привязки PriceMaster", reason: "no_pricemaster_link", status: "all" },
    { label: "Подтверждено", reason: "all", status: "verified" },
  ];
  const rawItems = itemsQuery.data?.items || [];
  const okReasons = ["ok", "unchanged", "unchanged_verified", "verified"];
  const items = useMemo(() => {
    const words = debouncedQ.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return rawItems;
    return rawItems.filter((item) => {
      const haystack = [text(item.offerId), text(item.marketplace), supplierName(item)].join(" ").toLowerCase();
      return words.every((w) => haystack.includes(w));
    });
  }, [rawItems, debouncedQ]);
  const rowVirtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => tableRef.current,
    estimateSize: () => 44,
    overscan: 8,
  });
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
        subtitle="Что ушло в Ozon и Маркет, что ждёт повтора и какие товары требуют внимания."
        action={(
          <button className="secondary-action" type="button" onClick={() => { void summary.refetch(); void itemsQuery.refetch(); }} disabled={summary.isFetching || itemsQuery.isFetching}>
            {summary.isFetching || itemsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Обновить контроль
          </button>
        )}
      />

      {/* цены уходят сами — страница для контроля; одна строка вместо баннера */}
      <div className={`price-auto-line${summary.data?.autoEnabled ? " is-on" : ""}`} role="status">
        <CheckCircle2 size={16} />
        <strong>{summary.data?.autoEnabled ? "Автоотправка включена" : "Автоотправка выключена"}</strong>
        <span>Цены и остатки уходят сами после изменений PriceMaster, курса, наценок и привязок.</span>
        <small>последний расчёт {formatDate(summary.data?.updatedAt)}</small>
      </div>

      <section className="dashboard-metrics">
        <Stat label="Товаров под контролем" value={Number(summary.data?.total ?? 0).toLocaleString("ru-RU")} tone="accent" icon={<BadgeDollarSign size={18} />} />
        <Stat label="Ждут повтора" value={Number(summary.data?.retryTotal ?? 0).toLocaleString("ru-RU")} tone={summary.data?.retryTotal ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
        <Stat label="В автоархиве Ozon" value={Number(summary.data?.ozonUnarchiveQueued ?? 0).toLocaleString("ru-RU")} tone={summary.data?.ozonUnarchiveQueued ? "warn" : "success"} icon={<Zap size={18} />} />
        <Stat label="Проблемы Ozon / Маркет" value={`${ozonIssues} / ${yandexIssues}`} tone={ozonIssues || yandexIssues ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
      </section>

      <div className="control-grid price-controls">
        <label>
          Маркетплейс
          <SelectField
            ariaLabel="Маркетплейс"
            value={marketplace}
            onChange={setMarketplace}
            options={[
              { value: "all", label: "Ozon + Маркет" },
              { value: "ozon", label: "Только Ozon" },
              { value: "yandex", label: "Только Маркет" },
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
              { value: "verified", label: "Подтверждено Ozon" },
              { value: "api_accepted", label: "Маркетплейс принял" },
              { value: "ozon_price_not_applied", label: "Ozon не применил" },
              { value: "ozon_price_delayed", label: "Ozon отложил" },
            ]}
          />
        </label>
      </div>
      <div className="price-manual-row">
        <span>Отправить вручную:</span>
        <button className="secondary-action" type="button" onClick={() => run.mutate({ marketplace, force: true, onlyChanged: false, reason: "sales_automation_retry_errors" })} disabled={run.isPending}>
          <RefreshCcw size={15} /> Повторить ошибки
        </button>
        <button className="secondary-action" type="button" onClick={() => run.mutate({ marketplace, force: true, onlyChanged: false, reason: "sales_automation_reprice_selected" })} disabled={run.isPending}>
          {run.isPending ? <Loader2 className="spin" size={15} /> : <Send size={15} />} Пересчитать по фильтру
        </button>
        <button className="secondary-action" type="button" onClick={() => run.mutate({ marketplace: "ozon", force: true, onlyChanged: false, reason: "sales_automation_force_ozon" })} disabled={run.isPending}>
          <BadgeDollarSign size={15} /> Все цены Ozon
        </button>
        <button className="secondary-action" type="button" onClick={() => run.mutate({ marketplace: "yandex", force: true, onlyChanged: false, reason: "sales_automation_force_yandex" })} disabled={run.isPending}>
          <BadgeDollarSign size={15} /> Все цены Маркета
        </button>
        <button className="secondary-action is-danger" type="button" onClick={() => { if (window.confirm("Отправить цены по ВСЕМ товарам прямо сейчас? Это перезапишет цены на маркетплейсах.")) run.mutate({ marketplace: "all", force: true, onlyChanged: false, reason: "force_all_immediate" }); }} disabled={run.isPending}>
          <Zap size={15} /> Всё и сразу
        </button>
      </div>

      {run.error ? <div className="inline-error">{String((run.error as Error).message || run.error)}</div> : null}
      {itemsQuery.error ? <div className="inline-error">{String((itemsQuery.error as Error).message || itemsQuery.error)}</div> : null}
      {runResult ? (
        <div className="success-strip">
          {runResult.accepted
            ? `Пересчёт поставлен в очередь: ${numberValue(runResult.queued)} товаров.`
            : `Отправлено: ${numberValue(runResult.sent)} · Ozon ${numberValue(runResult.ozonSent)} · Маркет ${numberValue(runResult.yandexSent)} · ошибок ${numberValue(runResult.failed)}`}
        </div>
      ) : null}

      <div className="price-search-row">
        <Search size={15} className="price-search-icon" />
        <input
          className="price-search-input"
          placeholder="Поиск по артикулу или поставщику…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        {items.length !== rawItems.length ? (
          <span className="muted-note" style={{ fontSize: 12, whiteSpace: "nowrap" }}>{items.length} из {rawItems.length}</span>
        ) : (
          <span className="muted-note" style={{ fontSize: 12, whiteSpace: "nowrap" }}>{rawItems.length} строк</span>
        )}
      </div>

      {/* one line of filters: the usual reasons with their counts, then whatever else the run reported */}
      <div className="price-chips" role="group" aria-label="Быстрые фильтры">
        <button type="button" className={`price-chip${reason === "all" && applyStatus === "all" ? " is-active" : ""}`} onClick={() => { setReason("all"); setApplyStatus("all"); }}>Все</button>
        {quickFilters.map((filter) => {
          const count = filter.status === "all" ? Number(reasons[filter.reason] || 0) : 0;
          return (
            <button
              className={`price-chip${reason === filter.reason && applyStatus === filter.status ? " is-active" : ""}${count ? " has-count" : ""}`}
              type="button"
              key={`${filter.reason}-${filter.status}`}
              onClick={() => { setReason(filter.reason); setApplyStatus(filter.status); }}
            >
              {filter.label}{count ? <b>{count}</b> : null}
            </button>
          );
        })}
        {reasonOptions.filter(([key]) => !quickFilters.some((f) => f.reason === key) && !okReasons.includes(key)).slice(0, 8).map(([key, count]) => (
          <button className={`price-chip has-count${reason === key && applyStatus === "all" ? " is-active" : ""}`} type="button" key={key} onClick={() => { setReason(key); setApplyStatus("all"); }}>
            {reasonLabel(key)}<b>{count}</b>
          </button>
        ))}
      </div>

      <div className="table-panel price-table price-status-table price-table--virtual" ref={tableRef}>
        <div className="table-head">
          <span>Маркет</span><span>Артикул</span><span>Поставщик</span><span>Закупка</span><span>Расчет</span><span>Запрос</span><span>Подтверждено</span><span>Статус</span><span>Интент</span><span>Ошибка</span><span>Проверено</span>
        </div>
        {itemsQuery.isLoading && !items.length ? <ListSkeleton rows={8} /> : null}
        {!items.length && !itemsQuery.isLoading ? <div className="empty-state">Сейчас нет строк по выбранному фильтру. Автоматизация продолжает работать в фоне.</div> : null}
        {items.length > 0 ? (
          <div style={{ height: rowVirtualizer.getTotalSize(), position: "relative" }}>
            {rowVirtualizer.getVirtualItems().map((virtualItem) => {
              const item = items[virtualItem.index];
              return (
                <div
                  className="table-row"
                  key={`${text(item.marketplace)}-${text(item.productId || item.offerId)}-${text(item.target)}`}
                  style={{ position: "absolute", top: 0, left: 0, width: "100%", transform: `translateY(${virtualItem.start}px)` }}
                >
                  <span data-label="Маркет"><Zap size={14} /> {text(item.marketplace)}</span>
                  <span data-label="Артикул">{text(item.offerId)}</span>
                  <span data-label="Поставщик">{supplierName(item)}</span>
                  <span data-label="Закупка">{money(itemValue(item, "supplierPurchasePrice"))}</span>
                  <span data-label="Расчет"><strong>{money(item.targetPrice ?? item.price)}</strong></span>
                  <span data-label="Запрос">{money(itemValue(item, "lastRequestedPrice"))}</span>
                  <span data-label="Подтверждено">{money(itemValue(item, "lastVerifiedPrice"))}</span>
                  <span data-label="Статус">{reasonLabel(itemValue(item, "priceApplyStatus"))}</span>
                  <span data-label="Интент">{text(itemValue(item, "priceIntentId")).slice(0, 8) || "-"}</span>
                  <span data-label="Ошибка">{text(item.lastError) || reasonLabel(item.reason)}</span>
                  <span data-label="Проверено">{formatDate(itemValue(item, "lastPriceVerifiedAt") || item.updatedAt || item.lastCalculatedAt)}</span>
                </div>
              );
            })}
          </div>
        ) : null}
      </div>
    </section>
  );
}
