import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Archive, CheckCircle2, ClipboardList, HelpCircle, MessageCircle, PackageCheck, RefreshCw, ShoppingCart, Star, TrendingUp, Truck } from "lucide-react";
import { fetchJson } from "../api";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { DashboardSummarySchema, FinanceSummarySchema, OperationsSchema, SalesAutomationSummarySchema, SupplierPickingListSchema, SuppliersResponseSchema, WarehousePageSchema } from "../types";
import { asRecord, compactDate, errorMessage, numberValue } from "../lib/common";

type DayStat = { date: string; orders: number; income: number; profit: number };
type MpStat = { marketplace: string; orders: number; income: number; profit: number };
type ProductStat = { offerId: string; name: string; orders: number; income: number; profit: number };
type SalesAnalytics = { ok: boolean; period: string; totalOrders: number; byDay: DayStat[]; byMarketplace: MpStat[]; topProducts: ProductStat[] };

const MP_LABELS: Record<string, string> = { ozon: "Ozon", yandex: "Яндекс", wb: "WB", avito: "Avito", other: "Прочие" };

function SalesBarChart({ days, metric }: { days: DayStat[]; metric: "income" | "profit" }) {
  const values = days.map((d) => d[metric]);
  const maxVal = Math.max(...values, 1);
  const barW = Math.max(4, Math.min(18, Math.floor(540 / Math.max(days.length, 1)) - 2));
  const chartW = days.length * (barW + 2);
  return (
    <svg className="sales-bar-chart" viewBox={`0 0 ${chartW} 60`} preserveAspectRatio="none" aria-label="График продаж по дням">
      {days.map((d, i) => {
        const h = Math.max(2, Math.round((d[metric] / maxVal) * 52));
        const x = i * (barW + 2);
        return (
          <g key={d.date}>
            <title>{d.date}: {Math.round(d[metric]).toLocaleString("ru-RU")} ₽ ({d.orders} заказ.)</title>
            <rect x={x} y={60 - h} width={barW} height={h} className={`bar-${metric}`} rx={2} />
          </g>
        );
      })}
    </svg>
  );
}

const warehouseUrl = "/api/warehouse/products/page?page=1&pageSize=8&q=&marketplace=all&linked=all&state=all&autoOnly=false&grouped=true";

const money = (value: unknown) => {
  const n = Number(value || 0);
  return n ? `${Math.round(n).toLocaleString("ru-RU")} ₽` : "-";
};

const statusText = (value: unknown) => ({
  queued: "Ожидает",
  running: "В работе",
  completed: "Готово",
  failed: "Ошибка",
}[String(value || "")] || String(value || "-"));

function supplierActive(supplier: { active?: boolean; stopped?: boolean }) {
  return supplier.active !== false && supplier.stopped !== true;
}

function MiniTrend({ tone = "" }: { tone?: "success" | "warn" | "danger" | "" }) {
  return (
    <svg className={`mini-trend ${tone}`} viewBox="0 0 120 36" aria-hidden="true">
      <path d="M3 30 L18 24 L31 28 L45 17 L58 20 L73 11 L88 15 L101 6 L117 10" />
    </svg>
  );
}

export function DashboardPage() {
  const [analyticsPeriod, setAnalyticsPeriod] = useState<"7d" | "30d">("30d");
  const warehouse = useQuery({
    queryKey: ["dashboard", "warehouse"],
    queryFn: () => fetchJson(warehouseUrl, WarehousePageSchema),
  });
  const suppliers = useQuery({
    queryKey: ["dashboard", "suppliers"],
    queryFn: () => fetchJson("/api/suppliers", SuppliersResponseSchema),
  });
  const picking = useQuery({
    queryKey: ["dashboard", "picking"],
    queryFn: () => fetchJson("/api/supplier-picking-list?status=open&limit=60", SupplierPickingListSchema),
  });
  const finance = useQuery({
    queryKey: ["dashboard", "finance"],
    queryFn: () => fetchJson("/api/finance/summary?period=30d&linkedOnly=true", FinanceSummarySchema),
  });
  const sales = useQuery({
    queryKey: ["dashboard", "sales"],
    queryFn: () => fetchJson("/api/sales-automation/summary", SalesAutomationSummarySchema),
  });
  const operations = useQuery({
    queryKey: ["dashboard", "operations"],
    queryFn: () => fetchJson("/api/operations?limit=8", OperationsSchema),
    refetchInterval: 5000,
  });
  const summary = useQuery({
    queryKey: ["dashboard", "summary"],
    queryFn: () => fetchJson("/api/dashboard/summary", DashboardSummarySchema),
  });
  const analyticsQuery = useQuery({
    queryKey: ["dashboard", "analytics", analyticsPeriod],
    queryFn: async () => {
      const res = await fetch(`/api/analytics/sales?period=${analyticsPeriod}`, { credentials: "same-origin" });
      return res.json() as Promise<SalesAnalytics>;
    },
  });

  const supplierList = suppliers.data?.suppliers || [];
  const activeSuppliers = supplierList.filter(supplierActive).length;
  const financeSummary = asRecord(finance.data?.summary);
  const jobs = operations.data?.jobs || [];
  const rows = picking.data?.rows || [];
  const analytics = analyticsQuery.data;
  const analyticsReady = Boolean(analytics?.ok);
  const analyticsDays = useMemo(() => analytics?.byDay || [], [analytics]);
  const analyticsMaxIncome = useMemo(() => Math.max(...analyticsDays.map((d) => d.income), 1), [analyticsDays]);
  const salesToday = summary.data?.salesToday || { orders: 0, income: 0, profit: 0 };
  const salesWeek = summary.data?.salesWeek || { orders: 0, income: 0, profit: 0 };
  const topSuppliers = summary.data?.topSuppliers || [];
  const archiveBacklog = summary.data?.archiveBacklog || { yandex: 0, ozon: 0, ozonDue: 0 };
  const notifications = summary.data?.notifications || { unread: 0, byType: {} };
  const priceHealth = summary.data?.priceHealth || { stalePriceLinked: 0, staleHours: 1, alertThreshold: 50, alert: false, oldestPriceJobAgeMs: 0, oldestPriceJobAlertThresholdMs: 10 * 60_000, oldestPriceJobAlert: false };
  const error = warehouse.error || suppliers.error || picking.error || finance.error || sales.error || operations.error || summary.error;
  const refresh = () => {
    void warehouse.refetch();
    void suppliers.refetch();
    void picking.refetch();
    void finance.refetch();
    void sales.refetch();
    void operations.refetch();
    void summary.refetch();
    void analyticsQuery.refetch();
  };

  return (
    <section className="page-section dashboard-page">
      <PageHeader
        title="Дашборд"
        subtitle="Общая картина склада: товары, поставщики, сборка, автоматизация и финансы."
        action={<button className="secondary-action" type="button" onClick={refresh}><RefreshCw size={16} /> Обновить</button>}
      />

      <section className="dashboard-metrics">
        <Stat label="Активных товаров" value={warehouse.data?.groupTotal || warehouse.data?.total || 0} tone="accent" icon={<PackageCheck size={18} />} trend={<MiniTrend />} />
        <Stat label="Готовы к продаже" value={warehouse.data?.ready || 0} tone="success" icon={<CheckCircle2 size={18} />} trend={<MiniTrend tone="success" />} />
        <Stat label="Продажи сегодня" value={money(salesToday.income)} tone="success" icon={<TrendingUp size={18} />} trend={<MiniTrend tone="success" />} delta={`${salesToday.orders} заказ(ов), прибыль ${money(salesToday.profit)}`} />
        <Stat label="Очередь сборки" value={picking.data?.total || rows.length || 0} icon={<ClipboardList size={18} />} trend={<MiniTrend />} delta="в работе" />
        <Stat label="Нужны действия" value={warehouse.data?.withoutSupplier || sales.data?.retryTotal || 0} tone="warn" icon={<AlertTriangle size={18} />} trend={<MiniTrend tone="warn" />} delta="проверить" />
      </section>

      <section className="dashboard-layout">
        <div className="dashboard-main">
          <section className="table-panel dashboard-panel">
            <div className="section-title">
              <div><span>Склад</span><h3>Последние карточки каталога</h3></div>
              <a className="secondary-action" href="/app/warehouse">Открыть склад</a>
            </div>
            <div className="dashboard-product-list">
              {(warehouse.data?.items || []).slice(0, 6).map((item) => {
                const row = asRecord(item);
                const primary = asRecord(row.primary || (Array.isArray(row.products) ? row.products[0] : row));
                return (
                  <a className="dashboard-product-row" href={`/app/warehouse/${encodeURIComponent(String(row.groupKey || primary.offerId || primary.id || ""))}`} key={String(row.groupKey || primary.id || primary.offerId)}>
                    <span className="dashboard-product-thumb">
                      {primary.imageUrl ? <img src={String(primary.imageUrl)} alt="" loading="lazy" /> : <PackageCheck size={16} />}
                    </span>
                    <span>
                      <strong>{String(primary.name || primary.offerId || "Товар")}</strong>
                      <small>{String(primary.offerId || primary.sku || "-")} · {String(primary.brand || "без бренда")}</small>
                    </span>
                    <b>{money(primary.currentPrice || primary.targetPrice || primary.newPrice)}</b>
                  </a>
                );
              })}
              {warehouse.isLoading ? <div className="soft-empty"><RefreshCw className="spin" size={16} /> Загружаю товары...</div> : null}
            </div>
          </section>

          <section className="table-panel dashboard-panel">
            <div className="section-title">
              <div><span>Операции</span><h3>Последние фоновые задачи</h3></div>
              <a className="secondary-action" href="/app/operations">Все операции</a>
            </div>
            {jobs.slice(0, 5).map((job) => (
              <article className="dashboard-job-row" key={String(job.id)}>
                <div>
                  <strong>{String(job.title || job.type || "Операция")}</strong>
                  <span>{statusText(job.status)} · {compactDate(String(job.createdAt || ""))}</span>
                </div>
                <div className="progress-pill">{Math.round(numberValue(job.progress, 0))}%</div>
              </article>
            ))}
            {!operations.isLoading && !jobs.length ? <div className="soft-empty">Операций пока нет.</div> : null}
          </section>
        </div>

        <aside className="dashboard-side">
          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Поставщики</span><h3>Состояние сети</h3></div>
              <Truck size={18} />
            </div>
            <div className="summary-grid compact-summary">
              <div><span>Активные</span><strong>{activeSuppliers}</strong></div>
              <div><span>Остановлены</span><strong>{Math.max(0, supplierList.length - activeSuppliers)}</strong></div>
              <div><span>Всего</span><strong>{supplierList.length}</strong></div>
            </div>
          </section>

          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Финансы</span><h3>30 дней</h3></div>
              <ShoppingCart size={18} />
            </div>
            <div className="dashboard-money">
              <strong>{money(financeSummary.netProfit)}</strong>
              <span>чистая прибыль</span>
            </div>
            <div className="dashboard-kpis">
              <span>Выручка <b>{money(financeSummary.orderIncome)}</b></span>
              <span>Заказов <b>{String(financeSummary.orders || 0)}</b></span>
              <span>Закупка <b>{money(financeSummary.purchaseCost)}</b></span>
            </div>
          </section>

          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Маркетплейсы</span><h3>Отзывы и продажи</h3></div>
              <Star size={18} />
            </div>
            <div className="dashboard-kpis">
              <span>Автоматизация <b>{sales.data?.autoEnabled ? "активна" : "выключена"}</b></span>
              <span>Повторов цены <b>{sales.data?.retryTotal || 0}</b></span>
              <span>Восстановление Ozon <b>{sales.data?.ozonUnarchiveQueued || 0}</b></span>
            </div>
          </section>

          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Продажи</span><h3>За неделю</h3></div>
              <TrendingUp size={18} />
            </div>
            <div className="dashboard-money">
              <strong>{money(salesWeek.profit)}</strong>
              <span>чистая прибыль за 7 дней</span>
            </div>
            <div className="dashboard-kpis">
              <span>Заказов <b>{salesWeek.orders}</b></span>
              <span>Выручка <b>{money(salesWeek.income)}</b></span>
            </div>
            {topSuppliers.length ? (
              <div className="dashboard-kpis">
                {topSuppliers.slice(0, 5).map((supplier) => (
                  <span key={supplier.supplierName}>
                    {supplier.supplierName} <b>{money(supplier.profit)}</b>
                  </span>
                ))}
              </div>
            ) : <div className="soft-empty">Нет продаж за неделю.</div>}
          </section>

          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Очередь и архив</span><h3>Требует внимания</h3></div>
              <Archive size={18} />
            </div>
            <div className="dashboard-kpis">
              <span>Очередь цен <b>{summary.data?.priceQueue ?? 0}</b></span>
              <span>Архив Яндекс (привязано) <b>{archiveBacklog.yandex}</b></span>
              <span>Очередь восстановления Ozon <b>{archiveBacklog.ozon}</b> (готово {archiveBacklog.ozonDue})</span>
              <span className={priceHealth.alert ? "dashboard-kpi-alert" : undefined}>
                Цены не сходятся &gt;{priceHealth.staleHours} ч <b>{priceHealth.stalePriceLinked}</b>
              </span>
              <span className={priceHealth.oldestPriceJobAlert ? "dashboard-kpi-alert" : undefined}>
                Старейшая задача цены <b>{Math.round(priceHealth.oldestPriceJobAgeMs / 60000)} мин</b>
              </span>
            </div>
          </section>

          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Уведомления</span><h3>Непрочитанные</h3></div>
              <Star size={18} />
            </div>
            <div className="dashboard-kpis">
              <span><MessageCircle size={14} /> Чаты <b>{notifications.byType.chat || 0}</b></span>
              <span><HelpCircle size={14} /> Вопросы <b>{notifications.byType.question || 0}</b></span>
              <span><Star size={14} /> Отзывы <b>{notifications.byType.review || 0}</b></span>
              <span>Всего <b>{notifications.unread}</b></span>
            </div>
          </section>
        </aside>
      </section>

      {/* Аналитика продаж */}
      <section className="dashboard-analytics">
        <div className="section-title">
          <div><span>Аналитика</span><h3>Продажи по дням и каналам</h3></div>
          <div className="row-actions">
            <button className={`secondary-action${analyticsPeriod === "7d" ? " active" : ""}`} type="button" onClick={() => setAnalyticsPeriod("7d")}>7 дней</button>
            <button className={`secondary-action${analyticsPeriod === "30d" ? " active" : ""}`} type="button" onClick={() => setAnalyticsPeriod("30d")}>30 дней</button>
          </div>
        </div>

        {analyticsQuery.isLoading ? (
          <div className="table-note"><RefreshCw className="spin" size={14} /> Загружаю аналитику…</div>
        ) : !analyticsReady ? (
          <div className="table-note">Нет данных о продажах за период.</div>
        ) : (
          <div className="analytics-grid">
            {/* График по дням */}
            <div className="analytics-chart-panel">
              <div className="analytics-chart-legend">
                <span className="legend-income">Выручка</span>
                <span className="legend-profit">Прибыль</span>
                <span className="analytics-total">{analytics!.totalOrders} заказ(ов) · выручка {money(analytics!.byDay.reduce((s, d) => s + d.income, 0))} · прибыль {money(analytics!.byDay.reduce((s, d) => s + d.profit, 0))}</span>
              </div>
              <div className="analytics-chart-scroll">
                {analyticsDays.length ? <SalesBarChart days={analyticsDays} metric="income" /> : <div className="soft-empty">Нет данных</div>}
              </div>
              <div className="analytics-chart-dates">
                {analyticsDays.length >= 2 ? (
                  <>
                    <span>{analyticsDays[0].date.slice(5)}</span>
                    <span>{analyticsDays[Math.floor(analyticsDays.length / 2)].date.slice(5)}</span>
                    <span>{analyticsDays[analyticsDays.length - 1].date.slice(5)}</span>
                  </>
                ) : null}
              </div>
            </div>

            {/* Каналы продаж */}
            <div className="analytics-mp-panel">
              <div className="analytics-section-label">По каналу</div>
              {(analytics!.byMarketplace || []).map((mp, _i, arr) => {
                const maxMpIncome = Math.max(...arr.map((m) => m.income), 1);
                const pct = Math.round((mp.income / maxMpIncome) * 100);
                return (
                  <div className="analytics-mp-row" key={mp.marketplace}>
                    <span className="analytics-mp-name">{MP_LABELS[mp.marketplace] || mp.marketplace}</span>
                    <div className="analytics-mp-bar-wrap">
                      <div className="analytics-mp-bar" style={{ width: `${pct}%` }} />
                    </div>
                    <span className="analytics-mp-val">{money(mp.profit)}</span>
                    <span className="analytics-mp-orders">{mp.orders} шт.</span>
                  </div>
                );
              })}
            </div>

            {/* Топ товаров */}
            <div className="analytics-top-panel">
              <div className="analytics-section-label">Топ товаров по прибыли</div>
              {(analytics!.topProducts || []).slice(0, 10).map((p, i) => (
                <div className="analytics-product-row" key={p.offerId}>
                  <span className="analytics-product-rank">{i + 1}</span>
                  <span className="analytics-product-name" title={`${p.name} (${p.offerId})`}>{p.name || p.offerId}</span>
                  <span className="analytics-product-orders">{p.orders} шт.</span>
                  <span className="analytics-product-profit">{money(p.profit)}</span>
                </div>
              ))}
              {!(analytics!.topProducts || []).length ? <div className="soft-empty">Нет данных</div> : null}
            </div>
          </div>
        )}
      </section>

      {error ? <div className="inline-error">{errorMessage(error)}</div> : null}
    </section>
  );
}
