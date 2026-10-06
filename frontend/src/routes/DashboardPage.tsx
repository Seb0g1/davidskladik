import { useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Archive, ChevronRight, ClipboardList, HelpCircle, MessageCircle, PackageX, RefreshCw, Star, Tag, TrendingUp, Truck, Wallet } from "lucide-react";
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

// Counts only: the warehouse page call is used for its totals (ready / without supplier), not for items
const warehouseUrl = "/api/warehouse/products/page?page=1&pageSize=1&q=&marketplace=all&linked=all&state=all&autoOnly=false&grouped=true";

const money = (value: unknown) => {
  const n = Number(value || 0);
  return `${Math.round(n).toLocaleString("ru-RU")} ₽`;
};

const statusText = (value: unknown) => ({
  queued: "ждёт",
  running: "идёт",
  completed: "готово",
  failed: "ошибка",
}[String(value || "")] || String(value || "—"));

const JOB_TITLES: Record<string, string> = {
  "yandex-import-send": "Импорт Ozon → Яндекс",
  "yandex-stock-sync": "Синхронизация остатков Яндекс",
  "yandex-price-push": "Отправка цен Яндекс",
  "linked-supplier-recovery": "Восстановление карточек маркетплейсов",
  "ozon-linked-unarchive": "Восстановление из автоархива Ozon",
  "restore-archived-stock": "Восстановление остатков из архива",
  "initialize-linked-ozon-stock": "Инициализация FBS-остатков Ozon",
  "scan-and-fix-zero-stock": "Исправление нулевых остатков",
  "yandex-card-quality-ai-drafts": "AI-черновики качества карточек Яндекс",
  "repair-dalik-disambiguation-links": "Ремонт привязок Далик",
  "repair-pricemaster-group-links": "Ремонт привязок Ozon/Яндекс",
  "marketplace-supplier-cart-preview": "Предпросмотр корзины поставщика",
  "marketplace-supplier-cart-commit": "Подтверждение корзины поставщика",
  "ozon-unarchive-queue-process": "Очередь разархивации Ozon",
  "sales-automation-run": "Запуск автоматизации продаж",
  "problem-products-repair": "Ремонт проблемных товаров",
  "brand-index-rebuild": "Перестройка индекса брендов",
  "health-deep": "Глубокая диагностика системы",
  "restore-yandex-markups": "Восстановление наценок Яндекс",
  "bulk-stale-recovery": "Массовый ремонт привязок PM",
};

function localizeJobTitle(title: string, type: string): string {
  return JOB_TITLES[type] || JOB_TITLES[title] || title || "Операция";
}

function supplierActive(supplier: { active?: boolean; stopped?: boolean }) {
  return supplier.active !== false && supplier.stopped !== true;
}

/** Income bars with the profit part inside each bar; one column per day, the day under the cursor is described. */
function SalesChart({ days }: { days: DayStat[] }) {
  const max = Math.max(...days.map((d) => d.income), 1);
  const [hover, setHover] = useState<number | null>(null);
  const active = hover === null ? null : days[hover];
  return (
    <div className="dash-chart">
      <div className="dash-chart-readout" aria-live="polite">
        {active
          ? <><b>{new Date(active.date).toLocaleDateString("ru-RU", { day: "numeric", month: "long" })}</b> · {active.orders} заказ. · выручка {money(active.income)} · прибыль {money(active.profit)}</>
          : <span>Выберите день на графике</span>}
      </div>
      <div className="dash-chart-bars" role="img" aria-label="Выручка и прибыль по дням" onMouseLeave={() => setHover(null)}>
        {days.map((d, i) => (
          <div key={d.date} className={`dash-chart-col${hover === i ? " is-hover" : ""}`} onMouseEnter={() => setHover(i)} onClick={() => setHover(i)}>
            <div className="dash-chart-bar" style={{ height: `${Math.max(2, (d.income / max) * 100)}%` }}>
              <div className="dash-chart-profit" style={{ height: `${d.income ? Math.max(0, Math.min(100, (d.profit / d.income) * 100)) : 0}%` }} />
            </div>
          </div>
        ))}
      </div>
      <div className="dash-chart-axis">
        {days.length >= 2 ? (
          <>
            <span>{days[0].date.slice(8, 10)}.{days[0].date.slice(5, 7)}</span>
            <span>{days[Math.floor(days.length / 2)].date.slice(8, 10)}.{days[Math.floor(days.length / 2)].date.slice(5, 7)}</span>
            <span>{days[days.length - 1].date.slice(8, 10)}.{days[days.length - 1].date.slice(5, 7)}</span>
          </>
        ) : null}
      </div>
    </div>
  );
}

type AttentionItem = { key: string; icon: ReactNode; label: string; count: number; hint?: string; href: string; tone?: "warn" | "danger" };

export function DashboardPage() {
  const [analyticsPeriod, setAnalyticsPeriod] = useState<"7d" | "30d">("30d");
  const warehouse = useQuery({ queryKey: ["dashboard", "warehouse"], queryFn: () => fetchJson(warehouseUrl, WarehousePageSchema) });
  const suppliers = useQuery({ queryKey: ["dashboard", "suppliers"], queryFn: () => fetchJson("/api/suppliers", SuppliersResponseSchema) });
  const picking = useQuery({ queryKey: ["dashboard", "picking"], queryFn: () => fetchJson("/api/supplier-picking-list?status=open&limit=60", SupplierPickingListSchema) });
  const finance = useQuery({ queryKey: ["dashboard", "finance"], queryFn: () => fetchJson("/api/finance/summary?period=30d&linkedOnly=true", FinanceSummarySchema) });
  const sales = useQuery({ queryKey: ["dashboard", "sales"], queryFn: () => fetchJson("/api/sales-automation/summary", SalesAutomationSummarySchema) });
  const operations = useQuery({ queryKey: ["dashboard", "operations"], queryFn: () => fetchJson("/api/operations?limit=8", OperationsSchema), refetchInterval: 5000 });
  const summary = useQuery({ queryKey: ["dashboard", "summary"], queryFn: () => fetchJson("/api/dashboard/summary", DashboardSummarySchema) });
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
  const pickingRows = picking.data?.rows || [];
  const analytics = analyticsQuery.data;
  const analyticsReady = Boolean(analytics?.ok);
  const analyticsDays = useMemo(() => analytics?.byDay || [], [analytics]);
  const salesToday = summary.data?.salesToday || { orders: 0, income: 0, profit: 0 };
  const salesWeek = summary.data?.salesWeek || { orders: 0, income: 0, profit: 0 };
  const topSuppliers = summary.data?.topSuppliers || [];
  const archiveBacklog = summary.data?.archiveBacklog || { yandex: 0, ozon: 0, ozonDue: 0 };
  const notifications = summary.data?.notifications || { unread: 0, byType: {} as Record<string, number> };
  const priceHealth = summary.data?.priceHealth || { stalePriceLinked: 0, staleHours: 1, alertThreshold: 50, alert: false, oldestPriceJobAgeMs: 0, oldestPriceJobAlertThresholdMs: 10 * 60_000, oldestPriceJobAlert: false };
  const withoutSupplier = Number(warehouse.data?.withoutSupplier || 0);
  const error = warehouse.error || suppliers.error || picking.error || finance.error || sales.error || operations.error || summary.error;

  // What needs a person today, most urgent first; zero counts are not shown
  const attention: AttentionItem[] = [
    { key: "reviews", icon: <Star size={16} />, label: "Отзывы ждут ответа", count: Number(notifications.byType.review || 0), href: "/app/reviews" },
    { key: "questions", icon: <HelpCircle size={16} />, label: "Вопросы покупателей", count: Number(notifications.byType.question || 0), href: "/app/questions" },
    { key: "chats", icon: <MessageCircle size={16} />, label: "Непрочитанные чаты", count: Number(notifications.byType.chat || 0), href: "/app/chats" },
    { key: "prices", icon: <Tag size={16} />, label: `Цены не сходятся дольше ${priceHealth.staleHours} ч`, count: Number(priceHealth.stalePriceLinked || 0), href: "/app/price-guard", tone: priceHealth.alert ? "danger" : "warn" },
    { key: "price-queue", icon: <Tag size={16} />, label: "Цены в очереди на отправку", count: Number(summary.data?.priceQueue || 0), hint: priceHealth.oldestPriceJobAlert ? `старейшая ${Math.round(priceHealth.oldestPriceJobAgeMs / 60000)} мин` : undefined, href: "/app/prices", tone: priceHealth.oldestPriceJobAlert ? "danger" : undefined },
    { key: "no-supplier", icon: <PackageX size={16} />, label: "Товары без поставщика", count: withoutSupplier, href: "/app/no-supplier" },
    { key: "ozon-archive", icon: <Archive size={16} />, label: "Ozon: ждут разархивации", count: Number(archiveBacklog.ozon || 0), hint: archiveBacklog.ozonDue ? `готово ${archiveBacklog.ozonDue}` : undefined, href: "/app/recovery-queue" },
    { key: "yandex-archive", icon: <Archive size={16} />, label: "Яндекс: привязанные в архиве", count: Number(archiveBacklog.yandex || 0), href: "/app/recovery-queue" },
    { key: "retries", icon: <RefreshCw size={16} />, label: "Повторы отправки цены", count: Number(sales.data?.retryTotal || 0), href: "/app/prices" },
  ].filter((item) => item.count > 0) as AttentionItem[];

  const refresh = () => {
    for (const q of [warehouse, suppliers, picking, finance, sales, operations, summary, analyticsQuery]) void q.refetch();
  };
  const loadingSummary = summary.isLoading || warehouse.isLoading;

  return (
    <section className="page-section dashboard-page dash2">
      <PageHeader
        title="Дашборд"
        subtitle="Что продали, что ждёт людей и как идут фоновые задачи."
        action={<button className="secondary-action" type="button" onClick={refresh}><RefreshCw size={16} /> Обновить</button>}
      />

      <section className="dashboard-metrics dash-kpis">
        <Stat label="Сегодня" value={money(salesToday.income)} tone="success" icon={<TrendingUp size={18} />} delta={`${salesToday.orders} заказ. · прибыль ${money(salesToday.profit)}`} />
        <Stat label="За 7 дней" value={money(salesWeek.profit)} tone="accent" icon={<Wallet size={18} />} delta={`прибыль · ${salesWeek.orders} заказ. · выручка ${money(salesWeek.income)}`} />
        <Stat label="Сборка" value={Number(picking.data?.total || pickingRows.length || 0).toLocaleString("ru-RU")} icon={<ClipboardList size={18} />} delta="открытых строк" />
        <Stat label="Без поставщика" value={withoutSupplier.toLocaleString("ru-RU")} tone={withoutSupplier ? "warn" : "success"} icon={<AlertTriangle size={18} />} delta={`готовы к продаже ${Number(warehouse.data?.ready || 0).toLocaleString("ru-RU")}`} />
      </section>

      <section className="dash-grid">
        <div className="dash-main">
          <section className="table-panel dash-panel">
            <div className="section-title">
              <div><span>Сегодня</span><h3>Требует внимания</h3></div>
            </div>
            {loadingSummary ? <div className="soft-empty"><RefreshCw className="spin" size={16} /> Считаю…</div> : null}
            {!loadingSummary && !attention.length ? <div className="dash-all-clear">Всё разобрано — ни ответов, ни очередей не ждёт.</div> : null}
            <div className="dash-attention">
              {attention.map((item) => (
                <a key={item.key} href={item.href} className={`dash-attention-row${item.tone ? ` is-${item.tone}` : ""}`}>
                  <span className="dash-attention-icon">{item.icon}</span>
                  <span className="dash-attention-label">{item.label}{item.hint ? <small>{item.hint}</small> : null}</span>
                  <b>{item.count.toLocaleString("ru-RU")}</b>
                  <ChevronRight size={16} className="dash-attention-go" />
                </a>
              ))}
            </div>
          </section>

          <section className="table-panel dash-panel">
            <div className="section-title">
              <div><span>Продажи</span><h3>{analyticsPeriod === "7d" ? "За 7 дней" : "За 30 дней"}</h3></div>
              <div className="dash-period" role="group" aria-label="Период">
                <button type="button" className={analyticsPeriod === "7d" ? "is-on" : ""} aria-pressed={analyticsPeriod === "7d"} onClick={() => setAnalyticsPeriod("7d")}>7 дней</button>
                <button type="button" className={analyticsPeriod === "30d" ? "is-on" : ""} aria-pressed={analyticsPeriod === "30d"} onClick={() => setAnalyticsPeriod("30d")}>30 дней</button>
              </div>
            </div>
            {analyticsQuery.isLoading ? (
              <div className="soft-empty"><RefreshCw className="spin" size={14} /> Загружаю продажи…</div>
            ) : !analyticsReady || !analyticsDays.length ? (
              <div className="soft-empty">Продаж за период нет.</div>
            ) : (
              <>
                <div className="dash-sales-totals">
                  <span><small>Выручка</small><b>{money(analyticsDays.reduce((s, d) => s + d.income, 0))}</b></span>
                  <span><small>Прибыль</small><b className="is-profit">{money(analyticsDays.reduce((s, d) => s + d.profit, 0))}</b></span>
                  <span><small>Заказов</small><b>{analytics!.totalOrders}</b></span>
                </div>
                <SalesChart days={analyticsDays} />
                <div className="dash-sales-split">
                  <div>
                    <div className="dash-label">По каналам</div>
                    {(analytics!.byMarketplace || []).map((mp, _i, arr) => {
                      const max = Math.max(...arr.map((m) => m.income), 1);
                      return (
                        <div className="dash-mp-row" key={mp.marketplace}>
                          <span>{MP_LABELS[mp.marketplace] || mp.marketplace}</span>
                          <div className="dash-mp-bar"><i style={{ width: `${Math.round((mp.income / max) * 100)}%` }} /></div>
                          <b>{money(mp.profit)}</b>
                          <small>{mp.orders} шт.</small>
                        </div>
                      );
                    })}
                  </div>
                  <div>
                    <div className="dash-label">Топ товаров по прибыли</div>
                    <ol className="dash-top">
                      {(analytics!.topProducts || []).slice(0, 7).map((p) => (
                        <li key={p.offerId}>
                          <span title={`${p.name} (${p.offerId})`}>{p.name || p.offerId}</span>
                          <small>{p.orders} шт.</small>
                          <b>{money(p.profit)}</b>
                        </li>
                      ))}
                    </ol>
                    {!(analytics!.topProducts || []).length ? <div className="soft-empty">Нет данных</div> : null}
                  </div>
                </div>
              </>
            )}
          </section>
        </div>

        <aside className="dash-side">
          <section className="dashboard-summary-card dash-card">
            <div className="section-title compact-title"><div><span>Финансы</span><h3>30 дней</h3></div><Wallet size={18} /></div>
            <div className="dashboard-money"><strong>{money(financeSummary.netProfit)}</strong><span>чистая прибыль</span></div>
            <dl className="dash-dl">
              <div><dt>Выручка</dt><dd>{money(financeSummary.orderIncome)}</dd></div>
              <div><dt>Закупка</dt><dd>{money(financeSummary.purchaseCost)}</dd></div>
              <div><dt>Заказов</dt><dd>{String(financeSummary.orders || 0)}</dd></div>
            </dl>
          </section>

          <section className="dashboard-summary-card dash-card">
            <div className="section-title compact-title"><div><span>Поставщики</span><h3>Прибыль за неделю</h3></div><Truck size={18} /></div>
            {topSuppliers.length ? (
              <dl className="dash-dl">
                {topSuppliers.slice(0, 5).map((s) => <div key={s.supplierName}><dt>{s.supplierName}</dt><dd>{money(s.profit)}</dd></div>)}
              </dl>
            ) : <div className="soft-empty">Продаж за неделю нет.</div>}
            <div className="dash-foot">Работают {activeSuppliers} из {supplierList.length}{supplierList.length - activeSuppliers ? ` · остановлены ${supplierList.length - activeSuppliers}` : ""}</div>
          </section>

          <section className="dashboard-summary-card dash-card">
            <div className="section-title compact-title"><div><span>Фон</span><h3>Задачи</h3></div><a className="dash-link" href="/app/operations">все</a></div>
            {jobs.slice(0, 5).map((job) => (
              <div className={`dash-job is-${String(job.status || "")}`} key={String(job.id)}>
                <span className="dash-job-title">{localizeJobTitle(String(job.title || ""), String(job.type || ""))}</span>
                <small>{statusText(job.status)} · {compactDate(String(job.createdAt || ""))}</small>
                {String(job.status) === "running"
                  ? <div className="dash-job-progress"><i style={{ width: `${Math.round(numberValue(job.progress, 0))}%` }} /></div>
                  : null}
              </div>
            ))}
            {!operations.isLoading && !jobs.length ? <div className="soft-empty">Задач нет.</div> : null}
            <div className="dash-foot">Автоматизация продаж {sales.data?.autoEnabled ? "включена" : "выключена"}</div>
          </section>
        </aside>
      </section>

      {error ? <div className="inline-error">{errorMessage(error)}</div> : null}
    </section>
  );
}
