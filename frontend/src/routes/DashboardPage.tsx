import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, CheckCircle2, ClipboardList, PackageCheck, RefreshCw, ShoppingCart, Star, Truck } from "lucide-react";
import { fetchJson } from "../api";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { FinanceSummarySchema, OperationsSchema, SalesAutomationSummarySchema, SupplierPickingListSchema, SuppliersResponseSchema, WarehousePageSchema } from "../types";
import { asRecord, compactDate, errorMessage, numberValue } from "../lib/common";

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

  const supplierList = suppliers.data?.suppliers || [];
  const activeSuppliers = supplierList.filter(supplierActive).length;
  const financeSummary = asRecord(finance.data?.summary);
  const jobs = operations.data?.jobs || [];
  const rows = picking.data?.rows || [];
  const error = warehouse.error || suppliers.error || picking.error || finance.error || sales.error || operations.error;
  const refresh = () => {
    void warehouse.refetch();
    void suppliers.refetch();
    void picking.refetch();
    void finance.refetch();
    void sales.refetch();
    void operations.refetch();
  };

  return (
    <section className="page-section dashboard-page">
      <PageHeader
        title="Дашборд"
        subtitle="Общая картина склада: товары, поставщики, сборка, автоматизация и финансы."
        action={<button className="secondary-action" type="button" onClick={refresh}><RefreshCw size={16} /> Обновить</button>}
      />

      <section className="dashboard-metrics">
        <Stat label="Активных товаров" value={warehouse.data?.groupTotal || warehouse.data?.total || 0} tone="accent" icon={<PackageCheck size={18} />} trend={<MiniTrend />} delta="+5.3% к вчера" />
        <Stat label="Готовы к продаже" value={warehouse.data?.ready || 0} tone="success" icon={<CheckCircle2 size={18} />} trend={<MiniTrend tone="success" />} delta="+8% к вчера" />
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
        </aside>
      </section>

      {error ? <div className="inline-error">{errorMessage(error)}</div> : null}
    </section>
  );
}
