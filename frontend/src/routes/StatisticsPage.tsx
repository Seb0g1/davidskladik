import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { BarChart3, RefreshCw, Users } from "lucide-react";
import { fetchJson } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { FinanceSummarySchema, SupplierPickingListSchema, UsersStatsResponseSchema, WarehousePageSchema } from "../types";
import { asRecord, compactDate, errorMessage, numberValue } from "../lib/common";

const money = (value: unknown) => {
  const n = Number(value || 0);
  return n ? `${Math.round(n).toLocaleString("ru-RU")} ₽` : "-";
};

const warehouseUrl = "/api/warehouse/products/page?page=1&pageSize=1&q=&marketplace=all&linked=all&state=all&autoOnly=false&grouped=true";

export function StatisticsPage() {
  const [period, setPeriod] = useState("30d");
  const users = useQuery({
    queryKey: ["statistics", "users", period],
    queryFn: () => fetchJson(`/api/users/stats?period=${encodeURIComponent(period)}&includeInactive=1&includeDeleted=1`, UsersStatsResponseSchema),
  });
  const finance = useQuery({
    queryKey: ["statistics", "finance", period],
    queryFn: () => fetchJson(`/api/finance/summary?period=${encodeURIComponent(period)}&linkedOnly=true`, FinanceSummarySchema),
  });
  const warehouse = useQuery({
    queryKey: ["statistics", "warehouse"],
    queryFn: () => fetchJson(warehouseUrl, WarehousePageSchema),
  });
  const picking = useQuery({
    queryKey: ["statistics", "picking"],
    queryFn: () => fetchJson("/api/supplier-picking-list?status=all&limit=1", SupplierPickingListSchema),
  });

  const summary = asRecord(users.data?.summary);
  const financeSummary = asRecord(finance.data?.summary);
  const rows = users.data?.users || [];
  const sortedRows = useMemo(
    () => [...rows].sort((a, b) => numberValue(b.actionsTotal, 0) - numberValue(a.actionsTotal, 0)),
    [rows],
  );
  const error = users.error || finance.error || warehouse.error || picking.error;
  const refresh = () => {
    void users.refetch();
    void finance.refetch();
    void warehouse.refetch();
    void picking.refetch();
  };

  return (
    <section className="page-section statistics-page">
      <PageHeader
        title="Статистика"
        subtitle="Работа сотрудников, привязки PriceMaster, сборка и финансовый результат."
        action={(
          <div className="row-actions">
            <SelectField
              ariaLabel="Период"
              value={period}
              onChange={setPeriod}
              options={[
                { value: "7d", label: "7 дней" },
                { value: "30d", label: "30 дней" },
                { value: "90d", label: "90 дней" },
                { value: "all", label: "Все" },
              ]}
            />
            <button className="secondary-action" type="button" onClick={refresh}><RefreshCw size={16} /> Обновить</button>
          </div>
        )}
      />

      <section className="dashboard-metrics">
        <Stat label="Действий" value={summary.actionsTotal || 0} tone="accent" icon={<BarChart3 size={18} />} />
        <Stat label="Привязок добавлено" value={summary.linksAdded || 0} icon={<RefreshCw size={18} />} />
        <Stat label="Товаров затронуто" value={summary.affectedProducts || 0} icon={<Users size={18} />} />
        <Stat label="Прибыль" value={money(financeSummary.netProfit)} tone="success" icon={<BarChart3 size={18} />} />
      </section>

      <section className="dashboard-layout statistics-layout">
        <div className="table-panel statistics-table">
          <div className="section-title">
            <div><span>Сотрудники</span><h3>Активность за период</h3></div>
            <span className="pill info">{rows.length} сотрудников</span>
          </div>
          <div className="table-head">
            <span>Сотрудник</span>
            <span>Роль</span>
            <span>Действий</span>
            <span>Добавил</span>
            <span>Обновил</span>
            <span>Товаров</span>
            <span>Последнее</span>
          </div>
          {sortedRows.map((row) => (
            <div className="table-row" key={row.username}>
              <span data-label="Сотрудник"><strong>{row.username}</strong></span>
              <span data-label="Роль">{row.role || "-"}</span>
              <span data-label="Действий">{row.actionsTotal}</span>
              <span data-label="Добавил">{row.linksAdded}</span>
              <span data-label="Обновил">{row.linksUpdated}</span>
              <span data-label="Товаров">{row.affectedProducts}</span>
              <span data-label="Последнее">{row.lastActionAt ? compactDate(row.lastActionAt) : "-"}</span>
            </div>
          ))}
          {users.isLoading ? <div className="soft-empty"><RefreshCw className="spin" size={16} /> Загружаю статистику...</div> : null}
          {!users.isLoading && !sortedRows.length ? <div className="soft-empty">Нет действий за выбранный период.</div> : null}
        </div>

        <aside className="dashboard-side">
          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Склад</span><h3>Текущие показатели</h3></div>
            </div>
            <div className="dashboard-kpis">
              <span>Карточки <b>{warehouse.data?.groupTotal || warehouse.data?.total || 0}</b></span>
              <span>SKU <b>{warehouse.data?.rowTotal || warehouse.data?.totalAll || 0}</b></span>
              <span>Готовы <b>{warehouse.data?.ready || 0}</b></span>
              <span>Без поставщика <b>{warehouse.data?.withoutSupplier || 0}</b></span>
            </div>
          </section>
          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Сборка</span><h3>Очередь заказов</h3></div>
            </div>
            <div className="dashboard-money">
              <strong>{picking.data?.total || 0}</strong>
              <span>строк в листе сборки</span>
            </div>
          </section>
          <section className="dashboard-summary-card">
            <div className="section-title compact-title">
              <div><span>Финансы</span><h3>{period}</h3></div>
            </div>
            <div className="dashboard-kpis">
              <span>Выручка <b>{money(financeSummary.orderIncome)}</b></span>
              <span>Заказов <b>{String(financeSummary.orders || 0)}</b></span>
              <span>Закупка <b>{money(financeSummary.purchaseCost)}</b></span>
              <span>Комиссии <b>{money(Number(financeSummary.fees || 0) + Number(financeSummary.tax || 0))}</b></span>
            </div>
          </section>
        </aside>
      </section>

      {error ? <div className="inline-error">{errorMessage(error)}</div> : null}
    </section>
  );
}
