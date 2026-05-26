import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, Plus, RefreshCw } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { FinanceExpenseCreateSchema, FinanceExpensesSchema, FinanceOrdersSchema, FinanceSummarySchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { errorMessage } from "../lib/common";

const money = (value: unknown) => {
  const n = Number(value || 0);
  return n ? `${Math.round(n).toLocaleString("ru-RU")} ₽` : "-";
};

const text = (value: unknown) => String(value ?? "").trim();

const dateText = (value: unknown) => {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleDateString("ru-RU") : raw;
};

function SummaryCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="system-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function FinancePage() {
  const queryClient = useQueryClient();
  const [period, setPeriod] = useState("30d");
  const [form, setForm] = useState({
    supplierName: "",
    partnerId: "",
    offerId: "",
    productName: "",
    quantity: 1,
    amount: "",
    note: "",
  });
  const summary = useQuery({
    queryKey: ["finance", "summary", period],
    queryFn: () => fetchJson(`/api/finance/summary?period=${encodeURIComponent(period)}`, FinanceSummarySchema),
  });
  const orders = useQuery({
    queryKey: ["finance", "orders", period],
    queryFn: () => fetchJson(`/api/finance/orders?period=${encodeURIComponent(period)}&limit=200`, FinanceOrdersSchema),
  });
  const expenses = useQuery({
    queryKey: ["finance", "expenses", period],
    queryFn: () => fetchJson(`/api/finance/expenses?period=${encodeURIComponent(period)}&limit=200`, FinanceExpensesSchema),
  });
  const createExpense = useMutation({
    mutationFn: () => fetchJson("/api/finance/expenses", FinanceExpenseCreateSchema, mutationBody({
      ...form,
      quantity: Number(form.quantity || 1),
      amount: Number(form.amount || 0),
      type: "manual_purchase",
    })),
    onSuccess: () => {
      setForm({ supplierName: "", partnerId: "", offerId: "", productName: "", quantity: 1, amount: "", note: "" });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });
  const s = summary.data?.summary || {};
  const refresh = () => {
    void summary.refetch();
    void orders.refetch();
    void expenses.refetch();
  };

  return (
    <section className="page-section finance-page">
      <PageHeader
        title="Финансы"
        subtitle="Заказы, закупки, ручные расходы и чистая прибыль по ДавидСкладу."
        action={(
          <div className="row-actions">
            <select value={period} onChange={(event) => setPeriod(event.target.value)}>
              <option value="7d">7 дней</option>
              <option value="30d">30 дней</option>
              <option value="90d">90 дней</option>
              <option value="all">Все</option>
            </select>
            <button className="secondary-action" type="button" onClick={refresh}>
              <RefreshCw size={16} /> Обновить
            </button>
          </div>
        )}
      />
      <div className="summary-grid">
        <SummaryCard label="Чистая прибыль" value={money(s.netProfit)} />
        <SummaryCard label="Прибыль по заказам" value={money(s.orderProfit)} />
        <SummaryCard label="Ручные закупки" value={money(s.manualExpenses)} />
        <SummaryCard label="Заказов" value={Number(s.orders || 0)} />
        <SummaryCard label="Закупочная цена" value={money(s.purchaseCost)} />
        <SummaryCard label="Комиссии/налоги" value={money(Number(s.fees || 0) + Number(s.tax || 0))} />
      </div>

      <section className="settings-panel settings-panel-wide">
        <div className="section-title"><div><span>Ручная закупка</span><h3>Добавить парфюм не из marketplace-заказа</h3></div></div>
        <div className="settings-form-row">
          <input placeholder="Поставщик" value={form.supplierName} onChange={(event) => setForm({ ...form, supplierName: event.target.value })} />
          <input placeholder="SKU / offerId" value={form.offerId} onChange={(event) => setForm({ ...form, offerId: event.target.value })} />
          <input placeholder="Товар" value={form.productName} onChange={(event) => setForm({ ...form, productName: event.target.value })} />
          <input type="number" min="1" placeholder="Кол-во" value={form.quantity} onChange={(event) => setForm({ ...form, quantity: Number(event.target.value || 1) })} />
          <input type="number" min="0" step="0.01" placeholder="Сумма, ₽" value={form.amount} onChange={(event) => setForm({ ...form, amount: event.target.value })} />
          <button className="primary-action" type="button" disabled={createExpense.isPending || !(Number(form.amount) > 0)} onClick={() => createExpense.mutate()}>
            {createExpense.isPending ? <Loader2 className="spin" size={16} /> : <Plus size={16} />} Добавить
          </button>
        </div>
        <input placeholder="Комментарий" value={form.note} onChange={(event) => setForm({ ...form, note: event.target.value })} />
        {createExpense.error ? <div className="inline-error">{errorMessage(createExpense.error)}</div> : null}
        {createExpense.isSuccess ? <div className="success-strip">Закупка добавлена в финансы и историю поставщика.</div> : null}
      </section>

      <div className="table-panel price-table">
        <div className="table-head"><span>Дата</span><span>Заказ/SKU</span><span>Товар</span><span>Поставщик</span><span>Закупка</span><span>Прибыль</span></div>
        {(orders.data?.orders || []).map((order) => (
          <div className="table-row" key={order.id}>
            <span data-label="Дата">{dateText(order.createdAt)}</span>
            <span data-label="Заказ/SKU">{order.orderId || order.postingNumber || order.offerId}</span>
            <span data-label="Товар">{order.productName || "-"}</span>
            <span data-label="Поставщик">{order.supplierName || "-"}</span>
            <span data-label="Закупка">{money(order.purchaseCost)}</span>
            <span data-label="Прибыль">{money(order.profitAmount)}</span>
          </div>
        ))}
        {!orders.data?.orders?.length && !orders.isLoading ? <div className="empty-state">Заказов за выбранный период пока нет.</div> : null}
      </div>

      <div className="table-panel price-table">
        <div className="table-head"><span>Дата</span><span>SKU</span><span>Товар</span><span>Поставщик</span><span>Кол-во</span><span>Сумма</span></div>
        {(expenses.data?.expenses || []).map((expense) => (
          <div className="table-row" key={expense.id}>
            <span data-label="Дата">{dateText(expense.spentAt)}</span>
            <span data-label="SKU">{expense.offerId || "-"}</span>
            <span data-label="Товар">{expense.productName || expense.note || "-"}</span>
            <span data-label="Поставщик">{expense.supplierName || "-"}</span>
            <span data-label="Кол-во">{expense.quantity}</span>
            <span data-label="Сумма">{money(expense.amount)}</span>
          </div>
        ))}
        {!expenses.data?.expenses?.length && !expenses.isLoading ? <div className="empty-state">Ручных закупок за период нет.</div> : null}
      </div>
    </section>
  );
}
