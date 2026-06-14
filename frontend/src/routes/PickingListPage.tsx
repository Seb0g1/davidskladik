import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, CheckCircle2, ClipboardList, Copy, Loader2, RefreshCw, RotateCcw, Trash2, Truck, X } from "lucide-react";
import { useMemo, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { SupplierCartCancelSchema, SupplierLedgerPaymentSchema, SupplierPickingInvoiceSchema, SupplierPickingListSchema, SupplierPickingRowSchema, SupplierPickingUpdateSchema } from "../types";
import { compactDate, copyPlainText, errorMessage, money, numberValue } from "../lib/common";

type PickingRow = z.infer<typeof SupplierPickingRowSchema>;

const statusLabel = (status: string) => {
  const labels: Record<string, string> = {
    open: "к сборке",
    picked: "собрано",
    missing: "не было",
    reordered: "перезаказано",
    all: "все",
  };
  return labels[status] || status || "-";
};

const rowSearchText = (row: PickingRow) => [
  row.productName,
  row.offerId,
  row.orderId,
  row.postingNumber,
  row.supplierName,
].join(" ").toLowerCase();

const moneySigned = (value: unknown) => {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n === 0) return "0 ₽";
  const sign = n > 0 ? "+" : "-";
  return `${sign}${Math.round(Math.abs(n)).toLocaleString("ru-RU")} ₽`;
};

const currentGroupTotal = (rows: PickingRow[]) => rows.reduce((sum, row) => {
  const price = Number(row.price || 0);
  const quantity = Number(row.quantity || 1) || 1;
  return sum + price * quantity;
}, 0);

export function PickingListPage() {
  const [status, setStatus] = useState("open");
  const [supplier, setSupplier] = useState("");
  const [q, setQ] = useState("");
  const [period, setPeriod] = useState("30d");
  const [copied, setCopied] = useState(false);
  const [paymentDrafts, setPaymentDrafts] = useState<Record<string, string>>({});
  const [paymentNotes, setPaymentNotes] = useState<Record<string, string>>({});
  const queryClient = useQueryClient();
  const sessionQuery = useQuery({
    queryKey: ["session"],
    queryFn: () => fetchJson("/api/session", z.object({ role: z.coerce.string().optional().nullable() }).passthrough()),
    staleTime: 60_000,
  });
  const isAdmin = sessionQuery.data?.role === "admin";
  const listQuery = useQuery({
    queryKey: ["supplier-picking-list", status, supplier, q],
    queryFn: () => {
      const params = new URLSearchParams({ status, limit: "500" });
      if (supplier) params.set("supplier", supplier);
      if (q) params.set("q", q);
      return fetchJson(`/api/supplier-picking-list?${params.toString()}`, SupplierPickingListSchema);
    },
    refetchInterval: 15000,
  });
  const invoiceQuery = useQuery({
    queryKey: ["supplier-picking-list", "invoices", period],
    queryFn: () => fetchJson(`/api/supplier-picking-list/invoices?period=${encodeURIComponent(period)}`, SupplierPickingInvoiceSchema),
  });
  const updateMutation = useMutation({
    mutationFn: ({ key, nextStatus }: { key: string; nextStatus: string }) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({ status: nextStatus })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });
  const cancelCartMutation = useMutation({
    mutationFn: (key: string) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}/cancel-cart`, SupplierCartCancelSchema, mutationBody({})),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });
  const paymentMutation = useMutation({
    mutationFn: ({ supplierName, amount, note }: { supplierName: string; amount: number; note: string }) =>
      fetchJson("/api/supplier-ledger/payments", SupplierLedgerPaymentSchema, mutationBody({ supplierName, amount, note })),
    onSuccess: (_data, variables) => {
      setPaymentDrafts((current) => ({ ...current, [variables.supplierName]: "" }));
      setPaymentNotes((current) => ({ ...current, [variables.supplierName]: "" }));
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });
  const rows = listQuery.data?.rows || [];
  const filteredRows = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return needle ? rows.filter((row) => rowSearchText(row).includes(needle)) : rows;
  }, [q, rows]);
  const grouped = useMemo(() => {
    const groups = new Map<string, PickingRow[]>();
    for (const row of filteredRows) {
      const key = row.supplierName || "Без поставщика";
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)?.push(row);
    }
    return Array.from(groups.entries()).sort((left, right) => left[0].localeCompare(right[0], "ru", { sensitivity: "base" }));
  }, [filteredRows]);
  const summary = listQuery.data?.summary || {};
  const suppliers = listQuery.data?.suppliers || [];
  const supplierLedger = listQuery.data?.supplierLedger || {};
  const invoiceRows = invoiceQuery.data?.rows || [];
  const copyInvoice = async () => {
    const text = invoiceRows.map((row) => [
      row.supplierName,
      row.offerId,
      row.productName,
      `x${row.quantity}`,
      row.price ? `${row.price} ${row.priceCurrency}` : "",
    ].filter(Boolean).join(" | ")).join("\n");
    setCopied(await copyPlainText(text));
    window.setTimeout(() => setCopied(false), 1600);
  };

  return (
    <section className="page-section picking-page">
      <PageHeader
        title="Сборка"
        subtitle="Лист закупки для сотрудников: собрать товар у поставщика или отметить, что товара не было."
        action={<button className="secondary-action" type="button" onClick={() => listQuery.refetch()} disabled={listQuery.isFetching}>{listQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить</button>}
      />

      <section className="dashboard-metrics">
        <Stat label="К сборке" value={numberValue(summary.open)} tone={numberValue(summary.open) ? "warn" : "success"} icon={<ClipboardList size={18} />} />
        <Stat label="Собрано" value={numberValue(summary.picked)} tone="success" icon={<CheckCircle2 size={18} />} />
        <Stat label="Не было" value={numberValue(summary.missing)} tone={numberValue(summary.missing) ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
        <Stat label="Поставщиков" value={numberValue(summary.suppliers)} tone="accent" icon={<Truck size={18} />} />
      </section>

      <div className="control-grid compact-controls">
        <label>Статус
          <SelectField
            ariaLabel="Статус сборки"
            value={status}
            onChange={setStatus}
            options={[
              { value: "open", label: "К сборке" },
              { value: "picked", label: "Собрано" },
              { value: "missing", label: "Не было" },
              { value: "reordered", label: "Перезаказано" },
              { value: "all", label: "Все" },
            ]}
          />
        </label>
        <label>Поставщик
          <SelectField
            ariaLabel="Поставщик"
            value={supplier}
            onChange={setSupplier}
            options={[
              { value: "", label: "Все поставщики" },
              ...suppliers.map((item) => ({ value: String(item), label: String(item) })),
            ]}
          />
        </label>
        <label>Поиск
          <input value={q} onChange={(event) => setQ(event.target.value)} placeholder="SKU, товар, заказ" />
        </label>
      </div>

      {listQuery.error ? <div className="inline-error">{errorMessage(listQuery.error)}</div> : null}
      {updateMutation.error ? <div className="inline-error">{errorMessage(updateMutation.error)}</div> : null}
      {cancelCartMutation.error ? <div className="inline-error">{errorMessage(cancelCartMutation.error)}</div> : null}
      {paymentMutation.error ? <div className="inline-error">{errorMessage(paymentMutation.error)}</div> : null}

      <div className="picking-groups">
        {grouped.map(([supplierName, supplierRows]) => {
          const ledger = supplierLedger[supplierName] || {};
          const balance = Number(ledger.balance || 0);
          const draftAmount = paymentDrafts[supplierName] || "";
          const draftNote = paymentNotes[supplierName] || "";
          const total = currentGroupTotal(supplierRows);
          return (
          <article className="picking-supplier-card" key={supplierName}>
            <div className="picking-supplier-toolbar">
              <div className="picking-supplier-head">
                <div>
                  <span>Поставщик</span>
                  <h3>{supplierName}</h3>
                </div>
                <strong>{supplierRows.length}</strong>
              </div>
              <div className="summary-grid compact-summary supplier-ledger-strip">
                <DiagnosticValue label={balance < 0 ? "Долг поставщику" : "Аванс / баланс"} value={moneySigned(balance)} tone={balance < 0 ? "danger" : balance > 0 ? "success" : ""} />
                <DiagnosticValue label="Собрано в долг" value={moneySigned(-Number(ledger.debtTotal || 0))} />
                <DiagnosticValue label="Оплачено" value={moneySigned(Number(ledger.paidTotal || 0))} tone={Number(ledger.paidTotal || 0) ? "success" : ""} />
                <DiagnosticValue label="Текущая сборка" value={moneySigned(total)} />
              </div>
            </div>
            <div className="settings-form-row supplier-payment-row">
              <input
                type="number"
                min="0"
                step="0.01"
                placeholder="Сумма оплаты, ₽"
                value={draftAmount}
                onChange={(event) => setPaymentDrafts((current) => ({ ...current, [supplierName]: event.target.value }))}
              />
              <input
                placeholder="Комментарий"
                value={draftNote}
                onChange={(event) => setPaymentNotes((current) => ({ ...current, [supplierName]: event.target.value }))}
              />
              <button
                className="primary-action"
                type="button"
                disabled={paymentMutation.isPending || !(Number(draftAmount) > 0)}
                onClick={() => paymentMutation.mutate({ supplierName, amount: Number(draftAmount || 0), note: draftNote })}
              >
                {paymentMutation.isPending ? <Loader2 className="spin" size={16} /> : <Check size={16} />} Заплатил
              </button>
            </div>
            <div className="picking-row-list">
              {supplierRows.map((row) => (
                <div className={`picking-row status-${row.status}`} key={row.key}>
                  <div className="picking-main">
                    <strong>{row.productName || row.offerId}</strong>
                    <span>{row.marketplace.toUpperCase()} · {row.orderId || row.postingNumber || "-"} · {row.offerId}</span>
                  </div>
                  <div className="meta-grid">
                    <span>Кол-во: {row.quantity}</span>
                    <span>Цена PM: {row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
                    <span className="picking-meta-secondary">Доверие: {row.trustFactor}/100</span>
                    <span className="picking-meta-secondary">{row.orderCutoffTime ? `Заказы до ${row.orderCutoffTime}` : "Без дедлайна"}</span>
                    {row.reseller ? <span className="picking-meta-secondary">Перекупщик</span> : null}
                    <span className="picking-meta-secondary">Doc/Row: {row.requestDocId || "-"}/{row.requestRowId || "-"}</span>
                    <span>Статус: {statusLabel(row.status)}</span>
                  </div>
                  {row.status === "missing" ? <small className="danger-text">Поставщик пропущен для этого SKU до {compactDate(row.nextRetryAt)}. Автокорзина попробует другого поставщика.</small> : null}
                  <div className="picking-actions">
                    <button className="primary-action success-action" type="button" disabled={updateMutation.isPending || row.status !== "open"} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "picked" })}>
                      <Check size={16} /> Собрал
                    </button>
                    <button className="secondary-action danger-action" type="button" disabled={updateMutation.isPending || row.status !== "open"} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "missing" })}>
                      <X size={16} /> Не было
                    </button>
                    {isAdmin && row.status !== "open" ? <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}><RotateCcw size={16} /> Вернуть</button> : null}
                    {isAdmin && row.requestRowId ? <button className="secondary-action danger-action" type="button" disabled={cancelCartMutation.isPending} onClick={() => cancelCartMutation.mutate(row.key)}><Trash2 size={16} /> Отменить автокорзину</button> : null}
                  </div>
                </div>
              ))}
            </div>
          </article>
          );
        })}
        {!grouped.length && !listQuery.isLoading ? <div className="empty-state">Строк для выбранного фильтра нет.</div> : null}
      </div>

      <section className="table-panel picking-invoices">
        <div className="section-title">
          <div>
            <span>Внутренняя накладная</span>
            <h3>Собранные позиции</h3>
          </div>
          <div className="supplier-cart-actions">
            <SelectField
              ariaLabel="Период"
              value={period}
              onChange={setPeriod}
              options={[
                { value: "7d", label: "7 дней" },
                { value: "30d", label: "30 дней" },
                { value: "all", label: "Все" },
              ]}
            />
            <button className="secondary-action" type="button" onClick={copyInvoice} disabled={!invoiceRows.length}><Copy size={16} /> {copied ? "Скопировано" : "Скопировать"}</button>
          </div>
        </div>
        <div className="picking-invoice-list">
          {invoiceRows.slice(0, 80).map((row) => (
            <div className="picking-invoice-row" key={`${row.key}-${row.pickedAt || ""}`}>
              <span>{compactDate(row.pickedAt)}</span>
              <strong>{row.supplierName}</strong>
              <span>{row.offerId}</span>
              <span>{row.productName}</span>
              <span>x{row.quantity}</span>
              <span>{row.price ? `${row.price} ${row.priceCurrency}` : money(0)}</span>
            </div>
          ))}
          {!invoiceRows.length ? <div className="soft-empty">Собранных строк за период нет.</div> : null}
        </div>
      </section>
    </section>
  );
}
