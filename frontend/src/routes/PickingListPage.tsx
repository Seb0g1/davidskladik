import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, CalendarDays, Check, CheckCircle2, ClipboardList, Clock, Copy, Loader2, RefreshCw, Repeat2, RotateCcw, Trash2, Truck, Wallet, X, Zap } from "lucide-react";
import { useMemo, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { ListSkeleton } from "../components/Skeleton";
import { Stat } from "../components/Stat";
import { SupplierAltPicker } from "../components/SupplierAltPicker";
import { PickerCashSchema, SupplierCartCancelSchema, SupplierLedgerPaymentSchema, SupplierPickingInvoiceSchema, SupplierPickingListSchema, SupplierPickingRowSchema, SupplierPickingUpdateSchema, SupplierReplaceResponseSchema } from "../types";
import { compactDate, copyPlainText, errorMessage, money, numberValue } from "../lib/common";

type PickingRow = z.infer<typeof SupplierPickingRowSchema>;

const statusLabel = (status: string) => {
  const labels: Record<string, string> = {
    open: "к сборке",
    picked: "собрано",
    missing: "не было",
    reordered: "перезаказано",
    returned: "возврат из ПВЗ",
    return_used: "возврат использован",
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
  const [view, setView] = useState<"list" | "sheets">("list");
  const [status, setStatus] = useState("open");
  const [supplier, setSupplier] = useState("");
  const [q, setQ] = useState("");
  const [period, setPeriod] = useState("30d");
  const [copied, setCopied] = useState(false);
  const [paymentDrafts, setPaymentDrafts] = useState<Record<string, string>>({});
  const [paymentNotes, setPaymentNotes] = useState<Record<string, string>>({});
  const [expandedSheets, setExpandedSheets] = useState<Set<string>>(new Set());
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
  const sheetsQuery = useQuery({
    queryKey: ["supplier-picking-list", "sheets"],
    queryFn: () => fetchJson("/api/supplier-picking-list?status=picked&limit=1000", SupplierPickingListSchema),
    enabled: view === "sheets",
    refetchInterval: 30_000,
  });
  const sheets = useMemo(() => {
    const pickedRows = sheetsQuery.data?.rows || [];
    const groups = new Map<string, PickingRow[]>();
    for (const row of pickedRows) {
      const dateKey = row.pickedAt ? row.pickedAt.slice(0, 10) : "";
      if (!dateKey) continue;
      if (!groups.has(dateKey)) groups.set(dateKey, []);
      groups.get(dateKey)!.push(row);
    }
    return Array.from(groups.entries()).sort((a, b) => b[0].localeCompare(a[0]));
  }, [sheetsQuery.data]);
  const [cashDraft, setCashDraft] = useState("");
  const [cashNoteDraft, setCashNoteDraft] = useState("");
  const [replaceKey, setReplaceKey] = useState<string | null>(null);
  const [missingRow, setMissingRow] = useState<PickingRow | null>(null);
  const todayStr = new Date().toISOString().slice(0, 10);
  const cashQuery = useQuery({
    queryKey: ["picker-cash", todayStr],
    queryFn: () => fetchJson(`/api/picker-cash?date=${todayStr}`, PickerCashSchema),
    refetchInterval: 30_000,
  });
  const addCashMutation = useMutation({
    mutationFn: ({ amount, note }: { amount: number; note: string }) =>
      fetchJson("/api/picker-cash", PickerCashSchema, mutationBody({ amount, note, date: todayStr })),
    onSuccess: () => {
      setCashDraft("");
      setCashNoteDraft("");
      void queryClient.invalidateQueries({ queryKey: ["picker-cash"] });
    },
  });
  const deleteCashMutation = useMutation({
    mutationFn: (id: string) =>
      fetchJson(`/api/picker-cash/${encodeURIComponent(id)}?date=${todayStr}`, PickerCashSchema, { method: "DELETE" }),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["picker-cash"] }),
  });
  const updateMutation = useMutation({
    mutationFn: ({ key, nextStatus, snoozeDays, permanent }: { key: string; nextStatus: string; snoozeDays?: number; permanent?: boolean }) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({
        status: nextStatus,
        ...(snoozeDays ? { snoozeDays } : {}),
        ...(permanent ? { permanent: true } : {}),
      })),
    onSuccess: (_data, variables) => {
      // «Не было» → сразу предлагаем работнику заменить поставщика для этой строки.
      if (variables.nextStatus === "missing") {
        setMissingRow(null);
        setReplaceKey(variables.key);
      }
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });
  const replaceMutation = useMutation({
    mutationFn: ({ key, partnerId, rowId }: { key: string; partnerId: string; rowId: string }) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}/replace-supplier`, SupplierReplaceResponseSchema, mutationBody({ partnerId, rowId })),
    onSuccess: () => {
      setReplaceKey(null);
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
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

      <div className="page-tabs">
        <button className={`page-tab-btn${view === "list" ? " active" : ""}`} type="button" onClick={() => setView("list")}>
          <ClipboardList size={15} /> Список
        </button>
        <button className={`page-tab-btn${view === "sheets" ? " active" : ""}`} type="button" onClick={() => setView("sheets")}>
          <CalendarDays size={15} /> Листы сборки
        </button>
      </div>

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
              { value: "returned", label: "Возврат из ПВЗ" },
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
      {replaceMutation.error ? <div className="inline-error">Замена поставщика: {errorMessage(replaceMutation.error)}</div> : null}
      {replaceMutation.data ? <div className="success-strip">Перезаказано у «{replaceMutation.data.supplierName || "нового поставщика"}»: заявка в PriceMaster создана (док {replaceMutation.data.docIds?.join(", ") || "-"}), строка сборки появилась у нового поставщика.</div> : null}
      {paymentMutation.error ? <div className="inline-error">{errorMessage(paymentMutation.error)}</div> : null}

      {view === "sheets" ? (
        <section className="table-panel assembly-sheets">
          {sheetsQuery.isLoading ? <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю листы сборки...</div> : null}
          {sheets.map(([dateKey, sheetRows]) => {
            const expanded = expandedSheets.has(dateKey);
            const totalCost = sheetRows.reduce((sum, row) => sum + (row.price || 0) * (row.quantity || 1), 0);
            const supplierSet = new Set(sheetRows.map((row) => row.supplierName).filter(Boolean));
            const dateLabel = new Date(dateKey + "T12:00:00").toLocaleDateString("ru-RU", {
              weekday: "long", day: "numeric", month: "long", year: "numeric",
            });
            return (
              <article className="assembly-sheet" key={dateKey}>
                <button
                  className="assembly-sheet-header"
                  type="button"
                  onClick={() => setExpandedSheets((prev) => {
                    const next = new Set(prev);
                    if (next.has(dateKey)) next.delete(dateKey);
                    else next.add(dateKey);
                    return next;
                  })}
                >
                  <div>
                    <span className="assembly-sheet-date">{dateLabel}</span>
                    <span className="assembly-sheet-meta">
                      {sheetRows.length} позиций · {supplierSet.size} поставщ. · {Math.round(totalCost).toLocaleString("ru-RU")} ₽
                    </span>
                  </div>
                  <span className="sheet-toggle">{expanded ? "▲" : "▼"}</span>
                </button>
                {expanded ? (
                  <div className="assembly-sheet-body">
                    {sheetRows.map((row) => (
                      <div className="picking-row status-picked" key={row.key}>
                        <div className="picking-main">
                          <strong>{row.productName || row.offerId}</strong>
                          <span>{row.marketplace.toUpperCase()} · {row.orderId || row.postingNumber || "-"} · {row.offerId}</span>
                        </div>
                        <div className="meta-grid">
                          <span>Поставщик: {row.supplierName || "-"}</span>
                          <span>Кол-во: {row.quantity}</span>
                          <span>Собрал: {row.pickedBy || "-"} · {compactDate(row.pickedAt)}</span>
                          <span>Цена PM: {row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
                          <span className="picking-meta-secondary">Doc/Row: {row.requestDocId || "-"}/{row.requestRowId || "-"}</span>
                        </div>
                        {isAdmin ? (
                          <div className="picking-actions">
                            <button
                              className="secondary-action"
                              type="button"
                              disabled={updateMutation.isPending}
                              onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}
                            >
                              <RotateCcw size={14} /> Отменить сборку
                            </button>
                          </div>
                        ) : null}
                      </div>
                    ))}
                  </div>
                ) : null}
              </article>
            );
          })}
          {!sheets.length && !sheetsQuery.isLoading ? <div className="soft-empty">Нет завершённых листов сборки.</div> : null}
        </section>
      ) : null}

      {view === "list" ? (
      <>
      <section className="picker-cash-card">
        <div className="picker-cash-head">
          <Wallet size={16} />
          <span>Касса сборщика — {todayStr}</span>
        </div>
        <div className="summary-grid compact-summary">
          <DiagnosticValue label="Выдано" value={money(cashQuery.data?.totalIssued ?? 0)} />
          <DiagnosticValue label="Потрачено" value={money(cashQuery.data?.spent ?? 0)} tone={cashQuery.data && cashQuery.data.spent > 0 ? "warn" : ""} />
          <DiagnosticValue
            label="Остаток"
            value={money(Math.abs(cashQuery.data?.remaining ?? 0))}
            tone={(cashQuery.data?.remaining ?? 0) >= 0 ? "success" : "danger"}
          />
        </div>
        {isAdmin ? (
          <div className="settings-form-row picker-cash-input-row">
            <input
              type="number"
              min="0"
              step="1"
              placeholder="Выдать сборщику, ₽"
              value={cashDraft}
              onChange={(event) => setCashDraft(event.target.value)}
            />
            <input
              placeholder="Комментарий"
              value={cashNoteDraft}
              onChange={(event) => setCashNoteDraft(event.target.value)}
            />
            <button
              className="primary-action"
              type="button"
              disabled={addCashMutation.isPending || !(Number(cashDraft) > 0)}
              onClick={() => addCashMutation.mutate({ amount: Number(cashDraft), note: cashNoteDraft })}
            >
              {addCashMutation.isPending ? <Loader2 className="spin" size={16} /> : <Check size={16} />} Выдал
            </button>
          </div>
        ) : null}
        {(cashQuery.data?.advances || []).length > 0 ? (
          <div className="picker-cash-advances">
            {(cashQuery.data?.advances || []).map((adv) => (
              <div className="picker-cash-advance-row" key={adv.id}>
                <span className="picker-cash-amount">{money(adv.amount)}</span>
                {adv.note ? <span className="muted-note">{adv.note}</span> : null}
                <span className="muted-note">{compactDate(adv.createdAt ?? null)}</span>
                {isAdmin ? (
                  <button
                    className="icon-action danger-action"
                    type="button"
                    title="Удалить выдачу"
                    disabled={deleteCashMutation.isPending}
                    onClick={() => deleteCashMutation.mutate(adv.id)}
                  >
                    <Trash2 size={13} />
                  </button>
                ) : null}
              </div>
            ))}
          </div>
        ) : null}
        {addCashMutation.error ? <div className="inline-error">{errorMessage(addCashMutation.error)}</div> : null}
      </section>
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
                    <span>
                      {row.marketplace.toUpperCase()} · {row.orderId || row.postingNumber || "-"} · {row.offerId}
                      {row.isExpress ? <span className="express-badge"><Zap size={12} /> Экспресс</span> : null}
                    </span>
                  </div>
                  <div className="meta-grid">
                    <span>Кол-во: {row.quantity}</span>
                    <span>Цена PM: {row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
                    <span className="picking-meta-secondary">Доверие: {row.trustFactor}/100</span>
                    <span className="picking-meta-secondary">{row.orderCutoffTime ? `Заказы до ${row.orderCutoffTime}` : "Без дедлайна"}</span>
                    {row.reseller ? <span className="picking-meta-secondary">Перекупщик</span> : null}
                    <span className="picking-meta-secondary">Doc/Row: {row.requestDocId || "-"}/{row.requestRowId || "-"}</span>
                    <span>Статус: {statusLabel(row.status)}</span>
                    {row.wbSupplyId ? (
                      <span>
                        WB поставка: <strong>{row.wbSupplyId}</strong>
                        {" · "}
                        <a href={`/api/wb/supplies/${row.wbSupplyId}/barcode?type=png`} target="_blank" rel="noreferrer" className="link-plain">Стикер</a>
                      </span>
                    ) : null}
                  </div>
                  {row.status === "missing" && !row.replacementKey ? (
                    <small className="danger-text">
                      {row.missingPermanent || !row.nextRetryAt
                        ? "Поставщик отправлен в инактив насовсем (снять можно в карточке товара или вернув строку в сборку)."
                        : `Поставщик в инактиве для этого SKU до ${compactDate(row.nextRetryAt)}.`}
                      {" "}Замените поставщика кнопкой ниже или автокорзина попробует другого сама.
                    </small>
                  ) : null}
                  {row.status === "reordered" && row.replacementKey ? <small>Перезаказано у другого поставщика (строка {row.replacementKey}).</small> : null}
                  <div className="picking-actions">
                    <button className="primary-action success-action" type="button" disabled={updateMutation.isPending || row.status !== "open"} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "picked" })}>
                      <Check size={16} /> Собрал
                    </button>
                    <button className="secondary-action danger-action" type="button" disabled={updateMutation.isPending || row.status !== "open"} onClick={() => setMissingRow(row)}>
                      <X size={16} /> Не было
                    </button>
                    {["open", "missing"].includes(row.status) && !row.replacementKey ? (
                      <button className="secondary-action" type="button" disabled={replaceMutation.isPending} onClick={() => setReplaceKey(replaceKey === row.key ? null : row.key)}>
                        <Repeat2 size={16} /> Заменить поставщика
                      </button>
                    ) : null}
                    {isAdmin && row.status === "picked" ? (
                      <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "returned" })}>
                        <RotateCcw size={16} /> Вернули из ПВЗ
                      </button>
                    ) : null}
                    {isAdmin && row.status !== "open" && row.status !== "picked" ? <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}><RotateCcw size={16} /> Вернуть</button> : null}
                    {isAdmin && row.status === "picked" ? <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}><RotateCcw size={16} /> Вернуть к сборке</button> : null}
                    {isAdmin && row.requestRowId ? <button className="secondary-action danger-action" type="button" disabled={cancelCartMutation.isPending} onClick={() => cancelCartMutation.mutate(row.key)}><Trash2 size={16} /> Отменить автокорзину</button> : null}
                  </div>
                  {replaceKey === row.key ? (
                    <SupplierAltPicker
                      offerId={row.offerId}
                      currentPartnerId={row.partnerId}
                      busy={replaceMutation.isPending}
                      actionLabel="Заказать у него"
                      onPick={(option) => replaceMutation.mutate({ key: row.key, partnerId: option.partnerId, rowId: option.rowId })}
                      onClose={() => setReplaceKey(null)}
                    />
                  ) : null}
                </div>
              ))}
            </div>
          </article>
          );
        })}
        {listQuery.isLoading && !grouped.length ? <ListSkeleton rows={8} /> : null}
        {!grouped.length && !listQuery.isLoading ? <div className="empty-state">Строк для выбранного фильтра нет.</div> : null}
      </div>

      <section className="table-panel picking-invoices" key="invoices">
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
      </>
      ) : null}

      {missingRow ? (
        <div className="page-access-overlay" onClick={() => setMissingRow(null)}>
          <div className="picking-missing-modal" onClick={(event) => event.stopPropagation()}>
            <div className="page-access-head">
              <div>
                <span className="muted-note">Товара не было</span>
                <h3>{missingRow.productName || missingRow.offerId}</h3>
              </div>
              <button className="secondary-action" type="button" onClick={() => setMissingRow(null)}><X size={16} /> Отмена</button>
            </div>
            <p className="muted-note">
              Поставщик «{missingRow.supplierName || "-"}» уйдёт в инактив для этого товара: автокорзина и цены переключатся на другого поставщика,
              строка сборки получит статус «не было». На какой срок отправить поставщика в инактив?
            </p>
            <div className="picking-missing-options">
              {[
                { label: "Завтра товар появится", hint: "инактив 1 день", snoozeDays: 1 },
                { label: "2 дня инактива", hint: "поставщик вернётся через 2 дня", snoozeDays: 2 },
                { label: "3 дня инактива", hint: "поставщик вернётся через 3 дня", snoozeDays: 3 },
                { label: "5 дней инактива", hint: "поставщик вернётся через 5 дней", snoozeDays: 5 },
              ].map((option) => (
                <button
                  key={option.snoozeDays}
                  className="secondary-action picking-missing-option"
                  type="button"
                  disabled={updateMutation.isPending}
                  onClick={() => updateMutation.mutate({ key: missingRow.key, nextStatus: "missing", snoozeDays: option.snoozeDays })}
                >
                  <Clock size={15} />
                  <span><strong>{option.label}</strong><small>{option.hint}</small></span>
                </button>
              ))}
              <button
                className="secondary-action danger-action picking-missing-option"
                type="button"
                disabled={updateMutation.isPending}
                onClick={() => updateMutation.mutate({ key: missingRow.key, nextStatus: "missing", permanent: true })}
              >
                {updateMutation.isPending ? <Loader2 className="spin" size={15} /> : <AlertTriangle size={15} />}
                <span><strong>Товар уходит в инактив</strong><small>насовсем — снять можно в карточке товара</small></span>
              </button>
            </div>
            {updateMutation.error ? <div className="inline-error">{errorMessage(updateMutation.error)}</div> : null}
          </div>
        </div>
      ) : null}
    </section>
  );
}
