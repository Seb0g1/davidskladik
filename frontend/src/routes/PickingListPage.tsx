import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, CalendarDays, Check, CheckCircle2, ChevronDown, ClipboardList, Clock, Copy, Database, Loader2, RefreshCw, Repeat2, RotateCcw, Trash2, Truck, Users, Wallet, X, Zap } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { ListSkeleton } from "../components/Skeleton";
import { Stat } from "../components/Stat";
import { SupplierAltPicker } from "../components/SupplierAltPicker";
import { DailyCartTotalSchema, PickerBalanceSchema, PickerBalancesSchema, PickerMyDaySchema, SupplierCartCancelSchema, SupplierLedgerPaymentSchema, SupplierPickingInvoiceSchema, SupplierPickingListSchema, SupplierPickingRowSchema, SupplierPickingUpdateSchema, SupplierReplaceResponseSchema } from "../types";
import { PmSearchPanel } from "./SupplierCartPage";
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

const currencySymbol = (currency: string) => (String(currency || "USD").toUpperCase() === "RUB" ? "₽" : "$");

const moneyAmount = (value: unknown, currency = "USD") => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "-";
  return `${Math.round(n).toLocaleString("ru-RU")} ${currencySymbol(currency)}`;
};

const moneySigned = (value: unknown, currency = "USD") => {
  const n = Number(value || 0);
  const sym = currencySymbol(currency);
  if (!Number.isFinite(n) || n === 0) return `0 ${sym}`;
  const sign = n > 0 ? "+" : "-";
  return `${sign}${Math.round(Math.abs(n)).toLocaleString("ru-RU")} ${sym}`;
};

const currentGroupTotal = (rows: PickingRow[]) => rows.reduce((sum, row) => {
  const price = Number(row.price || 0);
  const quantity = Number(row.quantity || 1) || 1;
  return sum + price * quantity;
}, 0);

const currentGroupTotalRub = (rows: PickingRow[], rate: number) => rows.reduce((sum, row) => {
  const price = Number(row.price || 0);
  const quantity = Number(row.quantity || 1) || 1;
  const priceRub = String(row.priceCurrency || "").toUpperCase() === "RUB" ? price : price * rate;
  return sum + priceRub * quantity;
}, 0);

export function PickingListPage() {
  const [view, setView] = useState<"list" | "sheets">("list");
  const [status, setStatus] = useState("open");
  const [supplier, setSupplier] = useState("");
  const [q, setQ] = useState("");
  const searchRef = useRef<HTMLInputElement>(null);
  const [period, setPeriod] = useState("1d");
  const [invoicesOpen, setInvoicesOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const [paymentDrafts, setPaymentDrafts] = useState<Record<string, string>>({});
  const [paymentNotes, setPaymentNotes] = useState<Record<string, string>>({});
  const [expandedSheets, setExpandedSheets] = useState<Set<string>>(new Set());
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const [collapsedProductGroups, setCollapsedProductGroups] = useState<Set<string>>(new Set());
  const [balancePanelOpen, setBalancePanelOpen] = useState(false);
  const [issuePickerDraft, setIssuePickerDraft] = useState("");
  const [issueAmountDraft, setIssueAmountDraft] = useState("");
  const [issueNoteDraft, setIssueNoteDraft] = useState("");
  const queryClient = useQueryClient();

  const sessionQuery = useQuery({
    queryKey: ["session"],
    queryFn: () => fetchJson("/api/session", z.object({ role: z.coerce.string().optional().nullable(), username: z.coerce.string().optional().nullable() }).passthrough()),
    staleTime: 60_000,
  });
  const isAdmin = sessionQuery.data?.role === "admin";
  const myUsername = sessionQuery.data?.username ?? "";

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
    enabled: invoicesOpen,
  });
  const sheetsQuery = useQuery({
    queryKey: ["supplier-picking-list", "sheets"],
    queryFn: () => fetchJson("/api/supplier-picking-list?status=picked&limit=1000", SupplierPickingListSchema),
    enabled: view === "sheets",
    refetchInterval: 30_000,
  });
  const myBalanceQuery = useQuery({
    queryKey: ["picker-balance", "me"],
    queryFn: () => fetchJson("/api/picker-cash/balance", PickerBalanceSchema),
    refetchInterval: 30_000,
  });
  const allBalancesQuery = useQuery({
    queryKey: ["picker-balances"],
    queryFn: () => fetchJson("/api/picker-cash/balances", PickerBalancesSchema),
    enabled: isAdmin,
    refetchInterval: 60_000,
  });
  const allUsersQuery = useQuery({
    queryKey: ["app-users"],
    queryFn: () => fetchJson("/api/users", z.object({ users: z.array(z.object({ username: z.coerce.string(), role: z.coerce.string().optional().default("manager"), disabled: z.boolean().optional().default(false) })).optional().default([]) }).passthrough()),
    enabled: isAdmin,
    staleTime: 120_000,
  });

  const [replaceKey, setReplaceKey] = useState<string | null>(null);
  const [editCredit, setEditCredit] = useState<{ username: string; id: string; amount: string; note: string } | null>(null);
  const [missingRow, setMissingRow] = useState<PickingRow | null>(null);
  const [pmSearchOpen, setPmSearchOpen] = useState(false);

  const issueBalanceMutation = useMutation({
    mutationFn: ({ pickerUsername, amount, note }: { pickerUsername: string; amount: number; note: string }) =>
      fetchJson("/api/picker-cash/balance", PickerBalanceSchema, mutationBody({ pickerUsername, amount, note })),
    onSuccess: () => {
      setIssueAmountDraft("");
      setIssueNoteDraft("");
      void queryClient.invalidateQueries({ queryKey: ["picker-balances"] });
      void queryClient.invalidateQueries({ queryKey: ["picker-balance"] });
    },
  });
  const deleteBalanceCreditMutation = useMutation({
    mutationFn: ({ username, id }: { username: string; id: string }) =>
      fetchJson(`/api/picker-cash/balance/${encodeURIComponent(username)}/${encodeURIComponent(id)}`, PickerBalanceSchema, { method: "DELETE" }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["picker-balances"] });
      void queryClient.invalidateQueries({ queryKey: ["picker-balance"] });
    },
  });
  const editBalanceCreditMutation = useMutation({
    mutationFn: ({ username, id, amount, note }: { username: string; id: string; amount: number; note: string }) =>
      fetchJson(`/api/picker-cash/balance/${encodeURIComponent(username)}/${encodeURIComponent(id)}`, PickerBalanceSchema, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ amount, note }) }),
    onSuccess: () => {
      setEditCredit(null);
      void queryClient.invalidateQueries({ queryKey: ["picker-balances"] });
      void queryClient.invalidateQueries({ queryKey: ["picker-balance"] });
    },
  });
  const [pickQtyDrafts, setPickQtyDrafts] = useState<Record<string, string>>({});
  const [priceDrafts, setPriceDrafts] = useState<Record<string, string>>({});
  const [returnDraftAmount, setReturnDraftAmount] = useState<string>("");
  const [returnDraftNote, setReturnDraftNote] = useState<string>("");
  const [returnTargetUser, setReturnTargetUser] = useState<string>("");

  const myDayQuery = useQuery({
    queryKey: ["picker-my-day"],
    queryFn: () => fetchJson("/api/picker-cash/balance/my-day", PickerMyDaySchema),
    refetchInterval: 60_000,
  });

  const dailyTotalQuery = useQuery({
    queryKey: ["daily-cart-total"],
    queryFn: () => fetchJson("/api/picker-cash/daily-total", DailyCartTotalSchema),
    refetchInterval: 30_000,
  });

  const resetAllBalancesMutation = useMutation({
    mutationFn: () => fetchJson("/api/picker-cash/balances/reset-all", z.object({ ok: z.boolean(), deletedBalances: z.number().optional(), deletedTotal: z.number().optional() }).passthrough(), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["picker-balances"] });
      void queryClient.invalidateQueries({ queryKey: ["picker-balance"] });
      void queryClient.invalidateQueries({ queryKey: ["picker-my-day"] });
      void queryClient.invalidateQueries({ queryKey: ["daily-cart-total"] });
    },
  });

  const [resetConfirm, setResetConfirm] = useState(false);

  const updateMutation = useMutation({
    mutationFn: ({ key, nextStatus, snoozeDays, permanent, pickedQuantity, pricePaidRub }: { key: string; nextStatus: string; snoozeDays?: number; permanent?: boolean; pickedQuantity?: number; pricePaidRub?: number }) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({
        status: nextStatus,
        ...(snoozeDays ? { snoozeDays } : {}),
        ...(permanent ? { permanent: true } : {}),
        ...(pickedQuantity != null ? { pickedQuantity } : {}),
        ...(pricePaidRub != null ? { pricePaidRub } : {}),
      })),
    onSuccess: (_data, variables) => {
      if (variables.nextStatus === "missing") {
        setMissingRow(null);
        setReplaceKey(variables.key);
      }
      if (variables.key) {
        setPickQtyDrafts((prev) => { const next = { ...prev }; delete next[variables.key]; return next; });
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
  const deferMutation = useMutation({
    mutationFn: ({ key, clear }: { key: string; clear?: boolean }) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}/defer`, z.object({ ok: z.boolean(), deferredUntil: z.string().nullable().optional() }).passthrough(), mutationBody({ clear: clear ?? false })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });
  const paymentMutation = useMutation({
    mutationFn: ({ supplierName, partnerId, amount, note }: { supplierName: string; partnerId?: string; amount: number; note: string }) =>
      fetchJson("/api/supplier-ledger/payments", SupplierLedgerPaymentSchema, mutationBody({ supplierName, partnerId, amount, note })),
    onSuccess: (_data, variables) => {
      setPaymentDrafts((current) => ({ ...current, [variables.supplierName]: "" }));
      setPaymentNotes((current) => ({ ...current, [variables.supplierName]: "" }));
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });

  const returnCashMutation = useMutation({
    mutationFn: ({ pickerUsername, amount, note }: { pickerUsername: string; amount: number; note: string }) =>
      fetchJson("/api/picker-cash/balance", PickerBalanceSchema, mutationBody({ pickerUsername, amount: -Math.abs(amount), note })),
    onSuccess: (_data, vars) => {
      setReturnDraftAmount("");
      setReturnDraftNote("");
      setReturnTargetUser("");
      void queryClient.invalidateQueries({ queryKey: ["picker-balances"] });
      void queryClient.invalidateQueries({ queryKey: ["picker-balance", vars.pickerUsername] });
      void queryClient.invalidateQueries({ queryKey: ["picker-my-day"] });
    },
  });

  const usdRate = listQuery.data?.usdRate ?? 95;

  const rows = listQuery.data?.rows || [];

  const stalledCount = useMemo(() => {
    if (status !== "open") return 0;
    const cutoff = Date.now() - 24 * 3600 * 1000;
    return rows.filter((r) => r.createdAt && new Date(r.createdAt).getTime() < cutoff).length;
  }, [rows, status]);

  useEffect(() => {
    const openCount = (listQuery.data?.summary as Record<string, number> | undefined)?.open ?? 0;
    const prev = document.title.replace(/^\(\d+\)\s*/, "");
    document.title = openCount > 0 ? `(${openCount}) ${prev}` : prev;
    return () => { document.title = document.title.replace(/^\(\d+\)\s*/, ""); };
  }, [listQuery.data?.summary]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key !== "/") return;
      const tag = (document.activeElement as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      e.preventDefault();
      searchRef.current?.focus();
      searchRef.current?.select();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  const viewersQuery = useQuery({
    queryKey: ["picking-list-viewers"],
    queryFn: () => fetchJson("/api/supplier-picking-list/viewers", z.object({ ok: z.boolean(), viewers: z.array(z.object({ username: z.string() })) }).passthrough()),
    refetchInterval: 30_000,
  });

  useEffect(() => {
    const beat = () => {
      void fetchJson("/api/supplier-picking-list/heartbeat", z.object({ ok: z.boolean() }).passthrough(), { method: "PUT", headers: { "Content-Type": "application/json" }, body: "{}" }).catch(() => {});
    };
    beat();
    const id = setInterval(beat, 30_000);
    return () => clearInterval(id);
  }, []);
  const filteredRows = useMemo(() => {
    const words = q.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return rows;
    return rows.filter((row) => {
      const text = rowSearchText(row);
      return words.every((w) => text.includes(w));
    });
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

  // When status=picked: group by date descending, then by supplier within each date
  const groupedByDay = useMemo(() => {
    if (status !== "picked") return null;
    const dayMap = new Map<string, Map<string, PickingRow[]>>();
    for (const row of filteredRows) {
      const dateKey = (row.pickedAt || row.createdAt || "").slice(0, 10) || "—";
      if (!dayMap.has(dateKey)) dayMap.set(dateKey, new Map());
      const supplierKey = row.supplierName || "Без поставщика";
      const inner = dayMap.get(dateKey)!;
      if (!inner.has(supplierKey)) inner.set(supplierKey, []);
      inner.get(supplierKey)!.push(row);
    }
    return Array.from(dayMap.entries())
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([dateKey, supplierMap]) => ({
        dateKey,
        dateLabel: dateKey !== "—" ? new Date(dateKey + "T12:00:00").toLocaleDateString("ru-RU", { weekday: "short", day: "numeric", month: "short" }) : "Без даты",
        suppliers: Array.from(supplierMap.entries()).sort((a, b) => a[0].localeCompare(b[0], "ru", { sensitivity: "base" })),
        totalPaid: Array.from(supplierMap.values()).flat().reduce((s, r) => s + (Number(r.pricePaidRub) || 0), 0),
      }));
  }, [filteredRows, status]);
  const deferredByDay = useMemo(() => {
    if (status !== "open") return [];
    const now = new Date();
    const deferred = rows.filter((r) => r.deferredUntil && new Date(r.deferredUntil) > now);
    const dayMap = new Map<string, PickingRow[]>();
    for (const row of deferred) {
      const dateKey = String(row.deferredUntil ?? "").slice(0, 10);
      if (!dayMap.has(dateKey)) dayMap.set(dateKey, []);
      dayMap.get(dateKey)!.push(row);
    }
    const tomorrow = new Date(now); tomorrow.setDate(tomorrow.getDate() + 1);
    const dayAfter = new Date(now); dayAfter.setDate(dayAfter.getDate() + 2);
    const fmt = (d: string) => {
      const dt = new Date(d + "T12:00:00");
      const tomorrowStr = tomorrow.toISOString().slice(0, 10);
      const dayAfterStr = dayAfter.toISOString().slice(0, 10);
      if (d === tomorrowStr) return "Завтра";
      if (d === dayAfterStr) return "Послезавтра";
      return dt.toLocaleDateString("ru-RU", { weekday: "short", day: "numeric", month: "short" });
    };
    return Array.from(dayMap.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([dateKey, dateRows]) => ({ dateKey, label: fmt(dateKey), rows: dateRows }));
  }, [rows, status]);

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

  const summary = listQuery.data?.summary || {};
  const suppliers = listQuery.data?.suppliers || [];
  const supplierLedger = listQuery.data?.supplierLedger || {};
  const invoiceRows = invoiceQuery.data?.rows || [];
  const myBalance = myBalanceQuery.data?.total ?? 0;
  const allBalances = allBalancesQuery.data?.balances ?? [];
  // All non-admin active accounts, for populating the picker datalist
  const knownPickerUsernames = useMemo(() => {
    const fromUsers = (allUsersQuery.data?.users ?? [])
      .filter((u) => !u.disabled && u.role !== "admin")
      .map((u) => u.username);
    const fromBalances = allBalances.map((b) => b.username);
    return [...new Set([...fromUsers, ...fromBalances])].sort();
  }, [allUsersQuery.data, allBalances]);

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

  const toggleRowExpand = (key: string) =>
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  const balanceTone = myBalance > 500 ? "success" : myBalance > 0 ? "warn" : myBalance < 0 ? "danger" : "";
  const balanceStr = (n: number) => `$${Math.round(n).toLocaleString("ru-RU")}`;
  const dailyTotal = dailyTotalQuery.data?.total ?? 0;
  const dailyItems = dailyTotalQuery.data?.items ?? 0;

  return (
    <section className="page-section picking-page">
      <PageHeader
        title="Сборка"
        subtitle="Лист закупки: собрать товар у поставщика или отметить, что товара не было."
        action={
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            {(() => {
              const others = (viewersQuery.data?.viewers || []).filter((v) => v.username !== myUsername);
              return others.length > 0 ? (
                <div className="picker-balance-chip picker-balance-chip--neutral" title={others.map((v) => v.username).join(", ")}>
                  <Users size={13} />
                  <span>{others.map((v) => v.username).join(", ")}</span>
                </div>
              ) : null;
            })()}
            {stalledCount > 0 ? (
              <div className="picker-balance-chip picker-balance-chip--danger" title={`${stalledCount} позиций ожидают сборки более 24 часов`}>
                <AlertTriangle size={13} />
                <span>{stalledCount} зависли</span>
              </div>
            ) : null}
            {/* Daily order total chip */}
            {(dailyTotal > 0 || dailyItems > 0) ? (
              <div className="picker-balance-chip picker-balance-chip--neutral" title={`Суммарный заказ сегодня: ${dailyItems} поз.`}>
                <ClipboardList size={14} />
                <span>{balanceStr(dailyTotal)}</span>
                {dailyItems > 0 ? <span style={{ opacity: 0.6, fontSize: "0.8em" }}>{dailyItems} поз.</span> : null}
              </div>
            ) : null}
            {/* Balance chip — visible to all */}
            <button
              className={`picker-balance-chip${balanceTone ? ` picker-balance-chip--${balanceTone}` : ""}`}
              type="button"
              onClick={() => setBalancePanelOpen((v) => !v)}
              title="Мой баланс"
            >
              <Wallet size={14} />
              <span>{balanceStr(myBalance)}</span>
              <ChevronDown size={12} style={{ opacity: 0.6, transform: balancePanelOpen ? "rotate(180deg)" : "none", transition: "transform .2s" }} />
            </button>
            <button className="secondary-action" type="button" onClick={() => setPmSearchOpen(true)}>
              <Database size={16} /> <span className="hide-xs">PM Поиск</span>
            </button>
            <button className="secondary-action" type="button" onClick={() => listQuery.refetch()} disabled={listQuery.isFetching}>
              {listQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />}
              <span className="hide-xs">Обновить</span>
            </button>
          </div>
        }
      />

      {/* Balance flyout panel */}
      {balancePanelOpen ? (
        <div className="picker-balance-panel">
          {/* My balance */}
          <div className="picker-balance-panel-my">
            <div className="picker-balance-panel-label"><Wallet size={14} /> Мой баланс · {myUsername || "—"}</div>
            <div className={`picker-balance-panel-total${balanceTone ? ` tone-${balanceTone}` : ""}`}>{balanceStr(myBalance)}</div>
            {/* End-of-day summary */}
            {myDayQuery.data ? (() => {
              const d = myDayQuery.data;
              const hasActivity = d.issuedToday > 0 || d.spentToday > 0;
              return (
                <div className="picker-day-summary">
                  {hasActivity ? <div className="picker-select-label" style={{ paddingTop: 0, marginBottom: 4 }}>Отчёт за сегодня</div> : null}
                  <div className="picker-day-row">
                    <span className="muted-note">Выдано</span>
                    <span className="tone-success">{balanceStr(d.issuedToday)}</span>
                  </div>
                  <div className="picker-day-row">
                    <span className="muted-note">Потрачено</span>
                    <span className="tone-warn">{balanceStr(d.spentToday)}</span>
                  </div>
                  <div className="picker-day-row" style={{ fontWeight: 600 }}>
                    <span>Остаток</span>
                    <span className={d.returnAmount > 0 ? "tone-danger" : d.returnAmount < 0 ? "tone-success" : ""}>{balanceStr(d.returnAmount)}</span>
                  </div>
                </div>
              );
            })() : null}
            {(myBalanceQuery.data?.credits ?? []).length > 0 ? (
              <div className="picker-balance-history">
                {(myBalanceQuery.data?.credits ?? []).slice(-8).reverse().map((c) => (
                  <div className="picker-balance-history-row" key={c.id}>
                    <span className={`picker-cash-amount${Number(c.amount) >= 0 ? " tone-success" : " tone-danger"}`}>{Number(c.amount) >= 0 ? "+" : ""}{balanceStr(c.amount ?? 0)}</span>
                    <span className="muted-note">{c.note || "—"}</span>
                    <span className="muted-note" style={{ marginLeft: "auto" }}>{compactDate(c.createdAt ?? null)}</span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="picker-balance-empty-hint">Пополнений ещё не было — обратитесь к администратору.</p>
            )}
          </div>

          {/* Admin: issue form + all pickers */}
          {isAdmin ? (
            <div className="picker-balance-panel-admin">

              {/* Picker selector chips */}
              {knownPickerUsernames.length > 0 ? (
                <div className="picker-select-section">
                  <div className="picker-select-label"><Users size={13} /> Сборщик</div>
                  <div className="picker-select-chips">
                    {knownPickerUsernames.map((u) => {
                      const b = allBalances.find((x) => x.username === u);
                      const total = b?.total ?? 0;
                      return (
                        <button
                          key={u}
                          type="button"
                          className={`picker-select-chip${issuePickerDraft === u ? " active" : ""}`}
                          onClick={() => setIssuePickerDraft(issuePickerDraft === u ? "" : u)}
                        >
                          <span className="picker-chip-avatar">{u[0].toUpperCase()}</span>
                          <span className="picker-chip-name">{u}</span>
                          <span className={`picker-chip-balance${total > 0 ? " pos" : total < 0 ? " neg" : ""}`}>{balanceStr(total)}</span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              ) : null}

              {/* Amount + issue */}
              <div className="picker-issue-body">
                <div className="picker-issue-amount-wrap">
                  <span className="picker-issue-currency">$</span>
                  <input
                    type="number"
                    min="0"
                    step="1"
                    placeholder="0"
                    className="picker-issue-amount-input"
                    value={issueAmountDraft}
                    onChange={(e) => setIssueAmountDraft(e.target.value)}
                  />
                </div>
                <input
                  className="picker-issue-note-input"
                  placeholder="Комментарий (необязательно)"
                  value={issueNoteDraft}
                  onChange={(e) => setIssueNoteDraft(e.target.value)}
                />
                <button
                  className="primary-action picker-issue-submit"
                  type="button"
                  disabled={issueBalanceMutation.isPending || !issuePickerDraft.trim() || !(Number(issueAmountDraft) > 0)}
                  onClick={() => issueBalanceMutation.mutate({ pickerUsername: issuePickerDraft.trim(), amount: Number(issueAmountDraft), note: issueNoteDraft })}
                >
                  {issueBalanceMutation.isPending
                    ? <><Loader2 className="spin" size={15} /> Выдаю…</>
                    : <><Check size={15} /> Выдать {issuePickerDraft ? `→ ${issuePickerDraft}` : ""}</>}
                </button>
              </div>

              {issueBalanceMutation.error ? <div className="inline-error" style={{ margin: "6px 0 0" }}>{errorMessage(issueBalanceMutation.error)}</div> : null}

              {/* Return cash form */}
              <div className="picker-return-body">
                <div className="picker-select-label" style={{ paddingTop: 0 }}>Принять возврат</div>
                <div className="picker-issue-body">
                  <div className="picker-issue-amount-wrap">
                    <span className="picker-issue-currency">$</span>
                    <input
                      type="number"
                      min="0"
                      step="1"
                      placeholder="0"
                      className="picker-issue-amount-input"
                      value={returnDraftAmount}
                      onChange={(e) => setReturnDraftAmount(e.target.value)}
                    />
                  </div>
                  <input
                    className="picker-issue-note-input"
                    placeholder="Комментарий (необязательно)"
                    value={returnDraftNote}
                    onChange={(e) => setReturnDraftNote(e.target.value)}
                  />
                  <button
                    className="secondary-action picker-issue-submit"
                    type="button"
                    disabled={returnCashMutation.isPending || !issuePickerDraft.trim() || !(Number(returnDraftAmount) > 0)}
                    onClick={() => returnCashMutation.mutate({ pickerUsername: issuePickerDraft.trim(), amount: Number(returnDraftAmount), note: returnDraftNote || "Возврат наличных" })}
                  >
                    {returnCashMutation.isPending
                      ? <><Loader2 className="spin" size={15} /> Записываю…</>
                      : <><RotateCcw size={15} /> Возврат {issuePickerDraft ? `← ${issuePickerDraft}` : ""}</>}
                  </button>
                </div>
                {returnCashMutation.error ? <div className="inline-error" style={{ margin: "6px 0 0" }}>{errorMessage(returnCashMutation.error)}</div> : null}
              </div>

              {/* Per-picker credit history with live edit/delete */}
              {issuePickerDraft && (() => {
                const b = allBalances.find((x) => x.username === issuePickerDraft);
                const credits = b?.credits ?? [];
                return (
                  <div className="picker-credit-history">
                    <div className="picker-credit-history-label">История выдач — {issuePickerDraft} {credits.length ? `(${credits.length})` : ""}</div>
                    {credits.length === 0 ? (
                      <p className="picker-balance-empty-hint">Выдач ещё не было.</p>
                    ) : credits.slice().reverse().map((c) => {
                      const isEditing = editCredit?.id === c.id && editCredit?.username === issuePickerDraft;
                      if (isEditing) {
                        return (
                          <div className="picker-credit-row picker-credit-row--edit" key={c.id}>
                            <input
                              type="number"
                              min="1"
                              className="picker-credit-edit-amount"
                              value={editCredit.amount}
                              onChange={(e) => setEditCredit((prev) => prev ? { ...prev, amount: e.target.value } : prev)}
                              autoFocus
                            />
                            <input
                              className="picker-credit-edit-note"
                              placeholder="Комментарий"
                              value={editCredit.note}
                              onChange={(e) => setEditCredit((prev) => prev ? { ...prev, note: e.target.value } : prev)}
                            />
                            <button
                              className="icon-action success-action"
                              type="button"
                              title="Сохранить"
                              disabled={editBalanceCreditMutation.isPending || !(Number(editCredit.amount) > 0)}
                              onClick={() => editBalanceCreditMutation.mutate({ username: issuePickerDraft, id: c.id, amount: Number(editCredit.amount), note: editCredit.note })}
                            >
                              {editBalanceCreditMutation.isPending ? <Loader2 className="spin" size={12} /> : <Check size={12} />}
                            </button>
                            <button className="icon-action" type="button" title="Отмена" onClick={() => setEditCredit(null)}>
                              <X size={12} />
                            </button>
                          </div>
                        );
                      }
                      return (
                        <div className="picker-credit-row" key={c.id}>
                          <span className={`picker-credit-amount${Number(c.amount) >= 0 ? " tone-success" : " tone-danger"}`}>{Number(c.amount) >= 0 ? "+" : "−"}{balanceStr(Math.abs(Number(c.amount)))}</span>
                          <span className="muted-note picker-credit-note">{c.note || "—"}</span>
                          <span className="muted-note picker-credit-date">{compactDate(c.createdAt ?? null)}</span>
                          <button
                            className="icon-action"
                            type="button"
                            title="Редактировать"
                            onClick={() => setEditCredit({ username: issuePickerDraft, id: c.id, amount: String(c.amount ?? ""), note: c.note ?? "" })}
                          >
                            <Clock size={11} />
                          </button>
                          <button
                            className="icon-action danger-action"
                            type="button"
                            title="Удалить"
                            disabled={deleteBalanceCreditMutation.isPending}
                            onClick={() => deleteBalanceCreditMutation.mutate({ username: issuePickerDraft, id: c.id })}
                          >
                            <Trash2 size={11} />
                          </button>
                        </div>
                      );
                    })}
                    {editBalanceCreditMutation.error ? <div className="inline-error" style={{ marginTop: 4 }}>{errorMessage(editBalanceCreditMutation.error)}</div> : null}
                  </div>
                );
              })()}

              {/* Daily order total summary */}
              {(dailyTotal > 0 || dailyItems > 0) ? (
                <div className="picker-day-summary" style={{ marginTop: 12, borderTop: "1px solid var(--border-soft)", paddingTop: 10 }}>
                  <div className="picker-select-label" style={{ paddingTop: 0 }}><ClipboardList size={13} /> Итог заказа сегодня</div>
                  <div className="picker-day-row">
                    <span className="muted-note">Заказано (накоплено)</span>
                    <span className="tone-success">{balanceStr(dailyTotal)}</span>
                  </div>
                  {dailyItems > 0 ? (
                    <div className="picker-day-row">
                      <span className="muted-note">Позиций</span>
                      <span>{dailyItems}</span>
                    </div>
                  ) : null}
                </div>
              ) : null}

              {/* Reset all balances */}
              <div style={{ marginTop: 12, borderTop: "1px solid var(--border-soft)", paddingTop: 10 }}>
                {resetConfirm ? (
                  <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                    <span className="muted-note" style={{ fontSize: "0.85em" }}>Обнулить все балансы?</span>
                    <button
                      className="danger-action"
                      type="button"
                      disabled={resetAllBalancesMutation.isPending}
                      onClick={() => { resetAllBalancesMutation.mutate(); setResetConfirm(false); }}
                    >
                      {resetAllBalancesMutation.isPending ? <Loader2 className="spin" size={13} /> : <Check size={13} />} Да, обнулить
                    </button>
                    <button className="secondary-action" type="button" onClick={() => setResetConfirm(false)}><X size={13} /></button>
                  </div>
                ) : (
                  <button
                    className="secondary-action"
                    type="button"
                    style={{ width: "100%" }}
                    onClick={() => setResetConfirm(true)}
                  >
                    <Trash2 size={13} /> Обнулить все балансы
                  </button>
                )}
                {resetAllBalancesMutation.error ? <div className="inline-error" style={{ marginTop: 4 }}>{errorMessage(resetAllBalancesMutation.error)}</div> : null}
              </div>
            </div>
          ) : null}
        </div>
      ) : null}

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
        <Stat label="На завтра" value={numberValue((summary as Record<string, number>).deferred ?? 0)} tone={(summary as Record<string, number>).deferred ? "accent" : ""} icon={<CalendarDays size={18} />} />
      </section>

      <div className="control-grid compact-controls picking-filters">
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
          <input ref={searchRef} value={q} onChange={(event) => setQ(event.target.value)} placeholder="SKU, товар, заказ" />
        </label>
      </div>

      {listQuery.error ? <div className="inline-error">{errorMessage(listQuery.error)}</div> : null}
      {updateMutation.error ? <div className="inline-error">{errorMessage(updateMutation.error)}</div> : null}
      {cancelCartMutation.error ? <div className="inline-error">{errorMessage(cancelCartMutation.error)}</div> : null}
      {replaceMutation.error ? <div className="inline-error">Замена поставщика: {errorMessage(replaceMutation.error)}</div> : null}
      {replaceMutation.data ? <div className="success-strip">Перезаказано у «{replaceMutation.data.supplierName || "нового поставщика"}»: заявка в PriceMaster создана (doc {replaceMutation.data.docIds?.join(", ") || "-"}).</div> : null}
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
                      {sheetRows.length} позиций · {supplierSet.size} поставщ. · {moneyAmount(totalCost)}
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
                        </div>
                        <div className="picking-actions">
                          <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}>
                            <RotateCcw size={14} /> Отменить сборку
                          </button>
                        </div>
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
          {deferredByDay.length > 0 ? (
            <div className="deferred-calendar-row">
              <span className="deferred-calendar-label"><CalendarDays size={13} /> На ближайшие дни:</span>
              {deferredByDay.map(({ dateKey, label, rows: dRows }) => (
                <span key={dateKey} className="deferred-calendar-chip">
                  <strong>{label}</strong>
                  <span>{dRows.length} поз.</span>
                </span>
              ))}
            </div>
          ) : null}
          <div className="picking-groups">
            {groupedByDay ? (
              // Picked status: group by date descending, then by supplier within each day
              groupedByDay.map(({ dateKey, dateLabel, suppliers, totalPaid }) => (
                <div key={dateKey} className="picking-day-group">
                  <div className="picking-day-header">
                    <span className="picking-day-label">{dateLabel}</span>
                    {totalPaid > 0 ? <span className="picking-day-paid">Оплачено: {moneyAmount(totalPaid)}</span> : null}
                  </div>
                  {suppliers.map(([supplierName, supplierRows]) => {
                    const pickedCurrency = String(supplierRows[0]?.priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
                    return (
                    <article className="picking-supplier-card" key={`${dateKey}||${supplierName}`}>
                      <div className="picking-supplier-toolbar">
                        <div className="picking-supplier-head">
                          <div className="picking-supplier-name-block">
                            <span>Поставщик</span>
                            <h3>{supplierName}</h3>
                          </div>
                          <div className="picking-supplier-head-meta">
                            <span className="picking-supplier-count">{supplierRows.length} поз.</span>
                            <span className="muted-note">{moneyAmount(supplierRows.reduce((s, r) => s + (Number(r.pricePaidRub) || 0), 0), "RUB")} оплачено</span>
                          </div>
                        </div>
                      </div>
                      <div className="picking-row-list">
                        {supplierRows.map((row) => {
                          const rowExpanded = expandedRows.has(row.key);
                          return (
                            <div className={`picking-row status-${row.status}`} key={row.key}>
                              <div className="picking-row-header" onClick={() => toggleRowExpand(row.key)}>
                                <div className="picking-row-header-left">
                                  <strong className="picking-row-name">{row.productName || row.offerId}</strong>
                                  <span className="picking-row-sub">{row.marketplace.toUpperCase()}{row.isExpress ? <span className="express-badge"><Zap size={11} /> Экспресс</span> : null}</span>
                                </div>
                                <div className="picking-row-header-right">
                                  <span className="picking-row-qty">×{row.pickedQuantity && row.pickedQuantity !== row.quantity ? `${row.pickedQuantity}/${row.quantity}` : row.quantity}</span>
                                  {row.pricePaidRub ? <span className="picking-row-price tone-warn">{moneyAmount(row.pricePaidRub, pickedCurrency)}</span> : row.price ? <span className="picking-row-price">{row.price} {row.priceCurrency}</span> : null}
                                  <ChevronDown size={14} className="picking-row-expand-icon" style={{ transform: rowExpanded ? "rotate(180deg)" : "none", transition: "transform .2s", opacity: 0.5 }} />
                                </div>
                              </div>
                              {rowExpanded ? (
                                <div className="picking-row-meta">
                                  <span>Заказ: {row.marketplace === "ozon" && (row.orderId || row.postingNumber)
                                    ? <a href={`https://seller.ozon.ru/app/orders/${row.orderId || row.postingNumber}`} target="_blank" rel="noopener noreferrer" className="link-plain">{row.orderId || row.postingNumber}</a>
                                    : (row.orderId || row.postingNumber || "-")}</span>
                                  <span>Собрал: {row.pickedBy || "-"}</span>
                                  {row.saleAmount ? <span className="tone-success">Продажа: {moneyAmount(row.saleAmount, pickedCurrency)}</span> : null}
                                  <span className="muted-note">Doc/Row: {row.requestDocId || "-"}/{row.requestRowId || "-"}</span>
                                </div>
                              ) : null}
                              <div className="picking-actions">
                                {row.status === "picked" ? (
                                  <>
                                    <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "returned" })}>
                                      <RotateCcw size={14} /> Возврат
                                    </button>
                                    <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}>
                                      <RotateCcw size={14} /> К сборке
                                    </button>
                                  </>
                                ) : null}
                                {isAdmin ? (
                                  <button className="secondary-action danger-action" type="button" disabled={cancelCartMutation.isPending} onClick={() => cancelCartMutation.mutate(row.key)}>
                                    <Trash2 size={14} /> Удалить
                                  </button>
                                ) : null}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </article>
                    );
                  })}
                </div>
              ))
            ) : grouped.map(([supplierName, supplierRows]) => {
              const ledger = supplierLedger[supplierName] || {};
              const balance = Number(ledger.balance || 0);
              const draftAmount = paymentDrafts[supplierName] || "";
              const draftNote = paymentNotes[supplierName] || "";
              const total = currentGroupTotal(supplierRows);
              const totalRub = currentGroupTotalRub(supplierRows, usdRate);
              const supplierCurrency = String(supplierRows[0]?.priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
              const totalQtyAll = supplierRows.reduce((s, r) => s + (r.quantity || 1), 0);
              return (
                <article className="picking-supplier-card" key={supplierName}>
                  <div className="picking-supplier-toolbar">
                    <div className="picking-supplier-head">
                      <div className="picking-supplier-name-block">
                        <span>Поставщик</span>
                        <h3>{supplierName}</h3>
                      </div>
                      <div className="picking-supplier-head-meta">
                        <span className="picking-supplier-count">{supplierRows.length} поз.{totalQtyAll !== supplierRows.length ? ` · ${totalQtyAll} шт.` : ""}</span>
                        {supplierCurrency === "RUB"
                          ? <span className={`picking-supplier-total-price${total > 0 ? " tone-warn" : ""}`}>{moneyAmount(total, "RUB")}</span>
                          : <span className={`picking-supplier-total-price${total > 0 ? " tone-warn" : ""}`}>{moneyAmount(total, "USD")} <span style={{ fontSize: "0.8em", opacity: 0.7 }}>≈{moneyAmount(totalRub, "RUB")}</span></span>
                        }
                      </div>
                    </div>
                    <div className="supplier-ledger-row">
                      {supplierCurrency === "USD" ? (
                        <DiagnosticValue label={Number(ledger.debtTotalUsd || 0) > 0 ? "Долг" : "Аванс"} value={moneySigned(-Number(ledger.debtTotalUsd || 0), "USD")} tone={Number(ledger.debtTotalUsd || 0) > 0 ? "danger" : "success"} />
                      ) : (
                        <DiagnosticValue label={balance < 0 ? "Долг" : "Аванс"} value={moneySigned(balance, "RUB")} tone={balance < 0 ? "danger" : balance > 0 ? "success" : ""} />
                      )}
                      <DiagnosticValue label="В долг" value={supplierCurrency === "USD" ? moneyAmount(Number(ledger.debtTotalUsd || 0), "USD") : moneyAmount(Number(ledger.debtTotal || 0), "RUB")} />
                      <DiagnosticValue label="Оплачено" value={moneySigned(Number(ledger.paidTotal || 0), "RUB")} tone={Number(ledger.paidTotal || 0) ? "success" : ""} />
                      <DiagnosticValue label="Сборка" value={supplierCurrency === "RUB" ? moneyAmount(total, "RUB") : `${moneyAmount(total, "USD")} / ≈${moneyAmount(totalRub, "RUB")}`} />
                    </div>
                  </div>
                  <div className="supplier-payment-row">
                    <input
                      type="number"
                      min="0"
                      step="0.01"
                      placeholder={supplierCurrency === "USD" ? "Оплата, $" : "Оплата, ₽"}
                      value={draftAmount}
                      onChange={(event) => setPaymentDrafts((current) => ({ ...current, [supplierName]: event.target.value }))}
                    />
                    <input
                      className="supplier-payment-note"
                      placeholder="Комментарий"
                      value={draftNote}
                      onChange={(event) => setPaymentNotes((current) => ({ ...current, [supplierName]: event.target.value }))}
                    />
                    <button
                      className="primary-action"
                      type="button"
                      disabled={paymentMutation.isPending || !(Number(draftAmount) > 0)}
                      onClick={() => paymentMutation.mutate({ supplierName, partnerId: supplierRows[0]?.partnerId || "", amount: supplierCurrency === "USD" ? Math.round(Number(draftAmount || 0) * usdRate) : Number(draftAmount || 0), note: draftNote })}
                    >
                      {paymentMutation.isPending ? <Loader2 className="spin" size={16} /> : <Check size={16} />} Заплатил
                    </button>
                  </div>
                  <div className="picking-row-list">
                    {(() => {
                      // Group rows by offerId for visual consolidation (same product, different orders)
                      const productMap = new Map<string, PickingRow[]>();
                      for (const row of supplierRows) {
                        const pk = row.offerId || row.productName || row.key;
                        if (!productMap.has(pk)) productMap.set(pk, []);
                        productMap.get(pk)!.push(row);
                      }
                      return Array.from(productMap.entries()).map(([pKey, pRows]) => {
                        const isMulti = pRows.length > 1;
                        const groupId = `${supplierName}||${pKey}`;
                        const isCollapsed = collapsedProductGroups.has(groupId);
                        const openRows = isMulti ? pRows.filter(r => r.status === "open") : [];
                        const totalQty = isMulti ? pRows.reduce((s, r) => s + r.quantity, 0) : 0;
                        const pickedCount = isMulti ? pRows.filter(r => r.status === "picked").length : 0;
                        const rowsToShow = isCollapsed ? [] : pRows;
                        return (
                          <div key={groupId} className={isMulti ? "picking-product-group" : undefined}>
                            {isMulti ? (
                              <div
                                className="picking-group-header"
                                onClick={() => setCollapsedProductGroups(prev => {
                                  const n = new Set(prev);
                                  n.has(groupId) ? n.delete(groupId) : n.add(groupId);
                                  return n;
                                })}
                              >
                                <div className="picking-group-header-left">
                                  <strong>{pRows[0].productName || pKey}</strong>
                                  <span className="picking-row-sub">{pKey} · {pRows[0].marketplace.toUpperCase()} · {pRows.length} заказа</span>
                                </div>
                                <button
                                  type="button"
                                  className="icon-action picking-copy-btn"
                                  title="Копировать название"
                                  onClick={(e) => { e.stopPropagation(); void copyPlainText(pRows[0].productName || pKey); }}
                                >
                                  <Copy size={13} />
                                </button>
                                <div className="picking-group-header-right">
                                  <span className="picking-row-qty">×{totalQty}</span>
                                  {pickedCount > 0 && pickedCount < pRows.length ? <span className="picking-row-status-badge status-picked">{pickedCount}/{pRows.length} собрано</span> : null}
                                  {pickedCount === pRows.length ? <span className="picking-row-status-badge status-picked">все собраны</span> : null}
                                  {openRows.length > 0 ? (
                                    <button
                                      className="primary-action success-action"
                                      type="button"
                                      style={{ fontSize: "0.75rem", padding: "3px 10px", minHeight: 30 }}
                                      disabled={updateMutation.isPending}
                                      onClick={(e) => { e.stopPropagation(); openRows.forEach(r => updateMutation.mutate({ key: r.key, nextStatus: "picked" })); }}
                                    >
                                      <Check size={13} /> Все собрал
                                    </button>
                                  ) : null}
                                  <ChevronDown size={14} style={{ transform: isCollapsed ? "none" : "rotate(180deg)", transition: "transform .2s", opacity: 0.5, flexShrink: 0 }} />
                                </div>
                              </div>
                            ) : null}
                            {rowsToShow.map((row) => {
                              const rowExpanded = expandedRows.has(row.key);
                              const rowQty = row.pickedQuantity && row.pickedQuantity !== row.quantity ? `${row.pickedQuantity}/${row.quantity}` : String(row.quantity);
                              const isUsd = String(row.priceCurrency || "").toUpperCase() !== "RUB";
                              const priceRub = row.price && isUsd ? Math.round(row.price * usdRate) : null;
                              return (
                                <div className={`picking-row status-${row.status}${isMulti ? " in-group" : ""}`} key={row.key}>
                                  <div className="picking-row-header" onClick={() => toggleRowExpand(row.key)}>
                                    <div className="picking-row-header-left">
                                      <strong className="picking-row-name">
                                        {row.productName || row.offerId}
                                      </strong>
                                      <button
                                        type="button"
                                        className="icon-action picking-copy-btn"
                                        title="Копировать название"
                                        onClick={(e) => { e.stopPropagation(); void copyPlainText(row.productName || row.offerId); }}
                                      >
                                        <Copy size={12} />
                                      </button>
                                      <span className="picking-row-sub">
                                        {isMulti
                                          ? `${row.orderId || row.postingNumber || row.offerId} · ${row.marketplace.toUpperCase()}`
                                          : row.marketplace.toUpperCase()}
                                      </span>
                                    </div>
                                    <div className="picking-row-header-right">
                                      {row.isExpress ? <span className="express-badge picking-express-inline"><Zap size={11} /></span> : null}
                                      <span className={`picking-row-qty${row.quantity > 1 ? " picking-row-qty--multi" : ""}`}>×{rowQty}</span>
                                      {row.saleAmount ? <span className="picking-row-sale">{moneyAmount(row.saleAmount, supplierCurrency)}</span> : null}
                                      {row.price ? (
                                        <span className="picking-row-price">
                                          {row.price} {row.priceCurrency}
                                          {priceRub ? <span className="picking-row-price-rub">≈{priceRub.toLocaleString("ru-RU")} ₽</span> : null}
                                        </span>
                                      ) : null}
                                      <span className={`picking-row-status-badge status-${row.status}`}>{statusLabel(row.status)}</span>
                                      <ChevronDown size={14} className="picking-row-expand-icon" style={{ transform: rowExpanded ? "rotate(180deg)" : "none", transition: "transform .2s", opacity: 0.5 }} />
                                    </div>
                                  </div>
                                  {rowExpanded ? (
                                    <div className="picking-row-meta">
                                      <span>Заказ: {row.marketplace === "ozon" && (row.orderId || row.postingNumber)
                                        ? <a href={`https://seller.ozon.ru/app/orders/${row.orderId || row.postingNumber}`} target="_blank" rel="noopener noreferrer" className="link-plain">{row.orderId || row.postingNumber}</a>
                                        : (row.orderId || row.postingNumber || "-")}</span>
                                      {row.saleAmount ? <span className="tone-success">Продажа: {moneyAmount(row.saleAmount, supplierCurrency)}</span> : null}
                                      {row.pricePaidRub ? <span className="tone-warn">Оплачено: {moneyAmount(row.pricePaidRub, "RUB")}</span> : null}
                                      <span>Доверие: {row.trustFactor}/100</span>
                                      {row.orderCutoffTime ? <span>До {row.orderCutoffTime}</span> : null}
                                      {row.reseller ? <span className="tone-warn">Перекупщик</span> : null}
                                      {row.wbSupplyId ? (
                                        <span>
                                          WB: <strong>{row.wbSupplyId}</strong>
                                          {" · "}
                                          <a href={`/api/wb/supplies/${row.wbSupplyId}/barcode?type=png`} target="_blank" rel="noreferrer" className="link-plain">Стикер</a>
                                        </span>
                                      ) : null}
                                      <span className="muted-note">Doc/Row: {row.requestDocId || "-"}/{row.requestRowId || "-"}</span>
                                    </div>
                                  ) : null}
                                  {row.status === "missing" && !row.replacementKey ? (
                                    <small className="danger-text" style={{ paddingTop: 4 }}>
                                      {row.missingPermanent || !row.nextRetryAt
                                        ? "Поставщик в инактиве насовсем."
                                        : `Инактив до ${compactDate(row.nextRetryAt)}.`}
                                      {" "}Замените кнопкой ниже или автокорзина попробует сама.
                                    </small>
                                  ) : null}
                                  {row.status === "reordered" && row.replacementKey ? <small>Перезаказано у другого поставщика.</small> : null}
                                  <div className="picking-actions">
                                    <div className="picking-action-pick-group">
                                      <button
                                        className="primary-action success-action picking-action-main"
                                        type="button"
                                        disabled={updateMutation.isPending || row.status !== "open"}
                                        onClick={() => {
                                          const qty = pickQtyDrafts[row.key] ? Math.min(row.quantity, Math.max(1, parseInt(pickQtyDrafts[row.key], 10) || 1)) : undefined;
                                          const priceRaw = priceDrafts[row.key] ? Number(priceDrafts[row.key].replace(",", ".")) : undefined;
                                          const pricePaidRub = priceRaw && priceRaw > 0
                                            ? (isUsd ? Math.round(priceRaw * usdRate) : priceRaw)
                                            : undefined;
                                          updateMutation.mutate({ key: row.key, nextStatus: "picked", pickedQuantity: qty, pricePaidRub });
                                        }}
                                      >
                                        <Check size={16} /> Собрал
                                      </button>
                                      {row.status === "open" && row.quantity > 1 ? (
                                        <input
                                          className="picking-qty-input"
                                          type="number"
                                          min={1}
                                          max={row.quantity}
                                          placeholder={String(row.quantity)}
                                          value={pickQtyDrafts[row.key] || ""}
                                          onChange={(e) => setPickQtyDrafts((p) => ({ ...p, [row.key]: e.target.value }))}
                                          onClick={(e) => e.stopPropagation()}
                                          title={`Частичная сборка: введите количество из ${row.quantity}`}
                                        />
                                      ) : null}
                                    </div>
                                    {row.status === "open" ? (
                                      <div className="picking-price-wrap" onClick={(e) => e.stopPropagation()}>
                                        {row.price ? (
                                          <span className="picking-price-hint">
                                            {isUsd
                                              ? `Цена: ${row.price} $${priceRub ? ` ≈ ${priceRub.toLocaleString("ru-RU")} ₽` : ""}`
                                              : `Цена: ${row.price} ₽`}
                                          </span>
                                        ) : null}
                                        <input
                                          className="picking-price-input"
                                          type="number"
                                          min={0}
                                          step={0.01}
                                          placeholder={isUsd ? (row.price ? `${row.price} $` : "Сумма, $") : (priceRub ? `${priceRub.toLocaleString("ru-RU")} ₽` : "Сумма, ₽")}
                                          value={priceDrafts[row.key] || ""}
                                          onChange={(e) => setPriceDrafts((p) => ({ ...p, [row.key]: e.target.value }))}
                                          title={isUsd ? "Сумма в USD — автоматически конвертируется в рубли" : "Фактическая сумма оплаты поставщику в рублях"}
                                        />
                                      </div>
                                    ) : null}
                                    <button
                                      className="secondary-action danger-action"
                                      type="button"
                                      disabled={updateMutation.isPending || row.status !== "open"}
                                      onClick={() => setMissingRow(row)}
                                    >
                                      <X size={16} /> Не было
                                    </button>
                                    {["open", "missing"].includes(row.status) && !row.replacementKey ? (
                                      <button className="secondary-action" type="button" disabled={replaceMutation.isPending} onClick={() => setReplaceKey(replaceKey === row.key ? null : row.key)}>
                                        <Repeat2 size={15} /> Замена
                                      </button>
                                    ) : null}
                                    {row.status === "picked" ? (
                                      <>
                                        <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "returned" })}>
                                          <RotateCcw size={14} /> Возврат
                                        </button>
                                        <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}>
                                          <RotateCcw size={14} /> К сборке
                                        </button>
                                      </>
                                    ) : null}
                                    {row.status !== "open" && row.status !== "picked" ? (
                                      <button className="secondary-action" type="button" disabled={updateMutation.isPending} onClick={() => updateMutation.mutate({ key: row.key, nextStatus: "open" })}>
                                        <RotateCcw size={14} /> Вернуть
                                      </button>
                                    ) : null}
                                    {row.status === "open" ? (
                                      row.deferredUntil && new Date(row.deferredUntil) > new Date() ? (
                                        <button className="secondary-action" type="button" disabled={deferMutation.isPending} onClick={() => deferMutation.mutate({ key: row.key, clear: true })} title="Снять перенос">
                                          <CalendarDays size={14} /> Сегодня
                                        </button>
                                      ) : (
                                        <button className="secondary-action" type="button" disabled={deferMutation.isPending} onClick={() => deferMutation.mutate({ key: row.key })} title="Перенести в завтрашний лист">
                                          <CalendarDays size={14} /> Завтра
                                        </button>
                                      )
                                    ) : null}
                                    {isAdmin ? (
                                      <button className="secondary-action danger-action" type="button" disabled={cancelCartMutation.isPending} onClick={() => cancelCartMutation.mutate(row.key)}>
                                        <Trash2 size={14} /> Удалить
                                      </button>
                                    ) : null}
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
                              );
                            })}
                          </div>
                        );
                      });
                    })()}
                  </div>
                </article>
              );
            })}
            {listQuery.isLoading && !grouped.length && !groupedByDay ? <ListSkeleton rows={8} /> : null}
            {!grouped.length && !groupedByDay?.length && !listQuery.isLoading ? <div className="empty-state">Строк для выбранного фильтра нет.</div> : null}
          </div>

          <section className="table-panel picking-invoices" key="invoices">
            <button
              type="button"
              className="invoices-toggle-header"
              onClick={() => setInvoicesOpen(v => !v)}
            >
              <div>
                <span className="muted-note">Внутренняя накладная</span>
                <span className="invoices-toggle-title">Собранные позиции</span>
              </div>
              <ChevronDown size={15} style={{ transform: invoicesOpen ? "rotate(180deg)" : "none", transition: "transform .2s", opacity: 0.6, flexShrink: 0 }} />
            </button>
            {invoicesOpen ? (
              <>
                <div className="section-title" style={{ marginTop: 8 }}>
                  <div />
                  <div className="supplier-cart-actions">
                    <SelectField
                      ariaLabel="Период"
                      value={period}
                      onChange={setPeriod}
                      options={[
                        { value: "1d", label: "Сегодня" },
                        { value: "7d", label: "7 дней" },
                        { value: "30d", label: "30 дней" },
                        { value: "all", label: "Все" },
                      ]}
                    />
                    <button className="secondary-action" type="button" onClick={copyInvoice} disabled={!invoiceRows.length}>
                      <Copy size={16} /> {copied ? "Скопировано" : "Скопировать"}
                    </button>
                  </div>
                </div>
                <div className="picking-invoice-list">
                  {invoiceRows.length > 80 && (
                    <div className="inline-warning" style={{ marginTop: 4 }}>Показаны первые 80 строк из {invoiceRows.length}</div>
                  )}
                  {invoiceRows.slice(0, 80).map((row) => (
                    <div className="picking-invoice-row" key={`${row.key}-${row.pickedAt || ""}`}>
                      <span>{compactDate(row.pickedAt)}</span>
                      <strong>{row.supplierName}</strong>
                      <span>{row.productName}</span>
                      <span>x{row.quantity}</span>
                      <span>{row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
                    </div>
                  ))}
                  {!invoiceRows.length && !invoiceQuery.isFetching ? <div className="soft-empty">Собранных строк за период нет.</div> : null}
                  {invoiceQuery.isFetching ? <div className="soft-empty"><Loader2 className="spin" size={14} /> Загружаю…</div> : null}
                </div>
              </>
            ) : null}
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
              Поставщик «{missingRow.supplierName || "-"}» уйдёт в инактив для этого товара. На какой срок?
            </p>
            <div className="picking-missing-options">
              {[
                { label: "Завтра появится", hint: "инактив 1 день", snoozeDays: 1 },
                { label: "2 дня", hint: "поставщик вернётся через 2 дня", snoozeDays: 2 },
                { label: "3 дня", hint: "через 3 дня", snoozeDays: 3 },
                { label: "5 дней", hint: "через 5 дней", snoozeDays: 5 },
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
                <span><strong>Насовсем</strong><small>снять можно в карточке товара</small></span>
              </button>
            </div>
            {updateMutation.error ? <div className="inline-error">{errorMessage(updateMutation.error)}</div> : null}
          </div>
        </div>
      ) : null}

      {pmSearchOpen ? (
        <>
          <div className="pm-search-backdrop" onClick={() => setPmSearchOpen(false)} />
          <PmSearchPanel onClose={() => setPmSearchOpen(false)} />
        </>
      ) : null}
    </section>
  );
}
