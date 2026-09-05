import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Boxes, CheckCircle2, ChevronRight, CreditCard, Edit3, Filter, Loader2, Package, Plus, RefreshCw, RotateCcw, Scale, Search, Trash2, Truck, UserX, X } from "lucide-react";
import { useMemo, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { SupplierLedgerEntrySchema, SupplierLedgerPaymentSchema, SupplierProfileResponseSchema, SupplierSchema, SuppliersResponseSchema } from "../types";
import { asRecord, compactDate, errorMessage, numberValue } from "../lib/common";

type Supplier = z.infer<typeof SupplierSchema>;
type LedgerEntry = z.infer<typeof SupplierLedgerEntrySchema>;
type SupplierProfile = z.infer<typeof SupplierProfileResponseSchema>;

type SupplierForm = {
  id: string;
  name: string;
  note: string;
  stopReason: string;
  priceCurrency: string;
};

type ArticleDraft = {
  id?: string;
  article: string;
  note: string;
};

type InactiveDraft = {
  supplier: Supplier;
  comment: string;
  inactiveUntil: string;
  inactiveUntilUnknown: boolean;
};

const MutationResultSchema = z.object({ ok: z.boolean().optional().default(true) }).passthrough();
const emptySupplierForm: SupplierForm = { id: "", name: "", note: "", stopReason: "", priceCurrency: "USD" };

const currencySymbol = (currency: string) => (String(currency || "USD").toUpperCase() === "RUB" ? "₽" : "$");

const moneyAmount = (value: unknown, currency = "USD") => {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return "-";
  return `${Math.round(n).toLocaleString("ru-RU")} ${currencySymbol(currency)}`;
};

const moneySigned = (value: unknown, currency = "USD") => {
  const n = Number(value || 0);
  const sym = currencySymbol(currency);
  if (!Number.isFinite(n) || n === 0) return `0 ${sym}`;
  const sign = n > 0 ? "+" : "-";
  return `${sign}${Math.abs(n).toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${sym}`;
};

function supplierId(supplier: Supplier) {
  return String(supplier.id || supplier.partnerId || supplier.name || "");
}

function supplierArticles(supplier: Supplier) {
  const raw = asRecord(supplier).articles;
  return Array.isArray(raw) ? raw.map(asRecord) : [];
}

function supplierSearchText(supplier: Supplier) {
  return [
    supplier.name,
    supplier.partnerId,
    supplier.stopReason,
    asRecord(supplier).note,
    supplier.pricingMode,
    ...supplierArticles(supplier).flatMap((article) => [article.article, article.note]),
  ].join(" ").toLowerCase();
}

function supplierIsActive(supplier: Supplier) {
  return supplier.stopped !== true && supplier.active !== false;
}

function dateInput(days = 7) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
}

function endOfMonthInput() {
  const date = new Date();
  date.setMonth(date.getMonth() + 1, 0);
  return date.toISOString().slice(0, 10);
}

function inactiveText(supplier: Supplier) {
  const raw = asRecord(supplier);
  if (raw.inactiveUntilUnknown) return "срок не указан";
  return raw.inactiveUntil ? `до ${compactDate(String(raw.inactiveUntil))}` : "срок не указан";
}

export function SuppliersPage() {
  const queryClient = useQueryClient();
  const [view, setView] = useState<"active" | "inactive">("active");
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState<"name" | "debt">("name");
  const [hasDebtFilter, setHasDebtFilter] = useState(false);
  const [form, setForm] = useState<SupplierForm>(emptySupplierForm);
  const [articleDrafts, setArticleDrafts] = useState<Record<string, ArticleDraft>>({});
  const [inactiveDraft, setInactiveDraft] = useState<InactiveDraft | null>(null);
  const [paymentDrafts, setPaymentDrafts] = useState<Record<string, string>>({});
  const [paymentNotes, setPaymentNotes] = useState<Record<string, string>>({});
  const [returnDrafts, setReturnDrafts] = useState<Record<string, string>>({});
  const [returnNotes, setReturnNotes] = useState<Record<string, string>>({});
  const [adjustDrafts, setAdjustDrafts] = useState<Record<string, string>>({});
  const [adjustNotes, setAdjustNotes] = useState<Record<string, string>>({});
  const [adjustOpen, setAdjustOpen] = useState<Set<string>>(new Set());
  const [drawerSupplier, setDrawerSupplier] = useState<Supplier | null>(null);
  const [historyShowAll, setHistoryShowAll] = useState(false);
  const [payHistoryOpen, setPayHistoryOpen] = useState(false);

  const suppliersQuery = useQuery({
    queryKey: ["suppliers"],
    queryFn: () => fetchJson("/api/suppliers", SuppliersResponseSchema),
    staleTime: 30_000,
  });

  const refreshMutation = useMutation({
    mutationFn: () => fetchJson("/api/suppliers?refresh=true", SuppliersResponseSchema),
    onSuccess: (data) => {
      queryClient.setQueryData(["suppliers"], data);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const saveSupplier = useMutation({
    mutationFn: (payload: SupplierForm) => {
      const body = {
        id: payload.id || undefined,
        name: payload.name.trim(),
        note: payload.note.trim(),
        stopReason: payload.stopReason.trim(),
        priceCurrency: payload.priceCurrency,
      };
      return payload.id
        ? fetchJson(`/api/suppliers/${encodeURIComponent(payload.id)}`, MutationResultSchema, patchBody(body))
        : fetchJson("/api/suppliers", MutationResultSchema, mutationBody(body));
    },
    onSuccess: () => {
      setForm(emptySupplierForm);
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const patchSupplier = useMutation({
    mutationFn: ({ id, patch }: { id: string; patch: Record<string, unknown> }) =>
      fetchJson(`/api/suppliers/${encodeURIComponent(id)}`, MutationResultSchema, patchBody(patch)),
    onSuccess: () => {
      setInactiveDraft(null);
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const deleteSupplier = useMutation({
    mutationFn: (id: string) => fetchJson(`/api/suppliers/${encodeURIComponent(id)}`, MutationResultSchema, { method: "DELETE" }),
    onSuccess: () => {
      setDrawerSupplier(null);
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const saveArticle = useMutation({
    mutationFn: ({ supplierIdValue, draft }: { supplierIdValue: string; draft: ArticleDraft }) =>
      fetchJson(`/api/suppliers/${encodeURIComponent(supplierIdValue)}/articles`, MutationResultSchema, mutationBody(draft)),
    onSuccess: (_data, variables) => {
      setArticleDrafts((current) => ({ ...current, [variables.supplierIdValue]: { article: "", note: "" } }));
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const deleteArticle = useMutation({
    mutationFn: ({ supplierIdValue, articleId }: { supplierIdValue: string; articleId: string }) =>
      fetchJson(`/api/suppliers/${encodeURIComponent(supplierIdValue)}/articles/${encodeURIComponent(articleId)}`, MutationResultSchema, { method: "DELETE" }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const historyQuery = useQuery<SupplierProfile>({
    queryKey: ["supplier-profile", drawerSupplier ? supplierId(drawerSupplier) : "none"],
    queryFn: () => {
      const s = drawerSupplier;
      if (!s) return Promise.resolve({ ok: true, history: [], ledger: { source: "", total: 0, entries: [], error: "" } } as unknown as SupplierProfile);
      return fetchJson(`/api/suppliers/${encodeURIComponent(supplierId(s))}/profile`, SupplierProfileResponseSchema);
    },
    enabled: !!drawerSupplier,
    staleTime: 5_000,
  });

  const returnSupplier = useMutation({
    mutationFn: ({ supplier, amount, note }: { supplier: Supplier; amount: number; note: string }) =>
      fetchJson("/api/supplier-ledger/returns", SupplierLedgerPaymentSchema, mutationBody({
        supplierName: supplier.name || "",
        partnerId: supplier.partnerId || "",
        amount,
        note,
      })),
    onSuccess: (_data, variables) => {
      const id = supplierId(variables.supplier);
      setReturnDrafts((current) => ({ ...current, [id]: "" }));
      setReturnNotes((current) => ({ ...current, [id]: "" }));
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-profile"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });

  const returnPicking = useMutation({
    mutationFn: ({ pickingKey, note }: { pickingKey: string; note?: string }) =>
      fetchJson("/api/supplier-ledger/return-picking", SupplierLedgerPaymentSchema, mutationBody({ pickingKey, note: note || "" })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-profile"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });

  const paySupplier = useMutation({
    mutationFn: ({ supplier, amount, note }: { supplier: Supplier; amount: number; note: string }) =>
      fetchJson("/api/supplier-ledger/payments", SupplierLedgerPaymentSchema, mutationBody({
        supplierName: supplier.name || "",
        partnerId: supplier.partnerId || "",
        amount,
        note,
      })),
    onSuccess: (_data, variables) => {
      const id = supplierId(variables.supplier);
      setPaymentDrafts((current) => ({ ...current, [id]: "" }));
      setPaymentNotes((current) => ({ ...current, [id]: "" }));
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-profile"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });

  const adjustBalance = useMutation({
    mutationFn: ({ supplier, targetBalance, note }: { supplier: Supplier; targetBalance: number; note: string }) =>
      fetchJson("/api/supplier-ledger/adjust", z.object({ ok: z.boolean(), skipped: z.boolean().optional(), currentBalance: z.number().optional(), targetBalance: z.number().optional(), delta: z.number().optional(), message: z.string().optional() }).passthrough(), mutationBody({
        supplierName: supplier.name || "",
        partnerId: supplier.partnerId || "",
        targetBalance,
        note,
      })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-profile"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });

  const resetAllHistory = useMutation({
    mutationFn: () => fetchJson("/api/supplier-ledger/reset-all-history", z.object({ ok: z.boolean(), ledger: z.number(), picking: z.number(), cart: z.number() }), { method: "DELETE" }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-profile"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });

  const suppliers = suppliersQuery.data?.suppliers || [];
  const sync = asRecord(suppliersQuery.data?.supplierSync);
  const usdRate = suppliersQuery.data?.usdRate ?? 95;
  const activeCount = suppliers.filter(supplierIsActive).length;
  const inactiveCount = suppliers.length - activeCount;
  const articleCount = suppliers.reduce((sum, supplier) => sum + supplierArticles(supplier).length, 0);
  const affectedCount = suppliers.reduce((sum, supplier) => sum + numberValue(supplier.impactProductCount, 0), 0);

  const filtered = useMemo(() => {
    const words = search.trim().toLowerCase().split(/\s+/).filter(Boolean);
    return suppliers
      .filter((supplier) => view === "active" ? supplierIsActive(supplier) : !supplierIsActive(supplier))
      .filter((supplier) => {
        if (!words.length) return true;
        const text = supplierSearchText(supplier);
        return words.every((w) => text.includes(w));
      })
      .filter((supplier) => !hasDebtFilter || Number(asRecord(asRecord(supplier).ledger).balance || 0) < 0)
      .sort((a, b) => {
        if (sortBy === "debt") {
          const debtA = -Number(asRecord(asRecord(a).ledger).balance || 0);
          const debtB = -Number(asRecord(asRecord(b).ledger).balance || 0);
          return debtB - debtA;
        }
        return String(a.name || "").localeCompare(String(b.name || ""), "ru");
      });
  }, [search, suppliers, view, sortBy, hasDebtFilter]);

  const startEdit = (supplier: Supplier) => {
    setForm({
      id: supplierId(supplier),
      name: supplier.name || "",
      note: String(asRecord(supplier).note || ""),
      stopReason: String(supplier.stopReason || ""),
      priceCurrency: String(asRecord(supplier).priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD",
    });
    setDrawerSupplier(null);
  };

  const startInactive = (supplier: Supplier) => {
    const raw = asRecord(supplier);
    setInactiveDraft({
      supplier,
      comment: String(raw.inactiveComment || supplier.stopReason || ""),
      inactiveUntil: typeof raw.inactiveUntil === "string" ? raw.inactiveUntil.slice(0, 10) : dateInput(7),
      inactiveUntilUnknown: Boolean(raw.inactiveUntilUnknown),
    });
  };

  const openDrawer = (supplier: Supplier) => {
    setDrawerSupplier(supplier);
    setHistoryShowAll(false);
    setPayHistoryOpen(false);
  };

  const setArticleDraft = (id: string, patch: Partial<ArticleDraft>) => {
    setArticleDrafts((current) => {
      const currentDraft = current[id];
      return { ...current, [id]: { ...(currentDraft || { article: "", note: "" }), ...patch } };
    });
  };

  const submitInactive = () => {
    if (!inactiveDraft) return;
    const id = supplierId(inactiveDraft.supplier);
    patchSupplier.mutate({
      id,
      patch: {
        stopped: true,
        stopReason: inactiveDraft.comment,
        inactiveComment: inactiveDraft.comment,
        inactiveUntil: inactiveDraft.inactiveUntilUnknown ? null : inactiveDraft.inactiveUntil,
        inactiveUntilUnknown: inactiveDraft.inactiveUntilUnknown,
      },
    });
  };

  const anyError = suppliersQuery.error || refreshMutation.error || saveSupplier.error || patchSupplier.error || deleteSupplier.error || saveArticle.error || deleteArticle.error || paySupplier.error || returnSupplier.error || returnPicking.error;

  // Derive all drawer-specific data in one pass
  const drawerData = drawerSupplier ? (() => {
    const supplier = drawerSupplier;
    const id = supplierId(supplier);
    const raw = asRecord(supplier);
    const articles = supplierArticles(supplier);
    const draft = articleDrafts[id] || { article: "", note: "" };
    const ledger = asRecord(raw.ledger);
    const balance = Number(ledger.balance || 0);
    const supplierCurrency = String(raw.priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
    // Use original USD prices from raw picking data to avoid drift when the rate changes.
    const debtTotalUsdDrawer = Number((ledger as Record<string, unknown>).debtTotalUsd || 0);
    const debtTotalRubDrawer = Number((ledger as Record<string, unknown>).debtTotal || 0);
    const paidTotalRubDrawer = Number(ledger.paidTotal || 0);
    // creditTotal includes payments + balance corrections + returns — use for balance formula so corrections are reflected
    const creditTotalRubDrawer = Number((ledger as Record<string, unknown>).creditTotal || paidTotalRubDrawer);
    const balanceUsd = supplierCurrency === "USD" ? -debtTotalUsdDrawer + creditTotalRubDrawer / usdRate : balance / usdRate;
    const paymentAmount = paymentDrafts[id] || "";
    const paymentNote = paymentNotes[id] || "";
    const active = supplierIsActive(supplier);
    const stockOnly = supplier.pricingMode === "stock_only" || supplier.stockOnly === true;
    const profile = historyQuery.data;
    const ledgerEntries = profile?.ledger?.entries || [];
    const returnedKeys = new Set(
      ledgerEntries.filter((e) => e.entryType === "supplier_return" && e.pickingKey).map((e) => e.pickingKey as string)
    );
    const debtByKey = new Map(
      ledgerEntries.filter((e) => e.entryType === "purchase_debt" && e.pickingKey).map((e) => [e.pickingKey as string, e])
    );
    const paymentEntries = ledgerEntries
      .filter((e) => e.entryType === "payment" || e.entryType === "balance_correction" || e.entryType === "supplier_return")
      .sort((a, b) => String(b.occurredAt || "").localeCompare(String(a.occurredAt || "")));
    const cutoff = historyShowAll ? null : new Date(Date.now() - 30 * 86_400_000);
    const pickedRows = (profile?.history || []).filter((r) => {
      if (r.status !== "picked") return false;
      if (cutoff && r.pickedAt) return new Date(r.pickedAt) >= cutoff;
      if (cutoff && !r.pickedAt) return false;
      return true;
    });
    return { id, supplier, raw, articles, draft, ledger, balance, balanceUsd, supplierCurrency, paymentAmount, paymentNote, active, stockOnly, profile, ledgerEntries, returnedKeys, debtByKey, pickedRows, debtTotalUsdDrawer, debtTotalRubDrawer, paymentEntries };
  })() : null;

  return (
    <section className="page-section suppliers-page">
      <PageHeader
        title="Поставщики"
        subtitle="Импорт из PriceMaster, баланс долгов, история заказов и управление артикулами."
        action={(
          <button className="primary-action" type="button" disabled={refreshMutation.isPending} onClick={() => refreshMutation.mutate()}>
            {refreshMutation.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Загрузить из PriceMaster
          </button>
        )}
      />

      {anyError ? <div className="inline-error" style={{ marginBottom: 12 }}>{errorMessage(anyError)}</div> : null}

      <section className="dashboard-metrics">
        <Stat label="Активных" value={activeCount} tone={activeCount ? "success" : ""} icon={<Truck size={18} />} />
        <Stat label="Остановлено" value={inactiveCount} tone={inactiveCount ? "warn" : "success"} icon={<UserX size={18} />} />
        <Stat label="Артикулов" value={articleCount} tone="accent" icon={<Boxes size={18} />} />
        <Stat label="Связанных товаров" value={affectedCount} tone="accent" icon={<Package size={18} />} />
      </section>

      {sync.ok === true || sync.error ? (
        <div className={sync.error ? "inline-error" : "success-strip"}>
          {sync.error ? `PriceMaster: ${String(sync.error)}` : `PriceMaster: найдено партнеров ${String(sync.partners || 0)}, импортировано ${String(sync.imported || 0)}.`}
        </div>
      ) : null}

      <section className="settings-grid supplier-layout-grid">
        {/* Left panel: add/edit form */}
        <div className="settings-panel supplier-form-panel">
          <div className="section-title">
            <div><span>Карточка</span><h3>{form.id ? "Редактировать поставщика" : "Новый поставщик"}</h3></div>
          </div>
          <form
            className="supplier-form"
            onSubmit={(event) => {
              event.preventDefault();
              if (!form.name.trim()) return;
              saveSupplier.mutate(form);
            }}
          >
            <label>Название<input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} /></label>
            <label>Заметка<input value={form.note} onChange={(event) => setForm({ ...form, note: event.target.value })} /></label>
            <label>Причина остановки<input value={form.stopReason} onChange={(event) => setForm({ ...form, stopReason: event.target.value })} /></label>
            <label>Валюта закупки в PriceMaster
              <SelectField
                ariaLabel="Валюта поставщика"
                value={form.priceCurrency}
                onChange={(next) => setForm({ ...form, priceCurrency: next })}
                options={[
                  { value: "USD", label: "Доллары (USD) — цена × курс × наценка" },
                  { value: "RUB", label: "Рубли (RUB) — цена × наценка" },
                ]}
              />
            </label>
            <div className="row-actions">
              <button className="primary-action" type="submit" disabled={saveSupplier.isPending || !form.name.trim()}>
                {saveSupplier.isPending ? <Loader2 className="spin" size={16} /> : <Plus size={16} />} Сохранить
              </button>
              {form.id ? <button className="secondary-action" type="button" onClick={() => setForm(emptySupplierForm)}><X size={16} /> Отмена</button> : null}
            </div>
          </form>
        </div>

        {/* Right panel: compact table */}
        <div className="settings-panel supplier-list-panel">
          <div className="section-title">
            <div><span>Список</span><h3>Все поставщики</h3></div>
            <button className="secondary-action" type="button" disabled={suppliersQuery.isFetching} onClick={() => suppliersQuery.refetch()}>
              {suppliersQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
            </button>
          </div>

          <div className="supplier-toolbar-flex">
            <div className="settings-tabs">
              <button className={view === "active" ? "is-active" : ""} type="button" onClick={() => setView("active")}>Активные</button>
              <button className={view === "inactive" ? "is-active" : ""} type="button" onClick={() => setView("inactive")}>Остановленные</button>
            </div>
            <div className="settings-tabs">
              <button className={sortBy === "name" ? "is-active" : ""} type="button" title="Сортировка по имени" onClick={() => setSortBy("name")}>А→Я</button>
              <button className={sortBy === "debt" ? "is-active" : ""} type="button" title="Сортировка по долгу" onClick={() => setSortBy("debt")}><Scale size={13} /> Долг</button>
              <button
                className={hasDebtFilter ? "is-active" : ""}
                type="button"
                title={hasDebtFilter ? "Сбросить фильтр долга" : "Показать только поставщиков с долгом"}
                onClick={() => setHasDebtFilter((v) => !v)}
              >
                <Filter size={13} /> Есть долг
              </button>
            </div>
            <label className="supplier-search" style={{ flex: 1 }}>
              <Search size={16} />
              <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Поиск" />
            </label>
          </div>

          {/* Compact table */}
          <div className="supplier-compact-table">
            <div className="supplier-table-head">
              <span>Поставщик</span>
              <span>Валюта</span>
              <span>Долг / Баланс</span>
              <span>Оплачено</span>
              <span>Последняя оплата</span>
              <span></span>
            </div>

            {suppliersQuery.isLoading ? (
              <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю поставщиков...</div>
            ) : null}
            {!suppliersQuery.isLoading && !filtered.length ? (
              <div className="soft-empty">{hasDebtFilter ? "Нет поставщиков с долгом." : "Поставщики не найдены."}</div>
            ) : null}

            {filtered.map((supplier) => {
              const id = supplierId(supplier);
              const raw = asRecord(supplier);
              const ledger = asRecord(raw.ledger);
              const balance = Number(ledger.balance || 0);
              const supplierCurrency = String(raw.priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
              const paidTotal = Number(ledger.paidTotal || 0);
              const debtTotalUsdList = Number((ledger as Record<string, unknown>).debtTotalUsd || 0);
              const creditTotalList = Number((ledger as Record<string, unknown>).creditTotal || paidTotal);
              // Use original USD prices to avoid drift when the rate changes between purchase and display.
              const balanceDisplay = supplierCurrency === "USD" ? -debtTotalUsdList + creditTotalList / usdRate : balance;
              const paidDisplay = supplierCurrency === "USD" ? paidTotal / usdRate : paidTotal;
              const active = supplierIsActive(supplier);
              const isOpen = drawerSupplier && supplierId(drawerSupplier) === id;
              return (
                <div
                  className={`supplier-table-row${active ? "" : " is-inactive"}${isOpen ? " is-open" : ""}`}
                  key={id}
                  role="button"
                  tabIndex={0}
                  onClick={() => openDrawer(supplier)}
                  onKeyDown={(e) => e.key === "Enter" && openDrawer(supplier)}
                >
                  <div className="supplier-table-name">
                    <strong>{supplier.name || "Поставщик"}</strong>
                    <small>{supplier.partnerId ? `partner ${supplier.partnerId}` : "local"}</small>
                    {!active ? <span className="supplier-stopped-tag"><UserX size={11} /> стоп</span> : null}
                  </div>
                  <div>{supplierCurrency}</div>
                  <div className={balanceDisplay < 0 ? "danger-text" : balanceDisplay > 0 ? "success-text" : ""} style={{ fontVariantNumeric: "tabular-nums" }}>
                    {moneySigned(balanceDisplay, supplierCurrency)}
                  </div>
                  <div style={{ fontVariantNumeric: "tabular-nums", color: paidDisplay > 0 ? "var(--success, #4ed39a)" : "var(--text-muted)" }}>
                    {paidDisplay > 0 ? moneyAmount(paidDisplay, supplierCurrency) : "—"}
                  </div>
                  <div className="muted-note">
                    {ledger.lastPaymentAt ? compactDate(String(ledger.lastPaymentAt)) : "—"}
                  </div>
                  <div className="supplier-table-open">
                    <ChevronRight size={15} />
                  </div>
                </div>
              );
            })}
          </div>

          <details className="supplier-reset-details">
            <summary className="muted-note" style={{ cursor: "pointer", padding: "8px 0", display: "flex", alignItems: "center", gap: 6, fontSize: "0.8rem" }}>
              <Trash2 size={13} /> Сбросить все данные (долги, история заказов)
            </summary>
            <div style={{ padding: "10px 0 4px", display: "flex", flexDirection: "column", gap: 8 }}>
              <p className="muted-note" style={{ fontSize: "0.8rem", margin: 0 }}>
                Удаляет все записи долгов, историю сборки и черновики корзин. Операция необратима.
              </p>
              {resetAllHistory.isSuccess ? (
                <div className="success-strip" style={{ fontSize: "0.82rem" }}>
                  Сброшено: долгов {resetAllHistory.data.ledger}, строк сборки {resetAllHistory.data.picking}, корзин {resetAllHistory.data.cart}
                </div>
              ) : null}
              {resetAllHistory.isError ? <div className="inline-error">{errorMessage(resetAllHistory.error)}</div> : null}
              <button
                className="secondary-action danger-action"
                type="button"
                disabled={resetAllHistory.isPending}
                onClick={() => {
                  if (window.confirm("Удалить ВСЮ историю долгов, сборки и корзин для всех поставщиков? Это действие необратимо.")) {
                    resetAllHistory.mutate();
                  }
                }}
              >
                {resetAllHistory.isPending ? <Loader2 className="spin" size={15} /> : <Trash2 size={15} />} Сбросить всё
              </button>
            </div>
          </details>
        </div>
      </section>

      {/* ===== Supplier Drawer ===== */}
      {drawerSupplier && drawerData ? (
        <div
          className="supplier-modal-backdrop"
          onClick={() => setDrawerSupplier(null)}
        >
          <aside
            className="supplier-drawer"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Header */}
            <div className="supplier-drawer-head">
              <div>
                <small className="muted-note">{drawerData.supplier.partnerId ? `partner ${drawerData.supplier.partnerId}` : "local"}</small>
                <h3>{drawerData.supplier.name || "Поставщик"}</h3>
                <div className="supplier-badge-row" style={{ marginTop: 6 }}>
                  <span className={`supplier-status-pill${drawerData.active ? "" : " is-stopped"}`}>
                    {drawerData.active ? <><CheckCircle2 size={12} /> активен</> : <><UserX size={12} /> остановлен</>}
                  </span>
                  {drawerData.stockOnly ? <span className="warning-badge">не берет цену</span> : null}
                  {drawerData.supplier.reseller ? <span>перекупщик</span> : null}
                  <span>товаров {drawerData.supplier.impactProductCount || 0}</span>
                  <span>доверие {drawerData.supplier.trustFactor ?? 100}</span>
                </div>
              </div>
              <button className="icon-action" type="button" onClick={() => setDrawerSupplier(null)} style={{ flexShrink: 0 }}><X size={18} /></button>
            </div>

            {/* Ledger strip */}
            <div className="summary-grid compact-summary supplier-ledger-strip">
              {drawerData.supplierCurrency === "USD" ? (
                <DiagnosticValue label={drawerData.balanceUsd < 0 ? "Долг поставщику" : "Аванс / баланс"} value={moneySigned(drawerData.balanceUsd, "USD")} tone={drawerData.balanceUsd < 0 ? "danger" : drawerData.balanceUsd > 0 ? "success" : ""} />
              ) : (
                <DiagnosticValue label={drawerData.balance < 0 ? "Долг поставщику" : "Аванс / баланс"} value={moneySigned(drawerData.balance, "RUB")} tone={drawerData.balance < 0 ? "danger" : drawerData.balance > 0 ? "success" : ""} />
              )}
              <DiagnosticValue label="Собрано в долг" value={drawerData.supplierCurrency === "USD" ? moneyAmount(drawerData.debtTotalUsdDrawer, "USD") : moneyAmount(drawerData.debtTotalRubDrawer, "RUB")} />
              <DiagnosticValue label="Оплачено" value={drawerData.supplierCurrency === "USD" ? moneySigned(Number(drawerData.ledger.paidTotal || 0) / usdRate, "USD") : moneySigned(Number(drawerData.ledger.paidTotal || 0), "RUB")} tone={Number(drawerData.ledger.paidTotal || 0) ? "success" : ""} />
              <DiagnosticValue label="Последняя оплата" value={drawerData.ledger.lastPaymentAt ? compactDate(String(drawerData.ledger.lastPaymentAt)) : "—"} />
            </div>

            {/* Currency selector */}
            <label className="supplier-currency-inline">
              <span>Валюта закупки</span>
              <SelectField
                ariaLabel="Валюта закупки"
                value={drawerData.supplierCurrency}
                disabled={patchSupplier.isPending}
                onChange={(next) => patchSupplier.mutate({ id: drawerData.id, patch: { priceCurrency: next } })}
                options={[{ value: "USD", label: "USD" }, { value: "RUB", label: "RUB" }]}
              />
            </label>

            {drawerData.raw.note ? <p className="supplier-note">{String(drawerData.raw.note)}</p> : null}
            {!drawerData.active ? <p className="supplier-note danger-text">Остановлен {inactiveText(drawerData.supplier)}. {String(drawerData.raw.inactiveComment || drawerData.supplier.stopReason || "")}</p> : null}

            {/* Payment */}
            <div className="supplier-drawer-section">
              <strong>Оплата</strong>
              <div className="settings-form-row supplier-payment-row">
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  placeholder={drawerData.supplierCurrency === "USD" ? "Сумма, $" : "Сумма, ₽"}
                  value={drawerData.paymentAmount}
                  onChange={(event) => setPaymentDrafts((current) => ({ ...current, [drawerData.id]: event.target.value }))}
                />
                {drawerData.supplierCurrency === "USD" ? <span className="muted" style={{ fontSize: 12 }}>Курс: {usdRate}₽</span> : null}
                <input
                  className="supplier-payment-note"
                  placeholder="Комментарий"
                  value={drawerData.paymentNote}
                  onChange={(event) => setPaymentNotes((current) => ({ ...current, [drawerData.id]: event.target.value }))}
                />
                <button
                  className="primary-action"
                  type="button"
                  disabled={paySupplier.isPending || !(Number(drawerData.paymentAmount) > 0)}
                  onClick={() => paySupplier.mutate({
                    supplier: drawerData.supplier,
                    amount: drawerData.supplierCurrency === "USD" ? Math.round(Number(drawerData.paymentAmount || 0) * usdRate) : Number(drawerData.paymentAmount || 0),
                    note: drawerData.paymentNote,
                  })}
                >
                  {paySupplier.isPending ? <Loader2 className="spin" size={16} /> : <CheckCircle2 size={16} />} Заплатил
                </button>
              </div>
            </div>

            {/* Adjust balance */}
            <details open={adjustOpen.has(drawerData.id)} onToggle={(e) => {
              const el = e.currentTarget as HTMLDetailsElement;
              setAdjustOpen((prev) => { const n = new Set(prev); el.open ? n.add(drawerData.id) : n.delete(drawerData.id); return n; });
            }}>
              <summary className="muted-note" style={{ cursor: "pointer", padding: "4px 0", display: "flex", alignItems: "center", gap: 6 }}>
                <Scale size={13} /> Свести баланс с поставщиком
              </summary>
              <div className="settings-form-row supplier-payment-row" style={{ marginTop: 8 }}>
                <input
                  type="number"
                  step="0.01"
                  placeholder={drawerData.supplierCurrency === "USD" ? "Фактический баланс, $" : "Фактический баланс, ₽"}
                  value={adjustDrafts[drawerData.id] || ""}
                  onChange={(e) => setAdjustDrafts((p) => ({ ...p, [drawerData.id]: e.target.value }))}
                  title={`Текущий: ${drawerData.supplierCurrency === "USD" ? `${moneySigned(drawerData.balanceUsd, "USD")} (≈ ${moneySigned(drawerData.balance, "RUB")})` : moneySigned(drawerData.balance, "RUB")}`}
                />
                <input
                  className="supplier-payment-note"
                  placeholder="Комментарий"
                  value={adjustNotes[drawerData.id] || ""}
                  onChange={(e) => setAdjustNotes((p) => ({ ...p, [drawerData.id]: e.target.value }))}
                />
                <button
                  className="primary-action"
                  type="button"
                  disabled={adjustBalance.isPending || !Number.isFinite(Number(adjustDrafts[drawerData.id] || undefined))}
                  onClick={() => {
                    const inputVal = Number(adjustDrafts[drawerData.id] || 0);
                    if (!Number.isFinite(inputVal)) return;
                    const targetBalance = drawerData.supplierCurrency === "USD" ? Math.round(inputVal * usdRate) : inputVal;
                    adjustBalance.mutate({ supplier: drawerData.supplier, targetBalance, note: adjustNotes[drawerData.id] || "" }, {
                      onSuccess: () => {
                        setAdjustDrafts((p) => ({ ...p, [drawerData.id]: "" }));
                        setAdjustNotes((p) => ({ ...p, [drawerData.id]: "" }));
                      },
                    });
                  }}
                >
                  {adjustBalance.isPending ? <Loader2 className="spin" size={16} /> : <Scale size={16} />} Свести
                </button>
              </div>
              {adjustBalance.isSuccess && adjustBalance.data && adjustBalance.variables && supplierId(adjustBalance.variables.supplier) === drawerData.id && (
                <div className="inline-success" style={{ marginTop: 6, fontSize: "0.82rem" }}>
                  {adjustBalance.data.skipped
                    ? adjustBalance.data.message
                    : drawerData.supplierCurrency === "USD"
                      ? `Корректировка: ${moneySigned((adjustBalance.data.currentBalance ?? 0) / usdRate, "USD")} → ${moneySigned((adjustBalance.data.targetBalance ?? 0) / usdRate, "USD")} (запись на ${moneySigned((adjustBalance.data.delta ?? 0) / usdRate, "USD")})`
                      : `Корректировка: ${moneySigned(adjustBalance.data.currentBalance ?? 0, "RUB")} → ${moneySigned(adjustBalance.data.targetBalance ?? 0, "RUB")} (запись на ${moneySigned(adjustBalance.data.delta ?? 0, "RUB")})`}
                </div>
              )}
              {adjustBalance.isError && adjustBalance.variables && supplierId(adjustBalance.variables.supplier) === drawerData.id && <div className="inline-error" style={{ marginTop: 6 }}>{errorMessage(adjustBalance.error)}</div>}
            </details>

            {/* Payment history */}
            {drawerData.paymentEntries.length > 0 ? (
              <details open={payHistoryOpen} onToggle={(e) => setPayHistoryOpen((e.currentTarget as HTMLDetailsElement).open)} style={{ marginTop: 4 }}>
                <summary className="muted-note" style={{ cursor: "pointer", padding: "4px 0", display: "flex", alignItems: "center", gap: 6 }}>
                  <CreditCard size={13} /> История оплат ({drawerData.paymentEntries.length})
                </summary>
                <div className="supplier-orders-list" style={{ marginTop: 8 }}>
                  <div className="supplier-orders-header">
                    <span>Тип</span><span>Сумма</span><span>Дата</span><span>Заметка</span>
                  </div>
                  {drawerData.paymentEntries.map((entry) => {
                    const amountUsd = drawerData.supplierCurrency === "USD" ? entry.amount / usdRate : null;
                    const typeLabel = entry.entryType === "payment" ? "Оплата"
                      : entry.entryType === "balance_correction" ? "Корректировка"
                      : entry.entryType === "supplier_return" ? "Возврат"
                      : entry.entryType;
                    const isNeg = entry.amount < 0;
                    return (
                      <div className="supplier-order-row" key={entry.id}>
                        <div className="supplier-order-name"><span>{typeLabel}</span></div>
                        <span className="supplier-order-amount" style={{ color: isNeg ? "var(--danger, #f87171)" : "var(--success, #4ed39a)" }}>
                          {drawerData.supplierCurrency === "USD" && amountUsd !== null
                            ? moneySigned(amountUsd, "USD")
                            : moneySigned(entry.amount, "RUB")}
                        </span>
                        <span className="muted-note">{compactDate(entry.occurredAt ?? null)}</span>
                        <span className="muted-note" style={{ fontSize: "0.78rem" }}>{entry.note || ""}</span>
                      </div>
                    );
                  })}
                </div>
              </details>
            ) : null}

            {/* Action buttons */}
            <div className="row-actions">
              <button className="secondary-action" type="button" onClick={() => startEdit(drawerData.supplier)}><Edit3 size={16} /> Редактировать</button>
              {drawerData.active ? (
                <button className="secondary-action danger-action" type="button" onClick={() => startInactive(drawerData.supplier)}><UserX size={16} /> Не работает</button>
              ) : (
                <button className="secondary-action" type="button" disabled={patchSupplier.isPending} onClick={() => patchSupplier.mutate({ id: drawerData.id, patch: { stopped: false } })}><CheckCircle2 size={16} /> Вернуть</button>
              )}
              <button
                className="secondary-action danger-action"
                type="button"
                disabled={deleteSupplier.isPending}
                onClick={() => {
                  if (window.confirm(`Удалить поставщика ${drawerData.supplier.name || drawerData.id}?`)) deleteSupplier.mutate(drawerData.id);
                }}
              >
                <Trash2 size={16} /> Удалить
              </button>
            </div>

            {/* Order history */}
            <div className="supplier-drawer-section">
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <strong>История заказов</strong>
                <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                  {historyQuery.isFetching ? <Loader2 className="spin" size={13} /> : null}
                  <div className="settings-tabs">
                    <button type="button" className={!historyShowAll ? "is-active" : ""} onClick={() => setHistoryShowAll(false)}>30 дней</button>
                    <button type="button" className={historyShowAll ? "is-active" : ""} onClick={() => setHistoryShowAll(true)}>Все</button>
                  </div>
                </div>
              </div>

              {historyQuery.isLoading ? (
                <div className="soft-empty" style={{ marginTop: 8 }}><Loader2 className="spin" size={14} /> Загружаю историю...</div>
              ) : drawerData.pickedRows.length > 0 ? (
                <div className="supplier-orders-list" style={{ marginTop: 8 }}>
                  <div className="supplier-orders-header">
                    <span>Товар</span><span>Сумма</span><span>Дата</span><span></span>
                  </div>
                  {drawerData.pickedRows.map((row) => {
                    const debt = drawerData.debtByKey.get(row.key);
                    const amountRub = debt ? Math.abs(Number(debt.amount)) : null;
                    const isReturned = drawerData.returnedKeys.has(row.key);
                    const isPending = returnPicking.isPending && returnPicking.variables?.pickingKey === row.key;
                    return (
                      <div className="supplier-order-row" key={row.key}>
                        <div className="supplier-order-name">
                          <span>{row.productName || row.offerId || row.key}</span>
                          {row.offerId ? <small className="muted-note">{row.offerId}</small> : null}
                        </div>
                        <span className="supplier-order-amount">
                          {drawerData.supplierCurrency === "USD" && row.price
                            ? moneyAmount(Number(row.price) * Math.max(1, Number(row.quantity || 1)), String(row.priceCurrency || "USD"))
                            : amountRub !== null ? moneyAmount(amountRub, "RUB") : `${row.price} ${row.priceCurrency}`}
                        </span>
                        <span className="muted-note">{compactDate(row.pickedAt ?? null)}</span>
                        {isReturned ? (
                          <span className="supplier-returned-badge"><RotateCcw size={12} /> Возврат</span>
                        ) : (
                          <button
                            className="secondary-action danger-action supplier-return-btn"
                            type="button"
                            disabled={isPending || returnPicking.isPending}
                            onClick={() => {
                              if (window.confirm(`Вернуть «${row.productName || row.offerId || row.key}» поставщику? Это действие нельзя отменить.`)) {
                                returnPicking.mutate({ pickingKey: row.key });
                              }
                            }}
                          >
                            {isPending ? <Loader2 className="spin" size={13} /> : <RotateCcw size={13} />} Возврат
                          </button>
                        )}
                      </div>
                    );
                  })}
                </div>
              ) : (
                <div className="soft-empty" style={{ marginTop: 8 }}>
                  {historyShowAll ? "Заказов пока нет." : "Заказов за последние 30 дней нет."}
                  {!historyShowAll ? (
                    <button type="button" style={{ marginLeft: 8, background: "none", border: "none", color: "var(--accent, #3b6dff)", cursor: "pointer", fontSize: "inherit" }} onClick={() => setHistoryShowAll(true)}>
                      Показать всё
                    </button>
                  ) : null}
                </div>
              )}

              <details className="supplier-manual-return-details" style={{ marginTop: 10 }}>
                <summary className="muted-note" style={{ cursor: "pointer", padding: "4px 0" }}>Ручной возврат (произвольная сумма)</summary>
                <div className="settings-form-row supplier-payment-row" style={{ marginTop: 8 }}>
                  <input
                    type="number"
                    min="0"
                    step="0.01"
                    placeholder={drawerData.supplierCurrency === "USD" ? "Сумма возврата, $" : "Сумма возврата, ₽"}
                    value={returnDrafts[drawerData.id] || ""}
                    onChange={(event) => setReturnDrafts((current) => ({ ...current, [drawerData.id]: event.target.value }))}
                  />
                  <input
                    placeholder="Комментарий"
                    value={returnNotes[drawerData.id] || ""}
                    onChange={(event) => setReturnNotes((current) => ({ ...current, [drawerData.id]: event.target.value }))}
                  />
                  <button
                    className="primary-action"
                    type="button"
                    disabled={returnSupplier.isPending || !(Number(returnDrafts[drawerData.id]) > 0)}
                    onClick={() => returnSupplier.mutate({ supplier: drawerData.supplier, amount: drawerData.supplierCurrency === "USD" ? Math.round(Number(returnDrafts[drawerData.id] || 0) * usdRate) : Number(returnDrafts[drawerData.id] || 0), note: returnNotes[drawerData.id] || "" })}
                  >
                    {returnSupplier.isPending ? <Loader2 className="spin" size={16} /> : <RotateCcw size={16} />} Возврат
                  </button>
                </div>
              </details>
            </div>

            {/* Articles */}
            <div className="supplier-articles">
              <strong>Артикулы поставщика</strong>
              {drawerData.articles.map((article) => {
                const articleId = String(article.id || article.article || "");
                return (
                  <div className="supplier-article-row" key={articleId}>
                    <div>
                      <span>{String(article.article || "-")}</span>
                      <small>{String(article.note || "")}</small>
                    </div>
                    <button className="icon-action" type="button" title="Редактировать" onClick={() => setArticleDraft(drawerData.id, { id: articleId, article: String(article.article || ""), note: String(article.note || "") })}><Edit3 size={15} /></button>
                    <button className="icon-action danger-action" type="button" title="Удалить" onClick={() => deleteArticle.mutate({ supplierIdValue: drawerData.id, articleId })}><Trash2 size={15} /></button>
                  </div>
                );
              })}
              {!drawerData.articles.length ? <small className="muted-line">Артикулы пока не добавлены.</small> : null}
              <form
                className="supplier-article-form"
                onSubmit={(event) => {
                  event.preventDefault();
                  if (!drawerData.draft.article.trim()) return;
                  saveArticle.mutate({ supplierIdValue: drawerData.id, draft: drawerData.draft });
                }}
              >
                <input value={drawerData.draft.article} onChange={(event) => setArticleDraft(drawerData.id, { article: event.target.value })} placeholder="Артикул" />
                <input value={drawerData.draft.note} onChange={(event) => setArticleDraft(drawerData.id, { note: event.target.value })} placeholder="Заметка" />
                <button className="secondary-action" type="submit" disabled={saveArticle.isPending || !drawerData.draft.article.trim()}>{drawerData.draft.id ? "Сохранить" : "Добавить"}</button>
                {drawerData.draft.id ? <button className="icon-action" type="button" title="Отмена" onClick={() => setArticleDraft(drawerData.id, { id: undefined, article: "", note: "" })}><X size={16} /></button> : null}
              </form>
            </div>
          </aside>
        </div>
      ) : null}

      {/* Inactive modal */}
      {inactiveDraft ? (
        <div className="supplier-modal-backdrop" onClick={() => setInactiveDraft(null)}>
          <section className="supplier-inactive-modal" onClick={(e) => e.stopPropagation()}>
            <div className="section-title">
              <div><span>Поставщик</span><h3>Временно не работает</h3></div>
              <button className="icon-action" type="button" onClick={() => setInactiveDraft(null)}><X size={16} /></button>
            </div>
            <p className="settings-hint">{inactiveDraft.supplier.name || supplierId(inactiveDraft.supplier)}</p>
            <label>Комментарий<textarea value={inactiveDraft.comment} onChange={(event) => setInactiveDraft({ ...inactiveDraft, comment: event.target.value })} /></label>
            <label className="toggle-line">
              <input type="checkbox" checked={inactiveDraft.inactiveUntilUnknown} onChange={(event) => setInactiveDraft({ ...inactiveDraft, inactiveUntilUnknown: event.target.checked })} />
              <span>срок неизвестен</span>
            </label>
            {!inactiveDraft.inactiveUntilUnknown ? <label>Вернется<input type="date" value={inactiveDraft.inactiveUntil} onChange={(event) => setInactiveDraft({ ...inactiveDraft, inactiveUntil: event.target.value })} /></label> : null}
            <div className="row-actions">
              <button className="secondary-action" type="button" onClick={() => setInactiveDraft({ ...inactiveDraft, inactiveUntil: dateInput(7), inactiveUntilUnknown: false })}>+7 дней</button>
              <button className="secondary-action" type="button" onClick={() => setInactiveDraft({ ...inactiveDraft, inactiveUntil: dateInput(14), inactiveUntilUnknown: false })}>+14 дней</button>
              <button className="secondary-action" type="button" onClick={() => setInactiveDraft({ ...inactiveDraft, inactiveUntil: endOfMonthInput(), inactiveUntilUnknown: false })}>до конца месяца</button>
            </div>
            <div className="row-actions">
              <button className="primary-action" type="button" disabled={patchSupplier.isPending} onClick={submitInactive}>
                {patchSupplier.isPending ? <Loader2 className="spin" size={16} /> : <Truck size={16} />} Сохранить остановку
              </button>
              <button className="secondary-action" type="button" onClick={() => setInactiveDraft(null)}>Отмена</button>
            </div>
          </section>
        </div>
      ) : null}
    </section>
  );
}
