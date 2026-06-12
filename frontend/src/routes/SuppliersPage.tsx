import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Boxes, CheckCircle2, Edit3, Loader2, Package, Plus, RefreshCw, Search, Trash2, Truck, UserX, X } from "lucide-react";
import { useMemo, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { SupplierLedgerPaymentSchema, SupplierSchema, SuppliersResponseSchema } from "../types";
import { asRecord, compactDate, errorMessage, numberValue } from "../lib/common";

type Supplier = z.infer<typeof SupplierSchema>;

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

const moneySigned = (value: unknown) => {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n === 0) return "0 ₽";
  const sign = n > 0 ? "+" : "-";
  return `${sign}${Math.round(Math.abs(n)).toLocaleString("ru-RU")} ₽`;
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
  const [form, setForm] = useState<SupplierForm>(emptySupplierForm);
  const [articleDrafts, setArticleDrafts] = useState<Record<string, ArticleDraft>>({});
  const [inactiveDraft, setInactiveDraft] = useState<InactiveDraft | null>(null);
  const [paymentDrafts, setPaymentDrafts] = useState<Record<string, string>>({});
  const [paymentNotes, setPaymentNotes] = useState<Record<string, string>>({});

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
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["finance"] });
    },
  });

  const suppliers = suppliersQuery.data?.suppliers || [];
  const sync = asRecord(suppliersQuery.data?.supplierSync);
  const activeCount = suppliers.filter(supplierIsActive).length;
  const inactiveCount = suppliers.length - activeCount;
  const articleCount = suppliers.reduce((sum, supplier) => sum + supplierArticles(supplier).length, 0);
  const affectedCount = suppliers.reduce((sum, supplier) => sum + numberValue(supplier.impactProductCount, 0), 0);

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return suppliers
      .filter((supplier) => view === "active" ? supplierIsActive(supplier) : !supplierIsActive(supplier))
      .filter((supplier) => !needle || supplierSearchText(supplier).includes(needle))
      .sort((a, b) => String(a.name || "").localeCompare(String(b.name || ""), "ru"));
  }, [search, suppliers, view]);

  const startEdit = (supplier: Supplier) => {
    setForm({
      id: supplierId(supplier),
      name: supplier.name || "",
      note: String(asRecord(supplier).note || ""),
      stopReason: String(supplier.stopReason || ""),
      priceCurrency: String(asRecord(supplier).priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD",
    });
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

  const anyError = suppliersQuery.error || refreshMutation.error || saveSupplier.error || patchSupplier.error || deleteSupplier.error || saveArticle.error || deleteArticle.error || paySupplier.error;

  return (
    <section className="page-section suppliers-page">
      <PageHeader
        title="Поставщики"
        subtitle="Полная карточка поставщика из legacy: импорт из PriceMaster, валюта, остановка, возврат, статьи и влияние на товары."
        action={(
          <button className="primary-action" type="button" disabled={refreshMutation.isPending} onClick={() => refreshMutation.mutate()}>
            {refreshMutation.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Загрузить из PriceMaster
          </button>
        )}
      />

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
              <select value={form.priceCurrency} onChange={(event) => setForm({ ...form, priceCurrency: event.target.value })}>
                <option value="USD">Доллары (USD) — цена × курс × наценка</option>
                <option value="RUB">Рубли (RUB) — цена × наценка</option>
              </select>
            </label>
            <div className="row-actions">
              <button className="primary-action" type="submit" disabled={saveSupplier.isPending || !form.name.trim()}>
                {saveSupplier.isPending ? <Loader2 className="spin" size={16} /> : <Plus size={16} />} Сохранить
              </button>
              {form.id ? <button className="secondary-action" type="button" onClick={() => setForm(emptySupplierForm)}><X size={16} /> Отмена</button> : null}
            </div>
          </form>
        </div>

        <div className="settings-panel supplier-list-panel">
          <div className="section-title">
            <div><span>Список</span><h3>Все поставщики</h3></div>
            <button className="secondary-action" type="button" disabled={suppliersQuery.isFetching} onClick={() => suppliersQuery.refetch()}>
              {suppliersQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
            </button>
          </div>
          <div className="supplier-toolbar">
            <div className="settings-tabs">
              <button className={view === "active" ? "is-active" : ""} type="button" onClick={() => setView("active")}>Активные</button>
              <button className={view === "inactive" ? "is-active" : ""} type="button" onClick={() => setView("inactive")}>Остановленные</button>
            </div>
            <label className="supplier-search"><Search size={16} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Поиск по названию, partnerId, артикулу" /></label>
          </div>

          <div className="supplier-card-list">
            {suppliersQuery.isLoading ? <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю поставщиков...</div> : null}
            {!suppliersQuery.isLoading && !filtered.length ? <div className="soft-empty">Поставщики не найдены.</div> : null}
            {filtered.map((supplier) => {
              const id = supplierId(supplier);
              const raw = asRecord(supplier);
              const articles = supplierArticles(supplier);
              const draft = articleDrafts[id] || { article: "", note: "" };
              const ledger = asRecord(raw.ledger);
              const balance = Number(ledger.balance || 0);
              const paymentAmount = paymentDrafts[id] || "";
              const paymentNote = paymentNotes[id] || "";
              const active = supplierIsActive(supplier);
              const stockOnly = supplier.pricingMode === "stock_only" || supplier.stockOnly === true;
              return (
                <article className={`supplier-modern-card ${active ? "" : "is-inactive"}`} key={id}>
                  <div className="supplier-card-head">
                    <div>
                      <span>{supplier.partnerId ? `partner ${supplier.partnerId}` : "local"}</span>
                      <h3>{supplier.name || "Поставщик"}</h3>
                    </div>
                    <div className="supplier-status-pill">{active ? <CheckCircle2 size={15} /> : <UserX size={15} />}{active ? "активен" : "остановлен"}</div>
                  </div>

                  <div className="supplier-badge-row">
                    <label className="supplier-currency-inline">
                      <span>Закупка</span>
                      <select
                        value={String(raw.priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD"}
                        disabled={patchSupplier.isPending}
                        onChange={(event) => patchSupplier.mutate({ id, patch: { priceCurrency: event.target.value } })}
                      >
                        <option value="USD">USD</option>
                        <option value="RUB">RUB</option>
                      </select>
                    </label>
                    <span>товаров {supplier.impactProductCount || 0}</span>
                    <span>доверие {supplier.trustFactor ?? 100}</span>
                    {supplier.orderCutoffTime ? <span>до {supplier.orderCutoffTime}</span> : null}
                    {supplier.reseller ? <span>перекупщик</span> : null}
                    {stockOnly ? <span className="warning-badge">не берет цену</span> : null}
                  </div>

                  <div className="summary-grid compact-summary supplier-ledger-strip">
                    <DiagnosticValue label={balance < 0 ? "Долг поставщику" : "Аванс / баланс"} value={moneySigned(balance)} tone={balance < 0 ? "danger" : balance > 0 ? "success" : ""} />
                    <DiagnosticValue label="Собрано в долг" value={moneySigned(-Number(ledger.debtTotal || 0))} />
                    <DiagnosticValue label="Оплачено" value={moneySigned(Number(ledger.paidTotal || 0))} tone={Number(ledger.paidTotal || 0) ? "success" : ""} />
                    <DiagnosticValue label="Последняя оплата" value={ledger.lastPaymentAt ? compactDate(String(ledger.lastPaymentAt)) : "-"} />
                  </div>
                  <div className="settings-form-row supplier-payment-row">
                    <input
                      type="number"
                      min="0"
                      step="0.01"
                      placeholder="Сумма оплаты, ₽"
                      value={paymentAmount}
                      onChange={(event) => setPaymentDrafts((current) => ({ ...current, [id]: event.target.value }))}
                    />
                    <input
                      placeholder="Комментарий"
                      value={paymentNote}
                      onChange={(event) => setPaymentNotes((current) => ({ ...current, [id]: event.target.value }))}
                    />
                    <button
                      className="primary-action"
                      type="button"
                      disabled={paySupplier.isPending || !(Number(paymentAmount) > 0)}
                      onClick={() => paySupplier.mutate({ supplier, amount: Number(paymentAmount || 0), note: paymentNote })}
                    >
                      {paySupplier.isPending ? <Loader2 className="spin" size={16} /> : <CheckCircle2 size={16} />} Заплатил
                    </button>
                  </div>

                  {raw.note ? <p className="supplier-note">{String(raw.note)}</p> : null}
                  {!active ? <p className="supplier-note danger-text">Остановлен {inactiveText(supplier)}. {String(raw.inactiveComment || supplier.stopReason || "")}</p> : null}

                  <div className="row-actions">
                    <button className="secondary-action" type="button" onClick={() => startEdit(supplier)}><Edit3 size={16} /> Редактировать</button>
                    {active ? (
                      <button className="secondary-action danger-action" type="button" onClick={() => startInactive(supplier)}><UserX size={16} /> Не работает</button>
                    ) : (
                      <button className="secondary-action" type="button" disabled={patchSupplier.isPending} onClick={() => patchSupplier.mutate({ id, patch: { stopped: false } })}><CheckCircle2 size={16} /> Вернуть</button>
                    )}
                    <button
                      className="secondary-action danger-action"
                      type="button"
                      disabled={deleteSupplier.isPending}
                      onClick={() => {
                        if (window.confirm(`Удалить поставщика ${supplier.name || id}?`)) deleteSupplier.mutate(id);
                      }}
                    >
                      <Trash2 size={16} /> Удалить
                    </button>
                  </div>

                  <div className="supplier-articles">
                    <strong>Артикулы поставщика</strong>
                    {articles.map((article) => {
                      const articleId = String(article.id || article.article || "");
                      return (
                        <div className="supplier-article-row" key={articleId}>
                          <div>
                            <span>{String(article.article || "-")}</span>
                            <small>{String(article.note || "")}</small>
                          </div>
                          <button className="icon-action" type="button" title="Редактировать" onClick={() => setArticleDraft(id, { id: articleId, article: String(article.article || ""), note: String(article.note || "") })}><Edit3 size={15} /></button>
                          <button className="icon-action danger-action" type="button" title="Удалить" onClick={() => deleteArticle.mutate({ supplierIdValue: id, articleId })}><Trash2 size={15} /></button>
                        </div>
                      );
                    })}
                    {!articles.length ? <small className="muted-line">Артикулы пока не добавлены.</small> : null}
                    <form
                      className="supplier-article-form"
                      onSubmit={(event) => {
                        event.preventDefault();
                        if (!draft.article.trim()) return;
                        saveArticle.mutate({ supplierIdValue: id, draft });
                      }}
                    >
                      <input value={draft.article} onChange={(event) => setArticleDraft(id, { article: event.target.value })} placeholder="Артикул" />
                      <input value={draft.note} onChange={(event) => setArticleDraft(id, { note: event.target.value })} placeholder="Заметка" />
                      <button className="secondary-action" type="submit" disabled={saveArticle.isPending || !draft.article.trim()}>{draft.id ? "Сохранить" : "Добавить"}</button>
                      {draft.id ? <button className="icon-action" type="button" title="Отмена" onClick={() => setArticleDraft(id, { id: undefined, article: "", note: "" })}><X size={16} /></button> : null}
                    </form>
                  </div>
                </article>
              );
            })}
          </div>
        </div>
      </section>

      {inactiveDraft ? (
        <div className="supplier-modal-backdrop">
          <section className="supplier-inactive-modal">
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

      {anyError ? <div className="inline-error">{errorMessage(anyError)}</div> : null}
    </section>
  );
}
