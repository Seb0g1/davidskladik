import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Check, FilePen, FilePlus2, FileText, Loader2, Plus, RotateCcw, Save, Search, Trash2, Undo2, Wallet, X } from "lucide-react";
import { fetchJson, mutationBody, patchBody } from "../api";
import { ConsignmentInvoice, ConsignmentInvoicesSchema, ConsignmentPmNomenclatureSchema } from "../types";
import { PageHeader } from "./PageHeader";
import { ListSkeleton } from "./Skeleton";
import { errorMessage } from "../lib/common";

// Приходные накладные реализации. Проведённая накладная создаёт операции прихода от спонсора
// или закупки с баланса; её можно снять с проводки (операции удаляются, остатки и баланс
// откатываются), поправить как черновик и провести заново.

const money = (value: unknown) => `${(Math.round(Number(value || 0) * 100) / 100).toLocaleString("ru-RU")} $`;
const dateText = (value: string | null | undefined) => {
  if (!value) return "-";
  const date = new Date(value);
  return Number.isFinite(date.getTime())
    ? `${date.toLocaleDateString("ru-RU")} ${date.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}`
    : value;
};

type Line = { key: string; itemId: string | null; name: string; article: string; quantity: string; unitPrice: string };
type Form = { supplierName: string; note: string; fromBalance: boolean; lines: Line[] };

const emptyForm: Form = { supplierName: "", note: "", fromBalance: false, lines: [] };
const newKey = () => (globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`);

const formFromInvoice = (invoice: ConsignmentInvoice): Form => ({
  supplierName: invoice.supplierName || "",
  note: invoice.note || "",
  fromBalance: Boolean(invoice.fromBalance),
  lines: (invoice.items || []).map((item) => ({
    key: item.id || newKey(),
    itemId: item.itemId || null,
    name: item.name,
    article: item.article || "",
    quantity: String(item.quantity),
    unitPrice: String(item.unitPrice),
  })),
});

const linesPayload = (lines: Line[]) => lines
  .filter((line) => line.name.trim())
  .map((line) => ({
    itemId: line.itemId,
    name: line.name.trim(),
    article: line.article.trim() || null,
    quantity: Math.max(1, Math.round(Number(line.quantity) || 1)),
    unitPrice: Math.max(0, Number(line.unitPrice) || 0),
  }));

const formTotals = (lines: Line[]) => lines.reduce(
  (acc, line) => ({ quantity: acc.quantity + (Number(line.quantity) || 0), amount: acc.amount + (Number(line.quantity) || 0) * (Number(line.unitPrice) || 0) }),
  { quantity: 0, amount: 0 },
);

export function ConsignmentInvoices({ onBack }: { onBack: () => void }) {
  const queryClient = useQueryClient();
  // "new" — новая накладная, иначе id выбранной.
  const [selectedId, setSelectedId] = useState<string>("new");
  const [form, setForm] = useState<Form>(emptyForm);
  const [dirty, setDirty] = useState(false);
  const [statusFilter, setStatusFilter] = useState<"all" | "posted" | "draft">("all");
  const [pmQuery, setPmQuery] = useState("");
  const [pmPage, setPmPage] = useState(1);
  const [notice, setNotice] = useState("");

  const invoices = useQuery({
    queryKey: ["consignment", "invoices", "all"],
    queryFn: () => fetchJson("/api/consignment/invoices?limit=200", ConsignmentInvoicesSchema),
  });
  const pmNomenclature = useQuery({
    queryKey: ["consignment", "invoice-pm-nomenclature", pmQuery, pmPage],
    queryFn: () => fetchJson(`/api/consignment/pm-nomenclature?q=${encodeURIComponent(pmQuery)}&page=${pmPage}&limit=30`, ConsignmentPmNomenclatureSchema),
  });

  const list = invoices.data?.invoices || [];
  const selected = selectedId === "new" ? null : list.find((invoice) => invoice.id === selectedId) || null;
  const editable = !selected || selected.status === "draft";

  // Выбранная накладная загружается в форму, пока пользователь её не правит.
  useEffect(() => {
    if (selected && !dirty) setForm(formFromInvoice(selected));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected?.id, selected?.updatedAt, selected?.status]);

  const counts = useMemo(() => ({
    all: list.length,
    posted: list.filter((invoice) => invoice.status !== "draft").length,
    draft: list.filter((invoice) => invoice.status === "draft").length,
  }), [list]);
  const visible = list.filter((invoice) => statusFilter === "all" || (statusFilter === "draft" ? invoice.status === "draft" : invoice.status !== "draft"));

  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: ["consignment"] });
  };
  const afterSave = (invoice: ConsignmentInvoice | undefined, message: string) => {
    setDirty(false);
    setNotice(message);
    if (invoice) {
      setSelectedId(invoice.id);
      setForm(formFromInvoice(invoice));
    }
    refresh();
  };

  const body = () => ({ supplierName: form.supplierName, note: form.note, fromBalance: form.fromBalance, items: linesPayload(form.lines) });
  const create = useMutation({
    mutationFn: (post: boolean) => fetchJson("/api/consignment/invoices", ConsignmentInvoicesSchema, mutationBody({ ...body(), post })),
    onSuccess: (data, post) => afterSave(data.invoices[0], post ? "Накладная проведена: остатки обновлены." : "Черновик сохранён."),
  });
  const save = useMutation({
    mutationFn: (id: string) => fetchJson(`/api/consignment/invoices/${encodeURIComponent(id)}`, ConsignmentInvoicesSchema, patchBody(body())),
    onSuccess: (data) => afterSave(data.invoices[0], "Изменения сохранены."),
  });
  const saveAndPost = useMutation({
    mutationFn: async (id: string) => {
      if (dirty) await fetchJson(`/api/consignment/invoices/${encodeURIComponent(id)}`, ConsignmentInvoicesSchema, patchBody(body()));
      return fetchJson(`/api/consignment/invoices/${encodeURIComponent(id)}/post`, ConsignmentInvoicesSchema, mutationBody({}));
    },
    onSuccess: (data) => afterSave(data.invoices[0], "Накладная проведена: остатки и баланс обновлены."),
  });
  const unpost = useMutation({
    mutationFn: (id: string) => fetchJson(`/api/consignment/invoices/${encodeURIComponent(id)}/unpost`, ConsignmentInvoicesSchema, mutationBody({})),
    onSuccess: (data) => afterSave(data.invoices[0], "Накладная снята с проводки — теперь её можно править."),
  });
  const remove = useMutation({
    mutationFn: (id: string) => fetchJson(`/api/consignment/invoices/${encodeURIComponent(id)}`, ConsignmentInvoicesSchema, { method: "DELETE" }),
    onSuccess: () => {
      setSelectedId("new");
      setForm(emptyForm);
      setDirty(false);
      setNotice("Черновик удалён.");
      refresh();
    },
  });
  const busy = create.isPending || save.isPending || saveAndPost.isPending || unpost.isPending || remove.isPending;
  const error = create.error || save.error || saveAndPost.error || unpost.error || remove.error;

  const resetMutations = () => {
    for (const mutation of [create, save, saveAndPost, unpost, remove]) mutation.reset();
  };
  const select = (id: string) => {
    if (dirty && !window.confirm("Есть несохранённые изменения. Перейти без сохранения?")) return;
    resetMutations();
    setNotice("");
    setDirty(false);
    setSelectedId(id);
    if (id === "new") setForm(emptyForm);
  };
  const edit = (patch: Partial<Form>) => {
    setForm((current) => ({ ...current, ...patch }));
    setDirty(true);
    setNotice("");
  };
  const editLine = (key: string, patch: Partial<Line>) => edit({ lines: form.lines.map((line) => (line.key === key ? { ...line, ...patch } : line)) });
  const addLine = (line: Omit<Line, "key">) => edit({ lines: [...form.lines, { ...line, key: newKey() }] });

  const totals = formTotals(form.lines);
  const hasLines = form.lines.some((line) => line.name.trim());

  return (
    <section className="page-section consignment-page">
      <PageHeader
        title="Приходные накладные"
        subtitle="Приход товара от спонсора или закупка с общего баланса. Проведённую накладную можно снять с проводки, исправить и провести заново."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" onClick={onBack}><ArrowLeft size={16} /> К реализации</button>
            <button className="primary-action" type="button" onClick={() => select("new")}><FilePlus2 size={16} /> Новая накладная</button>
          </div>
        )}
      />

      <div className="cn-inv-layout">
        <aside className="cn-inv-list table-panel">
          <div className="cn-inv-tabs" role="tablist" aria-label="Статус накладных">
            {([["all", "Все"], ["posted", "Проведены"], ["draft", "Черновики"]] as const).map(([value, label]) => (
              <button key={value} type="button" role="tab" aria-selected={statusFilter === value} className={statusFilter === value ? "is-active" : ""} onClick={() => setStatusFilter(value)}>
                {label} <span>{counts[value]}</span>
              </button>
            ))}
          </div>
          {invoices.isLoading ? <ListSkeleton rows={6} /> : null}
          {invoices.error ? <div className="inline-error">{errorMessage(invoices.error)}</div> : null}
          <button type="button" className={`cn-inv-card cn-inv-card-new${selectedId === "new" ? " is-selected" : ""}`} onClick={() => select("new")}>
            <FilePlus2 size={16} /> <span>Новая накладная</span>
          </button>
          {visible.map((invoice) => {
            const quantity = (invoice.items || []).reduce((sum, item) => sum + item.quantity, 0);
            return (
              <button key={invoice.id} type="button" className={`cn-inv-card${selectedId === invoice.id ? " is-selected" : ""}`} onClick={() => select(invoice.id)}>
                <span className="cn-inv-card-top">
                  <strong>{invoice.number}</strong>
                  <span className={`pill ${invoice.status === "draft" ? "warn" : "ok"}`}>{invoice.status === "draft" ? "Черновик" : "Проведена"}</span>
                </span>
                <span className="cn-inv-card-meta">
                  {dateText(invoice.createdAt)}{invoice.supplierName ? ` · ${invoice.supplierName}` : ""}
                </span>
                <span className="cn-inv-card-sum">
                  <span>{(invoice.items || []).length} поз. · {quantity} шт</span>
                  <strong>{money(invoice.totalAmount)}</strong>
                </span>
                {invoice.fromBalance ? <span className="cn-inv-card-flag"><Wallet size={12} /> с баланса</span> : null}
              </button>
            );
          })}
          {!invoices.isLoading && !visible.length ? <div className="empty-state">Накладных нет.</div> : null}
        </aside>

        <div className="cn-inv-editor settings-panel">
          <div className="section-title">
            <div>
              <span>{selected ? (selected.status === "draft" ? "Черновик — можно править" : "Проведена") : "Новая накладная"}</span>
              <h3>
                {selected ? selected.number : "Приходная накладная"}
                {selected ? <span className={`pill ${selected.status === "draft" ? "warn" : "ok"}`}>{selected.status === "draft" ? "Черновик" : "Проведена"}</span> : null}
              </h3>
            </div>
            {selected ? (
              <span className="muted-note cn-inv-dates">
                Создана {dateText(selected.createdAt)}{selected.createdBy ? ` · ${selected.createdBy}` : ""}
                {selected.postedAt ? ` · проведена ${dateText(selected.postedAt)}` : ""}
              </span>
            ) : null}
          </div>

          {selected && selected.status !== "draft" ? (
            <div className="cn-inv-posted-note">
              <FileText size={16} />
              <span>
                Накладная проведена: товар {selected.fromBalance ? "закуплен с общего баланса" : "принят от спонсора"}. Чтобы изменить состав, цены или способ оплаты,
                снимите её с проводки — остатки и баланс откатятся, после правки проведите заново.
              </span>
            </div>
          ) : null}

          <div className="cn-inv-head-fields">
            <label>
              <span>Поставщик</span>
              <input disabled={!editable} placeholder="Необязательно" value={form.supplierName} onChange={(event) => edit({ supplierName: event.target.value })} />
            </label>
            <label>
              <span>Примечание</span>
              <input disabled={!editable} placeholder="Необязательно" value={form.note} onChange={(event) => edit({ note: event.target.value })} />
            </label>
            <div className="cn-inv-pay" role="radiogroup" aria-label="Оплата">
              <span>Оплата</span>
              <div className="cn-inv-seg">
                <button type="button" disabled={!editable} role="radio" aria-checked={!form.fromBalance} className={!form.fromBalance ? "is-active" : ""} onClick={() => edit({ fromBalance: false })}>
                  Товар спонсора
                </button>
                <button type="button" disabled={!editable} role="radio" aria-checked={form.fromBalance} className={form.fromBalance ? "is-active" : ""} onClick={() => edit({ fromBalance: true })}>
                  <Wallet size={14} /> Закупка с баланса
                </button>
              </div>
            </div>
          </div>

          <div className="cn-table-scroll">
            <table className="cn-lines-table">
              <thead>
                <tr>
                  <th className="cn-th-left">Товар</th>
                  <th className="cn-th-qty">Кол-во</th>
                  <th className="cn-th-price">Цена, $</th>
                  <th className="cn-th-price">Сумма</th>
                  {editable ? <th className="cn-th-del" /> : null}
                </tr>
              </thead>
              <tbody>
                {form.lines.map((line) => (
                  <tr key={line.key}>
                    <td className="cn-td-name">
                      {editable ? (
                        <input className="cn-td-name-input" value={line.name} placeholder="Наименование" onChange={(event) => editLine(line.key, { name: event.target.value })} />
                      ) : <div className="cn-td-name-text">{line.name}</div>}
                      {line.article ? <div className="cn-td-article">{line.article.replace(/^pm:/, "PM ")}</div> : null}
                    </td>
                    <td className="cn-td-center">
                      {editable
                        ? <input type="number" min="1" className="cn-td-input" value={line.quantity} onChange={(event) => editLine(line.key, { quantity: event.target.value })} />
                        : `${line.quantity} шт`}
                    </td>
                    <td className="cn-td-center">
                      {editable
                        ? <input type="number" min="0" step="0.01" placeholder="0.00" className="cn-td-input" value={line.unitPrice} onChange={(event) => editLine(line.key, { unitPrice: event.target.value })} />
                        : money(line.unitPrice)}
                    </td>
                    <td className="cn-td-center cn-td-sum">{money((Number(line.quantity) || 0) * (Number(line.unitPrice) || 0))}</td>
                    {editable ? (
                      <td>
                        <button className="icon-action" type="button" title="Удалить строку" onClick={() => edit({ lines: form.lines.filter((row) => row.key !== line.key) })}><X size={14} /></button>
                      </td>
                    ) : null}
                  </tr>
                ))}
                {!form.lines.length ? (
                  <tr><td colSpan={editable ? 5 : 4} className="cn-inv-empty-lines">{editable ? "Добавьте товары из номенклатуры ниже или строку вручную." : "В накладной нет строк."}</td></tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="cn-inv-total">
            <span>{form.lines.length} поз. · {totals.quantity} шт</span>
            <strong>Итого {money(totals.amount)}</strong>
          </div>

          {notice ? <div className="success-strip">{notice}</div> : null}
          {error ? <div className="inline-error">{errorMessage(error)}</div> : null}

          <div className="cn-inv-actions">
            {!selected ? (
              <>
                <button className="secondary-action" type="button" disabled={busy || !hasLines} onClick={() => create.mutate(false)}>
                  {create.isPending && create.variables === false ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить черновик
                </button>
                <button className="primary-action" type="button" disabled={busy || !hasLines} onClick={() => create.mutate(true)}>
                  {create.isPending && create.variables === true ? <Loader2 className="spin" size={16} /> : <Check size={16} />} Провести накладную
                </button>
              </>
            ) : selected.status === "draft" ? (
              <>
                <button
                  className="secondary-action danger"
                  type="button"
                  disabled={busy}
                  onClick={() => { if (window.confirm(`Удалить черновик ${selected.number}?`)) remove.mutate(selected.id); }}
                >
                  {remove.isPending ? <Loader2 className="spin" size={16} /> : <Trash2 size={16} />} Удалить черновик
                </button>
                {dirty ? (
                  <>
                    <button className="secondary-action" type="button" disabled={busy} onClick={() => { setForm(formFromInvoice(selected)); setDirty(false); }}>
                      <RotateCcw size={16} /> Отменить правки
                    </button>
                    <button className="secondary-action" type="button" disabled={busy || !hasLines} onClick={() => save.mutate(selected.id)}>
                      {save.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить
                    </button>
                  </>
                ) : null}
                <button className="primary-action" type="button" disabled={busy || !hasLines} onClick={() => saveAndPost.mutate(selected.id)}>
                  {saveAndPost.isPending ? <Loader2 className="spin" size={16} /> : <Check size={16} />} {dirty ? "Сохранить и провести" : "Провести"}
                </button>
              </>
            ) : (
              <button
                className="secondary-action"
                type="button"
                disabled={busy}
                onClick={() => {
                  if (window.confirm(`Снять ${selected.number} с проводки? Товар уйдёт со склада реализации${selected.fromBalance ? ", деньги вернутся на общий баланс" : ""}. После правки накладную можно провести снова.`)) {
                    unpost.mutate(selected.id);
                  }
                }}
              >
                {unpost.isPending ? <Loader2 className="spin" size={16} /> : <Undo2 size={16} />} Снять с проводки
              </button>
            )}
          </div>

          {editable ? (
            <div className="cn-inv-picker">
              <div className="cn-inv-picker-head">
                <h4><FilePen size={15} /> Добавить товары</h4>
                <button
                  className="secondary-action"
                  type="button"
                  onClick={() => addLine({ itemId: null, name: "", article: "", quantity: "1", unitPrice: "" })}
                >
                  <Plus size={14} /> Строка вручную
                </button>
              </div>
              <div className="cn-nm-panel">
                <div className="cn-nm-head">
                  <Search size={14} />
                  <input
                    placeholder="Поиск по номенклатуре PriceMaster (название или ID)"
                    value={pmQuery}
                    onChange={(event) => { setPmQuery(event.target.value); setPmPage(1); }}
                    className="cn-nm-search"
                  />
                </div>
                <div className="cn-nm-list">
                  {pmNomenclature.isLoading ? <div className="empty-state cn-empty-p12">Загрузка номенклатуры…</div> : null}
                  {(pmNomenclature.data?.items || []).map((product) => {
                    const article = `pm:${product.productId}`;
                    const added = form.lines.some((line) => line.article === article);
                    return (
                      <button
                        key={product.productId}
                        type="button"
                        className="cn-nm-row"
                        disabled={added}
                        onClick={() => addLine({ itemId: null, name: product.name, article, quantity: "1", unitPrice: String(product.purchasePrice || "") })}
                      >
                        <span className="cn-nm-id">{product.productId}</span>
                        <span className="cn-nm-name">{product.name || "-"}</span>
                        <span className="cn-nm-price">{money(product.purchasePrice)}</span>
                        {added ? <span className="cn-nm-added">✓ в накладной</span> : <span className="cn-nm-pick"><Plus size={12} /> добавить</span>}
                      </button>
                    );
                  })}
                  {!pmNomenclature.isLoading && !(pmNomenclature.data?.items || []).length ? <div className="empty-state cn-empty-p12">Ничего не найдено.</div> : null}
                </div>
                {(pmNomenclature.data?.total ?? 0) > 0 ? (
                  <div className="cn-nm-pager">
                    <span className="muted-note">Всего: {pmNomenclature.data?.total}</span>
                    <div className="row-actions">
                      <button className="secondary-action cn-nm-pager-btn" type="button" disabled={pmPage <= 1} onClick={() => setPmPage((page) => page - 1)}>← Пред.</button>
                      <span className="muted-note">стр. {pmPage}</span>
                      <button className="secondary-action cn-nm-pager-btn" type="button" disabled={!pmNomenclature.data?.hasMore} onClick={() => setPmPage((page) => page + 1)}>След. →</button>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}
