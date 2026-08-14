import { Fragment, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Banknote, Boxes, Check, ChevronDown, ChevronRight, HandCoins, History, ListPlus, Loader2, Package, PackageMinus, PackagePlus, Plus, RefreshCw, RotateCcw, Search, ShoppingCart, Trash2, TrendingUp, Upload, Wallet, X } from "lucide-react";
import { fetchJson, mutationBody, patchBody } from "../api";
import {
  ConsignmentBulkCreateSchema,
  ConsignmentGroupMutationSchema,
  ConsignmentItem,
  ConsignmentItemsSchema,
  ConsignmentMutationSchema,
  ConsignmentOperation,
  ConsignmentOperationsSchema,
  ConsignmentPmSearchSchema,
  ConsignmentPmSync,
  ConsignmentPmSyncSchema,
  ConsignmentSummarySchema,
  ConsignmentSuppliersSchema,
} from "../types";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { ListSkeleton } from "../components/Skeleton";
import { Stat } from "../components/Stat";
import { errorMessage } from "../lib/common";

// Все цены на странице реализации ведутся в долларах.
const money = (value: unknown) => {
  const n = Number(value || 0);
  return `${(Math.round(n * 100) / 100).toLocaleString("ru-RU")} $`;
};

const displayArticle = (article: string | null | undefined) =>
  article ? article.replace(/^pm:/, "") || "-" : "-";

const pmPriceText = (row: { price?: number | null; currency?: string }) => {
  if (row.price === null || row.price === undefined) return "-";
  const symbol = ["RUB", "RUR"].includes(String(row.currency || "USD").toUpperCase()) ? "₽" : "$";
  return `${(Math.round(Number(row.price) * 100) / 100).toLocaleString("ru-RU")} ${symbol}`;
};

const dateText = (value: unknown) => {
  const raw = String(value ?? "").trim();
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime())
    ? `${date.toLocaleDateString("ru-RU")} ${date.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}`
    : raw;
};

const OPERATION_LABELS: Record<string, string> = {
  receive: "Приход от спонсора",
  purchase: "Закупка с баланса",
  sale: "Продажа",
  writeoff: "Списание",
  return: "Возврат",
  sponsor_topup: "Пополнение баланса спонсора",
  sponsor_payout: "Выплата спонсору (с баланса)",
  sponsor_profit_payout: "Вывод профита спонсора",
  my_profit_payout: "Вывод моего профита",
};

// group задан — операция по всему товару сразу: продажа/списание распределяются
// по партиям FIFO на сервере, приход-докупка попадает в самую свежую партию.
type StockAction = { item: ConsignmentItem; mode: "sale" | "writeoff" | "receive"; group?: ItemGroup };

type ItemGroup = {
  key: string;
  article: string;
  items: ConsignmentItem[];
  quantity: number;
  avgPurchasePrice: number;
  avgSalePrice: number;
  purchaseTotal: number;
};

// Средняя цена взвешена по остатку партий; когда всё распродано — простое среднее.
const averagePrice = (items: ConsignmentItem[], pick: (item: ConsignmentItem) => number, totalQuantity: number) => {
  if (totalQuantity > 0) {
    return items.reduce((sum, item) => sum + pick(item) * item.quantity, 0) / totalQuantity;
  }
  return items.length ? items.reduce((sum, item) => sum + pick(item), 0) / items.length : 0;
};

const groupItemsByArticle = (list: ConsignmentItem[]): ItemGroup[] => {
  const groups = new Map<string, ConsignmentItem[]>();
  for (const item of list) {
    const article = item.article.trim().toLowerCase();
    const key = article ? `article:${article}` : `single:${item.id}`;
    const bucket = groups.get(key);
    if (bucket) bucket.push(item);
    else groups.set(key, [item]);
  }
  return Array.from(groups.entries()).map(([key, items]) => {
    const quantity = items.reduce((sum, item) => sum + item.quantity, 0);
    return {
      key,
      article: items[0].article,
      items,
      quantity,
      avgPurchasePrice: averagePrice(items, (item) => item.purchasePrice, quantity),
      avgSalePrice: averagePrice(items, (item) => item.salePrice, quantity),
      purchaseTotal: items.reduce((sum, item) => sum + item.purchasePrice * item.quantity, 0),
    };
  });
};

const operationPrice = (op: ConsignmentOperation) => (op.type === "sale" || op.type === "return" ? op.unitSale : op.unitPurchase);

const OPERATION_SORT_OPTIONS = [
  { value: "date_desc", label: "Сначала новые" },
  { value: "date_asc", label: "Сначала старые" },
  { value: "name_asc", label: "Товар А→Я" },
  { value: "name_desc", label: "Товар Я→А" },
  { value: "price_desc", label: "Цена: по убыванию" },
  { value: "price_asc", label: "Цена: по возрастанию" },
  { value: "qty_desc", label: "Кол-во: по убыванию" },
  { value: "qty_asc", label: "Кол-во: по возрастанию" },
  { value: "type_asc", label: "По типу операции" },
];

const sortOperations = (list: ConsignmentOperation[], sort: string): ConsignmentOperation[] => {
  const time = (op: ConsignmentOperation) => {
    const value = new Date(op.createdAt || 0).getTime();
    return Number.isFinite(value) ? value : 0;
  };
  const byName = (a: ConsignmentOperation, b: ConsignmentOperation) =>
    String(a.itemName || "").localeCompare(String(b.itemName || ""), "ru");
  const byLabel = (op: ConsignmentOperation) => OPERATION_LABELS[op.type] || op.type;
  const sorted = [...list];
  switch (sort) {
    case "date_asc": sorted.sort((a, b) => time(a) - time(b)); break;
    case "name_asc": sorted.sort((a, b) => byName(a, b) || time(b) - time(a)); break;
    case "name_desc": sorted.sort((a, b) => byName(b, a) || time(b) - time(a)); break;
    case "price_desc": sorted.sort((a, b) => operationPrice(b) - operationPrice(a) || time(b) - time(a)); break;
    case "price_asc": sorted.sort((a, b) => operationPrice(a) - operationPrice(b) || time(b) - time(a)); break;
    case "qty_desc": sorted.sort((a, b) => b.quantity - a.quantity || time(b) - time(a)); break;
    case "qty_asc": sorted.sort((a, b) => a.quantity - b.quantity || time(b) - time(a)); break;
    case "type_asc": sorted.sort((a, b) => byLabel(a).localeCompare(byLabel(b), "ru") || time(b) - time(a)); break;
    default: sorted.sort((a, b) => time(b) - time(a));
  }
  return sorted;
};

const emptyAddForm = {
  name: "",
  article: "",
  supplierName: "",
  purchasePrice: "",
  salePrice: "",
  quantity: "1",
  note: "",
  fromBalance: false,
};

type DraftItem = typeof emptyAddForm & { draftId: string };

export function ConsignmentPage() {
  const queryClient = useQueryClient();
  const [itemSearch, setItemSearch] = useState("");
  const [addForm, setAddForm] = useState(emptyAddForm);
  const [pmQuery, setPmQuery] = useState("");
  const [pmOpen, setPmOpen] = useState(false);
  const [action, setAction] = useState<StockAction | null>(null);
  const [actionForm, setActionForm] = useState({ quantity: "1", price: "", note: "", fromBalance: false });
  const [priceDrafts, setPriceDrafts] = useState<Record<string, string>>({});
  const [payoutForm, setPayoutForm] = useState({ kind: "sponsor_payout", amount: "", note: "" });
  const [topupForm, setTopupForm] = useState({ amount: "", note: "" });
  const [opFilter, setOpFilter] = useState("all");
  const [opSearch, setOpSearch] = useState("");
  const [opSort, setOpSort] = useState("date_desc");
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});
  const [draftItems, setDraftItems] = useState<DraftItem[]>([]);
  const [pmSyncResult, setPmSyncResult] = useState<ConsignmentPmSync | null>(null);

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ["consignment"] });

  const summary = useQuery({
    queryKey: ["consignment", "summary"],
    queryFn: () => fetchJson("/api/consignment/summary", ConsignmentSummarySchema),
  });
  const items = useQuery({
    queryKey: ["consignment", "items", itemSearch],
    queryFn: () => fetchJson(`/api/consignment/items?q=${encodeURIComponent(itemSearch)}&limit=500`, ConsignmentItemsSchema),
  });
  const operations = useQuery({
    queryKey: ["consignment", "operations", opFilter],
    queryFn: () => fetchJson(`/api/consignment/operations?type=${encodeURIComponent(opFilter)}&limit=300`, ConsignmentOperationsSchema),
  });
  const suppliers = useQuery({
    queryKey: ["consignment", "suppliers"],
    queryFn: () => fetchJson("/api/consignment/suppliers", ConsignmentSuppliersSchema),
  });
  const pmSearch = useQuery({
    queryKey: ["consignment", "pm-search", pmQuery],
    queryFn: () => fetchJson(`/api/consignment/pm-search?q=${encodeURIComponent(pmQuery)}`, ConsignmentPmSearchSchema),
    enabled: pmQuery.trim().length >= 2,
  });

  const createItem = useMutation({
    mutationFn: () => fetchJson("/api/consignment/items", ConsignmentMutationSchema, mutationBody({
      name: addForm.name,
      article: addForm.article,
      supplierName: addForm.supplierName,
      purchasePrice: Number(addForm.purchasePrice || 0),
      salePrice: Number(addForm.salePrice || 0),
      quantity: Number(addForm.quantity || 0),
      note: addForm.note,
      fromBalance: addForm.fromBalance,
    })),
    onSuccess: () => {
      setAddForm(emptyAddForm);
      setPmQuery("");
      invalidate();
    },
  });

  const bulkCreate = useMutation({
    mutationFn: (drafts: DraftItem[]) => fetchJson("/api/consignment/items/bulk", ConsignmentBulkCreateSchema, mutationBody({
      items: drafts.map((draft) => ({
        name: draft.name,
        article: draft.article,
        supplierName: draft.supplierName,
        purchasePrice: Number(draft.purchasePrice || 0),
        salePrice: Number(draft.salePrice || 0),
        quantity: Number(draft.quantity || 0),
        note: draft.note,
        fromBalance: draft.fromBalance,
      })),
    })),
    onSuccess: () => {
      setDraftItems([]);
      invalidate();
    },
  });

  const addToDraft = () => {
    if (!addForm.name.trim()) return;
    setDraftItems((current) => [...current, { ...addForm, draftId: crypto.randomUUID() }]);
    setAddForm(emptyAddForm);
    setPmQuery("");
  };

  const savePrice = useMutation({
    mutationFn: ({ id, salePrice }: { id: string; salePrice: number }) =>
      fetchJson(`/api/consignment/items/${encodeURIComponent(id)}`, ConsignmentMutationSchema, patchBody({ salePrice })),
    onSuccess: (_data, variables) => {
      setPriceDrafts((current) => {
        const next = { ...current };
        delete next[variables.id];
        return next;
      });
      invalidate();
    },
  });

  const stockOperation = useMutation({
    mutationFn: ({ item, mode, group }: StockAction) => {
      const payload = {
        quantity: Number(actionForm.quantity || 0),
        note: actionForm.note,
        ...(mode === "sale" && actionForm.price !== "" ? { salePrice: Number(actionForm.price) } : {}),
        ...(mode === "receive" && actionForm.price !== "" ? { purchasePrice: Number(actionForm.price) } : {}),
        ...(mode === "receive" ? { fromBalance: actionForm.fromBalance } : {}),
      };
      return group
        ? fetchJson(`/api/consignment/groups/${mode}`, ConsignmentGroupMutationSchema, mutationBody({
          ...payload,
          itemIds: group.items.map((lot) => lot.id),
        }))
        : fetchJson(`/api/consignment/items/${encodeURIComponent(item.id)}/${mode}`, ConsignmentMutationSchema, mutationBody(payload));
    },
    onSuccess: () => {
      setAction(null);
      setActionForm({ quantity: "1", price: "", note: "", fromBalance: false });
      invalidate();
    },
  });

  const returnSale = useMutation({
    mutationFn: ({ id, note }: { id: string; note: string }) =>
      fetchJson(`/api/consignment/operations/${encodeURIComponent(id)}/return`, ConsignmentMutationSchema, mutationBody({ note })),
    onSuccess: invalidate,
  });

  const deleteOperation = useMutation({
    mutationFn: (id: string) =>
      fetchJson(`/api/consignment/operations/${encodeURIComponent(id)}`, ConsignmentMutationSchema, { method: "DELETE" }),
    onSuccess: invalidate,
  });

  // Подстраница операций: /app/consignment/operations (не в сайдбаре).
  const [view, setView] = useState<"main" | "operations">(() => (window.location.pathname.includes("/operations") ? "operations" : "main"));
  useEffect(() => {
    const onPop = () => setView(window.location.pathname.includes("/operations") ? "operations" : "main");
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const openOperations = () => {
    window.history.pushState(null, "", "/app/consignment/operations");
    setView("operations");
  };
  const backToMain = () => {
    window.history.pushState(null, "", "/app/consignment");
    setView("main");
  };

  const payout = useMutation({
    mutationFn: () => fetchJson("/api/consignment/payouts", ConsignmentMutationSchema, mutationBody({
      kind: payoutForm.kind,
      amount: Number(payoutForm.amount || 0),
      note: payoutForm.note,
    })),
    onSuccess: () => {
      setPayoutForm({ kind: payoutForm.kind, amount: "", note: "" });
      invalidate();
    },
  });

  const topup = useMutation({
    mutationFn: () => fetchJson("/api/consignment/topups", ConsignmentMutationSchema, mutationBody({
      amount: Number(topupForm.amount || 0),
      note: topupForm.note,
    })),
    onSuccess: () => {
      setTopupForm({ amount: "", note: "" });
      invalidate();
    },
  });

  const pmSync = useMutation({
    mutationFn: () => fetchJson("/api/consignment/pm-sync", ConsignmentPmSyncSchema, mutationBody({})),
    onSuccess: (data) => {
      setPmSyncResult(data);
      invalidate();
    },
  });

  const s = summary.data?.summary || ({} as NonNullable<typeof summary.data>["summary"] & Record<string, number>);
  const openAction = (item: ConsignmentItem, mode: StockAction["mode"]) => {
    setAction({ item, mode });
    setActionForm({
      quantity: "1",
      price: mode === "sale" ? String(item.salePrice || "") : (mode === "receive" ? String(item.purchasePrice || "") : ""),
      note: "",
      fromBalance: false,
    });
  };
  // Операция по всему товару: цена по умолчанию пустая — продажа возьмёт цену
  // каждой партии, докупка сохранит цену свежей партии.
  const openGroupAction = (group: ItemGroup, mode: StockAction["mode"]) => {
    setAction({ item: group.items[0], mode, group });
    setActionForm({ quantity: "1", price: "", note: "", fromBalance: false });
  };
  const applyPmRow = (row: { article: string; name: string; supplierName: string; price?: number | null }) => {
    setAddForm((current) => ({
      ...current,
      name: row.name || current.name,
      article: row.article || current.article,
      supplierName: row.supplierName || current.supplierName,
      purchasePrice: row.price !== null && row.price !== undefined ? String(row.price) : current.purchasePrice,
    }));
    setPmOpen(false);
  };

  const actionTitle = action
    ? (action.mode === "sale" ? "Продажа" : action.mode === "writeoff" ? "Списание" : "Приход / докупка")
    : "";

  const itemGroups = useMemo(() => groupItemsByArticle(items.data?.items || []), [items.data]);
  const visibleOperations = useMemo(() => {
    const query = opSearch.trim().toLowerCase();
    const list = (operations.data?.operations || []).filter((op) => {
      if (!query) return true;
      const label = OPERATION_LABELS[op.type] || op.type;
      return [op.itemName, op.note, label].some((text) => String(text || "").toLowerCase().includes(query));
    });
    return sortOperations(list, opSort);
  }, [operations.data, opSearch, opSort]);

  const operationsTable = (
    <div className="table-panel price-table consignment-operations-table">
      <div className="section-title">
        <div>
          <span>История</span>
          <h3>Операции</h3>
        </div>
        <div className="row-actions consignment-operations-filters">
          <input
            placeholder="Поиск: товар или примечание"
            value={opSearch}
            onChange={(event) => setOpSearch(event.target.value)}
          />
          <SelectField
            ariaLabel="Фильтр операций"
            value={opFilter}
            onChange={setOpFilter}
            options={[
              { value: "all", label: "Все операции" },
              { value: "sale", label: "Продажи" },
              { value: "receive", label: "Приходы" },
              { value: "purchase", label: "Закупки с баланса" },
              { value: "writeoff", label: "Списания" },
              { value: "return", label: "Возвраты" },
              { value: "sponsor_topup", label: "Пополнения баланса" },
              { value: "sponsor_payout", label: "Выплаты спонсору" },
              { value: "sponsor_profit_payout", label: "Вывод профита спонсора" },
              { value: "my_profit_payout", label: "Вывод моего профита" },
            ]}
          />
          <SelectField
            ariaLabel="Сортировка операций"
            value={opSort}
            onChange={setOpSort}
            options={OPERATION_SORT_OPTIONS}
          />
        </div>
      </div>
      <div className="table-head">
        <span>Дата</span><span>Операция</span><span>Товар</span><span>Кол-во</span><span>Цена</span><span>На баланс</span><span>Профит (спонсор / мой)</span><span>Примечание</span><span></span>
      </div>
      {visibleOperations.map((op) => (
        <div className="table-row" key={op.id}>
          <span data-label="Дата">{dateText(op.createdAt)}</span>
          <span data-label="Операция">
            {OPERATION_LABELS[op.type] || op.type}
            {op.status === "returned" ? " (возвращена)" : ""}
          </span>
          <span data-label="Товар">{op.itemName || "-"}</span>
          <span data-label="Кол-во">{op.quantity ? `${op.quantity} шт` : "-"}</span>
          <span data-label="Цена">{op.type === "sale" || op.type === "return" ? money(op.unitSale) : (op.quantity ? money(op.unitPurchase) : "-")}</span>
          <span data-label="На баланс">{op.balanceDelta ? money(op.balanceDelta) : "-"}</span>
          <span data-label="Профит">{op.sponsorDelta || op.myDelta ? `${money(op.sponsorDelta)} / ${money(op.myDelta)}` : "-"}</span>
          <span data-label="Примечание">{op.note || "-"}</span>
          <span data-label="" className="consignment-op-actions">
            {op.type === "sale" && op.status === "active" ? (
              <button
                className="secondary-action"
                type="button"
                disabled={returnSale.isPending}
                onClick={() => {
                  const note = window.prompt("Примечание к возврату (необязательно):", "") ?? "";
                  returnSale.mutate({ id: op.id, note });
                }}
              >
                {returnSale.isPending ? <Loader2 className="spin" size={14} /> : <RotateCcw size={14} />} Возврат
              </button>
            ) : null}
            <button
              className="icon-action danger"
              type="button"
              title="Удалить операцию (остаток и балансы откатятся)"
              disabled={deleteOperation.isPending}
              onClick={() => {
                if (window.confirm(`Удалить операцию «${OPERATION_LABELS[op.type] || op.type}»${op.itemName ? ` по «${op.itemName}»` : ""}? Остаток товара и балансы будут пересчитаны.`)) {
                  deleteOperation.mutate(op.id);
                }
              }}
            >
              <Trash2 size={14} />
            </button>
          </span>
        </div>
      ))}
      {returnSale.error ? <div className="inline-error">{errorMessage(returnSale.error)}</div> : null}
      {deleteOperation.error ? <div className="inline-error">{errorMessage(deleteOperation.error)}</div> : null}
      {!operations.data?.operations?.length && operations.isLoading ? <ListSkeleton rows={6} /> : null}
      {!operations.data?.operations?.length && !operations.isLoading ? <div className="empty-state">Операций пока нет.</div> : null}
      {(operations.data?.operations?.length || 0) > 0 && !visibleOperations.length ? <div className="empty-state">По заданному поиску операций не найдено.</div> : null}
    </div>
  );

  if (view === "operations") {
    return (
      <section className="page-section consignment-page">
        <PageHeader
          title="Операции реализации"
          subtitle="Полная история операций: продажи, приходы, списания, возвраты и выплаты."
          action={(
            <div className="row-actions">
              <button className="primary-action" type="button" onClick={backToMain}>
                <ArrowLeft size={16} /> К реализации
              </button>
              <button className="secondary-action" type="button" onClick={invalidate}>
                <RefreshCw size={16} /> Обновить
              </button>
            </div>
          )}
        />
        {operationsTable}
      </section>
    );
  }

  return (
    <section className="page-section consignment-page">
      <PageHeader
        title="Реализация (товар спонсора)"
        subtitle="Товар от спонсора: остатки, продажи с профитом 50/50, списания, возвраты, общий баланс и закупки со свободных денег."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" onClick={openOperations}>
              <History size={16} /> Операции ({operations.data?.operations?.length ?? "…"})
            </button>
            <button
              className="secondary-action"
              type="button"
              disabled={pmSync.isPending}
              onClick={() => { setPmSyncResult(null); pmSync.mutate(); }}
              title="Импортировать продажи из PriceMaster автоматически"
            >
              {pmSync.isPending ? <Loader2 className="spin" size={16} /> : <Upload size={16} />}
              {pmSync.isPending ? "Синк PM…" : "Синк PM"}
            </button>
            <button className="secondary-action" type="button" onClick={invalidate}>
              <RefreshCw size={16} /> Обновить
            </button>
          </div>
        )}
      />
      {pmSyncResult && (
        <div className="inline-success" style={{ marginBottom: 12 }}>
          Синк завершён: добавлено <strong>{pmSyncResult.created}</strong> продаж
          {(pmSyncResult.skippedBefore ?? 0) > 0 ? `, пропущено (до добавления в реализацию) ${pmSyncResult.skippedBefore}` : ""}
          {pmSyncResult.skipped ? `, дублей ${pmSyncResult.skipped}` : ""}
          {pmSyncResult.itemsCreated ? `, создано товаров ${pmSyncResult.itemsCreated}` : ""}
          {pmSyncResult.itemsMatched ? `, сопоставлено товаров ${pmSyncResult.itemsMatched}` : ""}
          {" "}(всего в PM: {pmSyncResult.total}).
          <button className="icon-action" type="button" style={{ marginLeft: 8 }} onClick={() => setPmSyncResult(null)}>
            <X size={14} />
          </button>
        </div>
      )}
      {pmSync.error && (
        <div className="inline-error" style={{ marginBottom: 12 }}>{errorMessage(pmSync.error)}</div>
      )}

      <section className="dashboard-metrics">
        <Stat label="Капитализация (по закупке)" value={money(s?.capitalization)} tone="accent" icon={<Boxes size={18} />} />
        <Stat label="Товара на складе" value={`${Number(s?.stockQuantity || 0)} шт`} tone="" icon={<Package size={18} />} />
        <Stat label="Общий баланс" value={money(s?.balance)} tone={Number(s?.balance || 0) >= 0 ? "success" : "warn"} icon={<Wallet size={18} />} />
        <Stat label="Профит спонсора" value={money(s?.sponsorProfit)} tone="accent" icon={<HandCoins size={18} />} />
        <Stat label="Мой профит" value={money(s?.myProfit)} tone="success" icon={<TrendingUp size={18} />} />
      </section>

      <div className="summary-grid">
        <div className="system-card"><span>Продано (за всё время)</span><strong>{Number(s?.soldQuantity || 0)} шт на {money(s?.salesRevenue)}</strong></div>
        <div className="system-card"><span>Общий профит с продаж</span><strong>{money(s?.profitTotal)}</strong></div>
        <div className="system-card"><span>Стоимость склада по продаже</span><strong>{money(s?.stockSaleValue)}</strong></div>
        <div className="system-card"><span>Закупки с баланса</span><strong>{money(s?.purchasesFromBalance)}</strong></div>
        <div className="system-card"><span>Пополнения баланса</span><strong>{money(s?.sponsorTopUps)}</strong></div>
        <div className="system-card"><span>Выплачено спонсору</span><strong>{money(Number(s?.sponsorPayouts || 0) + Number(s?.sponsorProfitPaidOut || 0))}</strong></div>
        <div className="system-card"><span>Выведено моего профита</span><strong>{money(s?.myProfitPaidOut)}</strong></div>
      </div>

      <section className="settings-panel settings-panel-wide">
        <div className="section-title">
          <div>
            <span>Новый товар</span>
            <h3>Принять товар от спонсора или закупить с баланса</h3>
          </div>
        </div>
        <div className="settings-form-row" style={{ position: "relative" }}>
          <input
            placeholder="Поиск по базе (артикул или наименование)"
            value={pmQuery}
            onChange={(event) => {
              setPmQuery(event.target.value);
              setPmOpen(true);
            }}
            onFocus={() => setPmOpen(true)}
          />
          <span className="muted-note"><Search size={14} /> Подставит наименование, артикул и поставщика из номенклатуры</span>
        </div>
        {pmOpen && pmQuery.trim().length >= 2 ? (
          <div className="consignment-pm-results">
            {pmSearch.isLoading ? <div className="empty-state">Ищу в номенклатуре…</div> : null}
            {(pmSearch.data?.items || []).map((row, index) => (
              <button
                key={`${row.article}-${index}`}
                type="button"
                className="consignment-pm-result"
                onClick={() => applyPmRow(row)}
              >
                <span>{row.article || "-"}</span>
                <span title={row.name}>{row.name || "-"}</span>
                <span>{row.supplierName || "-"}</span>
                <span>{pmPriceText(row)}</span>
              </button>
            ))}
            {!pmSearch.isLoading && !pmSearch.data?.items?.length ? <div className="empty-state">Ничего не найдено в номенклатуре.</div> : null}
          </div>
        ) : null}
        <div className="settings-form-row">
          <input placeholder="Наименование *" value={addForm.name} onChange={(event) => setAddForm({ ...addForm, name: event.target.value })} />
          <input placeholder="Артикул" value={addForm.article} onChange={(event) => setAddForm({ ...addForm, article: event.target.value })} />
          <input
            placeholder="Поставщик"
            list="consignment-suppliers"
            value={addForm.supplierName}
            onChange={(event) => setAddForm({ ...addForm, supplierName: event.target.value })}
          />
          <datalist id="consignment-suppliers">
            {(suppliers.data?.suppliers || []).map((name) => <option key={name} value={name} />)}
          </datalist>
        </div>
        <div className="settings-form-row">
          <input type="number" min="0" step="0.01" placeholder="Закупка, $" value={addForm.purchasePrice} onChange={(event) => setAddForm({ ...addForm, purchasePrice: event.target.value })} />
          <input type="number" min="0" step="0.01" placeholder="Продажа, $" value={addForm.salePrice} onChange={(event) => setAddForm({ ...addForm, salePrice: event.target.value })} />
          <input type="number" min="0" placeholder="Кол-во" value={addForm.quantity} onChange={(event) => setAddForm({ ...addForm, quantity: event.target.value })} />
          <input placeholder="Примечание" value={addForm.note} onChange={(event) => setAddForm({ ...addForm, note: event.target.value })} />
          <label className="toggle-row" title="Если включено — сумма закупки спишется с общего баланса">
            <input type="checkbox" checked={addForm.fromBalance} onChange={(event) => setAddForm({ ...addForm, fromBalance: event.target.checked })} />
            Закупка с баланса
          </label>
          <button
            className="secondary-action"
            type="button"
            title="Добавить товар в список, чтобы потом загрузить всё разом"
            disabled={!addForm.name.trim()}
            onClick={addToDraft}
          >
            <ListPlus size={16} /> В список
          </button>
          <button
            className="primary-action"
            type="button"
            disabled={createItem.isPending || !addForm.name.trim()}
            onClick={() => createItem.mutate()}
          >
            {createItem.isPending ? <Loader2 className="spin" size={16} /> : <Plus size={16} />} Добавить товар
          </button>
        </div>
        {createItem.error ? <div className="inline-error">{errorMessage(createItem.error)}</div> : null}
        {createItem.isSuccess ? <div className="success-strip">Товар добавлен на склад реализации.</div> : null}

        {draftItems.length ? (
          <div className="consignment-draft-panel">
            <div className="section-title">
              <div>
                <span>Список к загрузке</span>
                <h3>
                  {draftItems.length} шт на {money(draftItems.reduce((sum, draft) => sum + Number(draft.purchasePrice || 0) * Number(draft.quantity || 0), 0))} (закупка)
                </h3>
              </div>
              <div className="row-actions">
                <button
                  className="secondary-action"
                  type="button"
                  disabled={bulkCreate.isPending}
                  onClick={() => setDraftItems([])}
                >
                  <Trash2 size={14} /> Очистить
                </button>
                <button
                  className="primary-action"
                  type="button"
                  disabled={bulkCreate.isPending}
                  onClick={() => bulkCreate.mutate(draftItems)}
                >
                  {bulkCreate.isPending ? <Loader2 className="spin" size={16} /> : <Upload size={16} />} Загрузить в базу ({draftItems.length})
                </button>
              </div>
            </div>
            <div className="consignment-draft-head">
              <span>Товар</span><span>Артикул</span><span>Поставщик</span><span>Закупка</span><span>Продажа</span><span>Кол-во</span><span>Примечание</span><span></span>
            </div>
            {draftItems.map((draft) => (
              <div className="consignment-draft-row" key={draft.draftId}>
                <span title={draft.name}>{draft.name}</span>
                <span>{draft.article || "-"}</span>
                <span>{draft.supplierName || "-"}</span>
                <span>{money(draft.purchasePrice)}</span>
                <span>{money(draft.salePrice)}</span>
                <span>{Number(draft.quantity || 0)} шт{draft.fromBalance ? " · с баланса" : ""}</span>
                <span title={draft.note}>{draft.note || "-"}</span>
                <span>
                  <button
                    className="secondary-action"
                    type="button"
                    title="Убрать из списка"
                    disabled={bulkCreate.isPending}
                    onClick={() => setDraftItems((current) => current.filter((row) => row.draftId !== draft.draftId))}
                  >
                    <X size={14} />
                  </button>
                </span>
              </div>
            ))}
            {bulkCreate.error ? <div className="inline-error">{errorMessage(bulkCreate.error)}</div> : null}
          </div>
        ) : null}
        {bulkCreate.isSuccess && !draftItems.length ? <div className="success-strip">Список загружен: товары добавлены на склад реализации.</div> : null}
      </section>

      {action ? (
        <section className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div>
              <span>{actionTitle}</span>
              <h3>{action.item.name} — остаток {action.group ? action.group.quantity : action.item.quantity} шт{action.group ? ` (${action.group.items.length} парт.)` : ""}</h3>
            </div>
            <button className="secondary-action" type="button" onClick={() => setAction(null)}><X size={16} /> Отмена</button>
          </div>
          {action.group ? (
            <span className="muted-note">
              {action.mode === "receive"
                ? "Докупка попадёт в самую свежую партию товара."
                : "Количество спишется по партиям: сначала старые (FIFO), профит считается по закупке каждой партии."}
            </span>
          ) : null}
          <div className="settings-form-row">
            <input type="number" min="1" placeholder="Кол-во" value={actionForm.quantity} onChange={(event) => setActionForm({ ...actionForm, quantity: event.target.value })} />
            {action.mode !== "writeoff" ? (
              <input
                type="number"
                min="0"
                step="0.01"
                placeholder={action.mode === "sale"
                  ? (action.group ? "Цена продажи, $ (пусто — цена партии)" : "Цена продажи, $")
                  : (action.group ? "Цена закупки, $ (пусто — цена партии)" : "Цена закупки, $")}
                value={actionForm.price}
                onChange={(event) => setActionForm({ ...actionForm, price: event.target.value })}
              />
            ) : null}
            <input placeholder="Примечание" value={actionForm.note} onChange={(event) => setActionForm({ ...actionForm, note: event.target.value })} />
            {action.mode === "receive" ? (
              <label className="toggle-row" title="Списать сумму закупки с общего баланса">
                <input type="checkbox" checked={actionForm.fromBalance} onChange={(event) => setActionForm({ ...actionForm, fromBalance: event.target.checked })} />
                Закупка с баланса
              </label>
            ) : null}
            <button
              className="primary-action"
              type="button"
              disabled={stockOperation.isPending || !(Number(actionForm.quantity) > 0)}
              onClick={() => stockOperation.mutate(action)}
            >
              {stockOperation.isPending ? <Loader2 className="spin" size={16} /> : <Check size={16} />} Подтвердить
            </button>
          </div>
          {action.mode === "sale" ? (() => {
            const purchase = action.group ? action.group.avgPurchasePrice : action.item.purchasePrice;
            const sale = Number(actionForm.price === "" ? (action.group ? action.group.avgSalePrice : action.item.salePrice) : actionForm.price);
            const quantity = Number(actionForm.quantity || 0);
            const profit = (sale - purchase) * quantity;
            return (
              <span className="muted-note">
                Профит{action.group ? " (оценка по средним ценам партий)" : ""}: {money(profit)}
                {" "}(спонсору {money(profit / 2)}, мне {money(profit / 2)}).
                Закупочная часть {money(purchase * quantity)} уйдёт на общий баланс.
              </span>
            );
          })() : null}
          {stockOperation.error ? <div className="inline-error">{errorMessage(stockOperation.error)}</div> : null}
        </section>
      ) : null}

      <div className="table-panel price-table consignment-items-table">
        <div className="section-title">
          <div>
            <span>Склад реализации</span>
            <h3>Товары ({items.data?.items?.length || 0})</h3>
          </div>
          <input placeholder="Поиск: название, артикул, поставщик" value={itemSearch} onChange={(event) => setItemSearch(event.target.value)} />
        </div>
        <div className="table-head">
          <span>Товар</span><span>Артикул</span><span>Поставщик</span><span>Закупка</span><span>Продажа</span><span>Кол-во</span><span>Сумма (закупка)</span><span>Действия</span>
        </div>
        {itemGroups.map((group) => {
          const renderItemRow = (item: ConsignmentItem, lot = false) => {
            const draft = priceDrafts[item.id];
            const draftValue = draft ?? String(item.salePrice);
            const dirty = draft !== undefined && Number(draft) !== item.salePrice;
            return (
              <div className={lot ? "table-row consignment-group-lot" : "table-row"} key={item.id}>
                <span data-label="Товар">{item.name}</span>
                <span data-label="Артикул">{displayArticle(item.article)}</span>
                <span data-label="Поставщик">{item.supplierName || "-"}</span>
                <span data-label="Закупка">{money(item.purchasePrice)}</span>
                <span data-label="Продажа" className="finance-inline-edit">
                  <input
                    type="number"
                    min="0"
                    step="0.01"
                    value={draftValue}
                    onChange={(event) => setPriceDrafts((current) => ({ ...current, [item.id]: event.target.value }))}
                    style={{ width: 90 }}
                  />
                  {dirty ? (
                    <button
                      className="secondary-action"
                      type="button"
                      title="Сохранить цену продажи"
                      disabled={savePrice.isPending}
                      onClick={() => savePrice.mutate({ id: item.id, salePrice: Number(draft || 0) })}
                    >
                      {savePrice.isPending ? <Loader2 className="spin" size={14} /> : <Check size={14} />}
                    </button>
                  ) : null}
                </span>
                <span data-label="Кол-во">{item.quantity} шт</span>
                <span data-label="Сумма">{money(item.purchasePrice * item.quantity)}</span>
                <span data-label="Действия" className="row-actions">
                  <button className="secondary-action" type="button" title="Продать" disabled={!item.quantity} onClick={() => openAction(item, "sale")}><ShoppingCart size={14} /></button>
                  <button className="secondary-action" type="button" title="Списать" disabled={!item.quantity} onClick={() => openAction(item, "writeoff")}><PackageMinus size={14} /></button>
                  <button className="secondary-action" type="button" title="Приход / докупка" onClick={() => openAction(item, "receive")}><PackagePlus size={14} /></button>
                </span>
              </div>
            );
          };
          if (group.items.length === 1) return renderItemRow(group.items[0]);
          const expanded = Boolean(expandedGroups[group.key]);
          const toggle = () => setExpandedGroups((current) => ({ ...current, [group.key]: !expanded }));
          const supplierNames = Array.from(new Set(group.items.map((item) => item.supplierName).filter(Boolean))).join(", ");
          return (
            <Fragment key={group.key}>
              <div className="table-row consignment-group-row">
                <span data-label="Товар">
                  <button type="button" className="consignment-group-toggle" onClick={toggle} title={expanded ? "Свернуть партии" : "Показать партии"}>
                    {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />} {group.items[0].name}
                  </button>
                </span>
                <span data-label="Артикул">{displayArticle(group.article)}</span>
                <span data-label="Поставщик">{supplierNames || "-"}</span>
                <span data-label="Закупка" title="Средняя цена закупки по партиям">{money(group.avgPurchasePrice)} <span className="muted-note">сред.</span></span>
                <span data-label="Продажа" title="Средняя цена продажи по партиям">{money(group.avgSalePrice)} <span className="muted-note">сред.</span></span>
                <span data-label="Кол-во">{group.quantity} шт</span>
                <span data-label="Сумма">{money(group.purchaseTotal)}</span>
                <span data-label="Действия" className="row-actions">
                  <button className="secondary-action" type="button" title="Продать (спишется по партиям, сначала старые)" disabled={!group.quantity} onClick={() => openGroupAction(group, "sale")}><ShoppingCart size={14} /></button>
                  <button className="secondary-action" type="button" title="Списать (спишется по партиям, сначала старые)" disabled={!group.quantity} onClick={() => openGroupAction(group, "writeoff")}><PackageMinus size={14} /></button>
                  <button className="secondary-action" type="button" title="Приход / докупка (в свежую партию)" onClick={() => openGroupAction(group, "receive")}><PackagePlus size={14} /></button>
                </span>
              </div>
              {expanded ? group.items.map((item) => renderItemRow(item, true)) : null}
            </Fragment>
          );
        })}
        {savePrice.error ? <div className="inline-error">{errorMessage(savePrice.error)}</div> : null}
        {!items.data?.items?.length && items.isLoading ? <ListSkeleton rows={5} /> : null}
        {!items.data?.items?.length && !items.isLoading ? <div className="empty-state">Товаров пока нет — добавьте первый через форму выше.</div> : null}
      </div>

      <section className="settings-panel settings-panel-wide">
        <div className="section-title">
          <div>
            <span>Выплаты</span>
            <h3>Вывести деньги с балансов</h3>
          </div>
        </div>
        <div className="settings-form-row">
          <SelectField
            ariaLabel="Тип выплаты"
            value={payoutForm.kind}
            onChange={(kind) => setPayoutForm({ ...payoutForm, kind })}
            options={[
              { value: "sponsor_payout", label: `Спонсору с общего баланса (${money(s?.balance)})` },
              { value: "sponsor_profit_payout", label: `Профит спонсора (${money(s?.sponsorProfit)})` },
              { value: "my_profit_payout", label: `Мой профит (${money(s?.myProfit)})` },
            ]}
          />
          <input type="number" min="0" step="0.01" placeholder="Сумма, $" value={payoutForm.amount} onChange={(event) => setPayoutForm({ ...payoutForm, amount: event.target.value })} />
          <input placeholder="Примечание" value={payoutForm.note} onChange={(event) => setPayoutForm({ ...payoutForm, note: event.target.value })} />
          <button
            className="primary-action"
            type="button"
            disabled={payout.isPending || !(Number(payoutForm.amount) > 0)}
            onClick={() => payout.mutate()}
          >
            {payout.isPending ? <Loader2 className="spin" size={16} /> : <Banknote size={16} />} Выплатить
          </button>
        </div>
        {payout.error ? <div className="inline-error">{errorMessage(payout.error)}</div> : null}
        {payout.isSuccess ? <div className="success-strip">Выплата записана.</div> : null}
      </section>

      <section className="settings-panel settings-panel-wide">
        <div className="section-title">
          <div>
            <span>Пополнение</span>
            <h3>Пополнить баланс спонсора</h3>
          </div>
        </div>
        <span className="muted-note">
          Довнесение денег на общий баланс — например, компенсация за испорченный товар в поставке. Текущий баланс: {money(s?.balance)}.
        </span>
        <div className="settings-form-row">
          <input type="number" min="0" step="0.01" placeholder="Сумма, $" value={topupForm.amount} onChange={(event) => setTopupForm({ ...topupForm, amount: event.target.value })} />
          <input placeholder="Примечание (за что пополнение)" value={topupForm.note} onChange={(event) => setTopupForm({ ...topupForm, note: event.target.value })} />
          <button
            className="primary-action"
            type="button"
            disabled={topup.isPending || !(Number(topupForm.amount) > 0)}
            onClick={() => topup.mutate()}
          >
            {topup.isPending ? <Loader2 className="spin" size={16} /> : <HandCoins size={16} />} Пополнить
          </button>
        </div>
        {topup.error ? <div className="inline-error">{errorMessage(topup.error)}</div> : null}
        {topup.isSuccess ? <div className="success-strip">Пополнение записано — баланс обновлён.</div> : null}
      </section>

      <button className="consignment-operations-link" type="button" onClick={openOperations}>
        <History size={18} />
        <span>
          <strong>Операции реализации</strong>
          <small>Вся история продаж, приходов, возвратов и выплат — {operations.data?.operations?.length ?? "…"} записей</small>
        </span>
        <ChevronRight size={17} />
      </button>
    </section>
  );
}
