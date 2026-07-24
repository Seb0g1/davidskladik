import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, CalendarClock, ChevronDown, ChevronUp, Clock3, Database, ListChecks, Loader2, Package, PackageOpen, RefreshCw, Repeat2, RotateCcw, Search, Settings2, Trash2, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SupplierAltPicker } from "../components/SupplierAltPicker";
import { SupplierPickingListSchema, SupplierPickingRowSchema, SupplierPickingUpdateSchema, SupplierReplaceResponseSchema } from "../types";
import { SupplierCartPanel } from "./OperationsPage";
import { compactDate, errorMessage } from "../lib/common";

type PickingRow = z.infer<typeof SupplierPickingRowSchema>;

const SupplierCartScheduleSchema = z.object({
  ok: z.boolean().optional(),
  settings: z.record(z.string(), z.unknown()).optional().default({}),
  autoRunning: z.boolean().optional().default(false),
  lastAutoRunAt: z.coerce.string().optional().nullable(),
  nextAutoRunAt: z.coerce.string().optional().nullable(),
  lastAutoResult: z.record(z.string(), z.unknown()).optional().nullable(),
}).passthrough();

const RollbackSummarySchema = z.object({
  cartProcessed: z.number().optional().default(0),
  draftRows: z.number().optional().default(0),
  pickingRows: z.number().optional().default(0),
  supplierBlocks: z.number().optional().default(0),
  jsonHistory: z.number().optional().default(0),
  pm: z.record(z.string(), z.unknown()).optional().default({}),
  postgres: z.record(z.string(), z.unknown()).optional().nullable(),
}).passthrough();

const SupplierCartRollbackSchema = z.object({
  ok: z.boolean().optional(),
  dryRun: z.boolean().optional().default(true),
  before: RollbackSummarySchema.optional(),
  after: RollbackSummarySchema.optional(),
  pm: z.record(z.string(), z.unknown()).optional().default({}),
}).passthrough();

const PriceMasterStatusSchema = z.object({
  ok: z.boolean().optional().default(false),
  config: z.record(z.string(), z.unknown()).optional().default({}),
  db: z.coerce.string().optional().default(""),
  tables: z.record(z.string(), z.unknown()).optional().default({}),
  davidskladDocs: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  latestDocs: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  latestRows: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

const PmSearchItemSchema = z.object({
  id: z.string(),
  rowId: z.coerce.string().optional().default(""),
  article: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  supplierName: z.coerce.string().optional().default(""),
  name: z.coerce.string().optional().default(""),
  price: z.number().optional().default(0),
  currency: z.coerce.string().optional().default("USD"),
  docDate: z.coerce.string().optional().nullable(),
}).passthrough();

const PmSearchResponseSchema = z.object({
  ok: z.boolean().optional(),
  total: z.number().optional().default(0),
  items: z.array(PmSearchItemSchema).optional().default([]),
}).passthrough();

const PmManualCommitSchema = z.object({
  ok: z.boolean().optional(),
  inserted: z.number().optional().default(0),
  skipped: z.number().optional().default(0),
  docIds: z.array(z.unknown()).optional().default([]),
  pickingCreated: z.number().optional().default(0),
}).passthrough();

type PmSearchItem = z.infer<typeof PmSearchItemSchema>;

const text = (value: unknown) => String(value ?? "").trim();
const formatDate = (value: unknown) => {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
};

const countArray = (value: unknown) => Array.isArray(value) ? value.length : 0;
const rollbackCount = (summary: z.infer<typeof RollbackSummarySchema> | undefined) => {
  if (!summary) return 0;
  const pm = summary.pm || {};
  return Number(summary.cartProcessed || 0)
    + Number(summary.draftRows || 0)
    + Number(summary.pickingRows || 0)
    + Number(summary.supplierBlocks || 0)
    + countArray(pm.rowIds)
    + countArray(pm.docIds);
};

function ReadyToShipPanel() {
  const [q, setQ] = useState("");
  const [replaceKey, setReplaceKey] = useState<string | null>(null);
  const queryClient = useQueryClient();

  const sessionQuery = useQuery({
    queryKey: ["session"],
    queryFn: () => fetchJson("/api/session", z.object({ role: z.coerce.string().optional().nullable() }).passthrough()),
    staleTime: 60_000,
  });
  const isAdmin = sessionQuery.data?.role === "admin";

  const listQuery = useQuery({
    queryKey: ["supplier-picking-list", "picked", "ready-to-ship"],
    queryFn: () => fetchJson("/api/supplier-picking-list?status=picked&limit=500", SupplierPickingListSchema),
    refetchInterval: 15_000,
  });

  const revertMutation = useMutation({
    mutationFn: (key: string) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({ status: "open" })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });

  const returnMutation = useMutation({
    mutationFn: (key: string) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({ status: "returned" })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });

  const revertAndReplaceMutation = useMutation({
    mutationFn: async ({ key, partnerId, rowId }: { key: string; partnerId: string; rowId: string }) => {
      await fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({ status: "open" }));
      return fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}/replace-supplier`, SupplierReplaceResponseSchema, mutationBody({ partnerId, rowId }));
    },
    onSuccess: () => {
      setReplaceKey(null);
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
    },
  });

  const rows = listQuery.data?.rows || [];
  const filteredRows = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return needle
      ? rows.filter((row: PickingRow) =>
          [row.productName, row.offerId, row.orderId, row.postingNumber, row.supplierName]
            .join(" ").toLowerCase().includes(needle))
      : rows;
  }, [q, rows]);

  return (
    <section className="table-panel supplier-cart-panel">
      <div className="section-title">
        <div>
          <span>Готовы к отгрузке</span>
          <h3>Позиции со статусом «собрано» — {rows.length} шт.</h3>
        </div>
        <button className="secondary-action" type="button" onClick={() => listQuery.refetch()} disabled={listQuery.isFetching}>
          {listQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
        </button>
      </div>
      <div className="control-grid compact-controls">
        <label>Поиск
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="SKU, товар, заказ, поставщик" />
        </label>
      </div>
      {!isAdmin ? (
        <div className="soft-empty compact">Замена поставщика и возврат к сборке доступны только администратору.</div>
      ) : null}
      {listQuery.error ? <div className="inline-error">{errorMessage(listQuery.error)}</div> : null}
      {revertMutation.error ? <div className="inline-error">{errorMessage(revertMutation.error)}</div> : null}
      {returnMutation.error ? <div className="inline-error">{errorMessage(returnMutation.error)}</div> : null}
      {returnMutation.data ? <div className="success-strip">Товар отмечен как «вернули из ПВЗ». При следующем заказе этого SKU PM-заявка не создаётся — берётся из пула возвратов.</div> : null}
      {revertAndReplaceMutation.error ? <div className="inline-error">Замена поставщика: {errorMessage(revertAndReplaceMutation.error)}</div> : null}
      {revertAndReplaceMutation.data ? (
        <div className="success-strip">
          Перезаказано у «{revertAndReplaceMutation.data.supplierName || "нового поставщика"}»: заявка в PriceMaster создана (doc {revertAndReplaceMutation.data.docIds?.join(", ") || "-"}).
        </div>
      ) : null}
      <div className="supplier-cart-list">
        {listQuery.isLoading ? <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю...</div> : null}
        {filteredRows.map((row: PickingRow) => (
          <article className="supplier-cart-row ready" key={row.key}>
            <span className="checkline">
              <span>{row.marketplace.toUpperCase()} · {row.orderId || row.postingNumber || "-"} · {row.offerId}</span>
            </span>
            <strong>{row.productName || row.offerId}</strong>
            <div className="meta-grid">
              <span>Кол-во: {row.quantity}</span>
              <span>Поставщик: {row.supplierName || "-"}</span>
              <span>Цена PM: {row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
              <span>Собрал: {row.pickedBy || "-"} · {compactDate(row.pickedAt)}</span>
              <span>Doc/Row: {row.requestDocId || "-"}/{row.requestRowId || "-"}</span>
              {row.wbSupplyId ? (
                <span>
                  WB поставка: <strong>{row.wbSupplyId}</strong>
                  {" · "}
                  <a href={`/api/wb/supplies/${row.wbSupplyId}/barcode?type=png`} target="_blank" rel="noreferrer" className="link-plain">Стикер</a>
                </span>
              ) : null}
            </div>
            {isAdmin ? (
              <div className="supplier-cart-actions">
                <button
                  className="secondary-action"
                  type="button"
                  disabled={returnMutation.isPending || revertMutation.isPending || revertAndReplaceMutation.isPending}
                  onClick={() => returnMutation.mutate(row.key)}
                  title="Товар вернулся из ПВЗ — следующий заказ этого SKU не пойдёт в PM, а возьмётся из этого возврата"
                >
                  <PackageOpen size={14} /> Вернули из ПВЗ
                </button>
                <button
                  className="secondary-action"
                  type="button"
                  disabled={revertAndReplaceMutation.isPending || revertMutation.isPending}
                  onClick={() => setReplaceKey(replaceKey === row.key ? null : row.key)}
                >
                  <Repeat2 size={14} /> Заменить поставщика и заказать в PM
                </button>
                <button
                  className="secondary-action"
                  type="button"
                  disabled={revertMutation.isPending || revertAndReplaceMutation.isPending}
                  onClick={() => revertMutation.mutate(row.key)}
                >
                  <RotateCcw size={14} /> Вернуть к сборке
                </button>
              </div>
            ) : null}
            {replaceKey === row.key ? (
              <SupplierAltPicker
                offerId={row.offerId}
                currentPartnerId={row.partnerId}
                busy={revertAndReplaceMutation.isPending}
                actionLabel="Вернуть к сборке и заказать у него"
                onPick={(option) => revertAndReplaceMutation.mutate({ key: row.key, partnerId: option.partnerId, rowId: option.rowId })}
                onClose={() => setReplaceKey(null)}
              />
            ) : null}
          </article>
        ))}
        {!filteredRows.length && !listQuery.isLoading ? <div className="soft-empty">Позиций со статусом «собрано» нет.</div> : null}
      </div>
    </section>
  );
}

type SortMode = "price_asc" | "price_desc" | "name_asc" | "supplier_asc";

function PmSearchPanel({ onClose }: { onClose: () => void }) {
  const [searchQ, setSearchQ] = useState("");
  const [activeQ, setActiveQ] = useState("");
  const [selected, setSelected] = useState<Record<string, { qty: number; item: PmSearchItem }>>({});
  const [sortMode, setSortMode] = useState<SortMode>("price_asc");
  const [supplierFilter, setSupplierFilter] = useState("");
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const queryClient = useQueryClient();

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    const trimmed = searchQ.trim();
    if (!trimmed) { setActiveQ(""); return; }
    debounceRef.current = setTimeout(() => setActiveQ(trimmed), 350);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [searchQ]);

  // Auto-focus when panel opens
  useEffect(() => { setTimeout(() => inputRef.current?.focus(), 80); }, []);

  const searchQuery = useQuery({
    queryKey: ["pm-search", activeQ],
    queryFn: () => fetchJson(`/api/supplier-cart/pm-search?q=${encodeURIComponent(activeQ)}&limit=150`, PmSearchResponseSchema),
    enabled: Boolean(activeQ),
    staleTime: 60_000,
  });

  const commitMutation = useMutation({
    mutationFn: () => {
      const items = Object.entries(selected).map(([id, { qty }]) => ({ id, quantity: qty }));
      return fetchJson("/api/supplier-cart/pm-manual-commit", PmManualCommitSchema, mutationBody({ items }));
    },
    onSuccess: () => {
      setSelected({});
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
    },
  });

  const allItems = searchQuery.data?.items ?? [];
  const supplierNames = useMemo(() => [...new Set(allItems.map((i) => i.supplierName).filter(Boolean))].sort(), [allItems]);

  const sortedItems = useMemo(() => {
    let list = supplierFilter ? allItems.filter((i) => i.supplierName === supplierFilter) : allItems;
    if (sortMode === "price_asc") list = [...list].sort((a, b) => (a.price ?? 0) - (b.price ?? 0));
    else if (sortMode === "price_desc") list = [...list].sort((a, b) => (b.price ?? 0) - (a.price ?? 0));
    else if (sortMode === "name_asc") list = [...list].sort((a, b) => (a.name || "").localeCompare(b.name || "", "ru"));
    else if (sortMode === "supplier_asc") list = [...list].sort((a, b) => (a.supplierName || "").localeCompare(b.supplierName || "", "ru"));
    return list;
  }, [allItems, sortMode, supplierFilter]);

  const selectedCount = Object.keys(selected).length;
  const isSearching = searchQuery.isLoading || (searchQ.trim() !== activeQ && Boolean(searchQ.trim()));

  const toggleItem = (item: PmSearchItem) => {
    setSelected((prev) => {
      const next = { ...prev };
      if (next[item.id]) { delete next[item.id]; } else { next[item.id] = { qty: 1, item }; }
      return next;
    });
  };

  const setQty = (id: string, qty: number) =>
    setSelected((prev) => prev[id] ? { ...prev, [id]: { ...prev[id], qty: Math.max(1, qty) } } : prev);

  return (
    <div className="pm-search-panel">
      {/* Header */}
      <div className="pm-search-panel-header">
        <div>
          <span className="pm-search-panel-title">Поиск в PriceMaster</span>
          <span className="pm-search-panel-sub">Найдите товар и добавьте заявку</span>
        </div>
        <button className="icon-action" type="button" onClick={onClose} title="Закрыть"><X size={18} /></button>
      </div>

      {/* Search input */}
      <div className="pm-search-input-wrap">
        <Search size={15} className="pm-search-icon" />
        <input
          ref={inputRef}
          className="pm-search-input"
          value={searchQ}
          onChange={(e) => setSearchQ(e.target.value)}
          placeholder="Chanel No5, Dior Sauvage, артикул…"
        />
        {isSearching ? (
          <Loader2 className="spin pm-search-spinner" size={14} />
        ) : searchQ ? (
          <button className="pm-search-clear" type="button" onClick={() => { setSearchQ(""); setActiveQ(""); inputRef.current?.focus(); }}>
            <X size={14} />
          </button>
        ) : null}
      </div>

      {/* Filters bar */}
      {allItems.length > 0 ? (
        <div className="pm-search-filters">
          <div className="pm-search-filter-group">
            {(["price_asc", "price_desc", "name_asc", "supplier_asc"] as SortMode[]).map((mode) => {
              const labels: Record<SortMode, string> = { price_asc: "Цена ↑", price_desc: "Цена ↓", name_asc: "А–Я", supplier_asc: "Поставщик" };
              return (
                <button
                  key={mode}
                  className={`pm-search-filter-btn${sortMode === mode ? " active" : ""}`}
                  type="button"
                  onClick={() => setSortMode(mode)}
                >
                  {labels[mode]}
                </button>
              );
            })}
          </div>
          {supplierNames.length > 1 ? (
            <select
              className="pm-search-supplier-filter"
              value={supplierFilter}
              onChange={(e) => setSupplierFilter(e.target.value)}
            >
              <option value="">Все поставщики ({allItems.length})</option>
              {supplierNames.map((s) => (
                <option key={s} value={s}>{s} ({allItems.filter((i) => i.supplierName === s).length})</option>
              ))}
            </select>
          ) : null}
        </div>
      ) : null}

      {/* Error / success */}
      {searchQuery.error ? <div className="inline-error" style={{ margin: "8px 16px 0" }}>{errorMessage(searchQuery.error)}</div> : null}
      {commitMutation.error ? <div className="inline-error" style={{ margin: "8px 16px 0" }}>{errorMessage(commitMutation.error)}</div> : null}
      {commitMutation.data ? (
        <div className="success-strip" style={{ margin: "8px 16px 0" }}>
          Добавлено: {commitMutation.data.inserted} строк · doc {commitMutation.data.docIds?.join(", ") || "-"} · строк сборки: {commitMutation.data.pickingCreated}
        </div>
      ) : null}

      {/* Results count */}
      {sortedItems.length > 0 ? (
        <div className="pm-search-count">
          {sortedItems.length} результатов{selectedCount > 0 ? ` · выбрано ${selectedCount}` : ""}
          {selectedCount > 0 ? (
            <button className="secondary-action" type="button" style={{ marginLeft: 8, padding: "2px 8px", fontSize: "0.75rem" }} onClick={() => setSelected({})}>
              Снять всё
            </button>
          ) : null}
        </div>
      ) : null}

      {/* Results list */}
      <div className="pm-search-results">
        {!searchQ ? (
          <div className="soft-empty pm-search-empty">Введите название товара или артикул — результаты появятся автоматически.</div>
        ) : activeQ && !sortedItems.length && !isSearching ? (
          <div className="soft-empty pm-search-empty">Ничего не найдено по «{activeQ}».</div>
        ) : null}
        {sortedItems.map((item) => {
          const sel = selected[item.id];
          return (
            <article
              key={item.id}
              className={`pm-search-item${sel ? " pm-search-item--selected" : ""}`}
              onClick={() => toggleItem(item)}
            >
              <div className="pm-search-item-check">
                <input
                  type="checkbox"
                  checked={Boolean(sel)}
                  onChange={() => toggleItem(item)}
                  onClick={(e) => e.stopPropagation()}
                />
              </div>
              <div className="pm-search-item-body">
                <strong className="pm-search-item-name">{item.name || item.article}</strong>
                <div className="pm-search-item-meta">
                  {item.supplierName ? <span className="pm-search-item-supplier">{item.supplierName}</span> : null}
                  <span className="pm-search-item-price">{item.price ? `${item.price} ${item.currency}` : "—"}</span>
                  {item.article ? <span>{item.article}</span> : null}
                  {item.docDate ? <span>{compactDate(item.docDate)}</span> : null}
                </div>
                {sel ? (
                  <div className="pm-search-item-qty" onClick={(e) => e.stopPropagation()}>
                    <span>Кол-во:</span>
                    <input
                      type="number"
                      min={1}
                      value={sel.qty}
                      onChange={(e) => setQty(item.id, Number(e.target.value))}
                    />
                  </div>
                ) : null}
              </div>
            </article>
          );
        })}
      </div>

      {/* Footer: commit button */}
      {selectedCount > 0 ? (
        <div className="pm-search-footer">
          <button
            className="primary-action pm-search-commit-btn"
            type="button"
            disabled={commitMutation.isPending}
            onClick={() => commitMutation.mutate()}
          >
            {commitMutation.isPending ? <Loader2 className="spin" size={16} /> : <Database size={16} />}
            Добавить в PriceMaster — {selectedCount} поз.
          </button>
        </div>
      ) : null}
    </div>
  );
}

const ALL_MARKETPLACES = [
  { id: "ozon", label: "Ozon" },
  { id: "yandex", label: "Yandex Market" },
  { id: "wb", label: "Wildberries" },
] as const;

export function SupplierCartPage() {
  const [tab, setTab] = useState<"cart" | "ready">("cart");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [pmSearchOpen, setPmSearchOpen] = useState(false);
  const [pendingMarketplaces, setPendingMarketplaces] = useState<string[] | null>(null);
  const queryClient = useQueryClient();
  const schedule = useQuery({
    queryKey: ["supplier-cart", "schedule"],
    queryFn: () => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema),
    refetchInterval: 30_000,
  });
  const toggle = useMutation({
    mutationFn: (autoEnabled: boolean) => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema, patchBody({ autoEnabled })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart", "schedule"] });
    },
  });
  const marketplacesMutation = useMutation({
    mutationFn: (marketplaces: string[]) => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema, patchBody({ marketplaces })),
    onSuccess: () => {
      setPendingMarketplaces(null);
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart", "schedule"] });
    },
  });
  const pmStatus = useQuery({
    queryKey: ["supplier-cart", "pricemaster-status"],
    queryFn: () => fetchJson("/api/supplier-cart/pricemaster/status", PriceMasterStatusSchema),
    refetchInterval: 60_000,
    enabled: settingsOpen,
  });
  const rollbackDryRun = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/rollback-all", SupplierCartRollbackSchema, mutationBody({
      confirm: "ROLLBACK_DAVIDSKLAD_SUPPLIER_CART",
      dryRun: true,
    })),
  });
  const rollbackApply = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/rollback-all", SupplierCartRollbackSchema, mutationBody({
      confirm: "ROLLBACK_DAVIDSKLAD_SUPPLIER_CART",
      dryRun: false,
    })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });
  const settings = schedule.data?.settings || {};
  const times = Array.isArray(settings.scheduleTimes) ? settings.scheduleTimes.map(text).filter(Boolean) : ["09:30", "12:00", "15:00"];
  const last = schedule.data?.lastAutoResult;
  const autoEnabled = settings.autoEnabled !== false;
  const savedMarketplaces: string[] = Array.isArray(settings.marketplaces) ? settings.marketplaces as string[] : ["ozon", "yandex", "wb"];
  const activeMarketplaces = pendingMarketplaces ?? savedMarketplaces;
  const dryRun = rollbackDryRun.data?.before;
  const pm = dryRun?.pm || {};

  return (
    <section className="page-section supplier-cart-page">
      <PageHeader
        title="Автокорзина"
        subtitle={`Заказы ${activeMarketplaces.map((m) => m === "wb" ? "Wildberries" : m === "yandex" ? "Yandex Market" : "Ozon").join(", ")} автоматически отправляются в корзину PriceMaster по расписанию.`}
        action={
          <div style={{ display: "flex", gap: 8 }}>
            <button
              className={`secondary-action${pmSearchOpen ? " active" : ""}`}
              type="button"
              onClick={() => setPmSearchOpen((v) => !v)}
            >
              <Search size={15} />
              Поиск в PM
            </button>
            <button
              className={`secondary-action${settingsOpen ? " active" : ""}`}
              type="button"
              onClick={() => setSettingsOpen((v) => !v)}
              title="Настройки расписания и диагностика"
            >
              <Settings2 size={15} />
              Настройки
              {settingsOpen ? <ChevronUp size={13} /> : <ChevronDown size={13} />}
            </button>
            <a className="secondary-action" href="/app/operations"><RefreshCw size={15} /> Операции</a>
          </div>
        }
      />

      {/* Compact status strip */}
      <div className="cart-status-strip">
        <div className={`cart-status-chip${autoEnabled ? " cart-status-chip--on" : " cart-status-chip--off"}`}>
          <Clock3 size={13} />
          Авторежим: {autoEnabled ? "включён" : "выключен"}
        </div>
        <div className="cart-status-chip">
          <CalendarClock size={13} />
          {schedule.data?.nextAutoRunAt ? `Запуск: ${formatDate(schedule.data.nextAutoRunAt)}` : "Следующий запуск: —"}
        </div>
        <button
          className={`cart-status-toggle${autoEnabled ? "" : " cart-status-toggle--off"}`}
          type="button"
          disabled={toggle.isPending}
          onClick={() => toggle.mutate(!autoEnabled)}
        >
          {toggle.isPending ? <Loader2 className="spin" size={13} /> : <Clock3 size={13} />}
          {autoEnabled ? "Выключить авто" : "Включить авто"}
        </button>
      </div>

      {/* Collapsible settings drawer */}
      {settingsOpen ? (
        <div className="cart-settings-drawer">
          <section className="cart-settings-section">
            <div className="cart-settings-section-title">
              <ListChecks size={14} /> Маркетплейсы
            </div>
            <div className="cart-marketplace-toggles">
              {ALL_MARKETPLACES.map(({ id, label }) => {
                const active = activeMarketplaces.includes(id);
                return (
                  <label key={id} className={`cart-mp-toggle${active ? " cart-mp-toggle--on" : ""}`}>
                    <input
                      type="checkbox"
                      checked={active}
                      onChange={() => {
                        const next = active
                          ? activeMarketplaces.filter((m) => m !== id)
                          : [...activeMarketplaces, id];
                        setPendingMarketplaces(next);
                      }}
                    />
                    {label}
                  </label>
                );
              })}
              {pendingMarketplaces ? (
                <button
                  className="primary-action"
                  type="button"
                  disabled={marketplacesMutation.isPending || activeMarketplaces.length === 0}
                  onClick={() => marketplacesMutation.mutate(activeMarketplaces)}
                  style={{ marginLeft: 8 }}
                >
                  {marketplacesMutation.isPending ? <Loader2 className="spin" size={13} /> : null}
                  Сохранить
                </button>
              ) : null}
            </div>
            {marketplacesMutation.error ? <div className="inline-error" style={{ marginTop: 8 }}>{errorMessage(marketplacesMutation.error)}</div> : null}
          </section>

          <section className="cart-settings-section">
            <div className="cart-settings-section-title">
              <Clock3 size={14} /> Расписание
              <span className="cart-settings-badge">{times.join(" · ")}</span>
            </div>
            <div className="summary-grid">
              <div><span>Статус</span><strong>{autoEnabled ? "включено" : "выключено"}</strong></div>
              <div><span>Время запусков</span><strong>{times.join(" · ")}</strong></div>
              <div><span>Следующий запуск</span><strong>{formatDate(schedule.data?.nextAutoRunAt)}</strong></div>
              <div><span>Последний запуск</span><strong>{formatDate(schedule.data?.lastAutoRunAt)}</strong></div>
            </div>
            {last ? (
              <div className="success-strip" style={{ marginTop: 8 }}>
                Последний запуск: всего {Number(last.total || 0)}, готово {Number(last.ready || 0)}, добавлено в PM {Number(last.inserted || 0)}, строк сборки {Number(last.pickingCreated || 0)}, пропущено {Number(last.skipped || 0)}.
              </div>
            ) : null}
          </section>

          <section className="cart-settings-section">
            <div className="cart-settings-section-title">
              <Database size={14} /> PriceMaster / Диагностика
            </div>
            <div className="summary-grid">
              <div><span>PM база</span><strong>{pmStatus.data?.db || String(pmStatus.data?.config?.database || "-")}</strong></div>
              <div><span>RequestDocs</span><strong>{pmStatus.data?.tables?.requestDocs ? "ok" : "нет"}</strong></div>
              <div><span>Документы ДавидСклад</span><strong>{pmStatus.data?.davidskladDocs?.length || 0}</strong></div>
              <div><span>Строки PM</span><strong>{pmStatus.data?.latestRows?.length || 0}</strong></div>
            </div>
            {pmStatus.error ? <div className="inline-error">{String(pmStatus.error)}</div> : null}
            <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
              <button className="secondary-action danger-action" type="button" disabled={rollbackDryRun.isPending} onClick={() => rollbackDryRun.mutate()}>
                {rollbackDryRun.isPending ? <Loader2 className="spin" size={14} /> : <AlertTriangle size={14} />} Проверить откат
              </button>
              {dryRun ? (
                <button
                  className="secondary-action danger-action"
                  type="button"
                  disabled={rollbackApply.isPending || rollbackCount(dryRun) === 0}
                  onClick={() => rollbackApply.mutate()}
                >
                  {rollbackApply.isPending ? <Loader2 className="spin" size={14} /> : <Trash2 size={14} />} Откатить автокорзину и сборку
                </button>
              ) : null}
            </div>
            {dryRun ? (
              <div className="inline-warning" style={{ marginTop: 8 }}>
                Будет очищено: processed {dryRun.cartProcessed}, черновик {dryRun.draftRows}, сборка {dryRun.pickingRows}, блокировки {dryRun.supplierBlocks}, PM rows {countArray(pm.rowIds)}, PM docs {countArray(pm.docIds)}.
              </div>
            ) : null}
            {rollbackApply.data ? <div className="success-strip">Откат выполнен. Осталось строк сборки: {rollbackApply.data.after?.pickingRows || 0}, PM rows: {countArray(rollbackApply.data.after?.pm?.rowIds)}.</div> : null}
            {rollbackDryRun.error ? <div className="inline-error">{String(rollbackDryRun.error)}</div> : null}
            {rollbackApply.error ? <div className="inline-error">{String(rollbackApply.error)}</div> : null}
          </section>
        </div>
      ) : null}

      <div className="page-tabs">
        <button className={`page-tab-btn${tab === "cart" ? " active" : ""}`} type="button" onClick={() => setTab("cart")}>
          <ListChecks size={15} /> Корзина
        </button>
        <button className={`page-tab-btn${tab === "ready" ? " active" : ""}`} type="button" onClick={() => setTab("ready")}>
          <Package size={15} /> Готовы к отгрузке
        </button>
      </div>
      {tab === "cart" ? <SupplierCartPanel /> : <ReadyToShipPanel />}

      {/* PM Search side panel overlay */}
      {pmSearchOpen ? (
        <>
          <div className="pm-search-backdrop" onClick={() => setPmSearchOpen(false)} />
          <PmSearchPanel onClose={() => setPmSearchOpen(false)} />
        </>
      ) : null}
    </section>
  );
}
