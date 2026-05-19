import {
  AlertCircle,
  Archive,
  Bot,
  Check,
  ChevronRight,
  CirclePlay,
  ImagePlus,
  Link2,
  Loader2,
  PackageCheck,
  RefreshCw,
  Save,
  Search,
  Settings,
  Sparkles,
  Trash2,
  X,
} from "lucide-react";
import { ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { QueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ApiError, fetchJson, mutationBody } from "./api";
import {
  AiDraftsSchema,
  AiImagesResponseSchema,
  DiagnosticsSchema,
  Filters,
  GroupDetailSchema,
  MutationProductResponseSchema,
  NoSupplierSchema,
  OperationCreateSchema,
  OperationsSchema,
  PriceMasterSearchRow,
  PriceMasterSearchSchema,
  Product,
  ProductLink,
  SettingsResponseSchema,
  WarehousePageSchema,
  YandexQualityCandidatesSchema,
} from "./types";
import type { WarehousePage } from "./types";

const pageSize = 80;

type MutationPayload = {
  product?: Product;
  products?: Product[];
};

type LinkDraft = {
  article: string;
  supplierName: string;
  keyword: string;
  priceCurrency: string;
  partnerId?: string;
  sourceRowId?: string;
  exactName?: string;
  matchType?: string;
};

type AppRoute = "warehouse" | "operations" | "settings" | "ai-drafts" | "no-supplier";

const navItems: Array<{ route: AppRoute; href: string; label: string; icon: ReactNode }> = [
  { route: "warehouse", href: "/app/warehouse", label: "Каталог", icon: <PackageCheck size={16} /> },
  { route: "operations", href: "/app/operations", label: "Операции", icon: <CirclePlay size={16} /> },
  { route: "settings", href: "/app/settings", label: "Настройки", icon: <Settings size={16} /> },
  { route: "ai-drafts", href: "/app/ai-drafts", label: "AI drafts", icon: <Sparkles size={16} /> },
  { route: "no-supplier", href: "/app/no-supplier", label: "Ошибки наличия", icon: <AlertCircle size={16} /> },
];

function mutationProducts(payload?: MutationPayload | null): Product[] {
  if (!payload) return [];
  const products = [...(Array.isArray(payload.products) ? payload.products : [])];
  if (payload.product) products.push(payload.product);
  const unique = new Map(products.filter(Boolean).map((product) => [product.id, product]));
  return Array.from(unique.values());
}

function updateCachedProducts(queryClient: QueryClient, payload?: MutationPayload | null) {
  const products = mutationProducts(payload);
  if (!products.length) return;
  const byId = new Map(products.map((product) => [String(product.id), product]));
  queryClient.setQueriesData({ queryKey: ["warehouse", "page"] }, (old: WarehousePage | undefined) => {
    if (!old?.items?.length) return old;
    let changed = false;
    const items = old.items.map((item) => {
      const next = byId.get(String(item.id));
      if (!next) return item;
      changed = true;
      return { ...item, ...next };
    });
    return changed ? { ...old, items } : old;
  });
}

function errorMessage(error: unknown): string {
  if (!error) return "";
  if (error instanceof ApiError) {
    const detail = error.detail && typeof error.detail === "object" ? error.detail as Record<string, unknown> : {};
    const code = error.code || detail.code;
    const model = detail.model || detail.imageModel;
    const endpoint = detail.endpoint || detail.apiBaseUrl;
    const suffix = [code && `code: ${code}`, model && `model: ${model}`, endpoint && `endpoint: ${endpoint}`].filter(Boolean).join(" · ");
    return suffix ? `${error.message} · ${suffix}` : error.message;
  }
  return error instanceof Error ? error.message : String(error);
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function numberValue(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function productGroupKey(product: Product): string {
  const raw = asRecord(product.raw);
  const manualGroupId = String(raw.manualGroupId || raw.manual_group_id || "").trim().toLowerCase();
  if (manualGroupId) return `manual:${manualGroupId}`;
  return `offer:${String(product.offerId || product.sku || product.id).trim().toLowerCase()}`;
}

function firstImage(product?: Product): string {
  if (!product) return "";
  if (product.imageUrl) return product.imageUrl;
  if (Array.isArray(product.images) && product.images[0]) return product.images[0];
  const raw = asRecord(product.raw);
  const rawImage = raw.primaryImage || raw.image || raw.imageUrl;
  return typeof rawImage === "string" ? rawImage : "";
}

function money(value: unknown): string {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "-";
  return `${Math.round(n).toLocaleString("ru-RU")} ₽`;
}

function compactDate(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function statusLabel(product: Product): { label: string; tone: string; icon: ReactNode } {
  const stateCode = String(product.marketplaceState?.code || product.status || "").toLowerCase();
  const linked = (product.links || []).length > 0;
  if (product.archived || stateCode.includes("archiv")) return { label: "Архив", tone: "danger", icon: <Archive size={14} /> };
  if (!linked) return { label: "Нет привязки", tone: "warn", icon: <AlertCircle size={14} /> };
  if (Number(product.newPrice || product.targetPrice || 0) > 0 && Number(product.currentPrice || 0) !== Number(product.newPrice || product.targetPrice || 0)) {
    return { label: "Цена изменилась", tone: "info", icon: <RefreshCw size={14} /> };
  }
  return { label: "Готов к продаже", tone: "success", icon: <PackageCheck size={14} /> };
}

function currentRoute(): AppRoute {
  const path = window.location.pathname;
  if (path.startsWith("/app/operations")) return "operations";
  if (path.startsWith("/app/settings")) return "settings";
  if (path.startsWith("/app/ai-drafts")) return "ai-drafts";
  if (path.startsWith("/app/no-supplier")) return "no-supplier";
  return "warehouse";
}

function readFilters(): Filters {
  const params = new URLSearchParams(window.location.search);
  return {
    q: params.get("q") || "",
    marketplace: params.get("marketplace") || "all",
    linked: params.get("linked") || "all",
    state: params.get("state") || "all",
    brand: params.get("brand") || "",
    autoOnly: params.get("autoOnly") === "true",
    page: Math.max(1, Number(params.get("page") || 1) || 1),
  };
}

function selectedGroupFromPath(): string {
  const prefix = "/app/warehouse/";
  if (!window.location.pathname.startsWith(prefix)) return "";
  return decodeURIComponent(window.location.pathname.slice(prefix.length));
}

function writeWarehouseLocation(filters: Filters, selectedGroup: string, replace = false) {
  const params = new URLSearchParams();
  if (filters.q) params.set("q", filters.q);
  if (filters.marketplace !== "all") params.set("marketplace", filters.marketplace);
  if (filters.linked !== "all") params.set("linked", filters.linked);
  if (filters.state !== "all") params.set("state", filters.state);
  if (filters.brand) params.set("brand", filters.brand);
  if (filters.autoOnly) params.set("autoOnly", "true");
  if (filters.page > 1) params.set("page", String(filters.page));
  const path = selectedGroup ? `/app/warehouse/${encodeURIComponent(selectedGroup)}` : "/app/warehouse";
  const next = `${path}${params.toString() ? `?${params}` : ""}`;
  if (replace) window.history.replaceState(null, "", next);
  else window.history.pushState(null, "", next);
}

function useDebounced<T>(value: T, delayMs: number) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(id);
  }, [value, delayMs]);
  return debounced;
}

function buildPageUrl(filters: Filters) {
  const params = new URLSearchParams({
    page: String(filters.page),
    pageSize: String(pageSize),
    q: filters.q,
    marketplace: filters.marketplace,
    linked: filters.linked,
    state: filters.state,
    autoOnly: String(filters.autoOnly),
  });
  if (filters.brand) params.set("brand", filters.brand);
  return `/api/warehouse/products/page?${params}`;
}

function PageHeader({ title, subtitle, action }: { title: string; subtitle: string; action?: ReactNode }) {
  return (
    <section className="page-heading">
      <div>
        <span className="eyebrow">ДавидСклад 2.0</span>
        <h1>{title}</h1>
        <p>{subtitle}</p>
      </div>
      {action}
    </section>
  );
}

function Stat({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="stat">
      <span>{label}</span>
      <strong>{String(value ?? "-")}</strong>
    </div>
  );
}

function ProductRow({ product, selected, onSelect }: { product: Product; selected: boolean; onSelect: () => void }) {
  const status = statusLabel(product);
  const image = firstImage(product);
  return (
    <button className={`product-row ${selected ? "is-selected" : ""}`} type="button" onClick={onSelect}>
      <div className="product-thumb">
        {image ? <img src={image} alt="" loading="lazy" /> : <PackageCheck size={20} />}
      </div>
      <div className="product-main">
        <div className="product-title-line">
          <strong>{product.offerId || product.sku || product.id}</strong>
          <span className={`pill ${status.tone}`}>{status.icon}{status.label}</span>
        </div>
        <div className="product-name">{product.name || "Без названия"}</div>
        <div className="product-meta">
          <span>{product.marketplace || "marketplace"}</span>
          <span>{product.brand || "без бренда"}</span>
          <span>{(product.links || []).length} прив.</span>
          <span>остаток {Number(product.targetStock || product.stock || 0) || "-"}</span>
        </div>
      </div>
      <div className="product-price">
        <strong>{money(product.newPrice || product.targetPrice || product.currentPrice)}</strong>
        <span>текущая {money(product.currentPrice)}</span>
      </div>
      <ChevronRight className="row-chevron" size={18} />
    </button>
  );
}

function draftFromSearchRow(row: PriceMasterSearchRow): LinkDraft {
  return {
    article: row.article,
    supplierName: row.supplierName,
    keyword: row.keyword || row.name,
    priceCurrency: row.priceCurrency || row.currency || "USD",
    partnerId: row.partnerId || "",
    sourceRowId: row.rowId || row.id,
    exactName: row.keyword || row.name,
    matchType: "selected_row",
  };
}

function LinksPanel({ products, onSaved }: { products: Product[]; onSaved: () => void }) {
  const queryClient = useQueryClient();
  const productIds = products.map((item) => item.id).filter(Boolean);
  const optimisticLocks = products.map((item) => ({ id: item.id, expectedUpdatedAt: item.updatedAt || "" }));
  const links = products.flatMap((item) => (item.links || []).map((link) => ({ ...link, productId: item.id })));
  const [drafts, setDrafts] = useState<LinkDraft[]>([]);
  const [draft, setDraft] = useState<LinkDraft>({ article: "", supplierName: "", keyword: "", priceCurrency: "USD" });
  const [search, setSearch] = useState("");
  const [supplierFilter, setSupplierFilter] = useState("");
  const debouncedSearch = useDebounced(search, 250);
  const debouncedSupplier = useDebounced(supplierFilter, 250);
  const draftIsFilled = Boolean(draft.article.trim() || draft.supplierName.trim() || draft.keyword.trim());
  const pendingDrafts = draftIsFilled ? [...drafts, draft] : drafts;

  const searchQuery = useQuery({
    queryKey: ["pricemaster", "search", debouncedSearch, debouncedSupplier],
    queryFn: () => fetchJson(
      `/api/pricemaster/search?q=${encodeURIComponent(debouncedSearch)}&supplier=${encodeURIComponent(debouncedSupplier)}&limit=20`,
      PriceMasterSearchSchema,
    ),
    enabled: debouncedSearch.trim().length >= 2 || debouncedSupplier.trim().length >= 2,
    staleTime: 30_000,
  });

  const saveMutation = useMutation({
    mutationFn: async () => fetchJson("/api/warehouse/products/links/bulk", MutationProductResponseSchema, mutationBody({
      productIds,
      optimisticLocks,
      links: pendingDrafts,
    })),
    onSuccess: (payload) => {
      setDrafts([]);
      setDraft({ article: "", supplierName: "", keyword: "", priceCurrency: "USD" });
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (link: ProductLink & { productId?: string }) => fetchJson(
      `/api/warehouse/products/${encodeURIComponent(String(link.productId || productIds[0]))}/links/${encodeURIComponent(String(link.id || ""))}`,
      MutationProductResponseSchema,
      { method: "DELETE", body: JSON.stringify({ expectedUpdatedAt: products.find((item) => item.id === link.productId)?.updatedAt || products[0]?.updatedAt || "" }) },
    ),
    onSuccess: (payload) => {
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
  });

  const addDraft = (nextDraft: LinkDraft) => {
    if (!nextDraft.article.trim() && !nextDraft.supplierName.trim()) return;
    setDrafts((items) => [...items, nextDraft]);
    setDraft({ article: "", supplierName: "", keyword: "", priceCurrency: "USD" });
  };

  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>PriceMaster</span>
          <h3>Привязки поставщиков</h3>
        </div>
        <span className="section-count">{links.length}</span>
      </div>

      <div className="links-list">
        {links.length ? links.map((link) => (
          <div className="link-item" key={`${link.productId}-${link.id}-${link.article}-${link.supplierName}`}>
            <div>
              <strong>{link.article || link.supplierArticle || "без артикула"}</strong>
              <span>{link.supplierName || "поставщик не указан"}</span>
              <small>{link.keyword || "без ключевого слова"} · {link.priceCurrency || "USD"} · {compactDate(link.updatedAt || link.createdAt)}</small>
            </div>
            <button className="icon-action danger" type="button" onClick={() => deleteMutation.mutate(link)} title="Удалить привязку">
              <Trash2 size={16} />
            </button>
          </div>
        )) : <div className="soft-empty">У товара пока нет привязок PriceMaster.</div>}
      </div>

      <div className="draft-box">
        <div className="section-subtitle">Найти строку PriceMaster</div>
        <div className="draft-grid">
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Артикул, название или штрихкод" />
          <input value={supplierFilter} onChange={(event) => setSupplierFilter(event.target.value)} placeholder="Поставщик" />
        </div>
        {(searchQuery.isFetching || searchQuery.data?.rows.length || searchQuery.error) && (
          <div className="pm-results">
            {searchQuery.isFetching && <div className="soft-empty compact"><Loader2 className="spin" size={16} /> Ищу в PriceMaster...</div>}
            {searchQuery.error && <div className="inline-error">{errorMessage(searchQuery.error)}</div>}
            {searchQuery.data?.rows.map((row) => (
              <button className="pm-result" type="button" key={`${row.id}-${row.article}-${row.supplierName}`} onClick={() => addDraft(draftFromSearchRow(row))}>
                <strong>{row.article || "без артикула"}</strong>
                <span>{row.supplierName || "поставщик не указан"} · {row.keyword || row.name || "без названия"}</span>
                <small>{money(row.price)} · {row.priceCurrency || row.currency || "USD"} · {row.available ? "в наличии" : "нет наличия"} · {compactDate(row.updatedAt)}</small>
              </button>
            ))}
          </div>
        )}

        <div className="section-subtitle">Ручная привязка</div>
        <div className="draft-grid">
          <input value={draft.article} onChange={(event) => setDraft({ ...draft, article: event.target.value })} placeholder="Артикул PriceMaster" />
          <input value={draft.supplierName} onChange={(event) => setDraft({ ...draft, supplierName: event.target.value })} placeholder="Поставщик" />
          <input value={draft.keyword} onChange={(event) => setDraft({ ...draft, keyword: event.target.value })} placeholder="Ключевое слово" />
          <select value={draft.priceCurrency} onChange={(event) => setDraft({ ...draft, priceCurrency: event.target.value })}>
            <option value="USD">USD</option>
            <option value="RUB">RUB</option>
          </select>
        </div>
        <div className="draft-actions">
          <button className="secondary-action" type="button" onClick={() => addDraft(draft)}>
            <Link2 size={16} /> Добавить в черновик
          </button>
          <button className="primary-action" disabled={!pendingDrafts.length || saveMutation.isPending} type="button" onClick={() => saveMutation.mutate()}>
            {saveMutation.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить привязки
          </button>
        </div>
        <div className="draft-preview">
          <strong>Черновик: {pendingDrafts.length}</strong>
          {drafts.length ? drafts.map((item, index) => (
            <button className="draft-chip" type="button" key={`${item.article}-${index}`} onClick={() => setDrafts(drafts.filter((_, itemIndex) => itemIndex !== index))} title="Убрать из черновика">
              {item.article || "без артикула"} · {item.supplierName || "без поставщика"} <X size={12} />
            </button>
          )) : <span>Новые привязки появятся здесь до сохранения.</span>}
          {draftIsFilled && <span className="draft-chip is-current">{draft.article || "текущий ввод"} · {draft.supplierName || "без поставщика"}</span>}
        </div>
        {(saveMutation.error || deleteMutation.error) && <div className="inline-error">{errorMessage(saveMutation.error || deleteMutation.error)}</div>}
      </div>
    </section>
  );
}

function AiImagesPanel({ product, onSaved }: { product: Product; onSaved: () => void }) {
  const queryClient = useQueryClient();
  const [progress, setProgress] = useState("");
  const [freshDrafts, setFreshDrafts] = useState(product.aiImages || []);
  const visibleDrafts = freshDrafts.length ? freshDrafts : product.aiImages || [];
  useEffect(() => setFreshDrafts(product.aiImages || []), [product.id, product.aiImages]);

  const generateMutation = useMutation({
    mutationFn: async () => {
      setProgress("подготовка");
      const timer = window.setInterval(() => setProgress((current) => {
        if (current === "подготовка") return "генерация 1/5";
        if (current === "генерация 1/5") return "генерация 3/5";
        if (current === "генерация 3/5") return "сохранение";
        return current;
      }), 1400);
      try {
        return await fetchJson(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/generate`, AiImagesResponseSchema, mutationBody({
          sourceImageUrl: firstImage(product),
          prompt: `Создай 5 реалистичных marketplace-фото для товара ${product.name || product.offerId}: белый фон, премиальный свет, без лишнего текста и водяных знаков.`,
          count: 5,
          expectedUpdatedAt: product.updatedAt || "",
        }));
      } finally {
        window.clearInterval(timer);
      }
    },
    onSuccess: (payload) => {
      setProgress("готово");
      const responseDrafts = payload.product?.aiImages?.length ? payload.product.aiImages : payload.drafts;
      setFreshDrafts(responseDrafts || []);
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
    onError: () => setProgress("ошибка"),
  });

  const reviewMutation = useMutation({
    mutationFn: async ({ draftId, action }: { draftId: string; action: "approve" | "reject" }) => fetchJson(
      `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/${encodeURIComponent(draftId)}/${action}`,
      MutationProductResponseSchema,
      mutationBody({ expectedUpdatedAt: product.updatedAt || "" }),
    ),
    onSuccess: (payload) => {
      if (payload.product?.aiImages?.length) setFreshDrafts(payload.product.aiImages);
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
  });

  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>AI</span>
          <h3>Фото-черновики</h3>
        </div>
        <button className="primary-action" type="button" disabled={generateMutation.isPending} onClick={() => generateMutation.mutate()}>
          {generateMutation.isPending ? <Loader2 className="spin" size={16} /> : <ImagePlus size={16} />} Сгенерировать 5 фото
        </button>
      </div>
      {progress && <div className="progress-line"><span style={{ width: progress === "готово" ? "100%" : progress === "сохранение" ? "82%" : "48%" }} />{progress}</div>}
      <div className="ai-grid">
        {visibleDrafts.length ? visibleDrafts.slice(0, 10).map((draft) => (
          <div className={`ai-card ${draft.status === "approved" ? "is-approved" : draft.status === "rejected" ? "is-rejected" : ""}`} key={draft.id}>
            {draft.resultUrl ? <img src={draft.resultUrl} alt="" /> : <div className="image-placeholder"><Bot size={20} /></div>}
            <div className="ai-actions">
              <span>{draft.status || "pending"}{draft.variantIndex ? ` · ${draft.variantIndex}/${draft.variantTotal || "?"}` : ""}</span>
              <div className="ai-action-buttons">
                <button type="button" disabled={reviewMutation.isPending || draft.status === "approved"} onClick={() => reviewMutation.mutate({ draftId: draft.id, action: "approve" })} title="Одобрить"><Check size={15} /></button>
                <button type="button" disabled={reviewMutation.isPending || draft.status === "rejected"} onClick={() => reviewMutation.mutate({ draftId: draft.id, action: "reject" })} title="Отклонить"><X size={15} /></button>
              </div>
            </div>
          </div>
        )) : <div className="soft-empty">Здесь появятся сгенерированные изображения.</div>}
      </div>
      {(generateMutation.error || reviewMutation.error) && <div className="inline-error">{errorMessage(generateMutation.error || reviewMutation.error)}</div>}
    </section>
  );
}

function DiagnosticValue({ label, value, tone }: { label: string; value: unknown; tone?: string }) {
  const text = value === true ? "да" : value === false ? "нет" : String(value ?? "-");
  return (
    <div className={`diagnostic-value ${tone || ""}`}>
      <span>{label}</span>
      <strong>{text}</strong>
    </div>
  );
}

function commandText(command: unknown, empty = "нет отправки") {
  const item = asRecord(command);
  if (!Object.keys(item).length) return empty;
  const parts = [
    item.type,
    item.status,
    item.stock !== undefined ? `остаток ${item.stock}` : "",
    item.requestedPrice !== undefined ? `цена ${money(item.requestedPrice)}` : "",
    item.target,
    compactDate(typeof item.at === "string" ? item.at : ""),
  ].filter(Boolean);
  return parts.join(" · ") || empty;
}

function DiagnosticsPanel({ data, error, loading }: { data?: Record<string, unknown>; error: unknown; loading: boolean }) {
  if (loading) return <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю диагностику...</div>;
  if (error) return <div className="inline-error">{errorMessage(error)}</div>;
  if (!data) return <div className="soft-empty">Диагностика пока не загружена.</div>;
  const products = Array.isArray(data.products) ? data.products.map(asRecord) : [];
  const warnings = Array.isArray(data.warnings) ? data.warnings : [];
  const audit = Array.isArray(data.audit) ? data.audit.map(asRecord).slice(0, 6) : [];
  return (
    <div className="diagnostics-panel">
      <div className="diagnostics-summary">
        <DiagnosticValue label="SKU" value={data.sku} />
        <DiagnosticValue label="Найдено" value={data.matched} />
        <DiagnosticValue label="Скрыто supplier-only" value={data.hiddenSupplierOnlyMatches} />
      </div>
      {warnings.length > 0 && <div className="warning-strip">{warnings.map(String).join(" · ")}</div>}
      {products.map((item) => {
        const supplier = asRecord(item.selectedSupplier);
        const automation = asRecord(item.automation);
        return (
          <div className="diagnostic-card" key={String(item.id)}>
            <div className="diagnostic-card-head">
              <strong>{String(item.offerId || item.id || "товар")}</strong>
              <span>{String(item.marketplace || "marketplace")} · {String(item.status || "status")}</span>
            </div>
            <div className="diagnostics-summary">
              <DiagnosticValue label="Архив" value={item.archived} tone={item.archived ? "danger" : "success"} />
              <DiagnosticValue label="Привязки" value={item.hasLinks} tone={item.hasLinks ? "success" : "warn"} />
              <DiagnosticValue label="Готов" value={item.ready} tone={item.ready ? "success" : "warn"} />
              <DiagnosticValue label="Остаток" value={item.targetStock} />
              <DiagnosticValue label="Цена" value={money(item.targetPrice || item.currentPrice)} />
            </div>
            <div className="diagnostic-lines">
              <span><b>Поставщик:</b> {supplier.supplierName ? `${supplier.supplierName} · ${supplier.article || "без артикула"} · ${supplier.currency || ""}` : "не выбран"}</span>
              <span><b>Остаток:</b> {commandText(item.lastStockSend)}</span>
              <span><b>Архив:</b> {commandText(item.lastArchiveSend)}</span>
              <span><b>Yandex цена:</b> {commandText(item.lastYandexPriceSend, "нет цены")}</span>
              <span><b>Защита:</b> {automation.protectedFromNoSupplierArchive ? "не архивировать автоматикой" : "без защиты"} · {automation.wouldArchiveAsNoSupplier ? "может уйти в архив" : "не уйдет в архив"}</span>
            </div>
          </div>
        );
      })}
      {audit.length > 0 && (
        <div className="audit-list">
          <strong>Последние действия</strong>
          {audit.map((entry, index) => (
            <span key={`${entry.at}-${index}`}>{compactDate(typeof entry.at === "string" ? entry.at : "")} · {String(entry.user || "system")} · {String(entry.action || "audit")}</span>
          ))}
        </div>
      )}
    </div>
  );
}

function QuickActions({ primary, onDone }: { primary: Product; onDone: () => void }) {
  const start = useMutation({
    mutationFn: (type: string) => fetchJson("/api/operations", OperationCreateSchema, mutationBody({
      type,
      payload: { productIds: [primary.id], offerIds: [primary.offerId].filter(Boolean), limit: 1 },
    })),
    onSuccess: onDone,
  });
  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>Быстрые действия</span>
          <h3>Остаток, цена, восстановление</h3>
        </div>
      </div>
      <div className="quick-actions">
        <button className="secondary-action" type="button" onClick={() => start.mutate("linked-supplier-recovery")} disabled={start.isPending}>Восстановить товар</button>
        <button className="secondary-action" type="button" onClick={() => start.mutate("yandex-stock-sync")} disabled={start.isPending}>Отправить остаток</button>
        <button className="secondary-action" type="button" onClick={() => start.mutate("yandex-import-send")} disabled={start.isPending}>Отправить цену</button>
      </div>
      {start.error && <div className="inline-error">{errorMessage(start.error)}</div>}
    </section>
  );
}

function DetailPanel({ selectedGroup, products, onClose }: { selectedGroup: string; products: Product[]; onClose: () => void }) {
  const primary = products[0];
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(false);
  const groupQueryKey = ["warehouse", "group-detail", selectedGroup];
  const queryClient = useQueryClient();
  const diagnostics = useQuery({
    queryKey: ["warehouse", "diagnostics", primary?.offerId],
    queryFn: () => fetchJson(`/api/warehouse/products/diagnostics?sku=${encodeURIComponent(primary?.offerId || "")}`, DiagnosticsSchema),
    enabled: diagnosticsOpen && Boolean(primary?.offerId),
  });

  if (!primary) {
    return (
      <aside className="detail-panel empty-panel">
        <PackageCheck size={28} />
        <strong>Выберите товар</strong>
        <span>Здесь откроются привязки, цены, остатки, AI-фото и диагностика.</span>
      </aside>
    );
  }
  const image = firstImage(primary);
  const status = statusLabel(primary);
  const refreshDetail = () => void queryClient.invalidateQueries({ queryKey: groupQueryKey });

  return (
    <aside className="detail-panel">
      <div className="detail-head">
        <div className="detail-image">{image ? <img src={image} alt="" /> : <Sparkles size={24} />}</div>
        <div className="detail-title">
          <span className={`pill ${status.tone}`}>{status.icon}{status.label}</span>
          <h2>{primary.name || primary.offerId}</h2>
          <p>{primary.offerId} · {primary.marketplace} · {primary.brand || "без бренда"}</p>
        </div>
        <button className="mobile-close" type="button" onClick={onClose}><X size={18} /></button>
      </div>
      <div className="stats-grid">
        <Stat label="Текущая цена" value={money(primary.currentPrice)} />
        <Stat label="Новая цена" value={money(primary.newPrice || primary.targetPrice)} />
        <Stat label="Остаток" value={primary.targetStock || primary.stock || "-"} />
        <Stat label="Привязки" value={products.reduce((sum, item) => sum + (item.links || []).length, 0)} />
      </div>
      <LinksPanel products={products} onSaved={refreshDetail} />
      <QuickActions primary={primary} onDone={refreshDetail} />
      <AiImagesPanel product={primary} onSaved={refreshDetail} />
      <section className="detail-section">
        <div className="section-title">
          <div>
            <span>Диагностика</span>
            <h3>Почему товар продается или нет</h3>
          </div>
          <button className="secondary-action" type="button" onClick={() => setDiagnosticsOpen(!diagnosticsOpen)}>
            {diagnosticsOpen ? "Скрыть" : "Показать"}
          </button>
        </div>
        {diagnosticsOpen && <DiagnosticsPanel data={diagnostics.data} error={diagnostics.error} loading={diagnostics.isLoading} />}
      </section>
    </aside>
  );
}

function WarehousePage() {
  const [filters, setFilters] = useState<Filters>(() => readFilters());
  const [selectedGroup, setSelectedGroup] = useState(() => selectedGroupFromPath());
  const debouncedQ = useDebounced(filters.q, 250);
  const effectiveFilters = { ...filters, q: debouncedQ };
  const parentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const onPop = () => {
      setFilters(readFilters());
      setSelectedGroup(selectedGroupFromPath());
    };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  useEffect(() => {
    writeWarehouseLocation(filters, selectedGroup, true);
  }, [filters, selectedGroup]);

  const pageQuery = useQuery({
    queryKey: ["warehouse", "page", effectiveFilters],
    queryFn: () => fetchJson(buildPageUrl(effectiveFilters), WarehousePageSchema),
  });
  const rows = pageQuery.data?.items || [];
  const selectedRowsOnPage = useMemo(() => rows.filter((item) => productGroupKey(item) === selectedGroup), [rows, selectedGroup]);

  useEffect(() => {
    if (!selectedGroup || pageQuery.isLoading) return;
    if (!rows.some((item) => productGroupKey(item) === selectedGroup)) setSelectedGroup("");
  }, [pageQuery.isLoading, rows, selectedGroup]);

  const detailQuery = useQuery({
    queryKey: ["warehouse", "group-detail", selectedGroup],
    queryFn: () => fetchJson(`/api/warehouse/products/group-detail?group=${encodeURIComponent(selectedGroup)}&marketplace=${encodeURIComponent(filters.marketplace)}&state=${encodeURIComponent(filters.state)}`, GroupDetailSchema),
    enabled: Boolean(selectedGroup),
  });
  const detailProducts = detailQuery.data?.products?.length ? detailQuery.data.products : selectedRowsOnPage;
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 136,
    overscan: 8,
  });
  const setFilter = (key: keyof Filters, value: string | boolean | number) => {
    setFilters((current) => ({ ...current, [key]: value, page: key === "page" ? Number(value) : 1 }));
  };

  return (
    <>
      <PageHeader
        title="Новый каталог"
        subtitle="Быстрый поиск, привязки PriceMaster, остатки, цены, AI-фото и диагностика в одном рабочем экране."
        action={<button className="secondary-action" type="button" onClick={() => pageQuery.refetch()}><RefreshCw size={16} /> Обновить</button>}
      />
      <section className="toolbar">
        <label className="search-box">
          <Search size={18} />
          <input value={filters.q} onChange={(event) => setFilter("q", event.target.value)} placeholder="Поиск: 41059, CC-AASH5001, НФ-00004538" />
        </label>
        <select value={filters.marketplace} onChange={(event) => setFilter("marketplace", event.target.value)}>
          <option value="all">Все маркетплейсы</option>
          <option value="ozon">Ozon</option>
          <option value="yandex">Yandex</option>
        </select>
        <select value={filters.linked} onChange={(event) => setFilter("linked", event.target.value)}>
          <option value="all">Все привязки</option>
          <option value="linked">С привязками</option>
          <option value="unlinked">Без привязок</option>
          <option value="changed">Цена изменилась</option>
          <option value="linked_archived">Привязанные в архиве</option>
        </select>
        <select value={filters.state} onChange={(event) => setFilter("state", event.target.value)}>
          <option value="all">Все статусы</option>
          <option value="archived">Архив</option>
          <option value="inactive">Неактивные</option>
          <option value="out_of_stock">Нет остатка</option>
        </select>
        <input className="brand-filter" value={filters.brand} onChange={(event) => setFilter("brand", event.target.value)} placeholder="Бренд" />
        <label className="toggle-filter">
          <input type="checkbox" checked={filters.autoOnly} onChange={(event) => setFilter("autoOnly", event.target.checked)} />
          AUTO
        </label>
      </section>
      <section className="summary-grid">
        <Stat label="Найдено" value={pageQuery.data?.total || 0} />
        <Stat label="Всего" value={pageQuery.data?.totalAll || 0} />
        <Stat label="Готовы" value={pageQuery.data?.ready || 0} />
        <Stat label="Изменения" value={pageQuery.data?.changed || 0} />
        <Stat label="Без поставщика" value={pageQuery.data?.withoutSupplier || 0} />
      </section>
      <section className={`workspace ${selectedGroup ? "detail-open" : ""}`}>
        <div className="list-panel">
          {pageQuery.error && <div className="inline-error">{errorMessage(pageQuery.error)}</div>}
          <div ref={parentRef} className="virtual-list">
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
              {virtualizer.getVirtualItems().map((virtualRow) => {
                const product = rows[virtualRow.index];
                const key = productGroupKey(product);
                return (
                  <div key={product.id} style={{ position: "absolute", top: 0, left: 0, width: "100%", transform: `translateY(${virtualRow.start}px)` }}>
                    <ProductRow product={product} selected={key === selectedGroup} onSelect={() => setSelectedGroup(key)} />
                  </div>
                );
              })}
            </div>
            {pageQuery.isLoading && <div className="list-loading"><Loader2 className="spin" /> Загружаю каталог...</div>}
            {!pageQuery.isLoading && !rows.length && <div className="list-loading">Ничего не найдено.</div>}
          </div>
          <div className="pager">
            <button disabled={filters.page <= 1} onClick={() => setFilter("page", Math.max(1, filters.page - 1))}>Назад</button>
            <span>Страница {filters.page}</span>
            <button disabled={!pageQuery.data?.hasMore} onClick={() => setFilter("page", filters.page + 1)}>Дальше</button>
          </div>
        </div>
        <DetailPanel selectedGroup={selectedGroup} products={detailProducts} onClose={() => setSelectedGroup("")} />
      </section>
    </>
  );
}

function jobStatusLabel(status: unknown): string {
  return ({ queued: "Ожидает", running: "В работе", completed: "Готово", failed: "Ошибка" } as Record<string, string>)[String(status || "")] || String(status || "-");
}

function jobSummary(job: Record<string, unknown>): string {
  const result = asRecord(job.result);
  const parts = [
    result.partial ? "частично" : "",
    Number.isFinite(Number(result.candidates)) ? `привязанных ${result.candidates}` : "",
    Number.isFinite(Number(result.recovered)) ? `восстановлено ${result.recovered}` : "",
    Number.isFinite(Number(result.sellableRecovered)) ? `готовы ${result.sellableRecovered}` : "",
    Number.isFinite(Number(result.unarchived)) ? `разархивировано ${result.unarchived}` : "",
    Number.isFinite(Number(result.stockFailed)) && Number(result.stockFailed) > 0 ? `ошибки остатков ${result.stockFailed}` : "",
    Number.isFinite(Number(result.unarchiveFailed)) && Number(result.unarchiveFailed) > 0 ? `ошибки архива ${result.unarchiveFailed}` : "",
    Number.isFinite(Number(result.qualityLoaded)) ? `quality ${result.qualityLoaded}` : "",
    Number.isFinite(Number(result.lowQuality)) ? `ниже порога ${result.lowQuality}` : "",
    Number.isFinite(Number(result.draftsCreated)) ? `AI drafts ${result.draftsCreated}` : "",
    Number.isFinite(Number(result.imageDraftsCreated)) ? `AI фото ${result.imageDraftsCreated}` : "",
    String(result.imageGenerationStoppedReason || ""),
    String(job.error || ""),
  ].filter(Boolean);
  return parts.join(" · ") || String(job.summary || "");
}

function OperationsPage() {
  const [limit, setLimit] = useState(30000);
  const [sendLimit, setSendLimit] = useState(5000);
  const [stock, setStock] = useState(3);
  const [threshold, setThreshold] = useState(40);
  const [draftLimit, setDraftLimit] = useState(20);
  const queryClient = useQueryClient();
  const jobsQuery = useQuery({
    queryKey: ["operations"],
    queryFn: () => fetchJson("/api/operations?limit=80", OperationsSchema),
    refetchInterval: 5000,
  });
  const startMutation = useMutation({
    mutationFn: (type: string) => fetchJson("/api/operations", OperationCreateSchema, mutationBody({
      type,
      payload: type === "yandex-import-send"
        ? { limit, sendLimit }
        : type === "restore-archived-stock"
          ? { limit, stock, marketplace: "yandex" }
          : type === "yandex-card-quality-ai-drafts"
            ? { limit, threshold, draftLimit, generateImages: true }
            : { limit },
    })),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["operations"] }),
  });
  const jobs = jobsQuery.data?.jobs || [];
  return (
    <>
      <PageHeader title="Операции" subtitle="Массовые задачи, прогресс, частичные ошибки и быстрый повтор через очередь." action={<button className="secondary-action" onClick={() => jobsQuery.refetch()}><RefreshCw size={16} /> Обновить</button>} />
      <section className="control-grid">
        <label>Лимит товаров<input type="number" value={limit} onChange={(event) => setLimit(numberValue(event.target.value, 30000))} /></label>
        <label>Лимит отправки<input type="number" value={sendLimit} onChange={(event) => setSendLimit(numberValue(event.target.value, 5000))} /></label>
        <label>Остаток восстановления<input type="number" value={stock} onChange={(event) => setStock(numberValue(event.target.value, 3))} /></label>
        <label>Качество до<input type="number" value={threshold} onChange={(event) => setThreshold(numberValue(event.target.value, 40))} /></label>
        <label>AI drafts<input type="number" value={draftLimit} onChange={(event) => setDraftLimit(numberValue(event.target.value, 20))} /></label>
      </section>
      <section className="action-strip">
        <button className="primary-action" onClick={() => startMutation.mutate("linked-supplier-recovery")} disabled={startMutation.isPending}>Восстановить привязанные</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("restore-archived-stock")} disabled={startMutation.isPending}>Восстановить архив</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("yandex-card-quality-ai-drafts")} disabled={startMutation.isPending}>AI качество карточек</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("health-deep")} disabled={startMutation.isPending}>Глубокий health</button>
      </section>
      {startMutation.error && <div className="inline-error">{errorMessage(startMutation.error)}</div>}
      <section className="table-panel">
        {jobsQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю операции...</div>}
        {jobs.map((job) => (
          <article className="job-row" key={String(job.id)}>
            <div>
              <strong>{String(job.title || job.type)}</strong>
              <span>{jobStatusLabel(job.status)} · {String(job.user || "system")} · {compactDate(String(job.createdAt || ""))}</span>
              <small>{jobSummary(job)}</small>
            </div>
            <div className="progress-pill">{Math.round(numberValue(job.progress, 0))}%</div>
          </article>
        ))}
        {!jobsQuery.isLoading && !jobs.length && <div className="soft-empty">Задач пока нет.</div>}
      </section>
    </>
  );
}

function SettingsPage() {
  const queryClient = useQueryClient();
  const settingsQuery = useQuery({ queryKey: ["settings"], queryFn: () => fetchJson("/api/settings", SettingsResponseSchema) });
  const settings = settingsQuery.data?.settings || {};
  const ai = asRecord(settings.ai);
  const markups = asRecord(settings.defaultMarkups);
  const [draft, setDraft] = useState<Record<string, unknown>>({});

  useEffect(() => {
    if (settingsQuery.data?.settings) setDraft(settingsQuery.data.settings);
  }, [settingsQuery.data]);

  const draftAi = asRecord(draft.ai);
  const draftMarkups = asRecord(draft.defaultMarkups);
  const save = useMutation({
    mutationFn: () => fetchJson("/api/settings", SettingsResponseSchema, mutationBody(draft)),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
  const testAi = useMutation({
    mutationFn: () => fetchJson("/api/settings/ai/test", SettingsResponseSchema, mutationBody({ ai: draftAi })),
  });
  const update = (patch: Record<string, unknown>) => setDraft((current) => ({ ...current, ...patch }));
  const updateAi = (patch: Record<string, unknown>) => update({ ai: { ...draftAi, ...patch } });
  const updateMarkups = (patch: Record<string, unknown>) => update({ defaultMarkups: { ...draftMarkups, ...patch } });

  return (
    <>
      <PageHeader title="Настройки" subtitle="Курс, наценки, правила доступности, маркетплейсы и AI-провайдер в новом интерфейсе." action={<button className="primary-action" onClick={() => save.mutate()} disabled={save.isPending}><Save size={16} /> Сохранить</button>} />
      {settingsQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю настройки...</div>}
      <section className="settings-grid">
        <div className="settings-panel">
          <div className="section-title"><div><span>Цены</span><h3>Курс и наценки</h3></div></div>
          <label>Курс USD<input type="number" value={String(draft.fixedUsdRate ?? settings.fixedUsdRate ?? "")} onChange={(event) => update({ fixedUsdRate: numberValue(event.target.value) })} /></label>
          <label>Наценка Ozon<input type="number" step="0.01" value={String(draftMarkups.ozon ?? markups.ozon ?? "")} onChange={(event) => updateMarkups({ ozon: numberValue(event.target.value) })} /></label>
          <label>Наценка Yandex<input type="number" step="0.01" value={String(draftMarkups.yandex ?? markups.yandex ?? "")} onChange={(event) => updateMarkups({ yandex: numberValue(event.target.value) })} /></label>
          <div className="soft-empty compact">После изменения курса или наценки backend ставит пересчет цен в очередь.</div>
        </div>
        <div className="settings-panel">
          <div className="section-title"><div><span>AI</span><h3>Провайдер и модели</h3></div><button className="secondary-action" onClick={() => testAi.mutate()} disabled={testAi.isPending}>Тест</button></div>
          <label>Provider ID<input value={String(draftAi.providerId ?? ai.providerId ?? "")} onChange={(event) => updateAi({ providerId: event.target.value })} /></label>
          <label>Base URL<input value={String(draftAi.baseUrl ?? ai.baseUrl ?? "")} onChange={(event) => updateAi({ baseUrl: event.target.value })} /></label>
          <label>Text model<input value={String(draftAi.textModel ?? ai.textModel ?? "")} onChange={(event) => updateAi({ textModel: event.target.value })} /></label>
          <label>Image model<input value={String(draftAi.imageModel ?? ai.imageModel ?? "gpt-image-2")} onChange={(event) => updateAi({ imageModel: event.target.value })} /></label>
          <label>API key<input type="password" placeholder={ai.apiKeySet ? "ключ сохранен" : "вставьте ключ"} onChange={(event) => updateAi({ apiKey: event.target.value })} /></label>
          {testAi.error && <div className="inline-error">{errorMessage(testAi.error)}</div>}
          {testAi.isSuccess && <div className="success-strip">AI подключен.</div>}
        </div>
        <div className="settings-panel">
          <div className="section-title"><div><span>Yandex</span><h3>Склад остатков</h3></div></div>
          <label>Warehouse ID<input value={String(asRecord(draft.yandex).warehouseId ?? asRecord(settings.yandex).warehouseId ?? "128820967")} onChange={(event) => update({ yandex: { ...asRecord(draft.yandex), warehouseId: event.target.value } })} /></label>
          <div className="soft-empty compact">Остатки должны уходить только в Magic Stick: 128820967.</div>
        </div>
      </section>
      {(save.error) && <div className="inline-error">{errorMessage(save.error)}</div>}
      {save.isSuccess && <div className="success-strip">Настройки сохранены. Если менялись цены, пересчет поставлен в очередь.</div>}
    </>
  );
}

function NoSupplierPage() {
  const query = useQuery({ queryKey: ["no-supplier"], queryFn: () => fetchJson("/api/warehouse/no-supplier", NoSupplierSchema) });
  const alerts = query.data?.alerts || [];
  return (
    <>
      <PageHeader title="Ошибки наличия" subtitle="Товары с привязками, у которых сейчас нет активного поставщика или есть риск некорректного остатка." action={<button className="secondary-action" onClick={() => query.refetch()}><RefreshCw size={16} /> Обновить</button>} />
      <section className="summary-grid three">
        <Stat label="Всего товаров" value={query.data?.total || 0} />
        <Stat label="Без поставщика" value={query.data?.withoutSupplier || 0} />
        <Stat label="В списке" value={alerts.length} />
      </section>
      <section className="table-panel">
        {query.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю ошибки...</div>}
        {alerts.map((item) => (
          <article className="job-row" key={String(item.id || item.offerId)}>
            <div>
              <strong>{String(item.offerId || item.name || item.id)}</strong>
              <span>{String(item.name || "Без названия")} · {String(item.marketplace || "-")} · привязок {String(item.supplierCount || 0)} · активных {String(item.availableSupplierCount || 0)}</span>
            </div>
            <a className="secondary-action" href={`/app/warehouse/${encodeURIComponent(`offer:${String(item.offerId || "").toLowerCase()}`)}?q=${encodeURIComponent(String(item.offerId || ""))}`}>Открыть</a>
          </article>
        ))}
        {!query.isLoading && !alerts.length && <div className="soft-empty">Ошибок наличия нет.</div>}
      </section>
    </>
  );
}

function AiDraftsPage() {
  const [status, setStatus] = useState("pending");
  const [threshold, setThreshold] = useState(40);
  const [limit, setLimit] = useState(300);
  const queryClient = useQueryClient();
  const draftsQuery = useQuery({
    queryKey: ["ai-drafts", status],
    queryFn: () => fetchJson(`/api/warehouse/ai-drafts?status=${encodeURIComponent(status)}&marketplace=yandex&limit=300`, AiDraftsSchema),
  });
  const candidatesQuery = useQuery({
    queryKey: ["quality-candidates", threshold, limit],
    queryFn: () => fetchJson(`/api/warehouse/yandex-quality-candidates?cached=1&threshold=${threshold}&limit=${limit}&resultLimit=300`, YandexQualityCandidatesSchema),
  });
  const generate = useMutation({
    mutationFn: (productId: string) => fetchJson(`/api/warehouse/products/${encodeURIComponent(productId)}/yandex-quality-draft/generate`, AiImagesResponseSchema, mutationBody({ count: 5, imagesCount: 5 })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["ai-drafts"] });
      void queryClient.invalidateQueries({ queryKey: ["quality-candidates"] });
    },
  });
  const send = useMutation({
    mutationFn: (productId: string) => fetchJson(`/api/warehouse/products/${encodeURIComponent(productId)}/yandex-quality-draft/send`, MutationProductResponseSchema, mutationBody({})),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["ai-drafts"] });
      void queryClient.invalidateQueries({ queryKey: ["quality-candidates"] });
    },
  });
  const drafts = draftsQuery.data?.drafts || [];
  const candidates = candidatesQuery.data?.products || [];
  return (
    <>
      <PageHeader title="AI drafts" subtitle="Карточки качества ниже порога, генерация текста и 5 фото, ручная отправка на маркетплейс." action={<button className="secondary-action" onClick={() => { candidatesQuery.refetch(); draftsQuery.refetch(); }}><RefreshCw size={16} /> Обновить</button>} />
      <section className="control-grid">
        <label>Статус<select value={status} onChange={(event) => setStatus(event.target.value)}><option value="pending">На проверке</option><option value="approved">Одобрено</option><option value="rejected">Отклонено</option><option value="">Все</option></select></label>
        <label>Качество до<input type="number" value={threshold} onChange={(event) => setThreshold(numberValue(event.target.value, 40))} /></label>
        <label>Лимит проверки<input type="number" value={limit} onChange={(event) => setLimit(numberValue(event.target.value, 300))} /></label>
      </section>
      <section className="summary-grid three">
        <Stat label="Проверено" value={candidatesQuery.data?.checked || 0} />
        <Stat label="Ниже порога" value={candidatesQuery.data?.total || 0} />
        <Stat label="Черновики" value={draftsQuery.data?.total || 0} />
      </section>
      {(generate.error || send.error) && <div className="inline-error">{errorMessage(generate.error || send.error)}</div>}
      <section className="table-panel">
        <div className="section-title"><div><span>Кандидаты</span><h3>Качество карточки до {threshold}</h3></div></div>
        {candidatesQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю кандидатов...</div>}
        {candidates.slice(0, 80).map((row) => {
          const product = asRecord(row.product || row);
          const quality = asRecord(row.cardQuality || product.cardQuality);
          const productId = String(product.id || row.id || "");
          return (
            <article className="job-row" key={productId || String(product.offerId)}>
              <div>
                <strong>{String(product.offerId || product.name || productId)}</strong>
                <span>{String(product.name || "Без названия")} · качество {String(quality.contentRating || row.quality || "-")}</span>
              </div>
              <div className="row-actions">
                <button className="secondary-action" onClick={() => generate.mutate(productId)} disabled={generate.isPending || !productId}>Текст + 5 фото</button>
                <button className="primary-action" onClick={() => send.mutate(productId)} disabled={send.isPending || !productId}>Отправить</button>
              </div>
            </article>
          );
        })}
      </section>
      <section className="table-panel">
        <div className="section-title"><div><span>Черновики</span><h3>На ручной проверке</h3></div></div>
        {draftsQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю черновики...</div>}
        <div className="draft-card-grid">
          {drafts.map((row) => {
            const product = asRecord(row.product);
            const draft = asRecord(row.draft);
            const related = asRecord(row.relatedImageDraft);
            const imageUrl = String(draft.resultUrl || related.resultUrl || product.imageUrl || "");
            return (
              <article className="ai-review-card" key={`${product.id}-${draft.id}`}>
                <div className="ai-review-image">{imageUrl ? <img src={imageUrl} alt="" /> : <Bot size={22} />}</div>
                <div>
                  <strong>{String(product.offerId || product.name || product.id)}</strong>
                  <span>{String(row.type || "draft")} · {String(draft.status || "pending")} · качество {String(asRecord(product.cardQuality).contentRating || "-")}</span>
                  <p>{String(draft.description || draft.text || draft.prompt || "Черновик без текста").slice(0, 360)}</p>
                </div>
              </article>
            );
          })}
        </div>
        {!draftsQuery.isLoading && !drafts.length && <div className="soft-empty">Черновиков пока нет.</div>}
      </section>
    </>
  );
}

function AppShell() {
  const [route, setRoute] = useState<AppRoute>(() => currentRoute());
  useEffect(() => {
    const onPop = () => setRoute(currentRoute());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const navigate = (event: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    event.preventDefault();
    window.history.pushState(null, "", href);
    setRoute(currentRoute());
  };
  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <span className="eyebrow">Рабочий интерфейс</span>
          <h1>ДавидСклад</h1>
        </div>
        <nav>
          {navItems.map((item) => (
            <a key={item.route} className={route === item.route ? "is-active" : ""} href={item.href} onClick={(event) => navigate(event, item.href)}>
              {item.icon}{item.label}
            </a>
          ))}
          <a href="/legacy">Legacy</a>
        </nav>
      </header>
      {route === "operations" ? <OperationsPage /> : null}
      {route === "settings" ? <SettingsPage /> : null}
      {route === "ai-drafts" ? <AiDraftsPage /> : null}
      {route === "no-supplier" ? <NoSupplierPage /> : null}
      {route === "warehouse" ? <WarehousePage /> : null}
    </main>
  );
}

export function App() {
  return <AppShell />;
}

export default App;
