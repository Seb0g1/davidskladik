import {
  AlertCircle,
  Archive,
  Bot,
  Check,
  ChevronRight,
  ImagePlus,
  Link2,
  Loader2,
  PackageCheck,
  RefreshCw,
  Save,
  Search,
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
  AiImagesResponseSchema,
  DiagnosticsSchema,
  Filters,
  GroupDetailSchema,
  MutationProductResponseSchema,
  Product,
  ProductLink,
  WarehousePageSchema,
} from "./types";
import type { WarehousePage } from "./types";

const pageSize = 80;

type MutationPayload = {
  product?: Product;
  products?: Product[];
};

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

function productGroupKey(product: Product): string {
  const raw = product.raw && typeof product.raw === "object" && !Array.isArray(product.raw)
    ? product.raw as Record<string, unknown>
    : {};
  const manualGroupId = String(raw.manualGroupId || raw.manual_group_id || "").trim().toLowerCase();
  if (manualGroupId) return `manual:${manualGroupId}`;
  return `offer:${String(product.offerId || product.sku || product.id).trim().toLowerCase()}`;
}

function firstImage(product?: Product): string {
  if (!product) return "";
  if (product.imageUrl) return product.imageUrl;
  if (Array.isArray(product.images) && product.images[0]) return product.images[0];
  const raw = product.raw && typeof product.raw === "object" ? product.raw as Record<string, unknown> : {};
  const rawImage = raw.primaryImage || raw.image || raw.imageUrl;
  return typeof rawImage === "string" ? rawImage : "";
}

function money(value: unknown): string {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "—";
  return `${Math.round(n).toLocaleString("ru-RU")} ₽`;
}

function compactDate(value?: string | null): string {
  if (!value) return "—";
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
    return { label: "Цена изменена", tone: "info", icon: <RefreshCw size={14} /> };
  }
  return { label: "Готов к продаже", tone: "success", icon: <PackageCheck size={14} /> };
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

function writeLocation(filters: Filters, selectedGroup: string, replace = false) {
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
          <span>остаток {Number(product.targetStock || product.stock || 0) || "—"}</span>
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

function Stat({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="stat">
      <span>{label}</span>
      <strong>{String(value ?? "—")}</strong>
    </div>
  );
}

function LinksPanel({ products, onSaved }: { products: Product[]; onSaved: () => void }) {
  const queryClient = useQueryClient();
  const productIds = products.map((item) => item.id).filter(Boolean);
  const optimisticLocks = products.map((item) => ({ id: item.id, expectedUpdatedAt: item.updatedAt || "" }));
  const links = products.flatMap((item) => (item.links || []).map((link) => ({ ...link, productId: item.id })));
  const [drafts, setDrafts] = useState<Array<{ article: string; supplierName: string; keyword: string; priceCurrency: string }>>([]);
  const [draft, setDraft] = useState({ article: "", supplierName: "", keyword: "", priceCurrency: "USD" });
  const draftIsFilled = Boolean(draft.article.trim() || draft.supplierName.trim() || draft.keyword.trim());
  const pendingDrafts = draftIsFilled ? [...drafts, draft] : drafts;

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
        <div className="draft-grid">
          <input value={draft.article} onChange={(e) => setDraft({ ...draft, article: e.target.value })} placeholder="Артикул или строка PriceMaster" />
          <input value={draft.supplierName} onChange={(e) => setDraft({ ...draft, supplierName: e.target.value })} placeholder="Поставщик" />
          <input value={draft.keyword} onChange={(e) => setDraft({ ...draft, keyword: e.target.value })} placeholder="Ключевое слово" />
          <select value={draft.priceCurrency} onChange={(e) => setDraft({ ...draft, priceCurrency: e.target.value })}>
            <option value="USD">USD</option>
            <option value="RUB">RUB</option>
          </select>
        </div>
        <div className="draft-actions">
          <button className="secondary-action" type="button" onClick={() => {
            if (!draft.article.trim() && !draft.supplierName.trim()) return;
            setDrafts([...drafts, draft]);
            setDraft({ article: "", supplierName: "", keyword: "", priceCurrency: "USD" });
          }}>
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
  const drafts = product.aiImages || [];
  const visibleDrafts = freshDrafts.length ? freshDrafts : drafts;
  useEffect(() => {
    setFreshDrafts(product.aiImages || []);
  }, [product.id, product.aiImages]);
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
  const text = value === true ? "да" : value === false ? "нет" : String(value ?? "—");
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
  const refreshDetail = () => {
    void queryClient.invalidateQueries({ queryKey: groupQueryKey });
  };

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
        <Stat label="Остаток" value={primary.targetStock || primary.stock || "—"} />
        <Stat label="Привязки" value={products.reduce((sum, item) => sum + (item.links || []).length, 0)} />
      </div>
      <LinksPanel products={products} onSaved={refreshDetail} />
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
        {diagnosticsOpen && (
          <DiagnosticsPanel data={diagnostics.data} error={diagnostics.error} loading={diagnostics.isLoading} />
        )}
      </section>
    </aside>
  );
}

function WarehouseApp() {
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
    writeLocation(filters, selectedGroup, true);
  }, [filters, selectedGroup]);

  const pageQuery = useQuery({
    queryKey: ["warehouse", "page", effectiveFilters],
    queryFn: () => fetchJson(buildPageUrl(effectiveFilters), WarehousePageSchema),
  });

  const rows = pageQuery.data?.items || [];
  const selectedProductsFromPage = useMemo(
    () => rows.filter((item) => productGroupKey(item) === selectedGroup),
    [rows, selectedGroup],
  );

  useEffect(() => {
    if (!selectedGroup || pageQuery.isLoading) return;
    if (!rows.some((item) => productGroupKey(item) === selectedGroup)) setSelectedGroup("");
  }, [pageQuery.isLoading, rows, selectedGroup]);

  const detailQuery = useQuery({
    queryKey: ["warehouse", "group-detail", selectedGroup],
    queryFn: () => fetchJson(`/api/warehouse/products/group-detail?group=${encodeURIComponent(selectedGroup)}&marketplace=${encodeURIComponent(filters.marketplace)}&state=${encodeURIComponent(filters.state)}`, GroupDetailSchema),
    enabled: Boolean(selectedGroup),
  });

  const detailProducts = detailQuery.data?.products?.length ? detailQuery.data.products : selectedProductsFromPage;
  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 136,
    overscan: 8,
  });

  const setFilter = <K extends keyof Filters>(key: K, value: Filters[K]) => {
    setFilters((current) => ({ ...current, [key]: value, page: key === "page" ? Number(value) : 1 }));
  };

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <span className="eyebrow">ДавидСклад 2.0</span>
          <h1>Новый каталог</h1>
        </div>
        <nav>
          <a href="/">Старый склад</a>
          <a href="/ai-drafts.html">AI drafts</a>
          <a href="/settings.html">Настройки</a>
        </nav>
      </header>

      <section className="toolbar">
        <label className="search-box">
          <Search size={18} />
          <input value={filters.q} onChange={(e) => setFilter("q", e.target.value)} placeholder="Поиск: 41059, CC-AASH5001, НФ-00004538" />
        </label>
        <select value={filters.marketplace} onChange={(e) => setFilter("marketplace", e.target.value)}>
          <option value="all">Все маркетплейсы</option>
          <option value="ozon">Ozon</option>
          <option value="yandex">Yandex</option>
        </select>
        <select value={filters.linked} onChange={(e) => setFilter("linked", e.target.value)}>
          <option value="all">Все привязки</option>
          <option value="linked">С привязками</option>
          <option value="unlinked">Без привязок</option>
          <option value="changed">Цена изменилась</option>
          <option value="linked_archived">Привязанные в архиве</option>
        </select>
        <select value={filters.state} onChange={(e) => setFilter("state", e.target.value)}>
          <option value="all">Все статусы</option>
          <option value="archived">Архив</option>
          <option value="inactive">Неактивные</option>
          <option value="out_of_stock">Нет остатка</option>
        </select>
        <input className="brand-filter" value={filters.brand} onChange={(e) => setFilter("brand", e.target.value)} placeholder="Бренд" />
        <label className="toggle-filter">
          <input type="checkbox" checked={filters.autoOnly} onChange={(e) => setFilter("autoOnly", e.target.checked)} />
          AUTO
        </label>
        <button className="secondary-action" type="button" onClick={() => pageQuery.refetch()}>
          <RefreshCw size={16} /> Обновить
        </button>
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
            <div style={{ height: `${rowVirtualizer.getTotalSize()}px`, position: "relative" }}>
              {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                const product = rows[virtualRow.index];
                const groupKey = productGroupKey(product);
                return (
                  <div
                    key={product.id}
                    style={{
                      position: "absolute",
                      top: 0,
                      left: 0,
                      width: "100%",
                      transform: `translateY(${virtualRow.start}px)`,
                    }}
                  >
                    <ProductRow
                      product={product}
                      selected={groupKey === selectedGroup}
                      onSelect={() => setSelectedGroup(groupKey)}
                    />
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
    </main>
  );
}

export function App() {
  return <WarehouseApp />;
}
