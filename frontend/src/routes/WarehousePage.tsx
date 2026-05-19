import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Bot, Check, ChevronRight, Copy, ImagePlus, Link2, Loader2, PackageCheck, RefreshCw, Save, Search, Sparkles, Trash2, X } from "lucide-react";
import { fetchJson, mutationBody, patchBody } from "../api";
import { AiImagesResponseSchema, DiagnosticsSchema, Filters, GroupDetailSchema, MutationProductResponseSchema, OperationCreateSchema, PriceMasterSearchRow, PriceMasterSearchSchema, Product, ProductLink, WarehousePageSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { asRecord, compactDate, copyPlainText, errorMessage, money, numberValue, updateCachedProducts, useDebounced } from "../lib/common";
import { ProductGroup, firstImage, groupPrice, groupProductsForList, groupStatusLabel, marketplaceLabel, preferredGroupPrimary, statusLabel } from "../lib/warehouse";

const pageSize = 80;

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

function ProductGroupRow({ group, selected, onSelect }: { group: ProductGroup; selected: boolean; onSelect: () => void }) {
  const primary = group.primary;
  const status = groupStatusLabel(group);
  const image = firstImage(primary);
  const offer = primary.offerId || primary.sku || primary.id;
  return (
    <button className={`product-row group-row ${selected ? "is-selected" : ""}`} type="button" onClick={onSelect}>
      <div className="product-thumb">
        {image ? <img src={image} alt="" loading="lazy" /> : <PackageCheck size={20} />}
      </div>
      <div className="product-main">
        <div className="product-title-line">
          <strong>{offer}</strong>
          <span className={`pill ${status.tone}`}>{status.icon}{status.label}</span>
        </div>
        <div className="product-name">{primary.name || "Без названия"}</div>
        <div className="market-badges" aria-label="marketplaces">
          {group.marketplaces.map((marketplace) => <span className="market-badge" key={marketplace}>{marketplace}</span>)}
          <span className="market-badge muted">{group.products.length} стр.</span>
          <span className="market-badge muted">{group.links.length} прив.</span>
        </div>
        <div className="product-meta">
          <span>{primary.brand || "без бренда"}</span>
          <span>готовы {group.statusSummary.ready}/{group.statusSummary.total}</span>
          <span>архив {group.statusSummary.archived}</span>
        </div>
      </div>
      <div className="product-price">
        <strong>{money(groupPrice(group))}</strong>
        <span>мин. по группе</span>
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

function linkSourceId(link: ProductLink): string {
  return String(link.sourceRowId || link.rowId || link.id || "").trim();
}

function linkMatchText(link: ProductLink): string {
  const type = String(link.matchType || "").toLowerCase();
  if (type === "selected_row") return "точная строка PriceMaster";
  if (type) return type;
  return linkSourceId(link) ? "строка PriceMaster" : "ручная привязка";
}

function linkTitleText(link: ProductLink): string {
  return link.exactName || link.keyword || link.article || link.supplierArticle || "PriceMaster строка";
}

function LinksPanel({ products, onSaved }: { products: Product[]; onSaved: () => void }) {
  const queryClient = useQueryClient();
  const productIds = products.map((item) => item.id).filter(Boolean);
  const optimisticLocks = products.map((item) => ({ id: item.id, expectedUpdatedAt: item.updatedAt || "" }));
  const links = products.flatMap((item) => (item.links || []).map((link) => ({
    ...link,
    productId: item.id,
    productOfferId: item.offerId,
    productMarketplace: marketplaceLabel(item.marketplace),
    productTarget: item.target || "",
    productSelectedSupplier: item.selectedSupplier,
  })));
  const selectedSupplierCount = products.filter((item) => item.selectedSupplier).length;
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
          <h3>Точная связь с PriceMaster</h3>
        </div>
        <span className="section-count">{links.length}</span>
      </div>

      <div className="pm-link-summary">
        <div>
          <strong>{links.length}</strong>
          <span>строк PM сохранено</span>
        </div>
        <div>
          <strong>{products.length}</strong>
          <span>карточек в группе</span>
        </div>
        <div>
          <strong>{selectedSupplierCount}/{products.length}</strong>
          <span>выбран поставщик</span>
        </div>
      </div>

      <div className="links-list">
        {links.length ? links.map((link) => (
          <div className="link-item pm-link-item" key={`${link.productId}-${link.id}-${link.article}-${link.supplierName}`}>
            <div className="pm-link-body">
              <div className="pm-link-head">
                <div>
                  <strong>{link.article || link.supplierArticle || "без артикула PriceMaster"}</strong>
                  <span>{link.supplierName || "поставщик не указан"}</span>
                </div>
                <span className="pm-source-pill">{linkMatchText(link)}</span>
              </div>
              <div className="pm-link-grid">
                <span><b>Row ID</b>{linkSourceId(link) || "не сохранен"}</span>
                <span><b>Partner ID</b>{link.partnerId || "не указан"}</span>
                <span><b>Название/ключ</b>{linkTitleText(link)}</span>
                <span><b>Валюта</b>{link.priceCurrency || "USD"}</span>
                <span><b>Обновлено</b>{compactDate(link.updatedAt || link.createdAt)}</span>
                <span><b>Кто изменил</b>{link.updatedBy || link.createdBy || "system"}</span>
              </div>
              <div className="pm-route-list">
                <span className="pm-route-chip">
                  {link.productMarketplace} → {link.productOfferId || link.productId}{link.productTarget ? ` · ${link.productTarget}` : ""}
                </span>
                <span className="pm-route-chip muted">
                  выбран: {supplierText(link.productSelectedSupplier)}
                </span>
              </div>
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
                <div className="pm-result-head">
                  <strong>{row.article || "без артикула"}</strong>
                  <span>выбрать точную строку</span>
                </div>
                <span>{row.supplierName || "поставщик не указан"} · {row.keyword || row.name || "без названия"}</span>
                <small>row {row.rowId || row.id} · partner {row.partnerId || "-"} · {money(row.price)} · {row.priceCurrency || row.currency || "USD"} · {row.available ? "в наличии" : "нет наличия"} · {compactDate(row.updatedAt)}</small>
              </button>
            ))}
          </div>
        )}

        <div className="section-subtitle">Ручная привязка</div>
        <div className="info-strip compact">
          Лучше выбирать строку из поиска: тогда сохраняется rowId, partnerId и точное название PriceMaster. Ручная привязка нужна только как запасной вариант.
        </div>
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

function saleTone(code: unknown) {
  const text = String(code || "");
  if (text === "ready") return "success";
  if (text === "api_error" || text === "archived") return "danger";
  if (text === "api_pending" || text === "no_supplier" || text === "no_stock" || text === "no_links") return "warn";
  return "";
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

function supplierText(supplierInput: unknown) {
  const supplier = asRecord(supplierInput);
  if (!Object.keys(supplier).length || !supplier.supplierName) return "не выбран";
  return `${supplier.supplierName}${supplier.article ? ` · ${supplier.article}` : ""}${supplier.currency ? ` · ${supplier.currency}` : ""}`;
}

function DiagnosticsPanel({ data, error, loading }: { data?: Record<string, unknown>; error: unknown; loading: boolean }) {
  if (loading) return <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю диагностику...</div>;
  if (error) return <div className="inline-error">{errorMessage(error)}</div>;
  if (!data) return <div className="soft-empty">Диагностика пока не загружена.</div>;
  const products = Array.isArray(data.products) ? data.products.map(asRecord) : [];
  const warnings = Array.isArray(data.warnings) ? data.warnings : [];
  const audit = Array.isArray(data.audit) ? data.audit.map(asRecord).slice(0, 6) : [];
  const group = asRecord(data.group);
  const summary = asRecord(data.statusSummary || group.statusSummary);
  const marketplaces = Array.isArray(summary.marketplaces) ? summary.marketplaces.map(String).join(", ") : "-";
  return (
    <div className="diagnostics-panel">
      <div className="diagnostics-summary">
        <DiagnosticValue label="SKU" value={data.sku} />
        <DiagnosticValue label="В группе" value={summary.total ?? data.matched} />
        <DiagnosticValue label="Скрыто supplier-only" value={data.hiddenSupplierOnlyMatches} />
      </div>
      <div className="diagnostics-summary diagnostics-summary-wide">
        <DiagnosticValue label="Маркетплейсы" value={marketplaces} />
        <DiagnosticValue label="Готово" value={`${summary.ready ?? 0}/${summary.total ?? products.length}`} tone={Number(summary.ready || 0) > 0 ? "success" : "warn"} />
        <DiagnosticValue label="Архив" value={summary.archived ?? 0} tone={Number(summary.archived || 0) > 0 ? "danger" : "success"} />
        <DiagnosticValue label="Нет поставщика" value={summary.noSupplier ?? 0} tone={Number(summary.noSupplier || 0) > 0 ? "warn" : "success"} />
        <DiagnosticValue label="Нет остатка" value={summary.noStock ?? 0} tone={Number(summary.noStock || 0) > 0 ? "warn" : "success"} />
        <DiagnosticValue label="API ошибки" value={summary.apiError ?? 0} tone={Number(summary.apiError || 0) > 0 ? "danger" : "success"} />
      </div>
      {warnings.length > 0 && <div className="warning-strip">{warnings.map(String).join(" · ")}</div>}
      {products.map((item) => {
        const automation = asRecord(item.automation);
        const saleState = asRecord(item.saleState);
        const saleCode = String(item.saleStateCode || saleState.code || "");
        const itemLinks = Array.isArray(item.links) ? item.links.map(asRecord) : [];
        return (
          <div className="diagnostic-card" key={String(item.id)}>
            <div className="diagnostic-card-head">
              <strong>{String(item.offerId || item.id || "товар")}</strong>
              <span>{String(item.marketplace || "marketplace")} · {String(item.status || "status")}</span>
            </div>
            <div className={`diagnostic-state ${saleTone(saleCode)}`}>
              <strong>{String(item.saleStateLabel || saleState.label || "Статус неизвестен")}</strong>
              <span>{String(item.saleReason || saleState.reason || "нет причины")}</span>
            </div>
            <div className="diagnostics-summary">
              <DiagnosticValue label="Архив" value={item.archived} tone={item.archived ? "danger" : "success"} />
              <DiagnosticValue label="Привязки" value={item.hasLinks} tone={item.hasLinks ? "success" : "warn"} />
              <DiagnosticValue label="Готов" value={item.ready} tone={item.ready ? "success" : "warn"} />
              <DiagnosticValue label="Остаток" value={item.targetStock} />
              <DiagnosticValue label="Цена" value={money(item.targetPrice || item.currentPrice)} />
            </div>
            <div className="diagnostic-lines">
              <span><b>Поставщик:</b> {supplierText(item.selectedSupplier)}</span>
              <div className="diagnostic-pm-links">
                <b>PriceMaster:</b>
                {itemLinks.length ? itemLinks.map((link, index) => (
                  <span className="diagnostic-pm-chip" key={`${link.id || link.article}-${index}`}>
                    {String(link.article || link.supplierArticle || "без артикула")} · {String(link.supplierName || "поставщик не указан")} · row {String(link.sourceRowId || link.rowId || link.id || "-")} · partner {String(link.partnerId || "-")}
                  </span>
                )) : <span className="diagnostic-pm-chip muted">нет привязки</span>}
              </div>
              <span><b>Последний остаток:</b> {commandText(item.lastStockSend)}</span>
              <span><b>Последний архив/разархив:</b> {commandText(item.lastArchiveSend)}</span>
              <span><b>Yandex цена:</b> {commandText(item.lastYandexPriceSend, "нет отправки цены")}</span>
              <span><b>Ozon цена:</b> {commandText(item.lastOzonPriceSend, "нет отправки цены")}</span>
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

function MarketplaceRows({ products }: { products: Product[] }) {
  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>Marketplace</span>
          <h3>Строки карточки</h3>
        </div>
        <span className="section-count">{products.length}</span>
      </div>
      <div className="marketplace-rows">
        {products.map((product) => {
          const status = statusLabel(product);
          const stock = Number(product.targetStock || product.stock || 0);
          const changed = Number(product.newPrice || product.targetPrice || 0) > 0 && Number(product.currentPrice || 0) !== Number(product.newPrice || product.targetPrice || 0);
          return (
            <div className="marketplace-row" key={product.id}>
              <div>
                <strong>{marketplaceLabel(product.marketplace)}</strong>
                <span>{product.offerId || product.sku || product.id}</span>
              </div>
              <span className={`pill ${status.tone}`}>{status.icon}{status.label}</span>
              <div>
                <small>Цена</small>
                <strong>{money(product.newPrice || product.targetPrice || product.currentPrice)}</strong>
              </div>
              <div>
                <small>Остаток</small>
                <strong>{stock || "-"}</strong>
              </div>
              <div>
                <small>Привязки</small>
                <strong>{(product.links || []).length}</strong>
              </div>
              <div className="marketplace-flags">
                {product.archived && <span>Архив</span>}
                {changed && <span>Цена ждет</span>}
                {!product.selectedSupplier && (product.links || []).length > 0 && <span>Поставщик не выбран</span>}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function CopyActions({ product }: { product: Product }) {
  const [copied, setCopied] = useState("");
  const copyValue = async (label: string, value: unknown) => {
    const ok = await copyPlainText(value);
    if (!ok) return;
    setCopied(label);
    window.setTimeout(() => setCopied(""), 1400);
  };
  return (
    <div className="copy-actions" aria-label="quick copy">
      <button className="copy-action" type="button" onClick={() => copyValue("name", product.name || "")} title="Скопировать название">
        <Copy size={15} /> {copied === "name" ? "Скопировано" : "Название"}
      </button>
      <button className="copy-action" type="button" onClick={() => copyValue("article", product.offerId || product.sku || "")} title="Скопировать артикул">
        <Copy size={15} /> {copied === "article" ? "Скопировано" : "Артикул"}
      </button>
    </div>
  );
}

function GroupActions({ products, selectedGroup, onDone }: { products: Product[]; selectedGroup: string; onDone: () => void }) {
  const queryClient = useQueryClient();
  const [mergeQuery, setMergeQuery] = useState("");
  const optimisticLocks = products.map((item) => ({ id: item.id, expectedUpdatedAt: item.updatedAt || "" }));
  const productIds = products.map((item) => item.id).filter(Boolean);
  const primaryOffer = products.find((item) => item.offerId)?.offerId || products[0]?.id || "";

  const groupMutation = useMutation({
    mutationFn: async ({ productIds: ids, groupId }: { productIds: string[]; groupId: string }) => fetchJson(
      "/api/warehouse/products/group",
      MutationProductResponseSchema,
      patchBody({ productIds: ids, groupId, optimisticLocks }),
    ),
    onSuccess: (payload) => {
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onDone();
    },
  });

  const ungroupMutation = useMutation({
    mutationFn: async () => fetchJson(
      "/api/warehouse/products/ungroup",
      MutationProductResponseSchema,
      patchBody({ productIds, optimisticLocks }),
    ),
    onSuccess: (payload) => {
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onDone();
    },
  });

  const mergeMutation = useMutation({
    mutationFn: async () => {
      const q = mergeQuery.trim();
      if (!q) throw new Error("Введите SKU или артикул для объединения.");
      const result = await fetchJson(
        `/api/warehouse/products/page?q=${encodeURIComponent(q)}&pageSize=20&page=1&grouped=false`,
        WarehousePageSchema,
      );
      const ids = Array.from(new Set([...productIds, ...(result.items || []).map((item) => item.id).filter(Boolean)]));
      if (ids.length < 2) throw new Error("Не нашел вторую карточку для объединения.");
      const groupId = selectedGroup.startsWith("manual:") ? selectedGroup.slice(7) : `manual-${String(primaryOffer || Date.now()).trim().toLowerCase()}`;
      return groupMutation.mutateAsync({ productIds: ids, groupId });
    },
    onSuccess: () => setMergeQuery(""),
  });

  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>Группа</span>
          <h3>Объединение Ozon / Yandex</h3>
        </div>
      </div>
      <div className="group-tools">
        <button
          className="secondary-action"
          type="button"
          disabled={products.length < 2 || groupMutation.isPending}
          onClick={() => groupMutation.mutate({ productIds, groupId: selectedGroup.startsWith("manual:") ? selectedGroup.slice(7) : `manual-${String(primaryOffer || Date.now()).trim().toLowerCase()}` })}
        >
          <Link2 size={16} /> Закрепить группу
        </button>
        <button
          className="secondary-action"
          type="button"
          disabled={!products.length || ungroupMutation.isPending}
          onClick={() => ungroupMutation.mutate()}
        >
          <X size={16} /> Разъединить
        </button>
      </div>
      <div className="merge-form">
        <input value={mergeQuery} onChange={(event) => setMergeQuery(event.target.value)} placeholder="SKU/offerId другой карточки" />
        <button className="primary-action" type="button" disabled={mergeMutation.isPending} onClick={() => mergeMutation.mutate()}>
          {mergeMutation.isPending ? <Loader2 className="spin" size={16} /> : <Link2 size={16} />} Объединить
        </button>
      </div>
      {(groupMutation.error || ungroupMutation.error || mergeMutation.error) && (
        <div className="inline-error">{errorMessage(groupMutation.error || ungroupMutation.error || mergeMutation.error)}</div>
      )}
    </section>
  );
}

function QuickActions({ primary, products, onDone }: { primary: Product; products: Product[]; onDone: () => void }) {
  const start = useMutation({
    mutationFn: (type: string) => fetchJson("/api/operations", OperationCreateSchema, mutationBody({
      type,
      payload: {
        productIds: products.map((item) => item.id).filter(Boolean),
        offerIds: Array.from(new Set(products.map((item) => item.offerId).filter(Boolean))),
        limit: Math.max(1, products.length || 1),
      },
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
  const primary = products.length ? preferredGroupPrimary(products) : undefined;
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
          <CopyActions product={primary} />
        </div>
        <button className="mobile-close" type="button" onClick={onClose}><X size={18} /></button>
      </div>
      <div className="stats-grid">
        <Stat label="Текущая цена" value={money(primary.currentPrice)} />
        <Stat label="Новая цена" value={money(primary.newPrice || primary.targetPrice)} />
        <Stat label="Остаток" value={primary.targetStock || primary.stock || "-"} />
        <Stat label="Привязки" value={products.reduce((sum, item) => sum + (item.links || []).length, 0)} />
      </div>
      <MarketplaceRows products={products} />
      <GroupActions products={products} selectedGroup={selectedGroup} onDone={refreshDetail} />
      <LinksPanel products={products} onSaved={refreshDetail} />
      <QuickActions primary={primary} products={products} onDone={refreshDetail} />
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

export function WarehousePage() {
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
  const groups = useMemo(() => groupProductsForList(rows), [rows]);
  const selectedRowsOnPage = useMemo(() => groups.find((group) => group.groupKey === selectedGroup)?.products || [], [groups, selectedGroup]);

  useEffect(() => {
    if (!selectedGroup || pageQuery.isLoading) return;
    if (!groups.some((group) => group.groupKey === selectedGroup)) setSelectedGroup("");
  }, [pageQuery.isLoading, groups, selectedGroup]);

  const detailQuery = useQuery({
    queryKey: ["warehouse", "group-detail", selectedGroup],
    queryFn: () => fetchJson(`/api/warehouse/products/group-detail?group=${encodeURIComponent(selectedGroup)}&marketplace=${encodeURIComponent(filters.marketplace)}&state=${encodeURIComponent(filters.state)}`, GroupDetailSchema),
    enabled: Boolean(selectedGroup),
  });
  const detailProducts = detailQuery.data?.products?.length ? detailQuery.data.products : selectedRowsOnPage;
  const virtualizer = useVirtualizer({
    count: groups.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 174,
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
          Только автопрайс
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
                const group = groups[virtualRow.index];
                return (
                  <div key={group.groupKey} style={{ position: "absolute", top: 0, left: 0, width: "100%", transform: `translateY(${virtualRow.start}px)` }}>
                    <ProductGroupRow group={group} selected={group.groupKey === selectedGroup} onSelect={() => setSelectedGroup(group.groupKey)} />
                  </div>
                );
              })}
            </div>
            {pageQuery.isLoading && <div className="list-loading"><Loader2 className="spin" /> Загружаю каталог...</div>}
            {!pageQuery.isLoading && !groups.length && <div className="list-loading">Ничего не найдено.</div>}
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
