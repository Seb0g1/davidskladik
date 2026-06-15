import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Bot, Check, Copy, ImagePlus, Link2, Loader2, PackageCheck, RefreshCw, Save, Search, Sparkles, Trash2, X } from "lucide-react";
import { fetchJson, mutationBody, patchBody } from "../api";
import { AiAssistantResponseSchema, AiImageJobResponseSchema, BrandIndexStatusSchema, DiagnosticsSchema, Filters, GroupDetailSchema, isProductGroupPageItem, isProductPageItem, MutationProductResponseSchema, OperationCreateSchema, PriceMasterSearchRow, PriceMasterSearchSchema, Product, ProductGroupPageItem, ProductLink, ProductRepairSchema, WarehouseBrandsSchema, WarehousePageSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { BrandPicker } from "../components/BrandPicker";
import { SelectField } from "../components/SelectField";
import { CatalogSkeleton } from "../components/Skeleton";
import { Stat } from "../components/Stat";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { asRecord, compactDate, copyableLatinProductName, copyPlainText, errorMessage, money, type MutationPayload, numberValue, refreshWarehouseAfterMutation, updateCachedProducts, useDebounced } from "../lib/common";
import { ProductGroup, firstImage, groupMarketplaceLabels, groupPrice, groupProductsForList, groupStatusLabel, marketplaceLabel, marketplaceRowLabel, preferredGroupPrimary, statusLabel, uniqueLinks } from "../lib/warehouse";
import { demoWarehouseProducts, isDemoWarehouseProduct } from "../demoWarehouse";

const pageSize = 40;
const mobileListMedia = "(max-width: 640px)";
const studioPhotoPresets = [
  { id: "white-packshot", label: "White", prompt: "Clean white marketplace packshot, full perfume bottle visible from cap to base, centered, upright, not cropped, 10-15% margin on every side, remove any box or packaging." },
  { id: "premium-shadow", label: "Shadow", prompt: "Premium studio shadow, subtle reflection, one centered perfume bottle only, full bottle inside frame, no crop, no box, no packaging, no props." },
  { id: "lifestyle", label: "Lifestyle", prompt: "Minimal light studio scene, one centered bottle only, full bottle visible, no hands, no faces, no box or packaging, no typography." },
  { id: "bottle-only", label: "Bottle", prompt: "Bottle-only marketplace composition, clean studio layout, entire bottle fits inside square image, no packaging, no extra text outside original label." },
  { id: "close-up", label: "Full bottle", prompt: "Clean hero image with crisp highlights, but keep the full bottle visible from cap to base, centered, no crop, no text overlays." },
];

type LinkDraft = {
  article: string;
  supplierName: string;
  keyword: string;
  priceCurrency: string;
  partnerId?: string;
  sourceRowId?: string;
  exactName?: string;
  matchType?: string;
  price?: number | null;
  available?: boolean;
  updatedAt?: string | null;
};

type LinkRow = ProductLink & {
  productId?: string;
  productOfferId?: string;
  productMarketplace?: string;
  productTarget?: string;
  productSelectedSupplier?: unknown;
  productOfferIds?: string[];
  productMarketplaces?: string[];
  productTargets?: string[];
  productSelectedSuppliers?: unknown[];
  groupCount?: number;
};

function readFilters(): Filters {
  const params = new URLSearchParams(window.location.search);
  return {
    q: params.get("q") || "",
    marketplace: params.get("marketplace") || "all",
    linked: params.get("linked") || "all",
    state: params.get("state") || "all",
    brand: params.get("brand") || "",
    sort: params.get("sort") || "",
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
  if (filters.sort) params.set("sort", filters.sort);
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
    grouped: "true",
  });
  if (filters.brand) params.set("brand", filters.brand);
  if (filters.sort) params.set("sort", filters.sort);
  return `/api/warehouse/products/page?${params}`;
}

function isDiagnosticWarehouseProduct(product?: Product): boolean {
  const id = String(product?.id || "").toLowerCase();
  const offerId = String(product?.offerId || product?.sku || "").toLowerCase();
  return id.startsWith("diag-") || offerId === "stale-stock-1";
}

function groupHasRealProduct(group: ProductGroup): boolean {
  return group.products.some((product) => !isDiagnosticWarehouseProduct(product) && !isDemoWarehouseProduct(product));
}

function demoProductMatchesFilters(product: Product, filters: Filters): boolean {
  const q = filters.q.trim().toLowerCase();
  if (q) {
    const haystack = [
      product.name,
      product.brand,
      product.offerId,
      product.sku,
      product.barcode,
      product.links?.map((link) => [link.article, link.supplierName, link.exactName].join(" ")).join(" "),
    ].filter(Boolean).join(" ").toLowerCase();
    if (!haystack.includes(q)) return false;
  }
  if (filters.marketplace !== "all" && String(product.marketplace || "").toLowerCase() !== filters.marketplace) return false;
  if (filters.brand && !String(product.brand || "").toLowerCase().includes(filters.brand.trim().toLowerCase())) return false;
  if (filters.autoOnly && !product.autoPriceEnabled) return false;
  if (filters.state === "archived" && !product.archived) return false;
  if (filters.state === "inactive" && String(product.marketplaceState?.code || product.status || "").toLowerCase() !== "inactive") return false;
  if (filters.state === "out_of_stock" && Number(product.targetStock ?? product.stock ?? 0) > 0) return false;
  if (filters.linked === "linked" && !(product.links || []).length) return false;
  if (filters.linked === "unlinked" && (product.links || []).length) return false;
  if (filters.linked === "changed") {
    const changed = Number(product.newPrice || product.targetPrice || 0) > 0
      && Number(product.currentPrice || 0) !== Number(product.newPrice || product.targetPrice || 0);
    if (!changed) return false;
  }
  if (filters.linked === "linked_archived" && !((product.links || []).length && product.archived)) return false;
  return true;
}

function demoGroupsForFilters(filters: Filters): ProductGroup[] {
  return groupProductsForList(demoWarehouseProducts.filter((product) => demoProductMatchesFilters(product, filters)));
}

function buildDemoBreakdown(products: Product[]): MarketplaceBreakdownRow[] {
  return products.map((product) => {
    const state = asRecord(product.marketplaceState);
    const changed = Number(product.newPrice || product.targetPrice || 0) > 0
      && Number(product.currentPrice || 0) !== Number(product.newPrice || product.targetPrice || 0);
    return {
      productId: product.id,
      offerId: product.offerId,
      marketplace: product.marketplace,
      targetName: String(product.targetName || marketplaceLabel(product.marketplace)),
      statusCode: String(state.code || product.status || "active"),
      statusLabel: String(state.label || "Активен"),
      archived: Boolean(product.archived),
      changed,
      ready: Boolean((product.links || []).length && product.selectedSupplier && Number(product.targetStock || product.stock || 0) > 0),
      currentPrice: Number(product.currentPrice || 0),
      targetPrice: Number(product.newPrice || product.targetPrice || 0),
      targetStock: Number(product.targetStock || product.stock || 0),
      stockOnlyFallback: Boolean(product.stockOnlyFallbackActive),
      lastPriceSendAt: String((product as Record<string, unknown>).lastPriceSendAt || ""),
      ozonMinPrice: String(product.marketplace || "").toLowerCase() === "ozon" ? Math.max(0, Number(product.targetPrice || 0) - 450) : undefined,
    };
  });
}

function mapServerPageGroup(item: ProductGroupPageItem): ProductGroup | null {
  if (!item?.groupKey || !Array.isArray(item.products) || !item.products.length) return null;
  const products = item.products;
  return {
    groupKey: item.groupKey,
    primary: item.primary || preferredGroupPrimary(products),
    products,
    links: item.links?.length ? item.links : uniqueLinks(products),
    marketplaces: item.marketplaces?.length ? item.marketplaces : groupMarketplaceLabels(products),
    statusSummary: item.statusSummary || {
      total: products.length,
      linked: 0,
      archived: 0,
      ready: 0,
      changed: 0,
      withoutSupplier: 0,
    },
  };
}

function productSupplierName(product: Product, group: ProductGroup): string {
  const selected = asRecord(product.selectedSupplier);
  const selectedName = String(selected.name || selected.supplierName || selected.partnerName || "").trim();
  if (selectedName) return selectedName;
  const productSupplier = product.suppliers?.find((supplier) => supplier.name)?.name;
  if (productSupplier) return productSupplier;
  const linkSupplier = group.links.find((link) => link.supplierName)?.supplierName;
  return linkSupplier || "—";
}

function ProductGroupRow({ group, selected, onSelect }: { group: ProductGroup; selected: boolean; onSelect: () => void }) {
  const primary = group.primary;
  const status = groupStatusLabel(group);
  const image = firstImage(primary);
  const offer = primary.offerId || primary.sku || primary.id;
  const stock = primary.targetStock ?? primary.stock;
  const supplier = productSupplierName(primary, group);
  return (
    <button className={`product-row group-row ${selected ? "is-selected" : ""}`} type="button" onClick={onSelect}>
      <span className="row-check" aria-hidden="true" />
      <div className="product-cell">
        <div className="product-thumb">
          {image ? <img src={image} alt="" loading="lazy" /> : <PackageCheck size={20} />}
        </div>
        <div className="product-main">
          <div className="product-title-line">
            <strong>{primary.name || "Без названия"}</strong>
          </div>
          <div className="product-meta">
            <span>{primary.brand || "без бренда"}</span>
            <span>{group.products.length} стр.</span>
            <span>{group.links.length} прив.</span>
          </div>
        </div>
      </div>
      <span className="warehouse-sku">{offer}</span>
      <div className="warehouse-marketplaces" aria-label="marketplaces">
        {group.marketplaces.map((marketplace) => <span className="market-badge" key={marketplace}>{marketplace}</span>)}
      </div>
      <span className={`warehouse-stock ${Number(stock || 0) <= 0 ? "is-empty" : ""}`}>{stock ?? "—"}{stock === undefined || stock === null ? "" : " шт"}</span>
      <span className="warehouse-supplier">{supplier}</span>
      <span className="warehouse-price">{money(groupPrice(group))}</span>
      <span className={`warehouse-status pill ${status.tone}`}>{status.icon}{status.label}</span>
    </button>
  );
}

function draftFromSearchRow(row: PriceMasterSearchRow): LinkDraft {
  return {
    article: row.article,
    supplierName: row.supplierName,
    keyword: "",
    priceCurrency: row.priceCurrency || row.currency || "USD",
    partnerId: row.partnerId || "",
    sourceRowId: row.rowId || row.id,
    exactName: row.name || row.keyword,
    matchType: "selected_row",
    price: row.price,
    available: row.available,
    updatedAt: row.updatedAt,
  };
}

function draftFromFailedCandidate(candidate: unknown): LinkDraft {
  const row = asRecord(candidate);
  return draftFromSearchRow({
    id: String(row.rowId || row.id || ""),
    rowId: String(row.rowId || row.id || ""),
    article: String(row.article || ""),
    supplierName: String(row.supplierName || row.partnerName || ""),
    partnerId: String(row.partnerId || ""),
    keyword: "",
    name: String(row.name || ""),
    price: Number(row.price || 0) || null,
    currency: String(row.priceCurrency || row.currency || "USD"),
    priceCurrency: String(row.priceCurrency || row.currency || "USD"),
    available: row.available === undefined ? true : Boolean(row.available),
    updatedAt: row.updatedAt ? String(row.updatedAt) : null,
  } as PriceMasterSearchRow);
}

function emptyLinkDraft(currency = "USD"): LinkDraft {
  return { article: "", supplierName: "", keyword: "", priceCurrency: currency };
}

function cleanLinkPart(value: unknown): string {
  return String(value || "").trim();
}

function parseNoArticleRowId(value: unknown): string {
  const text = cleanLinkPart(value);
  const prefix = "__no_article__:";
  return text.toLowerCase().startsWith(prefix) ? cleanLinkPart(text.slice(prefix.length)) : "";
}

function linkPrimarySignature(link: Partial<ProductLink | LinkDraft>): string {
  const raw = link as Record<string, unknown>;
  let article = cleanLinkPart(raw.article || raw.supplierArticle);
  let sourceRowId = cleanLinkPart(raw.sourceRowId || raw.rowId);
  const exactName = cleanLinkPart(raw.exactName || raw.name);
  const noArticleRowId = parseNoArticleRowId(article);
  let matchType = cleanLinkPart(link.matchType || "").toLowerCase();
  if (noArticleRowId) {
    article = "";
    sourceRowId = sourceRowId || noArticleRowId;
    matchType = "selected_row";
  }
  if (!["article", "selected_row", "exact_name"].includes(matchType)) matchType = "article";
  if (article && !(matchType === "selected_row" && sourceRowId)) matchType = "article";
  const primary = matchType === "selected_row" && sourceRowId
    ? `row:${sourceRowId}`
    : article
    ? `article:${article.toLowerCase()}`
    : (sourceRowId ? `row:${sourceRowId}` : `name:${exactName.toLowerCase()}`);
  const currency = cleanLinkPart(link.priceCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
  const supplierKeys = [
    cleanLinkPart(raw.supplierName).toLowerCase(),
    cleanLinkPart(raw.partnerId) ? `partner:${cleanLinkPart(raw.partnerId)}` : "",
  ].filter(Boolean).sort();
  const supplierTarget = supplierKeys.length ? supplierKeys.join("&") : "manual";
  const keyword = cleanLinkPart(raw.keyword).toLowerCase();
  return [matchType, primary, supplierTarget, keyword, currency].join("|");
}

function productLinksSignature(product: Product): string {
  return Array.from(new Set((product.links || []).map(linkPrimarySignature).filter(Boolean))).sort().join("||");
}

function isStockOnlyLink(link: Partial<ProductLink>): boolean {
  const raw = link as Record<string, unknown>;
  return raw.stockOnly === true || raw.priceEligible === false || String(raw.pricingMode || "").toLowerCase() === "stock_only";
}

function stockOnlyManualPricesFromProducts(products: Product[]) {
  const first = products.map((item) => asRecord(item.stockOnlyManualPrices)).find((value) => Object.keys(value).length) || {};
  return {
    default: Number(first.default || 0) || "",
    ozon: Number(first.ozon || 0) || "",
    yandex: Number(first.yandex || 0) || "",
  };
}

function linkDeleteRef(link: ProductLink & { productId?: string }, product?: Product) {
  const raw = link as Record<string, unknown>;
  return {
    productId: link.productId || product?.id || "",
    linkId: link.id || "",
    linkTargetKey: linkPrimarySignature(link),
    article: link.article || link.supplierArticle || "",
    supplierArticle: link.supplierArticle || link.article || "",
    supplierName: link.supplierName || "",
    partnerId: link.partnerId || "",
    rowId: raw.rowId || "",
    sourceRowId: raw.sourceRowId || raw.rowId || "",
    exactName: raw.exactName || raw.name || "",
    matchType: link.matchType || "",
    keyword: link.keyword || "",
    priceCurrency: link.priceCurrency || "USD",
    expectedUpdatedAt: product?.updatedAt || "",
    expectedLinksSignature: product ? productLinksSignature(product) : "",
  };
}

function linkSelectionKey(link: ProductLink & { productId?: string }): string {
  return `${link.productId || ""}:${link.id || linkPrimarySignature(link)}`;
}

function draftTitle(draft: LinkDraft): string {
  return draft.article || "без артикула";
}

function draftSubtitle(draft: LinkDraft): string {
  return [
    draft.exactName || draft.keyword || "",
    draft.supplierName || "",
  ].filter(Boolean).join(" · ") || "ручной fallback";
}

function draftMeta(draft: LinkDraft): string {
  return [
    draft.sourceRowId ? `row ${draft.sourceRowId}` : "",
    draft.partnerId ? `partner ${draft.partnerId}` : "",
    draft.price ? money(draft.price) : "",
    draft.priceCurrency || "USD",
    draft.available === undefined ? "" : (draft.available ? "в наличии" : "нет наличия"),
    compactDate(draft.updatedAt || ""),
  ].filter(Boolean).join(" · ");
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

function LinksPanel({ products, onSaved, readOnly = false }: { products: Product[]; onSaved: () => void; readOnly?: boolean }) {
  const queryClient = useQueryClient();
  const productIds = products.map((item) => item.id).filter(Boolean);
  const uniqueGroupLinks = useMemo(() => uniqueLinks(products), [products]);
  const draftScopeKey = productIds.slice().sort().join("|");
  const optimisticLocks = products.map((item) => ({
    id: item.id,
    expectedUpdatedAt: item.updatedAt || "",
    expectedLinksSignature: productLinksSignature(item),
  }));
  const links = products.flatMap((item) => (item.links || []).map((link) => ({
    ...link,
    productId: item.id,
    productOfferId: item.offerId,
    productMarketplace: marketplaceLabel(item.marketplace),
    productTarget: item.target || "",
    productSelectedSupplier: item.selectedSupplier,
  })));
  const groupLinkRows = useMemo<LinkRow[]>(() => {
    const byKey = new Map<string, LinkRow>();
    for (const link of links as LinkRow[]) {
      const key = linkPrimarySignature(link);
      const existing = byKey.get(key);
      if (!existing) {
        byKey.set(key, {
          ...link,
          productOfferIds: link.productOfferId ? [link.productOfferId] : [],
          productMarketplaces: link.productMarketplace ? [link.productMarketplace] : [],
          productTargets: link.productTarget ? [link.productTarget] : [],
          productSelectedSuppliers: link.productSelectedSupplier ? [link.productSelectedSupplier] : [],
          groupCount: 1,
        });
        continue;
      }
      existing.groupCount = Number(existing.groupCount || 0) + 1;
      if (link.productOfferId && !existing.productOfferIds?.includes(link.productOfferId)) existing.productOfferIds = [...(existing.productOfferIds || []), link.productOfferId];
      if (link.productMarketplace && !existing.productMarketplaces?.includes(link.productMarketplace)) existing.productMarketplaces = [...(existing.productMarketplaces || []), link.productMarketplace];
      if (link.productTarget && !existing.productTargets?.includes(link.productTarget)) existing.productTargets = [...(existing.productTargets || []), link.productTarget];
      if (link.productSelectedSupplier) existing.productSelectedSuppliers = [...(existing.productSelectedSuppliers || []), link.productSelectedSupplier];
    }
    return Array.from(byKey.values());
  }, [links]);
  const selectedSupplierCount = products.filter((item) => item.selectedSupplier).length;
  const [drafts, setDrafts] = useState<LinkDraft[]>([]);
  const [draft, setDraft] = useState<LinkDraft>(() => emptyLinkDraft());
  const [search, setSearch] = useState("");
  const [linkFilter, setLinkFilter] = useState("");
  const [linkKind, setLinkKind] = useState<"all" | "normal" | "stock_only">("all");
  const [manualPrices, setManualPrices] = useState<Record<string, string | number>>(() => stockOnlyManualPricesFromProducts(products));
  const [selectedLinkIds, setSelectedLinkIds] = useState<string[]>([]);
  const debouncedSearch = useDebounced(search, 250);
  const draftIsFilled = Boolean(draft.article.trim() || draft.keyword.trim());
  const pendingDrafts = draftIsFilled ? [...drafts, draft] : drafts;
  const draftKeys = useMemo(() => new Set(drafts.map(linkPrimarySignature)), [drafts]);
  const savedSupplierList = useMemo(() => Array.from(new Set(groupLinkRows.map((link) => link.supplierName).filter(Boolean))).sort(), [groupLinkRows]);
  const filteredLinks = useMemo(() => {
    const q = linkFilter.trim().toLowerCase();
    return groupLinkRows.filter((link) => {
      if (linkKind === "stock_only" && !isStockOnlyLink(link)) return false;
      if (linkKind === "normal" && isStockOnlyLink(link)) return false;
      if (!q) return true;
      return [
        link.article,
        link.supplierArticle,
        link.supplierName,
        link.partnerId,
        link.exactName,
        link.keyword,
        link.sourceRowId,
        link.productOfferId,
        ...(link.productOfferIds || []),
        ...(link.productMarketplaces || []),
      ].filter(Boolean).join(" ").toLowerCase().includes(q);
    });
  }, [groupLinkRows, linkFilter, linkKind]);
  const groupLinkSignatures = useMemo(() => products.map(productLinksSignature), [products]);
  const groupLinkCounts = useMemo(() => products.map((item) => (item.links || []).length), [products]);
  const groupLinksSynced = products.length <= 1
    || (new Set(groupLinkSignatures).size <= 1 && new Set(groupLinkCounts).size <= 1);
  const refreshAfterMutation = (payload?: unknown) => {
    refreshWarehouseAfterMutation(queryClient, payload as MutationPayload | null | undefined);
    void queryClient.invalidateQueries({ queryKey: ["warehouse", "diagnostics"] });
    onSaved();
  };

  const searchQuery = useQuery({
    queryKey: ["pricemaster", "search", debouncedSearch],
    queryFn: () => fetchJson(
      `/api/pricemaster/search?q=${encodeURIComponent(debouncedSearch)}&limit=100`,
      PriceMasterSearchSchema,
    ),
    enabled: !readOnly && debouncedSearch.trim().length >= 2,
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
      setDraft(emptyLinkDraft(draft.priceCurrency));
      refreshAfterMutation(payload);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (link: ProductLink & { productId?: string }) => {
      const product = products.find((item) => item.id === link.productId) || products[0];
      return fetchJson("/api/warehouse/products/links/delete", MutationProductResponseSchema, mutationBody({ refs: [linkDeleteRef(link, product)] }));
    },
    onSuccess: (payload) => {
      refreshAfterMutation(payload);
    },
  });

  const bulkDeleteMutation = useMutation({
    mutationFn: async () => {
      const refs = groupLinkRows
        .filter((link) => selectedLinkIds.includes(linkSelectionKey(link)))
        .map((link) => linkDeleteRef(link, products.find((item) => item.id === link.productId) || products[0]));
      return fetchJson("/api/warehouse/products/links/delete", MutationProductResponseSchema, mutationBody({ refs }));
    },
    onSuccess: (payload) => {
      setSelectedLinkIds([]);
      refreshAfterMutation(payload);
    },
  });

  const syncMutation = useMutation({
    mutationFn: async () => fetchJson("/api/warehouse/products/links/sync-group", MutationProductResponseSchema, mutationBody({
      productIds,
      optimisticLocks,
    })),
    onSuccess: (payload) => {
      setSelectedLinkIds([]);
      refreshAfterMutation(payload);
    },
  });

  const manualPricesMutation = useMutation({
    mutationFn: async () => fetchJson("/api/warehouse/products/group", MutationProductResponseSchema, mutationBody({
      productIds,
      stockOnlyManualPrices: {
        default: numberValue(manualPrices.default, 0) || null,
        ozon: numberValue(manualPrices.ozon, 0) || null,
        yandex: numberValue(manualPrices.yandex, 0) || null,
      },
    })),
    onSuccess: (payload) => refreshAfterMutation(payload),
  });

  useEffect(() => {
    setDrafts([]);
    setDraft(emptyLinkDraft());
    setSearch("");
    setLinkFilter("");
    setLinkKind("all");
    setManualPrices(stockOnlyManualPricesFromProducts(products));
    setSelectedLinkIds([]);
    saveMutation.reset();
    deleteMutation.reset();
    bulkDeleteMutation.reset();
    syncMutation.reset();
    manualPricesMutation.reset();
  }, [draftScopeKey]);

  const addDraft = (nextDraft: LinkDraft) => {
    if (!nextDraft.article.trim() && !nextDraft.sourceRowId && !nextDraft.exactName && !nextDraft.keyword.trim()) return;
    const key = linkPrimarySignature(nextDraft);
    setDrafts((items) => items.some((item) => linkPrimarySignature(item) === key) ? items : [...items, nextDraft]);
    setDraft(emptyLinkDraft(nextDraft.priceCurrency));
  };

  const addAllSearchRows = () => {
    const rows = searchQuery.data?.rows || [];
    if (!rows.length) return;
    setDrafts((items) => {
      const byKey = new Map(items.map((item) => [linkPrimarySignature(item), item]));
      for (const row of rows) {
        const nextDraft = draftFromSearchRow(row);
        byKey.set(linkPrimarySignature(nextDraft), nextDraft);
      }
      return Array.from(byKey.values());
    });
  };

  const saveErrorDetail = saveMutation.error && typeof (saveMutation.error as { detail?: unknown }).detail === "object"
    ? (saveMutation.error as { detail?: { failedLinks?: unknown[] } }).detail
    : null;
  const failedLinks = Array.isArray(saveErrorDetail?.failedLinks) ? saveErrorDetail.failedLinks : [];

  return (
    <section className="detail-section pm-section">
      <div className="section-title">
        <div>
          <span>PriceMaster</span>
          <p className="section-note">Общие привязки для всей группы: Ozon и Yandex получают один набор PriceMaster, но цена считается отдельно по коэффициенту маркетплейса.</p>
          <h3>Точная связь с PriceMaster</h3>
        </div>
        <span className="section-count">{uniqueGroupLinks.length}</span>
      </div>

      {readOnly ? (
        <div className="info-strip compact demo-readonly-strip">
          Демо-режим: привязки, поставщики и цены показаны как пример. Сохранение и удаление здесь отключены.
        </div>
      ) : null}

      <div className="pm-link-summary">
        <div>
          <strong>{uniqueGroupLinks.length}</strong>
          <span>общих PM-привязок</span>
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

      <div className={groupLinksSynced ? "success-strip compact" : "warning-strip compact"}>
        <span>{groupLinksSynced ? "Привязки группы синхронизированы" : "Есть расхождение Ozon/Yandex по PriceMaster-привязкам"}</span>
        {!groupLinksSynced ? (
          <button className="secondary-action" type="button" onClick={() => syncMutation.mutate()} disabled={readOnly || syncMutation.isPending}>
            {syncMutation.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Синхронизировать группу
          </button>
        ) : null}
      </div>

      <div className="stock-only-price-box">
        <div>
          <strong>Ручная цена складского fallback</strong>
          <span>Если обычных поставщиков нет, товар можно оставить в продаже по этой цене. Цена складского PriceMaster-поставщика не используется.</span>
        </div>
        <div className="stock-only-price-grid">
          <label>База группы<input type="number" min="0" value={String(manualPrices.default ?? "")} onChange={(event) => setManualPrices((current) => ({ ...current, default: event.target.value }))} disabled={readOnly} /></label>
          <label>Ozon<input type="number" min="0" value={String(manualPrices.ozon ?? "")} onChange={(event) => setManualPrices((current) => ({ ...current, ozon: event.target.value }))} disabled={readOnly} /></label>
          <label>Yandex<input type="number" min="0" value={String(manualPrices.yandex ?? "")} onChange={(event) => setManualPrices((current) => ({ ...current, yandex: event.target.value }))} disabled={readOnly} /></label>
          <button className="secondary-action" type="button" onClick={() => manualPricesMutation.mutate()} disabled={readOnly || manualPricesMutation.isPending}>
            {manualPricesMutation.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить цену
          </button>
        </div>
        {manualPricesMutation.error && <div className="inline-error">{errorMessage(manualPricesMutation.error)}</div>}
      </div>

      <div className="pm-link-toolbar">
          <input value={linkFilter} onChange={(event) => setLinkFilter(event.target.value)} placeholder="Фильтр сохраненных поставщиков: поставщик, артикул или название" />
          <SelectField
            ariaLabel="Тип связи"
            value={linkKind}
            onChange={(next) => setLinkKind(next as "all" | "normal" | "stock_only")}
            options={[
              { value: "all", label: "Все связи" },
              { value: "normal", label: "Обычные" },
              { value: "stock_only", label: "Складские" },
            ]}
          />
          <button className="secondary-action" type="button" onClick={() => copyPlainText(savedSupplierList.join("\n"))} disabled={!savedSupplierList.length}>
            <Copy size={16} /> Скопировать поставщиков
          </button>
          <button className="secondary-action danger" type="button" onClick={() => bulkDeleteMutation.mutate()} disabled={readOnly || !selectedLinkIds.length || bulkDeleteMutation.isPending}>
            {bulkDeleteMutation.isPending ? <Loader2 className="spin" size={16} /> : <Trash2 size={16} />} Удалить выбранные {selectedLinkIds.length || ""}
          </button>
      </div>

      <div className="links-list">
        {filteredLinks.length ? filteredLinks.map((link) => (
          <div className="link-item pm-link-item" key={`${link.productId}-${link.id || linkPrimarySignature(link)}-${link.article}-${link.supplierName}`}>
            <label className="pm-link-select" title="Выбрать привязку">
              <input
                type="checkbox"
                disabled={readOnly}
                checked={selectedLinkIds.includes(linkSelectionKey(link))}
                onChange={(event) => setSelectedLinkIds((ids) => event.target.checked
                  ? Array.from(new Set([...ids, linkSelectionKey(link)]))
                  : ids.filter((id) => id !== linkSelectionKey(link)))}
              />
            </label>
            <div className="pm-link-body">
              <div className="pm-link-head">
                <div>
                  <strong>{link.article || link.supplierArticle || "без артикула PriceMaster"}</strong>
                  <span>{link.supplierName || "поставщик не указан"}</span>
                </div>
                <span className="pm-source-pill">{linkMatchText(link)}</span>
                {isStockOnlyLink(link) && <span className="pm-source-pill stock-only">не берет цену</span>}
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
                  Общая связь группы: {(link.productMarketplaces || [link.productMarketplace]).filter(Boolean).join(" + ") || "marketplace"}
                </span>
                <span className="pm-route-chip muted">
                  Карточки: {(link.productOfferIds || [link.productOfferId]).filter(Boolean).join(", ") || link.productId}
                </span>
                <span className="pm-route-chip muted">
                  Строк с этой связью: {link.groupCount || 1}/{products.length}
                </span>
                <span className="pm-route-chip muted">
                  выбран: {supplierText((link.productSelectedSuppliers || [link.productSelectedSupplier]).find(Boolean))}
                </span>
              </div>
            </div>
            <button className="icon-action danger" type="button" onClick={() => deleteMutation.mutate(link)} title="Удалить привязку" disabled={readOnly}>
              <Trash2 size={16} />
            </button>
          </div>
        )) : <div className="soft-empty">{groupLinkRows.length ? "По фильтру привязки не найдены." : "У товара пока нет привязок PriceMaster."}</div>}
      </div>

      <div className="draft-box">
        <div className="section-subtitle">Найти строку PriceMaster</div>
        <div className="draft-grid single-field">
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Артикул, название или штрихкод" disabled={readOnly} />
        </div>
        {searchQuery.data?.rows.length ? (
          <div className="pm-search-actions">
            <span>Найдено: {searchQuery.data.rows.length}</span>
            <button className="secondary-action" type="button" onClick={addAllSearchRows} disabled={readOnly}>
              <Check size={16} /> Выбрать все найденные
            </button>
          </div>
        ) : null}
        {(searchQuery.isFetching || searchQuery.data?.rows.length || searchQuery.error) && (
          <div className="pm-results">
            {searchQuery.isFetching && <div className="soft-empty compact"><Loader2 className="spin" size={16} /> Ищу в PriceMaster...</div>}
            {searchQuery.error && <div className="inline-error">{errorMessage(searchQuery.error)}</div>}
            {searchQuery.data?.rows.map((row) => {
              const rowDraft = draftFromSearchRow(row);
              const alreadySelected = draftKeys.has(linkPrimarySignature(rowDraft));
              return (
              <button className={`pm-result${alreadySelected ? " is-selected" : ""}${row.isTester || row.isOtlivant ? " pm-result-tester" : ""}`} type="button" key={`${row.id}-${row.article}-${row.supplierName}`} onClick={() => addDraft(rowDraft)}>
                <div className="pm-result-head">
                  <strong>{row.article || "без артикула"}{row.isTester ? <span className="pm-tester-badge">Тестер</span> : null}{row.isOtlivant ? <span className="pm-tester-badge">Отливант</span> : null}</strong>
                  <span>{alreadySelected ? "уже в черновике" : "выбрать точную строку"}</span>
                </div>
                <span className="pm-result-title">{row.name || row.keyword || "без названия"}</span>
                <span>{row.supplierName || "поставщик не указан"}</span>
                <small>row {row.rowId || row.id} · partner {row.partnerId || "-"} · {money(row.price)} · {row.priceCurrency || row.currency || "USD"} · {row.available ? "в наличии" : "нет наличия"} · {compactDate(row.updatedAt)}</small>
              </button>
            );})}
          </div>
        )}

        <div className="section-subtitle">Ручная привязка</div>
        <div className="info-strip compact">
          Лучше выбирать строку из поиска: тогда сохраняется rowId, partnerId и точное название PriceMaster. Ручная привязка нужна только как запасной вариант.
        </div>
        <div className="draft-grid manual-link-grid">
          <input value={draft.article} onChange={(event) => setDraft({ ...draft, article: event.target.value })} placeholder="Артикул PriceMaster" disabled={readOnly} />
          <input value={draft.keyword} onChange={(event) => setDraft({ ...draft, keyword: event.target.value })} placeholder="Ключевое слово" disabled={readOnly} />
          <SelectField
            ariaLabel="Валюта"
            value={draft.priceCurrency}
            onChange={(next) => setDraft({ ...draft, priceCurrency: next })}
            disabled={readOnly}
            options={[
              { value: "USD", label: "USD" },
              { value: "RUB", label: "RUB" },
            ]}
          />
        </div>
        <div className="draft-actions">
          <button className="secondary-action" type="button" onClick={() => addDraft(draft)} disabled={readOnly}>
            <Link2 size={16} /> Добавить в черновик
          </button>
          <button className="secondary-action" type="button" onClick={() => { setDrafts([]); setDraft(emptyLinkDraft(draft.priceCurrency)); }} disabled={readOnly || !pendingDrafts.length}>
            <X size={16} /> Очистить черновик
          </button>
          <button className="primary-action" disabled={readOnly || !pendingDrafts.length || saveMutation.isPending} type="button" onClick={() => saveMutation.mutate()}>
            {saveMutation.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить {pendingDrafts.length} привязок
          </button>
        </div>
        <div className="draft-preview">
          <strong>Черновик: {pendingDrafts.length}</strong>
          {drafts.length ? drafts.map((item, index) => (
            <button className="draft-chip draft-chip-rich" type="button" key={`${item.article || item.sourceRowId || item.exactName}-${index}`} onClick={() => setDrafts(drafts.filter((_, itemIndex) => itemIndex !== index))} title="Убрать из черновика">
              <span><b>{draftTitle(item)}</b>{draftSubtitle(item)}</span>
              {draftMeta(item) && <small>{draftMeta(item)}</small>}
              <X size={12} />
            </button>
          )) : <span>Новые привязки появятся здесь до сохранения.</span>}
          {draftIsFilled && <span className="draft-chip is-current">{draft.article || "текущий ввод"} · {draft.keyword || draft.priceCurrency}</span>}
        </div>
        {(saveMutation.error || deleteMutation.error || bulkDeleteMutation.error || syncMutation.error) && <div className="inline-error">
          {errorMessage(saveMutation.error || deleteMutation.error || bulkDeleteMutation.error || syncMutation.error)}
          {failedLinks.length ? (
            <ul className="pm-failed-links">
              {failedLinks.slice(0, 8).map((item, index) => {
                const row = asRecord(item);
                const candidates = Array.isArray(row.matches) ? row.matches : [];
                return (
                  <li key={`${row.index || index}-${row.article || row.sourceRowId || index}`}>
                    <span>{[row.index !== undefined ? `#${Number(row.index) + 1}` : "", row.article || row.sourceRowId || row.exactName, row.supplierName, row.detail].filter(Boolean).join(" \u00b7 ")}</span>
                    {candidates.length ? (
                      <div className="pm-candidate-list">
                        {candidates.slice(0, 5).map((candidate, candidateIndex) => {
                          const candidateRow = asRecord(candidate);
                          const label = [candidateRow.name, candidateRow.price ? money(Number(candidateRow.price)) : "", candidateRow.rowId ? `row ${candidateRow.rowId}` : ""].filter(Boolean).join(" \u00b7 ");
                          return (
                            <button className="secondary-action compact" type="button" key={`${candidateRow.rowId || candidateIndex}`} onClick={() => addDraft(draftFromFailedCandidate(candidate))}>
                              {"\u0412\u044b\u0431\u0440\u0430\u0442\u044c"}: {label || "PriceMaster"}
                            </button>
                          );
                        })}
                      </div>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          ) : null}
        </div>}
      </div>
    </section>
  );
}

function aiJobProgressText(jobInput: unknown) {
  const job = asRecord(jobInput);
  const status = String(job.status || "");
  const index = Number(job.variantIndex || 0);
  const total = Number(job.variantTotal || 5) || 5;
  const done = Array.isArray(job.draftIds) ? job.draftIds.length : 0;
  if (status === "queued") return "подготовка";
  if (status === "running") return index ? `Codex: генерация ${index}/${total}, готово ${done}/${total}` : `Codex: готово ${done}/${total}`;
  if (status === "completed") return `готово ${done || total}/${total}`;
  if (status === "partial") return `частично готово ${done}/${total}`;
  if (status === "failed") return "ошибка генерации";
  return "";
}

function aiJobProgressPercent(jobInput: unknown) {
  const job = asRecord(jobInput);
  const progress = Number(job.progress || 0);
  if (Number.isFinite(progress) && progress > 0) return Math.max(4, Math.min(100, progress));
  const total = Number(job.variantTotal || 5) || 5;
  const done = Array.isArray(job.draftIds) ? job.draftIds.length : 0;
  return Math.max(4, Math.min(100, Math.round((done / total) * 100)));
}

function AiImagesPanel({ product, products, onSaved }: { product: Product; products: Product[]; onSaved: () => void }) {
  const queryClient = useQueryClient();
  const [progress, setProgress] = useState("");
  const [freshDrafts, setFreshDrafts] = useState(product.aiImages || []);
  const [assistant, setAssistant] = useState<Record<string, unknown> | null>(null);
  const [activeJobId, setActiveJobId] = useState("");
  const [activeJob, setActiveJob] = useState<Record<string, unknown> | null>(null);
  const visibleDrafts = freshDrafts.length ? freshDrafts : product.aiImages || [];
  const targetProducts = useMemo(() => {
    const byMarketplace = new Map<string, Product>();
    for (const item of products.length ? products : [product]) {
      const raw = String(item.marketplace || "").toLowerCase();
      const marketplace = raw.includes("ozon") ? "ozon" : raw.includes("yandex") ? "yandex" : "";
      if (marketplace && !byMarketplace.has(marketplace)) byMarketplace.set(marketplace, item);
    }
    return Array.from(byMarketplace.entries()).map(([marketplace, item]) => ({ marketplace, product: item, label: marketplace === "ozon" ? "Ozon" : "Yandex" }));
  }, [product, products]);
  const assistantDraft = asRecord(assistant?.draft);
  const contentDraftId = String(assistantDraft.id || "");
  const premiumMarketplace = String(product.marketplace || "").toLowerCase().includes("yandex") ? "yandex" : "ozon";
  useEffect(() => {
    setFreshDrafts(product.aiImages || []);
  }, [product.aiImages]);
  useEffect(() => {
    setFreshDrafts(product.aiImages || []);
    setAssistant(null);
    setActiveJobId("");
    setActiveJob(null);
    setProgress("");
  }, [product.id]);

  const jobRunning = ["queued", "running"].includes(String(asRecord(activeJob).status || ""));
  const jobQuery = useQuery({
    queryKey: ["warehouse", "ai-image-job", product.id, activeJobId],
    enabled: Boolean(activeJobId),
    queryFn: async () => fetchJson(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/jobs/${encodeURIComponent(activeJobId)}`, AiImageJobResponseSchema),
    refetchInterval: jobRunning ? 3000 : false,
  });

  useEffect(() => {
    const payload = jobQuery.data;
    if (!payload) return;
    const job = asRecord(payload.job);
    if (Object.keys(job).length) {
      setActiveJob(job);
      setProgress(aiJobProgressText(job));
    }
    if (payload.product?.aiImages?.length) {
      setFreshDrafts(payload.product.aiImages);
      updateCachedProducts(queryClient, { product: payload.product });
    }
    const status = String(job.status || "");
    if (["completed", "failed", "partial"].includes(status)) {
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    } else if (status === "running") {
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    }
  }, [jobQuery.data, queryClient, onSaved]);

  const assistantMutation = useMutation({
    mutationFn: async () => fetchJson(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-assistant`, AiAssistantResponseSchema, mutationBody({ marketplace: "yandex" })),
    onSuccess: (payload) => {
      setAssistant(payload);
      if (payload.product) {
        updateCachedProducts(queryClient, { product: payload.product });
        void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
        onSaved();
      }
    },
  });

  const generateMutation = useMutation({
    mutationFn: async () => {
      const sourceImageUrl = firstImage(product);
      if (!sourceImageUrl) throw new Error("Source product photo is required for Codex generation.");
      setProgress("preparing");
      return await fetchJson(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/generate`, AiImageJobResponseSchema, mutationBody({
        sourceImageUrl,
        prompt: `Create exactly one standalone marketplace photo for perfume product ${product.name || product.offerId}. This request is one image only; do not create a collage, grid, contact sheet, multi-panel layout, before/after layout, or multiple bottles in one image. Use the source product photo as the required reference for bottle shape, cap, original label and color. Final image must show only one clean perfume bottle, upright and centered. The full bottle must fit inside the square image from cap to base, with 10-15% empty margin on every side; do not crop, cut off, zoom into, or place the bottle at the edge. Remove or ignore any box, outer packaging, cartons, bags, brochures, accessories or props. Do not create or hallucinate packaging. No headline, no marketing typography, no large brand text outside the original bottle label. White or light studio background, premium lighting, no watermarks, no product distortion.`,
        photoPresets: studioPhotoPresets,
        count: 5,
        forceCodexSale: true,
        requireSourceImage: true,
      }));
    },
    onSuccess: (payload) => {
      const job = asRecord(payload.job);
      const jobId = String(payload.jobId || job.id || job.jobId || "");
      if (jobId) {
        setActiveJobId(jobId);
        setActiveJob(job);
        setProgress(aiJobProgressText(job) || "preparing");
      }
      if (payload.product?.aiImages?.length) {
        setFreshDrafts(payload.product.aiImages);
        updateCachedProducts(queryClient, { product: payload.product });
      }
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
    onError: () => setProgress("start failed"),
  });

  const premiumMutation = useMutation({
    mutationFn: async () => fetchJson(
      `/api/warehouse/products/${encodeURIComponent(product.id)}/premium-images/generate`,
      MutationProductResponseSchema,
      mutationBody({ count: 5, marketplace: premiumMarketplace, useLogo: true }),
    ),
    onSuccess: (payload) => {
      if (payload.product?.aiImages?.length) setFreshDrafts(payload.product.aiImages);
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      setProgress("premium photos ready");
      onSaved();
    },
    onError: () => setProgress("premium failed"),
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

  const contentSendMutation = useMutation({
    mutationFn: async ({ marketplace }: { targetProduct: Product; marketplace: string }) => fetchJson(
      `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-content/${encodeURIComponent(contentDraftId)}/send`,
      MutationProductResponseSchema,
      mutationBody({ marketplace }),
    ),
    onSuccess: (payload) => {
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
  });

  const imageSendMutation = useMutation({
    mutationFn: async ({ marketplace, draftId }: { targetProduct: Product; marketplace: string; draftId: string }) => fetchJson(
      `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/${encodeURIComponent(draftId)}/send`,
      MutationProductResponseSchema,
      mutationBody({ marketplace }),
    ),
    onSuccess: (payload) => {
      if (payload.product?.aiImages?.length) setFreshDrafts(payload.product.aiImages);
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
  });

  const generationBusy = generateMutation.isPending || jobRunning;
  const premiumBusy = premiumMutation.isPending;
  const jobError = asRecord(asRecord(activeJob).lastError);
  const jobErrorText = String(jobError.detail || jobError.code || "");

  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>Фото карточки</span>
          <h3>AI-помощник карточки</h3>
        </div>
        <button className="primary-action" type="button" disabled={premiumBusy} onClick={() => premiumMutation.mutate()}>
          {premiumBusy ? <Loader2 className="spin" size={16} /> : <ImagePlus size={16} />} Собрать 5 премиум-фото
        </button>
        <button className="secondary-action" type="button" disabled={assistantMutation.isPending} onClick={() => assistantMutation.mutate()}>
          {assistantMutation.isPending ? <Loader2 className="spin" size={16} /> : <Sparkles size={16} />} Улучшить карточку
        </button>
        <button className="secondary-action" type="button" disabled={generationBusy} onClick={() => generateMutation.mutate()}>
          {generationBusy ? <Loader2 className="spin" size={16} /> : <ImagePlus size={16} />} Codex: 5 фото
        </button>
      </div>
      <div className="info-strip compact">Премиум-фото собираются из реальных фото товара через шаблоны: без коллажей, без обрезки флакона и без перерисовки AI.</div>
      <div className="preset-strip">
        {studioPhotoPresets.map((preset) => <span className="formula-chip" key={preset.id}>{preset.label}</span>)}
      </div>
      {assistant && <div className="ai-assistant-card">
        <strong>Черновик улучшения</strong>
        <p>{String(asRecord(assistant.draft).description || "").slice(0, 520) || "AI вернул черновик, но описание пустое."}</p>
        <div className="ai-assistant-tags">
          {(Array.isArray(asRecord(assistant.draft).seoKeywords) ? asRecord(assistant.draft).seoKeywords as unknown[] : []).slice(0, 10).map((item, index) => <span key={`${item}-${index}`}>{String(item)}</span>)}
        </div>
        <div className="ai-checklist">
          {(Array.isArray(assistant.checklist) ? assistant.checklist : []).map((item, index) => {
            const row = asRecord(item);
            return <span className={row.ok ? "is-ok" : ""} key={`${row.id || index}`}>{row.ok ? "✓" : "•"} {String(row.label || row.id)} · {String(row.detail || "")}</span>;
          })}
        </div>
        {contentDraftId ? <div className="ai-send-panel">
          <strong>Отправка текста</strong>
          <div className="ai-send-actions">
            {targetProducts.map((target) => (
              <button
                className="secondary-action"
                type="button"
                key={`content-${target.marketplace}`}
                disabled={contentSendMutation.isPending}
                onClick={() => contentSendMutation.mutate({ targetProduct: target.product, marketplace: target.marketplace })}
              >
                {contentSendMutation.isPending ? <Loader2 className="spin" size={15} /> : <Check size={15} />} Текст в {target.label}
              </button>
            ))}
          </div>
        </div> : null}
      </div>}
      {progress && <div className="progress-line"><span style={{ width: `${aiJobProgressPercent(activeJob || { progress: generateMutation.isPending ? 5 : 100 })}%` }} />{progress}</div>}
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
            <div className="ai-send-actions compact">
              {targetProducts.map((target) => (
                <button
                  type="button"
                  key={`image-${draft.id}-${target.marketplace}`}
                  disabled={imageSendMutation.isPending || !draft.resultUrl}
                  onClick={() => imageSendMutation.mutate({ targetProduct: target.product, marketplace: target.marketplace, draftId: draft.id })}
                  title={`Отправить фото в ${target.label}`}
                >
                  {imageSendMutation.isPending ? <Loader2 className="spin" size={14} /> : <ImagePlus size={14} />} {target.label}
                </button>
              ))}
            </div>
          </div>
        )) : <div className="soft-empty">Здесь появятся сгенерированные изображения.</div>}
      </div>
      {jobErrorText && ["failed", "partial"].includes(String(asRecord(activeJob).status || "")) && <div className="inline-error">
        {jobErrorText}{jobError.code ? ` | code: ${String(jobError.code)}` : ""}{jobError.status ? ` | status: ${String(jobError.status)}` : ""}{jobError.model ? ` | model: ${String(jobError.model)}` : ""}{jobError.endpoint ? ` | endpoint: ${String(jobError.endpoint)}` : ""}
      </div>}
      {(assistantMutation.error || generateMutation.error || premiumMutation.error || reviewMutation.error || contentSendMutation.error || imageSendMutation.error || jobQuery.error) && <div className="inline-error">{errorMessage(assistantMutation.error || generateMutation.error || premiumMutation.error || reviewMutation.error || contentSendMutation.error || imageSendMutation.error || jobQuery.error)}</div>}
    </section>
  );
}

function saleTone(code: unknown) {
  const text = String(code || "");
  if (text === "ready") return "success";
  if (text === "api_error" || text === "archived") return "danger";
  if (text === "api_pending" || text === "no_supplier" || text === "no_stock" || text === "no_links" || text === "stock_stale") return "warn";
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
    item.warning ? String(item.warning) : "",
    item.nextRetryAt ? `next ${compactDate(String(item.nextRetryAt))}` : "",
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
  const diagnosticLinks = products.flatMap((item) => Array.isArray(item.links) ? item.links.map(asRecord) : []);
  const activeLinks = diagnosticLinks.filter((link) => link.missingInPriceMaster !== true);
  const unavailableLinks = diagnosticLinks.filter((link) => link.available === false || link.missingInPriceMaster === true);
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
        <DiagnosticValue label="Остаток ждёт отправки" value={summary.stockStale ?? 0} tone={Number(summary.stockStale || 0) > 0 ? "warn" : "success"} />
        <DiagnosticValue label="API ошибки" value={summary.apiError ?? 0} tone={Number(summary.apiError || 0) > 0 ? "danger" : "success"} />
      </div>
      <div className="diagnostics-summary diagnostics-summary-wide">
        <DiagnosticValue label="PM привязки" value={diagnosticLinks.length} tone={diagnosticLinks.length ? "success" : "warn"} />
        <DiagnosticValue label="PM найдено" value={activeLinks.length} tone={activeLinks.length ? "success" : "warn"} />
        <DiagnosticValue label="PM без наличия/ошибка" value={unavailableLinks.length} tone={unavailableLinks.length ? "warn" : "success"} />
        <DiagnosticValue label="Выбран поставщик" value={products.filter((item) => asRecord(item.selectedSupplier).supplierName).length} tone="success" />
      </div>
      {warnings.length > 0 && <div className="warning-strip">{warnings.map(String).join(" · ")}</div>}
      {products.map((item) => {
        const automation = asRecord(item.automation);
        const saleState = asRecord(item.saleState);
        const formula = asRecord(item.priceFormula);
        const selectedSupplier = asRecord(item.selectedSupplier);
        const formulaText = [
          item.marketplace ? String(item.marketplace) : "",
          selectedSupplier.price ? `${String(selectedSupplier.price)} ${String(selectedSupplier.currency || "USD")}` : "",
          formula.usdRate ? `курс ${String(formula.usdRate)}` : "",
          formula.markupCoefficient ? `коэф. ${String(formula.markupCoefficient)}` : "",
          item.targetPrice ? `итог ${money(item.targetPrice)}` : "",
        ].filter(Boolean).join(" · ");
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
              {formulaText && <span><b>Формула цены:</b> {formulaText}</span>}
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

type MarketplaceBreakdownRow = {
  productId?: string;
  offerId?: string;
  marketplace?: string;
  targetName?: string;
  statusCode?: string;
  statusLabel?: string;
  archived?: boolean;
  changed?: boolean;
  ready?: boolean;
  currentPrice?: number | null;
  targetPrice?: number | null;
  targetStock?: number | null;
  supplierName?: string;
  supplierArticle?: string;
  ozonMinPrice?: number | null;
  ozonUnarchiveQueued?: boolean;
  ozonUnarchiveNextRetryAt?: string;
  lastPriceSendAt?: string;
  stockOnlyFallback?: boolean;
};

function marketplaceStatusTone(code = "", archived = false): "success" | "warning" | "danger" | "neutral" {
  if (archived || code === "archived") return "danger";
  if (code === "active" || code === "ready") return "success";
  if (code === "out_of_stock" || code === "inactive") return "warning";
  return "neutral";
}

function MarketplaceRows({ products, breakdown = [] }: { products: Product[]; breakdown?: MarketplaceBreakdownRow[] }) {
  const groupLinkCount = uniqueLinks(products).length;
  const marketplaceBadges = groupMarketplaceLabels(products);
  const breakdownByProductId = useMemo(
    () => new Map(breakdown.map((row) => [String(row.productId || ""), row])),
    [breakdown],
  );
  const marketplaceRows = useMemo(() => {
    const grouped = new Map<string, Product[]>();
    for (const product of products) {
      const label = marketplaceRowLabel(product);
      if (!grouped.has(label)) grouped.set(label, []);
      grouped.get(label)?.push(product);
    }
    return Array.from(grouped.entries()).map(([label, items]) => ({
      key: items.map((item) => item.id).sort().join("|"),
      label: items.length > 1 ? `${label} ×${items.length}` : label,
      product: preferredGroupPrimary(items),
      items,
      breakdown: breakdownByProductId.get(String(preferredGroupPrimary(items).id || "")),
    }));
  }, [products, breakdownByProductId]);
  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>Marketplace</span>
          <h3>Строки карточки</h3>
        </div>
        <span className="section-count">{marketplaceBadges.length || marketplaceRows.length}</span>
      </div>
      {marketplaceBadges.length ? (
        <div className="market-badges compact" aria-label="marketplaces-summary">
          {marketplaceBadges.map((marketplace) => <span className="market-badge" key={marketplace}>{marketplace}</span>)}
        </div>
      ) : null}
      <div className="marketplace-rows">
        {marketplaceRows.map(({ key, label, product, items, breakdown: rowBreakdown }) => {
          const collapsedGroup = items.length > 1 ? groupProductsForList(items)[0] : undefined;
          const status = collapsedGroup ? groupStatusLabel(collapsedGroup) : statusLabel(product);
          const rowStatusCode = String(rowBreakdown?.statusCode || asRecord(product.marketplaceState).code || "");
          const rowStatusLabel = String(rowBreakdown?.statusLabel || asRecord(product.marketplaceState).label || status.label);
          const rowArchived = Boolean(rowBreakdown?.archived || product.archived || rowStatusCode === "archived");
          const rowTone = marketplaceStatusTone(rowStatusCode, rowArchived);
          const stock = Number(product.targetStock || product.stock || 0);
          const changed = Number(product.newPrice || product.targetPrice || 0) > 0 && Number(product.currentPrice || 0) !== Number(product.newPrice || product.targetPrice || 0);
          const supplier = asRecord(product.selectedSupplier);
          const formula = asRecord(product.priceFormula);
          const markupCoefficient = Number(product.markupCoefficient || supplier.markupCoefficient || formula.markupCoefficient || 0) || 0;
          const baseMarkupCoefficient = Number(supplier.baseMarkupCoefficient || formula.baseMarkupCoefficient || 0) || 0;
          const usdRate = Number(product.usdRate || formula.usdRate || 0) || 0;
          const supplierPrice = Number(supplier.price || formula.selectedSupplierPrice || 0) || 0;
          const supplierCurrency = String(supplier.currency || supplier.priceCurrency || formula.selectedSupplierCurrency || "");
          const supplierEffectivePrice = Number(supplier.effectiveFinalPrice || supplier.calculatedPrice || 0) || 0;
          const supplierAlternatives = Array.isArray((product as any).supplierAlternatives) ? (product as any).supplierAlternatives as Array<Record<string, unknown>> : [];
          const targetPrice = Number(product.newPrice || product.targetPrice || formula.targetPrice || 0) || 0;
          const stockOnlyFallback = Boolean(product.stockOnlyFallbackActive || formula.stockOnlyFallbackActive || supplier.stockOnly || supplier.priceEligible === false);
          const stockOnlyManualPrice = Number(formula.stockOnlyManualPrice || supplier.manualPrice || targetPrice || 0) || 0;
          const lastArchiveSend = asRecord(asRecord(product).lastArchiveSend);
          const ozonUnarchiveQueued = Boolean(rowBreakdown?.ozonUnarchiveQueued || lastArchiveSend.queuedByDailyLimit || lastArchiveSend.warning === "ozon_unarchive_daily_limit_queued" || lastArchiveSend.warning === "linked_activation_queued");
          const ozonUnarchiveNextRetryAt = String(rowBreakdown?.ozonUnarchiveNextRetryAt || lastArchiveSend.nextRetryAt || "");
          const formulaParts = [
            stockOnlyFallback ? "Складской fallback · цена PM не используется" : "",
            supplier.supplierName ? `Поставщик: ${String(supplier.supplierName)}${supplier.article ? ` · ${String(supplier.article)}` : ""}${supplier.name || supplier.nativeName || supplier.exactName ? ` · ${String(supplier.name || supplier.nativeName || supplier.exactName)}` : ""}` : "",
            markupCoefficient ? `Коэф.: ${markupCoefficient}${baseMarkupCoefficient && baseMarkupCoefficient !== markupCoefficient ? ` (база ${baseMarkupCoefficient})` : ""}` : "",
            usdRate ? `Курс: ${usdRate}` : "",
            supplierPrice && !stockOnlyFallback ? `PM: ${supplierPrice} ${supplierCurrency || "USD"}` : "",
            supplierEffectivePrice && !stockOnlyFallback ? `Итог поставщика: ${money(supplierEffectivePrice)}` : "",
            product.priceSource ? `Источник PM: ${String(product.priceSource)}` : "",
            stockOnlyFallback && stockOnlyManualPrice ? `Ручная fallback-цена: ${money(stockOnlyManualPrice)}` : "",
            targetPrice ? `Новая цена: ${money(targetPrice)}` : "",
          ].filter(Boolean);
          const alternativeParts = supplierAlternatives
            .slice(0, 5)
            .map((alt) => {
              const name = String(alt.partnerName || alt.supplierName || "").trim();
              const pmRowName = String(alt.name || alt.nativeName || alt.exactName || "").trim();
              const finalPrice = Number(alt.effectiveFinalPrice || alt.calculatedPrice || 0) || 0;
              const rawPrice = Number(alt.price || 0) || 0;
              const currency = String(alt.priceCurrency || alt.sourceCurrency || "USD");
              const excluded = String(alt.exclusionReason || "").trim();
              return [name, pmRowName, finalPrice ? money(finalPrice) : "", rawPrice ? `PM ${rawPrice} ${currency}` : "", excluded ? `не выбран: ${excluded}` : ""].filter(Boolean).join(" · ");
            })
            .filter(Boolean);
          return (
            <div className="marketplace-row" key={key}>
              <div>
                <strong>{label}</strong>
                <span>{product.offerId || product.sku || product.id}</span>
              </div>
              <span className={`pill ${rowTone}`}>{status.icon}{rowStatusLabel || status.label}</span>
              <div>
                <small>Цена</small>
                <strong>{money(rowBreakdown?.targetPrice || product.newPrice || product.targetPrice || product.currentPrice)}</strong>
                {rowBreakdown?.currentPrice ? <small>кабинет {money(rowBreakdown.currentPrice)}</small> : null}
              </div>
              <div>
                <small>Остаток</small>
                <strong>{(rowBreakdown?.targetStock ?? stock) || "-"}</strong>
              </div>
              <div>
                <small>Общие PM</small>
                <strong>{groupLinkCount}</strong>
              </div>
              <div className="marketplace-flags">
                {alternativeParts.length ? <span className="formula-chip muted">Альтернативы: {alternativeParts.join(" / ")}</span> : null}
                {rowBreakdown?.ozonMinPrice ? <span className="formula-chip muted">Мин. Ozon: {money(rowBreakdown.ozonMinPrice)}</span> : null}
                {rowBreakdown?.lastPriceSendAt ? <span className="formula-chip muted">Цена отправлена: {compactDate(rowBreakdown.lastPriceSendAt)}</span> : null}
                {ozonUnarchiveQueued && <span>Ожидает разархива Ozon{ozonUnarchiveNextRetryAt ? ` · ${compactDate(ozonUnarchiveNextRetryAt)}` : ""}</span>}
                {rowArchived && product.marketplace === "yandex" && <span>В архиве Яндекс</span>}
                {rowBreakdown?.changed && <span>Цена изменилась</span>}
                {rowBreakdown?.ready && <span>Готов к продаже</span>}
                {rowBreakdown?.stockOnlyFallback && <span>Складской fallback</span>}
                {formulaParts.length ? formulaParts.map((part) => <span className="formula-chip" key={part}>{part}</span>) : <span className="formula-chip">PriceMaster не выбран</span>}
                <span className="formula-chip muted">общие привязки, отдельный расчет цены</span>
                {product.archived && <span>Архив</span>}
                {changed && <span>Цена ждет</span>}
                <EtaChips productId={String(product.id)} archived={rowArchived} changed={changed} />
                {!product.selectedSupplier && (product.links || []).length > 0 && <span>Поставщик не выбран</span>}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function formatEtaSeconds(seconds: number): string {
  if (seconds <= 30) return "вот-вот";
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `~${minutes} мин`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `~${hours} ч ${minutes % 60 ? `${minutes % 60} мин` : ""}`.trim();
  return `~${Math.round(hours / 24)} дн`;
}

function EtaChips({ productId, archived, changed }: { productId: string; archived: boolean; changed: boolean }) {
  const [eta, setEta] = useState<{ priceEtaAt?: string; unarchiveEtaAt?: string | null } | null>(null);
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    let alive = true;
    fetch(`/api/warehouse/products/${encodeURIComponent(productId)}/eta`, { credentials: "same-origin" })
      .then((response) => (response.ok ? response.json() : null))
      .then((data) => { if (alive && data?.ok) setEta(data); })
      .catch(() => {});
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => { alive = false; window.clearInterval(timer); };
  }, [productId]);
  if (!eta) return null;
  const priceSeconds = eta.priceEtaAt ? Math.max(0, Math.round((new Date(eta.priceEtaAt).getTime() - now) / 1000)) : null;
  const unarchiveSeconds = eta.unarchiveEtaAt ? Math.max(0, Math.round((new Date(eta.unarchiveEtaAt).getTime() - now) / 1000)) : null;
  return (
    <>
      {changed && priceSeconds !== null && (
        <span className="formula-chip muted" title="Когда реконсайлер дойдет до товара и отправит цену">
          ⏱ Цена обновится {formatEtaSeconds(priceSeconds)}
        </span>
      )}
      {archived && unarchiveSeconds !== null && (
        <span className="formula-chip" title="Оценка времени выхода из архива">
          ⏱ Разархив {formatEtaSeconds(unarchiveSeconds)}
        </span>
      )}
    </>
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
      <button className="copy-action" type="button" onClick={() => copyValue("name", copyableLatinProductName(product.name) || product.offerId || product.sku || "")} title="Скопировать название">
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
      refreshWarehouseAfterMutation(queryClient, payload);
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
      refreshWarehouseAfterMutation(queryClient, payload);
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
      const ids = Array.from(new Set([...productIds, ...(result.items || []).filter(isProductPageItem).map((item) => item.id).filter(Boolean)]));
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
  const repair = useMutation({
    mutationFn: () => fetchJson(`/api/warehouse/products/${encodeURIComponent(primary.id)}/repair`, ProductRepairSchema, mutationBody({})),
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
        <button className="primary-action" type="button" onClick={() => repair.mutate()} disabled={repair.isPending}>
          {repair.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Проверить и починить товар
        </button>
        <button className="secondary-action" type="button" onClick={() => start.mutate("linked-supplier-recovery")} disabled={start.isPending}>Восстановить товар</button>
        <button className="secondary-action" type="button" onClick={() => start.mutate("yandex-stock-sync")} disabled={start.isPending}>Отправить остаток</button>
        <button className="secondary-action" type="button" onClick={() => start.mutate("yandex-import-send")} disabled={start.isPending}>Отправить цену</button>
      </div>
      {repair.data ? (
        <div className="success-strip compact">
          {repair.data.accepted
            ? `Repair queued: ${String((repair.data.job as Record<string, unknown> | undefined)?.id || "background job")}`
            : `Links ${repair.data.linksSynced} · prices ${repair.data.priceSent} · stock ${repair.data.stockSent} · ${repair.data.pending ? "ожидает восстановления" : "готово"}`}
        </div>
      ) : null}
      {start.error && <div className="inline-error">{errorMessage(start.error)}</div>}
      {repair.error && <div className="inline-error">{errorMessage(repair.error)}</div>}
    </section>
  );
}

function DetailPanel({ selectedGroup, products, breakdown = [], onClose, isAdmin, filteredOut = false, loading = false, refreshing = false, demoMode = false }: { selectedGroup: string; products: Product[]; breakdown?: MarketplaceBreakdownRow[]; onClose: () => void; isAdmin: boolean; filteredOut?: boolean; loading?: boolean; refreshing?: boolean; demoMode?: boolean }) {
  const primary = products.length ? preferredGroupPrimary(products) : undefined;
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(false);
  const queryClient = useQueryClient();
  const diagnostics = useQuery({
    queryKey: ["warehouse", "diagnostics", primary?.offerId],
    queryFn: () => fetchJson(`/api/warehouse/products/diagnostics?sku=${encodeURIComponent(primary?.offerId || "")}`, DiagnosticsSchema),
    enabled: diagnosticsOpen && Boolean(primary?.offerId),
  });
  const detailGroup = useMemo(() => {
    if (!products.length) return undefined;
    const grouped = groupProductsForList(products);
    return grouped.find((group) => group.groupKey === selectedGroup) || grouped[0];
  }, [products, selectedGroup]);
  // Invalidating the "warehouse" prefix covers both the catalog list (["warehouse","page",...])
  // and this group's detail query (["warehouse","group-detail",selectedGroup]) — a silent
  // refetch so quick actions (repair/recovery/group/ungroup) reflect server-side changes
  // without an explicit reload (PLAN-HARDENING.md 5.2).
  const refreshDetail = () => void queryClient.invalidateQueries({ queryKey: ["warehouse"] });

  if (!primary) {
    return (
      <aside className={`detail-panel ${selectedGroup ? "" : "empty-panel"}`}>
        {selectedGroup && loading ? (
          <>
            <Loader2 className="spin" size={28} />
            <strong>Загружаю карточку...</strong>
            <span>Привязки, цены и остатки появятся через секунду.</span>
            <button className="mobile-close" type="button" onClick={onClose}><X size={18} /></button>
          </>
        ) : (
          <>
            <PackageCheck size={28} />
            <strong>Выберите товар</strong>
            <span>Здесь откроются привязки, цены, остатки, AI-фото и диагностика.</span>
          </>
        )}
      </aside>
    );
  }
  const image = firstImage(primary);
  const status = detailGroup ? groupStatusLabel(detailGroup) : statusLabel(primary);
  const groupLinkCount = uniqueLinks(products).length;

  return (
    <aside className="detail-panel">
      <div className="detail-head">
        <div className="detail-image">{image ? <img src={image} alt="" /> : <Sparkles size={24} />}</div>
        <div className="detail-title">
          <span className={`pill ${status.tone}`}>{status.icon}{status.label}</span>
          <h2>{primary.name || primary.offerId}</h2>
          <p>{primary.offerId} · {primary.brand || "без бренда"}</p>
          <div className="market-badges compact" aria-label="marketplaces">
            {groupMarketplaceLabels(products).map((marketplace) => <span className="market-badge" key={marketplace}>{marketplace}</span>)}
          </div>
          <CopyActions product={primary} />
        </div>
        <div className="detail-head-actions">
          {refreshing ? <Loader2 className="spin" size={16} aria-label="Обновляю карточку" /> : null}
          <button className="mobile-close" type="button" onClick={onClose}><X size={18} /></button>
        </div>
      </div>
      {filteredOut ? <div className="warning-strip compact">Карточка скрыта текущими фильтрами, но открыта для проверки.</div> : null}
      <div className="stats-grid">
        <Stat label="Текущая цена" value={money(primary.currentPrice)} />
        <Stat label="Новая цена" value={money(primary.newPrice || primary.targetPrice)} />
        <Stat label="Остаток" value={primary.targetStock || primary.stock || "-"} />
        <Stat label="Привязки" value={groupLinkCount} />
      </div>
      <LinksPanel key={products.map((item) => item.id).sort().join("|")} products={products} onSaved={refreshDetail} readOnly={demoMode} />
      <MarketplaceRows products={products} breakdown={breakdown} />
      {isAdmin && !demoMode ? <GroupActions products={products} selectedGroup={selectedGroup} onDone={refreshDetail} /> : null}
      {isAdmin && !demoMode ? <QuickActions primary={primary} products={products} onDone={refreshDetail} /> : null}
      {isAdmin && !demoMode ? <AiImagesPanel product={primary} products={products} onSaved={refreshDetail} /> : null}
      {isAdmin && !demoMode ? <section className="detail-section">
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
      </section> : null}
      {demoMode ? (
        <div className="info-strip compact demo-readonly-strip">
          Демо-карточка: синхронизация, AI-фото и диагностика недоступны для примера — здесь показана только структура карточки.
        </div>
      ) : null}
    </aside>
  );
}

export function WarehousePage({ isAdmin = true }: { isAdmin?: boolean }) {
  const queryClient = useQueryClient();
  const [filters, setFilters] = useState<Filters>(() => readFilters());
  const [selectedGroup, setSelectedGroup] = useState(() => selectedGroupFromPath());
  const [isMobileList, setIsMobileList] = useState(() => typeof window !== "undefined" && window.matchMedia(mobileListMedia).matches);
  const debouncedQ = useDebounced(filters.q, 450);
  const effectiveFilters = { ...filters, q: debouncedQ };
  const parentRef = useRef<HTMLDivElement>(null);
  const brandsQuery = useQuery({
    queryKey: ["warehouse", "brands"],
    queryFn: () => fetchJson("/api/warehouse/brands", WarehouseBrandsSchema),
    staleTime: 10 * 60_000,
  });
  const refreshBrands = useMutation({
    mutationFn: () => fetchJson("/api/warehouse/brands/rebuild-index", BrandIndexStatusSchema, mutationBody({ limit: 100000 })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["warehouse", "brands"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse", "page"] });
    },
  });

  const brandOptions = brandsQuery.data?.brands || [];

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
    placeholderData: (previousData, previousQuery) => {
      const prevFilters = previousQuery?.queryKey?.[2] as Filters | undefined;
      if (prevFilters?.linked !== effectiveFilters.linked) return undefined;
      return previousData;
    },
    staleTime: 30_000,
    gcTime: 120_000,
  });
  const rows = useMemo(
    () => (pageQuery.data?.items || []).filter(isProductPageItem),
    [pageQuery.data?.items],
  );
  const serverGroups = useMemo(() => {
    if (pageQuery.data?.grouped) {
      return (pageQuery.data.items || [])
        .filter(isProductGroupPageItem)
        .map((item) => mapServerPageGroup(item))
        .filter((group): group is ProductGroup => Boolean(group));
    }
    return groupProductsForList(rows);
  }, [pageQuery.data, rows]);
  const serverProductCount = useMemo(
    () => serverGroups.reduce((sum, group) => sum + group.products.length, 0),
    [serverGroups],
  );
  const hasRealServerCatalog = useMemo(
    () => serverGroups.some(groupHasRealProduct),
    [serverGroups],
  );
  const demoGroups = useMemo(() => demoGroupsForFilters(effectiveFilters), [effectiveFilters]);
  const useDemoCatalog = Boolean(
    !pageQuery.isLoading
      && !hasRealServerCatalog
      && (pageQuery.error
        || (pageQuery.data && Number(pageQuery.data.totalAll || pageQuery.data.total || pageQuery.data.groupTotal || serverProductCount || 0) <= Math.max(1, serverProductCount))),
  );
  const groups = useDemoCatalog ? demoGroups : serverGroups;
  const selectedRowsOnPage = useMemo(() => groups.find((group) => group.groupKey === selectedGroup)?.products || [], [groups, selectedGroup]);
  const selectedIsDemoGroup = Boolean(useDemoCatalog && groups.some((group) => group.groupKey === selectedGroup));
  const selectedFilteredOut = Boolean(selectedGroup && !pageQuery.isLoading && !groups.some((group) => group.groupKey === selectedGroup));

  const detailQuery = useQuery({
    queryKey: ["warehouse", "group-detail", selectedGroup],
    queryFn: () => fetchJson(`/api/warehouse/products/group-detail?group=${encodeURIComponent(selectedGroup)}`, GroupDetailSchema),
    enabled: Boolean(selectedGroup) && !selectedIsDemoGroup,
    staleTime: 60_000,
    gcTime: 180_000,
    retry: 2,
    retryDelay: (attempt) => Math.min(1500 * (attempt + 1), 4000),
  });
  const detailProducts = detailQuery.data?.products?.length
    ? detailQuery.data.products
    : selectedRowsOnPage;
  const detailBreakdown = useDemoCatalog
    ? buildDemoBreakdown(detailProducts)
    : (detailQuery.data as { marketplacePriceBreakdown?: MarketplaceBreakdownRow[] } | undefined)?.marketplacePriceBreakdown || [];
  const detailLoading = !detailProducts.length && detailQuery.isFetching && Boolean(selectedGroup);
  const detailRefreshing = Boolean(detailProducts.length && detailQuery.isFetching && selectedGroup);
  const catalogStats = useMemo(() => {
    if (!useDemoCatalog) return null;
    const products = groups.flatMap((group) => group.products);
    const ready = groups.filter((group) => group.statusSummary.ready > 0).length;
    const changed = groups.filter((group) => group.statusSummary.changed > 0).length;
    const withoutSupplier = groups.filter((group) => group.statusSummary.withoutSupplier > 0).length;
    return {
      groupTotal: groups.length,
      total: groups.length,
      rowTotal: products.length,
      totalAll: groups.length,
      ready,
      changed,
      withoutSupplier,
    };
  }, [groups, useDemoCatalog]);
  useEffect(() => {
    if (!useDemoCatalog) return;
    const firstGroup = groups[0]?.groupKey || "";
    if (!firstGroup) return;
    if (!selectedGroup || !groups.some((group) => group.groupKey === selectedGroup)) {
      setSelectedGroup(firstGroup);
    }
  }, [groups, selectedGroup, useDemoCatalog]);
  useEffect(() => {
    const media = window.matchMedia(mobileListMedia);
    const onChange = () => setIsMobileList(media.matches);
    onChange();
    media.addEventListener?.("change", onChange);
    return () => media.removeEventListener?.("change", onChange);
  }, []);
  const virtualizer = useVirtualizer({
    count: groups.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => (isMobileList ? 272 : 78),
    overscan: 8,
  });
  const setFilter = (key: keyof Filters, value: string | boolean | number) => {
    setFilters((current) => ({ ...current, [key]: value, page: key === "page" ? Number(value) : 1 }));
    if (key === "linked") setSelectedGroup("");
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
        <SelectField
          ariaLabel="Маркетплейс"
          value={filters.marketplace}
          onChange={(next) => setFilter("marketplace", next)}
          options={[
            { value: "all", label: "Все маркетплейсы" },
            { value: "ozon", label: "Ozon" },
            { value: "yandex", label: "Yandex" },
          ]}
        />
        <SelectField
          ariaLabel="Привязки"
          value={filters.linked}
          onChange={(next) => setFilter("linked", next)}
          options={[
            { value: "all", label: "Все привязки" },
            { value: "linked", label: "С привязками" },
            { value: "unlinked", label: "Без привязок" },
            { value: "changed", label: "Цена изменилась" },
            { value: "linked_archived", label: "Привязанные в архиве" },
          ]}
        />
        <SelectField
          ariaLabel="Статус"
          value={filters.state}
          onChange={(next) => setFilter("state", next)}
          options={[
            { value: "all", label: "Все статусы" },
            { value: "archived", label: "Архив" },
            { value: "inactive", label: "Неактивные" },
            { value: "out_of_stock", label: "Нет остатка" },
          ]}
        />
        <SelectField
          className="sort-select"
          title="Сортировка"
          ariaLabel="Сортировка"
          value={filters.sort}
          onChange={(next) => setFilter("sort", next)}
          options={[
            { value: "", label: "По умолчанию" },
            { value: "recent", label: "Недавнее изменение" },
            { value: "alpha", label: "По алфавиту" },
            { value: "price_desc", label: "Цена: дороже" },
            { value: "price_asc", label: "Цена: дешевле" },
          ]}
        />
        <BrandPicker
          value={filters.brand}
          options={brandOptions}
          loading={brandsQuery.isLoading}
          onChange={(brand) => setFilter("brand", brand)}
          onRefresh={() => refreshBrands.mutate()}
          refreshing={refreshBrands.isPending}
          canRefresh={isAdmin}
        />
        <label className="toggle-filter">
          <input type="checkbox" checked={filters.autoOnly} onChange={(event) => setFilter("autoOnly", event.target.checked)} />
          Только автопрайс
        </label>
      </section>
      {pageQuery.isFetching ? (
        <div className="catalog-loading-bar" role="status" aria-label="Каталог обновляется">
          <div className="catalog-loading-bar-fill" />
          <span>Обновляю каталог…</span>
        </div>
      ) : null}
      <section className={`summary-grid${pageQuery.isFetching ? " is-loading" : ""}`}>
        <Stat label={pageQuery.data?.grouped ? "Карточек" : "Найдено"} value={catalogStats?.groupTotal ?? (pageQuery.data?.groupTotal || pageQuery.data?.total || 0)} tone="accent" icon={<PackageCheck size={18} />} />
        {(catalogStats || (pageQuery.data?.grouped && pageQuery.data?.rowTotal)) ? <Stat label="SKU" value={catalogStats?.rowTotal ?? pageQuery.data?.rowTotal ?? 0} icon={<Copy size={18} />} /> : null}
        <Stat label="Всего" value={catalogStats?.totalAll ?? (pageQuery.data?.totalAll || 0)} icon={<Search size={18} />} />
        <Stat label="Готовы" value={catalogStats?.ready ?? (pageQuery.data?.ready || 0)} tone="success" icon={<Check size={18} />} />
        <Stat label="Изменения" value={catalogStats?.changed ?? (pageQuery.data?.changed || 0)} tone="warn" icon={<RefreshCw size={18} />} />
        <Stat label="Без поставщика" value={catalogStats?.withoutSupplier ?? (pageQuery.data?.withoutSupplier || 0)} icon={<Link2 size={18} />} />
      </section>
      {useDemoCatalog ? (
        <div className="demo-catalog-strip">
          <Sparkles size={16} />
          <div>
            <strong>Демо-витрина каталога</strong>
            <span>Реальный каталог пустой, поэтому показываю {groups.length} карточек и {groups.reduce((sum, group) => sum + group.products.length, 0)} SKU для оценки таблицы и карточки товара.</span>
          </div>
        </div>
      ) : null}
      <section className={`workspace ${selectedGroup ? "detail-open" : ""}${pageQuery.isFetching ? " is-loading" : ""}`}>
        <div className="list-panel">
          {pageQuery.error && !useDemoCatalog && <div className="inline-error">{errorMessage(pageQuery.error)}</div>}
          <div className="warehouse-table-head">
            <span />
            <span>Товар</span>
            <span>Артикул / SKU</span>
            <span>Маркетплейсы</span>
            <span>Остаток</span>
            <span>Поставщик</span>
            <span>Цена</span>
            <span>Статус</span>
          </div>
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
            {pageQuery.isLoading && <CatalogSkeleton rows={8} />}
            {!pageQuery.isLoading && !groups.length && <div className="list-loading">Ничего не найдено.</div>}
          </div>
          <div className="pager">
            <button disabled={filters.page <= 1} onClick={() => setFilter("page", Math.max(1, filters.page - 1))}>Назад</button>
            <span>Страница {filters.page}</span>
            <button disabled={!pageQuery.data?.hasMore} onClick={() => setFilter("page", filters.page + 1)}>Дальше</button>
          </div>
        </div>
        <DetailPanel
          selectedGroup={selectedGroup}
          products={detailProducts}
          breakdown={detailBreakdown}
          loading={detailLoading}
          refreshing={detailRefreshing}
          onClose={() => setSelectedGroup("")}
          isAdmin={isAdmin}
          filteredOut={selectedFilteredOut && Boolean(detailProducts.length)}
          demoMode={selectedIsDemoGroup}
        />
      </section>
    </>
  );
}
