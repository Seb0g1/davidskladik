import type { ReactNode } from "react";
import { AlertCircle, Archive, PackageCheck, RefreshCw } from "lucide-react";
import type { Product, ProductLink } from "../types";
import { asRecord, money } from "./common";

export type ProductGroup = {
  groupKey: string;
  primary: Product;
  products: Product[];
  links: ProductLink[];
  marketplaces: string[];
  statusSummary: {
    total: number;
    linked: number;
    archived: number;
    ready: number;
    changed: number;
    withoutSupplier: number;
  };
};

function extractYandexSourceProductId(product: Product): string {
  const raw = asRecord(product.raw);
  const yandex = asRecord(product.yandex);
  const rawYandex = asRecord(raw.yandex);
  const yandexExtra = asRecord(yandex.extra);
  const rawYandexExtra = asRecord(rawYandex.extra);
  return String(
    yandexExtra.sourceProductId
    || rawYandexExtra.sourceProductId
    || "",
  ).trim();
}

function resolveProductPairOzonId(product: Product): string {
  const raw = asRecord(product.raw);
  const manualGroupId = String(product.manualGroupId || raw.manualGroupId || raw.manual_group_id || "").trim().toLowerCase();
  if (manualGroupId.startsWith("auto-pair-")) {
    return manualGroupId.slice("auto-pair-".length);
  }
  if (String(product.marketplace || "").trim().toLowerCase() === "yandex") {
    return extractYandexSourceProductId(product);
  }
  return "";
}

export function buildWarehouseCatalogGroupContext(products: Product[]) {
  const ozonIdsReferencedByYandex = new Set<string>();
  const offerIdMarketplaces = new Map<string, Set<string>>();
  for (const product of products) {
    const marketplace = String(product.marketplace || "").trim().toLowerCase();
    const offerId = String(product.offerId || product.sku || "").trim().toLowerCase();
    if (offerId) {
      if (!offerIdMarketplaces.has(offerId)) offerIdMarketplaces.set(offerId, new Set());
      if (marketplace) offerIdMarketplaces.get(offerId)?.add(marketplace);
    }
    if (marketplace !== "yandex") continue;
    const sourceId = extractYandexSourceProductId(product);
    if (sourceId) ozonIdsReferencedByYandex.add(sourceId.toLowerCase());
  }
  const pairedOfferIds = new Set<string>();
  for (const [offerId, marketplaces] of offerIdMarketplaces) {
    if (marketplaces.has("ozon") && marketplaces.has("yandex")) pairedOfferIds.add(offerId);
  }
  return { ozonIdsReferencedByYandex, pairedOfferIds };
}

export function productGroupKey(product: Product, groupContext?: ReturnType<typeof buildWarehouseCatalogGroupContext>): string {
  const raw = asRecord(product.raw);
  const manualGroupId = String(product.manualGroupId || raw.manualGroupId || raw.manual_group_id || "").trim().toLowerCase();
  if (manualGroupId && !manualGroupId.startsWith("auto-pair-")) return `manual:${manualGroupId}`;

  const offerId = String(product.offerId || product.sku || product.id).trim().toLowerCase();
  if (offerId && groupContext?.pairedOfferIds?.has(offerId)) return `offer:${offerId}`;

  const marketplace = String(product.marketplace || "").trim().toLowerCase();
  const ozonId = String(product.id || "").trim().toLowerCase();
  if (marketplace === "ozon" && ozonId && groupContext?.ozonIdsReferencedByYandex.has(ozonId)) {
    return `pair:${ozonId}`;
  }

  const pairOzonId = resolveProductPairOzonId(product);
  if (pairOzonId) return `pair:${pairOzonId.toLowerCase()}`;

  if (offerId) return `offer:${offerId}`;
  return "";
}

export function uniqueLinks(products: Product[]): ProductLink[] {
  const byKey = new Map<string, ProductLink>();
  for (const product of products) {
    for (const link of product.links || []) {
      const raw = link as Record<string, unknown>;
      const article = String(link.article || link.supplierArticle || "").trim().toLowerCase();
      const rowId = String(raw.sourceRowId || raw.rowId || "").trim().toLowerCase();
      const exactName = String(raw.exactName || raw.name || "").trim().toLowerCase();
      const matchType = article ? "article" : (rowId ? "selected_row" : String(link.matchType || "exact_name").toLowerCase());
      const primary = article ? `article:${article}` : (rowId ? `row:${rowId}` : `name:${exactName}`);
      const supplier = [
        String(link.supplierName || "").trim().toLowerCase(),
        String(link.partnerId || "").trim() ? `partner:${String(link.partnerId).trim().toLowerCase()}` : "",
      ].filter(Boolean).sort().join("&") || "manual";
      const keyword = String(link.keyword || "").trim().toLowerCase();
      const currency = String(link.priceCurrency || "USD").trim().toUpperCase() === "RUB" ? "RUB" : "USD";
      const key = [matchType, primary, supplier, keyword, currency].join("|");
      if (!byKey.has(key)) byKey.set(key, link);
    }
  }
  return Array.from(byKey.values());
}

export function marketplaceLabel(value?: string | null): string {
  const key = String(value || "").toLowerCase();
  if (key.includes("ozon")) return "Ozon";
  if (key.includes("yandex")) return "Yandex";
  return value || "Marketplace";
}

function isGenericMarketplaceTargetName(name: string, marketplace = ""): boolean {
  const value = String(name || "").trim().toLowerCase();
  if (!value) return true;
  if (value === String(marketplace || "").trim().toLowerCase()) return true;
  return value === "ozon" || value === "yandex" || value === "yandex market" || value === "marketplace";
}

export function resolveMarketplaceTargetLabel(product: Product): string {
  const raw = asRecord(product.raw);
  const exports = asRecord(product.exports);
  const marketplace = String(product.marketplace || "").trim();
  const target = String(product.target || "").trim();
  const exportName = String(
    asRecord(exports[target])?.targetName
    || asRecord(exports[marketplace])?.targetName
    || "",
  ).trim();
  const targetName = String(product.targetName || raw.targetName || "").trim();
  for (const candidate of [exportName, targetName, target]) {
    if (candidate && !isGenericMarketplaceTargetName(candidate, marketplace)) return candidate;
  }
  return "";
}

export function marketplaceRowLabel(product: Product): string {
  const base = marketplaceLabel(product.marketplace);
  const account = resolveMarketplaceTargetLabel(product);
  const offer = String(product.offerId || product.sku || "").trim();
  if (account) return `${base} · ${account}`;
  if (offer) return `${base} · ${offer}`;
  return base;
}

export function marketplaceRowLabelsForProducts(products: Product[]): Array<{ key: string; label: string; product: Product }> {
  const seen = new Map<string, number>();
  return products.map((product) => {
    const base = marketplaceRowLabel(product);
    const next = (seen.get(base) || 0) + 1;
    seen.set(base, next);
    return {
      key: String(product.id),
      label: next > 1 ? `${base} #${next}` : base,
      product,
    };
  });
}

export function groupMarketplaceLabels(products: Product[]): string[] {
  const counts = new Map<string, number>();
  for (const product of products) {
    const label = marketplaceRowLabel(product);
    counts.set(label, (counts.get(label) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([label, count]) => (count > 1 ? `${label} ×${count}` : label))
    .sort((a, b) => a.localeCompare(b, "ru"));
}

export function preferredGroupPrimary(products: Product[]): Product {
  return [...products].sort((a, b) => {
    const aScore = (firstImage(a) ? 8 : 0) + (a.marketplace === "yandex" ? 4 : 0) + ((a.links || []).length ? 2 : 0) + (a.archived ? -2 : 0);
    const bScore = (firstImage(b) ? 8 : 0) + (b.marketplace === "yandex" ? 4 : 0) + ((b.links || []).length ? 2 : 0) + (b.archived ? -2 : 0);
    return bScore - aScore || String(a.name || "").localeCompare(String(b.name || ""), "ru");
  })[0] || products[0];
}

export function groupProductsForList(products: Product[]): ProductGroup[] {
  const groupContext = buildWarehouseCatalogGroupContext(products);
  const groups = new Map<string, Product[]>();
  for (const product of products) {
    const key = productGroupKey(product, groupContext);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)?.push(product);
  }
  return Array.from(groups.entries()).map(([groupKey, groupProducts]) => {
    const productsSorted = [...groupProducts].sort((a, b) => String(a.marketplace || "").localeCompare(String(b.marketplace || "")));
    const links = uniqueLinks(productsSorted);
    const marketplaces = groupMarketplaceLabels(productsSorted);
    const statusSummary = productsSorted.reduce<ProductGroup["statusSummary"]>((summary, product) => {
      const stateCode = String(product.marketplaceState?.code || product.status || "").toLowerCase();
      const linked = (product.links || []).length > 0;
      const archived = Boolean(product.archived || stateCode.includes("archiv"));
      const changed = Number(product.newPrice || product.targetPrice || product.nextPrice || 0) > 0
        && Number(product.currentPrice || product.marketplacePrice || 0) !== Number(product.newPrice || product.targetPrice || product.nextPrice || 0);
      const hasSupplier = Boolean(product.selectedSupplier && !asRecord(product.selectedSupplier).displayOnly) || Boolean(product.stockOnlyFallbackActive);
      const sellable = Boolean(product.ready) || (hasSupplier && Number(product.targetStock || product.stock || 0) > 0);
      summary.total += 1;
      if (linked) summary.linked += 1;
      if (archived) summary.archived += 1;
      if (linked && !archived && sellable) summary.ready += 1;
      if (changed) summary.changed += 1;
      if (linked && !hasSupplier) summary.withoutSupplier += 1;
      return summary;
    }, { total: 0, linked: 0, archived: 0, ready: 0, changed: 0, withoutSupplier: 0 });
    return {
      groupKey,
      primary: preferredGroupPrimary(productsSorted),
      products: productsSorted,
      links,
      marketplaces,
      statusSummary,
    };
  });
}

export function firstImage(product?: Product): string {
  if (!product) return "";
  if (product.imageUrl) return product.imageUrl;
  if (Array.isArray(product.images) && product.images[0]) return product.images[0];
  const raw = asRecord(product.raw);
  const rawImage = raw.primaryImage || raw.image || raw.imageUrl;
  return typeof rawImage === "string" ? rawImage : "";
}

export function statusLabel(product: Product): { label: string; tone: string; icon: ReactNode } {
  const lastArchiveSend = asRecord(asRecord(product).lastArchiveSend);
  if (lastArchiveSend.queuedByDailyLimit || lastArchiveSend.warning === "ozon_unarchive_daily_limit_queued") {
    return { label: "РћР¶РёРґР°РµС‚ СЂР°Р·Р°СЂС…РёРІР° Ozon", tone: "warn", icon: <AlertCircle size={14} /> };
  }
  const stateCode = String(product.marketplaceState?.code || product.status || "").toLowerCase();
  const linked = (product.links || []).length > 0;
  if (product.archived || stateCode.includes("archiv")) return { label: "Архив", tone: "danger", icon: <Archive size={14} /> };
  if (!linked) return { label: "Нет привязки", tone: "warn", icon: <AlertCircle size={14} /> };
  const hasSupplier = Boolean(product.selectedSupplier && !asRecord(product.selectedSupplier).displayOnly) || Boolean(product.stockOnlyFallbackActive);
  if (!hasSupplier) return { label: "Нет поставщика", tone: "warn", icon: <AlertCircle size={14} /> };
  if (Number(product.newPrice || product.targetPrice || product.nextPrice || 0) > 0
    && Number(product.currentPrice || product.marketplacePrice || 0) !== Number(product.newPrice || product.targetPrice || product.nextPrice || 0)) {
    return { label: "Цена изменилась", tone: "info", icon: <RefreshCw size={14} /> };
  }
  return { label: "Готов к продаже", tone: "success", icon: <PackageCheck size={14} /> };
}

export function groupStatusLabel(group: ProductGroup): { label: string; tone: string; icon: ReactNode } {
  const { statusSummary } = group;
  const anyHasSupplier = group.products.some((product) => Boolean(product.selectedSupplier && !asRecord(product.selectedSupplier).displayOnly) || Boolean(product.stockOnlyFallbackActive));
  if (statusSummary.archived > 0) return { label: `Архив ${statusSummary.archived}/${statusSummary.total}`, tone: "danger", icon: <Archive size={14} /> };
  if (!statusSummary.linked) return { label: "Нет привязки", tone: "warn", icon: <AlertCircle size={14} /> };
  if (statusSummary.changed > 0) return { label: `Цена изм. ${statusSummary.changed}`, tone: "info", icon: <RefreshCw size={14} /> };
  if (!anyHasSupplier && statusSummary.withoutSupplier > 0) return { label: `Нет пост. ${statusSummary.withoutSupplier}`, tone: "warn", icon: <AlertCircle size={14} /> };
  return { label: `Готовы ${statusSummary.ready}/${statusSummary.total}`, tone: "success", icon: <PackageCheck size={14} /> };
}

export function groupPrice(group: ProductGroup): number {
  const prices = group.products
    .map((product) => Number(product.newPrice || product.nextPrice || product.targetPrice || product.currentPrice || 0))
    .filter((price) => Number.isFinite(price) && price > 0);
  return prices.length ? Math.min(...prices) : 0;
}
