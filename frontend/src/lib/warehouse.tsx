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

export function productGroupKey(product: Product): string {
  const raw = asRecord(product.raw);
  const manualGroupId = String(raw.manualGroupId || raw.manual_group_id || "").trim().toLowerCase();
  if (manualGroupId) return `manual:${manualGroupId}`;
  return `offer:${String(product.offerId || product.sku || product.id).trim().toLowerCase()}`;
}

export function uniqueLinks(products: Product[]): ProductLink[] {
  const byKey = new Map<string, ProductLink>();
  for (const product of products) {
    for (const link of product.links || []) {
      const key = [
        link.id || "",
        link.article || link.supplierArticle || "",
        link.supplierName || "",
        link.partnerId || "",
        link.keyword || "",
      ].join("|").toLowerCase();
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

export function preferredGroupPrimary(products: Product[]): Product {
  return [...products].sort((a, b) => {
    const aScore = (firstImage(a) ? 8 : 0) + (a.marketplace === "yandex" ? 4 : 0) + ((a.links || []).length ? 2 : 0) + (a.archived ? -2 : 0);
    const bScore = (firstImage(b) ? 8 : 0) + (b.marketplace === "yandex" ? 4 : 0) + ((b.links || []).length ? 2 : 0) + (b.archived ? -2 : 0);
    return bScore - aScore || String(a.name || "").localeCompare(String(b.name || ""), "ru");
  })[0] || products[0];
}

export function groupProductsForList(products: Product[]): ProductGroup[] {
  const groups = new Map<string, Product[]>();
  for (const product of products) {
    const key = productGroupKey(product);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)?.push(product);
  }
  return Array.from(groups.entries()).map(([groupKey, groupProducts]) => {
    const productsSorted = [...groupProducts].sort((a, b) => String(a.marketplace || "").localeCompare(String(b.marketplace || "")));
    const links = uniqueLinks(productsSorted);
    const marketplaces = Array.from(new Set(productsSorted.map((product) => marketplaceLabel(product.marketplace)).filter(Boolean))).sort();
    const statusSummary = productsSorted.reduce<ProductGroup["statusSummary"]>((summary, product) => {
      const stateCode = String(product.marketplaceState?.code || product.status || "").toLowerCase();
      const linked = (product.links || []).length > 0;
      const archived = Boolean(product.archived || stateCode.includes("archiv"));
      const changed = Number(product.newPrice || product.targetPrice || 0) > 0 && Number(product.currentPrice || 0) !== Number(product.newPrice || product.targetPrice || 0);
      summary.total += 1;
      if (linked) summary.linked += 1;
      if (archived) summary.archived += 1;
      if (linked && !archived) summary.ready += 1;
      if (changed) summary.changed += 1;
      if (linked && !product.selectedSupplier) summary.withoutSupplier += 1;
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
  const stateCode = String(product.marketplaceState?.code || product.status || "").toLowerCase();
  const linked = (product.links || []).length > 0;
  if (product.archived || stateCode.includes("archiv")) return { label: "Архив", tone: "danger", icon: <Archive size={14} /> };
  if (!linked) return { label: "Нет привязки", tone: "warn", icon: <AlertCircle size={14} /> };
  if (Number(product.newPrice || product.targetPrice || 0) > 0 && Number(product.currentPrice || 0) !== Number(product.newPrice || product.targetPrice || 0)) {
    return { label: "Цена изменилась", tone: "info", icon: <RefreshCw size={14} /> };
  }
  return { label: "Готов к продаже", tone: "success", icon: <PackageCheck size={14} /> };
}

export function groupStatusLabel(group: ProductGroup): { label: string; tone: string; icon: ReactNode } {
  const { statusSummary } = group;
  if (statusSummary.archived > 0) return { label: `Архив ${statusSummary.archived}/${statusSummary.total}`, tone: "danger", icon: <Archive size={14} /> };
  if (!statusSummary.linked) return { label: "Нет привязки", tone: "warn", icon: <AlertCircle size={14} /> };
  if (statusSummary.changed > 0) return { label: `Цена изм. ${statusSummary.changed}`, tone: "info", icon: <RefreshCw size={14} /> };
  if (statusSummary.withoutSupplier > 0) return { label: `Нет пост. ${statusSummary.withoutSupplier}`, tone: "warn", icon: <AlertCircle size={14} /> };
  return { label: `Готовы ${statusSummary.ready}/${statusSummary.total}`, tone: "success", icon: <PackageCheck size={14} /> };
}

export function groupPrice(group: ProductGroup): number {
  const prices = group.products
    .map((product) => Number(product.newPrice || product.targetPrice || product.currentPrice || 0))
    .filter((price) => Number.isFinite(price) && price > 0);
  return prices.length ? Math.min(...prices) : 0;
}
