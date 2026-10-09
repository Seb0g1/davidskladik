// Brand landings helpers: resolve /brand/<slug>, and product counts of its sub-listings
// (/brand/<brand>/<sub>) — which ones are worth linking and indexing.
import { fetchBrands, fetchCatalog } from "./api";
import { BRAND_SUBS, BRAND_SUB_MIN, slugify, type BrandSub } from "./landings";

/** several spellings can share a slug — take the one with the most products */
export async function resolveBrand(slug: string) {
  const brands = await fetchBrands().catch(() => [] as { name: string; count: number }[]);
  const hits = brands.filter((b) => slugify(b.name) === slug).sort((a, b) => b.count - a.count);
  return hits[0] || null;
}

export async function brandSubCounts(brand: string): Promise<{ sub: BrandSub; count: number }[]> {
  const res = await Promise.all(BRAND_SUBS.map((sub) =>
    fetchCatalog({ brand, gender: sub.gender, category: sub.category, pageSize: 1 })
      .then((d) => ({ sub, count: d.total }))
      .catch(() => ({ sub, count: 0 }))));
  return res.filter((r) => r.count >= BRAND_SUB_MIN);
}
