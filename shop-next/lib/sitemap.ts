// Sitemap building blocks shared by app/sitemap.xml (index) and app/sitemaps/[file] (parts).
import { SITE_URL } from "./seo";

const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";

export const PRODUCTS_PER_FILE = 10000;

// Landing collections the backend understands (see SHOP_QUERY_COLLECTIONS) + storefront categories.
export const COLLECTION_QUERIES = [
  "женская", "мужская", "унисекс", "нишевая", "элитная", "арабская", "миниатюр",
  "цветочный", "древесный", "цитрусовый", "мускусный", "восточный", "свежий", "фужерный", "шипровый", "сладкий",
];
export const CATEGORY_SLUGS = ["edp", "edt", "parfum", "edc", "testers", "sets", "deo", "home", "body"];

export const STATIC_PAGES: [string, string, number][] = [
  ["/", "daily", 1.0], ["/catalog", "daily", 0.9], ["/new", "daily", 0.85], ["/brands", "weekly", 0.75],
  ["/gift", "weekly", 0.7], ["/find", "monthly", 0.6], ["/blog", "weekly", 0.7], ["/news", "daily", 0.6],
  ["/guide/women", "monthly", 0.6], ["/guide/men", "monthly", 0.6], ["/guide/gift", "monthly", 0.6], ["/guide/office", "monthly", 0.55],
  ["/delivery", "monthly", 0.5], ["/terms", "monthly", 0.4], ["/privacy", "yearly", 0.2], ["/consent", "yearly", 0.1], ["/consent-ads", "yearly", 0.1], ["/cookies", "yearly", 0.1], ["/faq", "monthly", 0.45], ["/warranty", "monthly", 0.4], ["/world", "monthly", 0.35],
];

export interface SitemapProduct { offerId: string; slug: string; name: string; image?: string | null; lastmod?: string | null }

export async function sitemapProducts(): Promise<SitemapProduct[]> {
  // ~3 MB: above the Next data-cache limit (2 MB); the API caches it for an hour anyway
  const r = await fetch(`${API}/sitemap-products`, { cache: "no-store" });
  if (!r.ok) throw new Error(`sitemap-products ${r.status}`);
  return (await r.json()).products ?? [];
}

export async function sitemapBrands(): Promise<string[]> {
  const r = await fetch(`${API}/brands`, { next: { revalidate: 3600 } });
  if (!r.ok) return [];
  const d = await r.json();
  const list: { name: string; count: number }[] = Array.isArray(d) ? d : d.brands ?? [];
  // real brands only: the brand field often holds "brand + model" with 1–2 items
  return list.filter((b) => b.count >= 3 && b.name.length <= 40).map((b) => b.name);
}

export async function sitemapBlog(): Promise<{ slug: string; publishedAt?: string }[]> {
  const r = await fetch(`${API}/blog?pageSize=500`, { next: { revalidate: 3600 } });
  if (!r.ok) return [];
  return (await r.json()).posts ?? [];
}

export const xmlEsc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");

export function urlEntry(path: string, opts: { lastmod?: string | null; changefreq?: string; priority?: number; image?: string | null; imageTitle?: string } = {}) {
  const loc = path.startsWith("http") ? path : `${SITE_URL}${path}`;
  return [
    "<url>",
    `<loc>${xmlEsc(loc)}</loc>`,
    opts.lastmod ? `<lastmod>${opts.lastmod}</lastmod>` : "",
    opts.changefreq ? `<changefreq>${opts.changefreq}</changefreq>` : "",
    opts.priority != null ? `<priority>${opts.priority.toFixed(2)}</priority>` : "",
    opts.image ? `<image:image><image:loc>${xmlEsc(opts.image)}</image:loc>${opts.imageTitle ? `<image:title>${xmlEsc(opts.imageTitle)}</image:title>` : ""}</image:image>` : "",
    "</url>",
  ].join("");
}

export function urlset(entries: string[]) {
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">\n${entries.join("\n")}\n</urlset>\n`;
}

export function xmlResponse(body: string) {
  return new Response(body, { headers: { "Content-Type": "application/xml; charset=utf-8", "Cache-Control": "public, max-age=3600, s-maxage=3600" } });
}
