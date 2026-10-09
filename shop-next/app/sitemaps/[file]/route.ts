// /sitemaps/{pages|collections|brands|blog|products-N}.xml — parts listed by /sitemap.xml
import {
  CATEGORY_SLUGS, COLLECTION_QUERIES, PRODUCTS_PER_FILE, STATIC_PAGES,
  sitemapBlog, sitemapBrands, sitemapListings, sitemapProducts, urlEntry, urlset, xmlResponse,
} from "@/lib/sitemap";
import { COLLECTION_DEFS, brandHref, collectionHref } from "@/lib/landings";
import { getFeatures } from "@/lib/features";

export const revalidate = 3600;

export async function GET(_req: Request, { params }: { params: Promise<{ file: string }> }) {
  const { file } = await params;
  const name = file.replace(/\.xml$/, "");
  const today = new Date().toISOString().slice(0, 10);

  if (name === "pages") {
    const features = await getFeatures();
    return xmlResponse(urlset(STATIC_PAGES.filter(([p]) => p !== "/gift" || features.giftBuilder).map(([p, cf, pr]) => urlEntry(p, { lastmod: today, changefreq: cf, priority: pr }))));
  }
  if (name === "collections") {
    return xmlResponse(urlset([
      ...COLLECTION_DEFS.map((c) => urlEntry(collectionHref(c), { lastmod: today, changefreq: "daily", priority: 0.85 })),
      // categories without their own landing stay on the catalog
      ...CATEGORY_SLUGS.filter((c) => !COLLECTION_DEFS.some((d) => d.category === c))
        .map((c) => urlEntry(`/catalog?category=${c}`, { lastmod: today, changefreq: "daily", priority: 0.7 })),
      ...COLLECTION_QUERIES.filter((q) => !COLLECTION_DEFS.some((d) => d.q === q))
        .map((q) => urlEntry(`/catalog?q=${encodeURIComponent(q)}`, { lastmod: today, changefreq: "daily", priority: 0.7 })),
    ]));
  }
  if (name === "brands") {
    const brands = await sitemapBrands();
    const hrefs = [...new Set(brands.map(brandHref))].filter((h) => h !== "/brand/");
    return xmlResponse(urlset(hrefs.map((h) => urlEntry(h, { lastmod: today, changefreq: "weekly", priority: 0.75 }))));
  }
  if (name === "listings") {
    const list = await sitemapListings();
    return xmlResponse(urlset(list.map((l) => urlEntry(`/brand/${l.brand}/${l.sub}`, { lastmod: today, changefreq: "weekly", priority: 0.7 }))));
  }
  if (name === "blog") {
    const posts = await sitemapBlog();
    return xmlResponse(urlset(posts.map((p) => urlEntry(`/blog/${encodeURIComponent(p.slug)}`, { lastmod: p.publishedAt?.slice(0, 10), changefreq: "monthly", priority: 0.6 }))));
  }
  const m = /^products-(\d+)$/.exec(name);
  if (m) {
    const n = Number(m[1]);
    const all = await sitemapProducts();
    const part = all.slice((n - 1) * PRODUCTS_PER_FILE, n * PRODUCTS_PER_FILE);
    if (!part.length) return new Response("Not found", { status: 404 });
    return xmlResponse(urlset(part.map((p) => urlEntry(`/product/${p.slug}`, {
      lastmod: p.lastmod, changefreq: "weekly", priority: 0.8, image: p.image, imageTitle: p.name,
    }))));
  }
  return new Response("Not found", { status: 404 });
}
