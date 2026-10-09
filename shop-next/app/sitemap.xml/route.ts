// /sitemap.xml — sitemap index: pages, collections, brands, blog and products split by 10 000.
import { SITE_URL } from "@/lib/seo";
import { PRODUCTS_PER_FILE, sitemapProducts, xmlResponse } from "@/lib/sitemap";

export const revalidate = 3600;

export async function GET() {
  let productFiles = 1;
  try { productFiles = Math.max(1, Math.ceil((await sitemapProducts()).length / PRODUCTS_PER_FILE)); } catch { /* keep 1 */ }
  const now = new Date().toISOString().slice(0, 10);
  const files = ["pages", "collections", "brands", "listings", "blog", ...Array.from({ length: productFiles }, (_, i) => `products-${i + 1}`)];
  const body = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${files.map((f) => `<sitemap><loc>${SITE_URL}/sitemaps/${f}.xml</loc><lastmod>${now}</lastmod></sitemap>`).join("\n")}
</sitemapindex>
`;
  return xmlResponse(body);
}
