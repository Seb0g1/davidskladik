import type { MetadataRoute } from "next";
import { fetchSitemapProducts, fetchBrands, fetchBlog } from "@/lib/api";

const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL ?? "https://magicvibes.ru";

export const revalidate = 86400; // 24h

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const now = new Date().toISOString();

  const staticPages: MetadataRoute.Sitemap = [
    { url: SITE_URL,                         lastModified: now, changeFrequency: "daily",   priority: 1.0 },
    { url: `${SITE_URL}/catalog`,            lastModified: now, changeFrequency: "hourly",  priority: 0.9 },
    { url: `${SITE_URL}/new`,                lastModified: now, changeFrequency: "daily",   priority: 0.85 },
    { url: `${SITE_URL}/brands`,             lastModified: now, changeFrequency: "daily",   priority: 0.75 },
    { url: `${SITE_URL}/gift`,               lastModified: now, changeFrequency: "weekly",  priority: 0.75 },
    { url: `${SITE_URL}/blog`,               lastModified: now, changeFrequency: "weekly",  priority: 0.7 },
    { url: `${SITE_URL}/news`,               lastModified: now, changeFrequency: "daily",   priority: 0.65 },
    { url: `${SITE_URL}/find`,               lastModified: now, changeFrequency: "monthly", priority: 0.6 },
    { url: `${SITE_URL}/guide/women`,        lastModified: now, changeFrequency: "monthly", priority: 0.6 },
    { url: `${SITE_URL}/guide/men`,          lastModified: now, changeFrequency: "monthly", priority: 0.6 },
    { url: `${SITE_URL}/guide/gift`,         lastModified: now, changeFrequency: "monthly", priority: 0.6 },
    { url: `${SITE_URL}/guide/office`,       lastModified: now, changeFrequency: "monthly", priority: 0.55 },
    { url: `${SITE_URL}/delivery`,           lastModified: now, changeFrequency: "monthly", priority: 0.5 },
    { url: `${SITE_URL}/faq`,                lastModified: now, changeFrequency: "monthly", priority: 0.45 },
    { url: `${SITE_URL}/warranty`,           lastModified: now, changeFrequency: "monthly", priority: 0.4 },
    { url: `${SITE_URL}/world`,              lastModified: now, changeFrequency: "monthly", priority: 0.35 },
  ];

  const [productsRes, brandsRes, blogRes] = await Promise.allSettled([
    fetchSitemapProducts(),
    fetchBrands(),
    fetchBlog({ pageSize: 200 }),
  ]);

  const productPages: MetadataRoute.Sitemap = productsRes.status === "fulfilled"
    ? productsRes.value.products.map(p => ({
        url: `${SITE_URL}/product/${encodeURIComponent(p.offerId)}`,
        lastModified: p.lastmod ?? now,
        changeFrequency: "weekly" as const,
        priority: 0.85,
      }))
    : [];

  const brandPages: MetadataRoute.Sitemap = brandsRes.status === "fulfilled"
    ? brandsRes.value.slice(0, 500).map(b => ({
        url: `${SITE_URL}/catalog?brand=${encodeURIComponent(b.name)}`,
        lastModified: now,
        changeFrequency: "weekly" as const,
        priority: 0.75,
      }))
    : [];

  const blogPages: MetadataRoute.Sitemap = blogRes.status === "fulfilled"
    ? blogRes.value.posts.map(p => ({
        url: `${SITE_URL}/blog/${p.slug}`,
        lastModified: p.publishedAt ?? now,
        changeFrequency: "monthly" as const,
        priority: 0.6,
      }))
    : [];

  return [...staticPages, ...productPages, ...brandPages, ...blogPages];
}
