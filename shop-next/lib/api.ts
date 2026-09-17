import type { ShopProduct, ShopBanner, ShopCategory, ShopSettings, CatalogResponse, BlogPost, TelegramNewsPost, ShopReview, MarketplaceReview, ProductQAItem, FragranceNotes } from "./types";

// Server-side API base — not exposed to browser
const API_HOST = process.env.API_BASE ?? "https://davidsklad.ru";
const BASE = API_HOST + "/api/shop";

async function get<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(BASE + path, {
    ...opts,
    headers: { "Content-Type": "application/json", ...(opts?.headers ?? {}) },
    next: { revalidate: 60 }, // ISR: revalidate every 60 seconds by default
  });
  if (!res.ok) throw new Error(`API ${path} → ${res.status}`);
  return res.json() as Promise<T>;
}

export interface CatalogParams {
  page?: number;
  pageSize?: number;
  brand?: string;
  category?: string;
  q?: string;
  inStock?: boolean;
  sort?: "price_asc" | "price_desc" | "name";
}

export async function fetchCatalog(params: CatalogParams = {}): Promise<CatalogResponse> {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.pageSize) qs.set("pageSize", String(params.pageSize));
  if (params.brand) qs.set("brand", params.brand);
  if (params.category) qs.set("category", params.category);
  if (params.q) qs.set("q", params.q);
  if (params.inStock) qs.set("inStock", "true");
  if (params.sort) qs.set("sort", params.sort);
  return get<CatalogResponse>(`/catalog?${qs}`, { next: { revalidate: 120 } });
}

export async function fetchProduct(offerId: string): Promise<ShopProduct> {
  return get<ShopProduct>(`/product/${encodeURIComponent(offerId)}`, { next: { revalidate: 300 } });
}

export async function fetchBanners(): Promise<ShopBanner[]> {
  return get<ShopBanner[]>("/banners", { next: { revalidate: 300 } });
}

export async function fetchCategories(): Promise<ShopCategory[]> {
  return get<ShopCategory[]>("/categories", { next: { revalidate: 3600 } });
}

export async function fetchSettings(): Promise<ShopSettings> {
  return get<ShopSettings>("/settings", { next: { revalidate: 3600 } });
}

export async function fetchBrands(): Promise<{ name: string; count: number }[]> {
  return get<{ name: string; count: number }[]>("/brands", { next: { revalidate: 3600 } });
}

export async function fetchPopular(limit = 8): Promise<{ products: ShopProduct[] }> {
  return get<{ ok: boolean; products: ShopProduct[] }>(`/popular?limit=${limit}`, { next: { revalidate: 300 } });
}

export async function fetchNewProducts(days = 14): Promise<CatalogResponse> {
  return get<CatalogResponse>(`/new?days=${days}`, { next: { revalidate: 300 } });
}

export async function fetchReviews(limit = 8, offerId?: string): Promise<{ reviews: ShopReview[] }> {
  const qs = new URLSearchParams({ limit: String(limit) });
  if (offerId) qs.set("offerId", offerId);
  return get<{ ok: boolean; reviews: ShopReview[] }>(`/reviews?${qs}`, { next: { revalidate: 300 } });
}

export async function fetchNews(limit = 12): Promise<{ posts: TelegramNewsPost[] }> {
  const res = await get<{ ok: boolean; posts: TelegramNewsPost[] }>(`/news?limit=${limit}`, { next: { revalidate: 300 } });
  // Resolve relative photoUrls to absolute (photos are stored on the API server)
  res.posts = res.posts.map(p => ({
    ...p,
    photoUrl: p.photoUrl?.startsWith("/") ? `${API_HOST}${p.photoUrl}` : p.photoUrl,
  }));
  return res;
}

export async function fetchBlog(params?: { page?: number; pageSize?: number; tag?: string }): Promise<{ posts: BlogPost[]; total: number }> {
  const qs = new URLSearchParams();
  if (params?.page) qs.set("page", String(params.page));
  if (params?.pageSize) qs.set("pageSize", String(params.pageSize));
  if (params?.tag) qs.set("tag", params.tag);
  return get<{ ok: boolean; posts: BlogPost[]; total: number }>(`/blog?${qs}`, { next: { revalidate: 600 } });
}

export async function fetchBlogPost(slug: string): Promise<{ post: BlogPost }> {
  return get<{ ok: boolean; post: BlogPost }>(`/blog/${encodeURIComponent(slug)}`, { next: { revalidate: 600 } });
}

export async function fetchMarketplaceReviews(offerId: string): Promise<{ reviews: MarketplaceReview[]; avgRating: number; reviewCount: number }> {
  return get(`/marketplace-reviews?offerId=${encodeURIComponent(offerId)}`, { next: { revalidate: 3600 } });
}

export async function fetchProductQA(offerId: string): Promise<{ items: ProductQAItem[] }> {
  return get(`/product-qa?offerId=${encodeURIComponent(offerId)}`, { next: { revalidate: 3600 } });
}

export async function fetchFragranceNotes(brand: string, name: string, offerId?: string): Promise<{ data: FragranceNotes | null }> {
  const p = new URLSearchParams({ brand, name });
  if (offerId) p.set("offerId", offerId);
  return get(`/product-notes?${p}`, { next: { revalidate: 86400 } });
}

export async function fetchAromaMesyatsa(): Promise<{ product: ShopProduct | null; note: string; validUntil: string | null }> {
  return get("/aroma-mesyatsa", { next: { revalidate: 3600 } });
}

// Sitemap helper — no cache, used only during build
export async function fetchSitemapProducts(): Promise<{ products: { offerId: string; lastmod?: string }[] }> {
  return get("/sitemap-products", { cache: "no-store" });
}

export async function fetchAutoCategories(): Promise<import("./types").AutoCategory[]> {
  return get<import("./types").AutoCategory[]>("/auto-categories", { next: { revalidate: 3600 } });
}
