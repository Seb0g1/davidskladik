import type { ShopProduct, ShopBanner, ShopCategory, ShopSettings, ShopOrderPayload, ShopOrder, CatalogResponse, AutoCategory, TelegramNewsPost, ShopReview, MarketplaceReview, ProductQAItem, FragranceNotes } from "./types";

// В dev Vite-прокси перенаправляет /api/shop → davidsklad.ru.
// В production установите VITE_API_BASE=https://davidsklad.ru
const BASE = (import.meta.env.VITE_API_BASE ?? "") + "/api/shop";

async function req<T>(path: string, init?: RequestInit, token?: string): Promise<T> {
  const authHeader: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {};
  const res = await fetch(BASE + path, {
    headers: { "Content-Type": "application/json", ...authHeader, ...(init?.headers || {}) },
    ...init,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || res.statusText);
  }
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

export const api = {
  catalog(params: CatalogParams = {}): Promise<CatalogResponse> {
    const qs = new URLSearchParams();
    if (params.page) qs.set("page", String(params.page));
    if (params.pageSize) qs.set("pageSize", String(params.pageSize));
    if (params.brand) qs.set("brand", params.brand);
    if (params.category) qs.set("category", params.category);
    if (params.q) qs.set("q", params.q);
    if (params.inStock) qs.set("inStock", "true");
    if (params.sort) qs.set("sort", params.sort);
    return req<CatalogResponse>(`/catalog?${qs}`);
  },

  product(offerId: string): Promise<ShopProduct> {
    return req<ShopProduct>(`/product/${encodeURIComponent(offerId)}`);
  },

  banners(): Promise<ShopBanner[]> {
    return req<ShopBanner[]>("/banners");
  },

  categories(): Promise<ShopCategory[]> {
    return req<ShopCategory[]>("/categories");
  },

  settings(): Promise<ShopSettings> {
    return req<ShopSettings>("/settings");
  },

  autoCategories(): Promise<AutoCategory[]> {
    return req<AutoCategory[]>("/auto-categories");
  },

  brands(): Promise<{ name: string; count: number }[]> {
    return req<{ name: string; count: number }[]>("/brands");
  },

  createOrder(payload: ShopOrderPayload, token?: string): Promise<ShopOrder> {
    return req<ShopOrder>("/orders", { method: "POST", body: JSON.stringify(payload) }, token);
  },

  getOrders(token: string): Promise<{ ok: boolean; orders: ShopOrder[] }> {
    return req<{ ok: boolean; orders: ShopOrder[] }>("/auth/orders", {}, token);
  },

  updateProfile(data: { firstName?: string; lastName?: string; phone?: string }, token: string): Promise<{ ok: boolean; customer: import("./AuthContext").ShopCustomer }> {
    return req<{ ok: boolean; customer: import("./AuthContext").ShopCustomer }>("/auth/profile", { method: "PATCH", body: JSON.stringify(data) }, token);
  },

  news(limit = 12): Promise<{ ok: boolean; posts: import("./types").TelegramNewsPost[] }> {
    return req<{ ok: boolean; posts: import("./types").TelegramNewsPost[] }>(`/news?limit=${limit}`);
  },

  reviews(limit = 8, offerId?: string): Promise<{ ok: boolean; reviews: import("./types").ShopReview[] }> {
    const qs = new URLSearchParams({ limit: String(limit) });
    if (offerId) qs.set("offerId", offerId);
    return req<{ ok: boolean; reviews: import("./types").ShopReview[] }>(`/reviews?${qs}`);
  },

  postReview(data: { offerId?: string; productName?: string; productImg?: string; rating: number; text: string; photoUrl?: string }, token: string): Promise<{ ok: boolean; review: import("./types").ShopReview; pointsEarned?: number }> {
    return req<{ ok: boolean; review: import("./types").ShopReview; pointsEarned?: number }>("/reviews", { method: "POST", body: JSON.stringify(data) }, token);
  },

  loyalty(token: string): Promise<import("./types").LoyaltyData & { ok: boolean }> {
    return req<import("./types").LoyaltyData & { ok: boolean }>("/loyalty", {}, token);
  },

  stockAlert(offerId: string, email: string): Promise<{ ok: boolean }> {
    return req<{ ok: boolean }>("/stock-alert", { method: "POST", body: JSON.stringify({ offerId, email }) });
  },

  newProducts(days = 14): Promise<import("./types").CatalogResponse> {
    return req<import("./types").CatalogResponse>(`/new?days=${days}`);
  },

  popular(limit = 8): Promise<{ ok: boolean; products: import("./types").ShopProduct[]; source?: string }> {
    return req<{ ok: boolean; products: import("./types").ShopProduct[]; source?: string }>(`/popular?limit=${limit}`);
  },

  marketplaceReviews(offerId: string): Promise<{ ok: boolean; reviews: MarketplaceReview[]; avgRating: number; reviewCount: number }> {
    return req<{ ok: boolean; reviews: MarketplaceReview[]; avgRating: number; reviewCount: number }>(`/marketplace-reviews?offerId=${encodeURIComponent(offerId)}`);
  },

  productQA(offerId: string): Promise<{ ok: boolean; items: ProductQAItem[] }> {
    return req<{ ok: boolean; items: ProductQAItem[] }>(`/product-qa?offerId=${encodeURIComponent(offerId)}`);
  },

  productNotes(brand: string, name: string): Promise<{ ok: boolean; data: FragranceNotes | null }> {
    return req<{ ok: boolean; data: FragranceNotes | null }>(`/product-notes?brand=${encodeURIComponent(brand)}&name=${encodeURIComponent(name)}`);
  },

  uploadMedia(file: File): Promise<{ ok: boolean; url: string; isVideo: boolean }> {
    const formData = new FormData();
    formData.append("file", file);
    return fetch((import.meta.env.VITE_API_BASE ?? "") + "/api/shop/upload-media", {
      method: "POST",
      body: formData,
    }).then((r) => r.json());
  },

  referral(token: string): Promise<{ ok: boolean; code: string; link: string; ordersFromRef: number; discountPct: number }> {
    return req<{ ok: boolean; code: string; link: string; ordersFromRef: number; discountPct: number }>("/auth/referral", {}, token);
  },

  validateReferral(code: string): Promise<{ ok: boolean; valid: boolean; discountPct: number }> {
    return req<{ ok: boolean; valid: boolean; discountPct: number }>("/referral/validate", { method: "POST", body: JSON.stringify({ code }) });
  },

  unboxings(): Promise<{ ok: boolean; unboxings: { id: string; name: string; mediaUrl: string; text: string; createdAt: string }[] }> {
    return req<{ ok: boolean; unboxings: { id: string; name: string; mediaUrl: string; text: string; createdAt: string }[] }>("/unboxings");
  },

  submitUnboxing(data: { name: string; mediaUrl: string; text: string }): Promise<{ ok: boolean }> {
    return req<{ ok: boolean }>("/unboxings", { method: "POST", body: JSON.stringify(data) });
  },
};

export const adminApi = {
  getBanners(): Promise<ShopBanner[]> {
    return req<ShopBanner[]>("/admin/banners");
  },

  saveBanner(banner: Partial<ShopBanner> & { id?: string }): Promise<ShopBanner> {
    if (banner.id) {
      return req<ShopBanner>(`/admin/banners/${banner.id}`, { method: "PUT", body: JSON.stringify(banner) });
    }
    return req<ShopBanner>("/admin/banners", { method: "POST", body: JSON.stringify(banner) });
  },

  deleteBanner(id: string): Promise<void> {
    return req<void>(`/admin/banners/${id}`, { method: "DELETE" });
  },

  getCategories(): Promise<ShopCategory[]> {
    return req<ShopCategory[]>("/admin/categories");
  },

  saveCategory(cat: Partial<ShopCategory> & { id?: string }): Promise<ShopCategory> {
    if (cat.id) {
      return req<ShopCategory>(`/admin/categories/${cat.id}`, { method: "PUT", body: JSON.stringify(cat) });
    }
    return req<ShopCategory>("/admin/categories", { method: "POST", body: JSON.stringify(cat) });
  },

  deleteCategory(id: string): Promise<void> {
    return req<void>(`/admin/categories/${id}`, { method: "DELETE" });
  },

  getSettings(): Promise<ShopSettings> {
    return req<ShopSettings>("/admin/settings");
  },

  saveSettings(s: Partial<ShopSettings>): Promise<ShopSettings> {
    return req<ShopSettings>("/admin/settings", { method: "PATCH", body: JSON.stringify(s) });
  },

  getNews(): Promise<{ ok: boolean; posts: (import("./types").TelegramNewsPost & { active: boolean })[] }> {
    return req<{ ok: boolean; posts: (import("./types").TelegramNewsPost & { active: boolean })[] }>("/admin/news");
  },

  importNews(): Promise<{ ok: boolean; message: string }> {
    return req<{ ok: boolean; message: string }>("/admin/news/import", { method: "POST" });
  },

  toggleNews(id: string, active: boolean): Promise<{ ok: boolean }> {
    return req<{ ok: boolean }>(`/admin/news/${id}`, { method: "PATCH", body: JSON.stringify({ active }) });
  },

  getReviews(): Promise<{ ok: boolean; reviews: (import("./types").ShopReview & { approved: boolean; customer?: { email: string } })[] }> {
    return req<{ ok: boolean; reviews: (import("./types").ShopReview & { approved: boolean; customer?: { email: string } })[] }>("/admin/reviews");
  },

  toggleReview(id: string, approved: boolean): Promise<{ ok: boolean }> {
    return req<{ ok: boolean }>(`/admin/reviews/${id}`, { method: "PATCH", body: JSON.stringify({ approved }) });
  },

  deleteReview(id: string): Promise<{ ok: boolean }> {
    return req<{ ok: boolean }>(`/admin/reviews/${id}`, { method: "DELETE" });
  },
};
