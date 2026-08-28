import type { ShopProduct, ShopBanner, ShopCategory, ShopSettings, ShopOrderPayload, ShopOrder, CatalogResponse, AutoCategory, TelegramNewsPost, ShopReview } from "./types";

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

  postReview(data: { offerId?: string; productName?: string; productImg?: string; rating: number; text: string }, token: string): Promise<{ ok: boolean; review: import("./types").ShopReview }> {
    return req<{ ok: boolean; review: import("./types").ShopReview }>("/reviews", { method: "POST", body: JSON.stringify(data) }, token);
  },

  stockAlert(offerId: string, email: string): Promise<{ ok: boolean }> {
    return req<{ ok: boolean }>("/stock-alert", { method: "POST", body: JSON.stringify({ offerId, email }) });
  },

  newProducts(days = 14): Promise<import("./types").CatalogResponse> {
    return req<import("./types").CatalogResponse>(`/new?days=${days}`);
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
