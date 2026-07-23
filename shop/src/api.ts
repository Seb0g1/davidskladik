import type { ShopProduct, ShopBanner, ShopCategory, ShopSettings, ShopOrderPayload, ShopOrder, CatalogResponse } from "./types";

const BASE = "/api/shop";

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + path, {
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
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

  createOrder(payload: ShopOrderPayload): Promise<ShopOrder> {
    return req<ShopOrder>("/orders", { method: "POST", body: JSON.stringify(payload) });
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
};
