import type { ShopOrderPayload, ShopOrder } from "./types";

const BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "") + "/api/shop";

async function req<T>(path: string, init?: RequestInit, token?: string): Promise<T> {
  const authHeader: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {};
  const res = await fetch(BASE + path, {
    headers: { "Content-Type": "application/json", ...authHeader, ...(init?.headers ?? {}) },
    ...init,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || res.statusText);
  }
  return res.json() as Promise<T>;
}

export interface LoyaltyData {
  points: number;
  tier: "silver" | "gold" | "platinum";
  nextTier: number | null;
  transactions: { id: string; points: number; reason: string; createdAt: string }[];
}

export interface ShopReview {
  id: string;
  offerId?: string | null;
  productName?: string | null;
  productImg?: string | null;
  rating: number;
  text: string;
  photoUrl?: string | null;
  createdAt: string;
  author?: string;
}

export interface ShopCustomer {
  id: string;
  email: string;
  firstName?: string | null;
  lastName?: string | null;
  phone?: string | null;
  avatarUrl?: string | null;
}

export async function createOrder(payload: ShopOrderPayload, token?: string): Promise<ShopOrder> {
  return req<ShopOrder>("/orders", { method: "POST", body: JSON.stringify(payload) }, token);
}

export async function getOrders(token: string): Promise<{ ok: boolean; orders: ShopOrder[] }> {
  return req<{ ok: boolean; orders: ShopOrder[] }>("/auth/orders", {}, token);
}

export async function updateProfile(data: { firstName?: string; lastName?: string; phone?: string }, token: string): Promise<{ ok: boolean; customer: ShopCustomer }> {
  return req<{ ok: boolean; customer: ShopCustomer }>("/auth/profile", { method: "PATCH", body: JSON.stringify(data) }, token);
}

export async function postReview(data: { offerId?: string; productName?: string; productImg?: string; rating: number; text: string; photoUrl?: string }, token: string): Promise<{ ok: boolean; review: ShopReview; pointsEarned?: number }> {
  return req<{ ok: boolean; review: ShopReview; pointsEarned?: number }>("/reviews", { method: "POST", body: JSON.stringify(data) }, token);
}

export async function getLoyalty(token: string): Promise<LoyaltyData & { ok: boolean }> {
  return req<LoyaltyData & { ok: boolean }>("/loyalty", {}, token);
}

export async function stockAlert(offerId: string, email: string): Promise<{ ok: boolean }> {
  return req<{ ok: boolean }>("/stock-alert", { method: "POST", body: JSON.stringify({ offerId, email }) });
}

export async function aiSearch(query: string): Promise<{ ok: boolean; label: string; terms: string[]; notes: string[]; accords: string[]; products: { id: string; offerId: string; name: string; brand: string; priceRub: number; images: string[]; inStock: boolean; _matchTerm?: string }[] }> {
  return req("/ai-search", { method: "POST", body: JSON.stringify({ query }) });
}

export async function referral(token: string): Promise<{ ok: boolean; code: string; link: string; ordersFromRef: number; discountPct: number }> {
  return req<{ ok: boolean; code: string; link: string; ordersFromRef: number; discountPct: number }>("/auth/referral", {}, token);
}

export async function validateReferral(code: string): Promise<{ ok: boolean; valid: boolean; discountPct: number }> {
  return req<{ ok: boolean; valid: boolean; discountPct: number }>("/referral/validate", { method: "POST", body: JSON.stringify({ code }) });
}

export async function vip(token: string): Promise<{ ok: boolean; eligible: boolean; ordersCount: number; vipLink: string | null }> {
  return req<{ ok: boolean; eligible: boolean; ordersCount: number; vipLink: string | null }>("/auth/vip", {}, token);
}

export async function submitUnboxing(data: { name: string; mediaUrl: string; text: string }): Promise<{ ok: boolean }> {
  return req<{ ok: boolean }>("/unboxings", { method: "POST", body: JSON.stringify(data) });
}

export async function uploadMedia(file: File): Promise<{ ok: boolean; url: string; isVideo: boolean }> {
  const formData = new FormData();
  formData.append("file", file);
  const res = await fetch((process.env.NEXT_PUBLIC_API_BASE ?? "") + "/api/shop/upload-media", {
    method: "POST",
    body: formData,
  });
  return res.json();
}

export async function emailSubscribe(email: string, source = "popup"): Promise<{ ok: boolean }> {
  return req<{ ok: boolean }>("/email-subscribe", { method: "POST", body: JSON.stringify({ email, source }) });
}

export interface CatalogFetchParams {
  category?: string; q?: string; brand?: string;
  page?: number; pageSize?: number; inStock?: boolean; sort?: string;
}

export async function catalogFetch(params: CatalogFetchParams): Promise<{ products: import("./types").ShopProduct[]; total: number; brands: string[] }> {
  const qs = new URLSearchParams();
  if (params.category) qs.set("category", params.category);
  if (params.q) qs.set("q", params.q);
  if (params.brand) qs.set("brand", params.brand);
  if (params.page && params.page > 1) qs.set("page", String(params.page));
  if (params.pageSize) qs.set("pageSize", String(params.pageSize));
  if (params.inStock) qs.set("inStock", "true");
  if (params.sort) qs.set("sort", params.sort);
  return req(`/catalog?${qs}`);
}

export async function fetchAutoCategoriesClient(): Promise<import("./types").AutoCategory[]> {
  return req<import("./types").AutoCategory[]>("/auto-categories");
}
