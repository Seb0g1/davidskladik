import { useEffect, useState } from "react";
import type { QueryClient } from "@tanstack/react-query";
import { ApiError } from "../api";
import type { Product, WarehousePage } from "../types";

export type MutationPayload = {
  product?: Product;
  products?: Product[];
};

export function mutationProducts(payload?: MutationPayload | null): Product[] {
  if (!payload) return [];
  const products = [...(Array.isArray(payload.products) ? payload.products : [])];
  if (payload.product) products.push(payload.product);
  const unique = new Map(products.filter(Boolean).map((product) => [product.id, product]));
  return Array.from(unique.values());
}

export function updateCachedProducts(queryClient: QueryClient, payload?: MutationPayload | null) {
  const products = mutationProducts(payload);
  if (!products.length) return;
  const byId = new Map(products.map((product) => [String(product.id), product]));
  queryClient.setQueriesData({ queryKey: ["warehouse", "page"] }, (old: WarehousePage | undefined) => {
    if (!old?.items?.length) return old;
    let changed = false;
    const items = old.items.map((item) => {
      const next = byId.get(String(item.id));
      if (!next) return item;
      changed = true;
      return { ...item, ...next };
    });
    return changed ? { ...old, items } : old;
  });
}

export function errorMessage(error: unknown): string {
  if (!error) return "";
  if (error instanceof ApiError) {
    const detail = error.detail && typeof error.detail === "object" ? error.detail as Record<string, unknown> : {};
    const code = error.code || detail.code;
    const model = detail.model || detail.imageModel;
    const endpoint = detail.endpoint || detail.apiBaseUrl;
    const status = detail.status || error.status;
    const suffix = [code && `code: ${code}`, model && `model: ${model}`, endpoint && `endpoint: ${endpoint}`, status && `status: ${status}`].filter(Boolean).join(" · ");
    return suffix ? `${error.message} · ${suffix}` : error.message;
  }
  return error instanceof Error ? error.message : String(error);
}

export function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

export function numberValue(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function money(value: unknown): string {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "-";
  return `${Math.round(n).toLocaleString("ru-RU")} ₽`;
}

export function compactDate(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

export async function copyPlainText(value: unknown): Promise<boolean> {
  const text = String(value ?? "");
  if (!text.trim()) return false;
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Fall through to the legacy textarea path when browser clipboard permissions are unavailable.
    }
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  textarea.remove();
  return copied || true;
}

export function useDebounced<T>(value: T, delayMs: number) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(id);
  }, [value, delayMs]);
  return debounced;
}
