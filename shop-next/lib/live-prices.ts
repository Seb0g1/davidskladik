"use client";
// Live price / stock from the warehouse API (POST /api/shop/prices — the price the order is charged at).
// Pages are ISR-cached and the cart lives in localStorage, so the HTML can carry an old price after a
// markup change; every price shown to the buyer is refreshed through here after hydration.
// Requests from all cards on a page are batched into one call; answers are cached for a minute.
import { useEffect, useMemo, useState } from "react";

export interface LivePrice {
  offerId: string;
  priceRub: number;
  oldPriceRub?: number | null;
  inStock: boolean;
  stockQty: number;
}

type Priced = { offerId: string; priceRub: number; oldPriceRub?: number | null; inStock: boolean; stockQty?: number };

const BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "") + "/api/shop";
const TTL_MS = 60_000;
const CHUNK = 200;

const cache = new Map<string, { at: number; value: LivePrice | null }>();
const waiting = new Map<string, Array<(v: LivePrice | null) => void>>();
let timer: ReturnType<typeof setTimeout> | null = null;

const keyOf = (offerId: string) => offerId.trim().toLowerCase();

async function flush() {
  timer = null;
  const batch = new Map(waiting);
  waiting.clear();
  const ids = [...batch.keys()];
  for (let i = 0; i < ids.length; i += CHUNK) {
    const chunk = ids.slice(i, i + CHUNK);
    let found = new Map<string, LivePrice>();
    try {
      const res = await fetch(`${BASE}/prices`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ offerIds: chunk }),
        cache: "no-store",
      });
      if (res.ok) {
        const data = (await res.json()) as { prices?: LivePrice[] };
        found = new Map((data.prices ?? []).map((p) => [keyOf(p.offerId), p]));
      }
    } catch { /* network error → keep the prices already on the page */ }
    for (const k of chunk) {
      const value = found.get(k) ?? null;
      if (found.size) cache.set(k, { at: Date.now(), value });
      for (const resolve of batch.get(k) ?? []) resolve(value);
    }
  }
}

function request(offerId: string): Promise<LivePrice | null> {
  const k = keyOf(offerId);
  const hit = cache.get(k);
  if (hit && Date.now() - hit.at < TTL_MS) return Promise.resolve(hit.value);
  return new Promise((resolve) => {
    const list = waiting.get(k) ?? [];
    list.push(resolve);
    waiting.set(k, list);
    if (!timer) timer = setTimeout(flush, 30);
  });
}

/** offerIds → Map(lowercase offerId → live price); missing = no live price (keep the old one). */
export async function fetchLivePrices(offerIds: string[]): Promise<Map<string, LivePrice>> {
  const ids = [...new Set(offerIds.filter(Boolean))];
  const values = await Promise.all(ids.map(request));
  const out = new Map<string, LivePrice>();
  ids.forEach((id, i) => { const v = values[i]; if (v && v.priceRub > 0) out.set(keyOf(id), v); });
  return out;
}

export function applyLivePrice<T extends Priced>(item: T, live: LivePrice | undefined): T {
  if (!live || !(live.priceRub > 0)) return item;
  if (item.priceRub === live.priceRub && (item.oldPriceRub ?? undefined) === (live.oldPriceRub ?? undefined)
    && item.inStock === live.inStock && (item.stockQty === undefined || item.stockQty === live.stockQty)) return item;
  return { ...item, priceRub: live.priceRub, oldPriceRub: live.oldPriceRub ?? undefined, inStock: live.inStock, stockQty: live.stockQty };
}

/** The items with live prices (first render = the props as rendered on the server, so hydration matches). */
export function useLivePrices<T extends Priced>(items: T[]): T[] {
  const [live, setLive] = useState<Map<string, LivePrice>>(() => new Map());
  const ids = items.map((p) => p.offerId).join("\n");
  useEffect(() => {
    if (!ids) return;
    let alive = true;
    fetchLivePrices(ids.split("\n")).then((m) => { if (alive && m.size) setLive(m); });
    return () => { alive = false; };
  }, [ids]);
  return useMemo(() => items.map((p) => applyLivePrice(p, live.get(keyOf(p.offerId)))), [items, live]);
}

export function useLivePrice<T extends Priced>(item: T): T {
  return useLivePrices(useMemo(() => [item], [item]))[0];
}
