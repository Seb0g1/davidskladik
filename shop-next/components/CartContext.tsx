"use client";
import { createContext, useContext, useReducer, useCallback, useEffect, useState, type ReactNode } from "react";
import type { CartItem, ShopProduct } from "@/lib/types";

const CART_KEY = "mv_cart_v1";

type Action =
  | { type: "ADD"; product: ShopProduct; qty?: number }
  | { type: "REMOVE"; offerId: string }
  | { type: "SET_QTY"; offerId: string; qty: number }
  | { type: "CLEAR" }
  | { type: "LOAD"; items: CartItem[] };

function reducer(state: { items: CartItem[] }, action: Action): { items: CartItem[] } {
  switch (action.type) {
    case "ADD": {
      const qty = action.qty ?? 1;
      const existing = state.items.find((i) => i.product.offerId === action.product.offerId);
      if (existing) {
        return { items: state.items.map((i) => i.product.offerId === action.product.offerId ? { ...i, quantity: Math.min(i.quantity + qty, i.product.stockQty) } : i) };
      }
      return { items: [...state.items, { product: action.product, quantity: qty }] };
    }
    case "REMOVE": return { items: state.items.filter((i) => i.product.offerId !== action.offerId) };
    case "SET_QTY":
      if (action.qty <= 0) return { items: state.items.filter((i) => i.product.offerId !== action.offerId) };
      return { items: state.items.map((i) => i.product.offerId === action.offerId ? { ...i, quantity: action.qty } : i) };
    case "CLEAR": return { items: [] };
    case "LOAD": return { items: action.items };
    default: return state;
  }
}

function loadCart(): { items: CartItem[] } {
  if (typeof window === "undefined") return { items: [] };
  try {
    const raw = localStorage.getItem(CART_KEY);
    if (!raw) return { items: [] };
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed?.items)) return { items: parsed.items };
  } catch { /* ignore */ }
  return { items: [] };
}

interface CartCtx {
  items: CartItem[];
  totalItems: number;
  totalRub: number;
  add: (product: ShopProduct, qty?: number) => void;
  remove: (offerId: string) => void;
  setQty: (offerId: string, qty: number) => void;
  clear: () => void;
}

const CartContext = createContext<CartCtx | null>(null);

export function CartProvider({ children }: { children: ReactNode }) {
  // Start with empty cart on both server and client for consistent SSR hydration.
  // Load from localStorage in useEffect (client-only) to avoid mismatch.
  const [state, dispatch] = useReducer(reducer, { items: [] });
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    const saved = loadCart();
    if (saved.items.length > 0) dispatch({ type: "LOAD", items: saved.items });
    setHydrated(true);
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    try { localStorage.setItem(CART_KEY, JSON.stringify({ items: state.items })); } catch { /* ignore */ }
  }, [state.items, hydrated]);

  const add = useCallback((product: ShopProduct, qty?: number) => dispatch({ type: "ADD", product, qty }), []);
  const remove = useCallback((offerId: string) => dispatch({ type: "REMOVE", offerId }), []);
  const setQty = useCallback((offerId: string, qty: number) => dispatch({ type: "SET_QTY", offerId, qty }), []);
  const clear = useCallback(() => dispatch({ type: "CLEAR" }), []);

  const totalItems = state.items.reduce((s, i) => s + i.quantity, 0);
  const totalRub = state.items.reduce((s, i) => s + i.product.priceRub * i.quantity, 0);

  return <CartContext.Provider value={{ items: state.items, totalItems, totalRub, add, remove, setQty, clear }}>{children}</CartContext.Provider>;
}

export function useCart(): CartCtx {
  const ctx = useContext(CartContext);
  if (!ctx) throw new Error("useCart must be inside CartProvider");
  return ctx;
}
