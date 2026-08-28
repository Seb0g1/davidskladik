import { createContext, useContext, useReducer, useCallback, useEffect, type ReactNode } from "react";
import type { CartItem, ShopProduct } from "./types";

const CART_STORAGE_KEY = "mv_cart_v1";

interface CartState {
  items: CartItem[];
}

type CartAction =
  | { type: "ADD"; product: ShopProduct; qty?: number }
  | { type: "REMOVE"; offerId: string }
  | { type: "SET_QTY"; offerId: string; qty: number }
  | { type: "CLEAR" };

function cartReducer(state: CartState, action: CartAction): CartState {
  switch (action.type) {
    case "ADD": {
      const qty = action.qty ?? 1;
      const existing = state.items.find((i) => i.product.offerId === action.product.offerId);
      if (existing) {
        return {
          items: state.items.map((i) =>
            i.product.offerId === action.product.offerId
              ? { ...i, quantity: Math.min(i.quantity + qty, i.product.stockQty) }
              : i
          ),
        };
      }
      return { items: [...state.items, { product: action.product, quantity: qty }] };
    }
    case "REMOVE":
      return { items: state.items.filter((i) => i.product.offerId !== action.offerId) };
    case "SET_QTY":
      if (action.qty <= 0) return { items: state.items.filter((i) => i.product.offerId !== action.offerId) };
      return {
        items: state.items.map((i) =>
          i.product.offerId === action.offerId ? { ...i, quantity: action.qty } : i
        ),
      };
    case "CLEAR":
      return { items: [] };
    default:
      return state;
  }
}

function loadFromStorage(): CartState {
  try {
    const raw = localStorage.getItem(CART_STORAGE_KEY);
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
  const [state, dispatch] = useReducer(cartReducer, undefined, loadFromStorage);

  useEffect(() => {
    try {
      localStorage.setItem(CART_STORAGE_KEY, JSON.stringify({ items: state.items }));
    } catch { /* ignore quota errors */ }
  }, [state.items]);

  const add = useCallback((product: ShopProduct, qty?: number) => dispatch({ type: "ADD", product, qty }), []);
  const remove = useCallback((offerId: string) => dispatch({ type: "REMOVE", offerId }), []);
  const setQty = useCallback((offerId: string, qty: number) => dispatch({ type: "SET_QTY", offerId, qty }), []);
  const clear = useCallback(() => dispatch({ type: "CLEAR" }), []);

  const totalItems = state.items.reduce((s, i) => s + i.quantity, 0);
  const totalRub = state.items.reduce((s, i) => s + i.product.priceRub * i.quantity, 0);

  return (
    <CartContext.Provider value={{ items: state.items, totalItems, totalRub, add, remove, setQty, clear }}>
      {children}
    </CartContext.Provider>
  );
}

export function useCart(): CartCtx {
  const ctx = useContext(CartContext);
  if (!ctx) throw new Error("useCart must be inside CartProvider");
  return ctx;
}
