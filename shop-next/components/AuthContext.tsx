"use client";
import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react";
import type { ShopCustomer } from "@/lib/types";

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop/auth";

interface AuthCtx {
  customer: ShopCustomer | null;
  token: string | null;
  loading: boolean;
  yandexLoading: boolean;
  yandexError: string | null;
  sendCode: (email: string) => Promise<void>;
  verifyCode: (email: string, code: string) => Promise<void>;
  updateProfile: (data: { firstName?: string; lastName?: string; phone?: string }) => Promise<void>;
  startYandexLogin: () => Promise<void>;
  clearYandexError: () => void;
  logout: () => void;
}

const Ctx = createContext<AuthCtx | null>(null);

async function apiPost(path: string, body: object, token?: string) {
  const res = await fetch(API_BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "Ошибка");
  return data;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [customer, setCustomer] = useState<ShopCustomer | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [yandexLoading, setYandexLoading] = useState(false);
  const [yandexError, setYandexError] = useState<string | null>(null);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");
    const state = params.get("state");
    if (code && state) {
      window.history.replaceState({}, "", window.location.pathname);
      setYandexLoading(true);
      apiPost("/yandex/callback", { code, state })
        .then((d) => { localStorage.setItem("mv_token", d.token); setToken(d.token); setCustomer(d.customer); })
        .catch((e: Error) => setYandexError(e.message))
        .finally(() => { setYandexLoading(false); setLoading(false); });
      return;
    }
    const saved = localStorage.getItem("mv_token");
    if (!saved) { setLoading(false); return; }
    setToken(saved);
    fetch(API_BASE + "/me", { headers: { Authorization: `Bearer ${saved}` } })
      .then((r) => r.json())
      .then((d) => { if (d.customer) setCustomer(d.customer); else localStorage.removeItem("mv_token"); })
      .catch(() => localStorage.removeItem("mv_token"))
      .finally(() => setLoading(false));
  }, []);

  const sendCode = useCallback(async (email: string) => { await apiPost("/send-code", { email }); }, []);
  const verifyCode = useCallback(async (email: string, code: string) => {
    const d = await apiPost("/verify-code", { email, code });
    localStorage.setItem("mv_token", d.token); setToken(d.token); setCustomer(d.customer);
  }, []);
  const updateProfile = useCallback(async (data: { firstName?: string; lastName?: string; phone?: string }) => {
    const saved = localStorage.getItem("mv_token");
    if (!saved) throw new Error("Не авторизован");
    const res = await fetch(API_BASE + "/profile", { method: "PATCH", headers: { "Content-Type": "application/json", Authorization: `Bearer ${saved}` }, body: JSON.stringify(data) });
    const d = await res.json();
    if (d.customer) setCustomer(d.customer);
  }, []);
  const startYandexLogin = useCallback(async () => {
    const res = await fetch(API_BASE + "/yandex/start");
    const { url } = await res.json();
    window.location.href = url;
  }, []);
  const clearYandexError = useCallback(() => setYandexError(null), []);
  const logout = useCallback(() => { localStorage.removeItem("mv_token"); setToken(null); setCustomer(null); }, []);

  return (
    <Ctx.Provider value={{ customer, token, loading, yandexLoading, yandexError, sendCode, verifyCode, updateProfile, startYandexLogin, clearYandexError, logout }}>
      {children}
    </Ctx.Provider>
  );
}

export function useAuth(): AuthCtx {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
