import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react";

const API = (import.meta.env.VITE_API_BASE ?? "") + "/api/shop/auth";

export interface ShopCustomer {
  id: string;
  email: string;
  firstName?: string | null;
  lastName?: string | null;
  phone?: string | null;
  avatarUrl?: string | null;
}

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

async function apiReq(path: string, init: RequestInit, token?: string) {
  const res = await fetch(API + path, {
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...init,
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "Ошибка");
  return data;
}

function apiPost(path: string, body: object, token?: string) {
  return apiReq(path, { method: "POST", body: JSON.stringify(body) }, token);
}

function apiPatch(path: string, body: object, token: string) {
  return apiReq(path, { method: "PATCH", body: JSON.stringify(body) }, token);
}

function applyTokenResponse(data: { token: string; customer: ShopCustomer }, setToken: (t: string) => void, setCustomer: (c: ShopCustomer) => void) {
  localStorage.setItem("mv_token", data.token);
  setToken(data.token);
  setCustomer(data.customer);
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [customer, setCustomer] = useState<ShopCustomer | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [yandexLoading, setYandexLoading] = useState(false);
  const [yandexError, setYandexError] = useState<string | null>(null);

  useEffect(() => {
    // Handle Yandex OAuth callback: Yandex redirects to / with ?code=&state=
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");
    const state = params.get("state");

    if (code && state) {
      window.history.replaceState({}, "", window.location.pathname);
      setYandexLoading(true);
      apiPost("/yandex/callback", { code, state })
        .then((data) => {
          applyTokenResponse(data, setToken, setCustomer);
        })
        .catch((err: Error) => {
          setYandexError(err.message || "Не удалось войти через Яндекс");
        })
        .finally(() => {
          setYandexLoading(false);
          setLoading(false);
        });
      return;
    }

    // Normal JWT restoration
    const saved = localStorage.getItem("mv_token");
    if (!saved) { setLoading(false); return; }
    setToken(saved);
    fetch(API + "/me", { headers: { Authorization: `Bearer ${saved}` } })
      .then((r) => r.json())
      .then((d) => { if (d.customer) setCustomer(d.customer); else localStorage.removeItem("mv_token"); })
      .catch(() => localStorage.removeItem("mv_token"))
      .finally(() => setLoading(false));
  }, []);

  const sendCode = useCallback(async (email: string) => {
    await apiPost("/send-code", { email });
  }, []);

  const verifyCode = useCallback(async (email: string, code: string) => {
    const data = await apiPost("/verify-code", { email, code });
    applyTokenResponse(data, setToken, setCustomer);
  }, []);

  const updateProfile = useCallback(async (data: { firstName?: string; lastName?: string; phone?: string }) => {
    const saved = localStorage.getItem("mv_token");
    if (!saved) throw new Error("Не авторизован");
    const res = await apiPatch("/profile", data, saved);
    if (res.customer) setCustomer(res.customer);
  }, []);

  const startYandexLogin = useCallback(async () => {
    const res = await fetch(API + "/yandex/start");
    if (!res.ok) throw new Error("Не удалось запустить вход через Яндекс");
    const { url } = await res.json();
    if (!url) throw new Error("Нет URL");
    window.location.href = url;
  }, []);

  const clearYandexError = useCallback(() => setYandexError(null), []);

  const logout = useCallback(() => {
    localStorage.removeItem("mv_token");
    setToken(null);
    setCustomer(null);
  }, []);

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
