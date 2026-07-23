import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react";

const API = (import.meta.env.VITE_API_BASE ?? "") + "/api/shop/auth";

export interface ShopCustomer {
  id: string;
  email: string;
  firstName?: string | null;
  lastName?: string | null;
  phone?: string | null;
}

interface AuthCtx {
  customer: ShopCustomer | null;
  token: string | null;
  loading: boolean;
  sendCode: (email: string) => Promise<void>;
  verifyCode: (email: string, code: string) => Promise<void>;
  logout: () => void;
}

const Ctx = createContext<AuthCtx | null>(null);

async function apiPost(path: string, body: object) {
  const res = await fetch(API + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
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

  useEffect(() => {
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
    const { token: t, customer: c } = await apiPost("/verify-code", { email, code });
    localStorage.setItem("mv_token", t);
    setToken(t);
    setCustomer(c);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem("mv_token");
    setToken(null);
    setCustomer(null);
  }, []);

  return (
    <Ctx.Provider value={{ customer, token, loading, sendCode, verifyCode, logout }}>
      {children}
    </Ctx.Provider>
  );
}

export function useAuth(): AuthCtx {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
