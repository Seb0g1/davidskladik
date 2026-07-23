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
  login: (email: string, password: string) => Promise<void>;
  register: (data: RegisterData) => Promise<void>;
  logout: () => void;
}

interface RegisterData {
  email: string;
  password: string;
  firstName?: string;
  lastName?: string;
  phone?: string;
}

const Ctx = createContext<AuthCtx | null>(null);

async function authReq(path: string, body: object): Promise<{ token: string; customer: ShopCustomer }> {
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

  const login = useCallback(async (email: string, password: string) => {
    const { token: t, customer: c } = await authReq("/login", { email, password });
    localStorage.setItem("mv_token", t);
    setToken(t);
    setCustomer(c);
  }, []);

  const register = useCallback(async (data: RegisterData) => {
    const { token: t, customer: c } = await authReq("/register", data);
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
    <Ctx.Provider value={{ customer, token, loading, login, register, logout }}>
      {children}
    </Ctx.Provider>
  );
}

export function useAuth(): AuthCtx {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
