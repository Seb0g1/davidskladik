import { useState, useEffect } from "react";
import { X, Eye, EyeOff, Loader2 } from "lucide-react";
import { useAuth } from "../AuthContext";
import clsx from "clsx";

interface Props {
  open: boolean;
  onClose: () => void;
  defaultTab?: "login" | "register";
}

export default function AuthModal({ open, onClose, defaultTab = "login" }: Props) {
  const { login, register } = useAuth();
  const [tab, setTab] = useState<"login" | "register">(defaultTab);
  const [showPw, setShowPw] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [form, setForm] = useState({
    email: "", password: "", firstName: "", lastName: "", phone: "",
  });

  useEffect(() => {
    if (open) { setTab(defaultTab); setError(""); setForm({ email: "", password: "", firstName: "", lastName: "", phone: "" }); }
  }, [open, defaultTab]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  function set(k: keyof typeof form) {
    return (e: React.ChangeEvent<HTMLInputElement>) => setForm((f) => ({ ...f, [k]: e.target.value }));
  }

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      if (tab === "login") {
        await login(form.email, form.password);
      } else {
        await register({ email: form.email, password: form.password, firstName: form.firstName, lastName: form.lastName, phone: form.phone });
      }
      onClose();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-4" onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className="absolute inset-0 bg-black/40 backdrop-blur-sm modal-overlay" onClick={onClose} />
      <div className="relative bg-white rounded-3xl shadow-xl-soft w-full max-w-md modal-content">
        <button onClick={onClose} className="absolute top-4 right-4 p-2 rounded-full text-gray-400 hover:bg-gray-100 hover:text-gray-600 transition-colors">
          <X size={18} />
        </button>

        {/* Tabs */}
        <div className="px-8 pt-8 pb-0">
          <div className="flex gap-1 bg-apple-gray-bg rounded-xl p-1 mb-6">
            {(["login", "register"] as const).map((t) => (
              <button
                key={t}
                onClick={() => { setTab(t); setError(""); }}
                className={clsx(
                  "flex-1 py-2.5 rounded-lg text-sm font-semibold transition-all duration-200",
                  tab === t ? "bg-white text-apple-black shadow-sm" : "text-apple-gray hover:text-apple-black"
                )}
              >
                {t === "login" ? "Войти" : "Регистрация"}
              </button>
            ))}
          </div>

          <h2 className="text-xl font-bold text-apple-black tracking-tight mb-1">
            {tab === "login" ? "Вход в аккаунт" : "Создать аккаунт"}
          </h2>
          <p className="text-sm text-apple-gray mb-6">
            {tab === "login" ? "Введите email и пароль" : "Регистрация займёт 1 минуту"}
          </p>
        </div>

        <form onSubmit={submit} className="px-8 pb-8 space-y-3">
          {tab === "register" && (
            <div className="grid grid-cols-2 gap-3">
              <input type="text" placeholder="Имя" value={form.firstName} onChange={set("firstName")}
                className="px-4 py-3 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all" />
              <input type="text" placeholder="Фамилия" value={form.lastName} onChange={set("lastName")}
                className="px-4 py-3 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all" />
            </div>
          )}

          <input type="email" placeholder="Email *" required value={form.email} onChange={set("email")}
            className="w-full px-4 py-3 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all" />

          {tab === "register" && (
            <input type="tel" placeholder="Телефон" value={form.phone} onChange={set("phone")}
              className="w-full px-4 py-3 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all" />
          )}

          <div className="relative">
            <input
              type={showPw ? "text" : "password"}
              placeholder="Пароль *"
              required
              minLength={6}
              value={form.password}
              onChange={set("password")}
              className="w-full px-4 py-3 pr-12 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all"
            />
            <button type="button" onClick={() => setShowPw(!showPw)}
              className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-apple-gray hover:text-apple-black transition-colors">
              {showPw ? <EyeOff size={16} /> : <Eye size={16} />}
            </button>
          </div>

          {error && (
            <div className="text-sm text-red-600 bg-red-50 rounded-xl px-4 py-3">{error}</div>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-violet-600 hover:bg-violet-700 disabled:opacity-60 text-white py-3.5 rounded-xl font-semibold text-sm transition-all duration-200 flex items-center justify-center gap-2 mt-2"
          >
            {loading && <Loader2 size={16} className="animate-spin" />}
            {tab === "login" ? "Войти" : "Зарегистрироваться"}
          </button>

          <p className="text-center text-xs text-apple-gray pt-1">
            {tab === "login" ? (
              <>Нет аккаунта?{" "}<button type="button" onClick={() => setTab("register")} className="text-violet-600 hover:underline font-medium">Зарегистрируйтесь</button></>
            ) : (
              <>Уже есть аккаунт?{" "}<button type="button" onClick={() => setTab("login")} className="text-violet-600 hover:underline font-medium">Войдите</button></>
            )}
          </p>
        </form>
      </div>
    </div>
  );
}
