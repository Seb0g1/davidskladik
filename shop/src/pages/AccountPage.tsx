import { useState, useEffect } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Package, User, Loader2, CheckCircle, Clock, Truck, LogOut, Save, ChevronRight, Star } from "lucide-react";
import { useAuth } from "../AuthContext";
import { api } from "../api";
import type { ShopOrder } from "../types";

const S = {
  bg:      "#0E0D0B",
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  borderMd:"rgba(255,252,245,0.13)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
  accent3: "#EDD9B0",
};

const STATUS_MAP: Record<string, { label: string; color: string; icon: typeof Package }> = {
  pending:         { label: "Принят",           color: "#f59e0b", icon: Clock },
  payment_pending: { label: "Ожидает оплаты",   color: "#f59e0b", icon: Clock },
  paid:            { label: "Оплачен",           color: "#10b981", icon: CheckCircle },
  confirmed:       { label: "Подтверждён",       color: "#3b82f6", icon: Clock },
  picking:         { label: "Собирается",        color: "#8b5cf6", icon: Package },
  shipped:         { label: "Отправлен",         color: "#0ea5e9", icon: Truck },
  delivered:       { label: "Доставлен",         color: "#10b981", icon: CheckCircle },
  cancelled:       { label: "Отменён",           color: "#ef4444", icon: Package },
};

function StatusBadge({ status }: { status: string }) {
  const s = STATUS_MAP[status] || { label: status, color: "#6e6e73", icon: Package };
  const Icon = s.icon;
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 5, fontSize: 11, fontWeight: 700, padding: "3px 10px", borderRadius: 100, background: s.color + "1a", color: s.color }}>
      <Icon size={11} strokeWidth={2.5} />
      {s.label}
    </span>
  );
}

function OrderCard({ order }: { order: ShopOrder }) {
  const date = new Date(order.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  const items = order.items || [];
  return (
    <div style={{ background: S.surface, borderRadius: 18, border: `1px solid ${S.border}`, overflow: "hidden" }}>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16, padding: "16px 20px", borderBottom: `1px solid ${S.border}` }}>
        <div>
          <div style={{ fontSize: 11, color: S.muted, marginBottom: 4 }}>{date}</div>
          <div style={{ fontSize: 12, fontFamily: "monospace", color: S.subtle }}>#{order.id.slice(-8).toUpperCase()}</div>
        </div>
        <div style={{ textAlign: "right", display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 8 }}>
          <StatusBadge status={order.status} />
          <div style={{ fontSize: 16, fontWeight: 700, color: S.text }}>{order.totalRub.toLocaleString("ru-RU")} ₽</div>
        </div>
      </div>
      {items.length > 0 && (
        <div style={{ padding: "12px 20px", display: "flex", flexDirection: "column", gap: 6 }}>
          {items.slice(0, 3).map((item, i) => (
            <div key={i} style={{ display: "flex", justifyContent: "space-between", fontSize: 12 }}>
              <span style={{ color: S.muted, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{(item as { name?: string; offerId: string }).name || (item as { offerId: string }).offerId}</span>
              <span style={{ color: S.subtle, marginLeft: 12, flexShrink: 0 }}>×{(item as { quantity: number }).quantity}</span>
              <span style={{ fontWeight: 600, color: S.text, marginLeft: 12, flexShrink: 0 }}>{((item as { priceRub: number; quantity: number }).priceRub * (item as { quantity: number }).quantity).toLocaleString("ru-RU")} ₽</span>
            </div>
          ))}
          {items.length > 3 && (
            <div style={{ fontSize: 11, color: S.subtle }}>+ещё {items.length - 3} товара</div>
          )}
        </div>
      )}
      {order.delivery?.city && (
        <div style={{ padding: "0 20px 14px", fontSize: 11, color: S.subtle, display: "flex", alignItems: "center", gap: 6 }}>
          <Truck size={11} />
          {[order.delivery.city, order.delivery.address].filter(Boolean).join(", ")}
        </div>
      )}
    </div>
  );
}

function OrdersTab({ token }: { token: string }) {
  const [orders, setOrders] = useState<ShopOrder[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    api.getOrders(token)
      .then((r) => setOrders(r.orders))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [token]);

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 10, padding: "80px 20px", fontSize: 13, color: S.muted }}>
      <Loader2 size={18} style={{ animation: "spin 1s linear infinite" }} /> Загружаем заказы...
    </div>
  );

  if (error) return (
    <div style={{ background: "rgba(239,68,68,0.08)", color: "#f87171", borderRadius: 16, padding: "14px 18px", fontSize: 13, border: "1px solid rgba(239,68,68,0.15)" }}>{error}</div>
  );

  if (!orders.length) return (
    <div style={{ textAlign: "center", padding: "80px 20px" }}>
      <div style={{ width: 56, height: 56, borderRadius: 18, display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px", background: "rgba(201,169,110,0.1)", border: "1px solid rgba(201,169,110,0.15)" }}>
        <Package size={24} style={{ color: S.accent3 }} strokeWidth={1.5} />
      </div>
      <div style={{ fontSize: 15, fontWeight: 600, color: S.text, marginBottom: 6 }}>Заказов пока нет</div>
      <div style={{ fontSize: 13, color: S.muted, marginBottom: 24 }}>Ваши заказы появятся здесь</div>
      <Link to="/catalog" style={{ fontSize: 13, fontWeight: 600, color: S.accent3, textDecoration: "none", transition: "color 0.15s" }}
        onMouseEnter={e => (e.currentTarget.style.color = "var(--accent)")}
        onMouseLeave={e => (e.currentTarget.style.color = S.accent3)}>
        Перейти в каталог
      </Link>
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      {orders.map((o) => <OrderCard key={o.id} order={o} />)}
    </div>
  );
}

function DarkInput({ label, ...props }: { label: string } & React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <div>
      <label style={{ display: "block", fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.subtle, marginBottom: 6 }}>{label}</label>
      <input {...props} style={{ width: "100%", padding: "11px 14px", background: "rgba(255,255,255,0.05)", border: `1.5px solid ${S.border}`, borderRadius: 12, fontSize: 13, fontFamily: "inherit", color: S.text, outline: "none", transition: "all 0.15s ease" }}
        onFocus={e => { (e.target as HTMLInputElement).style.borderColor = "rgba(201,169,110,0.4)"; (e.target as HTMLInputElement).style.background = "rgba(255,255,255,0.08)"; }}
        onBlur={e => { (e.target as HTMLInputElement).style.borderColor = S.border; (e.target as HTMLInputElement).style.background = "rgba(255,255,255,0.05)"; }} />
    </div>
  );
}

const TIER_META = {
  silver:   { label: "Серебро",  color: "#9ca3af", bg: "rgba(156,163,175,0.1)", next: 100 },
  gold:     { label: "Золото",   color: "#c9a25e", bg: "rgba(201,162,94,0.1)",  next: 300 },
  platinum: { label: "Платина",  color: "#a78bfa", bg: "rgba(167,139,250,0.1)", next: null },
} as const;

function LoyaltyPanel({ token }: { token: string }) {
  const { data } = useQuery({
    queryKey: ["shop-loyalty", token],
    queryFn: () => api.loyalty(token),
    staleTime: 2 * 60_000,
    enabled: !!token,
  });
  if (!data) return null;
  const { points, tier, nextTier, transactions } = data;
  const meta = TIER_META[tier];
  const progress = nextTier ? Math.min(100, Math.round((points / nextTier) * 100)) : 100;
  return (
    <div style={{ background: `linear-gradient(135deg, #1a1408 0%, ${S.surface} 70%)`, borderRadius: 18, padding: "20px", border: "1px solid rgba(201,162,94,0.25)" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 14 }}>
        <Star size={14} style={{ color: meta.color }} />
        <span style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.18em", color: S.subtle }}>Золото Magic Vibes</span>
      </div>
      <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 10 }}>
        <span style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontSize: 42, color: meta.color, lineHeight: 1 }}>{points}</span>
        <span style={{ fontSize: 12, color: S.muted }}>баллов</span>
        <span style={{ marginLeft: "auto", fontSize: 11, fontWeight: 600, padding: "3px 10px", borderRadius: 100, background: meta.bg, color: meta.color, border: `1px solid ${meta.color}40` }}>{meta.label}</span>
      </div>
      {nextTier && (
        <div style={{ marginBottom: 14 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: S.subtle, marginBottom: 5 }}>
            <span>{points} / {nextTier} до следующего уровня</span>
            <span>{progress}%</span>
          </div>
          <div style={{ height: 4, background: "rgba(255,255,255,0.07)", borderRadius: 2, overflow: "hidden" }}>
            <div style={{ height: "100%", width: `${progress}%`, background: `linear-gradient(90deg, ${meta.color}88, ${meta.color})`, borderRadius: 2, transition: "width 0.8s ease" }} />
          </div>
        </div>
      )}
      {transactions.length > 0 && (
        <div style={{ borderTop: `1px solid ${S.border}`, paddingTop: 12 }}>
          <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.12em", color: S.subtle, marginBottom: 8 }}>Последние начисления</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            {transactions.slice(0, 5).map((t) => (
              <div key={t.id} style={{ display: "flex", justifyContent: "space-between", fontSize: 12 }}>
                <span style={{ color: S.muted }}>{t.reason}</span>
                <span style={{ color: "#4ade80", fontWeight: 600 }}>+{t.points}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      <p style={{ fontSize: 11, color: S.subtle, marginTop: 12, lineHeight: 1.6 }}>
        Баллы начисляются за отзывы (+20), отзывы с фото (+50) и одобренные анбоксинги.
      </p>
    </div>
  );
}

function ProfileTab() {
  const { customer, updateProfile, token } = useAuth();
  const [form, setForm] = useState({
    firstName: customer?.firstName || "",
    lastName: customer?.lastName || "",
    phone: customer?.phone || "",
  });
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState("");

  function setF(k: keyof typeof form) {
    return (e: React.ChangeEvent<HTMLInputElement>) => { setForm((f) => ({ ...f, [k]: e.target.value })); setSaved(false); };
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setSaving(true);
    try {
      await updateProfile(form);
      setSaved(true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      {/* Email */}
      <div style={{ background: S.surface, borderRadius: 18, padding: "18px 20px", border: `1px solid ${S.border}` }}>
        <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.subtle, marginBottom: 12 }}>Email</div>
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 14px", background: S.surface2, borderRadius: 12, border: `1px solid ${S.border}` }}>
          <span style={{ fontSize: 13, color: S.text }}>{customer?.email}</span>
          <span style={{ marginLeft: "auto", fontSize: 10, color: "#4ade80", background: "rgba(74,222,128,0.1)", border: "1px solid rgba(74,222,128,0.2)", padding: "2px 8px", borderRadius: 100 }}>подтверждён</span>
        </div>
      </div>

      {/* Profile form */}
      <form onSubmit={handleSave} style={{ background: S.surface, borderRadius: 18, padding: "18px 20px", border: `1px solid ${S.border}`, display: "flex", flexDirection: "column", gap: 14 }}>
        <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.subtle }}>Личные данные</div>
        <style>{`@media(min-width:480px){.profile-grid{grid-template-columns:1fr 1fr!important;}}`}</style>
        <div className="profile-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 12 }}>
          <DarkInput label="Имя" type="text" value={form.firstName} onChange={setF("firstName")} placeholder="Иван" />
          <DarkInput label="Фамилия" type="text" value={form.lastName} onChange={setF("lastName")} placeholder="Иванов" />
        </div>
        <DarkInput label="Телефон" type="tel" value={form.phone} onChange={setF("phone")} placeholder="+7 900 000-00-00" />

        {error && (
          <div style={{ fontSize: 12, color: "#f87171", background: "rgba(239,68,68,0.08)", borderRadius: 10, padding: "10px 14px", border: "1px solid rgba(239,68,68,0.15)" }}>{error}</div>
        )}

        <button type="submit" disabled={saving} style={{
          display: "inline-flex", alignItems: "center", gap: 8, padding: "10px 20px", borderRadius: 12, fontSize: 13, fontWeight: 700, cursor: saving ? "not-allowed" : "pointer",
          background: saved ? "rgba(74,222,128,0.15)" : "linear-gradient(135deg, #7c3aed, #9333ea)",
          color: saved ? "#4ade80" : "#fff",
          border: saved ? "1px solid rgba(74,222,128,0.25)" : "none",
          opacity: saving ? 0.7 : 1, alignSelf: "flex-start", transition: "all 0.2s ease",
        } as React.CSSProperties}>
          {saving ? <Loader2 size={15} style={{ animation: "spin 1s linear infinite" }} /> : saved ? <CheckCircle size={15} /> : <Save size={15} />}
          {saving ? "Сохраняем..." : saved ? "Сохранено" : "Сохранить"}
        </button>
      </form>
      {/* Loyalty */}
      {token && <LoyaltyPanel token={token} />}
    </div>
  );
}

export default function AccountPage() {
  const { customer, token, loading, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const activeTab = location.pathname === "/orders" ? "orders" : "profile";

  useEffect(() => {
    if (!loading && !customer) navigate("/");
  }, [loading, customer, navigate]);

  if (loading) return (
    <div style={{ minHeight: "100vh", background: S.bg, display: "flex", alignItems: "center", justifyContent: "center" }}>
      <Loader2 size={24} style={{ animation: "spin 1s linear infinite", color: S.muted }} />
    </div>
  );

  if (!customer || !token) return null;

  const displayName = [customer.firstName, customer.lastName].filter(Boolean).join(" ") || customer.email;

  return (
    <div style={{ minHeight: "100vh", background: S.bg }}>
      <div style={{ maxWidth: 640, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px)" }}>

        {/* User header */}
        <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 28 }}>
          <div style={{
            width: 48, height: 48, borderRadius: 16, display: "flex", alignItems: "center", justifyContent: "center",
            fontWeight: 800, fontSize: 18, color: "#fff", flexShrink: 0,
            background: "linear-gradient(135deg, #7c3aed, #9333ea)",
            boxShadow: "0 0 24px rgba(201,169,110,0.3)",
          }}>
            {(customer.firstName || customer.email)[0].toUpperCase()}
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontWeight: 700, fontSize: 15, color: S.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{displayName}</div>
            <div style={{ fontSize: 12, color: S.muted, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{customer.email}</div>
          </div>
          <button onClick={() => { logout(); navigate("/"); }} style={{
            display: "flex", alignItems: "center", gap: 6, padding: "8px 14px", borderRadius: 12, fontSize: 12, fontWeight: 600,
            background: "rgba(239,68,68,0.08)", color: "#f87171", border: "1px solid rgba(239,68,68,0.15)", cursor: "pointer", transition: "all 0.15s",
          }}
            onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "rgba(239,68,68,0.15)"; }}
            onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "rgba(239,68,68,0.08)"; }}>
            <LogOut size={13} /> Выйти
          </button>
        </div>

        {/* Tab nav */}
        <div style={{ display: "flex", gap: 4, marginBottom: 20, padding: 4, background: S.surface, borderRadius: 16, border: `1px solid ${S.border}` }}>
          {([
            { to: "/account", key: "profile", icon: User,    label: "Профиль" },
            { to: "/orders",  key: "orders",  icon: Package, label: "Заказы" },
          ] as const).map(({ to, key, icon: Icon, label }) => (
            <Link key={to} to={to} style={{
              flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
              padding: "10px", borderRadius: 12, fontSize: 13, fontWeight: 600, textDecoration: "none",
              background: activeTab === key ? S.surface2 : "transparent",
              color: activeTab === key ? S.text : S.muted,
              border: activeTab === key ? `1px solid ${S.border}` : "1px solid transparent",
              transition: "all 0.15s ease",
            }}>
              <Icon size={15} /> {label}
              {activeTab === key && <ChevronRight size={12} />}
            </Link>
          ))}
        </div>

        {activeTab === "orders" ? <OrdersTab token={token} /> : <ProfileTab />}
      </div>
    </div>
  );
}
