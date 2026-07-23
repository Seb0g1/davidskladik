import { useState, useEffect } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { Package, User, ChevronRight, Loader2, CheckCircle, Clock, Truck, LogOut, Save } from "lucide-react";
import { useAuth } from "../AuthContext";
import { api } from "../api";
import type { ShopOrder } from "../types";
import clsx from "clsx";

const STATUS_MAP: Record<string, { label: string; color: string; icon: typeof Package }> = {
  pending:    { label: "Принят",       color: "#f59e0b", icon: Clock },
  processing: { label: "В обработке",  color: "#3b82f6", icon: Clock },
  picking:    { label: "Собирается",   color: "#8b5cf6", icon: Package },
  shipped:    { label: "Отправлен",    color: "#0ea5e9", icon: Truck },
  delivered:  { label: "Доставлен",    color: "#10b981", icon: CheckCircle },
  cancelled:  { label: "Отменён",      color: "#ef4444", icon: Package },
};

function StatusBadge({ status }: { status: string }) {
  const s = STATUS_MAP[status] || { label: status, color: "#6e6e73", icon: Package };
  const Icon = s.icon;
  return (
    <span className="inline-flex items-center gap-1 text-[11px] font-semibold px-2.5 py-1 rounded-full"
      style={{ background: s.color + "18", color: s.color }}>
      <Icon size={11} strokeWidth={2.5} />
      {s.label}
    </span>
  );
}

function OrderCard({ order }: { order: ShopOrder }) {
  const date = new Date(order.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  const items = order.items || [];
  return (
    <div className="bg-white rounded-2xl overflow-hidden" style={{ border: "1px solid #f0f0f2", boxShadow: "0 1px 4px rgba(0,0,0,0.04)" }}>
      <div className="flex items-start justify-between gap-4 p-5 border-b border-gray-50">
        <div>
          <div className="text-[11px] text-apple-gray mb-1">{date}</div>
          <div className="text-[13px] font-mono text-apple-gray">#{order.id.slice(-8).toUpperCase()}</div>
        </div>
        <div className="text-right flex flex-col items-end gap-2">
          <StatusBadge status={order.status} />
          <div className="text-[15px] font-bold text-apple-black">{order.totalRub.toLocaleString("ru-RU")} ₽</div>
        </div>
      </div>
      {items.length > 0 && (
        <div className="p-4 space-y-2">
          {items.slice(0, 3).map((item, i) => (
            <div key={i} className="flex justify-between text-[12px]">
              <span className="text-apple-gray line-clamp-1 flex-1">{item.name || item.offerId}</span>
              <span className="text-apple-gray ml-3 flex-shrink-0">×{item.quantity}</span>
              <span className="font-medium text-apple-black ml-3 flex-shrink-0">{(item.priceRub * item.quantity).toLocaleString("ru-RU")} ₽</span>
            </div>
          ))}
          {items.length > 3 && (
            <div className="text-[11px] text-apple-gray">+ещё {items.length - 3} товара</div>
          )}
        </div>
      )}
      {order.delivery?.city && (
        <div className="px-5 pb-4 text-[11px] text-apple-gray flex items-center gap-1">
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
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [token]);

  if (loading) return (
    <div className="flex items-center justify-center py-16 gap-2 text-apple-gray text-sm">
      <Loader2 size={18} className="animate-spin" /> Загружаем заказы...
    </div>
  );

  if (error) return (
    <div className="bg-red-50 text-red-600 rounded-2xl px-5 py-4 text-sm">{error}</div>
  );

  if (!orders.length) return (
    <div className="text-center py-16">
      <div className="w-14 h-14 rounded-2xl flex items-center justify-center mx-auto mb-4" style={{ background: "#f5f3ff" }}>
        <Package size={24} style={{ color: "#7c3aed" }} strokeWidth={1.5} />
      </div>
      <div className="text-[15px] font-semibold text-apple-black mb-1">Заказов пока нет</div>
      <div className="text-sm text-apple-gray mb-6">Ваши заказы появятся здесь</div>
      <Link to="/catalog" className="text-sm font-medium text-violet-600 hover:text-violet-700 transition-colors">
        Перейти в каталог
      </Link>
    </div>
  );

  return (
    <div className="space-y-3">
      {orders.map((o) => <OrderCard key={o.id} order={o} />)}
    </div>
  );
}

function ProfileTab() {
  const { customer, updateProfile } = useAuth();
  const [form, setForm] = useState({
    firstName: customer?.firstName || "",
    lastName: customer?.lastName || "",
    phone: customer?.phone || "",
  });
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState("");

  function set(k: keyof typeof form) {
    return (e: React.ChangeEvent<HTMLInputElement>) => {
      setForm((f) => ({ ...f, [k]: e.target.value }));
      setSaved(false);
    };
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
    <div className="space-y-4">
      <div className="bg-white rounded-2xl p-5" style={{ border: "1px solid #f0f0f2", boxShadow: "0 1px 4px rgba(0,0,0,0.04)" }}>
        <h3 className="text-[13px] font-semibold text-apple-gray uppercase tracking-wider mb-4">Email</h3>
        <div className="flex items-center gap-3 px-4 py-3 bg-gray-50 rounded-xl">
          <span className="text-sm text-apple-black">{customer?.email}</span>
          <span className="ml-auto text-[11px] text-apple-gray bg-white px-2 py-0.5 rounded-full border border-gray-100">подтверждён</span>
        </div>
      </div>

      <form onSubmit={handleSave} className="bg-white rounded-2xl p-5 space-y-4" style={{ border: "1px solid #f0f0f2", boxShadow: "0 1px 4px rgba(0,0,0,0.04)" }}>
        <h3 className="text-[13px] font-semibold text-apple-gray uppercase tracking-wider">Личные данные</h3>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div>
            <label className="text-[11px] font-semibold text-apple-gray uppercase tracking-wider block mb-1.5">Имя</label>
            <input
              type="text"
              value={form.firstName}
              onChange={set("firstName")}
              placeholder="Иван"
              className="w-full px-4 py-3 bg-gray-50 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all"
              style={{ border: "1px solid #e8e8ec" }}
            />
          </div>
          <div>
            <label className="text-[11px] font-semibold text-apple-gray uppercase tracking-wider block mb-1.5">Фамилия</label>
            <input
              type="text"
              value={form.lastName}
              onChange={set("lastName")}
              placeholder="Иванов"
              className="w-full px-4 py-3 bg-gray-50 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all"
              style={{ border: "1px solid #e8e8ec" }}
            />
          </div>
        </div>

        <div>
          <label className="text-[11px] font-semibold text-apple-gray uppercase tracking-wider block mb-1.5">Телефон</label>
          <input
            type="tel"
            value={form.phone}
            onChange={set("phone")}
            placeholder="+7 900 000-00-00"
            className="w-full px-4 py-3 bg-gray-50 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all"
            style={{ border: "1px solid #e8e8ec" }}
          />
        </div>

        {error && <div className="text-sm text-red-600 bg-red-50 rounded-xl px-4 py-3">{error}</div>}

        <button
          type="submit"
          disabled={saving}
          className={clsx(
            "flex items-center gap-2 px-5 py-2.5 rounded-xl text-sm font-semibold transition-all duration-200",
            saved
              ? "bg-emerald-500 text-white"
              : "text-white disabled:opacity-60"
          )}
          style={saved ? {} : { background: "linear-gradient(135deg, #7c3aed, #9333ea)" }}
        >
          {saving ? <Loader2 size={15} className="animate-spin" /> : saved ? <CheckCircle size={15} /> : <Save size={15} />}
          {saving ? "Сохраняем..." : saved ? "Сохранено" : "Сохранить"}
        </button>
      </form>
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
    <div className="min-h-screen bg-gray-50 flex items-center justify-center">
      <Loader2 size={24} className="animate-spin text-apple-gray" />
    </div>
  );

  if (!customer || !token) return null;

  const displayName = [customer.firstName, customer.lastName].filter(Boolean).join(" ") || customer.email;

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-2xl mx-auto px-4 sm:px-6 py-8">

        {/* User header */}
        <div className="flex items-center gap-4 mb-6">
          <div className="w-12 h-12 rounded-2xl flex items-center justify-center font-bold text-lg text-white flex-shrink-0"
            style={{ background: "linear-gradient(135deg, #7c3aed, #9333ea)" }}>
            {(customer.firstName || customer.email)[0].toUpperCase()}
          </div>
          <div className="flex-1 min-w-0">
            <div className="font-bold text-apple-black text-[15px] truncate">{displayName}</div>
            <div className="text-xs text-apple-gray truncate">{customer.email}</div>
          </div>
          <button onClick={() => { logout(); navigate("/"); }}
            className="flex items-center gap-1.5 text-[13px] text-apple-gray hover:text-red-500 transition-colors px-3 py-2 rounded-xl hover:bg-red-50">
            <LogOut size={15} /> Выйти
          </button>
        </div>

        {/* Tabs */}
        <div className="flex gap-1 bg-white rounded-2xl p-1 mb-5" style={{ border: "1px solid #f0f0f2" }}>
          {([
            { key: "orders", label: "Мои заказы", icon: Package, to: "/orders" },
            { key: "profile", label: "Профиль", icon: User, to: "/account" },
          ] as const).map((tab) => {
            const Icon = tab.icon;
            return (
              <Link key={tab.key} to={tab.to} replace
                className={clsx(
                  "flex-1 flex items-center justify-center gap-2 py-2.5 rounded-xl text-[13px] font-semibold transition-all duration-200",
                  activeTab === tab.key
                    ? "bg-violet-600 text-white shadow-sm"
                    : "text-apple-gray hover:text-apple-black"
                )}>
                <Icon size={15} />
                {tab.label}
              </Link>
            );
          })}
        </div>

        {/* Content */}
        {activeTab === "orders" ? <OrdersTab token={token} /> : <ProfileTab />}

        {/* Back to shop */}
        <div className="mt-6 text-center">
          <Link to="/" className="inline-flex items-center gap-1.5 text-sm text-apple-gray hover:text-apple-black transition-colors">
            <ChevronRight size={14} className="rotate-180" />
            Вернуться в магазин
          </Link>
        </div>
      </div>
    </div>
  );
}
