import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  ExternalLink, ShoppingBag, Users, TrendingUp, Package,
  Plus, Trash2, Edit2, Save, X, Loader2, Image as ImageIcon,
  ChevronLeft, ChevronRight, Check, AlertCircle, RefreshCw,
  LayoutDashboard, Settings, Tag, Image, ClipboardList, UserCheck,
} from "lucide-react";

// ── Types ────────────────────────────────────────────────────────────────────

interface ShopBanner {
  id: string; imageUrl: string; title?: string; subtitle?: string;
  linkUrl?: string; linkText?: string; active: boolean; order: number;
}
interface ShopCategory {
  id: string; name: string; slug: string; imageUrl?: string;
  order: number; filterTag?: string;
}
interface ShopSettings {
  markup: number; shopName: string; shopDescription: string;
  contactEmail?: string; contactPhone?: string; deliveryDays?: number; freeDeliveryFrom?: number;
}
interface ShopCustomer {
  id: string; email: string; firstName?: string; lastName?: string;
  phone?: string; createdAt: string; _count: { orders: number };
}
interface ShopOrder {
  id: string; status: string; totalRub: number; items: unknown[];
  delivery: { firstName?: string; lastName?: string; phone?: string; email?: string; city?: string };
  comment?: string; createdAt: string;
  customer?: { id: string; email: string; firstName?: string; lastName?: string } | null;
}
interface Stats {
  totalOrders: number; totalCustomers: number; todayOrders: number;
  weekOrders: number; totalRevenue: number; weekRevenue: number;
}

// ── API ──────────────────────────────────────────────────────────────────────

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { headers: { "Content-Type": "application/json" }, ...init });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
  return res.json() as Promise<T>;
}

const SHOP_URL = import.meta.env.VITE_SHOP_URL || "https://magicvibes.ru";

// ── Helpers ──────────────────────────────────────────────────────────────────

const STATUS_LABELS: Record<string, string> = {
  pending: "Новый", confirmed: "Подтверждён", picking: "Комплектация",
  shipped: "Отправлен", delivered: "Доставлен", cancelled: "Отменён",
};
const STATUS_COLORS: Record<string, string> = {
  pending: "bg-amber-100 text-amber-700",
  confirmed: "bg-blue-100 text-blue-700",
  picking: "bg-indigo-100 text-indigo-700",
  shipped: "bg-violet-100 text-violet-700",
  delivered: "bg-green-100 text-green-700",
  cancelled: "bg-red-100 text-red-600",
};

function fmt(n: number) { return n.toLocaleString("ru-RU"); }
function fmtDate(s: string) {
  const d = new Date(s);
  return d.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" });
}
function customerName(c: { firstName?: string; lastName?: string; email: string } | null | undefined, delivery?: ShopOrder["delivery"]) {
  const from = c || delivery;
  if (!from) return "—";
  const name = [from.firstName, (from as { lastName?: string }).lastName].filter(Boolean).join(" ");
  return name || (from as { email?: string }).email || (delivery?.phone ?? "—");
}

// ── Field input helper ────────────────────────────────────────────────────────

function Field({ label, value, onChange, placeholder, type = "text" }: {
  label: string; value: string | number; onChange: (v: string) => void;
  placeholder?: string; type?: string;
}) {
  return (
    <div>
      <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">{label}</label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full px-3.5 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 focus:border-transparent transition-all bg-gray-50 focus:bg-white"
      />
    </div>
  );
}

// ── Stat Card ─────────────────────────────────────────────────────────────────

function StatCard({ label, value, sub, icon: Icon, color }: {
  label: string; value: string | number; sub?: string;
  icon: React.ComponentType<{ size?: number; className?: string }>; color: string;
}) {
  return (
    <div className="bg-white rounded-2xl p-5 border border-gray-100 flex items-start gap-4">
      <div className={`w-12 h-12 rounded-2xl flex items-center justify-center flex-shrink-0 ${color}`}>
        <Icon size={22} className="text-white" />
      </div>
      <div>
        <div className="text-2xl font-bold text-gray-900 tracking-tight">{value}</div>
        <div className="text-sm text-gray-500 mt-0.5">{label}</div>
        {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
      </div>
    </div>
  );
}

// ── Pagination ────────────────────────────────────────────────────────────────

function Pagination({ page, total, pageSize, onChange }: { page: number; total: number; pageSize: number; onChange: (p: number) => void }) {
  const totalPages = Math.ceil(total / pageSize);
  if (totalPages <= 1) return null;
  return (
    <div className="flex items-center justify-between pt-4">
      <span className="text-sm text-gray-500">{fmt(total)} записей</span>
      <div className="flex items-center gap-1">
        <button onClick={() => onChange(page - 1)} disabled={page <= 1} className="w-8 h-8 flex items-center justify-center rounded-lg border border-gray-200 disabled:opacity-40 hover:border-violet-400 transition-colors">
          <ChevronLeft size={15} />
        </button>
        <span className="px-3 text-sm font-medium">{page} / {totalPages}</span>
        <button onClick={() => onChange(page + 1)} disabled={page >= totalPages} className="w-8 h-8 flex items-center justify-center rounded-lg border border-gray-200 disabled:opacity-40 hover:border-violet-400 transition-colors">
          <ChevronRight size={15} />
        </button>
      </div>
    </div>
  );
}

// ── Dashboard ─────────────────────────────────────────────────────────────────

function DashboardTab() {
  const { data: stats, isLoading, refetch } = useQuery<Stats>({
    queryKey: ["shop-admin-stats"],
    queryFn: () => apiFetch<Stats>("/api/shop/admin/stats"),
    refetchInterval: 60_000,
  });
  const { data: ordersData } = useQuery<{ orders: ShopOrder[]; total: number }>({
    queryKey: ["shop-admin-orders", { page: 1 }],
    queryFn: () => apiFetch<{ orders: ShopOrder[]; total: number }>("/api/shop/admin/orders?pageSize=5"),
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-gray-900">Обзор магазина</h2>
        <button onClick={() => refetch()} className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-violet-600 transition-colors">
          <RefreshCw size={14} /> Обновить
        </button>
      </div>

      {isLoading ? (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => <div key={i} className="h-24 bg-gray-100 rounded-2xl animate-pulse" />)}
        </div>
      ) : (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard label="Заказов всего" value={fmt(stats?.totalOrders ?? 0)} sub={`Сегодня: ${stats?.todayOrders ?? 0}`} icon={ShoppingBag} color="bg-violet-500" />
          <StatCard label="Выручка, ₽" value={fmt(stats?.totalRevenue ?? 0)} sub={`За неделю: ${fmt(stats?.weekRevenue ?? 0)} ₽`} icon={TrendingUp} color="bg-emerald-500" />
          <StatCard label="За неделю" value={stats?.weekOrders ?? 0} sub="заказов" icon={Package} color="bg-blue-500" />
          <StatCard label="Покупатели" value={fmt(stats?.totalCustomers ?? 0)} sub="зарегистрировано" icon={Users} color="bg-amber-500" />
        </div>
      )}

      {/* Recent orders */}
      <div>
        <h3 className="font-semibold text-gray-800 mb-3">Последние заказы</h3>
        {ordersData?.orders.length === 0 ? (
          <div className="text-center py-10 text-gray-400 border-2 border-dashed border-gray-200 rounded-2xl text-sm">Заказов пока нет</div>
        ) : (
          <div className="bg-white rounded-2xl border border-gray-100 overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-100">
                <tr>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Заказ</th>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden sm:table-cell">Покупатель</th>
                  <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Сумма</th>
                  <th className="text-center px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Статус</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {ordersData?.orders.map((o) => (
                  <tr key={o.id} className="hover:bg-gray-50/50 transition-colors">
                    <td className="px-4 py-3">
                      <div className="font-mono text-xs font-bold text-gray-800">{o.id}</div>
                      <div className="text-xs text-gray-400 mt-0.5">{fmtDate(o.createdAt)}</div>
                    </td>
                    <td className="px-4 py-3 hidden sm:table-cell text-gray-700">{customerName(o.customer, o.delivery)}</td>
                    <td className="px-4 py-3 text-right font-semibold text-gray-900">{fmt(o.totalRub)} ₽</td>
                    <td className="px-4 py-3 text-center">
                      <span className={`inline-block px-2.5 py-1 rounded-full text-xs font-medium ${STATUS_COLORS[o.status] ?? "bg-gray-100 text-gray-600"}`}>
                        {STATUS_LABELS[o.status] ?? o.status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Orders ────────────────────────────────────────────────────────────────────

function OrdersTab() {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);

  const { data, isLoading } = useQuery<{ orders: ShopOrder[]; total: number }>({
    queryKey: ["shop-admin-orders", { page, statusFilter }],
    queryFn: () => apiFetch<{ orders: ShopOrder[]; total: number }>(
      `/api/shop/admin/orders?page=${page}&pageSize=20${statusFilter ? `&status=${statusFilter}` : ""}`
    ),
  });

  const updateStatus = useMutation({
    mutationFn: ({ id, status }: { id: string; status: string }) =>
      apiFetch(`/api/shop/admin/orders/${id}`, { method: "PATCH", body: JSON.stringify({ status }) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shop-admin-orders"] }),
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h2 className="text-lg font-bold text-gray-900">Заказы</h2>
        <div className="flex gap-1.5 flex-wrap">
          {["", ...Object.keys(STATUS_LABELS)].map((s) => (
            <button
              key={s}
              onClick={() => { setStatusFilter(s); setPage(1); }}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${statusFilter === s
                ? "bg-violet-600 text-white"
                : "bg-gray-100 text-gray-600 hover:bg-gray-200"
              }`}
            >
              {s ? STATUS_LABELS[s] : "Все"}
            </button>
          ))}
        </div>
      </div>

      {isLoading ? (
        <div className="space-y-2">
          {[...Array(5)].map((_, i) => <div key={i} className="h-16 bg-gray-100 rounded-xl animate-pulse" />)}
        </div>
      ) : !data?.orders.length ? (
        <div className="text-center py-16 text-gray-400 border-2 border-dashed border-gray-200 rounded-2xl text-sm">Заказов нет</div>
      ) : (
        <div className="bg-white rounded-2xl border border-gray-100 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Заказ / Дата</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden md:table-cell">Покупатель</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden sm:table-cell">Товары</th>
                <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Сумма</th>
                <th className="text-center px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Статус</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {data.orders.map((o) => (
                <>
                  <tr
                    key={o.id}
                    className="hover:bg-gray-50/60 transition-colors cursor-pointer"
                    onClick={() => setExpanded(expanded === o.id ? null : o.id)}
                  >
                    <td className="px-4 py-3">
                      <div className="font-mono text-xs font-bold text-gray-800">{o.id}</div>
                      <div className="text-xs text-gray-400 mt-0.5">{fmtDate(o.createdAt)}</div>
                    </td>
                    <td className="px-4 py-3 hidden md:table-cell">
                      <div className="text-gray-800">{customerName(o.customer, o.delivery)}</div>
                      {o.delivery?.city && <div className="text-xs text-gray-400">{o.delivery.city}</div>}
                    </td>
                    <td className="px-4 py-3 hidden sm:table-cell text-gray-600 text-xs">
                      {Array.isArray(o.items) ? `${o.items.length} поз.` : "—"}
                    </td>
                    <td className="px-4 py-3 text-right font-semibold text-gray-900">{fmt(o.totalRub)} ₽</td>
                    <td className="px-4 py-3 text-center">
                      <span className={`inline-block px-2.5 py-1 rounded-full text-xs font-medium ${STATUS_COLORS[o.status] ?? "bg-gray-100 text-gray-600"}`}>
                        {STATUS_LABELS[o.status] ?? o.status}
                      </span>
                    </td>
                  </tr>
                  {expanded === o.id && (
                    <tr key={`${o.id}-exp`}>
                      <td colSpan={5} className="px-4 pb-4 bg-gray-50/80">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-3">
                          {/* Delivery info */}
                          <div className="text-sm space-y-1">
                            <div className="font-semibold text-gray-700 mb-2">Доставка</div>
                            {o.delivery && Object.entries(o.delivery).map(([k, v]) => v ? (
                              <div key={k} className="flex gap-2">
                                <span className="text-gray-400 w-24 flex-shrink-0 capitalize">{k}</span>
                                <span className="text-gray-800">{String(v)}</span>
                              </div>
                            ) : null)}
                            {o.comment && <div className="mt-2 text-gray-600 italic">Комментарий: {o.comment}</div>}
                          </div>
                          {/* Items */}
                          <div className="text-sm">
                            <div className="font-semibold text-gray-700 mb-2">Позиции</div>
                            <div className="space-y-1">
                              {Array.isArray(o.items) && o.items.map((item, idx) => {
                                const it = item as { name?: string; quantity?: number; priceRub?: number };
                                return (
                                  <div key={idx} className="flex justify-between gap-2">
                                    <span className="text-gray-700 truncate">{it.name || `Товар ${idx + 1}`} × {it.quantity ?? 1}</span>
                                    <span className="text-gray-900 font-medium flex-shrink-0">{fmt((it.priceRub ?? 0) * (it.quantity ?? 1))} ₽</span>
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        </div>
                        {/* Status change */}
                        <div className="flex flex-wrap gap-1.5 mt-4 pt-3 border-t border-gray-200">
                          <span className="text-xs text-gray-500 mr-1 self-center">Изменить статус:</span>
                          {Object.entries(STATUS_LABELS).map(([s, label]) => (
                            <button
                              key={s}
                              disabled={o.status === s || updateStatus.isPending}
                              onClick={(e) => { e.stopPropagation(); updateStatus.mutate({ id: o.id, status: s }); }}
                              className={`flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-medium transition-colors border ${
                                o.status === s
                                  ? "border-violet-300 bg-violet-50 text-violet-700"
                                  : "border-gray-200 hover:border-violet-300 text-gray-600 hover:text-violet-700"
                              } disabled:opacity-60`}
                            >
                              {o.status === s && <Check size={11} />}
                              {label}
                            </button>
                          ))}
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
          <div className="px-4 pb-4">
            <Pagination page={page} total={data.total} pageSize={20} onChange={setPage} />
          </div>
        </div>
      )}
    </div>
  );
}

// ── Customers ─────────────────────────────────────────────────────────────────

function CustomersTab() {
  const [page, setPage] = useState(1);
  const { data, isLoading } = useQuery<{ customers: ShopCustomer[]; total: number }>({
    queryKey: ["shop-admin-customers", page],
    queryFn: () => apiFetch<{ customers: ShopCustomer[]; total: number }>(`/api/shop/admin/customers?page=${page}&pageSize=20`),
  });

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-bold text-gray-900">Покупатели</h2>
      {isLoading ? (
        <div className="space-y-2">{[...Array(5)].map((_, i) => <div key={i} className="h-14 bg-gray-100 rounded-xl animate-pulse" />)}</div>
      ) : !data?.customers.length ? (
        <div className="text-center py-16 text-gray-400 border-2 border-dashed border-gray-200 rounded-2xl text-sm">Зарегистрированных покупателей нет</div>
      ) : (
        <div className="bg-white rounded-2xl border border-gray-100 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Покупатель</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden sm:table-cell">Email</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden md:table-cell">Телефон</th>
                <th className="text-center px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Заказов</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden lg:table-cell">Регистрация</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {data.customers.map((c) => (
                <tr key={c.id} className="hover:bg-gray-50/50 transition-colors">
                  <td className="px-4 py-3">
                    <div className="font-medium text-gray-900">{[c.firstName, c.lastName].filter(Boolean).join(" ") || "—"}</div>
                    <div className="text-xs text-gray-400 sm:hidden">{c.email}</div>
                  </td>
                  <td className="px-4 py-3 text-gray-600 hidden sm:table-cell">{c.email}</td>
                  <td className="px-4 py-3 text-gray-600 hidden md:table-cell">{c.phone || "—"}</td>
                  <td className="px-4 py-3 text-center">
                    <span className="inline-block w-8 h-8 rounded-full bg-violet-100 text-violet-700 text-xs font-bold leading-8 text-center">
                      {c._count.orders}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-gray-400 text-xs hidden lg:table-cell">{fmtDate(c.createdAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="px-4 pb-4">
            <Pagination page={page} total={data.total} pageSize={20} onChange={setPage} />
          </div>
        </div>
      )}
    </div>
  );
}

// ── Banners ───────────────────────────────────────────────────────────────────

function BannerForm({ banner, onSave, onCancel }: {
  banner?: Partial<ShopBanner>; onSave: (d: Partial<ShopBanner>) => void; onCancel: () => void;
}) {
  const [form, setForm] = useState<Partial<ShopBanner>>({
    imageUrl: "", title: "", subtitle: "", linkUrl: "", linkText: "", active: true, ...banner,
  });
  const set = (k: keyof ShopBanner) => (v: string | boolean) => setForm((f) => ({ ...f, [k]: v }));

  return (
    <div className="bg-violet-50 border border-violet-100 rounded-2xl p-5 space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <Field label="URL изображения" value={form.imageUrl ?? ""} onChange={set("imageUrl") as (v: string) => void} placeholder="https://..." />
        <Field label="Заголовок" value={form.title ?? ""} onChange={set("title") as (v: string) => void} placeholder="Летняя коллекция" />
        <Field label="Подзаголовок" value={form.subtitle ?? ""} onChange={set("subtitle") as (v: string) => void} placeholder="Скидки до 50%" />
        <Field label="Ссылка" value={form.linkUrl ?? ""} onChange={set("linkUrl") as (v: string) => void} placeholder="/catalog/sale" />
        <Field label="Текст кнопки" value={form.linkText ?? ""} onChange={set("linkText") as (v: string) => void} placeholder="Смотреть акции" />
        <div className="flex items-center gap-3 pt-5">
          <button
            type="button"
            onClick={() => set("active")(!form.active)}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${form.active ? "bg-violet-600" : "bg-gray-300"}`}
          >
            <span className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${form.active ? "translate-x-6" : "translate-x-1"}`} />
          </button>
          <span className="text-sm text-gray-700">Активен (показывать)</span>
        </div>
      </div>
      {form.imageUrl && (
        <div className="rounded-xl overflow-hidden h-32 bg-gray-100">
          <img src={form.imageUrl} alt="" className="w-full h-full object-cover" onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }} />
        </div>
      )}
      <div className="flex gap-2">
        <button onClick={() => onSave(form)} className="flex items-center gap-2 bg-violet-600 text-white px-5 py-2.5 rounded-xl text-sm font-medium hover:bg-violet-700 transition-colors">
          <Save size={15} /> Сохранить
        </button>
        <button onClick={onCancel} className="flex items-center gap-2 border border-gray-200 px-5 py-2.5 rounded-xl text-sm text-gray-600 hover:bg-gray-50 transition-colors">
          <X size={15} /> Отмена
        </button>
      </div>
    </div>
  );
}

function BannersTab() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<string | "new" | null>(null);

  const { data: banners = [], isLoading } = useQuery<ShopBanner[]>({
    queryKey: ["shop-admin-banners"],
    queryFn: () => apiFetch<ShopBanner[]>("/api/shop/admin/banners"),
  });

  const saveMut = useMutation({
    mutationFn: (d: Partial<ShopBanner> & { id?: string }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/banners${d.id ? `/${d.id}` : ""}`, {
        method: d.id ? "PUT" : "POST", body: JSON.stringify(d),
      }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["shop-admin-banners"] }); setEditing(null); },
  });
  const deleteMut = useMutation({
    mutationFn: (id: string) => apiFetch<{ ok: boolean }>(`/api/shop/admin/banners/${id}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shop-admin-banners"] }),
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-gray-900">Баннеры главной страницы</h2>
        <button onClick={() => setEditing("new")} className="flex items-center gap-2 bg-violet-600 text-white px-4 py-2.5 rounded-xl text-sm font-medium hover:bg-violet-700 transition-colors">
          <Plus size={16} /> Добавить баннер
        </button>
      </div>

      {editing === "new" && (
        <BannerForm onSave={(d) => saveMut.mutate(d)} onCancel={() => setEditing(null)} />
      )}

      {isLoading ? (
        <div className="space-y-2">{[...Array(3)].map((_, i) => <div key={i} className="h-16 bg-gray-100 rounded-xl animate-pulse" />)}</div>
      ) : banners.length === 0 && editing !== "new" ? (
        <div className="text-center py-12 border-2 border-dashed border-gray-200 rounded-2xl">
          <ImageIcon size={32} className="mx-auto mb-3 text-gray-300" />
          <p className="text-sm text-gray-400">Баннеры не добавлены. Без них на главной показывается градиентный фон.</p>
        </div>
      ) : (
        <div className="space-y-2">
          {banners.map((b) => (
            <div key={b.id} className="bg-white rounded-2xl border border-gray-100 overflow-hidden">
              <div className="flex items-center gap-3 p-4">
                <div className="w-20 h-12 rounded-xl bg-gray-100 flex-shrink-0 overflow-hidden">
                  {b.imageUrl
                    ? <img src={b.imageUrl} alt="" className="w-full h-full object-cover" />
                    : <div className="w-full h-full flex items-center justify-center text-gray-300"><ImageIcon size={16} /></div>
                  }
                </div>
                <div className="flex-1 min-w-0">
                  <div className="font-semibold text-sm text-gray-800">{b.title || "(без названия)"}</div>
                  <div className="text-xs text-gray-400 truncate mt-0.5">{b.subtitle || b.linkUrl || ""}</div>
                </div>
                <span className={`text-xs px-2.5 py-1 rounded-full font-medium flex-shrink-0 ${b.active ? "bg-green-100 text-green-700" : "bg-gray-100 text-gray-500"}`}>
                  {b.active ? "Активен" : "Скрыт"}
                </span>
                <button onClick={() => setEditing(editing === b.id ? null : b.id)} className="p-2 rounded-xl hover:bg-gray-100 text-gray-400 hover:text-violet-600 transition-colors flex-shrink-0">
                  <Edit2 size={15} />
                </button>
                <button onClick={() => { if (confirm("Удалить баннер?")) deleteMut.mutate(b.id); }} className="p-2 rounded-xl hover:bg-red-50 text-gray-400 hover:text-red-500 transition-colors flex-shrink-0">
                  <Trash2 size={15} />
                </button>
              </div>
              {editing === b.id && (
                <div className="px-4 pb-4">
                  <BannerForm banner={b} onSave={(d) => saveMut.mutate({ ...d, id: b.id })} onCancel={() => setEditing(null)} />
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Categories ────────────────────────────────────────────────────────────────

function CategoriesTab() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<string | "new" | null>(null);
  const [form, setForm] = useState<Partial<ShopCategory>>({});

  const { data: cats = [], isLoading } = useQuery<ShopCategory[]>({
    queryKey: ["shop-admin-categories"],
    queryFn: () => apiFetch<ShopCategory[]>("/api/shop/admin/categories"),
  });

  const saveMut = useMutation({
    mutationFn: (d: Partial<ShopCategory> & { id?: string }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/categories${d.id ? `/${d.id}` : ""}`, {
        method: d.id ? "PUT" : "POST", body: JSON.stringify(d),
      }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["shop-admin-categories"] }); setEditing(null); setForm({}); },
  });
  const deleteMut = useMutation({
    mutationFn: (id: string) => apiFetch<{ ok: boolean }>(`/api/shop/admin/categories/${id}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shop-admin-categories"] }),
  });

  function openNew() { setForm({ name: "", slug: "", imageUrl: "", filterTag: "" }); setEditing("new"); }
  function openEdit(c: ShopCategory) { setForm({ ...c }); setEditing(c.id); }

  const setF = (k: keyof ShopCategory) => (v: string) => setForm((f) => ({ ...f, [k]: v }));

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-gray-900">Категории каталога</h2>
        <button onClick={openNew} className="flex items-center gap-2 bg-violet-600 text-white px-4 py-2.5 rounded-xl text-sm font-medium hover:bg-violet-700 transition-colors">
          <Plus size={16} /> Добавить категорию
        </button>
      </div>

      {(editing === "new" || (editing && cats.find((c) => c.id === editing))) && (
        <div className="bg-violet-50 border border-violet-100 rounded-2xl p-5 space-y-4">
          <h3 className="font-semibold text-gray-800">{editing === "new" ? "Новая категория" : "Редактировать категорию"}</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <Field label="Название" value={form.name ?? ""} onChange={setF("name")} placeholder="Парфюмерия" />
            <Field label="Slug (URL)" value={form.slug ?? ""} onChange={setF("slug")} placeholder="parfumery" />
            <Field label="URL изображения" value={form.imageUrl ?? ""} onChange={setF("imageUrl")} placeholder="https://..." />
            <Field label="Тег фильтрации" value={form.filterTag ?? ""} onChange={setF("filterTag")} placeholder="parfum" />
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => saveMut.mutate(editing === "new" ? form : { ...form, id: editing })}
              disabled={saveMut.isPending}
              className="flex items-center gap-2 bg-violet-600 text-white px-5 py-2.5 rounded-xl text-sm font-medium hover:bg-violet-700 disabled:opacity-50 transition-colors"
            >
              {saveMut.isPending ? <Loader2 size={15} className="animate-spin" /> : <Save size={15} />} Сохранить
            </button>
            <button onClick={() => { setEditing(null); setForm({}); }} className="flex items-center gap-2 border border-gray-200 px-5 py-2.5 rounded-xl text-sm text-gray-600 hover:bg-gray-50 transition-colors">
              <X size={15} /> Отмена
            </button>
          </div>
        </div>
      )}

      {isLoading ? (
        <div className="space-y-2">{[...Array(3)].map((_, i) => <div key={i} className="h-14 bg-gray-100 rounded-xl animate-pulse" />)}</div>
      ) : cats.length === 0 && !editing ? (
        <div className="text-center py-12 border-2 border-dashed border-gray-200 rounded-2xl text-sm text-gray-400">Категории не добавлены</div>
      ) : (
        <div className="bg-white rounded-2xl border border-gray-100 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">Категория</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden sm:table-cell">Slug</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider hidden md:table-cell">Тег</th>
                <th className="px-4 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {cats.map((c) => (
                <tr key={c.id} className="hover:bg-gray-50/50 transition-colors">
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-3">
                      {c.imageUrl && <img src={c.imageUrl} alt="" className="w-8 h-8 rounded-lg object-cover flex-shrink-0" onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }} />}
                      <span className="font-medium text-gray-800">{c.name}</span>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs font-mono hidden sm:table-cell">{c.slug}</td>
                  <td className="px-4 py-3 text-gray-400 text-xs hidden md:table-cell">{c.filterTag || "—"}</td>
                  <td className="px-4 py-3 text-right">
                    <div className="flex justify-end gap-1">
                      <button onClick={() => openEdit(c)} className="p-2 rounded-xl hover:bg-gray-100 text-gray-400 hover:text-violet-600 transition-colors"><Edit2 size={14} /></button>
                      <button onClick={() => { if (confirm("Удалить категорию?")) deleteMut.mutate(c.id); }} className="p-2 rounded-xl hover:bg-red-50 text-gray-400 hover:text-red-500 transition-colors"><Trash2 size={14} /></button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Settings ──────────────────────────────────────────────────────────────────

function SettingsTab() {
  const qc = useQueryClient();
  const { data: settings, isLoading } = useQuery<ShopSettings>({
    queryKey: ["shop-admin-settings"],
    queryFn: () => apiFetch<ShopSettings>("/api/shop/admin/settings"),
  });
  const [form, setForm] = useState<Partial<ShopSettings>>({});
  const [saved, setSaved] = useState(false);

  useEffect(() => { if (settings) setForm(settings); }, [settings]);

  const saveMut = useMutation({
    mutationFn: (d: Partial<ShopSettings>) => apiFetch<{ ok: boolean; settings: ShopSettings }>("/api/shop/admin/settings", {
      method: "PATCH", body: JSON.stringify(d),
    }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["shop-admin-settings"] }); setSaved(true); setTimeout(() => setSaved(false), 3000); },
  });

  const setF = (k: keyof ShopSettings) => (v: string) => setForm((f) => ({ ...f, [k]: k === "markup" || k === "deliveryDays" || k === "freeDeliveryFrom" ? Number(v) : v }));

  if (isLoading) return <div className="flex justify-center py-16"><Loader2 className="animate-spin text-violet-600" size={28} /></div>;

  return (
    <div className="space-y-6 max-w-2xl">
      <h2 className="text-lg font-bold text-gray-900">Настройки магазина</h2>

      {/* Basic */}
      <div className="bg-white rounded-2xl border border-gray-100 p-6 space-y-4">
        <h3 className="font-semibold text-gray-800">Основное</h3>
        <Field label="Название магазина" value={form.shopName ?? ""} onChange={setF("shopName")} placeholder="Magic Vibes" />
        <div>
          <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Описание</label>
          <textarea
            rows={3}
            value={form.shopDescription ?? ""}
            onChange={(e) => setF("shopDescription")(e.target.value)}
            className="w-full px-3.5 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 focus:border-transparent transition-all bg-gray-50 focus:bg-white resize-none"
          />
        </div>
        <div className="grid grid-cols-2 gap-4">
          <Field label="Email для связи" value={form.contactEmail ?? ""} onChange={setF("contactEmail")} placeholder="info@magicvibes.ru" />
          <Field label="Телефон" value={form.contactPhone ?? ""} onChange={setF("contactPhone")} placeholder="+7 800 ..." />
        </div>
      </div>

      {/* Pricing & delivery */}
      <div className="bg-white rounded-2xl border border-gray-100 p-6 space-y-4">
        <h3 className="font-semibold text-gray-800">Цены и доставка</h3>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Коэффициент наценки</label>
            <div className="flex items-center gap-2">
              <input
                type="number" step="0.05" min="0.5" max="20"
                value={form.markup ?? 2.2}
                onChange={(e) => setF("markup")(e.target.value)}
                className="w-28 px-3.5 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 focus:border-transparent transition-all bg-gray-50 focus:bg-white"
              />
              <span className="text-xs text-gray-500">× (USD × курс)</span>
            </div>
            <p className="text-xs text-gray-400 mt-1">Итоговая цена = цена PM (USD) × курс USD/RUB × {form.markup ?? 2.2}</p>
          </div>
          <div>
            <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Бесплатная доставка от (₽)</label>
            <input
              type="number" min="0"
              value={form.freeDeliveryFrom ?? 3000}
              onChange={(e) => setF("freeDeliveryFrom")(e.target.value)}
              className="w-full px-3.5 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 focus:border-transparent transition-all bg-gray-50 focus:bg-white"
            />
          </div>
          <div>
            <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Срок доставки (дней)</label>
            <input
              type="number" min="1" max="30"
              value={form.deliveryDays ?? 3}
              onChange={(e) => setF("deliveryDays")(e.target.value)}
              className="w-full px-3.5 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 focus:border-transparent transition-all bg-gray-50 focus:bg-white"
            />
          </div>
        </div>
      </div>

      {saved && (
        <div className="flex items-center gap-2 text-emerald-600 bg-emerald-50 px-4 py-3 rounded-xl text-sm font-medium">
          <Check size={16} /> Настройки сохранены
        </div>
      )}
      {saveMut.isError && (
        <div className="flex items-center gap-2 text-red-600 bg-red-50 px-4 py-3 rounded-xl text-sm">
          <AlertCircle size={16} /> Ошибка сохранения
        </div>
      )}

      <button
        onClick={() => saveMut.mutate(form)}
        disabled={saveMut.isPending}
        className="flex items-center gap-2 bg-violet-600 text-white px-6 py-3 rounded-xl text-sm font-semibold hover:bg-violet-700 disabled:opacity-50 transition-colors shadow-md shadow-violet-100"
      >
        {saveMut.isPending ? <Loader2 size={16} className="animate-spin" /> : <Save size={16} />}
        Сохранить настройки
      </button>
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

type Tab = "dashboard" | "orders" | "customers" | "banners" | "categories" | "settings";

const TABS: { id: Tab; label: string; icon: React.ComponentType<{ size?: number }> }[] = [
  { id: "dashboard", label: "Обзор", icon: LayoutDashboard },
  { id: "orders", label: "Заказы", icon: ClipboardList },
  { id: "customers", label: "Покупатели", icon: UserCheck },
  { id: "banners", label: "Баннеры", icon: Image },
  { id: "categories", label: "Категории", icon: Tag },
  { id: "settings", label: "Настройки", icon: Settings },
];

export default function ShopAdminPage() {
  const [tab, setTab] = useState<Tab>("dashboard");

  return (
    <div className="min-h-screen bg-gray-50/60">
      <div className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 tracking-tight">Magic Vibes — Магазин</h1>
            <p className="text-sm text-gray-500 mt-0.5">Управление витриной, заказами и настройками</p>
          </div>
          <a
            href={SHOP_URL} target="_blank" rel="noreferrer"
            className="flex items-center gap-2 bg-white border border-gray-200 hover:border-violet-300 text-gray-700 hover:text-violet-700 px-4 py-2.5 rounded-xl text-sm font-medium transition-all shadow-sm"
          >
            <ExternalLink size={15} /> Открыть магазин
          </a>
        </div>

        {/* Tab bar */}
        <div className="bg-white border border-gray-100 rounded-2xl p-1 flex gap-0.5 flex-wrap shadow-sm">
          {TABS.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setTab(id)}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-xl text-sm font-medium transition-all ${
                tab === id
                  ? "bg-violet-600 text-white shadow-sm shadow-violet-200"
                  : "text-gray-600 hover:text-gray-900 hover:bg-gray-50"
              }`}
            >
              <Icon size={15} /> {label}
            </button>
          ))}
        </div>

        {/* Content */}
        <div>
          {tab === "dashboard" && <DashboardTab />}
          {tab === "orders" && <OrdersTab />}
          {tab === "customers" && <CustomersTab />}
          {tab === "banners" && <BannersTab />}
          {tab === "categories" && <CategoriesTab />}
          {tab === "settings" && <SettingsTab />}
        </div>
      </div>
    </div>
  );
}
