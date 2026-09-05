import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  ExternalLink, ShoppingBag, Users, TrendingUp, Package,
  Plus, Trash2, Edit2, Save, X, Loader2, Image as ImageIcon,
  ChevronLeft, ChevronRight, Check, RefreshCw,
  LayoutDashboard, Settings, Tag, Image, ClipboardList, UserCheck,
  ChevronDown, ChevronUp, Newspaper, Star, Eye, EyeOff, MessageSquare, Bell, Video,
} from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

// ── Types ────────────────────────────────────────────────────────────────────

interface ShopBanner {
  id: string; imageUrl: string; title?: string; subtitle?: string;
  linkUrl?: string; linkText?: string; endDate?: string; active: boolean; order: number;
}
interface ShopCategory {
  id: string; name: string; slug: string; imageUrl?: string; order: number; filterTag?: string;
}
interface ShopMarkupRule { minUsd: number; coefficient: number }
interface ShopSettings {
  markup: number; markupRules: ShopMarkupRule[];
  shopName: string; shopDescription: string;
  contactEmail?: string; contactPhone?: string; deliveryDays?: number; freeDeliveryFrom?: number;
}
interface ShopCustomer {
  id: string; email: string; firstName?: string; lastName?: string;
  phone?: string; createdAt: string; _count: { orders: number };
}
interface ShopOrder {
  id: string; status: string; totalRub: number; items: unknown[];
  delivery: { firstName?: string; lastName?: string; phone?: string; email?: string; city?: string; address?: string; pvz?: string };
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

const STATUS_TONE: Record<string, string> = {
  pending: "warn", confirmed: "info", picking: "info",
  shipped: "", delivered: "success", cancelled: "danger",
};

function fmt(n: number) { return n.toLocaleString("ru-RU"); }
function fmtDate(s: string) {
  const d = new Date(s);
  return d.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" });
}
function customerName(c?: ShopOrder["customer"], d?: ShopOrder["delivery"]) {
  const n = [c?.firstName ?? d?.firstName, c?.lastName ?? d?.lastName].filter(Boolean).join(" ");
  return n || c?.email || d?.phone || "—";
}

// ── Pagination ────────────────────────────────────────────────────────────────

function Pagination({ page, total, pageSize, onChange }: { page: number; total: number; pageSize: number; onChange: (p: number) => void }) {
  const totalPages = Math.ceil(total / pageSize);
  if (totalPages <= 1) return null;
  return (
    <div className="pager">
      <span style={{ color: "var(--muted)", fontSize: 12 }}>{fmt(total)} записей</span>
      <button onClick={() => onChange(page - 1)} disabled={page <= 1} className="secondary-action icon-action"><ChevronLeft size={15} /></button>
      <span style={{ fontSize: 13 }}>{page} / {totalPages}</span>
      <button onClick={() => onChange(page + 1)} disabled={page >= totalPages} className="secondary-action icon-action"><ChevronRight size={15} /></button>
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
  const { data: ordersData } = useQuery<{ orders: ShopOrder[] }>({
    queryKey: ["shop-admin-orders", { page: 1, statusFilter: "" }],
    queryFn: () => apiFetch<{ orders: ShopOrder[] }>("/api/shop/admin/orders?pageSize=5"),
  });

  return (
    <div className="page-section">
      <div className="section-title">
        <div><h2>Обзор магазина</h2></div>
        <button onClick={() => void refetch()} className="secondary-action" type="button" disabled={isLoading}>
          <RefreshCw size={14} className={isLoading ? "spin" : ""} /> Обновить
        </button>
      </div>

      <section className="dashboard-metrics">
        <Stat label="Заказов всего" value={isLoading ? "…" : fmt(stats?.totalOrders ?? 0)} icon={<ShoppingBag size={17} />} delta={`Сегодня: ${stats?.todayOrders ?? 0}`} tone="accent" />
        <Stat label="Выручка, ₽" value={isLoading ? "…" : fmt(stats?.totalRevenue ?? 0)} icon={<TrendingUp size={17} />} delta={`За неделю: ${fmt(stats?.weekRevenue ?? 0)} ₽`} tone="success" />
        <Stat label="За неделю" value={isLoading ? "…" : (stats?.weekOrders ?? 0)} icon={<Package size={17} />} delta="заказов" />
        <Stat label="Покупатели" value={isLoading ? "…" : fmt(stats?.totalCustomers ?? 0)} icon={<Users size={17} />} delta="зарегистрировано" />
      </section>

      <div className="section-title"><div><h3>Последние заказы</h3></div></div>

      {!ordersData?.orders.length ? (
        <div className="soft-empty"><Package size={18} /> Заказов пока нет</div>
      ) : (
        <div className="table-panel">
          <div className="table-head" style={{ display: "grid", gridTemplateColumns: "minmax(130px,1.2fr) minmax(130px,1fr) minmax(70px,.4fr) minmax(100px,.7fr) minmax(130px,.9fr)", gap: 10 }}>
            <span>Заказ / Дата</span><span>Покупатель</span><span>Товары</span><span>Сумма</span><span>Статус</span>
          </div>
          {ordersData.orders.map((o) => (
            <div key={o.id} className="table-row" style={{ display: "grid", gridTemplateColumns: "minmax(130px,1.2fr) minmax(130px,1fr) minmax(70px,.4fr) minmax(100px,.7fr) minmax(130px,.9fr)", gap: 10 }}>
              <span>
                <div style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700 }}>{o.id}</div>
                <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2 }}>{fmtDate(o.createdAt)}</div>
              </span>
              <span style={{ fontSize: 13 }}>{customerName(o.customer ?? undefined, o.delivery)}</span>
              <span style={{ color: "var(--muted)", fontSize: 12 }}>{Array.isArray(o.items) ? `${o.items.length} поз.` : "—"}</span>
              <span style={{ fontWeight: 600 }}>{fmt(o.totalRub)} ₽</span>
              <span>
                <span className={`pill ${STATUS_TONE[o.status] ?? ""}`}>{STATUS_LABELS[o.status] ?? o.status}</span>
              </span>
            </div>
          ))}
        </div>
      )}
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

  const COL = "minmax(130px,1.2fr) minmax(130px,1fr) minmax(70px,.4fr) minmax(100px,.7fr) minmax(130px,.9fr)";
  const DELIVERY_LABELS: Record<string, string> = { firstName: "Имя", lastName: "Фамилия", pvz: "ПВЗ", address: "Адрес", city: "Город", zip: "Индекс", phone: "Телефон", email: "Email" };

  return (
    <div className="page-section">
      {updateStatus.isError && <div className="inline-error">Ошибка: {String(updateStatus.error)}</div>}
      <div className="section-title" style={{ flexWrap: "wrap", gap: "10px" }}>
        <div><h2>Заказы</h2></div>
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          {["", ...Object.keys(STATUS_LABELS)].map((s) => (
            <button
              key={s}
              onClick={() => { setStatusFilter(s); setPage(1); }}
              className={`secondary-action${statusFilter === s ? " is-active" : ""}`}
              style={{ minHeight: 32, padding: "5px 10px", fontSize: 12 }}
            >
              {s ? STATUS_LABELS[s] : "Все"}
            </button>
          ))}
        </div>
      </div>

      {isLoading ? (
        <div className="list-loading"><Loader2 size={16} className="spin" /> Загружаю заказы…</div>
      ) : !data?.orders.length ? (
        <div className="soft-empty"><ShoppingBag size={18} /> Заказов нет</div>
      ) : (
        <div className="table-panel orders-table">
          <div className="table-head" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
            <span>Заказ / Дата</span><span>Покупатель</span><span>Позиций</span><span>Сумма</span><span>Статус</span>
          </div>

          {data.orders.map((o) => (
            <div key={o.id}>
              <button
                type="button"
                className="table-row"
                style={{ display: "grid", gridTemplateColumns: COL, gap: 10, width: "100%", textAlign: "left", cursor: "pointer" }}
                onClick={() => setExpanded(expanded === o.id ? null : o.id)}
              >
                <span>
                  <div style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700 }}>{o.id}</div>
                  <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2 }}>{fmtDate(o.createdAt)}</div>
                </span>
                <span style={{ fontSize: 13 }}>
                  <div>{customerName(o.customer ?? undefined, o.delivery)}</div>
                  {o.delivery?.city && <div style={{ fontSize: 11, color: "var(--muted)" }}>{o.delivery.city}</div>}
                </span>
                <span style={{ color: "var(--muted)", fontSize: 12 }}>{Array.isArray(o.items) ? o.items.length : "—"}</span>
                <span style={{ fontWeight: 600 }}>{fmt(o.totalRub)} ₽</span>
                <span style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 6 }}>
                  <span className={`pill ${STATUS_TONE[o.status] ?? ""}`}>{STATUS_LABELS[o.status] ?? o.status}</span>
                  {expanded === o.id ? <ChevronUp size={14} style={{ color: "var(--muted)", flexShrink: 0 }} /> : <ChevronDown size={14} style={{ color: "var(--muted)", flexShrink: 0 }} />}
                </span>
              </button>

              {expanded === o.id && (
                <div>
                  <div className="mv-order-expand">
                    <div>
                      <strong>Доставка</strong>
                      <dl>
                        {Object.entries(o.delivery).map(([k, v]) => v ? (
                          <div key={k} style={{ display: "flex", gap: 8 }}>
                            <dt style={{ width: 90, flexShrink: 0 }}>{DELIVERY_LABELS[k] ?? k}</dt>
                            <dd>{String(v)}</dd>
                          </div>
                        ) : null)}
                        {o.comment && <div style={{ marginTop: 8, fontStyle: "italic", color: "var(--muted)", fontSize: 12 }}>Комментарий: {o.comment}</div>}
                      </dl>
                    </div>
                    <div>
                      <strong>Позиции</strong>
                      <div style={{ display: "grid", gap: 6 }}>
                        {Array.isArray(o.items) && o.items.map((item, idx) => {
                          const it = item as { name?: string; quantity?: number; priceRub?: number };
                          return (
                            <div key={idx} style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 13 }}>
                              <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {it.name || `Товар ${idx + 1}`} × {it.quantity ?? 1}
                              </span>
                              <span style={{ flexShrink: 0, fontWeight: 600 }}>{fmt((it.priceRub ?? 0) * (it.quantity ?? 1))} ₽</span>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                  <div className="mv-order-status-bar">
                    <span style={{ fontSize: 12, color: "var(--muted)", marginRight: 4 }}>Статус:</span>
                    {Object.entries(STATUS_LABELS).map(([s, label]) => (
                      <button
                        key={s}
                        type="button"
                        disabled={o.status === s || updateStatus.isPending}
                        onClick={() => updateStatus.mutate({ id: o.id, status: s })}
                        className={`secondary-action${o.status === s ? " is-active" : ""}`}
                        style={{ minHeight: 30, padding: "4px 10px", fontSize: 12, display: "inline-flex", alignItems: "center", gap: 5 }}
                      >
                        {o.status === s && <Check size={12} />} {label}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}

          <Pagination page={page} total={data.total} pageSize={20} onChange={setPage} />
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

  const COL = "minmax(140px,1fr) minmax(140px,1fr) minmax(110px,.8fr) minmax(60px,.4fr) minmax(130px,.8fr)";

  return (
    <div className="page-section">
      <div className="section-title"><div><h2>Покупатели</h2></div></div>

      {isLoading ? (
        <div className="list-loading"><Loader2 size={16} className="spin" /> Загружаю…</div>
      ) : !data?.customers.length ? (
        <div className="soft-empty"><Users size={18} /> Зарегистрированных покупателей нет</div>
      ) : (
        <div className="table-panel customers-table">
          <div className="table-head" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
            <span>Имя</span><span>Email</span><span>Телефон</span><span>Заказов</span><span>Регистрация</span>
          </div>
          {data.customers.map((c) => (
            <div key={c.id} className="table-row" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
              <span style={{ fontWeight: 600 }}>{[c.firstName, c.lastName].filter(Boolean).join(" ") || "—"}</span>
              <span style={{ fontSize: 13, color: "var(--muted-soft)" }}>{c.email}</span>
              <span style={{ fontSize: 13 }}>{c.phone || "—"}</span>
              <span>
                <span className="section-count">{c._count.orders}</span>
              </span>
              <span style={{ fontSize: 12, color: "var(--muted)" }}>{fmtDate(c.createdAt)}</span>
            </div>
          ))}
          <Pagination page={page} total={data.total} pageSize={20} onChange={setPage} />
        </div>
      )}
    </div>
  );
}

// ── Banners ───────────────────────────────────────────────────────────────────

function BannerForm({ banner, onSave, onCancel, saving }: {
  banner?: Partial<ShopBanner>; onSave: (d: Partial<ShopBanner>) => void; onCancel: () => void; saving?: boolean;
}) {
  const [form, setForm] = useState<Partial<ShopBanner>>({
    imageUrl: "", title: "", subtitle: "", linkUrl: "", linkText: "", endDate: "", active: true, ...banner,
  });
  const set = (k: keyof ShopBanner) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm((f) => ({ ...f, [k]: e.target.type === "checkbox" ? e.target.checked : e.target.value }));

  return (
    <div className="mv-form-section">
      {form.imageUrl && (
        <img src={form.imageUrl} alt="" className="mv-banner-preview" style={{ marginBottom: 8, height: 80, width: "auto", maxWidth: "100%" }}
          onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }} />
      )}
      <div className="mv-field-grid">
        {([
          ["imageUrl", "URL изображения", "https://..."],
          ["title", "Заголовок", "Летняя коллекция"],
          ["subtitle", "Подзаголовок", "Скидки до 50%"],
          ["linkUrl", "Ссылка", "/catalog/sale"],
          ["linkText", "Текст кнопки", "Смотреть акции"],
        ] as const).map(([key, label, placeholder]) => (
          <div key={key} className="mv-field">
            <label>{label}</label>
            <input value={(form[key] as string) ?? ""} onChange={set(key)} placeholder={placeholder} />
          </div>
        ))}
        <div className="mv-field">
          <label>Конец акции (необязательно)</label>
          <input
            type="datetime-local"
            value={(form.endDate as string) ?? ""}
            onChange={set("endDate")}
          />
        </div>
        <div className="mv-field" style={{ justifyContent: "flex-end" }}>
          <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <input type="checkbox" checked={form.active !== false} onChange={set("active")} />
            Активен (показывать)
          </label>
        </div>
      </div>
      <div className="row-actions">
        <button onClick={() => onSave(form)} disabled={saving} className="primary-action">
          {saving ? <Loader2 size={15} className="spin" /> : <Save size={15} />} Сохранить
        </button>
        <button onClick={onCancel} className="secondary-action"><X size={15} /> Отмена</button>
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

  const COL = "80px minmax(130px,1fr) minmax(120px,.8fr) 90px 74px";

  return (
    <div className="page-section">
      <div className="section-title">
        <div><h2>Баннеры главной страницы</h2></div>
        <button onClick={() => setEditing("new")} className="primary-action">
          <Plus size={16} /> Добавить баннер
        </button>
      </div>

      {editing === "new" && (
        <BannerForm onSave={(d) => saveMut.mutate(d)} onCancel={() => setEditing(null)} saving={saveMut.isPending} />
      )}
      {saveMut.isError && <div className="inline-error">Ошибка: {String(saveMut.error)}</div>}

      {isLoading ? (
        <div className="list-loading"><Loader2 size={16} className="spin" /> Загружаю баннеры…</div>
      ) : banners.length === 0 && editing !== "new" ? (
        <div className="soft-empty"><ImageIcon size={18} /> Баннеры не добавлены — на главной показывается градиентный фон</div>
      ) : (
        <div className="table-panel banners-table">
          <div className="table-head" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
            <span>Фото</span><span>Заголовок</span><span>Ссылка</span><span>Статус</span><span />
          </div>
          {banners.map((b) => (
            <div key={b.id}>
              <div className="table-row" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
                <span>
                  {b.imageUrl
                    ? <img src={b.imageUrl} alt="" className="mv-banner-preview" />
                    : <div style={{ width: 72, height: 44, background: "rgba(8,17,31,.6)", display: "flex", alignItems: "center", justifyContent: "center" }}><ImageIcon size={16} style={{ color: "var(--muted)" }} /></div>
                  }
                </span>
                <span>
                  <div style={{ fontWeight: 600 }}>{b.title || "(без названия)"}</div>
                  {b.subtitle && <div style={{ fontSize: 12, color: "var(--muted)" }}>{b.subtitle}</div>}
                </span>
                <span style={{ fontSize: 12, color: "var(--muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{b.linkUrl || "—"}</span>
                <span>
                  <span className={`pill ${b.active ? "success" : ""}`}>{b.active ? "Активен" : "Скрыт"}</span>
                </span>
                <span style={{ display: "flex", gap: 4 }}>
                  <button onClick={() => setEditing(editing === b.id ? null : b.id)} className="secondary-action icon-action"><Edit2 size={14} /></button>
                  <button onClick={() => { if (confirm("Удалить баннер?")) deleteMut.mutate(b.id); }} className="icon-action danger"><Trash2 size={14} /></button>
                </span>
              </div>
              {editing === b.id && (
                <div style={{ padding: "0 0 12px" }}>
                  <BannerForm banner={b} onSave={(d) => saveMut.mutate({ ...d, id: b.id })} onCancel={() => setEditing(null)} saving={saveMut.isPending} />
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

  const setF = (k: keyof ShopCategory) => (e: React.ChangeEvent<HTMLInputElement>) => setForm((f) => ({ ...f, [k]: e.target.value }));
  const COL = "minmax(130px,1fr) minmax(120px,.8fr) minmax(100px,.6fr) 74px";

  return (
    <div className="page-section">
      <div className="section-title">
        <div><h2>Категории каталога</h2></div>
        <button onClick={() => { setForm({ name: "", slug: "", imageUrl: "", filterTag: "" }); setEditing("new"); }} className="primary-action">
          <Plus size={16} /> Добавить категорию
        </button>
      </div>

      {saveMut.isError && <div className="inline-error">Ошибка: {String(saveMut.error)}</div>}
      {editing && (
        <div className="mv-form-section">
          <h3>{editing === "new" ? "Новая категория" : "Редактировать категорию"}</h3>
          <div className="mv-field-grid">
            {([["name", "Название", "Парфюмерия"], ["slug", "Slug (URL)", "parfumery"], ["imageUrl", "URL изображения", "https://..."], ["filterTag", "Тег фильтра", "parfum"]] as const).map(([k, l, p]) => (
              <div key={k} className="mv-field">
                <label>{l}</label>
                <input value={(form[k] as string) ?? ""} onChange={setF(k)} placeholder={p} />
              </div>
            ))}
          </div>
          <div className="row-actions">
            <button
              onClick={() => saveMut.mutate(editing === "new" ? form : { ...form, id: editing })}
              disabled={saveMut.isPending}
              className="primary-action"
            >
              {saveMut.isPending ? <Loader2 size={15} className="spin" /> : <Save size={15} />} Сохранить
            </button>
            <button onClick={() => { setEditing(null); setForm({}); }} className="secondary-action"><X size={15} /> Отмена</button>
          </div>
        </div>
      )}

      {isLoading ? (
        <div className="list-loading"><Loader2 size={16} className="spin" /> Загружаю…</div>
      ) : cats.length === 0 && !editing ? (
        <div className="soft-empty"><Tag size={18} /> Категории не добавлены</div>
      ) : (
        <div className="table-panel cats-table">
          <div className="table-head" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
            <span>Категория</span><span>Slug</span><span>Тег фильтра</span><span />
          </div>
          {cats.map((c) => (
            <div key={c.id} className="table-row" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
              <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
                {c.imageUrl && <img src={c.imageUrl} alt="" style={{ width: 28, height: 28, objectFit: "cover", borderRadius: 4, background: "#f8fafc" }} onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }} />}
                <span style={{ fontWeight: 600 }}>{c.name}</span>
              </span>
              <span style={{ fontFamily: "monospace", fontSize: 12, color: "var(--muted)" }}>{c.slug}</span>
              <span style={{ fontSize: 12, color: "var(--muted)" }}>{c.filterTag || "—"}</span>
              <span style={{ display: "flex", gap: 4 }}>
                <button onClick={() => { setForm({ ...c }); setEditing(c.id); }} className="secondary-action icon-action"><Edit2 size={14} /></button>
                <button onClick={() => { if (confirm("Удалить категорию?")) deleteMut.mutate(c.id); }} className="icon-action danger"><Trash2 size={14} /></button>
              </span>
            </div>
          ))}
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
  const [rules, setRules] = useState<ShopMarkupRule[]>([]);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    if (settings) {
      setForm(settings);
      setRules(Array.isArray(settings.markupRules) ? settings.markupRules : []);
    }
  }, [settings]);

  const saveMut = useMutation({
    mutationFn: (d: Partial<ShopSettings>) => apiFetch<{ ok: boolean }>("/api/shop/admin/settings", {
      method: "PATCH", body: JSON.stringify(d),
    }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["shop-admin-settings"] }); setSaved(true); setTimeout(() => setSaved(false), 3000); },
  });

  const setF = (k: keyof ShopSettings) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
    setForm((f) => ({ ...f, [k]: ["markup", "deliveryDays", "freeDeliveryFrom"].includes(k) ? Number(e.target.value) : e.target.value }));

  const addRule = () => setRules(r => [...r, { minUsd: 0, coefficient: form.markup ?? 2.2 }]);
  const removeRule = (i: number) => setRules(r => r.filter((_, idx) => idx !== i));
  const updateRule = (i: number, field: keyof ShopMarkupRule, val: string) =>
    setRules(r => r.map((rule, idx) => idx === i ? { ...rule, [field]: Number(val) } : rule));

  const handleSave = () => saveMut.mutate({ ...form, markupRules: rules });

  if (isLoading) return <div className="list-loading"><Loader2 size={16} className="spin" /> Загружаю…</div>;

  const sortedRules = [...rules].sort((a, b) => a.minUsd - b.minUsd);

  return (
    <div className="page-section" style={{ maxWidth: 760 }}>
      <div className="section-title"><div><h2>Настройки магазина</h2></div></div>

      <div className="mv-form-section">
        <h3>Основное</h3>
        <div className="mv-field-grid">
          <div className="mv-field">
            <label>Название магазина</label>
            <input value={form.shopName ?? ""} onChange={setF("shopName")} placeholder="Magic Vibes" />
          </div>
          <div className="mv-field">
            <label>Email для связи</label>
            <input value={form.contactEmail ?? ""} onChange={setF("contactEmail")} placeholder="info@magicvibes.ru" />
          </div>
          <div className="mv-field">
            <label>Телефон</label>
            <input value={form.contactPhone ?? ""} onChange={setF("contactPhone")} placeholder="+7 800 ..." />
          </div>
        </div>
        <div className="mv-field">
          <label>Описание магазина</label>
          <textarea value={form.shopDescription ?? ""} onChange={setF("shopDescription")} />
        </div>
      </div>

      <div className="mv-form-section">
        <h3>Цены и доставка</h3>
        <div className="mv-field-grid">
          <div className="mv-field">
            <label>Коэффициент наценки (по умолчанию)</label>
            <div className="mv-form-inline">
              <input type="number" step="0.05" min="0.5" max="20" value={form.markup ?? 2.2} onChange={setF("markup")} style={{ width: 110 }} />
              <span style={{ color: "var(--muted)", fontSize: 12 }}>× (USD × курс)</span>
            </div>
            <span style={{ color: "var(--muted)", fontSize: 11, marginTop: 4 }}>
              Применяется, если ни одно гибкое правило не совпало
            </span>
          </div>
          <div className="mv-field">
            <label>Бесплатная доставка от (₽)</label>
            <input type="number" min="0" value={form.freeDeliveryFrom ?? 3000} onChange={setF("freeDeliveryFrom")} />
          </div>
          <div className="mv-field">
            <label>Срок доставки (дней)</label>
            <input type="number" min="1" max="30" value={form.deliveryDays ?? 3} onChange={setF("deliveryDays")} />
          </div>
        </div>

        {/* Flexible markup rules */}
        <div className="mv-field" style={{ marginTop: 20 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
            <label style={{ margin: 0 }}>Гибкие правила наценки</label>
            <button type="button" className="btn-outline" style={{ fontSize: 12, padding: "4px 10px", display: "flex", alignItems: "center", gap: 4 }} onClick={addRule}>
              <Plus size={13} /> Добавить правило
            </button>
          </div>
          <p style={{ color: "var(--muted)", fontSize: 11, marginBottom: 10 }}>
            Правило применяется, если цена закупки ≥ minUSD. При нескольких совпадениях побеждает наибольший порог.
          </p>

          {sortedRules.length === 0 ? (
            <div style={{ color: "var(--muted)", fontSize: 12, padding: "10px 0" }}>
              Гибких правил нет — используется коэффициент по умолчанию
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {sortedRules.map((rule, i) => {
                const origIdx = rules.indexOf(rule);
                return (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", background: "var(--surface)", borderRadius: 8, border: "1px solid var(--border)" }}>
                    <span style={{ color: "var(--muted)", fontSize: 12, flexShrink: 0 }}>Цена от</span>
                    <input
                      type="number" min="0" step="1"
                      value={rule.minUsd}
                      onChange={e => updateRule(origIdx, "minUsd", e.target.value)}
                      style={{ width: 80, fontSize: 13 }}
                    />
                    <span style={{ color: "var(--muted)", fontSize: 12, flexShrink: 0 }}>USD → коэф.</span>
                    <input
                      type="number" min="0.5" max="20" step="0.05"
                      value={rule.coefficient}
                      onChange={e => updateRule(origIdx, "coefficient", e.target.value)}
                      style={{ width: 90, fontSize: 13 }}
                    />
                    <span style={{ color: "var(--muted)", fontSize: 11, flex: 1 }}>
                      ≈ {(rule.coefficient * 100 - 100).toFixed(0)}% наценка
                    </span>
                    <button type="button" onClick={() => removeRule(origIdx)} style={{ color: "var(--danger)", background: "none", border: "none", cursor: "pointer", padding: 4 }}>
                      <Trash2 size={14} />
                    </button>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {saved && (
        <div className="success-strip" style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <Check size={16} /> Настройки сохранены
        </div>
      )}
      {saveMut.isError && (
        <div className="warning-strip">Ошибка сохранения</div>
      )}

      <button onClick={handleSave} disabled={saveMut.isPending} className="primary-action">
        {saveMut.isPending ? <Loader2 size={16} className="spin" /> : <Save size={16} />} Сохранить настройки
      </button>
    </div>
  );
}

// ── News Tab ──────────────────────────────────────────────────────────────────

interface TgNewsPost {
  id: string; text: string; photoUrl?: string | null;
  publishedAt: string; active: boolean;
}

function NewsTab() {
  const qc = useQueryClient();
  const { data, isLoading, refetch } = useQuery<{ ok: boolean; posts: TgNewsPost[] }>({
    queryKey: ["shop-admin-news"],
    queryFn: () => apiFetch<{ ok: boolean; posts: TgNewsPost[] }>("/api/shop/admin/news"),
  });
  const importMut = useMutation({
    mutationFn: () => apiFetch<{ ok: boolean }>("/api/shop/admin/news/import", { method: "POST" }),
    onSuccess: () => setTimeout(() => void refetch(), 2000),
  });
  const toggleMut = useMutation({
    mutationFn: ({ id, active }: { id: string; active: boolean }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/news/${id}`, { method: "PATCH", body: JSON.stringify({ active }) }),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["shop-admin-news"] }),
  });

  return (
    <div className="page-section">
      <div className="section-title">
        <div><h2>Новости из Telegram</h2><p style={{ color: "var(--muted)", fontSize: 13, marginTop: 4 }}>Посты с хэштегом #новости из канала @magicvibes_ru</p></div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => void importMut.mutate()} disabled={importMut.isPending} className="secondary-action" type="button">
            <RefreshCw size={14} className={importMut.isPending ? "spin" : ""} /> Импортировать
          </button>
          <button onClick={() => void refetch()} disabled={isLoading} className="secondary-action" type="button">
            <RefreshCw size={14} className={isLoading ? "spin" : ""} /> Обновить
          </button>
        </div>
      </div>

      {importMut.isSuccess && (
        <div style={{ padding: "10px 14px", borderRadius: 8, background: "rgba(74,222,128,0.1)", border: "1px solid rgba(74,222,128,0.2)", fontSize: 13, color: "var(--success)", marginBottom: 12 }}>
          Импорт запущен. Новые посты появятся через несколько секунд.
        </div>
      )}

      {isLoading && <div className="soft-empty"><Loader2 size={18} className="spin" /> Загрузка…</div>}

      {!isLoading && !data?.posts.length && (
        <div className="soft-empty"><Newspaper size={18} /> Новостей нет. Нажмите «Импортировать» чтобы загрузить посты из Telegram.</div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 14 }}>
        {data?.posts.map((post) => (
          <div key={post.id} className="card-panel" style={{ opacity: post.active ? 1 : 0.5 }}>
            {post.photoUrl && (
              <img src={post.photoUrl} alt="" style={{ width: "100%", aspectRatio: "16/9", objectFit: "cover", borderRadius: 8, marginBottom: 10 }} />
            )}
            <p style={{ fontSize: 11, color: "var(--muted)", marginBottom: 6 }}>
              {new Date(post.publishedAt).toLocaleString("ru-RU")}
            </p>
            <p style={{ fontSize: 13, color: "var(--text)", lineHeight: 1.55, marginBottom: 12 }}>
              {post.text.slice(0, 200)}{post.text.length > 200 && "…"}
            </p>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span className={`pill ${post.active ? "success" : ""}`}>{post.active ? "Показывается" : "Скрыт"}</span>
              <button
                onClick={() => void toggleMut.mutate({ id: post.id, active: !post.active })}
                disabled={toggleMut.isPending}
                className="secondary-action icon-action"
                type="button"
              >
                {post.active ? <EyeOff size={14} /> : <Eye size={14} />}
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Reviews Tab ───────────────────────────────────────────────────────────────

interface AdminReview {
  id: string; offerId?: string | null; productName?: string | null;
  rating: number; text: string; createdAt: string; approved: boolean;
  customer?: { email: string } | null;
}

function ReviewsTab() {
  const qc = useQueryClient();
  const { data, isLoading, refetch } = useQuery<{ ok: boolean; reviews: AdminReview[] }>({
    queryKey: ["shop-admin-reviews"],
    queryFn: () => apiFetch<{ ok: boolean; reviews: AdminReview[] }>("/api/shop/admin/reviews"),
  });
  const toggleMut = useMutation({
    mutationFn: ({ id, approved }: { id: string; approved: boolean }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/reviews/${id}`, { method: "PATCH", body: JSON.stringify({ approved }) }),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["shop-admin-reviews"] }),
  });
  const deleteMut = useMutation({
    mutationFn: (id: string) => apiFetch<{ ok: boolean }>(`/api/shop/admin/reviews/${id}`, { method: "DELETE" }),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["shop-admin-reviews"] }),
  });

  return (
    <div className="page-section">
      <div className="section-title">
        <div><h2>Отзывы покупателей</h2></div>
        <button onClick={() => void refetch()} disabled={isLoading} className="secondary-action" type="button">
          <RefreshCw size={14} className={isLoading ? "spin" : ""} /> Обновить
        </button>
      </div>

      {isLoading && <div className="soft-empty"><Loader2 size={18} className="spin" /> Загрузка…</div>}
      {!isLoading && !data?.reviews.length && (
        <div className="soft-empty"><MessageSquare size={18} /> Отзывов пока нет</div>
      )}

      <div className="table-panel">
        {data?.reviews.map((r) => (
          <div key={r.id} className="table-row" style={{ display: "grid", gridTemplateColumns: "minmax(120px,.8fr) minmax(80px,.5fr) minmax(200px,2fr) minmax(120px,.7fr) auto", gap: 12, alignItems: "center" }}>
            <span>
              <div style={{ fontSize: 12, fontWeight: 600 }}>{r.customer?.email ?? "—"}</div>
              <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2 }}>{fmtDate(r.createdAt)}</div>
            </span>
            <span style={{ display: "flex", gap: 2 }}>
              {Array.from({ length: 5 }).map((_, i) => (
                <Star key={i} size={11} fill={i < r.rating ? "gold" : "none"} stroke={i < r.rating ? "gold" : "var(--border-md)"} />
              ))}
            </span>
            <span>
              {r.productName && <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 3 }}>{r.productName}</div>}
              <div style={{ fontSize: 13 }}>{r.text.slice(0, 120)}{r.text.length > 120 && "…"}</div>
            </span>
            <span><span className={`pill ${r.approved ? "success" : "warn"}`}>{r.approved ? "Показывается" : "Скрыт"}</span></span>
            <span style={{ display: "flex", gap: 6 }}>
              <button
                onClick={() => void toggleMut.mutate({ id: r.id, approved: !r.approved })}
                disabled={toggleMut.isPending}
                className="secondary-action icon-action"
                type="button"
                title={r.approved ? "Скрыть" : "Показать"}
              >
                {r.approved ? <EyeOff size={13} /> : <Eye size={13} />}
              </button>
              <button
                onClick={() => { if (confirm("Удалить отзыв?")) void deleteMut.mutate(r.id); }}
                disabled={deleteMut.isPending}
                className="secondary-action icon-action danger"
                type="button"
              >
                <Trash2 size={13} />
              </button>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── UnboxingsTab ─────────────────────────────────────────────────────────────

interface Unboxing {
  id: string; name: string; mediaUrl: string; text: string;
  approved: boolean; createdAt: string;
}

function UnboxingsTab() {
  const qc = useQueryClient();
  const { data, isLoading } = useQuery({
    queryKey: ["admin-unboxings"],
    queryFn: () => apiFetch<{ ok: boolean; unboxings: Unboxing[] }>("/api/shop/admin/unboxings"),
  });

  const toggleMut = useMutation({
    mutationFn: ({ id, approved }: { id: string; approved: boolean }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/unboxings/${id}`, { method: "PATCH", body: JSON.stringify({ approved }) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["admin-unboxings"] }),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/unboxings/${id}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["admin-unboxings"] }),
  });

  const unboxings = data?.unboxings ?? [];

  return (
    <div className="page-section" style={{ marginTop: 0 }}>
      <p className="section-title">Распаковки покупателей</p>
      {isLoading && <p style={{ color: "var(--text-muted)", fontSize: 13 }}>Загрузка…</p>}
      {!isLoading && !unboxings.length && (
        <p style={{ color: "var(--text-muted)", fontSize: 13 }}>Нет распаковок</p>
      )}
      <div className="table-panel" style={{ display: "flex", flexDirection: "column", gap: 0 }}>
        {unboxings.map((u) => (
          <div key={u.id} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr auto auto auto", alignItems: "center", gap: 10, padding: "10px 14px", borderBottom: "1px solid var(--border)" }}>
            <span style={{ fontSize: 13, color: "var(--text)" }}>{u.name}</span>
            <span style={{ fontSize: 12, color: "var(--text-muted)" }}>
              {new Date(u.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "short", year: "numeric" })}
            </span>
            <span style={{ fontSize: 12, color: "var(--text-muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {u.text ? u.text.slice(0, 80) + (u.text.length > 80 ? "…" : "") : "—"}
            </span>
            {u.mediaUrl ? (
              <a href={u.mediaUrl} target="_blank" rel="noreferrer" style={{ fontSize: 12, color: "var(--accent)", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                Ссылка
              </a>
            ) : (
              <span style={{ fontSize: 12, color: "var(--text-muted)" }}>—</span>
            )}
            <span><span className={`pill ${u.approved ? "success" : "warn"}`}>{u.approved ? "Показывается" : "На проверке"}</span></span>
            <span style={{ display: "flex", gap: 6 }}>
              <button
                onClick={() => void toggleMut.mutate({ id: u.id, approved: !u.approved })}
                disabled={toggleMut.isPending}
                className="secondary-action icon-action"
                type="button"
                title={u.approved ? "Скрыть" : "Одобрить"}
              >
                {u.approved ? <EyeOff size={13} /> : <Eye size={13} />}
              </button>
              <button
                onClick={() => { if (confirm("Удалить распаковку?")) void deleteMut.mutate(u.id); }}
                disabled={deleteMut.isPending}
                className="secondary-action icon-action danger"
                type="button"
              >
                <Trash2 size={13} />
              </button>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── EmailSequencesTab ─────────────────────────────────────────────────────────

interface SeqStat { step: number; count: number; last_sent_at: string | null }

function EmailSequencesTab() {
  const qc = useQueryClient();
  const { data, isLoading } = useQuery({
    queryKey: ["admin-email-seq-stats"],
    queryFn: () => apiFetch<{ ok: boolean; stats: SeqStat[] }>("/api/shop/admin/email-sequences/stats"),
  });
  const runMut = useMutation({
    mutationFn: () => apiFetch<{ ok: boolean; result: { sent7?: number; sent30?: number; errors?: number } }>("/api/shop/admin/email-sequences/run-scan", { method: "POST" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["admin-email-seq-stats"] }),
  });

  const STEP_META: Record<number, { label: string; desc: string }> = {
    1:  { label: "День 1 — подтверждение", desc: "Отправляется сразу после оформления заказа. История аромата." },
    7:  { label: "День 7 — отзыв", desc: "Запрос отзыва + промокод REVIEW5 (−5%)." },
    30: { label: "День 30 — новинки", desc: "Рассылка аромата месяца с актуальными новостями." },
  };

  const stats = data?.stats ?? [];
  const byStep = Object.fromEntries(stats.map((s) => [s.step, s]));

  return (
    <div className="admin-tab-content">
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <h3 style={{ margin: 0, fontSize: 15 }}>Email-цепочки после покупки</h3>
        <button
          type="button"
          className="secondary-action"
          onClick={() => void runMut.mutate()}
          disabled={runMut.isPending}
          style={{ display: "inline-flex", alignItems: "center", gap: 6 }}
        >
          {runMut.isPending ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />}
          Запустить сканер
        </button>
      </div>

      {runMut.isSuccess && runMut.data && (
        <div style={{ background: "rgba(34,197,94,.08)", border: "1px solid rgba(34,197,94,.25)", borderRadius: 4, padding: "8px 12px", marginBottom: 12, fontSize: 12, color: "#22c55e" }}>
          Готово: День&nbsp;7 — {runMut.data.result.sent7 ?? 0} шт., День&nbsp;30 — {runMut.data.result.sent30 ?? 0} шт.
          {(runMut.data.result.errors ?? 0) > 0 && <span style={{ color: "#f87171", marginLeft: 8 }}>ошибок: {runMut.data.result.errors}</span>}
        </div>
      )}

      {isLoading ? (
        <div style={{ padding: 32, textAlign: "center", color: "var(--text-muted)" }}><Loader2 size={20} className="spin" /></div>
      ) : (
        <div style={{ display: "grid", gap: 10 }}>
          {[1, 7, 30].map((step) => {
            const meta = STEP_META[step];
            const stat = byStep[step];
            return (
              <div key={step} style={{ background: "var(--surface-raised)", border: "1px solid var(--border)", borderRadius: 6, padding: "14px 16px" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                  <div>
                    <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 3 }}>{meta.label}</div>
                    <div style={{ fontSize: 12, color: "var(--text-muted)" }}>{meta.desc}</div>
                  </div>
                  <div style={{ textAlign: "right", flexShrink: 0, marginLeft: 16 }}>
                    <div style={{ fontSize: 22, fontWeight: 700, color: "var(--accent)" }}>{stat?.count ?? 0}</div>
                    <div style={{ fontSize: 11, color: "var(--text-muted)" }}>отправлено</div>
                    {stat?.last_sent_at && (
                      <div style={{ fontSize: 10, color: "var(--text-muted)", marginTop: 2 }}>
                        последнее: {new Date(stat.last_sent_at).toLocaleDateString("ru-RU")}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      <div style={{ marginTop: 16, padding: "10px 14px", background: "var(--surface-raised)", borderRadius: 4, fontSize: 11, color: "var(--text-muted)", lineHeight: 1.6 }}>
        Сканер автоматически запускается ежедневно в 10:30. Для ручного запуска нажмите «Запустить сканер» выше.<br />
        Каждое письмо отправляется только один раз на заказ (защита от дублей).
      </div>
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

// ── PushTab ───────────────────────────────────────────────────────────────────

function PushTab() {
  const qc = useQueryClient();
  const { data: statsData } = useQuery({
    queryKey: ["admin-push-stats"],
    queryFn: () => apiFetch<{ ok: boolean; total: number; configured: boolean }>("/api/shop/admin/push/stats"),
  });
  const [form, setForm] = useState({ title: "", body: "", url: "https://magicvibes.ru" });
  const sendMut = useMutation({
    mutationFn: () => apiFetch<{ ok: boolean; sent: number; failed: number; total: number }>("/api/shop/admin/push/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(form),
    }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["admin-push-stats"] }),
  });

  const notConfigured = statsData && !statsData.configured;

  return (
    <div className="admin-tab-content">
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 18 }}>
        <Bell size={18} style={{ color: "var(--accent)" }} />
        <h3 style={{ margin: 0, fontSize: 15 }}>Web Push — рассылка уведомлений</h3>
        <span style={{ marginLeft: "auto", fontSize: 12, color: "var(--text-muted)", background: "var(--surface-raised)", border: "1px solid var(--border)", borderRadius: 20, padding: "2px 10px" }}>
          {statsData?.total ?? "…"} подписчиков
        </span>
      </div>

      {notConfigured && (
        <div style={{ background: "rgba(251,191,36,.07)", border: "1px solid rgba(251,191,36,.3)", borderRadius: 4, padding: "10px 14px", marginBottom: 16, fontSize: 12, color: "#fbbf24", lineHeight: 1.6 }}>
          VAPID-ключи не заданы — push-уведомления отключены.<br />
          Запустите <code style={{ fontFamily: "monospace" }}>node scripts/gen-vapid.cjs</code> и добавьте ключи в .env.
        </div>
      )}

      {sendMut.isSuccess && sendMut.data && (
        <div style={{ background: "rgba(34,197,94,.08)", border: "1px solid rgba(34,197,94,.25)", borderRadius: 4, padding: "8px 12px", marginBottom: 12, fontSize: 12, color: "#22c55e" }}>
          Отправлено: {sendMut.data.sent} / {sendMut.data.total}
          {sendMut.data.failed > 0 && <span style={{ color: "#f87171", marginLeft: 8 }}>ошибок: {sendMut.data.failed} (истёкшие подписки удалены)</span>}
        </div>
      )}

      <div style={{ display: "grid", gap: 10, maxWidth: 540 }}>
        <div>
          <label style={{ fontSize: 11, color: "var(--text-muted)", display: "block", marginBottom: 4 }}>Заголовок</label>
          <input
            className="input-base"
            value={form.title}
            onChange={e => setForm(f => ({ ...f, title: e.target.value }))}
            placeholder="Новинки февраля — Chanel, Dior, Tom Ford"
            maxLength={80}
          />
        </div>
        <div>
          <label style={{ fontSize: 11, color: "var(--text-muted)", display: "block", marginBottom: 4 }}>Текст уведомления</label>
          <textarea
            className="input-base"
            value={form.body}
            onChange={e => setForm(f => ({ ...f, body: e.target.value }))}
            placeholder="Поступили долгожданные ароматы. Успей выбрать!"
            rows={2}
            maxLength={200}
            style={{ resize: "vertical" }}
          />
        </div>
        <div>
          <label style={{ fontSize: 11, color: "var(--text-muted)", display: "block", marginBottom: 4 }}>URL (куда ведёт клик)</label>
          <input
            className="input-base"
            value={form.url}
            onChange={e => setForm(f => ({ ...f, url: e.target.value }))}
            placeholder="https://magicvibes.ru/catalog"
          />
        </div>
        <button
          type="button"
          className="secondary-action"
          onClick={() => {
            if (!form.title.trim() || !form.body.trim()) return;
            if (!confirm(`Отправить push-уведомление ${statsData?.total ?? 0} подписчикам?`)) return;
            void sendMut.mutate();
          }}
          disabled={sendMut.isPending || !form.title.trim() || !form.body.trim() || Boolean(notConfigured)}
          style={{ display: "inline-flex", alignItems: "center", gap: 6, width: "fit-content" }}
        >
          {sendMut.isPending ? <Loader2 size={14} className="spin" /> : <Bell size={14} />}
          Отправить всем подписчикам
        </button>
      </div>

      <div style={{ marginTop: 20, padding: "10px 14px", background: "var(--surface-raised)", borderRadius: 4, fontSize: 11, color: "var(--text-muted)", lineHeight: 1.7 }}>
        Колокольчик появляется в шапке сайта и предлагает разрешить уведомления.<br />
        Истёкшие подписки (410 от браузера) автоматически удаляются при рассылке.
      </div>
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

type Tab = "dashboard" | "orders" | "customers" | "banners" | "categories" | "news" | "reviews" | "unboxings" | "emails" | "push" | "settings";

const TABS: { id: Tab; label: string; icon: React.ComponentType<{ size?: number }> }[] = [
  { id: "dashboard", label: "Обзор", icon: LayoutDashboard },
  { id: "orders", label: "Заказы", icon: ClipboardList },
  { id: "customers", label: "Покупатели", icon: UserCheck },
  { id: "banners", label: "Баннеры", icon: Image },
  { id: "categories", label: "Категории", icon: Tag },
  { id: "news", label: "Новости", icon: Newspaper },
  { id: "reviews", label: "Отзывы", icon: Star },
  { id: "unboxings", label: "Анбоксинг", icon: Video },
  { id: "emails", label: "Email-цепочки", icon: MessageSquare },
  { id: "push", label: "Push", icon: Bell },
  { id: "settings", label: "Настройки", icon: Settings },
];

export default function ShopAdminPage() {
  const [tab, setTab] = useState<Tab>("dashboard");

  return (
    <section className="page-section mv-shop-page">
      <PageHeader
        title="Magic Vibes — Магазин"
        subtitle="Управление витриной, заказами и настройками"
        action={
          <a href={SHOP_URL} target="_blank" rel="noreferrer" className="secondary-action">
            <ExternalLink size={15} /> Открыть магазин
          </a>
        }
      />

      <div className="settings-tabs" style={{ marginBottom: 6 }}>
        {TABS.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            type="button"
            onClick={() => setTab(id)}
            className={`secondary-action${tab === id ? " is-active" : ""}`}
            style={{ display: "inline-flex", alignItems: "center", gap: 7 }}
          >
            <Icon size={15} /> {label}
          </button>
        ))}
      </div>

      {tab === "dashboard" && <DashboardTab />}
      {tab === "orders" && <OrdersTab />}
      {tab === "customers" && <CustomersTab />}
      {tab === "banners" && <BannersTab />}
      {tab === "categories" && <CategoriesTab />}
      {tab === "news" && <NewsTab />}
      {tab === "reviews" && <ReviewsTab />}
      {tab === "unboxings" && <UnboxingsTab />}
      {tab === "emails" && <EmailSequencesTab />}
      {tab === "push" && <PushTab />}
      {tab === "settings" && <SettingsTab />}
    </section>
  );
}
