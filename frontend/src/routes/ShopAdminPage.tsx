import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  ExternalLink, ShoppingBag, Users, TrendingUp, Package,
  Plus, Trash2, Edit2, Save, X, Loader2, Image as ImageIcon,
  ChevronLeft, ChevronRight, Check, RefreshCw,
  LayoutDashboard, Settings, Tag, Image, ClipboardList, UserCheck,
  ChevronDown, ChevronUp, Newspaper, Star, Eye, EyeOff, MessageSquare, Bell, Video, BookOpen,
  ToggleLeft, ToggleRight, Percent, Mail, Copy,
} from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

// ── Types ────────────────────────────────────────────────────────────────────

interface ShopBanner {
  id: string; imageUrl: string; title?: string; subtitle?: string;
  linkUrl?: string; linkText?: string; endDate?: string; active: boolean; order: number;
  promoCode?: string;
}
interface HolidayPreset {
  key: string; name: string; emoji: string;
  defaultPromoCode: string; defaultTitle: string; defaultSubtitle: string;
  windowStart: [number, number]; windowEnd: [number, number];
  inWindow: boolean; active: boolean; promoCode: string;
  title: string; subtitle: string; manualOverride: boolean;
}
interface ShopCategory {
  id: string; name: string; slug: string; imageUrl?: string; order: number; filterTag?: string;
}
interface ShopMarkupRule { minUsd: number; coefficient: number }
interface ShopSettings {
  markup: number; markupRules: ShopMarkupRule[];
  shopName: string; shopDescription: string;
  contactEmail?: string; contactPhone?: string; deliveryDays?: number; freeDeliveryFrom?: number;
  vipTelegramLink?: string;
  aromaMesyatsa?: { offerId: string; note: string; validUntil?: string } | null;
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
  promoCode?: string | null; refCode?: string | null;
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
      <span className="sa-muted-12">{fmt(total)} записей</span>
      <button onClick={() => onChange(page - 1)} disabled={page <= 1} className="secondary-action icon-action"><ChevronLeft size={15} /></button>
      <span className="sa-text-13">{page} / {totalPages}</span>
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
        <div className="sa-scroll-x">
          <div className="table-panel sa-table-560">
            <div className="table-head sa-grid-orders">
              <span>Заказ / Дата</span><span>Покупатель</span><span>Товары</span><span>Сумма</span><span>Статус</span>
            </div>
            {ordersData.orders.map((o) => (
              <div key={o.id} className="table-row sa-grid-orders">
                <span>
                  <div className="sa-mono-id">{o.id}</div>
                  <div className="sa-sub-date">{fmtDate(o.createdAt)}</div>
                </span>
                <span className="sa-text-13">{customerName(o.customer ?? undefined, o.delivery)}</span>
                <span className="sa-muted-12">{Array.isArray(o.items) ? `${o.items.length} поз.` : "—"}</span>
                <span className="sa-bold">{fmt(o.totalRub)} ₽</span>
                <span>
                  <span className={`pill ${STATUS_TONE[o.status] ?? ""}`}>{STATUS_LABELS[o.status] ?? o.status}</span>
                </span>
              </div>
            ))}
          </div>
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
      <div className="section-title sa-title-wrap">
        <div><h2>Заказы</h2></div>
        <div className="sa-filter-wrap">
          {["", ...Object.keys(STATUS_LABELS)].map((s) => (
            <button
              key={s}
              onClick={() => { setStatusFilter(s); setPage(1); }}
              className={`secondary-action sa-filter-btn${statusFilter === s ? " is-active" : ""}`}
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
        <div className="sa-scroll-x">
        <div className="table-panel orders-table sa-table-560">
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
                  <div className="sa-mono-id">{o.id}</div>
                  <div className="sa-sub-date">{fmtDate(o.createdAt)}</div>
                </span>
                <span className="sa-text-13">
                  <div>{customerName(o.customer ?? undefined, o.delivery)}</div>
                  {o.delivery?.city && <div className="sa-muted-11">{o.delivery.city}</div>}
                </span>
                <span className="sa-muted-12">{Array.isArray(o.items) ? o.items.length : "—"}</span>
                <span className="sa-bold">{fmt(o.totalRub)} ₽</span>
                <span className="sa-status-cell">
                  <span className={`pill ${STATUS_TONE[o.status] ?? ""}`}>{STATUS_LABELS[o.status] ?? o.status}</span>
                  {expanded === o.id ? <ChevronUp size={14} className="sa-muted-shrink" /> : <ChevronDown size={14} className="sa-muted-shrink" />}
                </span>
              </button>

              {expanded === o.id && (
                <div>
                  <div className="mv-order-expand">
                    <div>
                      <strong>Доставка</strong>
                      <dl>
                        {Object.entries(o.delivery).map(([k, v]) => v ? (
                          <div key={k} className="sa-dl-row">
                            <dt className="sa-dl-term">{DELIVERY_LABELS[k] ?? k}</dt>
                            <dd>{String(v)}</dd>
                          </div>
                        ) : null)}
                        {o.comment && <div className="sa-order-comment">Комментарий: {o.comment}</div>}
                        {o.promoCode && <div className="sa-order-promo">Промокод: <code className="sa-inline-code sa-inline-code--accent">{o.promoCode}</code></div>}
                        {o.refCode && <div className="sa-order-promo">Реферал: <code className="sa-inline-code sa-inline-code--blue">{o.refCode}</code></div>}
                      </dl>
                    </div>
                    <div>
                      <strong>Позиции</strong>
                      <div className="sa-items-grid">
                        {Array.isArray(o.items) && o.items.map((item, idx) => {
                          const it = item as { name?: string; quantity?: number; priceRub?: number };
                          return (
                            <div key={idx} className="sa-item-row">
                              <span className="sa-item-name">
                                {it.name || `Товар ${idx + 1}`} × {it.quantity ?? 1}
                              </span>
                              <span className="sa-item-price">{fmt((it.priceRub ?? 0) * (it.quantity ?? 1))} ₽</span>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                  <div className="mv-order-status-bar">
                    <span className="sa-muted-12-mr4">Статус:</span>
                    {Object.entries(STATUS_LABELS).map(([s, label]) => (
                      <button
                        key={s}
                        type="button"
                        disabled={o.status === s || updateStatus.isPending}
                        onClick={() => updateStatus.mutate({ id: o.id, status: s })}
                        className={`secondary-action sa-status-btn${o.status === s ? " is-active" : ""}`}
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
        <div className="sa-scroll-x">
          <div className="table-panel customers-table" style={{ minWidth: 580 }}>
            <div className="table-head" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
              <span>Имя</span><span>Email</span><span>Телефон</span><span>Заказов</span><span>Регистрация</span>
            </div>
            {data.customers.map((c) => (
              <div key={c.id} className="table-row" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
                <span className="sa-bold">{[c.firstName, c.lastName].filter(Boolean).join(" ") || "—"}</span>
                <span className="sa-muted-soft-13">{c.email}</span>
                <span className="sa-text-13">{c.phone || "—"}</span>
                <span>
                  <span className="section-count">{c._count.orders}</span>
                </span>
                <span className="sa-muted-12">{fmtDate(c.createdAt)}</span>
              </div>
            ))}
            <Pagination page={page} total={data.total} pageSize={20} onChange={setPage} />
          </div>
        </div>
      )}
    </div>
  );
}

// ── Holiday Banners ───────────────────────────────────────────────────────────

const MONTH_NAMES = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"];

function HolidayBannersSection() {
  const qc = useQueryClient();
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<{ promoCode: string; title: string; subtitle: string }>({ promoCode: "", title: "", subtitle: "" });

  const { data: holidays = [], isLoading } = useQuery<HolidayPreset[]>({
    queryKey: ["shop-admin-holiday-banners"],
    queryFn: () => apiFetch<HolidayPreset[]>("/api/shop/admin/holiday-banners"),
    refetchInterval: 60_000,
  });

  const patchMut = useMutation({
    mutationFn: ({ key, patch }: { key: string; patch: object }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/holiday-banners/${key}`, { method: "PATCH", body: JSON.stringify(patch) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["shop-admin-holiday-banners"] });
      setEditingKey(null);
    },
  });

  const toggleActive = (h: HolidayPreset) => {
    const nextActive = !h.active;
    patchMut.mutate({ key: h.key, patch: { active: nextActive } });
  };

  const formatWindow = (start: [number, number], end: [number, number]) => {
    const [sm, sd] = start; const [em, ed] = end;
    return `${sd} ${MONTH_NAMES[sm - 1]} — ${ed} ${MONTH_NAMES[em - 1]}`;
  };

  if (isLoading) return <div className="list-loading"><Loader2 size={14} className="spin" /> Загружаю…</div>;

  return (
    <div className="sa-mb-32">
      <div className="section-title sa-mb-16">
        <div><h2>Праздничные баннеры</h2><p className="sa-note-p">Автоматически включаются в сезон. Можно включить/выключить вручную.</p></div>
      </div>
      {patchMut.error ? <div className="inline-error sa-mb-12">{(patchMut.error as Error).message}</div> : null}
      <div className="sa-banners-grid">
        {holidays.map((h) => (
          <div key={h.key} style={{
            border: `1px solid ${h.active ? "rgba(201,162,94,0.35)" : "rgba(255,255,255,0.07)"}`,
            borderRadius: 6, background: h.active ? "rgba(201,162,94,0.04)" : "var(--surface)",
            padding: "16px 18px", transition: "border-color 0.3s",
          }}>
            {editingKey === h.key ? (
              <div className="sa-flex-col-10">
                <div className="sa-flex-center-8-mb4">
                  <span className="sa-fs-20">{h.emoji}</span>
                  <strong className="sa-fs-14">{h.name}</strong>
                </div>
                <div className="mv-field">
                  <label>Промокод</label>
                  <input value={editForm.promoCode} onChange={e => setEditForm(f => ({ ...f, promoCode: e.target.value }))} placeholder={h.defaultPromoCode} />
                </div>
                <div className="mv-field">
                  <label>Заголовок баннера</label>
                  <input value={editForm.title} onChange={e => setEditForm(f => ({ ...f, title: e.target.value }))} placeholder={h.defaultTitle} />
                </div>
                <div className="mv-field">
                  <label>Подзаголовок</label>
                  <input value={editForm.subtitle} onChange={e => setEditForm(f => ({ ...f, subtitle: e.target.value }))} placeholder={h.defaultSubtitle} />
                </div>
                <div className="sa-flex-8">
                  <button
                    onClick={() => patchMut.mutate({ key: h.key, patch: { promoCode: editForm.promoCode, title: editForm.title, subtitle: editForm.subtitle } })}
                    disabled={patchMut.isPending}
                    className="primary-action sa-flex1"
                  >
                    {patchMut.isPending ? <Loader2 size={13} className="spin" /> : <Save size={13} />} Сохранить
                  </button>
                  <button onClick={() => setEditingKey(null)} className="secondary-action"><X size={13} /></button>
                </div>
              </div>
            ) : (
              <>
                <div className="sa-banner-head">
                  <div className="sa-flex-center-10">
                    <span className="sa-fs-24">{h.emoji}</span>
                    <div>
                      <div className="sa-bold-14">{h.name}</div>
                      <div className="sa-sub-date">
                        {formatWindow(h.windowStart, h.windowEnd)}
                        {h.inWindow && <span className="sa-now-badge">● сейчас</span>}
                      </div>
                    </div>
                  </div>
                  {/* Toggle switch */}
                  <button
                    onClick={() => toggleActive(h)}
                    disabled={patchMut.isPending}
                    title={h.active ? "Выключить" : "Включить"}
                    style={{
                      width: 42, height: 24, borderRadius: 12, border: "none", cursor: "pointer",
                      background: h.active ? "#c9a25e" : "rgba(255,255,255,0.12)",
                      position: "relative", transition: "background 0.25s", flexShrink: 0,
                    }}
                  >
                    <span style={{
                      position: "absolute", top: 3, left: h.active ? 21 : 3,
                      width: 18, height: 18, borderRadius: "50%",
                      background: "#fff", transition: "left 0.25s",
                    }} />
                  </button>
                </div>

                <div className="sa-banner-desc">
                  {h.title}
                </div>

                <div className="sa-flex-between-8">
                  <div className="sa-promo-badge">
                    <span className="sa-promo-label">Промокод</span>
                    <span className="sa-promo-value">{h.promoCode}</span>
                  </div>
                  <button
                    onClick={() => { setEditingKey(h.key); setEditForm({ promoCode: h.promoCode, title: h.title, subtitle: h.subtitle }); }}
                    className="secondary-action sa-btn-sm"
                  >
                    <Edit2 size={12} /> Изменить
                  </button>
                </div>

                {h.manualOverride && !h.inWindow && (
                  <div style={{ marginTop: 8, fontSize: 11, color: h.active ? "#c9a25e" : "var(--muted)" }}>
                    {h.active ? "⚡ Включён вручную (вне сезона)" : "✗ Выключен вручную"}
                  </div>
                )}
              </>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Banners ───────────────────────────────────────────────────────────────────

function BannerForm({ banner, onSave, onCancel, saving }: {
  banner?: Partial<ShopBanner>; onSave: (d: Partial<ShopBanner>) => void; onCancel: () => void; saving?: boolean;
}) {
  const [form, setForm] = useState<Partial<ShopBanner>>({
    imageUrl: "", title: "", subtitle: "", linkUrl: "", linkText: "", endDate: "", promoCode: "", active: true, ...banner,
  });
  const set = (k: keyof ShopBanner) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm((f) => ({ ...f, [k]: e.target.type === "checkbox" ? e.target.checked : e.target.value }));

  return (
    <div className="mv-form-section">
      {form.imageUrl && (
        <img src={form.imageUrl} alt="" className="mv-banner-preview sa-banner-img-sm"
          onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }} />
      )}
      <div className="mv-field-grid">
        {([
          ["imageUrl", "URL изображения", "https://..."],
          ["title", "Заголовок", "Летняя коллекция"],
          ["subtitle", "Подзаголовок", "Скидки до 50%"],
          ["linkUrl", "Ссылка", "/catalog/sale"],
          ["linkText", "Текст кнопки", "Смотреть акции"],
          ["promoCode", "Промокод в баннере (необязательно)", "SUMMER20"],
        ] as const).map(([key, label, placeholder]) => (
          <div key={key} className="mv-field">
            <label>{label}</label>
            <input
              value={(form[key] as string) ?? ""}
              onChange={set(key)}
              placeholder={placeholder}
              style={key === "promoCode" ? { fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.08em" } : undefined}
            />
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
        <div className="mv-field sa-field-end">
          <label className="sa-flex-center-8">
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
      <HolidayBannersSection />

      <div className="section-title">
        <div><h2>Кастомные баннеры</h2></div>
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
                    : <div className="sa-img-placeholder"><ImageIcon size={16} className="sa-muted-icon" /></div>
                  }
                </span>
                <span>
                  <div className="sa-bold">{b.title || "(без названия)"}</div>
                  {b.subtitle && <div className="sa-muted-12">{b.subtitle}</div>}
                </span>
                <span className="sa-muted-12-ellipsis">{b.linkUrl || "—"}</span>
                <span>
                  <span className={`pill ${b.active ? "success" : ""}`}>{b.active ? "Активен" : "Скрыт"}</span>
                </span>
                <span className="sa-flex-4">
                  <button onClick={() => setEditing(editing === b.id ? null : b.id)} className="secondary-action icon-action"><Edit2 size={14} /></button>
                  <button onClick={() => { if (confirm("Удалить баннер?")) deleteMut.mutate(b.id); }} className="icon-action danger"><Trash2 size={14} /></button>
                </span>
              </div>
              {editing === b.id && (
                <div className="sa-pad-bottom-12">
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
              <span className="sa-flex-center-8">
                {c.imageUrl && <img src={c.imageUrl} alt="" className="sa-cat-img" onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }} />}
                <span className="sa-bold">{c.name}</span>
              </span>
              <span className="sa-mono-muted-12">{c.slug}</span>
              <span className="sa-muted-12">{c.filterTag || "—"}</span>
              <span className="sa-flex-4">
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

interface AromaMesyatsaValue { offerId: string; note: string; validUntil?: string }

function AromaMesyatsaEditor({ value, onChange }: {
  value: AromaMesyatsaValue | null;
  onChange: (v: AromaMesyatsaValue | null) => void;
}) {
  const [enabled, setEnabled] = useState(!!value?.offerId);
  const [offerId, setOfferId] = useState(value?.offerId ?? "");
  const [note, setNote] = useState(value?.note ?? "");
  const [validUntil, setValidUntil] = useState(value?.validUntil ? value.validUntil.slice(0, 10) : "");

  useEffect(() => {
    if (value) { setEnabled(true); setOfferId(value.offerId ?? ""); setNote(value.note ?? ""); setValidUntil(value.validUntil ? value.validUntil.slice(0, 10) : ""); }
    else setEnabled(false);
  }, [value]);

  function update(o: string, n: string, vu: string) {
    if (!o.trim()) { onChange(null); return; }
    onChange({ offerId: o.trim(), note: n.trim(), validUntil: vu || undefined });
  }

  return (
    <div>
      <label className="sa-check-label">
        <input type="checkbox" checked={enabled} onChange={e => {
          setEnabled(e.target.checked);
          if (!e.target.checked) onChange(null);
        }} />
        <span className="sa-text-on-13">Показывать «Аромат месяца» на главной</span>
      </label>
      {enabled && (
        <div className="sa-aroma-panel">
          <div className="mv-field">
            <label>OfferId товара</label>
            <input
              value={offerId}
              onChange={e => { setOfferId(e.target.value); update(e.target.value, note, validUntil); }}
              placeholder="Например: 1234567890"
            />
            <span className="sa-hint-11">Найдите в каталоге товаров и скопируйте Ozon offerId</span>
          </div>
          <div className="mv-field">
            <label>Описание / история аромата</label>
            <textarea
              value={note}
              onChange={e => { setNote(e.target.value); update(offerId, e.target.value, validUntil); }}
              placeholder="Расскажите историю аромата, его характер, настроение…"
              rows={3}
            />
          </div>
          <div className="mv-field">
            <label>Актуален до (необязательно)</label>
            <input
              type="date"
              value={validUntil}
              onChange={e => { setValidUntil(e.target.value); update(offerId, note, e.target.value); }}
            />
            <span className="sa-hint-11">Если указана — покажем таймер «ещё N дней»</span>
          </div>
        </div>
      )}
    </div>
  );
}

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
    <div className="page-section sa-settings-wrap">
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
              <input type="number" step="0.05" min="0.5" max="20" value={form.markup ?? 2.2} onChange={setF("markup")} className="sa-w-110" />
              <span className="sa-muted-12">× (USD × курс)</span>
            </div>
            <span className="sa-note-11-mt4">
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

        {/* VIP club */}
        <div className="mv-field sa-field-mt12">
          <label>VIP-клуб — ссылка на Telegram-группу</label>
          <input value={(form as ShopSettings).vipTelegramLink ?? ""} onChange={setF("vipTelegramLink")} placeholder="https://t.me/+..." />
          <p className="sa-note-11-mt4">
            Ссылка отображается в кабинете покупателей с 3+ завершёнными заказами.
          </p>
        </div>

        {/* Аромат месяца */}
        <div className="mv-form-section sa-section-sep">
          <h3 className="sa-h3-mb12">🌸 Аромат месяца</h3>
          <p className="sa-desc-p">
            Выделенный аромат отображается на главной странице magicvibes.ru в отдельной секции. Обновляйте ежемесячно для создания повода возвращаться на сайт.
          </p>
          <AromaMesyatsaEditor value={(form as ShopSettings).aromaMesyatsa ?? null} onChange={(am) => setForm((f) => ({ ...f, aromaMesyatsa: am }))} />
        </div>

        {/* Flexible markup rules */}
        <div className="mv-field sa-field-mt20">
          <div className="sa-rule-head">
            <label className="sa-label-nm">Гибкие правила наценки</label>
            <button type="button" className="btn-outline sa-add-rule-btn" onClick={addRule}>
              <Plus size={13} /> Добавить правило
            </button>
          </div>
          <p className="sa-hint-p">
            Правило применяется, если цена закупки ≥ minUSD. При нескольких совпадениях побеждает наибольший порог.
          </p>

          {sortedRules.length === 0 ? (
            <div className="sa-no-rules">
              Гибких правил нет — используется коэффициент по умолчанию
            </div>
          ) : (
            <div className="sa-flex-col-8">
              {sortedRules.map((rule, i) => {
                const origIdx = rules.indexOf(rule);
                return (
                  <div key={i} className="sa-rule-row">
                    <span className="sa-rule-label">Цена от</span>
                    <input
                      type="number" min="0" step="1"
                      value={rule.minUsd}
                      onChange={e => updateRule(origIdx, "minUsd", e.target.value)}
                      className="sa-w-80-13"
                    />
                    <span className="sa-rule-label">USD → коэф.</span>
                    <input
                      type="number" min="0.5" max="20" step="0.05"
                      value={rule.coefficient}
                      onChange={e => updateRule(origIdx, "coefficient", e.target.value)}
                      className="sa-w-90-13"
                    />
                    <span className="sa-rule-note">
                      ≈ {(rule.coefficient * 100 - 100).toFixed(0)}% наценка
                    </span>
                    <button type="button" onClick={() => removeRule(origIdx)} className="sa-del-rule-btn">
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
        <div className="success-strip sa-flex-center-8">
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
        <div><h2>Новости из Telegram</h2><p className="sa-muted-13-mt4">Посты с хэштегом #новости из канала @magicvibes_ru</p></div>
        <div className="sa-flex-8">
          <button onClick={() => void importMut.mutate()} disabled={importMut.isPending} className="secondary-action" type="button">
            <RefreshCw size={14} className={importMut.isPending ? "spin" : ""} /> Импортировать
          </button>
          <button onClick={() => void refetch()} disabled={isLoading} className="secondary-action" type="button">
            <RefreshCw size={14} className={isLoading ? "spin" : ""} /> Обновить
          </button>
        </div>
      </div>

      {importMut.isSuccess && (
        <div className="sa-import-ok">
          Импорт запущен. Новые посты появятся через несколько секунд.
        </div>
      )}

      {isLoading && <div className="soft-empty"><Loader2 size={18} className="spin" /> Загрузка…</div>}

      {!isLoading && !data?.posts.length && (
        <div className="soft-empty"><Newspaper size={18} /> Новостей нет. Нажмите «Импортировать» чтобы загрузить посты из Telegram.</div>
      )}

      <div className="sa-news-grid">
        {data?.posts.map((post) => (
          <div key={post.id} className="card-panel" style={{ opacity: post.active ? 1 : 0.5 }}>
            {post.photoUrl && (
              <img src={post.photoUrl} alt="" className="sa-news-img" />
            )}
            <p className="sa-news-date">
              {new Date(post.publishedAt).toLocaleString("ru-RU")}
            </p>
            <p className="sa-news-text">
              {post.text.slice(0, 200)}{post.text.length > 200 && "…"}
            </p>
            <div className="sa-flex-between">
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

      <div className="sa-scroll-x">
      <div className="table-panel sa-table-560">
        {data?.reviews.map((r) => (
          <div key={r.id} className="table-row sa-grid-reviews">
            <span>
              <div className="sa-bold-12">{r.customer?.email ?? "—"}</div>
              <div className="sa-sub-date">{fmtDate(r.createdAt)}</div>
            </span>
            <span className="sa-flex-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Star key={i} size={11} fill={i < r.rating ? "gold" : "none"} stroke={i < r.rating ? "gold" : "var(--border-md)"} />
              ))}
            </span>
            <span>
              {r.productName && <div className="sa-product-hint">{r.productName}</div>}
              <div className="sa-text-13">{r.text.slice(0, 120)}{r.text.length > 120 && "…"}</div>
            </span>
            <span><span className={`pill ${r.approved ? "success" : "warn"}`}>{r.approved ? "Показывается" : "Скрыт"}</span></span>
            <span className="sa-flex-6">
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
    <div className="page-section sa-mt-0">
      <p className="section-title">Распаковки покупателей</p>
      {isLoading && <p className="sa-muted-text-13">Загрузка…</p>}
      {!isLoading && !unboxings.length && (
        <p className="sa-muted-text-13">Нет распаковок</p>
      )}
      <div className="table-panel sa-col-0">
        {unboxings.map((u) => (
          <div key={u.id} className="sa-unboxing-row">
            <span className="sa-text-on-13">{u.name}</span>
            <span className="sa-muted-text-12">
              {new Date(u.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "short", year: "numeric" })}
            </span>
            <span className="sa-muted-text-12-ellipsis">
              {u.text ? u.text.slice(0, 80) + (u.text.length > 80 ? "…" : "") : "—"}
            </span>
            {u.mediaUrl ? (
              <a href={u.mediaUrl} target="_blank" rel="noreferrer" style={{ fontSize: 12, color: "var(--accent)", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                Ссылка
              </a>
            ) : (
              <span className="sa-muted-text-12">—</span>
            )}
            <span><span className={`pill ${u.approved ? "success" : "warn"}`}>{u.approved ? "Показывается" : "На проверке"}</span></span>
            <span className="sa-flex-6">
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

// ── BlogTab ───────────────────────────────────────────────────────────────────

interface BlogPost {
  id: string; slug: string; title: string; excerpt?: string;
  content?: string; coverUrl?: string; tags: string[];
  published: boolean; publishedAt?: string; createdAt: string;
}

const EMPTY_POST: Omit<BlogPost, "id" | "slug" | "createdAt"> = {
  title: "", excerpt: "", content: "", coverUrl: "", tags: [], published: false,
};

function BlogTab() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<Partial<BlogPost> & { id?: string } | null>(null);
  const [tagInput, setTagInput] = useState("");

  const { data, isLoading } = useQuery({
    queryKey: ["admin-blog"],
    queryFn: () => apiFetch<{ ok: boolean; posts: BlogPost[] }>("/api/shop/admin/blog"),
  });

  const saveMut = useMutation({
    mutationFn: (post: Partial<BlogPost> & { id?: string }) => {
      if (post.id) {
        return apiFetch<{ ok: boolean }>(`/api/shop/admin/blog/${post.id}`, { method: "PUT", body: JSON.stringify(post) });
      }
      return apiFetch<{ ok: boolean }>("/api/shop/admin/blog", { method: "POST", body: JSON.stringify(post) });
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["admin-blog"] }); setEditing(null); },
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => apiFetch<{ ok: boolean }>(`/api/shop/admin/blog/${id}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["admin-blog"] }),
  });

  const posts = data?.posts ?? [];

  if (editing !== null) {
    return (
      <div className="admin-tab-content">
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 20 }}>
          <button type="button" className="secondary-action icon-action" onClick={() => setEditing(null)}>
            <ChevronLeft size={15} />
          </button>
          <h3 className="sa-h3">{editing.id ? "Редактировать статью" : "Новая статья"}</h3>
        </div>

        <div style={{ display: "grid", gap: 12, maxWidth: 700 }}>
          <div>
            <label className="sa-form-label">Заголовок</label>
            <input className="input-base" value={editing.title ?? ""} onChange={e => setEditing(s => ({ ...s!, title: e.target.value }))} placeholder="Топ-10 ароматов весны 2027" maxLength={200} />
          </div>
          <div>
            <label className="sa-form-label">URL-slug (оставьте пустым для авто)</label>
            <input className="input-base" value={editing.slug ?? ""} onChange={e => setEditing(s => ({ ...s!, slug: e.target.value }))} placeholder="top-10-vesna-2027" maxLength={120} />
          </div>
          <div>
            <label className="sa-form-label">Обложка (URL изображения)</label>
            <input className="input-base" value={editing.coverUrl ?? ""} onChange={e => setEditing(s => ({ ...s!, coverUrl: e.target.value }))} placeholder="https://..." />
          </div>
          <div>
            <label className="sa-form-label">Краткое описание</label>
            <textarea className="input-base sa-resize-v" value={editing.excerpt ?? ""} onChange={e => setEditing(s => ({ ...s!, excerpt: e.target.value }))} rows={2} maxLength={500} />
          </div>
          <div>
            <label className="sa-form-label">Теги (через запятую)</label>
            <input
              className="input-base"
              value={tagInput}
              onChange={e => setTagInput(e.target.value)}
              onBlur={() => {
                const tags = tagInput.split(",").map(t => t.trim()).filter(Boolean);
                setEditing(s => ({ ...s!, tags }));
              }}
              placeholder="парфюмерия, новинки, гид"
            />
            {(editing.tags?.length ?? 0) > 0 && (
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 6 }}>
                {editing.tags!.map(t => (
                  <span key={t} style={{ fontSize: 11, padding: "2px 8px", border: "1px solid var(--border)", borderRadius: 2, color: "var(--text-muted)" }}>{t}</span>
                ))}
              </div>
            )}
          </div>
          <div>
            <label className="sa-form-label">Содержание (HTML)</label>
            <textarea className="input-base" value={editing.content ?? ""} onChange={e => setEditing(s => ({ ...s!, content: e.target.value }))} rows={12} style={{ resize: "vertical", fontFamily: "monospace", fontSize: 12 }} placeholder="<h2>Заголовок</h2><p>Текст статьи...</p>" />
          </div>
          <div className="sa-flex-center-10">
            <label style={{ fontSize: 13, cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}>
              <input type="checkbox" checked={editing.published ?? false} onChange={e => setEditing(s => ({ ...s!, published: e.target.checked }))} />
              Опубликовать
            </label>
          </div>

          {saveMut.isError && (
            <p style={{ fontSize: 12, color: "#f87171" }}>{(saveMut.error as Error).message}</p>
          )}

          <div className="sa-flex-8">
            <button
              type="button"
              className="secondary-action sa-inline-flex-6"
              onClick={() => {
                if (!editing.title?.trim()) return;
                void saveMut.mutate(editing);
              }}
              disabled={saveMut.isPending || !editing.title?.trim()}
            >
              {saveMut.isPending ? <Loader2 size={14} className="spin" /> : <Save size={14} />}
              Сохранить
            </button>
            <button type="button" className="secondary-action" onClick={() => setEditing(null)}>Отмена</button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="admin-tab-content">
      <div className="sa-section-head">
        <h3 className="sa-h3">Блог «Мир ароматов»</h3>
        <button
          type="button"
          className="secondary-action sa-inline-flex-6"
          onClick={() => { setEditing({ ...EMPTY_POST }); setTagInput(""); }}
        >
          <Plus size={14} /> Новая статья
        </button>
      </div>

      {isLoading && <p className="sa-muted-text-13">Загрузка…</p>}
      {!isLoading && posts.length === 0 && (
        <p className="sa-muted-text-13">Статей нет. Создайте первую!</p>
      )}

      <div className="table-panel sa-col-0">
        {posts.map((p) => (
          <div key={p.id} style={{ display: "grid", gridTemplateColumns: "1fr auto auto auto auto", alignItems: "center", gap: 10, padding: "10px 14px", borderBottom: "1px solid var(--border)" }}>
            <div>
              <div style={{ fontSize: 13, color: "var(--text)", fontWeight: 500 }}>{p.title}</div>
              <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 2 }}>
                /{p.slug}
                {p.tags?.length > 0 && <span style={{ marginLeft: 8 }}>{p.tags.join(", ")}</span>}
              </div>
            </div>
            <span style={{ fontSize: 11, padding: "3px 8px", borderRadius: 2, border: "1px solid var(--border)", color: p.published ? "#22c55e" : "var(--text-muted)" }}>
              {p.published ? "Опубл." : "Черновик"}
            </span>
            <span style={{ fontSize: 11, color: "var(--text-muted)", whiteSpace: "nowrap" }}>
              {new Date(p.createdAt).toLocaleDateString("ru-RU")}
            </span>
            <button
              type="button"
              className="secondary-action icon-action"
              onClick={() => { setEditing({ ...p }); setTagInput(p.tags?.join(", ") ?? ""); }}
              title="Редактировать"
            >
              <Edit2 size={13} />
            </button>
            <button
              type="button"
              className="secondary-action icon-action danger"
              onClick={() => { if (confirm(`Удалить «${p.title}»?`)) void deleteMut.mutate(p.id); }}
              disabled={deleteMut.isPending}
            >
              <Trash2 size={13} />
            </button>
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
      <div className="sa-section-head">
        <h3 className="sa-h3">Email-цепочки после покупки</h3>
        <button
          type="button"
          className="secondary-action sa-inline-flex-6"
          onClick={() => void runMut.mutate()}
          disabled={runMut.isPending}
        >
          {runMut.isPending ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />}
          Запустить сканер
        </button>
      </div>

      {runMut.isSuccess && runMut.data && (
        <div className="sa-success-banner">
          Готово: День&nbsp;7 — {runMut.data.result.sent7 ?? 0} шт., День&nbsp;30 — {runMut.data.result.sent30 ?? 0} шт.
          {(runMut.data.result.errors ?? 0) > 0 && <span className="sa-err-inline">ошибок: {runMut.data.result.errors}</span>}
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
                    <div className="sa-muted-text-12">{meta.desc}</div>
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

// ── PromocodesTab ─────────────────────────────────────────────────────────────

interface PromoCode {
  id: string; code: string; discountPct: number; active: boolean;
  usageLimit: number | null; usageCount: number; note?: string | null;
  expiresAt?: string | null; builtin?: boolean; createdAt?: string;
}

function PromocodesTab() {
  const qc = useQueryClient();
  const [creating, setCreating] = useState(false);
  const [newForm, setNewForm] = useState({ code: "", discountPct: 10, note: "", usageLimit: "", expiresAt: "" });
  const [copied, setCopied] = useState<string | null>(null);

  const { data, isLoading, refetch } = useQuery<{ ok: boolean; codes: PromoCode[] }>({
    queryKey: ["shop-admin-promo-codes"],
    queryFn: () => apiFetch<{ ok: boolean; codes: PromoCode[] }>("/api/shop/admin/promo-codes"),
  });

  const toggleMut = useMutation({
    mutationFn: ({ id, active }: { id: string; active: boolean }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/promo-codes/${id}`, { method: "PATCH", body: JSON.stringify({ active }) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shop-admin-promo-codes"] }),
  });

  const createMut = useMutation({
    mutationFn: (d: object) => apiFetch<{ ok: boolean; code: PromoCode }>("/api/shop/admin/promo-codes", {
      method: "POST", body: JSON.stringify(d),
    }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["shop-admin-promo-codes"] }); setCreating(false); setNewForm({ code: "", discountPct: 10, note: "", usageLimit: "", expiresAt: "" }); },
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => apiFetch<{ ok: boolean }>(`/api/shop/admin/promo-codes/${id}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shop-admin-promo-codes"] }),
  });

  function copyCode(code: string) {
    navigator.clipboard.writeText(code).catch(() => {});
    setCopied(code);
    setTimeout(() => setCopied(null), 1500);
  }

  const codes = data?.codes ?? [];
  const activeCodes = codes.filter(c => c.active);

  return (
    <div className="page-section">
      <div className="section-title">
        <div>
          <h2>Промокоды</h2>
          <p className="sa-note-p">
            Активных: <strong className="sa-success-text">{activeCodes.length}</strong> из {codes.length}
          </p>
        </div>
        <div className="sa-flex-8">
          <button onClick={() => void refetch()} disabled={isLoading} className="secondary-action" type="button">
            <RefreshCw size={14} className={isLoading ? "spin" : ""} />
          </button>
          <button onClick={() => setCreating(true)} className="primary-action" type="button">
            <Plus size={15} /> Создать промокод
          </button>
        </div>
      </div>

      {/* Warning if all disabled */}
      {codes.length > 0 && activeCodes.length === 0 && (
        <div style={{ padding: "10px 14px", borderRadius: 8, background: "rgba(251,191,36,.07)", border: "1px solid rgba(251,191,36,.3)", fontSize: 12, color: "#fbbf24", marginBottom: 16 }}>
          ⚠ Все промокоды отключены — покупатели не смогут получить скидку
        </div>
      )}

      {/* Create form */}
      {creating && (
        <div className="mv-form-section" style={{ marginBottom: 20 }}>
          <h3>Новый промокод</h3>
          <div className="mv-field-grid">
            <div className="mv-field">
              <label>Код (латиницей, без пробелов)</label>
              <input
                value={newForm.code}
                onChange={e => setNewForm(f => ({ ...f, code: e.target.value.toUpperCase().replace(/\s+/g, "") }))}
                placeholder="SUMMER20"
                style={{ fontFamily: "monospace", letterSpacing: "0.08em" }}
                maxLength={32}
              />
            </div>
            <div className="mv-field">
              <label>Скидка (%)</label>
              <input type="number" min={1} max={100} value={newForm.discountPct}
                onChange={e => setNewForm(f => ({ ...f, discountPct: Number(e.target.value) }))} />
            </div>
            <div className="mv-field">
              <label>Описание (для себя)</label>
              <input value={newForm.note} onChange={e => setNewForm(f => ({ ...f, note: e.target.value }))} placeholder="Летняя акция 2026" />
            </div>
            <div className="mv-field">
              <label>Лимит использований (пусто = безлимит)</label>
              <input type="number" min={1} value={newForm.usageLimit}
                onChange={e => setNewForm(f => ({ ...f, usageLimit: e.target.value }))} placeholder="100" />
            </div>
            <div className="mv-field">
              <label>Действует до (необязательно)</label>
              <input type="datetime-local" value={newForm.expiresAt}
                onChange={e => setNewForm(f => ({ ...f, expiresAt: e.target.value }))} />
            </div>
          </div>
          {createMut.isError && <div className="inline-error">{String(createMut.error)}</div>}
          <div className="row-actions">
            <button
              onClick={() => createMut.mutate({ code: newForm.code, discountPct: newForm.discountPct, note: newForm.note || null, usageLimit: newForm.usageLimit ? Number(newForm.usageLimit) : null, expiresAt: newForm.expiresAt || null })}
              disabled={createMut.isPending || !newForm.code.trim()}
              className="primary-action"
            >
              {createMut.isPending ? <Loader2 size={14} className="spin" /> : <Save size={14} />} Создать
            </button>
            <button onClick={() => setCreating(false)} className="secondary-action"><X size={14} /> Отмена</button>
          </div>
        </div>
      )}

      {isLoading ? (
        <div className="list-loading"><Loader2 size={16} className="spin" /> Загружаю промокоды…</div>
      ) : (
        <div className="sa-scroll-x">
          <div className="table-panel" style={{ minWidth: 600 }}>
            <div className="table-head" style={{ display: "grid", gridTemplateColumns: "minmax(120px,.8fr) minmax(60px,.35fr) minmax(180px,1.5fr) minmax(80px,.5fr) minmax(80px,.5fr) 80px 60px", gap: 10 }}>
              <span>Промокод</span><span>Скидка</span><span>Описание</span><span>Использований</span><span>Истекает</span><span>Статус</span><span />
            </div>
            {codes.map((c) => (
              <div key={c.id} className="table-row" style={{ display: "grid", gridTemplateColumns: "minmax(120px,.8fr) minmax(60px,.35fr) minmax(180px,1.5fr) minmax(80px,.5fr) minmax(80px,.5fr) 80px 60px", gap: 10, alignItems: "center" }}>
                <span className="sa-flex-center-8">
                  <code style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, letterSpacing: "0.08em", color: c.active ? "var(--accent)" : "var(--muted)", background: "rgba(0,0,0,0.3)", padding: "2px 8px", borderRadius: 4 }}>
                    {c.code}
                  </code>
                  {c.builtin && <span style={{ fontSize: 9, letterSpacing: "0.1em", color: "var(--muted)", background: "rgba(255,255,255,0.06)", border: "1px solid var(--border)", borderRadius: 2, padding: "1px 5px" }}>ВСТРОЕН</span>}
                  <button type="button" onClick={() => copyCode(c.code)} title="Скопировать" style={{ background: "none", border: "none", cursor: "pointer", color: "var(--muted)", padding: 2 }}>
                    {copied === c.code ? <Check size={12} className="sa-success-text" /> : <Copy size={12} />}
                  </button>
                </span>
                <span style={{ fontWeight: 700, color: "var(--accent)", fontSize: 14 }}>
                  <Percent size={11} style={{ verticalAlign: "middle", marginRight: 1 }} />{c.discountPct}
                </span>
                <span className="sa-muted-12">{c.note || "—"}</span>
                <span style={{ fontSize: 12 }}>
                  {c.usageCount}
                  {c.usageLimit !== null && <span style={{ color: "var(--muted)" }}> / {c.usageLimit}</span>}
                </span>
                <span style={{ fontSize: 11, color: "var(--muted)" }}>
                  {c.expiresAt ? new Date(c.expiresAt) < new Date()
                    ? <span style={{ color: "var(--danger)" }}>Истёк</span>
                    : fmtDate(c.expiresAt)
                  : "Бессрочно"}
                </span>
                <span>
                  <button
                    type="button"
                    onClick={() => toggleMut.mutate({ id: c.id, active: !c.active })}
                    disabled={toggleMut.isPending}
                    title={c.active ? "Отключить" : "Включить"}
                    style={{
                      display: "flex", alignItems: "center", gap: 5,
                      background: c.active ? "rgba(74,222,128,0.12)" : "rgba(255,255,255,0.06)",
                      border: `1px solid ${c.active ? "rgba(74,222,128,0.3)" : "var(--border)"}`,
                      borderRadius: 6, padding: "4px 10px", cursor: "pointer",
                      fontSize: 11, color: c.active ? "#4ade80" : "var(--muted)", fontWeight: 600,
                    }}
                  >
                    {c.active ? <><ToggleRight size={13} /> Вкл</> : <><ToggleLeft size={13} /> Выкл</>}
                  </button>
                </span>
                <span className="sa-flex-4">
                  {!c.builtin && (
                    <button
                      type="button"
                      onClick={() => { if (confirm(`Удалить промокод ${c.code}?`)) deleteMut.mutate(c.id); }}
                      disabled={deleteMut.isPending}
                      className="icon-action danger"
                      title="Удалить"
                    >
                      <Trash2 size={13} />
                    </button>
                  )}
                </span>
              </div>
            ))}
            {codes.length === 0 && (
              <div className="soft-empty"><Tag size={18} /> Промокодов нет</div>
            )}
          </div>
        </div>
      )}

      <div style={{ marginTop: 20, padding: "12px 16px", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 12, color: "var(--muted)", lineHeight: 1.7 }}>
        <strong style={{ color: "var(--text)" }}>Встроенные промокоды</strong> нельзя удалить, только отключить.<br />
        <strong style={{ color: "var(--accent)" }}>VIBES10</strong> — попап на сайте, −10%. <strong style={{ color: "var(--accent)" }}>QUIZ10</strong> — квиз, −10%. <strong style={{ color: "var(--accent)" }}>REVIEW5</strong> — отзыв, −5%. <strong style={{ color: "var(--accent)" }}>UNBOX7</strong> — анбоксинг, −7%.<br />
        Промокод применяется в корзине покупателем при оформлении заказа.
      </div>
    </div>
  );
}

// ── EmailSubscribersTab ───────────────────────────────────────────────────────

interface EmailSubscriber {
  id: string; email: string; source: string;
  quizCategory?: string | null; promoSent: boolean;
  unsubscribed: boolean; createdAt: string;
}

function EmailSubscribersTab() {
  const [page, setPage] = useState(1);
  const [sourceFilter, setSourceFilter] = useState("");

  const { data, isLoading } = useQuery<{ ok: boolean; subscribers: EmailSubscriber[]; total: number }>({
    queryKey: ["shop-admin-email-subscribers", { page, sourceFilter }],
    queryFn: () => apiFetch<{ ok: boolean; subscribers: EmailSubscriber[]; total: number }>(
      `/api/shop/admin/email-subscribers-list?page=${page}${sourceFilter ? `&source=${sourceFilter}` : ""}`
    ),
  });

  const COL = "minmax(180px,1.5fr) minmax(80px,.5fr) minmax(120px,.8fr) minmax(80px,.5fr) minmax(120px,.8fr)";

  return (
    <div className="page-section">
      <div className="section-title" style={{ flexWrap: "wrap", gap: 10 }}>
        <div>
          <h2>Email-подписчики</h2>
          <p className="sa-note-p">Всего: {data?.total ?? "…"}</p>
        </div>
        <div className="sa-flex-6">
          {["", "popup", "quiz"].map(s => (
            <button key={s} onClick={() => { setSourceFilter(s); setPage(1); }}
              className={`secondary-action sa-filter-btn${sourceFilter === s ? " is-active" : ""}`}>
              {s === "" ? "Все" : s === "popup" ? "Попап" : "Квиз"}
            </button>
          ))}
        </div>
      </div>

      {isLoading ? (
        <div className="list-loading"><Loader2 size={16} className="spin" /> Загружаю…</div>
      ) : !data?.subscribers.length ? (
        <div className="soft-empty"><Mail size={18} /> Подписчиков нет</div>
      ) : (
        <div className="sa-scroll-x">
          <div className="table-panel sa-table-560">
            <div className="table-head" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
              <span>Email</span><span>Источник</span><span>Квиз</span><span>Промокод</span><span>Дата</span>
            </div>
            {data.subscribers.map(s => (
              <div key={s.id} className="table-row" style={{ display: "grid", gridTemplateColumns: COL, gap: 10 }}>
                <span style={{ fontSize: 13 }}>{s.email}</span>
                <span>
                  <span className={`pill ${s.source === "popup" ? "info" : ""}`}>{s.source}</span>
                </span>
                <span className="sa-muted-12">{s.quizCategory || "—"}</span>
                <span>
                  <span className={`pill ${s.promoSent ? "success" : "warn"}`}>{s.promoSent ? "Отправлен" : "Не отправлен"}</span>
                </span>
                <span className="sa-muted-12">{fmtDate(s.createdAt)}</span>
              </div>
            ))}
            <Pagination page={page} total={data.total} pageSize={50} onChange={setPage} />
          </div>
        </div>
      )}
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
        <h3 className="sa-h3">Web Push — рассылка уведомлений</h3>
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
        <div className="sa-success-banner">
          Отправлено: {sendMut.data.sent} / {sendMut.data.total}
          {sendMut.data.failed > 0 && <span className="sa-err-inline">ошибок: {sendMut.data.failed} (истёкшие подписки удалены)</span>}
        </div>
      )}

      <div style={{ display: "grid", gap: 10, maxWidth: 540 }}>
        <div>
          <label className="sa-form-label">Заголовок</label>
          <input
            className="input-base"
            value={form.title}
            onChange={e => setForm(f => ({ ...f, title: e.target.value }))}
            placeholder="Новинки февраля — Chanel, Dior, Tom Ford"
            maxLength={80}
          />
        </div>
        <div>
          <label className="sa-form-label">Текст уведомления</label>
          <textarea
            className="input-base sa-resize-v"
            value={form.body}
            onChange={e => setForm(f => ({ ...f, body: e.target.value }))}
            placeholder="Поступили долгожданные ароматы. Успей выбрать!"
            rows={2}
            maxLength={200}
          />
        </div>
        <div>
          <label className="sa-form-label">URL (куда ведёт клик)</label>
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

type Tab = "dashboard" | "orders" | "customers" | "banners" | "categories" | "news" | "reviews" | "unboxings" | "blog" | "emails" | "push" | "promocodes" | "subscribers" | "settings";

const TABS: { id: Tab; label: string; icon: React.ComponentType<{ size?: number }> }[] = [
  { id: "dashboard", label: "Обзор", icon: LayoutDashboard },
  { id: "orders", label: "Заказы", icon: ClipboardList },
  { id: "customers", label: "Покупатели", icon: UserCheck },
  { id: "banners", label: "Баннеры", icon: Image },
  { id: "categories", label: "Категории", icon: Tag },
  { id: "promocodes", label: "Промокоды", icon: Percent },
  { id: "subscribers", label: "Подписчики", icon: Mail },
  { id: "news", label: "Новости", icon: Newspaper },
  { id: "reviews", label: "Отзывы", icon: Star },
  { id: "unboxings", label: "Анбоксинг", icon: Video },
  { id: "blog", label: "Блог", icon: BookOpen },
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
      {tab === "blog" && <BlogTab />}
      {tab === "emails" && <EmailSequencesTab />}
      {tab === "push" && <PushTab />}
      {tab === "promocodes" && <PromocodesTab />}
      {tab === "subscribers" && <EmailSubscribersTab />}
      {tab === "settings" && <SettingsTab />}
    </section>
  );
}
