"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { Package, User, Loader2, CheckCircle, Clock, Truck, LogOut, Save, MapPin, Gift, RotateCcw, X, ChevronDown, Copy, CreditCard } from "lucide-react";
import { useAuth } from "@/components/AuthContext";
import { useCart } from "@/components/CartContext";
import type { ShopOrder, ShopProduct } from "@/lib/types";
import { productImg } from "@/lib/img";
import { loadAddresses, forgetAddress, prettyAddress, type SavedAddress } from "@/lib/addresses";

const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";
const AUTH_API = API + "/auth";

type Tab = "orders" | "addresses" | "profile" | "bonus";

const STATUS: Record<string, { label: string; tone: string; icon: typeof Package }> = {
  pending:         { label: "Принят",         tone: "wait", icon: Clock },
  payment_pending: { label: "Ожидает оплаты", tone: "wait", icon: Clock },
  payment_failed:  { label: "Оплата не прошла", tone: "bad", icon: X },
  paid:            { label: "Оплачен",        tone: "ok",   icon: CheckCircle },
  confirmed:       { label: "Подтверждён",    tone: "ok",   icon: CheckCircle },
  picking:         { label: "Собирается",     tone: "info", icon: Package },
  shipped:         { label: "В пути",         tone: "info", icon: Truck },
  delivered:       { label: "Доставлен",      tone: "ok",   icon: CheckCircle },
  cancelled:       { label: "Отменён",        tone: "bad",  icon: X },
};
// progress steps shown on each order
const STEPS = ["paid", "picking", "shipped", "delivered"];
const STEP_LABEL = ["Оплачен", "Собираем", "В пути", "Получен"];

const rub = (n: number) => `${Math.round(n).toLocaleString("ru-RU")} ₽`;
function plural(n: number, one: string, few: string, many: string) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return one;
  if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return few;
  return many;
}

/* ─────────────── orders ─────────────── */
function OrderCard({ order }: { order: ShopOrder }) {
  const { add } = useCart();
  const { token } = useAuth();
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [paying, setPaying] = useState(false);
  const [payErr, setPayErr] = useState<string | null>(null);
  const unpaid = ["pending", "payment_pending", "payment_failed"].includes(order.status);

  // back to the Ozon Pay page: pay, pick the pickup point / courier address there
  async function pay() {
    setPaying(true);
    setPayErr(null);
    try {
      const r = await fetch(`${API}/auth/orders/${encodeURIComponent(order.id)}/pay`, {
        method: "POST", headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      });
      const d = await r.json().catch(() => ({}));
      if (d.paymentUrl) { window.location.href = d.paymentUrl; return; }
      if (d.paid) { window.location.reload(); return; }
      setPayErr(d.error || "Не удалось открыть оплату");
    } catch {
      setPayErr("Нет связи — попробуйте ещё раз");
    }
    setPaying(false);
  }
  const st = STATUS[order.status] || { label: order.status, tone: "info", icon: Package };
  const Icon = st.icon;
  const items = order.items || [];
  const date = new Date(order.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  const stepIdx = order.status === "confirmed" ? 0 : STEPS.indexOf(order.status);
  const d = order.delivery || {};

  async function repeat() {
    setBusy(true);
    try {
      for (const it of items) {
        const r = await fetch(`${API}/product/${encodeURIComponent(it.offerId)}`);
        if (!r.ok) continue;
        const p = (await r.json()) as ShopProduct;
        if (p?.offerId && p.inStock) add(p, it.quantity || 1);
      }
      router.push("/cart");
    } finally { setBusy(false); }
  }

  return (
    <article className="mv-acc-order">
      <header className="mv-acc-order-h">
        <div>
          <div className="mv-acc-order-date">{date}</div>
          <div className="mv-acc-order-id">№ {order.id.replace(/^MV-/, "")}</div>
        </div>
        <span className={`mv-acc-status ${st.tone}`}><Icon size={13} /> {st.label}</span>
      </header>

      <div className="mv-acc-thumbs">
        {items.slice(0, 5).map((it, i) => (
          <Link key={i} href={it.slug ? `/product/${it.slug}` : "#"} className="mv-acc-thumb" title={it.name || it.offerId}>
            {it.image
              // eslint-disable-next-line @next/next/no-img-element
              ? <img src={productImg(it.image, 160)} alt={it.name || ""} loading="lazy" />
              : <b>{(it.brand || it.name || "?")[0]}</b>}
            {it.quantity > 1 && <em>{it.quantity}</em>}
          </Link>
        ))}
        {items.length > 5 && <span className="mv-acc-more">+{items.length - 5}</span>}
        <div className="mv-acc-order-sum">{rub(order.totalRub)}</div>
      </div>

      {stepIdx >= 0 && (
        <ol className="mv-acc-steps">
          {STEP_LABEL.map((l, i) => <li key={l} className={i <= stepIdx ? "done" : ""}><span />{l}</li>)}
        </ol>
      )}

      <button type="button" className="mv-acc-toggle" onClick={() => setOpen((v) => !v)} aria-expanded={open}>
        {items.length} {plural(items.length, "товар", "товара", "товаров")} · подробнее <ChevronDown size={15} style={{ transform: open ? "rotate(180deg)" : "none", transition: "transform .2s" }} />
      </button>
      {open && (
        <div className="mv-acc-details">
          {items.map((it, i) => (
            <div key={i} className="mv-acc-line">
              <span className="mv-acc-line-name">{it.name || it.offerId}{it.volume ? `, ${it.volume}` : ""}</span>
              <span className="mv-acc-line-q">×{it.quantity}</span>
              <span className="mv-acc-line-p">{rub(it.priceRub * it.quantity)}</span>
            </div>
          ))}
          {typeof d.priceRub === "number" && (
            <div className="mv-acc-line">
              <span className="mv-acc-line-name">{d.carrierTitle || `Доставка${d.type === "ozon_pay" ? " Ozon" : ""}`}</span>
              <span className="mv-acc-line-q" />
              <span className="mv-acc-line-p">{d.priceRub > 0 ? rub(d.priceRub) : "бесплатно"}</span>
            </div>
          )}
          {d.type === "ozon_pay" && !d.address && (
            <div className="mv-acc-addr"><MapPin size={14} /> {unpaid ? "Пункт выдачи или адрес курьера выберете на странице оплаты Ozon" : "Адрес доставки выбран в Ozon Pay"}</div>
          )}
          {(d.address || d.city) && (
            <div className="mv-acc-addr"><MapPin size={14} /> {d.pvzName ? `${d.pvzName}, ` : ""}{[d.city, d.address].filter(Boolean).join(", ")}</div>
          )}
          {d.shipment && (
            <div className="mv-acc-addr">
              <Truck size={14} /> {d.shipment.statusLabel || "Отправка создана"}{d.shipment.number ? ` · трек ${d.shipment.number}` : ""}
              {order.trackUrl ? <> · <Link href={order.trackUrl} style={{ color: "var(--accent)", fontWeight: 700 }}>{d.carrier === "dostavista" ? "где курьер на карте" : "отследить"}</Link></>
                : d.shipment.trackingUrl && <> · <a href={d.shipment.trackingUrl} target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)" }}>отследить</a></>}
            </div>
          )}
        </div>
      )}

      {unpaid && (
        <div className="mv-acc-pay">
          <div>
            <b>Заказ ждёт оплаты</b>
            <span>{d.type === "ozon_pay" ? "Оплатите и выберите пункт выдачи или курьера на странице Ozon Pay" : "Оплатите заказ на странице Ozon Pay — товары зарезервированы"}</span>
          </div>
          <button type="button" className="mv-acc-btn pay" onClick={pay} disabled={paying}>
            {paying ? <Loader2 size={15} className="mv-spin" /> : <CreditCard size={15} />} Оплатить {rub(order.totalRub)}
          </button>
          {payErr && <span className="mv-acc-pay-err">{payErr}</span>}
        </div>
      )}

      <footer className="mv-acc-order-f">
        <button type="button" className={`mv-acc-btn${unpaid ? " ghost" : ""}`} onClick={repeat} disabled={busy || !items.length}>
          {busy ? <Loader2 size={15} className="mv-spin" /> : <RotateCcw size={15} />} Повторить заказ
        </button>
        <Link href="/delivery" className="mv-acc-btn ghost">Доставка и возврат</Link>
      </footer>
    </article>
  );
}

function OrdersTab({ orders, loading, error }: { orders: ShopOrder[]; loading: boolean; error: string }) {
  if (loading) return <div className="mv-acc-empty"><Loader2 size={20} className="mv-spin" /> Загружаем заказы…</div>;
  if (error) return <div className="mv-acc-error">{error}</div>;
  if (!orders.length) return (
    <div className="mv-acc-empty">
      <Package size={28} />
      <b>Заказов пока нет</b>
      <span>Здесь появятся ваши заказы, статусы доставки и быстрый повтор покупки.</span>
      <Link href="/catalog" className="btn-primary">Перейти в каталог</Link>
    </div>
  );
  return <div className="mv-acc-list">{orders.map((o) => <OrderCard key={o.id} order={o} />)}</div>;
}

/* ─────────────── addresses ─────────────── */
function AddressesTab({ token }: { token: string }) {
  const [list, setList] = useState<SavedAddress[] | null>(null);
  useEffect(() => { loadAddresses(token).then(({ addresses }) => setList(addresses)); }, [token]);
  if (!list) return <div className="mv-acc-empty"><Loader2 size={20} className="mv-spin" /> Загружаем адреса…</div>;
  if (!list.length) return (
    <div className="mv-acc-empty">
      <MapPin size={28} />
      <b>Адресов пока нет</b>
      <span>Пункт выдачи или адрес курьера сохранится после первого заказа — в следующий раз выберете его одним нажатием.</span>
    </div>
  );
  return (
    <div className="mv-acc-list">
      {list.map((a) => (
        <div key={a.id} className="mv-acc-addr-card">
          <span className="mv-co-saved-ic">{a.type === "pickup" ? <MapPin size={16} /> : <Truck size={16} />}</span>
          <div style={{ flex: 1, minWidth: 0 }}>
            <b>{a.type === "pickup" ? (a.name || "Пункт выдачи Ozon") : "Доставка курьером"}</b>
            <span>{prettyAddress(a)}</span>
            {a.lastUsed && <em>последний заказ {new Date(a.lastUsed).toLocaleDateString("ru-RU", { day: "numeric", month: "long" })}</em>}
          </div>
          <button type="button" className="mv-co-saved-x" style={{ position: "static" }} aria-label="Удалить адрес"
            onClick={() => { forgetAddress(a); setList((l) => (l || []).filter((x) => x.id !== a.id)); }}><X size={15} /></button>
        </div>
      ))}
      <p className="mv-acc-hint">Адреса запоминаются автоматически при оформлении заказа.</p>
    </div>
  );
}

/* ─────────────── profile ─────────────── */
function ProfileTab() {
  const { customer, updateProfile } = useAuth();
  const [form, setForm] = useState({ firstName: customer?.firstName || "", lastName: customer?.lastName || "", phone: customer?.phone || "" });
  const [state, setState] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [error, setError] = useState("");
  const set = (k: keyof typeof form) => (e: React.ChangeEvent<HTMLInputElement>) => { setForm((f) => ({ ...f, [k]: e.target.value })); setState("idle"); };
  async function save(e: React.FormEvent) {
    e.preventDefault(); setState("saving");
    try { await updateProfile(form); setState("saved"); } catch (err) { setError((err as Error).message); setState("error"); }
  }
  return (
    <form onSubmit={save} className="mv-acc-card">
      <div className="mv-acc-card-h">Личные данные</div>
      <label className="mv-co-field"><span className="mv-co-label">Email</span>
        <div className="mv-acc-email">{customer?.email}<span>подтверждён</span></div>
      </label>
      <div className="mv-co-2col">
        <label className="mv-co-field"><span className="mv-co-label">Имя</span><input className="mv-co-input" value={form.firstName} onChange={set("firstName")} placeholder="Иван" autoComplete="given-name" /></label>
        <label className="mv-co-field"><span className="mv-co-label">Фамилия</span><input className="mv-co-input" value={form.lastName} onChange={set("lastName")} placeholder="Иванов" autoComplete="family-name" /></label>
      </div>
      <label className="mv-co-field"><span className="mv-co-label">Телефон</span><input className="mv-co-input" type="tel" inputMode="tel" value={form.phone} onChange={set("phone")} placeholder="+7 900 000-00-00" autoComplete="tel" /></label>
      {state === "error" && <div className="mv-acc-error">{error}</div>}
      <button type="submit" className="btn-primary" disabled={state === "saving"} style={{ alignSelf: "flex-start" }}>
        {state === "saving" ? <Loader2 size={16} className="mv-spin" /> : state === "saved" ? <CheckCircle size={16} /> : <Save size={16} />}
        {state === "saved" ? "Сохранено" : "Сохранить"}
      </button>
    </form>
  );
}

/* ─────────────── bonuses ─────────────── */
const TIER = { silver: "Серебро", gold: "Золото", platinum: "Платина" } as const;
function BonusTab({ token }: { token: string }) {
  const [loyalty, setLoyalty] = useState<{ points: number; tier: keyof typeof TIER; nextTier: number | null; transactions: { id: string; reason: string; points: number }[] } | null>(null);
  const [ref, setRef] = useState<{ link: string; ordersFromRef: number } | null>(null);
  const [vip, setVip] = useState<{ eligible: boolean; ordersCount: number; vipLink?: string } | null>(null);
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    const h = { headers: { Authorization: `Bearer ${token}` } };
    fetch(AUTH_API + "/loyalty", h).then((r) => r.json()).then((d) => { if (d.ok !== false) setLoyalty(d); }).catch(() => {});
    fetch(AUTH_API + "/referral", h).then((r) => r.json()).then((d) => { if (d.ok) setRef(d); }).catch(() => {});
    // the API answers { disabled: true } while the VIP club is switched off in the admin
    fetch(AUTH_API + "/vip", h).then((r) => r.json()).then((d) => { if (d.ok && !d.disabled) setVip(d); }).catch(() => {});
  }, [token]);
  const progress = loyalty?.nextTier ? Math.min(100, Math.round((loyalty.points / loyalty.nextTier) * 100)) : 100;
  return (
    <div className="mv-acc-list">
      {loyalty && (
        <div className="mv-acc-card lime">
          <div className="mv-acc-card-h">Золото Magic Vibes <span className="mv-acc-tier">{TIER[loyalty.tier] || "Серебро"}</span></div>
          <div className="mv-acc-points">{loyalty.points}<small> {plural(loyalty.points, "балл", "балла", "баллов")}</small></div>
          {loyalty.nextTier && <div className="mv-acc-bar"><span style={{ width: `${progress}%` }} /></div>}
          {loyalty.nextTier && <div className="mv-acc-hint">{loyalty.points} из {loyalty.nextTier} до следующего уровня</div>}
          {loyalty.transactions.slice(0, 5).map((t) => <div key={t.id} className="mv-acc-line"><span className="mv-acc-line-name">{t.reason}</span><span className="mv-acc-line-p">+{t.points}</span></div>)}
          <p className="mv-acc-hint">Баллы за отзывы (+20), отзывы с фото (+50) и анбоксинги.</p>
        </div>
      )}
      {ref && (
        <div className="mv-acc-card">
          <div className="mv-acc-card-h">Пригласите друга</div>
          <p className="mv-acc-hint" style={{ marginTop: 0 }}>Друг получит −7% на первый заказ, вы — +100 баллов.</p>
          <div className="mv-acc-ref">
            <span>{ref.link}</span>
            <button type="button" onClick={() => navigator.clipboard.writeText(ref.link).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1800); })}>
              {copied ? <CheckCircle size={15} /> : <Copy size={15} />} {copied ? "Скопировано" : "Копировать"}
            </button>
          </div>
          <div className="mv-acc-hint">Заказов по вашей ссылке: <b>{ref.ordersFromRef}</b></div>
        </div>
      )}
      {vip && (
        <div className="mv-acc-card ink">
          <div className="mv-acc-card-h">VIP-клуб</div>
          {vip.eligible && vip.vipLink
            ? <><p className="mv-acc-hint" style={{ color: "rgba(245,242,236,.75)" }}>Ранний доступ к новинкам, закрытые промокоды и голосование за «аромат месяца».</p><a className="btn-primary on-dark" href={vip.vipLink} target="_blank" rel="noopener noreferrer">Вступить в клуб</a></>
            : <><p className="mv-acc-hint" style={{ color: "rgba(245,242,236,.75)" }}>Откроется после 3 оплаченных заказов.</p>
                <div className="mv-acc-dots">{[0, 1, 2].map((i) => <span key={i} className={i < vip.ordersCount ? "on" : ""} />)}<em>{vip.ordersCount}/3</em></div></>}
        </div>
      )}
    </div>
  );
}

/* ─────────────── page ─────────────── */
export default function AccountPage() {
  const { customer, token, loading, logout } = useAuth();
  const router = useRouter();
  const pathname = usePathname();
  const [tab, setTab] = useState<Tab>(pathname === "/orders" ? "orders" : "orders");
  const [orders, setOrders] = useState<ShopOrder[]>([]);
  const [stats, setStats] = useState<{ orders: number; paidOrders: number; spentRub: number } | null>(null);
  const [ordersLoading, setOrdersLoading] = useState(true);
  const [ordersError, setOrdersError] = useState("");

  useEffect(() => { if (!loading && !customer) router.replace(`/login?next=${encodeURIComponent(pathname || "/account")}`); }, [loading, customer, router, pathname]);
  useEffect(() => {
    if (!token) return;
    fetch(AUTH_API + "/orders", { headers: { Authorization: `Bearer ${token}` } })
      .then((r) => r.json())
      .then((d) => { if (d.orders) { setOrders(d.orders); setStats(d.stats || null); } else throw new Error(d.error || "Не удалось загрузить заказы"); })
      .catch((e: Error) => setOrdersError(e.message))
      .finally(() => setOrdersLoading(false));
  }, [token]);

  if (loading) return <div className="mv-acc"><div className="mv-acc-empty"><Loader2 size={22} className="mv-spin" /></div></div>;
  if (!customer || !token) return null;

  const name = [customer.firstName, customer.lastName].filter(Boolean).join(" ") || customer.email.split("@")[0];
  const TABS: { id: Tab; label: string; icon: typeof Package }[] = [
    { id: "orders", label: "Заказы", icon: Package },
    { id: "addresses", label: "Адреса", icon: MapPin },
    { id: "profile", label: "Профиль", icon: User },
    { id: "bonus", label: "Бонусы", icon: Gift },
  ];

  return (
    <div className="mv-acc">
      <div className="mv-acc-wrap">
        <section className="mv-acc-hero">
          <div className="mv-acc-avatar">{name[0]?.toUpperCase()}</div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div className="mv-acc-kicker">Личный кабинет</div>
            <h1 className="mv-acc-name">{name}</h1>
            <div className="mv-acc-mail">{customer.email}</div>
          </div>
          <button type="button" className="mv-acc-logout" onClick={() => { logout(); router.push("/"); }}><LogOut size={15} /> <span>Выйти</span></button>
          <div className="mv-acc-stats">
            <div><b>{stats?.orders ?? "—"}</b><span>{plural(stats?.orders ?? 0, "заказ", "заказа", "заказов")}</span></div>
            <div><b>{stats ? rub(stats.spentRub) : "—"}</b><span>оплачено</span></div>
            <div><b>{stats?.paidOrders ?? "—"}</b><span>оплаченных</span></div>
          </div>
        </section>

        <nav className="mv-acc-tabs" aria-label="Разделы кабинета">
          {TABS.map(({ id, label, icon: I }) => (
            <button key={id} type="button" className={tab === id ? "on" : ""} onClick={() => setTab(id)}><I size={16} /> {label}</button>
          ))}
        </nav>

        {tab === "orders" && <OrdersTab orders={orders} loading={ordersLoading} error={ordersError} />}
        {tab === "addresses" && <AddressesTab token={token} />}
        {tab === "profile" && <ProfileTab />}
        {tab === "bonus" && <BonusTab token={token} />}
      </div>
    </div>
  );
}
