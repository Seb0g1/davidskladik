import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle, ArrowLeft, Ban, CheckCircle2, ChevronRight, Clock, CreditCard, Edit3, FileText, Loader2, Mail, MapPin,
  MessageCircle, MoreHorizontal, Package, PackageX, Phone, Plus, RefreshCw, RotateCcw, Scale, Search, Send, Trash2, Truck, User, X,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { SupplierLedgerEntrySchema, SupplierLedgerPaymentSchema, SupplierProfileResponseSchema, SupplierSchema, SuppliersResponseSchema } from "../types";
import { asRecord, compactDate, errorMessage } from "../lib/common";
import { toast } from "../lib/toast";
import "./suppliers.css";

// «Поставщики» — рабочее место по поставщикам, как в CRM: слева список (поиск, фильтры, сигналы), справа карточка
// выбранного поставщика. Главное наверху карточки: баланс и оплата, «остановить / включить», связь с поставщиком.
// На телефоне список и карточка — отдельные экраны, быстрые действия закреплены снизу.

type Supplier = z.infer<typeof SupplierSchema>;
type LedgerEntry = z.infer<typeof SupplierLedgerEntrySchema>;
type SupplierProfile = z.infer<typeof SupplierProfileResponseSchema>;
type Contacts = { phone: string; telegram: string; whatsapp: string; email: string; manager: string; address: string; hours: string };
type Insight = { products: number; sellable: number; singleSource: number; sold30: number; revenue30: number; lastPriceAt: string | null; activeRows: number | null };
type InsightsResponse = { builtAt: string; priceMasterError: string | null; suppliers: Record<string, Insight> };
type Filter = "all" | "active" | "stopped" | "debt" | "attention";
type Tab = "overview" | "payments" | "orders" | "articles" | "settings";

const OkSchema = z.object({ ok: z.boolean().optional().default(true) }).passthrough();
const anyJson = <T,>(url: string, init?: RequestInit) => fetchJson<T>(url, z.custom<T>(() => true), init);
const emptyContacts: Contacts = { phone: "", telegram: "", whatsapp: "", email: "", manager: "", address: "", hours: "" };

// ── форматирование ───────────────────────────────────────────────────────────
const sym = (c: string) => (String(c).toUpperCase() === "RUB" ? "₽" : "$");
const money = (v: unknown, c = "USD", digits = 2) => {
  const n = Math.abs(Number(v || 0));
  return `${n.toLocaleString("ru-RU", { minimumFractionDigits: n % 1 ? digits : 0, maximumFractionDigits: digits })} ${sym(c)}`;
};
const signed = (v: unknown, c = "USD") => {
  const n = Number(v || 0);
  if (!n) return `0 ${sym(c)}`;
  return `${n > 0 ? "+" : "−"}${money(n, c)}`;
};
const ago = (iso: string | null | undefined) => {
  if (!iso) return "нет данных";
  const ms = Date.now() - new Date(iso).getTime();
  const h = Math.floor(ms / 3_600_000);
  if (h < 1) return "только что";
  if (h < 24) return `${h} ч назад`;
  const d = Math.floor(h / 24);
  return `${d} ${d % 10 === 1 && d % 100 !== 11 ? "день" : d % 10 >= 2 && d % 10 <= 4 && (d % 100 < 10 || d % 100 >= 20) ? "дня" : "дней"} назад`;
};
const daysSince = (iso: string | null | undefined) => (iso ? (Date.now() - new Date(iso).getTime()) / 86_400_000 : Infinity);
const plural = (n: number, one: string, few: string, many: string) => {
  const a = Math.abs(n) % 100; const b = a % 10;
  return a > 10 && a < 20 ? many : b === 1 ? one : b >= 2 && b <= 4 ? few : many;
};
const dateInput = (days: number) => { const d = new Date(); d.setDate(d.getDate() + days); return d.toISOString().slice(0, 10); };
const endOfMonth = () => { const d = new Date(); d.setMonth(d.getMonth() + 1, 0); return d.toISOString().slice(0, 10); };

// ── данные поставщика ────────────────────────────────────────────────────────
const sid = (s: Supplier) => String(s.id || s.partnerId || s.name || "");
const isActive = (s: Supplier) => s.stopped !== true && s.active !== false;
const currencyOf = (s: Supplier): "USD" | "RUB" => {
  const r = asRecord(s);
  const c = String(asRecord(r.ledger).currency || r.priceCurrency || "USD").toUpperCase();
  return c === "RUB" ? "RUB" : "USD";
};
const ledgerOf = (v: unknown) => {
  const l = asRecord(v);
  return {
    balance: Number(l.balance || 0), debt: Number(l.debtTotal || 0), paid: Number(l.paidTotal || 0),
    returns: Number(l.returnsTotal || 0), corrections: Number(l.correctionsTotal || 0),
    lastPaymentAt: l.lastPaymentAt ? String(l.lastPaymentAt) : null,
  };
};
const contactsOf = (s: Supplier): Contacts => ({ ...emptyContacts, ...(asRecord(asRecord(s).contacts) as Partial<Contacts>) });
const articlesOf = (s: Supplier) => {
  const a = asRecord(s).articles;
  return Array.isArray(a) ? a.map(asRecord) : [];
};
const stopText = (s: Supplier) => {
  const r = asRecord(s);
  const day = String(r.inactiveUntil || "").slice(0, 10).split("-").reverse().join(".");
  const until = r.inactiveUntilUnknown || !r.inactiveUntil ? "срок не указан" : `до ${day}`;
  const why = String(r.inactiveComment || s.stopReason || "").trim();
  return `${until}${why ? ` · ${why}` : ""}`;
};
const initials = (name: string) => name.split(/[\s«»"'()-]+/).filter(Boolean).slice(0, 2).map((w) => w[0]).join("").toUpperCase() || "?";
const phoneDigits = (p: string) => p.replace(/[^\d+]/g, "");
const waNumber = (p: string) => p.replace(/\D/g, "").replace(/^8(\d{10})$/, "7$1");

type Signal = { tone: "danger" | "warn" | "info"; text: string };
/** Что требует внимания: висят товары без замены, старый прайс, пустой прайс, большой долг. */
function signalsOf(s: Supplier, ins: Insight | undefined): Signal[] {
  const out: Signal[] = [];
  const active = isActive(s);
  if (!active && (ins?.singleSource || 0) > 0) out.push({ tone: "danger", text: `${ins!.singleSource} ${plural(ins!.singleSource, "товар", "товара", "товаров")} без замены — не продаются` });
  if (active && ins && ins.products > 0) {
    const d = daysSince(ins.lastPriceAt);
    if (d > 14) out.push({ tone: "danger", text: `Прайс не обновлялся ${ins.lastPriceAt ? ago(ins.lastPriceAt) : "давно"}` });
    else if (d > 4) out.push({ tone: "warn", text: `Прайс обновлён ${ago(ins.lastPriceAt)}` });
    if (ins.activeRows === 0) out.push({ tone: "danger", text: "В PriceMaster нет живых строк" });
  }
  if (s.pricingMode === "stock_only" || s.stockOnly) out.push({ tone: "info", text: "Только остаток — цену не берём" });
  return out;
}

export function SuppliersPage() {
  const queryClient = useQueryClient();
  const [filter, setFilter] = useState<Filter>("active");
  const [sort, setSort] = useState<"name" | "debt" | "sales">("debt");
  const [search, setSearch] = useState("");
  const [selectedId, setSelectedId] = useState<string>(() => new URLSearchParams(window.location.search).get("id") || "");
  const [tab, setTab] = useState<Tab>("overview");
  const [stopFor, setStopFor] = useState<Supplier | null>(null);
  const [creating, setCreating] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);

  const suppliersQuery = useQuery({ queryKey: ["suppliers"], queryFn: () => fetchJson("/api/suppliers", SuppliersResponseSchema), staleTime: 30_000 });
  const insightsQuery = useQuery({ queryKey: ["suppliers", "insights"], queryFn: () => anyJson<InsightsResponse>("/api/suppliers/insights"), staleTime: 5 * 60_000 });
  const suppliers = suppliersQuery.data?.suppliers || [];
  const insights = insightsQuery.data?.suppliers || {};

  // the selected supplier lives in the address (?id=…): back button and shared links work
  useEffect(() => {
    const url = new URL(window.location.href);
    if (selectedId) url.searchParams.set("id", selectedId); else url.searchParams.delete("id");
    window.history.replaceState(window.history.state, "", url.toString());
  }, [selectedId]);

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
    void queryClient.invalidateQueries({ queryKey: ["supplier-profile"] });
    void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    void queryClient.invalidateQueries({ queryKey: ["finance"] });
  };
  const refreshPm = useMutation({
    mutationFn: () => fetchJson("/api/suppliers?refresh=true", SuppliersResponseSchema),
    onSuccess: (data) => {
      queryClient.setQueryData(["suppliers"], data);
      const s = asRecord(data.supplierSync);
      toast.success(s.error ? `PriceMaster: ${String(s.error)}` : `PriceMaster: партнёров ${s.partners || 0}, новых ${s.imported || 0}`);
      void queryClient.invalidateQueries({ queryKey: ["suppliers", "insights"] });
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const dalik = useMutation({
    mutationFn: () => fetchJson("/api/warehouse/check-dalik-migrations", OkSchema, { method: "POST" }),
    onSuccess: (d) => {
      const r = asRecord(d);
      toast.success(`Далик: проверено ${r.checked ?? 0}${Number(r.diverged) ? `, сброшено ${r.diverged}` : ""}${Number(r.articleFixed) ? `, артикул обновлён у ${r.articleFixed}` : " — всё в порядке"}`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const resetAll = useMutation({
    mutationFn: () => anyJson<{ ledger: number; picking: number; cart: number }>("/api/supplier-ledger/reset-all-history", { method: "DELETE" }),
    onSuccess: (r) => { toast.success(`Сброшено: долгов ${r.ledger}, строк сборки ${r.picking}, корзин ${r.cart}`); invalidate(); },
    onError: (e) => toast.error(errorMessage(e)),
  });

  // KPI: долги по валютам, сколько остановлено, сколько требует внимания
  const kpi = useMemo(() => {
    const debt = { USD: 0, RUB: 0 };
    let stopped = 0; let attention = 0;
    for (const s of suppliers) {
      const b = ledgerOf(asRecord(s).ledger).balance;
      if (b < 0) debt[currencyOf(s)] += -b;
      if (!isActive(s)) stopped += 1;
      if (signalsOf(s, insights[sid(s)]).some((x) => x.tone !== "info")) attention += 1;
    }
    return { debt, stopped, active: suppliers.length - stopped, attention };
  }, [suppliers, insights]);

  const list = useMemo(() => {
    const words = search.trim().toLowerCase().split(/\s+/).filter(Boolean);
    return suppliers
      .filter((s) => {
        if (filter === "active") return isActive(s);
        if (filter === "stopped") return !isActive(s);
        if (filter === "debt") return ledgerOf(asRecord(s).ledger).balance < -0.005;
        if (filter === "attention") return signalsOf(s, insights[sid(s)]).some((x) => x.tone !== "info");
        return true;
      })
      .filter((s) => {
        if (!words.length) return true;
        const c = contactsOf(s);
        const text = [s.name, s.partnerId, asRecord(s).note, c.manager, c.phone, c.telegram, ...articlesOf(s).map((a) => a.article)].join(" ").toLowerCase();
        return words.every((w) => text.includes(w));
      })
      .sort((a, b) => {
        if (sort === "debt") return ledgerOf(asRecord(a).ledger).balance - ledgerOf(asRecord(b).ledger).balance || String(a.name).localeCompare(String(b.name), "ru");
        if (sort === "sales") return (insights[sid(b)]?.sold30 || 0) - (insights[sid(a)]?.sold30 || 0);
        return String(a.name).localeCompare(String(b.name), "ru");
      });
  }, [suppliers, insights, filter, search, sort]);

  const selected = suppliers.find((s) => sid(s) === selectedId) || null;
  // desktop: the first supplier is open by default; phone: the list stays until a tap
  useEffect(() => {
    if (!selectedId && list.length && window.matchMedia("(min-width: 980px)").matches) setSelectedId(sid(list[0]));
  }, [list, selectedId]);

  const pushedRef = useRef(false);
  // phone: opening a card is a history step, so the system «back» returns to the list
  useEffect(() => {
    const onPop = () => { pushedRef.current = false; setSelectedId(new URLSearchParams(window.location.search).get("id") || ""); };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const isPhone = () => !window.matchMedia("(min-width: 980px)").matches;
  const open = (s: Supplier) => {
    if (isPhone()) {
      const url = new URL(window.location.href);
      url.searchParams.set("id", sid(s));
      window.history.pushState(window.history.state, "", url.toString());
      pushedRef.current = true;
      window.scrollTo(0, 0);
    }
    setSelectedId(sid(s)); setTab("overview");
  };
  const back = () => {
    if (pushedRef.current) window.history.back();
    else setSelectedId("");
  };
  const filters: Array<[Filter, string, number]> = [
    ["active", "Активные", kpi.active],
    ["debt", "С долгом", suppliers.filter((s) => ledgerOf(asRecord(s).ledger).balance < -0.005).length],
    ["attention", "Внимание", kpi.attention],
    ["stopped", "Остановлены", kpi.stopped],
    ["all", "Все", suppliers.length],
  ];

  return (
    <section className={`page-section sp2${selected ? " has-selected" : ""}`}>
      <header className="sp2-head">
        <div className="sp2-title">
          <h1>Поставщики</h1>
          <p>
            {kpi.debt.USD || kpi.debt.RUB ? <>Мы должны <b className="sp2-debt">{[kpi.debt.USD ? money(kpi.debt.USD, "USD", 0) : "", kpi.debt.RUB ? money(kpi.debt.RUB, "RUB", 0) : ""].filter(Boolean).join(" + ")}</b> · </> : "Долгов нет · "}
            активных {kpi.active}, остановлено {kpi.stopped}{kpi.attention ? <> · <span className="sp2-warn-text">внимание {kpi.attention}</span></> : null}
          </p>
        </div>
        <div className="sp2-head-actions">
          <button className="secondary-action" type="button" disabled={refreshPm.isPending} onClick={() => refreshPm.mutate()} title="Подтянуть новых партнёров из PriceMaster">
            {refreshPm.isPending ? <Loader2 className="spin" size={15} /> : <RefreshCw size={15} />} <span className="sp2-hide-sm">Из PriceMaster</span>
          </button>
          <button className="primary-action" type="button" onClick={() => setCreating(true)}><Plus size={15} /> <span className="sp2-hide-sm">Поставщик</span></button>
          <div className="sp2-menu-wrap">
            <button className="icon-action" type="button" aria-label="Ещё" onClick={() => setMenuOpen((v) => !v)}><MoreHorizontal size={18} /></button>
            {menuOpen ? (
              <div className="sp2-menu" onMouseLeave={() => setMenuOpen(false)}>
                <button type="button" disabled={dalik.isPending} onClick={() => { setMenuOpen(false); dalik.mutate(); }}><RotateCcw size={14} /> Проверить артикулы Далика</button>
                <button type="button" onClick={() => { setMenuOpen(false); void insightsQuery.refetch(); }}><RefreshCw size={14} /> Пересчитать сводку</button>
                <button type="button" className="is-danger" disabled={resetAll.isPending} onClick={() => {
                  setMenuOpen(false);
                  if (window.confirm("Удалить ВСЮ историю долгов, сборки и корзин для всех поставщиков? Это необратимо.")) resetAll.mutate();
                }}><Trash2 size={14} /> Сбросить все долги и историю…</button>
              </div>
            ) : null}
          </div>
        </div>
      </header>
      {suppliersQuery.isError ? <div className="inline-error">{errorMessage(suppliersQuery.error)}</div> : null}

      <div className="sp2-body">
        {/* ── список ── */}
        <aside className="sp2-list" aria-label="Список поставщиков">
          <label className="sp2-search">
            <Search size={15} />
            <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Имя, артикул, менеджер, телефон" />
            {search ? <button type="button" aria-label="Очистить" onClick={() => setSearch("")}><X size={14} /></button> : null}
          </label>
          <div className="sp2-filters" role="tablist">
            {filters.map(([key, label, n]) => (
              <button key={key} type="button" role="tab" aria-selected={filter === key} className={filter === key ? "is-on" : ""} onClick={() => setFilter(key)}>
                {label}<span>{n}</span>
              </button>
            ))}
          </div>
          <div className="sp2-sort">
            Сортировка:
            {([["debt", "долг"], ["sales", "продажи"], ["name", "имя"]] as const).map(([k, l]) => (
              <button key={k} type="button" className={sort === k ? "is-on" : ""} onClick={() => setSort(k)}>{l}</button>
            ))}
          </div>
          <div className="sp2-rows">
            {suppliersQuery.isLoading ? <div className="sp2-empty"><Loader2 className="spin" size={15} /> Загружаем поставщиков…</div> : null}
            {!suppliersQuery.isLoading && !list.length ? <div className="sp2-empty">Никого не нашли. Измените фильтр или поиск.</div> : null}
            {list.map((s) => {
              const id = sid(s);
              const l = ledgerOf(asRecord(s).ledger);
              const cur = currencyOf(s);
              const ins = insights[id];
              const signals = signalsOf(s, ins).filter((x) => x.tone !== "info");
              const active = isActive(s);
              return (
                <button key={id} type="button" className={`sp2-row${id === selectedId ? " is-selected" : ""}${active ? "" : " is-stopped"}`} onClick={() => open(s)}>
                  <span className={`sp2-avatar${active ? "" : " is-off"}`}>{initials(String(s.name || ""))}</span>
                  <span className="sp2-row-main">
                    <span className="sp2-row-top">
                      <b>{s.name || "Без названия"}</b>
                      {signals.length ? <AlertTriangle size={13} className={`sp2-sig is-${signals[0].tone}`} aria-label={signals.map((x) => x.text).join("; ")} /> : null}
                    </span>
                    <span className="sp2-row-sub">
                      {active ? null : <span className="sp2-stop-tag">стоп</span>}
                      {ins ? `${ins.products} ${plural(ins.products, "товар", "товара", "товаров")} · ${ins.sold30} шт. за 30 дн.` : `${s.impactProductCount || 0} ${plural(Number(s.impactProductCount || 0), "товар", "товара", "товаров")}`}
                    </span>
                  </span>
                  <span className={`sp2-row-bal${l.balance < 0 ? " is-debt" : l.balance > 0 ? " is-plus" : ""}`}>
                    {l.balance ? (l.balance < 0 ? `−${money(l.balance, cur, 0)}` : `+${money(l.balance, cur, 0)}`) : "—"}
                  </span>
                  <ChevronRight size={15} className="sp2-row-go" />
                </button>
              );
            })}
          </div>
        </aside>

        {/* ── карточка ── */}
        <main className="sp2-card-wrap">
          {selected ? (
            <SupplierCard
              key={sid(selected)}
              supplier={selected}
              insight={insights[sid(selected)]}
              tab={tab}
              setTab={setTab}
              onBack={back}
              onStop={() => setStopFor(selected)}
              onChanged={invalidate}
              onDeleted={() => { setSelectedId(""); invalidate(); }}
            />
          ) : (
            <div className="sp2-placeholder"><Truck size={28} /> Выберите поставщика слева</div>
          )}
        </main>
      </div>

      {stopFor ? <StopDialog supplier={stopFor} onClose={() => setStopFor(null)} onDone={() => { setStopFor(null); invalidate(); }} /> : null}
      {creating ? <CreateDialog onClose={() => setCreating(false)} onCreated={(id) => { setCreating(false); invalidate(); if (id) setSelectedId(id); }} /> : null}
    </section>
  );
}

// ═══ Карточка поставщика ═════════════════════════════════════════════════════
function SupplierCard({ supplier, insight, tab, setTab, onBack, onStop, onChanged, onDeleted }: {
  supplier: Supplier; insight?: Insight; tab: Tab; setTab: (t: Tab) => void;
  onBack: () => void; onStop: () => void; onChanged: () => void; onDeleted: () => void;
}) {
  const queryClient = useQueryClient();
  const id = sid(supplier);
  const cur = currencyOf(supplier);
  const active = isActive(supplier);
  const contacts = contactsOf(supplier);
  const profileQuery = useQuery<SupplierProfile>({
    queryKey: ["supplier-profile", id],
    queryFn: () => fetchJson(`/api/suppliers/${encodeURIComponent(id)}/profile`, SupplierProfileResponseSchema),
    staleTime: 10_000,
  });
  const profile = profileQuery.data;
  const ledger = ledgerOf(profile?.ledger?.summary ?? asRecord(supplier).ledger);
  const signals = signalsOf(supplier, insight);
  const [moreOpen, setMoreOpen] = useState(false);
  const payRef = useRef<HTMLInputElement>(null);

  const resume = useMutation({
    mutationFn: () => fetchJson(`/api/suppliers/${encodeURIComponent(id)}`, OkSchema, patchBody({ stopped: false })),
    onSuccess: () => { toast.success(`${supplier.name} снова в работе`); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const zeroStock = useMutation({
    mutationFn: () => anyJson<{ zeroed: number; total: number }>(`/api/suppliers/${encodeURIComponent(id)}/zero-stock`, { method: "POST" }),
    onSuccess: (r) => { toast.success(`Остатки обнулены: ${r.zeroed} из ${r.total}`); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const remove = useMutation({
    mutationFn: () => fetchJson(`/api/suppliers/${encodeURIComponent(id)}`, OkSchema, { method: "DELETE" }),
    onSuccess: () => { toast.success("Поставщик удалён"); onDeleted(); },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const tabs: Array<[Tab, string]> = [["overview", "Обзор"], ["payments", "Оплаты"], ["orders", "Заказы"], ["articles", "Артикулы"], ["settings", "Настройки"]];
  const tg = contacts.telegram ? `https://t.me/${contacts.telegram.replace(/^@/, "")}` : "";
  const wa = contacts.whatsapp || contacts.phone ? `https://wa.me/${waNumber(contacts.whatsapp || contacts.phone)}` : "";

  return (
    <article className="sp2-card">
      <div className="sp2-card-head">
        <button className="sp2-back" type="button" onClick={onBack}><ArrowLeft size={18} /> Поставщики</button>
        <div className="sp2-card-id">
          <span className={`sp2-avatar is-lg${active ? "" : " is-off"}`}>{initials(String(supplier.name || ""))}</span>
          <div>
            <h2>{supplier.name || "Без названия"}</h2>
            <div className="sp2-card-meta">
              <span className={`sp2-status${active ? " is-on" : " is-off"}`}>{active ? <><CheckCircle2 size={12} /> в работе</> : <><Ban size={12} /> остановлен</>}</span>
              <span>{cur === "RUB" ? "₽ рубли" : "$ доллары"}</span>
              {supplier.partnerId ? <span>PriceMaster #{supplier.partnerId}</span> : <span>вне PriceMaster</span>}
              {contacts.manager ? <span><User size={12} /> {contacts.manager}</span> : null}
            </div>
          </div>
        </div>
        <div className="sp2-card-actions">
          {contacts.phone ? <a className="icon-action" href={`tel:${phoneDigits(contacts.phone)}`} title={`Позвонить ${contacts.phone}`}><Phone size={16} /></a> : null}
          {tg ? <a className="icon-action" href={tg} target="_blank" rel="noreferrer" title="Telegram"><Send size={16} /></a> : null}
          {wa ? <a className="icon-action" href={wa} target="_blank" rel="noreferrer" title="WhatsApp"><MessageCircle size={16} /></a> : null}
          {active
            ? <button className="secondary-action danger-action" type="button" onClick={onStop}><Ban size={15} /> Остановить</button>
            : <button className="primary-action" type="button" disabled={resume.isPending} onClick={() => resume.mutate()}>{resume.isPending ? <Loader2 className="spin" size={15} /> : <CheckCircle2 size={15} />} Включить</button>}
          <div className="sp2-menu-wrap">
            <button className="icon-action" type="button" aria-label="Ещё действия" onClick={() => setMoreOpen((v) => !v)}><MoreHorizontal size={18} /></button>
            {moreOpen ? (
              <div className="sp2-menu" onMouseLeave={() => setMoreOpen(false)}>
                <button type="button" onClick={() => { setMoreOpen(false); setTab("settings"); }}><Edit3 size={14} /> Изменить данные</button>
                <button type="button" className="is-danger" disabled={zeroStock.isPending} onClick={() => {
                  setMoreOpen(false);
                  if (window.confirm(`Обнулить остатки на маркетплейсах для всех товаров «${supplier.name}»? Товары сразу пропадут из продажи.`)) zeroStock.mutate();
                }}><PackageX size={14} /> Обнулить остатки товаров</button>
                <button type="button" className="is-danger" disabled={remove.isPending} onClick={() => {
                  setMoreOpen(false);
                  if (window.confirm(`Удалить поставщика «${supplier.name}»?`)) remove.mutate();
                }}><Trash2 size={14} /> Удалить поставщика</button>
              </div>
            ) : null}
          </div>
        </div>
      </div>

      {!active ? <div className="sp2-stopped-note"><Ban size={14} /> Остановлен {stopText(supplier)}</div> : null}

      {/* баланс + оплата — главное */}
      <section className="sp2-balance">
        <div className="sp2-balance-main">
          <span className="sp2-label">{ledger.balance < 0 ? "Мы должны" : ledger.balance > 0 ? "Аванс у поставщика" : "Расчёты"}</span>
          <strong className={ledger.balance < 0 ? "is-debt" : ledger.balance > 0 ? "is-plus" : ""}>
            {ledger.balance ? money(ledger.balance, cur) : "закрыты"}
          </strong>
          <span className="sp2-balance-sub">
            Последняя оплата: {ledger.lastPaymentAt ? compactDate(ledger.lastPaymentAt) : "не было"}
            {profileQuery.isFetching ? <Loader2 className="spin" size={12} /> : null}
          </span>
        </div>
        <PaymentBox supplier={supplier} currency={cur} inputRef={payRef} onDone={() => { void queryClient.invalidateQueries({ queryKey: ["supplier-profile", id] }); onChanged(); }} />
      </section>

      <nav className="sp2-tabs" role="tablist">
        {tabs.map(([k, l]) => (
          <button key={k} type="button" role="tab" aria-selected={tab === k} className={tab === k ? "is-on" : ""} onClick={() => setTab(k)}>{l}</button>
        ))}
      </nav>

      <div className="sp2-tab-body">
        {tab === "overview" ? <OverviewTab supplier={supplier} insight={insight} signals={signals} ledger={ledger} currency={cur} profile={profile} onChanged={onChanged} /> : null}
        {tab === "payments" ? <PaymentsTab supplier={supplier} currency={cur} ledger={ledger} profile={profile} loading={profileQuery.isLoading} onChanged={onChanged} /> : null}
        {tab === "orders" ? <OrdersTab supplier={supplier} currency={cur} profile={profile} loading={profileQuery.isLoading} onChanged={onChanged} /> : null}
        {tab === "articles" ? <ArticlesTab supplier={supplier} onChanged={onChanged} /> : null}
        {tab === "settings" ? <SettingsTab supplier={supplier} onChanged={onChanged} /> : null}
      </div>

      {/* телефон: быстрые действия всегда под пальцем */}
      <div className="sp2-mobile-bar">
        <button type="button" className="primary-action" onClick={() => { payRef.current?.focus(); payRef.current?.scrollIntoView({ block: "center", behavior: "smooth" }); }}><CreditCard size={16} /> Оплата</button>
        {active
          ? <button type="button" className="secondary-action danger-action" onClick={onStop}><Ban size={16} /> Стоп</button>
          : <button type="button" className="secondary-action" disabled={resume.isPending} onClick={() => resume.mutate()}><CheckCircle2 size={16} /> Включить</button>}
        {contacts.phone ? <a className="secondary-action" href={`tel:${phoneDigits(contacts.phone)}`}><Phone size={16} /> Звонок</a>
          : tg ? <a className="secondary-action" href={tg} target="_blank" rel="noreferrer"><Send size={16} /> Написать</a>
          : <button type="button" className="secondary-action" onClick={() => setTab("overview")}><Phone size={16} /> Контакты</button>}
      </div>
    </article>
  );
}

// ── оплата в одно действие ───────────────────────────────────────────────────
function PaymentBox({ supplier, currency, inputRef, onDone }: { supplier: Supplier; currency: "USD" | "RUB"; inputRef: React.RefObject<HTMLInputElement | null>; onDone: () => void }) {
  const queryClient = useQueryClient();
  const [amount, setAmount] = useState("");
  const [note, setNote] = useState("");
  const pay = useMutation({
    mutationFn: () => fetchJson("/api/supplier-ledger/payments", SupplierLedgerPaymentSchema, mutationBody({
      supplierName: supplier.name || "", partnerId: supplier.partnerId || "", amount: Number(amount.replace(",", ".")), currency, note,
    })),
    onSuccess: (data) => {
      toast.success(`Оплата ${money(Number(amount.replace(",", ".")), currency)} записана`);
      if (data?.summary) {
        queryClient.setQueryData(["suppliers"], (old: Record<string, unknown> | undefined) => (
          old && Array.isArray(old.suppliers) ? { ...old, suppliers: (old.suppliers as Supplier[]).map((s) => (sid(s) === sid(supplier) ? { ...s, ledger: data.summary } : s)) } : old
        ));
      }
      setAmount(""); setNote("");
      for (const key of [["supplier-picking-list"], ["picker-balances"], ["picker-balance"], ["picker-spending"]]) void queryClient.invalidateQueries({ queryKey: key });
      onDone();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const value = Number(amount.replace(",", "."));
  return (
    <form className="sp2-pay" onSubmit={(e) => { e.preventDefault(); if (value > 0) pay.mutate(); }}>
      <label className="sp2-pay-amount">
        <span className="sp2-label">Внести оплату</span>
        <span className="sp2-money-input">
          <input ref={inputRef} inputMode="decimal" value={amount} onChange={(e) => setAmount(e.target.value.replace(/[^\d.,]/g, ""))} placeholder="0" aria-label={`Сумма, ${sym(currency)}`} />
          <span>{sym(currency)}</span>
        </span>
      </label>
      <input className="sp2-pay-note" value={note} onChange={(e) => setNote(e.target.value)} placeholder="Комментарий" />
      <button className="primary-action" type="submit" disabled={!(value > 0) || pay.isPending}>
        {pay.isPending ? <Loader2 className="spin" size={15} /> : <CheckCircle2 size={15} />} Заплатил
      </button>
    </form>
  );
}

// ── Обзор: сигналы, товары и продажи, прайс, контакты ────────────────────────
function OverviewTab({ supplier, insight, signals, ledger, currency, profile, onChanged }: {
  supplier: Supplier; insight?: Insight; signals: Signal[]; ledger: ReturnType<typeof ledgerOf>; currency: "USD" | "RUB"; profile?: SupplierProfile; onChanged: () => void;
}) {
  const stats = asRecord(profile?.stats);
  const note = String(asRecord(supplier).note || "").trim();
  return (
    <div className="sp2-overview">
      {signals.length ? (
        <ul className="sp2-signals">
          {signals.map((s) => <li key={s.text} className={`is-${s.tone}`}>{s.tone === "info" ? <FileText size={14} /> : <AlertTriangle size={14} />} {s.text}</li>)}
        </ul>
      ) : <div className="sp2-ok"><CheckCircle2 size={14} /> Всё в порядке</div>}

      <div className="sp2-facts">
        <Fact icon={<Package size={15} />} label="Товаров на поставщике" value={insight ? String(insight.products) : String(supplier.impactProductCount || 0)}
          hint={insight ? `в продаже ${insight.sellable} · только у него ${insight.singleSource}` : ""} />
        <Fact icon={<Truck size={15} />} label="Взяли у него за 30 дней" value={insight ? `${insight.sold30} шт.` : "—"}
          hint={insight && insight.revenue30 ? `продали на ${Math.round(insight.revenue30).toLocaleString("ru")} ₽` : ""} />
        <Fact icon={<Clock size={15} />} label="Прайс в PriceMaster" value={insight?.lastPriceAt ? ago(insight.lastPriceAt) : supplier.partnerId ? "нет данных" : "не из PriceMaster"}
          hint={insight?.activeRows != null ? `живых строк ${insight.activeRows.toLocaleString("ru")}` : ""}
          tone={insight && daysSince(insight.lastPriceAt) > 14 ? "danger" : insight && daysSince(insight.lastPriceAt) > 3 ? "warn" : ""} />
        <Fact icon={<Scale size={15} />} label="Оборот по долгу" value={money(ledger.debt, currency)}
          hint={`оплачено ${money(ledger.paid, currency)}${ledger.returns ? ` · возвраты ${money(ledger.returns, currency)}` : ""}`} />
        {stats.totalPurchases ? <Fact icon={<CheckCircle2 size={15} />} label="Сборка заказов" value={`${stats.successRate ?? "—"}%`} hint={`собрано ${stats.picked ?? 0}, не было ${stats.missing ?? 0}`} /> : null}
      </div>

      {note ? <p className="sp2-note"><FileText size={14} /> {note}</p> : null}
      <ContactsBlock supplier={supplier} onChanged={onChanged} />
    </div>
  );
}

function Fact({ icon, label, value, hint, tone = "" }: { icon: React.ReactNode; label: string; value: string; hint?: string; tone?: string }) {
  return (
    <div className={`sp2-fact${tone ? ` is-${tone}` : ""}`}>
      <span className="sp2-fact-label">{icon} {label}</span>
      <b>{value}</b>
      {hint ? <small>{hint}</small> : null}
    </div>
  );
}

function ContactsBlock({ supplier, onChanged }: { supplier: Supplier; onChanged: () => void }) {
  const saved = contactsOf(supplier);
  const [edit, setEdit] = useState(false);
  const [draft, setDraft] = useState<Contacts>(saved);
  useEffect(() => { if (!edit) setDraft(contactsOf(supplier)); }, [supplier, edit]);
  const save = useMutation({
    mutationFn: () => fetchJson(`/api/suppliers/${encodeURIComponent(sid(supplier))}/profile`, OkSchema, patchBody({ contacts: draft })),
    onSuccess: () => { toast.success("Контакты сохранены"); setEdit(false); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const has = Object.values(saved).some(Boolean);
  const fields: Array<[keyof Contacts, string, React.ReactNode, string]> = [
    ["manager", "Менеджер", <User size={14} key="u" />, "Имя"],
    ["phone", "Телефон", <Phone size={14} key="p" />, "+7 900 000-00-00"],
    ["telegram", "Telegram", <Send size={14} key="t" />, "@username"],
    ["whatsapp", "WhatsApp", <MessageCircle size={14} key="w" />, "если отличается от телефона"],
    ["email", "Почта", <Mail size={14} key="m" />, "mail@example.ru"],
    ["address", "Адрес / склад", <MapPin size={14} key="a" />, "Откуда забирать"],
    ["hours", "Часы работы", <Clock size={14} key="h" />, "пн–пт 10–19"],
  ];
  return (
    <section className="sp2-contacts">
      <div className="sp2-section-head">
        <h3>Контакты</h3>
        {edit ? null : <button type="button" className="secondary-action compact" onClick={() => setEdit(true)}><Edit3 size={13} /> {has ? "Изменить" : "Добавить"}</button>}
      </div>
      {edit ? (
        <form className="sp2-contacts-form" onSubmit={(e) => { e.preventDefault(); save.mutate(); }}>
          {fields.map(([k, label, icon, ph]) => (
            <label key={k}><span>{icon} {label}</span><input value={draft[k]} onChange={(e) => setDraft({ ...draft, [k]: e.target.value })} placeholder={ph} /></label>
          ))}
          <div className="sp2-form-actions">
            <button className="primary-action" type="submit" disabled={save.isPending}>{save.isPending ? <Loader2 className="spin" size={14} /> : null} Сохранить</button>
            <button className="secondary-action" type="button" onClick={() => setEdit(false)}>Отмена</button>
          </div>
        </form>
      ) : has ? (
        <dl className="sp2-contacts-list">
          {fields.filter(([k]) => saved[k]).map(([k, label, icon]) => (
            <div key={k}>
              <dt>{icon} {label}</dt>
              <dd>
                {k === "phone" ? <a href={`tel:${phoneDigits(saved.phone)}`}>{saved.phone}</a>
                  : k === "telegram" ? <a href={`https://t.me/${saved.telegram}`} target="_blank" rel="noreferrer">@{saved.telegram}</a>
                  : k === "whatsapp" ? <a href={`https://wa.me/${waNumber(saved.whatsapp)}`} target="_blank" rel="noreferrer">{saved.whatsapp}</a>
                  : k === "email" ? <a href={`mailto:${saved.email}`}>{saved.email}</a>
                  : saved[k]}
              </dd>
            </div>
          ))}
        </dl>
      ) : <p className="sp2-muted">Контактов нет — добавьте телефон или Telegram, чтобы связываться в одно нажатие.</p>}
    </section>
  );
}

// ── Оплаты: история + сверка баланса ─────────────────────────────────────────
function PaymentsTab({ supplier, currency, ledger, profile, loading, onChanged }: {
  supplier: Supplier; currency: "USD" | "RUB"; ledger: ReturnType<typeof ledgerOf>; profile?: SupplierProfile; loading: boolean; onChanged: () => void;
}) {
  const [target, setTarget] = useState("");
  const [note, setNote] = useState("");
  const adjust = useMutation({
    mutationFn: () => anyJson<{ skipped?: boolean; message?: string; delta?: number }>("/api/supplier-ledger/adjust", mutationBody({
      supplierName: supplier.name || "", partnerId: supplier.partnerId || "", targetBalance: Number(target.replace(",", ".")), currency, note,
    })),
    onSuccess: (r) => { toast.success(r.skipped ? (r.message || "Баланс уже такой") : `Баланс сведён (поправка ${signed(r.delta, currency)})`); setTarget(""); setNote(""); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const entries = (profile?.ledger?.entries || [])
    .filter((e: LedgerEntry) => ["payment", "balance_correction", "supplier_return"].includes(String(e.entryType)))
    .sort((a: LedgerEntry, b: LedgerEntry) => String(b.occurredAt || "").localeCompare(String(a.occurredAt || "")));
  const label = (t: string) => (t === "payment" ? "Оплата" : t === "balance_correction" ? "Сверка" : t === "supplier_return" ? "Возврат" : t);
  return (
    <div className="sp2-stack">
      <section>
        <div className="sp2-section-head"><h3>История оплат</h3>{loading ? <Loader2 className="spin" size={14} /> : null}</div>
        {entries.length ? (
          <ul className="sp2-ledger">
            {entries.map((e: LedgerEntry) => {
              const c = String(e.currency || currency).toUpperCase() === "RUB" ? "RUB" : "USD";
              return (
                <li key={e.id}>
                  <span className="sp2-ledger-type">{label(String(e.entryType))}</span>
                  <span className="sp2-ledger-date">{compactDate(e.occurredAt ?? null)}</span>
                  <b className={Number(e.amount) < 0 ? "is-debt" : "is-plus"}>{signed(e.amount, c)}</b>
                  {e.note ? <small>{e.note}</small> : null}
                </li>
              );
            })}
          </ul>
        ) : <p className="sp2-muted">{loading ? "Загружаем…" : "Оплат ещё не было."}</p>}
      </section>
      <section className="sp2-box">
        <h3>Свести баланс с поставщиком</h3>
        <p className="sp2-muted">Если у поставщика другая цифра — впишите её, мы добавим поправку. Сейчас: {signed(ledger.balance, currency)}.</p>
        <form className="sp2-inline-form" onSubmit={(e) => { e.preventDefault(); if (Number.isFinite(Number(target.replace(",", "."))) && target !== "") adjust.mutate(); }}>
          <span className="sp2-money-input"><input inputMode="decimal" value={target} onChange={(e) => setTarget(e.target.value.replace(/[^\d.,-]/g, ""))} placeholder="Баланс у поставщика" /><span>{sym(currency)}</span></span>
          <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Комментарий" />
          <button className="secondary-action" type="submit" disabled={adjust.isPending || target === ""}>{adjust.isPending ? <Loader2 className="spin" size={14} /> : <Scale size={14} />} Свести</button>
        </form>
      </section>
    </div>
  );
}

// ── Заказы: собранное у поставщика + возвраты ────────────────────────────────
function OrdersTab({ supplier, currency, profile, loading, onChanged }: { supplier: Supplier; currency: "USD" | "RUB"; profile?: SupplierProfile; loading: boolean; onChanged: () => void }) {
  const [all, setAll] = useState(false);
  const [amount, setAmount] = useState("");
  const [note, setNote] = useState("");
  const returnPicking = useMutation({
    mutationFn: (pickingKey: string) => fetchJson("/api/supplier-ledger/return-picking", SupplierLedgerPaymentSchema, mutationBody({ pickingKey, note: "" })),
    onSuccess: () => { toast.success("Возврат записан"); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const manualReturn = useMutation({
    mutationFn: () => fetchJson("/api/supplier-ledger/returns", SupplierLedgerPaymentSchema, mutationBody({
      supplierName: supplier.name || "", partnerId: supplier.partnerId || "", amount: Number(amount.replace(",", ".")), currency, note,
    })),
    onSuccess: () => { toast.success("Возврат записан"); setAmount(""); setNote(""); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const entries = profile?.ledger?.entries || [];
  const returned = new Set(entries.filter((e: LedgerEntry) => e.entryType === "supplier_return" && e.pickingKey).map((e: LedgerEntry) => String(e.pickingKey)));
  const debtByKey = new Map(entries.filter((e: LedgerEntry) => e.entryType === "purchase_debt" && e.pickingKey).map((e: LedgerEntry) => [String(e.pickingKey), e]));
  const cutoff = Date.now() - 30 * 86_400_000;
  const rows = (profile?.history || []).filter((r) => r.status === "picked" && (all || (r.pickedAt && new Date(r.pickedAt).getTime() >= cutoff)));
  return (
    <div className="sp2-stack">
      <section>
        <div className="sp2-section-head">
          <h3>Собрано у поставщика</h3>
          <div className="sp2-seg">
            <button type="button" className={!all ? "is-on" : ""} onClick={() => setAll(false)}>30 дней</button>
            <button type="button" className={all ? "is-on" : ""} onClick={() => setAll(true)}>Все</button>
          </div>
        </div>
        {loading ? <p className="sp2-muted"><Loader2 className="spin" size={13} /> Загружаем…</p> : rows.length ? (
          <ul className="sp2-orders">
            {rows.map((r) => {
              const debt = debtByKey.get(r.key);
              const sum = currency === "USD" && r.price ? money(Number(r.price) * Math.max(1, Number(r.quantity || 1)), String(r.priceCurrency || "USD"))
                : debt ? money(Math.abs(Number(debt.amount)), "RUB") : `${r.price ?? ""} ${r.priceCurrency ?? ""}`;
              const isReturned = returned.has(r.key);
              return (
                <li key={r.key}>
                  <div className="sp2-order-name"><span>{r.productName || r.offerId || r.key}</span><small>{r.offerId} · {compactDate(r.pickedAt ?? null)}</small></div>
                  <b>{sum}</b>
                  {isReturned ? <span className="sp2-chip"><RotateCcw size={12} /> возврат</span> : (
                    <button type="button" className="secondary-action compact" disabled={returnPicking.isPending} onClick={() => {
                      if (window.confirm(`Вернуть «${r.productName || r.offerId}» поставщику?`)) returnPicking.mutate(r.key);
                    }}><RotateCcw size={13} /> Возврат</button>
                  )}
                </li>
              );
            })}
          </ul>
        ) : <p className="sp2-muted">{all ? "Заказов у поставщика ещё не было." : "За 30 дней заказов нет."}</p>}
      </section>
      <section className="sp2-box">
        <h3>Возврат на произвольную сумму</h3>
        <form className="sp2-inline-form" onSubmit={(e) => { e.preventDefault(); if (Number(amount.replace(",", ".")) > 0) manualReturn.mutate(); }}>
          <span className="sp2-money-input"><input inputMode="decimal" value={amount} onChange={(e) => setAmount(e.target.value.replace(/[^\d.,]/g, ""))} placeholder="Сумма" /><span>{sym(currency)}</span></span>
          <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Комментарий" />
          <button className="secondary-action" type="submit" disabled={manualReturn.isPending || !(Number(amount.replace(",", ".")) > 0)}><RotateCcw size={14} /> Записать возврат</button>
        </form>
      </section>
    </div>
  );
}

// ── Артикулы ─────────────────────────────────────────────────────────────────
function ArticlesTab({ supplier, onChanged }: { supplier: Supplier; onChanged: () => void }) {
  const id = sid(supplier);
  const [draft, setDraft] = useState<{ id?: string; article: string; note: string }>({ article: "", note: "" });
  const save = useMutation({
    mutationFn: () => fetchJson(`/api/suppliers/${encodeURIComponent(id)}/articles`, OkSchema, mutationBody(draft)),
    onSuccess: () => { toast.success(draft.id ? "Артикул обновлён" : "Артикул добавлен"); setDraft({ article: "", note: "" }); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const del = useMutation({
    mutationFn: (articleId: string) => fetchJson(`/api/suppliers/${encodeURIComponent(id)}/articles/${encodeURIComponent(articleId)}`, OkSchema, { method: "DELETE" }),
    onSuccess: () => { toast.success("Артикул удалён"); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const articles = articlesOf(supplier);
  return (
    <div className="sp2-stack">
      <form className="sp2-inline-form" onSubmit={(e) => { e.preventDefault(); if (draft.article.trim()) save.mutate(); }}>
        <input value={draft.article} onChange={(e) => setDraft({ ...draft, article: e.target.value })} placeholder="Артикул" />
        <input value={draft.note} onChange={(e) => setDraft({ ...draft, note: e.target.value })} placeholder="Заметка" />
        <button className="primary-action" type="submit" disabled={save.isPending || !draft.article.trim()}>{draft.id ? "Сохранить" : <><Plus size={14} /> Добавить</>}</button>
        {draft.id ? <button className="secondary-action" type="button" onClick={() => setDraft({ article: "", note: "" })}>Отмена</button> : null}
      </form>
      {articles.length ? (
        <ul className="sp2-articles">
          {articles.map((a) => {
            const aid = String(a.id || a.article || "");
            return (
              <li key={aid}>
                <div><b>{String(a.article || "—")}</b>{a.note ? <small>{String(a.note)}</small> : null}</div>
                <button className="icon-action" type="button" title="Изменить" onClick={() => setDraft({ id: aid, article: String(a.article || ""), note: String(a.note || "") })}><Edit3 size={15} /></button>
                <button className="icon-action danger-action" type="button" title="Удалить" disabled={del.isPending} onClick={() => del.mutate(aid)}><Trash2 size={15} /></button>
              </li>
            );
          })}
        </ul>
      ) : <p className="sp2-muted">Артикулов нет.</p>}
    </div>
  );
}

// ── Настройки ────────────────────────────────────────────────────────────────
function SettingsTab({ supplier, onChanged }: { supplier: Supplier; onChanged: () => void }) {
  const r = asRecord(supplier);
  const initial = {
    name: String(supplier.name || ""),
    note: String(r.note || ""),
    priceCurrency: currencyOf(supplier),
    pricingMode: String(supplier.pricingMode || "normal") === "stock_only" ? "stock_only" : "normal",
    trustFactor: String(supplier.trustFactor ?? 100),
    orderCutoffTime: String(supplier.orderCutoffTime || ""),
    reseller: Boolean(supplier.reseller),
  };
  const [d, setD] = useState(initial);
  const dirty = JSON.stringify(d) !== JSON.stringify(initial);
  const save = useMutation({
    mutationFn: () => fetchJson(`/api/suppliers/${encodeURIComponent(sid(supplier))}`, OkSchema, patchBody({
      name: d.name.trim(), note: d.note.trim(), priceCurrency: d.priceCurrency, pricingMode: d.pricingMode,
      trustFactor: Number(d.trustFactor) || 100, orderCutoffTime: d.orderCutoffTime, reseller: d.reseller,
    })),
    onSuccess: () => { toast.success("Сохранено"); onChanged(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  return (
    <form className="sp2-settings" onSubmit={(e) => { e.preventDefault(); if (d.name.trim()) save.mutate(); }}>
      <label><span>Название</span><input value={d.name} onChange={(e) => setD({ ...d, name: e.target.value })} /></label>
      <label><span>Заметка</span><textarea rows={2} value={d.note} onChange={(e) => setD({ ...d, note: e.target.value })} placeholder="Что важно помнить о поставщике" /></label>
      <fieldset>
        <legend>Валюта прайса</legend>
        <div className="sp2-seg">
          <button type="button" className={d.priceCurrency === "USD" ? "is-on" : ""} onClick={() => setD({ ...d, priceCurrency: "USD" })}>$ доллары — цена × курс × наценка</button>
          <button type="button" className={d.priceCurrency === "RUB" ? "is-on" : ""} onClick={() => setD({ ...d, priceCurrency: "RUB" })}>₽ рубли — цена × наценка</button>
        </div>
      </fieldset>
      <fieldset>
        <legend>Что берём от поставщика</legend>
        <div className="sp2-seg">
          <button type="button" className={d.pricingMode === "normal" ? "is-on" : ""} onClick={() => setD({ ...d, pricingMode: "normal" })}>Цену и остаток</button>
          <button type="button" className={d.pricingMode === "stock_only" ? "is-on" : ""} onClick={() => setD({ ...d, pricingMode: "stock_only" })}>Только остаток</button>
        </div>
      </fieldset>
      <div className="sp2-settings-row">
        <label><span>Доверие, %</span><input inputMode="numeric" value={d.trustFactor} onChange={(e) => setD({ ...d, trustFactor: e.target.value.replace(/\D/g, "") })} /></label>
        <label><span>Приём заказов до</span><input type="time" value={d.orderCutoffTime} onChange={(e) => setD({ ...d, orderCutoffTime: e.target.value })} /></label>
      </div>
      <label className="sp2-check"><input type="checkbox" checked={d.reseller} onChange={(e) => setD({ ...d, reseller: e.target.checked })} /> Перекупщик (берёт товар у других)</label>
      <div className="sp2-form-actions">
        <button className="primary-action" type="submit" disabled={!dirty || save.isPending || !d.name.trim()}>{save.isPending ? <Loader2 className="spin" size={14} /> : null} {dirty ? "Сохранить" : "Сохранено"}</button>
        {dirty ? <button className="secondary-action" type="button" onClick={() => setD(initial)}>Отменить изменения</button> : null}
      </div>
    </form>
  );
}

// ── Остановка поставщика ─────────────────────────────────────────────────────
function StopDialog({ supplier, onClose, onDone }: { supplier: Supplier; onClose: () => void; onDone: () => void }) {
  const r = asRecord(supplier);
  const [comment, setComment] = useState(String(r.inactiveComment || supplier.stopReason || ""));
  const [until, setUntil] = useState(typeof r.inactiveUntil === "string" ? r.inactiveUntil.slice(0, 10) : dateInput(7));
  const [unknown, setUnknown] = useState(false);
  const stop = useMutation({
    mutationFn: () => fetchJson(`/api/suppliers/${encodeURIComponent(sid(supplier))}`, OkSchema, patchBody({
      stopped: true, stopReason: comment, inactiveComment: comment, inactiveUntil: unknown ? null : until, inactiveUntilUnknown: unknown,
    })),
    onSuccess: () => { toast.success(`${supplier.name} остановлен`); onDone(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  const quick: Array<[string, string]> = [["Не отвечает", "не отвечает"], ["Нет товара", "нет товара"], ["Отпуск", "отпуск"], ["Проблемы с качеством", "проблемы с качеством"]];
  return (
    <div className="sp2-modal" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <form className="sp2-dialog" onSubmit={(e) => { e.preventDefault(); stop.mutate(); }}>
        <div className="sp2-section-head"><h3>Остановить «{supplier.name}»</h3><button type="button" className="icon-action" onClick={onClose} aria-label="Закрыть"><X size={16} /></button></div>
        <p className="sp2-muted">Его товары перестанут продаваться с его цен и остатков. Вернуть можно одной кнопкой «Включить».</p>
        <div className="sp2-chips">{quick.map(([l, v]) => <button key={v} type="button" className={comment === v ? "is-on" : ""} onClick={() => setComment(v)}>{l}</button>)}</div>
        <label><span>Причина</span><textarea rows={2} value={comment} onChange={(e) => setComment(e.target.value)} placeholder="Например: уехал до 15-го" /></label>
        <label className="sp2-check"><input type="checkbox" checked={unknown} onChange={(e) => setUnknown(e.target.checked)} /> Срок неизвестен</label>
        {!unknown ? (
          <>
            <label><span>Вернётся</span><input type="date" value={until} onChange={(e) => setUntil(e.target.value)} /></label>
            <div className="sp2-chips">
              <button type="button" onClick={() => setUntil(dateInput(1))}>завтра</button>
              <button type="button" onClick={() => setUntil(dateInput(7))}>+7 дней</button>
              <button type="button" onClick={() => setUntil(dateInput(14))}>+14 дней</button>
              <button type="button" onClick={() => setUntil(endOfMonth())}>до конца месяца</button>
            </div>
          </>
        ) : null}
        <div className="sp2-form-actions">
          <button className="primary-action danger-fill" type="submit" disabled={stop.isPending}>{stop.isPending ? <Loader2 className="spin" size={14} /> : <Ban size={14} />} Остановить</button>
          <button className="secondary-action" type="button" onClick={onClose}>Отмена</button>
        </div>
      </form>
    </div>
  );
}

// ── Новый поставщик ──────────────────────────────────────────────────────────
function CreateDialog({ onClose, onCreated }: { onClose: () => void; onCreated: (id: string) => void }) {
  const [name, setName] = useState("");
  const [currency, setCurrency] = useState<"USD" | "RUB">("USD");
  const [note, setNote] = useState("");
  const create = useMutation({
    mutationFn: () => anyJson<{ warehouse?: { suppliers?: Array<{ id?: string; name?: string }> } }>("/api/suppliers", mutationBody({ name: name.trim(), note: note.trim(), priceCurrency: currency })),
    onSuccess: (r) => {
      toast.success("Поставщик добавлен");
      const made = (r?.warehouse?.suppliers || []).filter((s) => String(s.name || "").trim().toLowerCase() === name.trim().toLowerCase()).pop();
      onCreated(String(made?.id || ""));
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  return (
    <div className="sp2-modal" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <form className="sp2-dialog" onSubmit={(e) => { e.preventDefault(); if (name.trim()) create.mutate(); }}>
        <div className="sp2-section-head"><h3>Новый поставщик</h3><button type="button" className="icon-action" onClick={onClose} aria-label="Закрыть"><X size={16} /></button></div>
        <p className="sp2-muted">Поставщики из PriceMaster появляются сами — кнопка «Из PriceMaster». Вручную — для тех, кого там нет.</p>
        <label><span>Название</span><input autoFocus value={name} onChange={(e) => setName(e.target.value)} /></label>
        <fieldset>
          <legend>Валюта прайса</legend>
          <div className="sp2-seg">
            <button type="button" className={currency === "USD" ? "is-on" : ""} onClick={() => setCurrency("USD")}>$ доллары</button>
            <button type="button" className={currency === "RUB" ? "is-on" : ""} onClick={() => setCurrency("RUB")}>₽ рубли</button>
          </div>
        </fieldset>
        <label><span>Заметка</span><input value={note} onChange={(e) => setNote(e.target.value)} /></label>
        <div className="sp2-form-actions">
          <button className="primary-action" type="submit" disabled={create.isPending || !name.trim()}>{create.isPending ? <Loader2 className="spin" size={14} /> : <Plus size={14} />} Добавить</button>
          <button className="secondary-action" type="button" onClick={onClose}>Отмена</button>
        </div>
      </form>
    </div>
  );
}
