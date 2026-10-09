import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, ChevronDown, ChevronUp, Loader2, Plus, RefreshCw, Trash2 } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { ListSkeleton } from "../components/Skeleton";
import { errorMessage } from "../lib/common";
import { toast } from "../lib/toast";
import "./money.css";

// Деньги с маркетплейсов: что начислили Ozon и Маркет, что они удержали, закупка, свои расходы и налог —
// и что осталось. Данные собирает воркер раз в час (/api/money/sync — свежие дни сразу).

type Line = {
  sales: number; returns: number; commission: number; logistics: number; promotion: number; storage: number; penalties: number;
  services: number; compensation: number; other: number; cost: number; expenses: number; tax: number; payout: number; revenue: number; net: number;
};
type Bucket = Line & { key: string; orders: number; ordersAmount: number; cancelled: number; cancelledAmount: number; returnedOrders: number };
type AccountLine = Line & { key: string; marketplace: string; account: string; name: string; orders: number; cancelled: number };
type CostRow = { marketplace: string; account: string; ref: string; day: string; sale: number; payout?: number; cost?: number; offerIds: string; name: string; status: "missing" | "low" };
type Summary = {
  from: string; to: string; group: string; account: string;
  totals: Line & { orders: number; ordersAmount: number; cancelled: number; cancelledAmount: number; returnedOrders: number; soldRefs: number };
  withoutCost: { count: number; sales: number; payout: number; items: CostRow[] };
  lowCost: CostRow[];
  series: Bucket[];
  byAccount: AccountLine[];
  breakdown: Array<{ category: string; marketplace: string; name: string; amount: number }>;
  expenses: Array<{ id: string; type: string; amount: number; note: string; day: string }>;
  settings: { taxMode: "none" | "income" | "profit"; taxRate: number };
  accounts: Array<{ id: string; name: string; marketplace: string }>;
  sync: { lastRunAt: string | null; running: boolean; historyDays: number; lastResult: { errors?: string[] } | null };
};

const api = <T,>(url: string, init?: RequestInit) => fetchJson<T>(url, z.custom<T>(() => true), init);

const CATEGORY: Record<string, string> = {
  sales: "Продажи", returns: "Возвраты", commission: "Комиссии и эквайринг", logistics: "Логистика", promotion: "Продвижение и реклама",
  storage: "Хранение и размещение", penalties: "Штрафы", services: "Подписки и услуги", compensation: "Компенсации", other: "Прочее",
  cost: "Закупка товара", expenses: "Свои расходы", tax: "Налог",
};
const MP_DEDUCTIONS = ["commission", "logistics", "promotion", "storage", "penalties", "services", "compensation", "other"] as const;
const EXPENSE_TYPES: Array<[string, string]> = [
  ["advertising", "Реклама вне маркетплейсов"], ["salary", "Зарплата"], ["rent", "Аренда"], ["packaging", "Упаковка"],
  ["delivery", "Доставка до склада / ПВЗ"], ["services", "Сервисы и подписки"], ["tax", "Налоги и взносы"], ["other", "Прочее"],
];
const EXPENSE_LABEL = Object.fromEntries(EXPENSE_TYPES);

const rub = (v: number) => `${Math.round(v).toLocaleString("ru-RU")} ₽`;
const signed = (v: number) => (Math.round(v) > 0 ? `+${rub(v)}` : rub(v));
const compact = (v: number) => {
  const a = Math.abs(v);
  if (a >= 1_000_000) return `${(v / 1_000_000).toLocaleString("ru-RU", { maximumFractionDigits: 1 })} млн`;
  if (a >= 1000) return `${Math.round(v / 1000).toLocaleString("ru-RU")} тыс`;
  return String(Math.round(v));
};
const pct = (part: number, whole: number) => (whole ? `${((part / whole) * 100).toLocaleString("ru-RU", { maximumFractionDigits: 1 })}%` : "—");

const iso = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
const shift = (d: Date, days: number) => { const x = new Date(d); x.setDate(x.getDate() + days); return x; };
type Preset = "today" | "yesterday" | "7d" | "month" | "prev-month" | "30d" | "90d" | "custom";
const PRESETS: Array<[Preset, string]> = [
  ["today", "Сегодня"], ["yesterday", "Вчера"], ["7d", "7 дней"], ["month", "Этот месяц"], ["prev-month", "Прошлый месяц"], ["30d", "30 дней"], ["90d", "90 дней"], ["custom", "Свой период"],
];
function presetRange(preset: Preset): [string, string] {
  const now = new Date();
  if (preset === "today") return [iso(now), iso(now)];
  if (preset === "yesterday") return [iso(shift(now, -1)), iso(shift(now, -1))];
  if (preset === "7d") return [iso(shift(now, -6)), iso(now)];
  if (preset === "month") return [iso(new Date(now.getFullYear(), now.getMonth(), 1)), iso(now)];
  if (preset === "prev-month") return [iso(new Date(now.getFullYear(), now.getMonth() - 1, 1)), iso(new Date(now.getFullYear(), now.getMonth(), 0))];
  if (preset === "90d") return [iso(shift(now, -89)), iso(now)];
  return [iso(shift(now, -29)), iso(now)];
}
const dayLabel = (key: string, group: string) => {
  const d = new Date(`${key}T00:00:00`);
  if (group === "month") return d.toLocaleDateString("ru-RU", { month: "long", year: "numeric" });
  if (group === "week") return `с ${d.toLocaleDateString("ru-RU", { day: "numeric", month: "short" })}`;
  return d.toLocaleDateString("ru-RU", { day: "numeric", month: "short", weekday: "short" });
};
const deductionsOf = (l: Line) => MP_DEDUCTIONS.reduce((s, c) => s + (l[c] || 0), 0);

export function MoneyPage() {
  const queryClient = useQueryClient();
  const [preset, setPreset] = useState<Preset>("30d");
  const [custom, setCustom] = useState<[string, string]>(() => presetRange("30d"));
  const [group, setGroup] = useState<"day" | "week" | "month">("day");
  const [account, setAccount] = useState("all");
  const [from, to] = preset === "custom" ? custom : presetRange(preset);

  const summary = useQuery({
    queryKey: ["money", "summary", from, to, group, account],
    queryFn: () => api<Summary>(`/api/money/summary?from=${from}&to=${to}&group=${group}&account=${encodeURIComponent(account)}`),
    placeholderData: (prev) => prev,
    refetchInterval: (q) => (q.state.data?.sync.running ? 5000 : 120_000),
  });
  const sync = useMutation({
    mutationFn: () => api<{ status: string }>("/api/money/sync", mutationBody({})),
    onSuccess: (res) => {
      toast.info(res.status === "already_running" ? "Данные уже обновляются" : "Обновляем последние 3 дня — займёт минуту-две");
      window.setTimeout(() => void queryClient.invalidateQueries({ queryKey: ["money"] }), 4000);
    },
    onError: (error) => toast.error(errorMessage(error)),
  });

  const data = summary.data;
  const t = data?.totals;

  return (
    <section className="page-section money-page">
      <PageHeader
        title="Прибыль"
        subtitle="Деньги с Ozon и Маркета: продажи, всё, что удержали маркетплейсы, закупка, свои расходы и налог — и сколько осталось."
        action={(
          <div className="money-sync">
            <button className="secondary-action compact" type="button" disabled={sync.isPending || data?.sync.running} onClick={() => sync.mutate()}>
              {sync.isPending || data?.sync.running ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />} Обновить данные
            </button>
            <span className="fr-hint">{data?.sync.lastRunAt ? `обновлено ${new Date(data.sync.lastRunAt).toLocaleString("ru-RU", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" })}` : "ещё не собирались"}</span>
          </div>
        )}
      />

      <div className="money-controls">
        <div className="money-chips" role="group" aria-label="Период">
          {PRESETS.map(([id, label]) => (
            <button key={id} type="button" className={`money-chip${preset === id ? " is-on" : ""}`} aria-pressed={preset === id} onClick={() => setPreset(id)}>{label}</button>
          ))}
        </div>
        {preset === "custom" ? (
          <div className="money-dates">
            <input type="date" value={custom[0]} max={custom[1]} onChange={(e) => setCustom([e.target.value, custom[1]])} aria-label="С" />
            <span>—</span>
            <input type="date" value={custom[1]} min={custom[0]} onChange={(e) => setCustom([custom[0], e.target.value])} aria-label="По" />
          </div>
        ) : null}
        <div className="money-chips" role="group" aria-label="Группировка">
          {([["day", "По дням"], ["week", "По неделям"], ["month", "По месяцам"]] as const).map(([id, label]) => (
            <button key={id} type="button" className={`money-chip${group === id ? " is-on" : ""}`} aria-pressed={group === id} onClick={() => setGroup(id)}>{label}</button>
          ))}
        </div>
        <select className="money-select" value={account} onChange={(e) => setAccount(e.target.value)} aria-label="Кабинет">
          <option value="all">Все кабинеты</option>
          <option value="ozon">Все Ozon</option>
          <option value="yandex">Все Маркет</option>
          {(data?.accounts || []).map((a) => <option key={a.id} value={a.id}>{a.marketplace === "ozon" ? "Ozon" : "Маркет"} · {a.name}</option>)}
        </select>
      </div>

      {summary.error ? <div className="fr-warn">{errorMessage(summary.error)}</div> : null}
      {!data || !t ? <ListSkeleton rows={6} /> : (
        <>
          <Notices data={data} />
          <div className="money-top">
            <Ledger line={t} />
            <OrdersCard t={t} />
          </div>
          <CostsToFill data={data} />
          <Chart series={data.series} group={data.group} />
          <PeriodTable series={data.series} group={data.group} />
          {data.byAccount.length > 1 ? <AccountsTable rows={data.byAccount} /> : null}
          <Breakdown rows={data.breakdown} />
          <div className="money-two">
            <Expenses data={data} />
            <TaxSettings settings={data.settings} />
          </div>
        </>
      )}
    </section>
  );
}

function Notices({ data }: { data: Summary }) {
  const t = data.totals;
  const notes: string[] = [];
  if (data.withoutCost.count) notes.push(`Не в расчёте ${data.withoutCost.count} ${data.withoutCost.count === 1 ? "заказ" : "заказов"} на ${rub(data.withoutCost.sales)}: их закупка была не через автокорзину. Впишите закупку ниже — заказ вернётся в прибыль.`);
  if (data.lowCost.length) notes.push(`У ${data.lowCost.length} заказов закупка подозрительно мала (меньше 8% продажи, например «Наш склад» по $1) — проверьте её ниже.`);
  for (const e of data.sync.lastResult?.errors || []) notes.push(`Последнее обновление: ${e}`);
  if (!data.sync.lastRunAt) notes.push(`Данные ещё не собраны: воркер начнёт в течение 5 минут после перезапуска и заполнит историю за ${data.sync.historyDays} дней за несколько часов.`);
  return (
    <>
      {notes.map((n) => <div key={n} className="fr-warn money-note"><AlertTriangle size={14} /> {n}</div>)}
      <p className="fr-hint money-legend">Ozon — по дате начисления (доставка, возврат, услуга). Маркет — по дате доставки или возврата заказа; его комиссии приходят примерно через сутки.</p>
    </>
  );
}

/** «Лента денег»: from the sales down to what is left, each step as a share of the sales. */
function Ledger({ line }: { line: Line }) {
  const base = Math.max(line.sales, 1);
  const steps: Array<{ label: string; value: number; kind: "in" | "out" | "sum" | "net" }> = [
    { label: CATEGORY.sales, value: line.sales, kind: "in" },
    { label: CATEGORY.returns, value: line.returns, kind: "out" },
    ...MP_DEDUCTIONS.filter((c) => Math.round(line[c] || 0) !== 0).map((c) => ({ label: CATEGORY[c], value: line[c], kind: (line[c] > 0 ? "in" : "out") as "in" | "out" })),
    { label: "К выплате от маркетплейсов", value: line.payout, kind: "sum" },
    { label: CATEGORY.cost, value: line.cost, kind: "out" },
    ...(Math.round(line.expenses) ? [{ label: CATEGORY.expenses, value: line.expenses, kind: "out" as const }] : []),
    ...(Math.round(line.tax) ? [{ label: CATEGORY.tax, value: line.tax, kind: "out" as const }] : []),
  ];
  return (
    <section className="money-ledger" aria-label="Из чего складывается прибыль">
      <div className="money-net">
        <span>Чистая прибыль</span>
        <strong className={line.net < 0 ? "is-loss" : ""}>{rub(line.net)}</strong>
        <small>{line.sales ? `${pct(line.net, line.revenue || line.sales)} от выручки · маркетплейсы удержали ${pct(-deductionsOf(line), line.sales)}` : "за период продаж нет"}</small>
      </div>
      <ol className="money-steps">
        {steps.map((s) => (
          <li key={s.label} className={`is-${s.kind}`}>
            <span className="money-step-label">{s.label}</span>
            <span className="money-step-bar" aria-hidden="true"><i style={{ width: `${Math.min(100, (Math.abs(s.value) / base) * 100)}%` }} /></span>
            <span className="money-step-value">{s.kind === "sum" ? rub(s.value) : signed(s.value)}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}

function OrdersCard({ t }: { t: Summary["totals"] }) {
  return (
    <section className="money-orders" aria-label="Заказы и отмены">
      <div><span>Заказов</span><strong>{t.orders.toLocaleString("ru-RU")}</strong><small>на {rub(t.ordersAmount)}</small></div>
      <div className={t.cancelled ? "is-warn" : ""}><span>Отменено</span><strong>{t.cancelled.toLocaleString("ru-RU")}</strong><small>{pct(t.cancelled, t.orders)} · {rub(t.cancelledAmount)}</small></div>
      <div className={Math.round(t.returns) ? "is-warn" : ""}><span>Возвраты</span><strong>{rub(-t.returns)}</strong><small>{pct(-t.returns, t.sales)} от продаж{t.returnedOrders ? ` · ${t.returnedOrders} заказов Маркета` : ""}</small></div>
      <div><span>Средний чек продажи</span><strong>{t.soldRefs ? rub(t.sales / t.soldRefs) : "—"}</strong><small>{t.soldRefs} продаж</small></div>
    </section>
  );
}

function Chart({ series, group }: { series: Bucket[]; group: string }) {
  const [hover, setHover] = useState<number | null>(null);
  if (!series.length) return null;
  const max = Math.max(1, ...series.map((b) => Math.max(b.sales, Math.abs(b.net))));
  const minNet = Math.min(0, ...series.map((b) => b.net));
  const H = 180, top = 8;
  const zero = top + (max / (max - minNet)) * (H - top);
  const y = (v: number) => zero - (v / (max - minNet)) * (H - top);
  const w = 100 / series.length;
  const active = hover !== null ? series[hover] : null;
  return (
    <section className="money-chart" aria-label="Продажи и чистая прибыль по периодам">
      <div className="money-chart-head">
        <b>Продажи и чистая прибыль</b>
        <span className="money-key"><i className="k-sales" /> продажи <i className="k-net" /> чистая прибыль <i className="k-loss" /> убыток</span>
        {active ? <span className="money-chart-tip">{dayLabel(active.key, group)}: продажи {rub(active.sales)}, прибыль {rub(active.net)}, заказов {active.orders}{active.cancelled ? `, отмен ${active.cancelled}` : ""}</span> : null}
      </div>
      <svg viewBox={`0 0 100 ${H}`} preserveAspectRatio="none" role="img" onMouseLeave={() => setHover(null)}>
        <line x1="0" x2="100" y1={zero} y2={zero} className="money-axis" />
        {series.map((b, i) => (
          <g key={b.key} onMouseEnter={() => setHover(i)} className={hover === i ? "is-hover" : ""}>
            <rect x={i * w} y={0} width={w} height={H} className="money-hit" />
            <rect x={i * w + w * 0.12} width={w * 0.38} y={y(b.sales)} height={Math.max(0, zero - y(b.sales))} className="money-bar-sales" />
            <rect x={i * w + w * 0.5} width={w * 0.38} y={b.net >= 0 ? y(b.net) : zero} height={Math.abs(y(b.net) - zero)} className={b.net >= 0 ? "money-bar-net" : "money-bar-loss"} />
          </g>
        ))}
      </svg>
      <div className="money-chart-axis">
        <span>{dayLabel(series[0].key, group)}</span>
        <span>макс. {compact(max)} ₽</span>
        <span>{dayLabel(series[series.length - 1].key, group)}</span>
      </div>
    </section>
  );
}

function PeriodTable({ series, group }: { series: Bucket[]; group: string }) {
  const [open, setOpen] = useState(false);
  const rows = [...series].reverse();
  const shown = open ? rows : rows.slice(0, 14);
  return (
    <section className="money-table-wrap">
      <div className="money-section-head"><b>По {group === "month" ? "месяцам" : group === "week" ? "неделям" : "дням"}</b></div>
      <div className="money-scroll">
        <table className="money-table">
          <thead>
            <tr>
              <th>Период</th><th>Заказы</th><th>Отмены</th><th>Продажи</th><th>Возвраты</th><th>Комиссии</th><th>Логистика</th><th>Реклама</th>
              <th>Хранение, штрафы, прочее</th><th>К выплате</th><th>Закупка</th><th>Расходы и налог</th><th>Чистая прибыль</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((b) => (
              <tr key={b.key}>
                <th scope="row">{dayLabel(b.key, group)}</th>
                <td>{b.orders || "—"}</td>
                <td className={b.cancelled ? "is-warn" : ""}>{b.cancelled ? `${b.cancelled} · ${pct(b.cancelled, b.orders)}` : "—"}</td>
                <td>{rub(b.sales)}</td>
                <td>{Math.round(b.returns) ? rub(b.returns) : "—"}</td>
                <td>{rub(b.commission)}</td>
                <td>{rub(b.logistics)}</td>
                <td>{Math.round(b.promotion) ? rub(b.promotion) : "—"}</td>
                <td>{rub(b.storage + b.penalties + b.services + b.compensation + b.other)}</td>
                <td>{rub(b.payout)}</td>
                <td>{rub(b.cost)}</td>
                <td>{Math.round(b.expenses + b.tax) ? rub(b.expenses + b.tax) : "—"}</td>
                <td className={b.net < 0 ? "is-loss" : "is-net"}>{rub(b.net)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {rows.length > 14 ? (
        <button className="secondary-action compact money-more" type="button" onClick={() => setOpen(!open)}>
          {open ? <ChevronUp size={13} /> : <ChevronDown size={13} />} {open ? "Свернуть" : `Показать все ${rows.length}`}
        </button>
      ) : null}
    </section>
  );
}

function AccountsTable({ rows }: { rows: AccountLine[] }) {
  return (
    <section className="money-table-wrap">
      <div className="money-section-head"><b>По кабинетам</b></div>
      <div className="money-scroll">
        <table className="money-table">
          <thead><tr><th>Кабинет</th><th>Заказы</th><th>Отмены</th><th>Продажи</th><th>Удержали маркетплейсы</th><th>К выплате</th><th>Закупка</th><th>Прибыль до расходов</th><th>Маржа</th></tr></thead>
          <tbody>
            {rows.sort((a, b) => b.sales - a.sales).map((r) => {
              const gross = r.payout + r.cost;
              return (
                <tr key={r.key}>
                  <th scope="row">{r.marketplace === "ozon" ? "Ozon" : "Маркет"} · {r.name}</th>
                  <td>{r.orders || "—"}</td>
                  <td>{r.cancelled ? `${r.cancelled} · ${pct(r.cancelled, r.orders)}` : "—"}</td>
                  <td>{rub(r.sales + r.returns)}</td>
                  <td>{rub(deductionsOf(r))} · {pct(-deductionsOf(r), r.sales)}</td>
                  <td>{rub(r.payout)}</td>
                  <td>{rub(r.cost)}</td>
                  <td className={gross < 0 ? "is-loss" : "is-net"}>{rub(gross)}</td>
                  <td>{pct(gross, r.sales + r.returns)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Breakdown({ rows }: { rows: Summary["breakdown"] }) {
  const [open, setOpen] = useState<string | null>(null);
  const groups = useMemo(() => {
    const map = new Map<string, { total: number; items: Summary["breakdown"] }>();
    for (const r of rows) {
      const g = map.get(r.category) || { total: 0, items: [] };
      g.total += r.amount; g.items.push(r);
      map.set(r.category, g);
    }
    return [...map.entries()].sort((a, b) => a[1].total - b[1].total);
  }, [rows]);
  if (!groups.length) return null;
  return (
    <section className="money-breakdown">
      <div className="money-section-head"><b>Что удержали маркетплейсы</b><span className="fr-hint">нажмите на статью — покажем, из чего она состоит</span></div>
      {groups.map(([category, g]) => (
        <div key={category} className="money-bd-group">
          <button type="button" className="money-bd-head" aria-expanded={open === category} onClick={() => setOpen(open === category ? null : category)}>
            <span>{CATEGORY[category] || category}</span>
            <b className={g.total > 0 ? "is-in" : ""}>{signed(g.total)}</b>
            {open === category ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          </button>
          {open === category ? (
            <ul>
              {g.items.sort((a, b) => a.amount - b.amount).map((i) => (
                <li key={`${i.marketplace}-${i.name}`}><span>{i.name}</span><small>{i.marketplace === "ozon" ? "Ozon" : "Маркет"}</small><b>{signed(i.amount)}</b></li>
              ))}
            </ul>
          ) : null}
        </div>
      ))}
    </section>
  );
}

function Expenses({ data }: { data: Summary }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState({ type: "advertising", amount: "", day: iso(new Date()), note: "" });
  const refresh = () => queryClient.invalidateQueries({ queryKey: ["money"] });
  const add = useMutation({
    mutationFn: () => api("/api/money/expenses", mutationBody({ ...form, amount: Number(String(form.amount).replace(",", ".").replace(/\s/g, "")) })),
    onSuccess: () => { toast.success("Расход добавлен"); setForm((f) => ({ ...f, amount: "", note: "" })); void refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const remove = useMutation({
    mutationFn: (id: string) => api(`/api/money/expenses/${encodeURIComponent(id)}`, { method: "DELETE" }),
    onSuccess: () => { toast.success("Расход удалён"); void refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const total = data.expenses.reduce((s, e) => s + e.amount, 0);
  return (
    <section className="money-card">
      <div className="money-section-head"><b>Свои расходы</b><span className="fr-hint">реклама вне маркетплейсов, зарплата, упаковка… — вычитаются из прибыли в день расхода</span></div>
      <form className="money-expense-form" onSubmit={(e) => { e.preventDefault(); add.mutate(); }}>
        <select value={form.type} onChange={(e) => setForm({ ...form, type: e.target.value })} aria-label="Статья">
          {EXPENSE_TYPES.map(([id, label]) => <option key={id} value={id}>{label}</option>)}
        </select>
        <input inputMode="decimal" placeholder="Сумма, ₽" value={form.amount} onChange={(e) => setForm({ ...form, amount: e.target.value })} aria-label="Сумма" />
        <input type="date" value={form.day} onChange={(e) => setForm({ ...form, day: e.target.value })} aria-label="Дата" />
        <input placeholder="Комментарий" value={form.note} onChange={(e) => setForm({ ...form, note: e.target.value })} aria-label="Комментарий" />
        <button className="primary-action compact" type="submit" disabled={add.isPending || !form.amount}>
          {add.isPending ? <Loader2 size={14} className="spin" /> : <Plus size={14} />} Добавить расход
        </button>
      </form>
      {data.account !== "all" ? <p className="fr-hint">Свои расходы считаются только в режиме «Все кабинеты».</p> : null}
      {data.expenses.length ? (
        <ul className="money-expense-list">
          {data.expenses.map((e) => (
            <li key={e.id}>
              <span>{new Date(`${e.day}T00:00:00`).toLocaleDateString("ru-RU", { day: "numeric", month: "short" })}</span>
              <span>{EXPENSE_LABEL[e.type] || e.type}{e.note ? ` — ${e.note}` : ""}</span>
              <b>{rub(e.amount)}</b>
              <button className="icon-action" type="button" title="Удалить расход" disabled={remove.isPending} onClick={() => remove.mutate(e.id)}><Trash2 size={14} /></button>
            </li>
          ))}
          <li className="money-expense-total"><span /><span>Итого за период</span><b>{rub(total)}</b><span /></li>
        </ul>
      ) : <p className="fr-hint">За период своих расходов нет.</p>}
    </section>
  );
}

function TaxSettings({ settings }: { settings: Summary["settings"] }) {
  const queryClient = useQueryClient();
  const [mode, setMode] = useState(settings.taxMode);
  const [rate, setRate] = useState(String(settings.taxRate || ""));
  const save = useMutation({
    mutationFn: () => api("/api/money/settings", mutationBody({ taxMode: mode, taxRate: Number(rate.replace(",", ".")) || 0 })),
    onSuccess: () => { toast.success("Налог сохранён"); void queryClient.invalidateQueries({ queryKey: ["money"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const changed = mode !== settings.taxMode || (Number(rate.replace(",", ".")) || 0) !== settings.taxRate;
  return (
    <section className="money-card">
      <div className="money-section-head"><b>Налог</b><span className="fr-hint">вычитается из прибыли по каждому дню</span></div>
      <div className="money-tax">
        {([["none", "Не считать"], ["income", "УСН «Доходы» — % от выручки"], ["profit", "УСН «Доходы минус расходы» — % от прибыли"]] as const).map(([id, label]) => (
          <label key={id} className="money-radio"><input type="radio" name="money-tax" checked={mode === id} onChange={() => setMode(id)} /> {label}</label>
        ))}
        {mode !== "none" ? (
          <label className="money-rate">Ставка <input inputMode="decimal" value={rate} onChange={(e) => setRate(e.target.value)} placeholder={mode === "income" ? "6" : "15"} /> %</label>
        ) : null}
        <button className="secondary-action compact" type="button" disabled={!changed || save.isPending} onClick={() => save.mutate()}>
          {save.isPending ? <Loader2 size={14} className="spin" /> : null} Сохранить
        </button>
      </div>
    </section>
  );
}

/** Orders left out of the profit (bought outside the autocart) and too-small purchases: the purchase typed by hand. */
function CostsToFill({ data }: { data: Summary }) {
  const [tab, setTab] = useState<"missing" | "low">("missing");
  const [open, setOpen] = useState(false);
  const rows = tab === "missing" ? data.withoutCost.items.filter((r) => r.sale > 0) : data.lowCost;
  if (!data.withoutCost.count && !data.lowCost.length) return null;
  const shown = open ? rows : rows.slice(0, 15);
  return (
    <section className="money-table-wrap">
      <div className="money-section-head">
        <b>Закупка вручную</b>
        <span className="fr-hint">Заказы, купленные не через автокорзину, не входят в прибыль, пока не указана их закупка.</span>
      </div>
      <div className="money-chips money-costs-tabs" role="tablist">
        <button type="button" role="tab" aria-selected={tab === "missing"} className={`money-chip${tab === "missing" ? " is-on" : ""}`} onClick={() => setTab("missing")}>
          Без закупки · {data.withoutCost.count}
        </button>
        <button type="button" role="tab" aria-selected={tab === "low"} className={`money-chip${tab === "low" ? " is-on" : ""}`} onClick={() => setTab("low")}>
          Низкая закупка · {data.lowCost.length}
        </button>
      </div>
      {rows.length ? (
        <div className="money-scroll">
          <table className="money-table money-costs">
            <thead>
              <tr><th>Дата</th><th>Заказ</th><th>Товар</th><th>Продажа</th><th>{tab === "missing" ? "К выплате" : "Закупка сейчас"}</th><th>Закупка, ₽</th></tr>
            </thead>
            <tbody>
              {shown.map((r) => <CostRowView key={`${r.marketplace}:${r.ref}`} row={r} />)}
            </tbody>
          </table>
        </div>
      ) : <p className="fr-hint">Здесь пусто.</p>}
      {rows.length > 15 ? (
        <button className="secondary-action compact money-more" type="button" onClick={() => setOpen(!open)}>
          {open ? <ChevronUp size={13} /> : <ChevronDown size={13} />} {open ? "Свернуть" : `Показать все ${rows.length}`}
        </button>
      ) : null}
    </section>
  );
}

function CostRowView({ row }: { row: CostRow }) {
  const queryClient = useQueryClient();
  const [value, setValue] = useState("");
  const save = useMutation({
    mutationFn: () => api("/api/money/costs", mutationBody({ marketplace: row.marketplace, ref: row.ref, cost: value })),
    onSuccess: () => { toast.success(`Закупка сохранена: заказ ${row.ref} в расчёте`); void queryClient.invalidateQueries({ queryKey: ["money"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  return (
    <tr>
      <td>{new Date(`${row.day}T00:00:00`).toLocaleDateString("ru-RU", { day: "numeric", month: "short" })}</td>
      <th scope="row" className="money-costs-ref">{row.marketplace === "ozon" ? "Ozon" : "Маркет"} · {row.ref}</th>
      <td className="money-costs-name" title={row.name}>{row.name || "—"}{row.offerIds ? <small> · {row.offerIds}</small> : null}</td>
      <td>{rub(row.sale)}</td>
      <td>{row.status === "missing" ? rub(row.payout || 0) : rub(row.cost || 0)}</td>
      <td>
        <form className="money-costs-form" onSubmit={(e) => { e.preventDefault(); if (value) save.mutate(); }}>
          <input inputMode="decimal" placeholder="₽" value={value} onChange={(e) => setValue(e.target.value)} aria-label={`Закупка по заказу ${row.ref}`} />
          <button className="secondary-action compact" type="submit" disabled={!value || save.isPending}>
            {save.isPending ? <Loader2 size={13} className="spin" /> : null} Сохранить
          </button>
        </form>
      </td>
    </tr>
  );
}
