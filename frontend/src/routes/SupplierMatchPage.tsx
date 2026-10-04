import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, Link2, Loader2, PackageCheck, RefreshCw, Rocket, Search, ShieldCheck, Users, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { errorMessage } from "../lib/common";
import { toast } from "../lib/toast";
import { ExcludedSuppliers } from "../components/ExcludedSuppliers";
import "./supplier-match.css";

// «Подбор поставщиков»: строки PriceMaster, найденные для товаров склада. Привязка — только по кнопке.
//   «Запустить в продажу» — товар без привязок или без живого поставщика: найдена строка, на которой он продастся.
//   «Новые привязки» — товар продаётся: найдены поставщики, которых у него ещё нет.

type Tab = "launch" | "extra";
type Row = { rowId?: string; article?: string; name?: string; supplierName?: string; partnerId?: string; price?: number; priceCurrency?: string; updatedAt?: string | null };
type Suggestion = { id: string; row: Row; ozonPrice: number | null; confidence: "exact" | "probable"; issues: string[]; status: string; error: string | null };
type Product = { offerKey: string; productName: string; productIds: string[]; shops: string[]; cardStatus: string; archived: boolean; sold30: number; suggestions: Suggestion[] };
type Counts = { launchProducts: number; launchExact: number; extraProducts: number; extraExact: number; filtered: number; exactSelected: number };
type SupplierOption = { partnerId: string; name: string; products: number; exact: number };
type ScanState = { at?: string; running?: boolean; status?: string; error?: string; suggestions?: number; elapsedMs?: number };
type ApproveJob = { running: boolean; total: number; linked: number; failed: number; products: number; error?: string } | null;
type Page = { products: Product[]; counts: Counts; suppliers: SupplierOption[]; scan: ScanState; scanRequest: { at?: string }; approveJob: ApproveJob };

const PAGE = 40;
const anyJson = <T,>(url: string, init?: RequestInit) => fetchJson<T>(url, z.custom<T>(() => true), init);

const shopLabel = (target: string) => (target.startsWith("yandex") ? "Маркет" : target === "ozon-3d10ec43" ? "AURA" : "Ozon");
const statusLabel: Record<string, [string, string]> = {
  no_links: ["нет привязки", "warn"],
  no_supplier: ["нет поставщика", "danger"],
  selling: ["в продаже", "ok"],
};
const ago = (iso?: string) => {
  if (!iso) return "ещё не проверялось";
  const min = Math.round((Date.now() - new Date(iso).getTime()) / 60_000);
  if (min < 1) return "только что";
  if (min < 60) return `${min} мин назад`;
  const h = Math.round(min / 60);
  return h < 24 ? `${h} ч назад` : `${Math.round(h / 24)} дн назад`;
};
const money = (v: unknown, currency = "USD") => {
  const n = Number(v || 0);
  return `${n.toLocaleString("ru-RU", { maximumFractionDigits: 2 })} ${String(currency).toUpperCase() === "RUB" ? "₽" : "$"}`;
};

export function SupplierMatchPage() {
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<Tab>("launch");
  const [exactOnly, setExactOnly] = useState(false);
  const [search, setSearch] = useState("");
  // «Только от поставщиков»: partner id (or name when there is none), empty = everyone
  const [only, setOnly] = useState<string[]>([]);
  const [q, setQ] = useState("");
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState<Set<string>>(new Set());

  useEffect(() => {
    const t = setTimeout(() => setQ(search.trim()), 300);
    return () => clearTimeout(t);
  }, [search]);

  const key = ["supplier-match", tab, exactOnly, q, only.join(",")];
  const list = useInfiniteQuery({
    queryKey: key,
    initialPageParam: 0,
    queryFn: ({ pageParam }) => anyJson<Page>(`/api/supplier-match?tab=${tab}&confidence=${exactOnly ? "exact" : "all"}&q=${encodeURIComponent(q)}&only=${encodeURIComponent(only.join(","))}&limit=${PAGE}&offset=${pageParam}`),
    getNextPageParam: (last, pages) => (last.products.length < PAGE ? undefined : pages.length * PAGE),
    refetchInterval: (query) => {
      const first = query.state.data?.pages[0];
      const scanPending = first && (first.scan.running || (first.scanRequest.at && (!first.scan.at || first.scanRequest.at > first.scan.at)));
      return first?.approveJob?.running || scanPending ? 5_000 : false;
    },
  });
  const first = list.data?.pages[0];
  const counts = first?.counts;
  const products = useMemo(
    () => (list.data?.pages.flatMap((p) => p.products) || [])
      .map((p) => ({ ...p, suggestions: p.suggestions.filter((s) => !hidden.has(s.id)) }))
      .filter((p) => p.suggestions.length),
    [list.data, hidden],
  );
  const scanPending = Boolean(first && (first.scan.running || (first.scanRequest.at && (!first.scan.at || first.scanRequest.at > first.scan.at))));
  const job = first?.approveJob;

  const refreshAll = () => void queryClient.invalidateQueries({ queryKey: ["supplier-match"] });
  const mark = (ids: string[], on: boolean) => setBusy((prev) => {
    const next = new Set(prev);
    for (const id of ids) if (on) next.add(id); else next.delete(id);
    return next;
  });
  const hide = (ids: string[]) => setHidden((prev) => new Set([...prev, ...ids]));

  const scan = useMutation({
    mutationFn: () => anyJson("/api/supplier-match/scan", { method: "POST" }),
    onSuccess: () => { toast.success("Проверка запущена — займёт пару минут"); refreshAll(); },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const approve = useMutation({
    mutationFn: (ids: string[]) => anyJson<{ linked: number; failed: number }>("/api/supplier-match/approve", mutationBody({ ids })),
    onMutate: (ids) => mark(ids, true),
    onSuccess: (r, ids) => {
      if (r.failed) toast.error(`Не удалось привязать: ${r.failed}`);
      else toast.success(ids.length > 1 ? `Привязано строк: ${r.linked}` : "Привязано — цена и остаток уйдут на маркетплейсы");
      hide(ids);
    },
    onError: (e) => toast.error(errorMessage(e)),
    onSettled: (_r, _e, ids) => mark(ids, false),
  });
  const reject = useMutation({
    mutationFn: (ids: string[]) => anyJson("/api/supplier-match/reject", mutationBody({ ids })),
    onMutate: (ids) => { mark(ids, true); hide(ids); },
    onError: (e, ids) => { toast.error(errorMessage(e)); setHidden((prev) => { const n = new Set(prev); ids.forEach((id) => n.delete(id)); return n; }); },
    onSettled: (_r, _e, ids) => mark(ids, false),
  });
  const approveAll = useMutation({
    mutationFn: () => anyJson<{ queued: number }>("/api/supplier-match/approve", mutationBody({ allExact: true, tab, only })),
    onSuccess: (r) => { toast.success(`Привязываем надёжные: ${r.queued}`); refreshAll(); },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const exactCount = only.length ? counts?.exactSelected || 0 : tab === "launch" ? counts?.launchExact || 0 : counts?.extraExact || 0;
  const tabs: Array<[Tab, string, number | undefined, React.ReactNode]> = [
    ["launch", "Запустить в продажу", counts?.launchProducts, <Rocket size={15} key="r" />],
    ["extra", "Новые привязки", counts?.extraProducts, <Link2 size={15} key="l" />],
  ];

  return (
    <section className="page-section sm2">
      <header className="sm2-head">
        <div>
          <h1>Подбор поставщиков</h1>
          <p>Строки PriceMaster, подходящие вашим товарам по бренду, аромату, объёму и концентрации. Привязывается только то, что вы одобрите.</p>
        </div>
        <div className="sm2-scan">
          <span>{scanPending ? <><Loader2 className="spin" size={13} /> Проверяем PriceMaster…</> : <>Проверено {ago(first?.scan.at)}</>}</span>
          <button className="secondary-action" type="button" disabled={scan.isPending || scanPending} onClick={() => scan.mutate()}>
            <RefreshCw size={15} /> Проверить сейчас
          </button>
        </div>
      </header>
      {first?.scan.status === "error" ? <div className="inline-error">Проверка не удалась: {first.scan.error}</div> : null}

      <nav className="sm2-tabs" role="tablist">
        {tabs.map(([k, label, n, icon]) => (
          <button key={k} type="button" role="tab" aria-selected={tab === k} className={tab === k ? "is-on" : ""} onClick={() => { setTab(k); setHidden(new Set()); }}>
            {icon} {label} {n !== undefined ? <span>{n}</span> : null}
          </button>
        ))}
      </nav>
      <p className="sm2-hint">
        {tab === "launch"
          ? "Товары, которые сейчас не продаются: нет привязки или у всех поставщиков закончился товар. После привязки цена и остаток сразу уйдут на маркетплейсы."
          : "Товары в продаже, для которых нашлись новые поставщики. Больше поставщиков — меньше пропаж из наличия и ниже закупка."}
      </p>

      <div className="sm2-toolbar">
        <label className="sm2-search">
          <Search size={15} />
          <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Товар, артикул или поставщик" />
          {search ? <button type="button" aria-label="Очистить" onClick={() => setSearch("")}><X size={14} /></button> : null}
        </label>
        <SupplierFilter options={first?.suppliers || []} value={only} onChange={(v) => { setOnly(v); setHidden(new Set()); }} />
        <label className="sm2-toggle">
          <input type="checkbox" checked={exactOnly} onChange={(e) => setExactOnly(e.target.checked)} /> Только надёжные
        </label>
        <button className="primary-action" type="button" disabled={!exactCount || approveAll.isPending || Boolean(job?.running)} onClick={() => {
          if (window.confirm(`Привязать все надёжные совпадения на вкладке «${tab === "launch" ? "Запустить в продажу" : "Новые привязки"}» — ${exactCount} строк?`)) approveAll.mutate();
        }}>
          <ShieldCheck size={15} /> Одобрить все надёжные ({exactCount})
        </button>
      </div>

      <ExcludedSuppliers onChange={refreshAll} />

      {job ? (
        <div className={`sm2-job${job.running ? " is-running" : ""}`}>
          {job.running ? <Loader2 className="spin" size={14} /> : <CheckCircle2 size={14} />}
          {job.running ? "Привязываем" : "Готово"}: {job.linked + job.failed} из {job.total}
          {job.failed ? <span className="sm2-bad"> · ошибок {job.failed}</span> : null}
          {job.running ? <span className="sm2-bar"><i style={{ transform: `scaleX(${job.total ? (job.linked + job.failed) / job.total : 0})` }} /></span> : null}
        </div>
      ) : null}

      {list.isError ? <div className="inline-error">{errorMessage(list.error)}</div> : null}
      {list.isLoading ? <div className="sm2-empty"><Loader2 className="spin" size={16} /> Загружаем…</div> : null}
      {!list.isLoading && !products.length ? (
        <div className="sm2-empty">
          <PackageCheck size={22} />
          {first?.scan.at
            ? (q || exactOnly ? "По этому фильтру ничего нет." : tab === "launch" ? "Все товары без поставщика проверены — новых строк в PriceMaster для них нет." : "Новых поставщиков для товаров в продаже пока нет.")
            : "Проверка ещё не запускалась. Нажмите «Проверить сейчас»."}
        </div>
      ) : null}

      <div className="sm2-list">
        {products.map((p) => {
          const [statusText, statusTone] = statusLabel[p.cardStatus] || ["", ""];
          const exactIds = p.suggestions.filter((s) => s.confidence === "exact").map((s) => s.id);
          return (
            <article key={p.offerKey} className="sm2-product">
              <div className="sm2-product-head">
                <div className="sm2-product-title">
                  <b>{p.productName || p.offerKey}</b>
                  <span>
                    {p.offerKey.toUpperCase()}
                    {[...new Set(p.shops.map(shopLabel))].map((s) => <em key={s} className="sm2-shop">{s}</em>)}
                    {statusText ? <em className={`sm2-status is-${statusTone}`}>{statusText}</em> : null}
                    {p.archived ? <em className="sm2-status">в архиве</em> : null}
                    {p.sold30 ? <em className="sm2-sold">продано {p.sold30} за 30 дн.</em> : null}
                  </span>
                </div>
                {exactIds.length > 1 ? (
                  <button className="secondary-action compact" type="button" disabled={approve.isPending} onClick={() => approve.mutate(exactIds)}>
                    <ShieldCheck size={13} /> Привязать надёжные ({exactIds.length})
                  </button>
                ) : null}
              </div>
              <ul className="sm2-rows">
                {p.suggestions.map((s) => (
                  <li key={s.id} className={busy.has(s.id) ? "is-busy" : ""}>
                    <div className="sm2-row-main">
                      <span className="sm2-supplier">{s.row.supplierName || "Поставщик"}</span>
                      <span className="sm2-rowname" title={s.row.name}>{s.row.name}</span>
                      <span className="sm2-meta">
                        арт. {s.row.article || "—"}
                        {s.confidence === "exact" ? <em className="sm2-badge is-exact"><ShieldCheck size={11} /> надёжно</em> : <em className="sm2-badge">проверьте</em>}
                        {s.issues.map((i) => <em key={i} className="sm2-issue">{i}</em>)}
                        {s.status === "failed" ? <em className="sm2-issue is-bad">ошибка: {s.error}</em> : null}
                      </span>
                    </div>
                    <div className="sm2-price">
                      <b>{money(s.row.price, s.row.priceCurrency)}</b>
                      {s.ozonPrice ? <small>≈ {s.ozonPrice.toLocaleString("ru-RU")} ₽ на Ozon</small> : null}
                    </div>
                    <div className="sm2-actions">
                      <button className="primary-action compact" type="button" disabled={busy.has(s.id)} onClick={() => approve.mutate([s.id])}>
                        {busy.has(s.id) ? <Loader2 className="spin" size={13} /> : <Link2 size={13} />} Привязать
                      </button>
                      <button className="secondary-action compact" type="button" disabled={busy.has(s.id)} onClick={() => reject.mutate([s.id])}>Не то</button>
                    </div>
                  </li>
                ))}
              </ul>
            </article>
          );
        })}
      </div>
      {list.hasNextPage ? (
        <button className="secondary-action sm2-more" type="button" disabled={list.isFetchingNextPage} onClick={() => void list.fetchNextPage()}>
          {list.isFetchingNextPage ? <Loader2 className="spin" size={14} /> : null} Показать ещё
        </button>
      ) : null}
    </section>
  );
}

// «Только от поставщиков»: one or several suppliers whose rows are shown and approved
function SupplierFilter({ options, value, onChange }: { options: SupplierOption[]; value: string[]; onChange: (v: string[]) => void }) {
  const [open, setOpen] = useState(false);
  const [find, setFind] = useState("");
  const keyOf = (o: SupplierOption) => o.partnerId || o.name;
  const chosen = new Set(value);
  const names = options.filter((o) => chosen.has(keyOf(o))).map((o) => o.name);
  const label = !value.length ? "Все поставщики" : names.length <= 2 ? names.join(", ") || `${value.length} выбрано` : `${names.slice(0, 2).join(", ")} +${names.length - 2}`;
  const list = options.filter((o) => !find.trim() || o.name.toLowerCase().includes(find.trim().toLowerCase()));
  const toggle = (k: string) => onChange(chosen.has(k) ? value.filter((v) => v !== k) : [...value, k]);
  return (
    <span className="sm2-sf">
      <button type="button" className={`secondary-action${value.length ? " is-on" : ""}`} onClick={() => setOpen((v) => !v)} aria-expanded={open}>
        <Users size={14} /> {label}
      </button>
      {open ? (
        <span className="sm2-sf-pop" onMouseLeave={() => setOpen(false)}>
          <input autoFocus value={find} onChange={(e) => setFind(e.target.value)} placeholder="Найти поставщика" />
          <span className="sm2-sf-list">
            {list.map((o) => (
              <label key={keyOf(o)}>
                <input type="checkbox" checked={chosen.has(keyOf(o))} onChange={() => toggle(keyOf(o))} />
                <span>{o.name || "Без названия"}</span>
                <small>{o.products} тов. · надёжных {o.exact}</small>
              </label>
            ))}
            {!list.length ? <span className="sm2-sf-empty">Никого не нашли</span> : null}
          </span>
          {value.length ? <button type="button" className="secondary-action compact" onClick={() => onChange([])}>Показать всех</button> : null}
        </span>
      ) : null}
    </span>
  );
}
