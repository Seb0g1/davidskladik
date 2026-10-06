import { type ReactNode, useMemo, useState } from "react";
import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, CheckCircle2, Clock3, ExternalLink, FileCheck2, Loader2, RefreshCw, Search, XCircle } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { errorMessage, useDebounced } from "../lib/common";
import { toast } from "../lib/toast";
import "./ozon-docs.css";

// «Документы Ozon»: товары, привязанные к декларациям и сертификатам, и что с ними — на проверке,
// одобрено, отклонено (с причиной). Сервер собирает статусы с Ozon каждые 30 минут (02d-ozon-doc-status.js).

function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

type DocItem = {
  accountId: string; accountName: string; productId: number; certificateId: number; offerId: string | null; sku: number | null;
  name: string | null; image: string | null; certificateNumber: string | null; certificateName: string | null; certificateType: string | null;
  certificateStatus: string | null; certificateReason: string | null; certificateComment: string | null; expireDate: string | null;
  status: "approved" | "declined" | "awaiting_verification" | string; statusLabel: string; reason: string | null;
  ozonStatus: string | null; ozonStatusText: string | null; changedAt: string | null; seenAt: string | null;
};
type DocsResponse = {
  items: DocItem[]; hasMore: boolean; total: number; page: number;
  counts: Array<{ accountId: string; accountName: string; status: string; n: number }>;
  notSelling?: Array<{ accountId: string; n: number }>;
  certificates: Array<{ number: string | null; type: string | null; n: number; declined: number }>;
  accounts: Array<{ id: string; name: string }>;
  sync: { running?: boolean; startedAt?: string; doneAt?: string; lastDoneAt?: string; accounts?: Array<{ account: string; products?: number; error?: string }> };
};

const STATUSES: Array<{ key: string; label: string; icon: ReactNode; tone: string }> = [
  { key: "awaiting_verification", label: "На проверке", icon: <Clock3 size={18} />, tone: "wait" },
  { key: "approved", label: "Одобрено", icon: <CheckCircle2 size={18} />, tone: "ok" },
  { key: "declined", label: "Отклонено", icon: <XCircle size={18} />, tone: "bad" },
];

function formatDate(value?: string | null, withTime = true) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString("ru", withTime ? { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" } : { day: "2-digit", month: "2-digit", year: "numeric" });
}

function plural(n: number, one: string, few: string, many: string) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return one;
  if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
  return many;
}

export function OzonDocsPage() {
  const queryClient = useQueryClient();
  const [account, setAccount] = useState("");
  const [status, setStatus] = useState("");
  const [q, setQ] = useState("");
  const [cert, setCert] = useState("");
  const [sort, setSort] = useState("status");
  const debouncedQ = useDebounced(q, 350);
  const debouncedCert = useDebounced(cert, 350);

  const params = useMemo(() => {
    const p = new URLSearchParams({ sort, limit: "100" });
    if (account) p.set("account", account);
    if (status) p.set("status", status);
    if (debouncedQ.trim()) p.set("q", debouncedQ.trim());
    if (debouncedCert.trim()) p.set("cert", debouncedCert.trim());
    return p.toString();
  }, [account, status, debouncedQ, debouncedCert, sort]);

  const list = useInfiniteQuery({
    queryKey: ["ozon-docs", params],
    queryFn: ({ pageParam }) => apiJson<DocsResponse>(`/api/ozon-docs?${params}&page=${pageParam}`),
    initialPageParam: 1,
    getNextPageParam: (last) => (last.hasMore ? last.page + 1 : undefined),
    refetchInterval: (query) => (query.state.data?.pages[0]?.sync?.running ? 5000 : 60_000),
  });
  const first = list.data?.pages[0];
  const items = list.data?.pages.flatMap((p) => p.items || []) ?? [];
  const sync = first?.sync || {};

  const sum = (s: string) => (first?.counts || []).filter((c) => c.status === s && (!account || c.accountId === account)).reduce((a, c) => a + c.n, 0);
  const all = STATUSES.reduce((a, s) => a + sum(s.key), 0);

  const startSync = useMutation({
    mutationFn: () => apiJson<{ started: boolean; running?: boolean }>("/api/ozon-docs/sync", mutationBody({})),
    onSuccess: (res) => {
      toast.success(res.started ? "Собираем статусы с Ozon — обычно 1–3 минуты." : "Сбор уже идёт.");
      void queryClient.invalidateQueries({ queryKey: ["ozon-docs"] });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });

  return (
    <section className="page-section od-page">
      <PageHeader
        title="Документы Ozon"
        subtitle="Товары, привязанные к декларациям и сертификатам: на проверке, одобрено или отклонено — и почему."
        action={
          <div className="od-head-actions">
            <span className="od-sync">
              {sync.running ? <><Loader2 size={13} className="spin" /> Собираем с Ozon…</> : sync.lastDoneAt || sync.doneAt ? `Обновлено ${formatDate(sync.lastDoneAt || sync.doneAt)}` : "Ещё не собирали"}
            </span>
            <button className="secondary-action" type="button" disabled={startSync.isPending || sync.running} onClick={() => startSync.mutate()}>
              {startSync.isPending || sync.running ? <Loader2 size={15} className="spin" /> : <RefreshCw size={15} />} Обновить с Ozon
            </button>
          </div>
        }
      />

      <div className="od-stats">
        <button type="button" className={`od-stat is-all${status === "" ? " is-on" : ""}`} onClick={() => setStatus("")}>
          <span className="od-stat-icon"><FileCheck2 size={18} /></span>
          <span><b>{all.toLocaleString("ru")}</b><small>Всего с документом</small></span>
        </button>
        {STATUSES.map((s) => (
          <button key={s.key} type="button" className={`od-stat is-${s.tone}${status === s.key ? " is-on" : ""}`} onClick={() => setStatus(status === s.key ? "" : s.key)}>
            <span className="od-stat-icon">{s.icon}</span>
            <span><b>{sum(s.key).toLocaleString("ru")}</b><small>{s.label}</small></span>
          </button>
        ))}
        <button type="button" className={`od-stat is-warn${status === "not_selling" ? " is-on" : ""}`} onClick={() => setStatus(status === "not_selling" ? "" : "not_selling")} title="Документ не отклонён, но карточка не продаётся">
          <span className="od-stat-icon"><AlertTriangle size={18} /></span>
          <span><b>{(first?.notSelling || []).filter((c) => !account || c.accountId === account).reduce((a, c) => a + c.n, 0).toLocaleString("ru")}</b><small>Не продаётся</small></span>
        </button>
      </div>

      <div className="od-filters">
        <label className="od-search">
          <Search size={15} />
          <input placeholder="Название, артикул, SKU или product_id" value={q} onChange={(e) => setQ(e.target.value)} />
        </label>
        <select value={account} onChange={(e) => setAccount(e.target.value)} aria-label="Магазин">
          <option value="">Все магазины</option>
          {(first?.accounts || []).map((a) => <option key={a.id} value={a.id}>{a.name}</option>)}
        </select>
        <select value={status} onChange={(e) => setStatus(e.target.value)} aria-label="Статус">
          <option value="">Любой статус</option>
          {STATUSES.map((s) => <option key={s.key} value={s.key}>{s.label}</option>)}
          <option value="not_selling">Не продаётся (документ не отклонён)</option>
        </select>
        <input className="od-cert" list="od-certs" placeholder="Номер документа" value={cert} onChange={(e) => setCert(e.target.value)} />
        <datalist id="od-certs">
          {(first?.certificates || []).filter((c) => c.number).map((c) => <option key={c.number!} value={c.number!}>{`${c.n} ${plural(c.n, "товар", "товара", "товаров")}${c.declined ? `, отклонено ${c.declined}` : ""}`}</option>)}
        </datalist>
        <select value={sort} onChange={(e) => setSort(e.target.value)} aria-label="Сортировка">
          <option value="status">Сначала отклонённые</option>
          <option value="changed">Недавно изменились</option>
          <option value="name">По названию</option>
        </select>
      </div>

      {(sync.accounts || []).filter((a) => a.error).map((a) => <div key={a.account} className="inline-error">{a.account}: {a.error}</div>)}
      {list.isError ? <div className="inline-error">{errorMessage(list.error)}</div> : null}
      <div className="od-count">{list.isLoading ? "Загрузка…" : `${(first?.total || 0).toLocaleString("ru")} ${plural(first?.total || 0, "товар", "товара", "товаров")}`}</div>

      <div className="od-table" role="table">
        <div className="od-row od-head" role="row">
          <span>Товар</span><span>Документ</span><span>Проверка</span><span>Причина / статус карточки</span>
        </div>
        {items.map((item) => (
          <div key={`${item.accountId}-${item.productId}-${item.certificateId}`} className={`od-row is-${item.status}`} role="row">
            <div className="od-product">
              {item.image ? <img src={item.image} alt="" loading="lazy" /> : <span className="od-noimg" />}
              <div>
                <div className="od-name">{item.name || `product_id ${item.productId}`}</div>
                <div className="od-meta">
                  <b>{item.offerId || "—"}</b>
                  {item.sku ? <> · <a href={`https://www.ozon.ru/product/${item.sku}`} target="_blank" rel="noreferrer">SKU {item.sku} <ExternalLink size={11} /></a></> : null}
                  <span className="od-shop">{item.accountName}</span>
                </div>
              </div>
            </div>
            <div className="od-doc">
              <span className="od-doc-type">{item.certificateType === "declaration" ? "Декларация" : item.certificateType === "certificate_of_conformity" ? "Сертификат" : item.certificateType || "Документ"}</span>
              <b>{item.certificateNumber || "—"}</b>
              <small>
                {item.certificateStatus ? `документ: ${item.certificateStatus}` : ""}
                {item.expireDate ? ` · до ${formatDate(item.expireDate, false)}` : ""}
              </small>
            </div>
            <div className="od-status">
              <span className={`od-pill is-${item.status}`}>
                {item.status === "approved" ? <CheckCircle2 size={13} /> : item.status === "declined" ? <XCircle size={13} /> : <Clock3 size={13} />}
                {item.statusLabel}
              </span>
              {item.changedAt ? <small>с {formatDate(item.changedAt)}</small> : null}
            </div>
            <div className="od-reason">
              {item.reason ? <div className="od-reason-bad">{item.reason}</div> : null}
              {item.ozonStatus ? (
                <div className={`od-ozon${/продается|продаётся/i.test(item.ozonStatus) ? " is-selling" : ""}`}>
                  Карточка: {item.ozonStatus}{item.ozonStatusText ? ` — ${item.ozonStatusText}` : ""}
                </div>
              ) : null}
              {!item.reason && !item.ozonStatus ? <span className="od-muted">—</span> : null}
            </div>
          </div>
        ))}
        {!list.isLoading && !items.length ? (
          <div className="od-empty">{sync.lastDoneAt || sync.doneAt ? "Под эти фильтры товаров нет." : "Статусы ещё не собраны — нажмите «Обновить с Ozon»."}</div>
        ) : null}
      </div>
      {list.hasNextPage ? (
        <div className="od-more">
          <button className="secondary-action" type="button" disabled={list.isFetchingNextPage} onClick={() => list.fetchNextPage()}>
            {list.isFetchingNextPage ? <Loader2 size={14} className="spin" /> : null}Показать ещё
          </button>
        </div>
      ) : null}
    </section>
  );
}
