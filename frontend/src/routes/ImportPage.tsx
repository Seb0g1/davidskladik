import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckSquare, Download, Loader2, RefreshCw, Search, Square, Upload } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

type Candidate = {
  id: string;
  offerId: string;
  name: string;
  vendor?: string;
  imageUrl?: string;
  eligible: boolean;
  existsOnYandex: boolean;
  yandexReady: boolean;
  blockReasons: string[];
  missing?: string[];
};

type CandidatesResponse = { ok: boolean; page: number; pageSize: number; total: number; scanCapped?: boolean; items: Candidate[]; };

async function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...(init || {}),
    headers: { ...(init?.body ? { "Content-Type": "application/json" } : {}), ...(init?.headers || {}) },
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error((data as any)?.error || `HTTP ${response.status}`);
  return data as T;
}

function statusLabel(item: Candidate): { text: string; tone: string } {
  if (item.existsOnYandex) return { text: "уже на Яндексе", tone: "muted" };
  if (item.eligible) return { text: "готов к импорту", tone: "ok" };
  if (item.blockReasons.length) return { text: item.blockReasons[0], tone: "warn" };
  if (!item.yandexReady) {
    const detail = item.missing?.length ? `: нет ${item.missing.join(", ")}` : "";
    return { text: `нет данных для карточки${detail}`, tone: "warn" };
  }
  return { text: "—", tone: "muted" };
}

export function ImportPage() {
  const queryClient = useQueryClient();
  const [query, setQuery] = useState("");
  const [debounced, setDebounced] = useState("");
  const [page, setPage] = useState(1);
  const [onlyEligible, setOnlyEligible] = useState(true);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const prevRefreshRunning = useRef(false);

  useEffect(() => {
    const timer = window.setTimeout(() => { setDebounced(query.trim()); setPage(1); }, 400);
    return () => window.clearTimeout(timer);
  }, [query]);

  const candidatesQuery = useQuery({
    queryKey: ["import-candidates", debounced, page, onlyEligible],
    queryFn: () => apiJson<CandidatesResponse>(
      `/api/ozon-yandex-import/candidates?q=${encodeURIComponent(debounced)}&page=${page}&pageSize=40&onlyEligible=${onlyEligible}`,
    ),
  });
  const refreshStatus = useQuery({
    queryKey: ["import-refresh-status"],
    queryFn: () => apiJson<{ running: boolean; lastResult?: { at?: string; imported?: number; error?: string } }>("/api/ozon-yandex-import/refresh/status"),
    refetchInterval: (q) => (q.state.data?.running ? 4000 : false),
  });

  const refresh = useMutation({
    mutationFn: () => apiJson("/api/ozon-yandex-import/refresh", { method: "POST", body: JSON.stringify({}) }),
    onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ["import-refresh-status"] }); },
  });
  const sendSelected = useMutation({
    mutationFn: () => apiJson<{ sent: number; failed: number; skipped: unknown[] }>("/api/ozon-yandex-import/send-selected", {
      method: "POST",
      body: JSON.stringify({ ids: Array.from(selected) }),
    }),
    onSuccess: () => {
      setSelected(new Set());
      void queryClient.invalidateQueries({ queryKey: ["import-candidates"] });
    },
  });

  const items = candidatesQuery.data?.items || [];
  const total = candidatesQuery.data?.total || 0;
  const eligibleOnPage = useMemo(() => items.filter((item) => item.eligible && !item.existsOnYandex), [items]);
  const allPageSelected = eligibleOnPage.length > 0 && eligibleOnPage.every((item) => selected.has(item.id));

  const toggle = (id: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };
  const togglePage = () => {
    setSelected((current) => {
      const next = new Set(current);
      if (allPageSelected) eligibleOnPage.forEach((item) => next.delete(item.id));
      else eligibleOnPage.forEach((item) => next.add(item.id));
      return next;
    });
  };

  const isRunning = Boolean(refreshStatus.data?.running);
  useEffect(() => {
    if (prevRefreshRunning.current && !isRunning) {
      void queryClient.invalidateQueries({ queryKey: ["import-candidates"] });
    }
    prevRefreshRunning.current = isRunning;
  }, [isRunning, queryClient]);

  const refreshing = refresh.isPending || isRunning;

  return (
    <section className="page-section import-page">
      <PageHeader
        title="Импорт на Яндекс"
        subtitle="Перенос товаров с Ozon на Яндекс.Маркет: обнови список, найди по артикулу, выбери и импортируй."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" disabled={refreshing} onClick={() => refresh.mutate()}>
              {refreshing ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить с Ozon
            </button>
            <button className="primary-action" type="button" disabled={!selected.size || sendSelected.isPending} onClick={() => sendSelected.mutate()}>
              {sendSelected.isPending ? <Loader2 className="spin" size={16} /> : <Upload size={16} />} Импортировать выбранные ({selected.size})
            </button>
          </div>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label={onlyEligible ? "Готовы к импорту" : "Всего товаров Ozon"} value={total} tone="accent" icon={<Download size={18} />} />
        <Stat label="Выбрано" value={selected.size} tone={selected.size ? "warn" : "success"} icon={<CheckSquare size={18} />} />
      </section>

      {candidatesQuery.data?.scanCapped ? (
        <div className="info-strip warn compact">
          Показаны первые 50 000 товаров. Используй поиск, чтобы найти конкретный артикул.
        </div>
      ) : null}
      {refreshStatus.data?.lastResult?.at ? (
        <div className="info-strip compact">
          Последнее обновление каталога Ozon: {new Date(refreshStatus.data.lastResult.at).toLocaleString("ru-RU")}
          {typeof refreshStatus.data.lastResult.imported === "number" ? ` · ${refreshStatus.data.lastResult.imported} карточек` : ""}
          {refreshStatus.data.lastResult.error ? ` · ошибка: ${refreshStatus.data.lastResult.error}` : ""}
        </div>
      ) : null}

      <div className="filters-row">
        <label className="search-box">
          <Search size={16} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Поиск по артикулу или названию" />
        </label>
        <label className="settings-toggle">
          <input type="checkbox" checked={onlyEligible} onChange={(event) => { setOnlyEligible(event.target.checked); setPage(1); }} />
          Только готовые к импорту
        </label>
      </div>

      {sendSelected.data ? (
        <div className={`info-strip ${sendSelected.data.sent ? "success" : "warn"}`}>
          <div>Импортировано: {sendSelected.data.sent}{sendSelected.data.failed ? ` · ошибок: ${sendSelected.data.failed}` : ""}{sendSelected.data.skipped?.length ? ` · пропущено: ${sendSelected.data.skipped.length}` : ""}</div>
          {sendSelected.data.skipped?.length ? (
            <ul className="import-skipped">
              {(sendSelected.data.skipped as Array<{ offerId?: string; reasons?: string[] }>).slice(0, 20).map((row, index) => (
                <li key={row.offerId || index}><b>{row.offerId || "—"}</b>: {(row.reasons || []).join("; ") || "не готов к выгрузке"}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
      {sendSelected.error ? <div className="inline-error">{String((sendSelected.error as Error).message)}</div> : null}
      {candidatesQuery.error ? <div className="inline-error">{String((candidatesQuery.error as Error).message)}</div> : null}

      <div className="table-panel import-table">
        <div className="table-head import-row">
          <span>
            <button className="checkbox-btn" type="button" onClick={togglePage} title="Выбрать все на странице" disabled={!eligibleOnPage.length}>
              {allPageSelected ? <CheckSquare size={16} /> : <Square size={16} />}
            </button>
          </span>
          <span>Товар</span>
          <span>Артикул</span>
          <span>Бренд</span>
          <span>Статус</span>
        </div>
        {candidatesQuery.isFetching ? <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю…</div> : null}
        {items.map((item) => {
          const status = statusLabel(item);
          const selectable = item.eligible && !item.existsOnYandex;
          return (
            <div className={`table-row import-row${selected.has(item.id) ? " is-selected" : ""}`} key={item.id}>
              <span>
                <button className="checkbox-btn" type="button" disabled={!selectable} onClick={() => toggle(item.id)}>
                  {selected.has(item.id) ? <CheckSquare size={16} /> : <Square size={16} />}
                </button>
              </span>
              <span className="import-product">
                {item.imageUrl ? <img src={item.imageUrl} alt="" loading="lazy" /> : <span className="import-noimg" />}
                <span className="import-name">{item.name || item.offerId}</span>
              </span>
              <span data-label="Артикул">{item.offerId}</span>
              <span data-label="Бренд">{item.vendor || "—"}</span>
              <span data-label="Статус"><span className={`pill ${status.tone}`}>{status.text}</span></span>
            </div>
          );
        })}
        {!items.length && !candidatesQuery.isFetching ? <div className="empty-state">Товаров по фильтру нет. Нажми «Обновить с Ozon», если добавил новые.</div> : null}
      </div>

      <div className="pager">
        <button className="secondary-action" type="button" disabled={page <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>Назад</button>
        <span>Стр. {page} · {total} тов.</span>
        <button className="secondary-action" type="button" disabled={page * 40 >= total} onClick={() => setPage((value) => value + 1)}>Дальше</button>
      </div>
    </section>
  );
}
