import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, Info, Loader2, RefreshCw, Search, Wrench, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { errorMessage, useDebounced } from "../lib/common";
import { toast } from "../lib/toast";
import "./card-health.css";
import { ImproveAdd } from "./CardImprovePage";

// «Ошибки карточек»: сервер раз в 2 часа сам проверяет все карточки Маркета, раскладывает ошибки по типам и
// предлагает починку. Чинится по кнопке; тип можно перевести в «чинить само» — тогда новые карточки с такой
// ошибкой починятся при следующей проверке. Ничего не уходит без вашего решения (включённый переключатель —
// это и есть решение для этого типа).

function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

type Issue = {
  id: number; marketplace: string; shop_id: string; offer_id: string; code: string; severity: "error" | "warning";
  message: string; fix: string | null; status: string; name: string | null; content_rating: number | null; error: string | null;
  first_seen: string; last_seen: string; applied_at: string | null;
};
type Rule = { title: string; fix: string; howTo: string };
type HealthResponse = {
  items: Issue[]; groups: Array<{ code: string; n: number; errors: number }>; statuses: Record<string, number>;
  rules: Record<string, Rule>; auto: Record<string, boolean>;
  scan: { at?: string; running?: boolean; issues?: number; fixed?: number; autoApplied?: number; elapsedMs?: number; quarantine?: { confirmed?: number; checked?: number } };
  improve?: Improve;
};
type Improve = {
  enabled: boolean; perScan: number; cards: number; matched: number; building: number; review: number; improved: number;
  rating: { avg: number | null; weak: number; checked: number }; error?: string;
};

const STATUSES: Array<[string, string]> = [["open", "Нужно решить"], ["failed", "Не починилось"], ["applied", "Починка отправлена"], ["fixed", "Исправлено"], ["dismissed", "Скрыто"]];
const FIX_LABEL: Record<string, string> = { resend_card: "Переотправить карточку", reprice: "Отправить цену", confirm_quarantine: "Подтвердить цену", dedupe_variant: "Оставить лучший, дубли в архив" };

export function CardHealthPage() {
  const [status, setStatus] = useState("open");
  const [code, setCode] = useState("");
  const [q, setQ] = useState("");
  const [picked, setPicked] = useState<number[]>([]);
  const query = useDebounced(q, 350).trim();
  const queryClient = useQueryClient();
  const health = useQuery({
    queryKey: ["card-health", status, code, query],
    queryFn: () => apiJson<HealthResponse>(`/api/card-health?status=${status}&code=${encodeURIComponent(code)}&q=${encodeURIComponent(query)}`),
    refetchInterval: (data) => (data?.state.data?.scan?.running ? 5000 : 60_000),
  });
  useEffect(() => setPicked([]), [status, code, query]);
  const refresh = () => queryClient.invalidateQueries({ queryKey: ["card-health"] });

  const apply = useMutation({
    mutationFn: (body: { ids?: number[]; code?: string }) => apiJson<{ applied: number; failed: number; skipped: number }>("/api/card-health/apply", mutationBody(body)),
    onSuccess: (r) => { toast.success(`Починка отправлена: ${r.applied}${r.failed ? `, не получилось: ${r.failed}` : ""}${r.skipped ? `, пропущено: ${r.skipped}` : ""}`); setPicked([]); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const dismiss = useMutation({
    mutationFn: (body: { ids: number[]; restore?: boolean }) => apiJson("/api/card-health/dismiss", mutationBody(body)),
    onSuccess: () => { setPicked([]); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const auto = useMutation({
    mutationFn: (body: { code: string; enabled: boolean }) => apiJson("/api/card-health/auto", mutationBody(body)),
    onSuccess: () => refresh(),
    onError: (error) => toast.error(errorMessage(error)),
  });
  const scan = useMutation({
    mutationFn: () => apiJson("/api/card-health/scan", mutationBody({})),
    onSuccess: () => { toast.info("Проверка запущена — займёт несколько минут"); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });

  const improve = useMutation({
    mutationFn: (body: { enabled?: boolean; perScan?: number; queueNow?: number }) => apiJson<{ queued: { queued: number } | null }>("/api/card-health/improve", mutationBody(body)),
    onSuccess: (r) => { if (r.queued) toast.success(r.queued.queued ? `В конвейер добавлено: ${r.queued.queued}` : "Новых карточек для улучшения нет"); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });

  const data = health.data;
  const rules = data?.rules || {};
  const items = data?.items || [];
  const running = Boolean(data?.scan?.running);
  const fixable = items.filter((i) => i.fix);

  return (
    <div className="page-shell ch-page">
      <PageHeader
        title="Ошибки карточек"
        subtitle="Склад сам проверяет все карточки Маркета раз в 2 часа: что не так, как починить — и починка по кнопке. Включите «чинить само» для типа ошибки — тогда новые карточки с ней будут чиниться без вас."
        action={
          <button className="secondary-action" type="button" disabled={running || scan.isPending} onClick={() => scan.mutate()}>
            {running ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />} {running ? "Проверяем…" : "Проверить сейчас"}
          </button>
        }
      />
      <div className="ch-scan">
        {data?.scan?.at ? <>Последняя проверка {new Date(data.scan.at).toLocaleString("ru")}: ошибок {data.scan.issues ?? 0}, исправилось {data.scan.fixed ?? 0}{data.scan.autoApplied ? `, починено само ${data.scan.autoApplied}` : ""}{data.scan.quarantine?.confirmed ? `, из карантина цен выпущено ${data.scan.quarantine.confirmed}` : ""}.</> : "Первая проверка запустится в течение 10 минут после запуска сервера."}
        {running ? <div className="fr-progress-bar is-indeterminate ch-bar"><span /></div> : null}
      </div>

      {data?.improve && !data.improve.error ? (
        // свёрнуто в одну строку: подробности и настройки — по клику, работа — на странице «Улучшение карточек»
        <details className="ch-improve-fold">
          <summary>
            <span className="ch-improve-fold-title">Улучшение карточек</span>
            <span>сопоставлено <b>{data.improve.matched.toLocaleString("ru")}</b></span>
            <span>в конвейере <b>{data.improve.building + data.improve.review}</b>{data.improve.review ? <> · ждут проверки <b>{data.improve.review}</b></> : null}</span>
            <span>улучшено <b>{data.improve.improved}</b></span>
            <span>рейтинг Маркета <b>{data.improve.rating.avg ?? "—"}</b></span>
            <a className="secondary-action compact" href="/app/card-improve" onClick={(e) => e.stopPropagation()}>Открыть</a>
          </summary>
        <section className="ch-improve" aria-label="Улучшение карточек">
          <div className="ch-improve-text">
            <p>
              Старые карточки сопоставляются с ароматом и попадают в конвейер как черновики: новые фото, описание и название.
              Артикул, цена и штрихкод не меняются. Первыми идут самые продаваемые. На маркетплейс уходит только то, что вы одобрите.
            </p>
          </div>
          <div className="ch-improve-stats">
            <div><b>{data.improve.cards.toLocaleString("ru")}</b><span>карточек на складе</span></div>
            <div><b>{data.improve.matched.toLocaleString("ru")}</b><span>товаров сопоставлено с ароматом</span></div>
            <div><b>{data.improve.building + data.improve.review}</b><span>в конвейере{data.improve.review ? `, ждут проверки ${data.improve.review}` : ""}</span></div>
            <div><b>{data.improve.improved}</b><span>улучшено</span></div>
            <div><b>{data.improve.rating.avg ?? "—"}</b><span>средний рейтинг Маркета{data.improve.rating.weak ? `, ниже 70: ${data.improve.rating.weak}` : ""}</span></div>
          </div>
          <div className="ch-improve-actions">
            <label className="ch-switch">
              <input type="checkbox" checked={data.improve.enabled} disabled={improve.isPending} onChange={(e) => improve.mutate({ enabled: e.target.checked })} />
              <span>Добавлять самые продаваемые товары после каждой проверки ({data.improve.perScan} шт.)</span>
            </label>
            <ImproveAdd compact />
            <a className="primary-action compact" href="/app/card-improve">Открыть «Улучшение карточек»</a>
          </div>
        </section>
        </details>
      ) : null}

      <div className="ch-tabs" role="tablist">
        {STATUSES.map(([key, label]) => (
          <button key={key} type="button" role="tab" aria-selected={status === key} className={status === key ? "is-on" : ""} onClick={() => setStatus(key)}>
            {label}{data?.statuses?.[key] ? <b>{data.statuses?.[key]}</b> : null}
          </button>
        ))}
      </div>

      <div className="ch-groups">
        <button type="button" className={`ch-group${!code ? " is-on" : ""}`} onClick={() => setCode("")}>
          <span className="ch-group-title">Все типы</span>
          <span className="ch-group-n">{(data?.groups || []).reduce((s, g) => s + g.n, 0)}</span>
        </button>
        {(data?.groups || []).map((g) => {
          const rule = rules[g.code] || { title: g.code, fix: "", howTo: "" };
          return (
            <div key={g.code} className={`ch-group${code === g.code ? " is-on" : ""}`}>
              <button type="button" className="ch-group-pick" onClick={() => setCode(g.code === code ? "" : g.code)}>
                <span className="ch-group-title">{g.errors ? <AlertTriangle size={13} className="is-error" /> : <Info size={13} />} {rule.title}</span>
                <span className="ch-group-n">{g.n}</span>
              </button>
              <p className="ch-group-how">{rule.howTo}</p>
              {rule.fix && status !== "fixed" && status !== "dismissed" ? (
                <div className="ch-group-actions">
                  <label className="ch-auto" title="Новые карточки с этой ошибкой будут чиниться при каждой проверке">
                    <input type="checkbox" checked={Boolean(data?.auto[g.code])} onChange={(e) => auto.mutate({ code: g.code, enabled: e.target.checked })} /> чинить само
                  </label>
                  <button className="secondary-action compact" type="button" disabled={apply.isPending} onClick={() => apply.mutate({ code: g.code })}>
                    <Wrench size={12} /> {FIX_LABEL[rule.fix] || "Починить"} — все {g.n}
                  </button>
                </div>
              ) : !rule.fix ? <span className="ch-manual">чинится вручную</span> : null}
            </div>
          );
        })}
      </div>

      <div className="ch-toolbar">
        <label className="fr-search ch-search">
          <Search size={15} />
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Артикул или название" />
        </label>
        {fixable.length ? (
          <label className="ch-pickall"><input type="checkbox" checked={picked.length > 0 && picked.length === fixable.length} onChange={() => setPicked(picked.length === fixable.length ? [] : fixable.map((i) => i.id))} /> выбрать с починкой ({fixable.length})</label>
        ) : null}
        {picked.length ? (
          <>
            <button className="secondary-action" type="button" onClick={() => dismiss.mutate({ ids: picked })}><X size={14} /> Скрыть ({picked.length})</button>
            <button className="primary-action" type="button" disabled={apply.isPending} onClick={() => apply.mutate({ ids: picked })}>
              {apply.isPending ? <Loader2 size={14} className="spin" /> : <Wrench size={14} />} Починить выбранные ({picked.length})
            </button>
          </>
        ) : null}
      </div>

      {health.isLoading ? <div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем…</div> : null}
      {health.isError ? <div className="inline-error">{errorMessage(health.error)}</div> : null}
      {!health.isLoading && !items.length ? <div className="ch-empty"><Check size={18} /> Здесь пусто.</div> : null}

      <div className="ch-list">
        {items.map((i) => {
          const rule = rules[i.code] || { title: i.code, fix: "", howTo: "" };
          return (
            <div key={i.id} className={`ch-row is-${i.severity}${picked.includes(i.id) ? " is-picked" : ""}`}>
              {i.fix && (status === "open" || status === "failed")
                ? <input type="checkbox" checked={picked.includes(i.id)} onChange={() => setPicked((p) => (p.includes(i.id) ? p.filter((x) => x !== i.id) : [...p, i.id]))} aria-label={`Выбрать ${i.offer_id}`} />
                : <span />}
              <div className="ch-main">
                <div className="ch-name">{i.name || i.offer_id}</div>
                <div className="ch-meta"><span className="ch-mp">Маркет</span> {i.offer_id} · {rule.title}{i.content_rating ? ` · рейтинг ${i.content_rating}` : ""}</div>
                <div className="ch-msg">{i.message}</div>
                {i.error ? <div className="ch-err">{i.error}</div> : null}
              </div>
              <div className="ch-actions">
                {i.fix && (status === "open" || status === "failed") ? (
                  <button className="secondary-action compact" type="button" disabled={apply.isPending} onClick={() => apply.mutate({ ids: [i.id] })}><Wrench size={12} /> {FIX_LABEL[i.fix] || "Починить"}</button>
                ) : null}
                {status === "dismissed"
                  ? <button className="icon-action" type="button" title="Вернуть" onClick={() => dismiss.mutate({ ids: [i.id], restore: true })}><RefreshCw size={14} /></button>
                  : status === "open" || status === "failed" ? <button className="icon-action" type="button" title="Скрыть" onClick={() => dismiss.mutate({ ids: [i.id] })}><X size={14} /></button> : null}
              </div>
            </div>
          );
        })}
      </div>
      {items.length >= 500 ? <div className="ch-more">Показаны первые 500 — выберите тип ошибки или найдите по артикулу.</div> : null}
    </div>
  );
}
