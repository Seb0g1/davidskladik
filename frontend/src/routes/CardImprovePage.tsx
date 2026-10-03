import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, CheckCheck, ChevronDown, ChevronUp, EyeOff, Loader2, Plus, RefreshCw, Search, Undo2 } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { errorMessage, useDebounced } from "../lib/common";
import { toast } from "../lib/toast";
import { OwnPhotos } from "./FragranticaConveyor";
import "./fragrantica.css";
import "./card-health.css";
import "./card-improve.css";

// «Улучшение карточек»: старые карточки, сопоставленные с ароматом Фрагрантики, собраны заново (фото,
// пирамида, описание, название). Здесь видно «было → станет»; уходит на маркетплейс только то, что одобрено.
// Артикул, цена и штрихкод остаются прежними, на Маркет отправляется только контент.

function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

type Side = { name: string; photos: string[]; description: string };
type Item = {
  id: number; status: string; stage: string | null; error: string | null; perfumeId: number; brand: string; perfumeName: string;
  volume: number | null; tester: boolean; shop: string; marketplace: string; offerId: string; rating: number | null; price: number;
  yandexPrice: number; sold: number;
  before: Side | null; after: Side; missing: string[]; exports: Array<{ status: string; error: string | null; shop: string }>;
  customPhotos?: string[]; onlyCustomPhotos?: boolean; keptExistingPhotos?: number;
};
type ImproveResponse = { tab: string; counts: Record<string, number>; items: Item[] };

const TABS: Array<[string, string]> = [
  ["review", "На проверке"],
  ["attention", "Нужно поправить"],
  ["queue", "Собираются"],
  ["sending", "Отправляются"],
  ["sent", "Отправлено"],
  ["skipped", "Не улучшать"],
];

export function CardImprovePage() {
  const [tab, setTab] = useState("review");
  const [q, setQ] = useState("");
  const [picked, setPicked] = useState<number[]>([]);
  const query = useDebounced(q, 350).trim();
  const queryClient = useQueryClient();
  const list = useQuery({
    queryKey: ["card-improve", tab, query],
    queryFn: () => apiJson<ImproveResponse>(`/api/card-improve?tab=${tab}&q=${encodeURIComponent(query)}`),
    refetchInterval: tab === "queue" || tab === "sending" ? 8000 : 30_000,
  });
  useEffect(() => setPicked([]), [tab, query]);
  const refresh = () => queryClient.invalidateQueries({ queryKey: ["card-improve"] });

  const approve = useMutation({
    mutationFn: (ids: number[]) => apiJson<{ approved: number }>("/api/fragrantica/drafts/approve", mutationBody({ ids })),
    onSuccess: (r) => { toast.success(`Одобрено: ${r.approved} — отправляем`); setPicked([]); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const skip = useMutation({
    mutationFn: (body: { ids: number[]; restore?: boolean }) => apiJson<{ count: number }>("/api/card-improve/skip", mutationBody(body)),
    onSuccess: (r, body) => { toast.info(body.restore ? `Вернули в работу: ${r.count}` : `Не улучшаем: ${r.count}`); setPicked([]); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const rebuild = useMutation({
    mutationFn: (id: number) => apiJson(`/api/fragrantica/drafts/${id}/rebuild`, mutationBody({})),
    onSuccess: () => { toast.info("Пересобираем"); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });

  const items = list.data?.items || [];
  const counts = list.data?.counts || {};
  const approvable = items.filter((i) => i.status === "ready" || i.status === "failed");
  const toggle = (id: number) => setPicked((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  const allPicked = approvable.length > 0 && approvable.every((i) => picked.includes(i.id));

  return (
    <div className="page-shell ci-page">
      <PageHeader
        title="Улучшение карточек"
        subtitle="Старые карточки, собранные заново по Фрагрантике: новые фото, пирамида аромата, описание и название. Сравните «было» и «станет» и одобрите — уйдёт только одобренное. Артикул, цена и штрихкод не меняются."
        action={
          <button className="secondary-action" type="button" onClick={refresh} disabled={list.isFetching}>
            {list.isFetching ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />} Обновить
          </button>
        }
      />

      <div className="ch-tabs" role="tablist">
        {TABS.map(([key, label]) => (
          <button key={key} type="button" role="tab" aria-selected={tab === key} className={tab === key ? "is-on" : ""} onClick={() => setTab(key)}>
            {label}{counts[key] ? <b>{counts[key]}</b> : null}
          </button>
        ))}
      </div>

      <ImproveAdd />

      <div className="ci-toolbar">
        <label className="ci-search">
          <Search size={14} />
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Бренд, аромат или артикул" />
        </label>
        {approvable.length ? (
          <>
            <label className="ci-pickall">
              <input type="checkbox" checked={allPicked} onChange={() => setPicked(allPicked ? [] : approvable.map((i) => i.id))} /> выбрать все ({approvable.length})
            </label>
            <button className="primary-action" type="button" disabled={!picked.length || approve.isPending} onClick={() => approve.mutate(picked)}>
              {approve.isPending ? <Loader2 size={14} className="spin" /> : <CheckCheck size={14} />} Одобрить выбранные{picked.length ? ` (${picked.length})` : ""}
            </button>
            <button className="secondary-action" type="button" disabled={!picked.length || skip.isPending} onClick={() => skip.mutate({ ids: picked })}>
              <EyeOff size={14} /> Не улучшать
            </button>
          </>
        ) : null}
      </div>

      {list.isLoading ? <div className="ci-empty"><Loader2 size={16} className="spin" /> Загружаем…</div> : null}
      {list.isError ? <div className="inline-error">{errorMessage(list.error)}</div> : null}
      {list.data && !items.length ? (
        <div className="ci-empty">
          {tab === "review"
            ? "Пока нечего проверять. Впишите выше, сколько товаров улучшить, — сначала берутся самые продаваемые; сборка займёт несколько минут."
            : "Здесь пусто."}
        </div>
      ) : null}

      <div className="ci-list">
        {items.map((item) => (
          <ImproveCard
            key={item.id}
            item={item}
            picked={picked.includes(item.id)}
            onPick={() => toggle(item.id)}
            onApprove={() => approve.mutate([item.id])}
            onSkip={() => skip.mutate({ ids: [item.id] })}
            onRestore={() => skip.mutate({ ids: [item.id], restore: true })}
            onRebuild={() => rebuild.mutate(item.id)}
            pending={approve.isPending || skip.isPending || rebuild.isPending}
          />
        ))}
      </div>
    </div>
  );
}

function ImproveCard({ item, picked, onPick, onApprove, onSkip, onRestore, onRebuild, pending }: {
  item: Item; picked: boolean; onPick: () => void; onApprove: () => void; onSkip: () => void; onRestore: () => void; onRebuild: () => void; pending: boolean;
}) {
  const [showText, setShowText] = useState(false);
  const canApprove = item.status === "ready" || item.status === "failed";
  const editable = ["ready", "attention", "failed"].includes(item.status);
  const busy = item.status === "queued" || item.status === "working" || item.status === "approved" || item.status === "sending";
  return (
    <article className={`ci-card is-${item.status}`}>
      <header className="ci-head">
        {canApprove ? <input type="checkbox" checked={picked} onChange={onPick} aria-label="Выбрать" /> : null}
        <div className="ci-title">
          <b>{item.brand} {item.perfumeName}</b>
          <span>
            {item.volume ? `${item.volume} мл` : ""}{item.tester ? " · тестер" : ""} · {item.offerId} · {item.shop}
            {item.price ? ` · Ozon ${item.price.toLocaleString("ru")} ₽` : ""}{item.yandexPrice ? ` · Маркет ${item.yandexPrice.toLocaleString("ru")} ₽` : ""}
          </span>
        </div>
        <span className={`ci-sold${item.sold ? "" : " is-none"}`} title="Продано штук за 90 дней во всех магазинах">{item.sold ? `продано ${item.sold}` : "без продаж"}</span>
        {item.rating != null ? <span className={`ci-rating${item.rating < 70 ? " is-low" : ""}`} title="Рейтинг карточки на Маркете сейчас">рейтинг {item.rating}</span> : null}
        <div className="ci-actions">
          {busy ? <span className="ci-busy"><Loader2 size={13} className="spin" /> {item.status === "approved" || item.status === "sending" ? "отправляем" : "собираем"}</span> : null}
          {canApprove ? <button className="primary-action compact" type="button" disabled={pending} onClick={onApprove}><Check size={13} /> Одобрить</button> : null}
          {item.status === "attention" || item.status === "failed" ? <button className="secondary-action compact" type="button" disabled={pending} onClick={onRebuild}><RefreshCw size={13} /> Пересобрать</button> : null}
          {editable ? <button className="secondary-action compact" type="button" disabled={pending} onClick={onSkip}><EyeOff size={13} /> Не улучшать</button> : null}
          {item.status === "skipped" ? <button className="secondary-action compact" type="button" disabled={pending} onClick={onRestore}><Undo2 size={13} /> Вернуть</button> : null}
        </div>
      </header>

      {item.error || item.missing.length ? (
        <div className="ci-problem"><AlertTriangle size={13} /> {item.missing.length ? `Не хватает: ${item.missing.join(", ")}` : item.error}</div>
      ) : null}
      {item.exports.length ? (
        <div className="ci-exports">
          {item.exports.map((e, i) => (
            <span key={i} className={`ci-export is-${e.status}`}>{e.shop}: {e.status === "imported" ? "обновлено" : e.status === "failed" ? `ошибка — ${e.error || ""}` : "отправляется"}</span>
          ))}
        </div>
      ) : null}

      <div className="ci-compare">
        <SideView label="Было" side={item.before} showText={showText} />
        <SideView label="Станет" side={item.after} showText={showText} fresh />
      </div>
      <div className="ci-foot">
        <button type="button" className="fr-link-button" onClick={() => setShowText((v) => !v)}>
          {showText ? <ChevronUp size={13} /> : <ChevronDown size={13} />} {showText ? "Скрыть описания" : "Сравнить описания"}
        </button>
        {item.keptExistingPhotos ? <span className="ci-muted">Фото старой карточки сохранены: {item.keptExistingPhotos} (рекламные «фото в конце» parfumdeclaration добавит заново). Порядок и состав — в «Свои фото».</span> : null}
        {editable ? <OwnPhotos draftId={item.id} photos={item.customPhotos || []} only={Boolean(item.onlyCustomPhotos)} editable /> : null}
      </div>
    </article>
  );
}

function SideView({ label, side, showText, fresh = false }: { label: string; side: Side | null; showText: boolean; fresh?: boolean }) {
  return (
    <section className={`ci-side${fresh ? " is-new" : ""}`}>
      <h3>{label}</h3>
      {!side ? <p className="ci-muted">Нет данных о текущей карточке — появятся после пересборки.</p> : (
        <>
          <p className="ci-name">{side.name || <span className="ci-muted">без названия</span>}</p>
          <div className="ci-photos">
            {side.photos.length ? side.photos.slice(0, 12).map((url, i) => (
              <a key={`${url}-${i}`} href={url} target="_blank" rel="noreferrer"><img src={url} alt={`${label}: фото ${i + 1}`} loading="lazy" /></a>
            )) : <span className="ci-muted">нет фото</span>}
          </div>
          <span className="ci-count">{side.photos.length} фото{side.description ? ` · описание ${side.description.length} зн.` : " · без описания"}</span>
          {showText ? <div className="ci-text">{side.description || <span className="ci-muted">описания нет</span>}</div> : null}
        </>
      )}
    </section>
  );
}

/**
 * «Сколько товаров улучшить»: один товар = один артикул сразу во всех магазинах (Ozon и Маркет вместе).
 * Берутся самые продаваемые за 90 дней, потом — с худшим рейтингом Маркета.
 */
export function ImproveAdd({ compact = false }: { compact?: boolean }) {
  const queryClient = useQueryClient();
  const [count, setCount] = useState("100");
  const add = useMutation({
    mutationFn: (queueNow: number) => apiJson<{ queued: { queued: number; withSales?: number } | null }>("/api/card-health/improve", mutationBody({ queueNow })),
    onSuccess: (r) => {
      const n = r.queued?.queued || 0;
      if (n) toast.success(`Добавлено товаров: ${n}${r.queued?.withSales ? ` (с продажами: ${r.queued.withSales})` : ""} — собираем`);
      else toast.info("Новых товаров для улучшения нет");
      queryClient.invalidateQueries({ queryKey: ["card-improve"] });
      queryClient.invalidateQueries({ queryKey: ["card-health"] });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const n = Math.min(1000, Math.max(0, Math.round(Number(count) || 0)));
  return (
    <form className={`ci-add${compact ? " is-compact" : ""}`} onSubmit={(e) => { e.preventDefault(); if (n) add.mutate(n); }}>
      <label>
        <span>Сколько товаров улучшить</span>
        <input inputMode="numeric" value={count} onChange={(e) => setCount(e.target.value.replace(/\D/g, "").slice(0, 4))} aria-label="Сколько товаров улучшить" />
      </label>
      <button className="primary-action" type="submit" disabled={!n || add.isPending}>
        {add.isPending ? <Loader2 size={14} className="spin" /> : <Plus size={14} />} Добавить {n || ""}
      </button>
      {compact ? null : <small>Один товар — это его карточки сразу в Ozon и на Маркете. Сначала самые продаваемые за 90 дней. До одобрения ничего не отправляется.</small>}
    </form>
  );
}
