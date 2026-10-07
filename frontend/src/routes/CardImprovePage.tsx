import { useEffect, useState, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, CheckCheck, ChevronDown, ChevronUp, EyeOff, Loader2, Pencil, Plus, RefreshCw, Search, Undo2 } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { errorMessage, useDebounced } from "../lib/common";
import { toast } from "../lib/toast";
import { OwnPhotos } from "./FragranticaConveyor";
import { PhotoThumb } from "../components/PhotoLightbox";
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
  gender?: string; genderChosen?: boolean;
  customPhotos?: string[]; onlyCustomPhotos?: boolean; keptExistingPhotos?: number; blurryExistingPhotos?: number; ownBottleOnly?: boolean; notes?: string[];
  brandMatched?: boolean; brandCandidates?: Array<{ id: number; value: string }>; retryAt?: string | null;
};
type ImproveResponse = { tab: string; counts: Record<string, number>; items: Item[]; fragranticaPausedUntil?: string | null };

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
  const approveAll = useMutation({
    mutationFn: () => apiJson<{ approved: number }>("/api/card-improve/approve-all", mutationBody({})),
    onSuccess: (r) => { toast.success(`Одобрено: ${r.approved} — отправляем`); setConfirmAll(false); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const [confirmAll, setConfirmAll] = useState(false);
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

      {list.data?.fragranticaPausedUntil ? (
        <div className="ci-pause">
          <AlertTriangle size={14} /> Фрагрантика временно не пускает (защита Cloudflare). Новые ароматы продолжат собираться
          в {new Date(list.data.fragranticaPausedUntil).toLocaleTimeString("ru", { hour: "2-digit", minute: "2-digit" })} —
          пока запросы к ней не идут, чтобы блокировка не продлевалась. Товары с уже скачанными ароматами собираются как обычно.
        </div>
      ) : null}

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
            {tab === "review" && (counts.review || 0) > approvable.length ? (
              confirmAll ? (
                <span className="ci-confirm">
                  Отправить все {counts.review} готовых?
                  <button className="primary-action compact" type="button" disabled={approveAll.isPending} onClick={() => approveAll.mutate()}>
                    {approveAll.isPending ? <Loader2 size={13} className="spin" /> : <CheckCheck size={13} />} Да, одобрить все
                  </button>
                  <button className="secondary-action compact" type="button" onClick={() => setConfirmAll(false)}>Нет</button>
                </span>
              ) : (
                <button className="secondary-action" type="button" onClick={() => setConfirmAll(true)}><CheckCheck size={14} /> Одобрить все готовые ({counts.review})</button>
              )
            ) : null}
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
      {(item.notes || []).map((n) => <div key={n} className="ci-note">{n}</div>)}
      {item.retryAt && (item.status === "queued" || item.status === "working") ? (
        <div className="ci-note">Фрагрантика просит подождать — соберём сами в {new Date(item.retryAt).toLocaleTimeString("ru", { hour: "2-digit", minute: "2-digit" })}</div>
      ) : null}
      {item.brandMatched === false && editable ? <BrandPicker item={item} /> : null}
      {editable || item.status === "sent" ? <GenderPicker item={item} /> : null}
      {/^(Вид меняется|Не выбран вид)/.test(String(item.error || "")) ? <TypePicker item={item} /> : null}
      {/^Ноты подобрал ИИ/.test(String(item.error || "")) ? <NotesConfirm id={item.id} /> : null}
      {/выберите аромат|другая версия аромата|не совпадает с товаром|Show Me Love/i.test(String(item.error || "")) ? <PerfumePicker item={item} /> : null}
      {item.exports.length ? (
        <div className="ci-exports">
          {item.exports.map((e, i) => (
            <span key={i} className={`ci-export is-${e.status}`}>{e.shop}: {e.status === "imported" ? "обновлено" : e.status === "failed" ? `ошибка — ${e.error || ""}` : "отправляется"}</span>
          ))}
        </div>
      ) : null}

      <div className="ci-compare">
        <SideView label="Было" side={item.before} showText={showText} />
        <SideView label="Станет" side={item.after} showText={showText} fresh
          nameEditor={editable ? <NameEditor item={item} /> : null} />
      </div>
      <div className="ci-foot">
        <button type="button" className="fr-link-button" onClick={() => setShowText((v) => !v)}>
          {showText ? <ChevronUp size={13} /> : <ChevronDown size={13} />} {showText ? "Скрыть описания" : "Сравнить описания"}
        </button>
        {item.ownBottleOnly ? <span className="ci-muted">Тестер или объём до 20 мл: фото только с карточки (флакон с Фрагрантики не используется), плюс пирамида и ноты.</span> : null}
        {!item.ownBottleOnly && (item.keptExistingPhotos || item.blurryExistingPhotos) ? (
          <span className="ci-muted">
            {item.keptExistingPhotos ? `Чёткие фото старой карточки сохранены: ${item.keptExistingPhotos} — после нового главного фото. ` : ""}
            {item.blurryExistingPhotos ? `Размытых убрано: ${item.blurryExistingPhotos}. ` : ""}
            Рекламные «фото в конце» parfumdeclaration добавит заново. Порядок и состав — в «Свои фото».
          </span>
        ) : null}
        {editable ? <OwnPhotos draftId={item.id} photos={item.customPhotos || []} only={Boolean(item.onlyCustomPhotos)} editable /> : null}
      </div>
    </article>
  );
}

/** «Станет» name corrected by hand — optional, approving does not wait for it. Kept when the card is rebuilt. */
function NameEditor({ item }: { item: Item }) {
  const queryClient = useQueryClient();
  const current = String(item.after?.name || "");
  const [open, setOpen] = useState(false);
  const [value, setValue] = useState(current);
  const save = useMutation({
    mutationFn: (name: string) => apiJson(`/api/fragrantica/drafts/${item.id}`, {
      method: "PATCH",
      // a Market card shows its own title (marketName), an Ozon card the main name
      body: JSON.stringify(item.marketplace === "yandex" ? { marketName: name } : { name }),
    }),
    onSuccess: () => { toast.success("Название сохранено"); setOpen(false); queryClient.invalidateQueries({ queryKey: ["card-improve"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  if (!open) {
    return (
      <button type="button" className="ci-name-edit" title="Исправить название" aria-label="Исправить название"
        onClick={() => { setValue(current); setOpen(true); }}><Pencil size={13} /></button>
    );
  }
  const v = value.trim();
  return (
    <form className="ci-name-form" onSubmit={(e) => { e.preventDefault(); if (v && v !== current) save.mutate(v); else setOpen(false); }}>
      <textarea value={value} rows={2} maxLength={500} autoFocus onChange={(e) => setValue(e.target.value)} aria-label="Новое название" />
      <div>
        <button className="primary-action compact" type="submit" disabled={save.isPending || !v}>{save.isPending ? <Loader2 size={13} className="spin" /> : <Check size={13} />} Сохранить</button>
        <button className="secondary-action compact" type="button" onClick={() => setOpen(false)}>Отмена</button>
        <small>{v.length} симв.</small>
      </div>
    </form>
  );
}

function SideView({ label, side, showText, fresh = false, nameEditor = null }: { label: string; side: Side | null; showText: boolean; fresh?: boolean; nameEditor?: ReactNode }) {
  return (
    <section className={`ci-side${fresh ? " is-new" : ""}`}>
      <h3>{label}</h3>
      {!side ? <p className="ci-muted">Нет данных о текущей карточке — появятся после пересборки.</p> : (
        <>
          <div className="ci-name">{side.name || <span className="ci-muted">без названия</span>}{nameEditor}</div>
          <div className="ci-photos">
            {side.photos.length ? side.photos.slice(0, 12).map((url, i) => (
              <PhotoThumb key={`${url}-${i}`} photos={side.photos} index={i} alt={`${label}: фото ${i + 1}`} title={label} />
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

// EDP — парфюмерная вода, EDT — туалетная вода, Extrait / exdp — духи, Parfum — парфюм
const CARD_TYPES: Array<[string, string]> = [["edp", "Парфюмерная вода"], ["edt", "Туалетная вода"], ["extrait", "Духи (Extrait)"], ["parfum", "Парфюм (Parfum)"], ["cologne", "Одеколон"]];

type PerfumeOption = { id: number; brand: string; name: string; year: number | null; gender: string; hasPage: boolean; hits: number };
const GENDER_LABEL: Record<string, string> = { male: "муж.", female: "жен.", unisex: "унисекс" };

/** «Выбрать аромат»: perfumes matching the card's supplier rows first, or a catalog search; the card is rebuilt.
 *  Loaded only when opened — every held card asking at once tripped the nginx connection limit (429). */
function PerfumePicker({ item }: { item: Item }) {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const query = useDebounced(q, 350).trim();
  const options = useQuery({
    queryKey: ["card-improve", "perfume-options", item.id, query],
    queryFn: () => apiJson<{ items: PerfumeOption[] }>(`/api/fragrantica/drafts/${item.id}/perfume-options?q=${encodeURIComponent(query)}`),
    staleTime: 5 * 60_000,
    enabled: open,
  });
  const pick = useMutation({
    mutationFn: (perfume: PerfumeOption) => apiJson(`/api/fragrantica/drafts/${item.id}`, { method: "PATCH", body: JSON.stringify({ perfumeId: perfume.id }) }),
    onSuccess: (_r, perfume) => { toast.success(`Аромат: ${perfume.brand} ${perfume.name} — карточка пересобирается`); queryClient.invalidateQueries({ queryKey: ["card-improve"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const confirm = useMutation({
    mutationFn: () => apiJson(`/api/fragrantica/drafts/${item.id}`, { method: "PATCH", body: JSON.stringify({ perfumeConfirmed: true }) }),
    onSuccess: () => { toast.success("Аромат подтверждён — карточка пересобирается"); queryClient.invalidateQueries({ queryKey: ["card-improve"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const list = options.data?.items || [];
  if (!open) {
    return (
      <div className="ci-brand">
        <div className="ci-brand-list">
          <button type="button" className="ci-brand-chip" disabled={confirm.isPending} onClick={() => confirm.mutate()} title="Проверка ошиблась: аромат и фото верные — собрать с ним">
            <Check size={13} /> Аромат верный
          </button>
          <button type="button" className="ci-brand-chip" onClick={() => setOpen(true)}><Search size={13} /> Выбрать другой аромат</button>
        </div>
      </div>
    );
  }
  return (
    <div className="ci-brand">
      <span className="ci-brand-title"><AlertTriangle size={13} /> Выберите аромат — {query ? "результаты поиска" : "подходят по строкам поставщиков"}:</span>
      <div className="ci-brand-row">
        <label className="ci-search ci-brand-search">
          <Search size={13} />
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Найти аромат: бренд и название" />
          {options.isFetching ? <Loader2 size={13} className="spin" /> : null}
        </label>
        <div className="ci-brand-list">
          {list.map((p) => (
            <button key={p.id} type="button" className="ci-brand-chip" disabled={pick.isPending || p.id === item.perfumeId} onClick={() => pick.mutate(p)}
              title={p.hasPage ? "Страница аромата есть — соберётся сразу" : "Страницы аромата ещё нет — соберётся, когда появятся данные"}>
              {p.brand} {p.name}{p.year ? ` (${p.year})` : ""}{p.gender ? ` · ${GENDER_LABEL[p.gender] || p.gender}` : ""}{p.hasPage ? "" : " · без страницы"}
            </button>
          ))}
          {!options.isFetching && !list.length ? <span className="ci-muted">{query ? "Не нашлось — попробуйте иначе." : "Подсказок нет — найдите аромат поиском."}</span> : null}
        </div>
      </div>
    </div>
  );
}

/** «Ноты подобрал ИИ»: a person checks the pyramid in the message above and confirms it. */
function NotesConfirm({ id }: { id: number }) {
  const queryClient = useQueryClient();
  const confirm = useMutation({
    mutationFn: () => apiJson(`/api/fragrantica/drafts/${id}`, { method: "PATCH", body: JSON.stringify({ notesConfirmed: true }) }),
    onSuccess: () => { toast.success("Ноты подтверждены — карточка пересобирается"); queryClient.invalidateQueries({ queryKey: ["card-improve"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  return (
    <div className="ci-brand">
      <div className="ci-brand-list">
        <button type="button" className="ci-brand-chip" disabled={confirm.isPending} onClick={() => confirm.mutate()}>Ноты верны</button>
      </div>
    </div>
  );
}

/** «Вид меняется» / «Не выбран вид»: a person picks the type; the card is rebuilt with it and keeps it. */
const GENDERS: Array<[string, string]> = [["female", "Женская"], ["male", "Мужская"], ["unisex", "Унисекс"]];

/** «Для кого»: the gender the new card is built with; a pick rebuilds the card (name, «Пол», description). */
function GenderPicker({ item }: { item: Item }) {
  const queryClient = useQueryClient();
  const pick = useMutation({
    mutationFn: (gender: string) => apiJson(`/api/fragrantica/drafts/${item.id}`, { method: "PATCH", body: JSON.stringify({ gender }) }),
    onSuccess: (_r, gender) => {
      toast.success(`Для кого: ${GENDERS.find(([key]) => key === gender)?.[1] || gender} — карточка пересобирается`);
      queryClient.invalidateQueries({ queryKey: ["card-improve"] });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });
  // drafts built before the gender was saved: read it from the new name
  const name = String(item.after?.name || "").toLowerCase();
  const current = item.gender || (/унисекс/.test(name) ? "unisex" : /женск/.test(name) ? "female" : /мужск/.test(name) ? "male" : "");
  return (
    <div className="ci-gender" role="radiogroup" aria-label="Для кого">
      <span>Для кого{item.genderChosen ? " (выбрано вручную)" : ""}:</span>
      {GENDERS.map(([key, label]) => (
        <button key={key} type="button" role="radio" aria-checked={current === key}
          className={`ci-gender-opt${current === key ? " is-on" : ""}`}
          disabled={pick.isPending || current === key} onClick={() => pick.mutate(key)}>{label}</button>
      ))}
    </div>
  );
}

function TypePicker({ item }: { item: Item }) {
  const queryClient = useQueryClient();
  const pick = useMutation({
    mutationFn: (typeKey: string) => apiJson(`/api/fragrantica/drafts/${item.id}`, { method: "PATCH", body: JSON.stringify({ typeKey }) }),
    onSuccess: (_r, typeKey) => {
      toast.success(`Вид: ${CARD_TYPES.find(([key]) => key === typeKey)?.[1] || typeKey} — карточка пересобирается`);
      queryClient.invalidateQueries({ queryKey: ["card-improve"] });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });
  return (
    <div className="ci-brand">
      <span className="ci-brand-title"><AlertTriangle size={13} /> Выберите вид — с ним соберётся новое название:</span>
      <div className="ci-brand-list">
        {CARD_TYPES.map(([key, label]) => (
          <button key={key} type="button" className="ci-brand-chip" disabled={pick.isPending} onClick={() => pick.mutate(key)}>{label}</button>
        ))}
      </div>
    </div>
  );
}

/** «Подобрать похожий бренд»: подсказки сборки + поиск по справочнику брендов Ozon. */
function BrandPicker({ item }: { item: Item }) {
  const queryClient = useQueryClient();
  const [q, setQ] = useState("");
  const query = useDebounced(q, 350).trim();
  const found = useQuery({
    queryKey: ["card-improve", "brand", item.id, query],
    queryFn: () => apiJson<{ items: Array<{ id: number; value: string }> }>(`/api/fragrantica/drafts/${item.id}/brand-search?q=${encodeURIComponent(query)}`),
    enabled: query.length >= 2,
    staleTime: 5 * 60_000,
  });
  const pick = useMutation({
    mutationFn: (brand: { id: number; value: string }) => apiJson(`/api/fragrantica/drafts/${item.id}`, { method: "PATCH", body: JSON.stringify({ brand }) }),
    onSuccess: (_r, brand) => { toast.success(`Бренд: ${brand.value}`); queryClient.invalidateQueries({ queryKey: ["card-improve"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const list = query.length >= 2 ? found.data?.items || [] : item.brandCandidates || [];
  return (
    <div className="ci-brand">
      <span className="ci-brand-title"><AlertTriangle size={13} /> Бренда «{item.brand}» нет в справочнике Ozon — выберите похожий:</span>
      <div className="ci-brand-row">
        <label className="ci-search ci-brand-search">
          <Search size={13} />
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Найти бренд в справочнике Ozon" />
          {found.isFetching ? <Loader2 size={13} className="spin" /> : null}
        </label>
        <div className="ci-brand-list">
          {list.map((b) => (
            <button key={b.id} type="button" className="ci-brand-chip" disabled={pick.isPending} onClick={() => pick.mutate(b)}>{b.value}</button>
          ))}
          {query.length >= 2 && found.data && !list.length ? <span className="ci-muted">Не нашлось — попробуйте другое написание.</span> : null}
          {query.length < 2 && !list.length ? <span className="ci-muted">Подсказок нет — начните вводить название.</span> : null}
        </div>
      </div>
    </div>
  );
}
