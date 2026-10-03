import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, ArrowLeft, ArrowRight, Camera, Check, CheckCheck, ChevronDown, ChevronUp, Link2, Loader2, Plus, RefreshCw, Search, Sparkles, Trash2, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { errorMessage, useDebounced } from "../lib/common";
import { toast } from "../lib/toast";

// «Конвейер» страницы «Фрагрантика»: сервер раскладывает ароматы на объёмы из PriceMaster и сам собирает
// карточки (привязка + цена, фото и пирамиды, описание ИИ — одно на аромат). Здесь — проверить и одобрить.

function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

export type WorkTarget = { key: string; kind: "ozon" | "yandex"; id: string; label: string; marketplace: string; style?: string };

type LinkRow = { id: string; name: string; supplierName: string; price: number; priceCurrency: string; ozonPrice: number; recommended: boolean; issues?: string[]; manual?: boolean; linked?: boolean };
type DraftExport = { id: number; accountName: string; offerId: string; status: string; error: string | null; result?: { links?: string; docs?: string; docsInfo?: string } | null };
type Draft = {
  id: number; perfumeId: number; brand: string; perfumeName: string; thumb: string; kind: "perfume" | "card";
  volume: number | null; tester: boolean; typeKey: string; status: string; stage: string | null; startedAt?: string | null; targets: string[]; error: string | null;
  exports: DraftExport[];
  data: {
    name?: string; offerId?: string; price?: number; oldPrice?: number; yandexPrice?: number; supplierName?: string; markup?: number;
    linkRows?: LinkRow[]; selectedLinks?: string[]; images?: { main?: string; notes?: Record<string, string> };
    description?: string; descriptionSource?: string; barcode?: string; missing?: string[]; warnings?: string[];
    brandMatched?: boolean; brandCandidates?: Array<{ id: number; value: string }>; skippedVolumes?: number[];
    existing?: { marketplace: string; target: string; offerId: string; rating?: number | null };
    customPhotos?: string[]; onlyCustomPhotos?: boolean;
  } | null;
};
type DraftsResponse = { items: Draft[]; counts: Record<string, number>; targets: WorkTarget[]; timing?: { medianBuildMs: number; parallel: number; serverNow: string } };
type Timing = { medianMs: number; parallel: number; freeSlots: number; now: number; queuePos: Map<number, number> };

const BUILD_STEPS: Array<[string, string]> = [["form", "Характеристики"], ["links", "Поставщики"], ["photos", "Фото"], ["description", "Описание"]];

/** Server clock + a tick every second, so the time bars move between polls. */
function useConveyorClock(serverNow?: string) {
  const [offset, setOffset] = useState(0);
  const [now, setNow] = useState(Date.now());
  useEffect(() => { if (serverNow) setOffset(Date.parse(serverNow) - Date.now()); }, [serverNow]);
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(t);
  }, []);
  return now + offset;
}

function formatSeconds(ms: number) {
  const s = Math.max(1, Math.round(ms / 1000));
  return s < 60 ? `${s} с` : `${Math.floor(s / 60)} мин ${s % 60 ? `${s % 60} с` : ""}`.trim();
}

/** Per card: 4 steps + a bar that grows with time (median build time), never claiming «done» before it is. */
function BuildProgress({ draft, timing }: { draft: Draft; timing: Timing }) {
  if (draft.status === "queued") {
    const pos = timing.queuePos.get(draft.id) || 1;
    // free slots take the first queued cards right away (the worker looks every second)
    const ahead = pos - timing.freeSlots;
    const wait = ahead <= 0 ? 0 : Math.ceil(ahead / Math.max(1, timing.parallel)) * timing.medianMs;
    return (
      <div className="fr-progress is-queued">
        <div className="fr-progress-bar"><span style={{ width: "0%" }} /></div>
        <div className="fr-progress-meta">в очереди · {pos}-я · {wait ? `старт примерно через ${formatSeconds(wait)}` : "стартует через пару секунд"}</div>
      </div>
    );
  }
  const done = new Set((draft.stage || "").startsWith("build:") ? (draft.stage || "").slice(6).split(",") : []);
  const elapsed = draft.startedAt ? Math.max(0, timing.now - Date.parse(draft.startedAt)) : 0;
  const byTime = Math.min(0.95, elapsed / Math.max(5000, timing.medianMs));
  const bySteps = done.size / BUILD_STEPS.length;
  const share = Math.max(byTime, bySteps * 0.97);
  const left = Math.max(0, timing.medianMs - elapsed);
  return (
    <div className="fr-progress">
      <div className="fr-progress-bar"><span style={{ width: `${Math.round(share * 100)}%` }} /></div>
      <div className="fr-progress-steps">
        {BUILD_STEPS.map(([key, label]) => (
          <span key={key} className={done.has(key) ? "is-done" : ""}>{done.has(key) ? <Check size={11} /> : <Loader2 size={11} className="spin" />}{label}</span>
        ))}
      </div>
      <div className="fr-progress-meta">
        {formatSeconds(elapsed)} прошло{left > 0 ? ` · осталось ~${formatSeconds(left)}` : " · почти готово"}
      </div>
    </div>
  );
}

const TYPES: Array<[string, string]> = [["edp", "Парфюмерная вода"], ["edt", "Туалетная вода"], ["parfum", "Духи"], ["cologne", "Одеколон"], ["oil", "Духи-масло"]];
const STAGE: Record<string, string> = {
  start: "начинаем", build: "характеристики, поставщики, фото и описание", volumes: "ищем объёмы в PriceMaster", form: "характеристики Ozon", links: "поставщики и цена", photos: "фото и пирамиды", description: "описание ИИ",
};
const STATUS: Record<string, string> = {
  queued: "в очереди", working: "собирается", ready: "готово", attention: "нужно внимание", skipped: "пропущено",
  approved: "одобрено", sending: "отправляем", sent: "отправлено", failed: "ошибка отправки",
};
const EXPORT_STATUS: Record<string, string> = { new: "создаётся", pending: "Ozon проверяет", imported: "создана", failed: "ошибка", queued_limit: "ждёт лимита Ozon" };
const BUSY = new Set(["queued", "working", "approved", "sending"]);
type Tab = "all" | "ready" | "attention" | "busy" | "sent";

export function useConveyor() {
  return useQuery({
    queryKey: ["fragrantica", "drafts"],
    queryFn: () => apiJson<DraftsResponse>("/api/fragrantica/drafts"),
    refetchInterval: (query) => {
      const items = query.state.data?.items || [];
      const waiting = items.some((d) => BUSY.has(d.status) || d.exports.some((e) => e.status === "pending" || e.status === "new"));
      return waiting ? 3000 : 20_000;
    },
  });
}

export function useAddToConveyor() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: { perfumeIds?: number[]; query?: Record<string, string>; targets: string[]; volumes?: Record<number, PickedVolume[]> }) =>
      apiJson<{ added: number; alreadyInQueue: number }>("/api/fragrantica/drafts", mutationBody(body)),
    onSuccess: (res) => {
      toast.success(res.added
        ? `В конвейер: ${res.added} ${plural(res.added, "аромат", "аромата", "ароматов")}. Объёмы, фото и описания соберутся сами.`
        : "Эти ароматы уже в конвейере.");
      queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });
}

function plural(n: number, one: string, few: string, many: string) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return one;
  if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
  return many;
}

/** Bottom bar: what the conveyor is doing; opens the review panel. */
export function ConveyorBar({ onOpen }: { onOpen: () => void }) {
  const drafts = useConveyor();
  const counts = drafts.data?.counts || {};
  const busy = (counts.queued || 0) + (counts.working || 0);
  const sending = (counts.approved || 0) + (counts.sending || 0);
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  if (!total) return null;
  return (
    <button type="button" className="fr-conv-bar" onClick={onOpen}>
      <span className="fr-conv-bar-title">Конвейер</span>
      {busy ? <span className="is-busy"><Loader2 size={13} className="spin" /> собирается {busy}</span> : null}
      {counts.ready ? <span className="is-ready"><Check size={13} /> готово {counts.ready}</span> : null}
      {counts.attention ? <span className="is-attention"><AlertTriangle size={13} /> внимание {counts.attention}</span> : null}
      {sending ? <span className="is-busy"><Loader2 size={13} className="spin" /> отправляем {sending}</span> : null}
      {counts.sent ? <span className="is-sent">отправлено {counts.sent}</span> : null}
      <span className="fr-conv-bar-open">Проверить и одобрить →</span>
    </button>
  );
}

export function ConveyorPanel({ onClose }: { onClose: () => void }) {
  const queryClient = useQueryClient();
  const drafts = useConveyor();
  const [tab, setTab] = useState<Tab>("all");
  const items = drafts.data?.items || [];
  const targets = drafts.data?.targets || [];
  const counts = drafts.data?.counts || {};
  const now = useConveyorClock(drafts.data?.timing?.serverNow);
  const timing: Timing = useMemo(() => {
    const queued = items.filter((d) => d.status === "queued" && d.kind === "card").sort((a, b) => a.id - b.id);
    const parallel = drafts.data?.timing?.parallel || 12;
    const working = items.filter((d) => d.status === "working" && d.kind === "card").length;
    return {
      medianMs: drafts.data?.timing?.medianBuildMs || 30_000,
      parallel,
      freeSlots: Math.max(0, parallel - working),
      now,
      queuePos: new Map(queued.map((d, i) => [d.id, i + 1])),
    };
  }, [items, drafts.data?.timing, now]);
  const active = items.filter((d) => d.kind === "card" && d.status !== "sent");
  const finished = active.filter((d) => !BUSY.has(d.status)).length;
  const remaining = active.length - finished;
  // the cards in work finish within about one build time; the queue behind them in rounds of «parallel»
  const queuedCount = items.filter((d) => d.status === "queued" && d.kind === "card").length;
  const overallEta = remaining ? timing.medianMs * (1 + Math.ceil(Math.max(0, queuedCount - timing.freeSlots) / Math.max(1, timing.parallel))) : 0;

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { window.removeEventListener("keydown", onKey); document.body.style.overflow = prev; };
  }, [onClose]);

  const refresh = () => queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] });
  const approve = useMutation({
    mutationFn: (ids: number[]) => apiJson<{ approved: number }>("/api/fragrantica/drafts/approve", mutationBody({ ids })),
    onSuccess: (res) => { toast.success(`Одобрено: ${res.approved}. Карточки создаются.`); refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const clear = useMutation({
    mutationFn: (statuses: string[]) => apiJson<{ removed: number }>("/api/fragrantica/drafts/clear", mutationBody({ statuses })),
    onSuccess: refresh,
    onError: (error) => toast.error(errorMessage(error)),
  });

  const visible = items.filter((d) => {
    if (tab === "ready") return d.status === "ready";
    if (tab === "attention") return d.status === "attention" || d.status === "failed" || d.status === "skipped";
    if (tab === "busy") return BUSY.has(d.status);
    if (tab === "sent") return d.status === "sent";
    return true;
  });
  // по ароматам: объёмы одного аромата рядом
  const groups = useMemo(() => {
    const map = new Map<number, Draft[]>();
    for (const d of visible) {
      if (!map.has(d.perfumeId)) map.set(d.perfumeId, []);
      map.get(d.perfumeId)!.push(d);
    }
    for (const list of map.values()) list.sort((a, b) => (a.volume || 0) - (b.volume || 0));
    return [...map.values()];
  }, [visible]);
  const readyIds = items.filter((d) => d.status === "ready").map((d) => d.id);
  const tabs: Array<[Tab, string, number]> = [
    ["all", "Все", items.length],
    ["ready", "Готовы", counts.ready || 0],
    ["attention", "Внимание", (counts.attention || 0) + (counts.failed || 0) + (counts.skipped || 0)],
    ["busy", "В работе", (counts.queued || 0) + (counts.working || 0) + (counts.approved || 0) + (counts.sending || 0)],
    ["sent", "Отправлены", counts.sent || 0],
  ];

  return createPortal(
    <div className="fr-drawer-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <aside className="fr-drawer fr-conv" role="dialog" aria-modal="true" aria-label="Конвейер">
        <button className="icon-action fr-drawer-close" type="button" onClick={onClose} aria-label="Закрыть"><X size={18} /></button>
        <div className="fr-conv-head">
          <div>
            <h2>Конвейер</h2>
            <p className="fr-hint">Объёмы взяты из PriceMaster, поставщики и цены подобраны, фото и «Пирамида аромата» нарисованы, описание одно на аромат. Проверьте и одобрите.</p>
          </div>
          <div className="fr-conv-head-actions">
            <button className="primary-action" type="button" disabled={!readyIds.length || approve.isPending} onClick={() => approve.mutate(readyIds)}>
              {approve.isPending ? <Loader2 size={14} className="spin" /> : <CheckCheck size={15} />} Одобрить все готовые ({readyIds.length})
            </button>
            {counts.sent || counts.skipped ? (
              <button className="secondary-action compact" type="button" disabled={clear.isPending} onClick={() => clear.mutate(["sent", "skipped"])}>
                Убрать отправленные и пропущенные
              </button>
            ) : null}
          </div>
        </div>
        {active.length ? (
          <div className="fr-overall">
            <div className="fr-overall-head">
              <b>{finished} из {active.length} собрано</b>
              <span>{remaining ? `осталось ~${formatSeconds(overallEta)} · одновременно до ${timing.parallel}` : "все карточки собраны"}</span>
            </div>
            <div className="fr-progress-bar is-big"><span style={{ width: `${Math.round((finished / active.length) * 100)}%` }} /></div>
          </div>
        ) : null}
        <div className="fr-conv-tabs" role="tablist">
          {tabs.map(([key, label, n]) => (
            <button key={key} type="button" role="tab" aria-selected={tab === key} className={tab === key ? "is-on" : ""} onClick={() => setTab(key)}>
              {label} <b>{n}</b>
            </button>
          ))}
        </div>
        {drafts.isLoading ? <div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем…</div> : null}
        {!drafts.isLoading && !groups.length ? <div className="fr-empty">Здесь пусто. Отметьте ароматы в каталоге или нажмите «Выбрать все» — они появятся тут.</div> : null}
        <div className="fr-conv-list">
          {groups.map((list) => <PerfumeGroup key={list[0].perfumeId} drafts={list} targets={targets} timing={timing} />)}
        </div>
      </aside>
    </div>,
    document.body,
  );
}

function PerfumeGroup({ drafts, targets, timing }: { drafts: Draft[]; targets: WorkTarget[]; timing: Timing }) {
  const first = drafts[0];
  const queryClient = useQueryClient();
  const [adding, setAdding] = useState(false);
  const [volume, setVolume] = useState("");
  const addVolume = useMutation({
    mutationFn: () => apiJson<{ id: number }>("/api/fragrantica/drafts/volume", mutationBody({
      perfumeId: first.perfumeId, volume, typeKey: first.typeKey || undefined,
      targets: first.targets.length ? first.targets : targets.map((t) => t.key),
    })),
    onSuccess: () => { setAdding(false); setVolume(""); queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  return (
    <section className="fr-conv-group">
      <header className="fr-conv-group-head">
        <img src={first.thumb} alt="" loading="lazy" />
        <div>
          <div className="fr-card-brand">{first.brand}</div>
          <b>{first.perfumeName}</b>
        </div>
        {adding ? (
          <form className="fr-conv-addvol" onSubmit={(e) => { e.preventDefault(); if (Number(volume.replace(",", ".")) > 0) addVolume.mutate(); }}>
            <input autoFocus inputMode="decimal" placeholder="мл" value={volume} onChange={(e) => setVolume(e.target.value.replace(/[^\d.,]/g, ""))} />
            <button className="secondary-action compact" type="submit" disabled={addVolume.isPending}>Добавить</button>
            <button className="icon-action" type="button" onClick={() => setAdding(false)} aria-label="Отмена"><X size={14} /></button>
          </form>
        ) : (
          <button className="secondary-action compact" type="button" onClick={() => setAdding(true)}><Plus size={13} /> объём</button>
        )}
      </header>
      {drafts.map((d) => (d.kind === "perfume" ? <PerfumeRow key={d.id} draft={d} /> : <DraftRow key={d.id} draft={d} targets={targets} timing={timing} />))}
    </section>
  );
}

function PerfumeRow({ draft }: { draft: Draft }) {
  const remove = useRemoveDraft();
  const rebuild = useRebuild();
  if (BUSY.has(draft.status)) {
    return (
      <div className="fr-conv-row is-busy">
        <span className="fr-conv-note"><Loader2 size={14} className="spin" /> Ищем объёмы в PriceMaster…</span>
        <div className="fr-progress-bar is-indeterminate"><span /></div>
      </div>
    );
  }
  return (
    <div className={`fr-conv-row is-${draft.status}`}>
      <span className="fr-conv-note"><AlertTriangle size={14} /> {draft.error || STATUS[draft.status]}</span>
      <span className="fr-conv-actions">
        <button className="icon-action" type="button" title="Проверить ещё раз" onClick={() => rebuild.mutate({ id: draft.id })}><RefreshCw size={14} /></button>
        <button className="icon-action" type="button" title="Убрать из конвейера" onClick={() => remove.mutate(draft.id)}><Trash2 size={14} /></button>
      </span>
    </div>
  );
}

function useRemoveDraft() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => apiJson(`/api/fragrantica/drafts/${id}`, { method: "DELETE" }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] }),
    onError: (error) => toast.error(errorMessage(error)),
  });
}

function useRebuild() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, newDescription = false }: { id: number; newDescription?: boolean }) =>
      apiJson(`/api/fragrantica/drafts/${id}/rebuild`, mutationBody({ newDescription })),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] }),
    onError: (error) => toast.error(errorMessage(error)),
  });
}

function DraftRow({ draft, targets, timing }: { draft: Draft; targets: WorkTarget[]; timing: Timing }) {
  const queryClient = useQueryClient();
  const d = draft.data || {};
  const [open, setOpen] = useState(false);
  const [showDescription, setShowDescription] = useState(false);
  const [name, setName] = useState(d.name || "");
  const [price, setPrice] = useState(String(d.price || ""));
  const [oldPrice, setOldPrice] = useState(String(d.oldPrice || ""));
  const [yandexPrice, setYandexPrice] = useState(String(d.yandexPrice || ""));
  const [description, setDescription] = useState(d.description || "");
  // свежие данные с сервера (сборка закончилась, другой объём поменял описание) — в поля
  useEffect(() => {
    setName(d.name || "");
    setPrice(String(d.price || ""));
    setOldPrice(String(d.oldPrice || ""));
    setYandexPrice(String(d.yandexPrice || ""));
    setDescription(d.description || "");
  }, [d.name, d.price, d.oldPrice, d.yandexPrice, d.description]);

  const patch = useMutation({
    mutationFn: (body: Record<string, unknown>) => apiJson(`/api/fragrantica/drafts/${draft.id}`, { method: "PATCH", body: JSON.stringify(body) }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] }),
    onError: (error) => toast.error(errorMessage(error)),
  });
  const approve = useMutation({
    mutationFn: () => apiJson("/api/fragrantica/drafts/approve", mutationBody({ ids: [draft.id] })),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] }),
    onError: (error) => toast.error(errorMessage(error)),
  });
  const remove = useRemoveDraft();
  const rebuild = useRebuild();

  const busy = BUSY.has(draft.status);
  const editable = ["ready", "attention", "failed"].includes(draft.status);
  const chosen = targets.filter((t) => draft.targets.includes(t.key));
  const hasOzon = chosen.some((t) => t.kind === "ozon");
  const hasYandex = chosen.some((t) => t.kind === "yandex");
  const toggleShop = (key: string) => patch.mutate({ targets: draft.targets.includes(key) ? draft.targets.filter((k) => k !== key) : [...draft.targets, key] });
  const toggleLink = (id: string) => {
    const now = d.selectedLinks || [];
    patch.mutate({ selectedLinks: now.includes(id) ? now.filter((x) => x !== id) : [...now, id] });
  };
  const commit = (key: string, value: string, before: unknown) => { if (String(before ?? "") !== value) patch.mutate({ [key]: value }); };
  const links = d.linkRows || [];
  const selectedLinks = d.selectedLinks || [];
  const shownLinks = open ? links : links.filter((r) => selectedLinks.includes(r.id)).slice(0, 3);

  return (
    <div className={`fr-conv-card is-${draft.status}`}>
      <div className="fr-conv-photocol">
        <div className="fr-conv-photos">
          {d.customPhotos?.length
            ? <a href={d.customPhotos[0]} target="_blank" rel="noreferrer" className="fr-conv-own" title="Своё фото — главное на карточке"><img src={d.customPhotos[0]} alt="Своё фото" loading="lazy" /></a>
            : d.images?.main ? <a href={d.images.main} target="_blank" rel="noreferrer"><img src={d.images.main} alt="Флакон" loading="lazy" /></a> : <span className="fr-conv-ph">{busy ? <Loader2 size={16} className="spin" /> : "фото"}</span>}
          {d.onlyCustomPhotos ? null : chosen.map((t) => {
            const notes = t.style ? d.images?.notes?.[t.style] : undefined;
            return notes ? <a key={t.key} href={notes} target="_blank" rel="noreferrer" title={`Пирамида аромата · ${t.label}`}><img src={notes} alt={`Пирамида ${t.label}`} loading="lazy" /></a> : null;
          })}
        </div>
        <OwnPhotos draftId={draft.id} photos={d.customPhotos || []} only={Boolean(d.onlyCustomPhotos)} editable={editable} />
      </div>

      <div className="fr-conv-main">
        {busy && (draft.status === "queued" || draft.status === "working") ? <BuildProgress draft={draft} timing={timing} /> : null}
        <div className="fr-conv-line">
          <span className={`fr-conv-status is-${draft.status}`}>
            {busy ? <Loader2 size={12} className="spin" /> : draft.status === "ready" ? <Check size={12} /> : draft.status === "attention" || draft.status === "failed" ? <AlertTriangle size={12} /> : null}
            {STATUS[draft.status] || draft.status}{draft.status === "working" && draft.stage ? `: ${STAGE[draft.stage] || draft.stage}` : ""}
          </span>
          <select value={draft.typeKey} disabled={!editable} onChange={(e) => patch.mutate({ typeKey: e.target.value })} aria-label="Тип">
            {TYPES.map(([key, label]) => <option key={key} value={key}>{label}</option>)}
          </select>
          <b className="fr-conv-vol">{draft.volume} мл{draft.tester ? " · тестер" : ""}</b>
          {draft.data?.existing ? (
            <span className="fr-conv-improve" title="Улучшение существующей карточки: артикул, цена и штрихкод остаются как есть, на Маркет уходит только контент">
              улучшение {String(draft.data.existing.offerId || "")}{draft.data.existing.rating != null ? ` · рейтинг ${draft.data.existing.rating}` : ""}
            </span>
          ) : null}
          {d.offerId ? <span className="fr-hint">{d.offerId}</span> : null}
        </div>

        {editable || draft.status === "sent" || (busy && d.name) ? (
          <input className="fr-conv-name" value={name} disabled={!editable} onChange={(e) => setName(e.target.value)} onBlur={() => commit("name", name, d.name)} placeholder="Название" />
        ) : null}

        <div className="fr-conv-shops">
          {targets.map((t) => {
            const on = draft.targets.includes(t.key);
            return (
              <button key={t.key} type="button" disabled={!editable} className={`fr-conv-shop is-${t.kind}${on ? " is-on" : ""}`} onClick={() => toggleShop(t.key)} title={on ? "Не грузить в этот магазин" : "Грузить и в этот магазин"}>
                {on ? <Check size={11} /> : <Plus size={11} />}{t.label}
              </button>
            );
          })}
        </div>

        {links.length || editable ? (
          <div className="fr-conv-links">
            {shownLinks.map((r) => (
              <label key={r.id} className={`fr-conv-link${selectedLinks.includes(r.id) ? " is-on" : ""}${r.recommended ? " is-recommended" : ""}`}>
                <input type="checkbox" disabled={!editable} checked={selectedLinks.includes(r.id)} onChange={() => toggleLink(r.id)} />
                <span>{r.name}<small>{r.supplierName}{(r.issues || []).length ? ` · ${(r.issues || []).join(", ")}` : ""}</small></span>
                <b>{r.price.toLocaleString("ru")} {r.priceCurrency === "RUB" ? "₽" : "$"}</b>
              </label>
            ))}
            {links.length > shownLinks.length || open ? (
              <button className="fr-link-button" type="button" onClick={() => setOpen((v) => !v)}>
                {open ? <><ChevronUp size={12} /> только выбранные</> : <><ChevronDown size={12} /> все строки поставщиков ({links.length})</>}
              </button>
            ) : null}
            {!links.length && !busy ? <span className="fr-hint">Автоматически строк поставщиков не нашлось — найдите их в PriceMaster ниже или поставьте цену сами.</span> : null}
            {editable ? <PmLinkSearch draft={draft} onAdd={(rows) => patch.mutate({ addLinks: rows })} adding={patch.isPending} /> : null}
          </div>
        ) : null}

        {editable || draft.status === "sent" ? (
          <div className="fr-conv-prices">
            {hasOzon || !hasYandex ? (
              <>
                <label><span>Ozon, ₽</span><input inputMode="numeric" disabled={!editable} value={price} onChange={(e) => setPrice(e.target.value.replace(/\D/g, ""))} onBlur={() => commit("price", price, d.price)} /></label>
                <label><span>до скидки</span><input inputMode="numeric" disabled={!editable} value={oldPrice} onChange={(e) => setOldPrice(e.target.value.replace(/\D/g, ""))} onBlur={() => commit("oldPrice", oldPrice, d.oldPrice)} /></label>
              </>
            ) : null}
            {hasYandex ? <label><span>Маркет, ₽</span><input inputMode="numeric" disabled={!editable} value={yandexPrice} onChange={(e) => setYandexPrice(e.target.value.replace(/\D/g, ""))} onBlur={() => commit("yandexPrice", yandexPrice, d.yandexPrice)} /></label> : null}
            {d.supplierName ? <span className="fr-hint">по {d.supplierName}{d.markup ? `, ×${d.markup}` : ""}</span> : null}
          </div>
        ) : null}

        {d.brandMatched === false && editable ? (
          <div className="fr-conv-brand">
            <AlertTriangle size={13} /> Бренда «{draft.brand}» нет в справочнике Ozon.
            {(d.brandCandidates || []).length ? (
              <select defaultValue="" onChange={(e) => {
                const hit = (d.brandCandidates || []).find((c) => String(c.id) === e.target.value);
                if (hit) patch.mutate({ brand: hit });
              }}>
                <option value="" disabled>Выбрать похожий…</option>
                {(d.brandCandidates || []).map((c) => <option key={c.id} value={c.id}>{c.value}</option>)}
              </select>
            ) : " Похожих нет — создайте карточку через обычную форму."}
          </div>
        ) : null}

        {editable || draft.status === "sent" ? (
          <div className="fr-conv-desc">
            <button className="fr-link-button" type="button" onClick={() => setShowDescription((v) => !v)}>
              {showDescription ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
              Описание · {description.length.toLocaleString("ru")} знаков
              {d.descriptionSource === "shared" ? " · общее с другим объёмом" : d.descriptionSource === "ai" ? " · ИИ" : d.descriptionSource === "edited" ? " · ваша правка" : d.descriptionSource === "fragrantica" ? " · с Фрагрантики" : ""}
            </button>
            {showDescription ? (
              <>
                <textarea className="fr-description-input" disabled={!editable} value={description} onChange={(e) => setDescription(e.target.value)} onBlur={() => commit("description", description, d.description)} />
                <div className="fr-actions is-tight">
                  <button className="secondary-action compact" type="button" disabled={!editable || rebuild.isPending} onClick={() => rebuild.mutate({ id: draft.id, newDescription: true })}>
                    {rebuild.isPending ? <Loader2 size={13} className="spin" /> : <Sparkles size={13} />} Переписать ИИ
                  </button>
                  <span className="fr-hint">Текст общий для всех объёмов этого аромата.</span>
                </div>
              </>
            ) : <p className="fr-conv-desc-preview">{description.slice(0, 180)}{description.length > 180 ? "…" : ""}</p>}
          </div>
        ) : null}

        {draft.error && draft.status !== "ready" ? <div className="fr-warn">{draft.error}</div> : null}
        {(d.warnings || []).filter(Boolean).slice(0, 3).map((w) => <div key={w} className="fr-hint">{w}</div>)}
        {draft.exports.length ? (
          <div className="fr-conv-exports">
            {draft.exports.map((e) => (
              <span key={e.id} className={`fr-status-pill is-${e.status}`} title={e.error || e.result?.docsInfo || ""}>
                {e.accountName}: {EXPORT_STATUS[e.status] || e.status}{e.status === "failed" && e.error ? ` — ${e.error.slice(0, 120)}` : ""}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <div className="fr-conv-side">
        {editable ? (
          <button className="primary-action compact" type="button" disabled={draft.status !== "ready" && draft.status !== "failed" || approve.isPending} onClick={() => approve.mutate()} title={draft.status === "attention" ? "Сначала заполните то, чего не хватает" : "Создать карточку"}>
            {approve.isPending ? <Loader2 size={13} className="spin" /> : <Check size={14} />} Одобрить
          </button>
        ) : null}
        {!busy && draft.status !== "sent" ? (
          <button className="icon-action" type="button" title="Собрать заново" onClick={() => rebuild.mutate({ id: draft.id })}><RefreshCw size={14} /></button>
        ) : null}
        {!busy ? <button className="icon-action" type="button" title="Убрать из конвейера" onClick={() => remove.mutate(draft.id)}><Trash2 size={14} /></button> : null}
      </div>
    </div>
  );
}

/** First visit (and «Изменить»): which shops do we load into. Saved in the browser; changeable any time. */
export function WorkShopsPicker({ shops, value, onSave, onClose }: {
  shops: Array<{ key: string; kind: "ozon" | "yandex"; label: string; marketplace: string; count: number }>;
  value: string[]; onSave: (keys: string[]) => void; onClose?: () => void;
}) {
  const [picked, setPicked] = useState<string[]>(value.length ? value : shops.map((s) => s.key));
  const toggle = (key: string) => setPicked((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
  return createPortal(
    <div className="fr-drawer-overlay fr-picker-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget && onClose) onClose(); }}>
      <div className="fr-picker" role="dialog" aria-modal="true" aria-label="Магазины для загрузки">
        <h2>Куда грузим?</h2>
        <p className="fr-hint">Отмеченные магазины подставятся во все карточки и в конвейер. Поменять можно в любой момент — кнопкой «Магазины» вверху страницы или прямо в карточке.</p>
        <div className="fr-picker-grid">
          {shops.map((s) => {
            const on = picked.includes(s.key);
            return (
              <button key={s.key} type="button" className={`fr-picker-shop is-${s.kind}${on ? " is-on" : ""}`} onClick={() => toggle(s.key)} aria-pressed={on}>
                <span className="fr-picker-logo">{s.label.charAt(0)}</span>
                <span className="fr-picker-name"><b>{s.label}</b><small>{s.marketplace}{s.count ? ` · ${s.count.toLocaleString("ru")} в магазине` : ""}</small></span>
                <span className="fr-picker-check">{on ? <Check size={16} /> : null}</span>
              </button>
            );
          })}
        </div>
        <div className="fr-actions">
          <button className="primary-action" type="button" disabled={!picked.length} onClick={() => onSave(picked)}>
            <Check size={15} /> {picked.length === shops.length ? "Грузить во все" : `Грузить в ${picked.length} ${plural(picked.length, "магазин", "магазина", "магазинов")}`}
          </button>
          {onClose ? <button className="secondary-action" type="button" onClick={onClose}>Отмена</button> : null}
        </div>
      </div>
    </div>,
    document.body,
  );
}

/** «Найти в PriceMaster»: свой запрос, когда автоподбор не нашёл строку или нашёл не ту. Найденная строка
 *  добавляется к строкам карточки и сразу выбирается — цена пересчитывается по наценке. */
function PmLinkSearch({ draft, onAdd, adding }: { draft: Draft; onAdd: (rows: LinkRow[]) => void; adding: boolean }) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const query = useDebounced(q, 350).trim();
  const search = useQuery({
    queryKey: ["fragrantica", "draft-pm-search", draft.id, query],
    queryFn: () => apiJson<{ rows: LinkRow[] }>(`/api/fragrantica/drafts/${draft.id}/pm-search?q=${encodeURIComponent(query)}`),
    enabled: open && query.length >= 2,
    staleTime: 60_000,
  });
  const linked = new Set((draft.data?.linkRows || []).map((r) => r.id));
  if (!open) {
    return (
      <button className="fr-link-button" type="button" onClick={() => { setOpen(true); setQ(`${draft.brand} ${draft.perfumeName}`.trim()); }}>
        <Search size={12} /> Найти в PriceMaster вручную
      </button>
    );
  }
  const rows = search.data?.rows || [];
  return (
    <div className="fr-pm-search">
      <label className="fr-search is-compact">
        <Search size={14} />
        <input autoFocus value={q} onChange={(e) => setQ(e.target.value)} placeholder="бренд, название, объём, артикул или штрихкод" />
        <button className="icon-action" type="button" aria-label="Закрыть поиск" onClick={() => setOpen(false)}><X size={14} /></button>
      </label>
      {search.isFetching ? <div className="fr-progress-bar is-indeterminate"><span /></div> : null}
      {search.isError ? <span className="fr-hint">{errorMessage(search.error)}</span> : null}
      {query.length >= 2 && !search.isFetching && !rows.length && !search.isError ? <span className="fr-hint">Ничего не нашлось — попробуйте короче: бренд и одно слово названия.</span> : null}
      <div className="fr-pm-results">
        {rows.map((r) => {
          const has = linked.has(r.id);
          return (
            <div key={r.id} className={`fr-pm-result${r.recommended ? " is-recommended" : ""}`}>
              <span>{r.name}<small>{r.supplierName}{(r.issues || []).length ? ` · ${(r.issues || []).join(", ")}` : ""}</small></span>
              <b>{r.price.toLocaleString("ru")} {r.priceCurrency === "RUB" ? "₽" : "$"}{r.ozonPrice ? <small>→ {r.ozonPrice.toLocaleString("ru")} ₽</small> : null}</b>
              <button className="secondary-action compact" type="button" disabled={has || adding} onClick={() => onAdd([r])}>
                {has ? <><Check size={12} /> привязана</> : <><Link2 size={12} /> привязать</>}
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export type PickedVolume = { volume: number; tester: boolean };
type VolumePlan = { brand: string; name: string; typeKey: string; volumes: Array<{ volume: number; rows: number; inShops: string[] }> };

/**
 * «Какие объёмы?» — при отметке аромата: объёмы из PriceMaster уже отмечены (кроме тех, что есть во всех
 * выбранных магазинах); можно снять, добавить свой (парсер мог не найти) и отметить тестер.
 */
export function VolumePicker({ perfumeId, title, targets, initial, onSave, onClose }: {
  perfumeId: number; title: string; targets: string[]; initial?: PickedVolume[];
  onSave: (volumes: PickedVolume[]) => void; onClose: () => void;
}) {
  const plan = useQuery({
    queryKey: ["fragrantica", "volume-plan", perfumeId, targets.join(",")],
    queryFn: () => apiJson<VolumePlan>(`/api/fragrantica/drafts/volume-plan?perfumeId=${perfumeId}&targets=${encodeURIComponent(targets.join(","))}`),
    staleTime: 5 * 60_000,
  });
  const [chosen, setChosen] = useState<PickedVolume[] | null>(initial?.length ? initial : null);
  const [custom, setCustom] = useState("");
  const [extra, setExtra] = useState<number[]>([]);
  // first load: PriceMaster volumes that are missing in at least one shop are pre-checked
  useEffect(() => {
    if (chosen || !plan.data) return;
    setChosen(plan.data.volumes.filter((v) => v.rows > 0 && v.inShops.length < targets.length).map((v) => ({ volume: v.volume, tester: false })));
  }, [plan.data, chosen, targets.length]);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const list = chosen || [];
  const rows = [...(plan.data?.volumes || []), ...extra.filter((v) => !(plan.data?.volumes || []).some((x) => x.volume === v)).map((volume) => ({ volume, rows: 0, inShops: [] as string[] }))]
    .sort((a, b) => a.volume - b.volume);
  const has = (volume: number, tester: boolean) => list.some((v) => v.volume === volume && v.tester === tester);
  const toggle = (volume: number, tester: boolean) => setChosen((prev) => {
    const now = prev || [];
    return has(volume, tester) ? now.filter((v) => !(v.volume === volume && v.tester === tester)) : [...now, { volume, tester }];
  });
  const addCustom = () => {
    const volume = Number(custom.replace(",", "."));
    if (!(volume > 0 && volume < 5000)) return;
    setExtra((prev) => (prev.includes(volume) ? prev : [...prev, volume]));
    if (!has(volume, false)) toggle(volume, false);
    setCustom("");
  };

  return createPortal(
    <div className="fr-drawer-overlay fr-picker-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="fr-picker fr-volpick" role="dialog" aria-modal="true" aria-label="Какие объёмы">
        <h2>Какие объёмы?</h2>
        <p className="fr-hint">{plan.data ? `${plan.data.brand} ${plan.data.name}` : title}. Отмечены объёмы из PriceMaster, которых нет хотя бы в одном магазине. Не нашлось нужного — допишите свой.</p>
        {plan.isLoading ? <div className="fr-volpick-loading"><div className="fr-progress-bar is-indeterminate"><span /></div> Ищем объёмы в PriceMaster…</div> : null}
        {plan.isError ? <div className="inline-error">{errorMessage(plan.error)}</div> : null}
        {plan.data && !rows.length ? <div className="fr-hint">В PriceMaster объёмов не нашлось — добавьте объём вручную.</div> : null}
        <div className="fr-volpick-list">
          {rows.map((v) => (
            <div key={v.volume} className={`fr-volpick-row${has(v.volume, false) || has(v.volume, true) ? " is-on" : ""}`}>
              <label className="fr-volpick-main">
                <input type="checkbox" checked={has(v.volume, false)} onChange={() => toggle(v.volume, false)} />
                <b>{v.volume} мл</b>
                <small>{v.rows ? `${v.rows} ${plural(v.rows, "строка", "строки", "строк")} в PriceMaster` : "своя / нет в PriceMaster"}</small>
              </label>
              <label className="fr-volpick-tester"><input type="checkbox" checked={has(v.volume, true)} onChange={() => toggle(v.volume, true)} /> тестер</label>
              {v.inShops.length ? <span className="fr-volpick-shops">уже есть: {v.inShops.join(", ")}</span> : null}
            </div>
          ))}
        </div>
        <form className="fr-volpick-add" onSubmit={(e) => { e.preventDefault(); addCustom(); }}>
          <input inputMode="decimal" value={custom} onChange={(e) => setCustom(e.target.value.replace(/[^\d.,]/g, ""))} placeholder="Свой объём, мл" />
          <button className="secondary-action compact" type="submit" disabled={!custom}><Plus size={13} /> Добавить</button>
        </form>
        <div className="fr-actions">
          <button className="primary-action" type="button" disabled={!list.length} onClick={() => onSave(list)}>
            <Check size={15} /> {list.length ? `Выбрать: ${list.map((v) => `${v.volume}${v.tester ? " T" : ""}`).join(", ")} мл` : "Отметьте объём"}
          </button>
          <button className="secondary-action" type="button" onClick={onClose}>Отмена</button>
        </div>
      </div>
    </div>,
    document.body,
  );
}

/**
 * «Свои фото» — для тестеров, пробников, наборов (они выглядят не как флакон с Фрагрантики). Первое фото
 * становится главным на карточке, остальные идут сразу за ним; «только мои» убирает пирамиду и карточки.
 */
export function OwnPhotos({ draftId, photos, only, editable, onChanged }: {
  draftId: number; photos: string[]; only: boolean; editable: boolean; onChanged?: () => void;
}) {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [drag, setDrag] = useState(false);
  const done = () => {
    queryClient.invalidateQueries({ queryKey: ["fragrantica", "drafts"] });
    queryClient.invalidateQueries({ queryKey: ["card-improve"] });
    onChanged?.();
  };
  const upload = useMutation({
    mutationFn: async (files: File[]) => {
      const form = new FormData();
      files.slice(0, 10).forEach((file) => form.append("photos", file));
      const res = await fetch(`/api/fragrantica/drafts/${draftId}/photos`, { method: "POST", credentials: "same-origin", body: form });
      const data = await res.json().catch(() => ({})) as { error?: string; added?: number };
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      return data;
    },
    onSuccess: (r) => { toast.success(`Фото добавлено: ${r.added || 0}`); done(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const patch = useMutation({
    mutationFn: (body: { customPhotos?: string[]; onlyCustomPhotos?: boolean }) => apiJson(`/api/fragrantica/drafts/${draftId}`, { method: "PATCH", body: JSON.stringify(body) }),
    onSuccess: done,
    onError: (error) => toast.error(errorMessage(error)),
  });
  const move = (i: number, dir: -1 | 1) => {
    const next = [...photos];
    const j = i + dir;
    if (j < 0 || j >= next.length) return;
    [next[i], next[j]] = [next[j], next[i]];
    patch.mutate({ customPhotos: next });
  };
  const pick = (list: FileList | null) => {
    const files = Array.from(list || []).filter((f) => /^image\//.test(f.type));
    if (files.length) upload.mutate(files);
  };

  if (!editable && !photos.length) return null;
  if (!open) {
    return (
      <button type="button" className="fr-own-toggle" onClick={() => setOpen(true)} title="Тестер, пробник, набор — загрузить своё фото">
        <Camera size={13} /> {photos.length ? `Свои фото: ${photos.length}` : "Свои фото"}
      </button>
    );
  }
  return (
    <div
      className={`fr-own${drag ? " is-drag" : ""}`}
      onDragOver={(e) => { if (editable) { e.preventDefault(); setDrag(true); } }}
      onDragLeave={() => setDrag(false)}
      onDrop={(e) => { e.preventDefault(); setDrag(false); if (editable) pick(e.dataTransfer.files); }}
    >
      <div className="fr-own-head">
        <b>Свои фото</b>
        <button type="button" className="fr-link-button" onClick={() => setOpen(false)} aria-label="Свернуть"><X size={13} /></button>
      </div>
      <p className="fr-hint">Первое — главное на карточке вместо флакона с Фрагрантики. Можно перетащить файлы сюда.</p>
      {photos.length ? (
        <div className="fr-own-list">
          {photos.map((url, i) => (
            <div key={url} className="fr-own-item">
              <a href={url} target="_blank" rel="noreferrer"><img src={url} alt={`Своё фото ${i + 1}`} loading="lazy" /></a>
              {i === 0 ? <span className="fr-own-main">главное</span> : null}
              {editable ? (
                <div className="fr-own-tools">
                  <button type="button" disabled={i === 0 || patch.isPending} onClick={() => move(i, -1)} aria-label="Левее"><ArrowLeft size={12} /></button>
                  <button type="button" disabled={i === photos.length - 1 || patch.isPending} onClick={() => move(i, 1)} aria-label="Правее"><ArrowRight size={12} /></button>
                  <button type="button" disabled={patch.isPending} onClick={() => patch.mutate({ customPhotos: photos.filter((u) => u !== url) })} aria-label="Убрать"><Trash2 size={12} /></button>
                </div>
              ) : null}
            </div>
          ))}
        </div>
      ) : null}
      {editable ? (
        <div className="fr-own-actions">
          <label className="secondary-action compact fr-own-upload">
            {upload.isPending ? <Loader2 size={13} className="spin" /> : <Plus size={13} />} Загрузить фото
            <input type="file" accept="image/png,image/jpeg,image/webp" multiple hidden disabled={upload.isPending} onChange={(e) => { pick(e.target.files); e.target.value = ""; }} />
          </label>
          {photos.length ? (
            <label className="fr-own-only">
              <input type="checkbox" checked={only} disabled={patch.isPending} onChange={(e) => patch.mutate({ onlyCustomPhotos: e.target.checked })} />
              только мои (без пирамиды и карточек аромата)
            </label>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
