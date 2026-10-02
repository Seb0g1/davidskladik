import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, CheckCheck, ChevronDown, ChevronUp, Loader2, Plus, RefreshCw, Sparkles, Trash2, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { errorMessage } from "../lib/common";
import { toast } from "../lib/toast";

// «Конвейер» страницы «Фрагрантика»: сервер раскладывает ароматы на объёмы из PriceMaster и сам собирает
// карточки (привязка + цена, фото и пирамиды, описание ИИ — одно на аромат). Здесь — проверить и одобрить.

function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

export type WorkTarget = { key: string; kind: "ozon" | "yandex"; id: string; label: string; marketplace: string; style?: string };

type LinkRow = { id: string; name: string; supplierName: string; price: number; priceCurrency: string; ozonPrice: number; recommended: boolean; issues?: string[] };
type DraftExport = { id: number; accountName: string; offerId: string; status: string; error: string | null; result?: { links?: string; docs?: string; docsInfo?: string } | null };
type Draft = {
  id: number; perfumeId: number; brand: string; perfumeName: string; thumb: string; kind: "perfume" | "card";
  volume: number | null; tester: boolean; typeKey: string; status: string; stage: string | null; targets: string[]; error: string | null;
  exports: DraftExport[];
  data: {
    name?: string; offerId?: string; price?: number; oldPrice?: number; yandexPrice?: number; supplierName?: string; markup?: number;
    linkRows?: LinkRow[]; selectedLinks?: string[]; images?: { main?: string; notes?: Record<string, string> };
    description?: string; descriptionSource?: string; barcode?: string; missing?: string[]; warnings?: string[];
    brandMatched?: boolean; brandCandidates?: Array<{ id: number; value: string }>; skippedVolumes?: number[];
  } | null;
};
type DraftsResponse = { items: Draft[]; counts: Record<string, number>; targets: WorkTarget[] };

const TYPES: Array<[string, string]> = [["edp", "Парфюмерная вода"], ["edt", "Туалетная вода"], ["parfum", "Духи"], ["cologne", "Одеколон"], ["oil", "Духи-масло"]];
const STAGE: Record<string, string> = {
  start: "начинаем", form: "характеристики Ozon", links: "поставщики и цена", photos: "фото и пирамиды", description: "описание ИИ",
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
    mutationFn: (body: { perfumeIds?: number[]; query?: Record<string, string>; targets: string[] }) =>
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
          {groups.map((list) => <PerfumeGroup key={list[0].perfumeId} drafts={list} targets={targets} />)}
        </div>
      </aside>
    </div>,
    document.body,
  );
}

function PerfumeGroup({ drafts, targets }: { drafts: Draft[]; targets: WorkTarget[] }) {
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
      {drafts.map((d) => (d.kind === "perfume" ? <PerfumeRow key={d.id} draft={d} /> : <DraftRow key={d.id} draft={d} targets={targets} />))}
    </section>
  );
}

function PerfumeRow({ draft }: { draft: Draft }) {
  const remove = useRemoveDraft();
  const rebuild = useRebuild();
  if (BUSY.has(draft.status)) {
    return <div className="fr-conv-row is-busy"><Loader2 size={14} className="spin" /> Ищем объёмы в PriceMaster…</div>;
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

function DraftRow({ draft, targets }: { draft: Draft; targets: WorkTarget[] }) {
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
      <div className="fr-conv-photos">
        {d.images?.main ? <a href={d.images.main} target="_blank" rel="noreferrer"><img src={d.images.main} alt="Флакон" loading="lazy" /></a> : <span className="fr-conv-ph">{busy ? <Loader2 size={16} className="spin" /> : "фото"}</span>}
        {chosen.map((t) => {
          const notes = t.style ? d.images?.notes?.[t.style] : undefined;
          return notes ? <a key={t.key} href={notes} target="_blank" rel="noreferrer" title={`Пирамида аромата · ${t.label}`}><img src={notes} alt={`Пирамида ${t.label}`} loading="lazy" /></a> : null;
        })}
      </div>

      <div className="fr-conv-main">
        <div className="fr-conv-line">
          <span className={`fr-conv-status is-${draft.status}`}>
            {busy ? <Loader2 size={12} className="spin" /> : draft.status === "ready" ? <Check size={12} /> : draft.status === "attention" || draft.status === "failed" ? <AlertTriangle size={12} /> : null}
            {STATUS[draft.status] || draft.status}{draft.status === "working" && draft.stage ? `: ${STAGE[draft.stage] || draft.stage}` : ""}
          </span>
          <select value={draft.typeKey} disabled={!editable} onChange={(e) => patch.mutate({ typeKey: e.target.value })} aria-label="Тип">
            {TYPES.map(([key, label]) => <option key={key} value={key}>{label}</option>)}
          </select>
          <b className="fr-conv-vol">{draft.volume} мл{draft.tester ? " · тестер" : ""}</b>
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
            {!links.length && !busy ? <span className="fr-hint">Строк поставщиков нет — карточка уйдёт без привязки, цену поставьте сами.</span> : null}
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
