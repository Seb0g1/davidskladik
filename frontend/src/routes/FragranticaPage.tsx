import { useEffect, useMemo, useRef, useState } from "react";
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, ExternalLink, Loader2, Pause, Play, RefreshCw, Search, Send, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { errorMessage, useDebounced } from "../lib/common";
import { toast } from "../lib/toast";
import "./fragrantica.css";

// Ответы /api/fragrantica/* описаны типами ниже; fetchJson даёт ApiError с телом ответа (missing и т. п.)
function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

// ─── Типы ───────────────────────────────────────────────────────────────────

type Gender = "male" | "female" | "unisex" | "";
type Note = { name: string; icon?: string };
type Accord = { name: string; share?: number; color?: string; background?: string };
type PmInfo = { rows: number; minUsd: number | null; volumes: number[] } | null;
type ExportedChip = { offerId: string; status: string; account: string; marketplace?: string; volume: number | null };
type ListItem = {
  id: number; url: string; brand: string; name: string; gender: Gender; year: number | null; votes: number | null;
  thumb: string; hasDetail: boolean; accords: Accord[]; exported: ExportedChip[]; pm: PmInfo;
};
type ListResponse = { items: ListItem[]; hasMore: boolean; total: number; page: number };
type CrawlerResponse = {
  stats: { brands: number; brandsCrawled: number; perfumes: number; details: number; exports: number; inPm?: number };
  paused: boolean;
  status: { lastError?: string | null; blockedUntil?: string | null };
};
type Perfume = {
  id: number; url: string; brand: string; name: string; gender: Gender; year: number | null; votes: number | null; rating: number | null;
  family: string; perfumers: string[]; notes: { top: Note[]; middle: Note[]; base: Note[]; flat: Note[] }; accords: Accord[];
  description: string; image: string; pm: PmInfo;
};
type DictValue = { dictionary_value_id?: number; value: string };
type FormAttribute = {
  id: number; name: string; description: string; type: string; required: boolean; collection: boolean;
  dictionaryId: number; maxValues: number; group: string; values: DictValue[];
};
type ExportRow = {
  id: number; marketplace?: string; offerId: string; accountName: string; volume: number | null; tester: boolean; status: string;
  productId: number | null; error: string | null; nextAttemptAt?: string | null; createdAt?: string;
  result?: { barcode?: string; warnings?: string | null; links?: string; linksAdded?: number; linksError?: string; market?: string } | null;
};
type Target = { key: string; kind: "ozon" | "yandex"; id: string; marketplace: string; label: string; style: string };
type OzonForm = {
  targets: Target[];
  account: { id: string; name: string };
  types: Array<{ key: string; typeId: number; label: string; nameLabel: string }>;
  typeKey: string; typeId: number; volume: string; tester: boolean; offerId: string; name: string; vat: string;
  dims: { depth: number; width: number; height: number; weight: number };
  attributes: FormAttribute[];
  brandMatched: boolean;
  brandCandidates: Array<{ id: number; value: string }>;
  sourceImage: string;
  exports: ExportRow[];
};
type LinkRow = {
  id: string; rowId: string; article: string; name: string; supplierName: string; partnerId: string;
  price: number; priceCurrency: string; volumeOk: boolean; nameOk: boolean; recommended: boolean; issues?: string[];
  markup: number; ozonPrice: number; yandexPrice?: number;
};
type LinkSuggestions = { productName: string; rows: LinkRow[]; suggested: string[] };
type PricePreview = { price: number; oldPrice: number; supplierName: string; markup: number; yandexPrice?: number; yandexMarkup?: number };
type ImageJob = {
  status: "running" | "done" | "failed"; error?: string | null;
  result?: { main: string | null; notes: Record<string, string>; source: string; warnings: string[] } | null;
};

const GENDER_LABEL: Record<string, string> = { male: "мужской", female: "женский", unisex: "унисекс" };
const VOLUMES = [2, 5, 10, 30, 50, 75, 90, 100, 125, 200];
const VAT_OPTIONS = [["0", "Без НДС"], ["0.05", "5%"], ["0.07", "7%"], ["0.1", "10%"], ["0.2", "20%"], ["0.22", "22%"]];
// Атрибуты, которые форма заполняет сама из полей выше
const AUTO_ATTRS = new Set([4180, 9024, 8229, 4497, 4191]);
const TEXTAREA_ATTRS = new Set([4191, 8050, 11254]);
const STATUS_LABEL: Record<string, string> = {
  new: "создаётся", pending: "Ozon проверяет", imported: "создана", failed: "ошибка", queued_limit: "ждёт лимита Ozon",
};

function plural(n: number, one: string, few: string, many: string) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return one;
  if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
  return many;
}

function formatDateTime(value?: string | null) {
  if (!value) return "";
  return new Date(value).toLocaleString("ru", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function usd(value: number | null | undefined) {
  return value ? `$${Math.round(value * 10) / 10}` : "";
}

// Полоса аккордов: цвета Фрагрантики, ширина по доле — аромат «читается» с одного взгляда
function AccordSpectrum({ accords, tall = false }: { accords: Accord[]; tall?: boolean }) {
  const list = accords.filter((a) => a.background).slice(0, 8);
  if (!list.length) return <div className={`fr-spectrum is-empty${tall ? " is-tall" : ""}`} />;
  const total = list.reduce((sum, a) => sum + Math.max(10, a.share || 50), 0);
  return (
    <div className={`fr-spectrum${tall ? " is-tall" : ""}`} title={list.map((a) => a.name).join(", ")}>
      {list.map((a) => (
        <span key={a.name} style={{ flexBasis: `${(Math.max(10, a.share || 50) / total) * 100}%`, background: a.background }} />
      ))}
    </div>
  );
}

function PmBadge({ pm, compact = false }: { pm: PmInfo; compact?: boolean }) {
  if (!pm) return null;
  if (!pm.rows) return <span className="fr-badge is-muted">нет в PriceMaster</span>;
  const vols = pm.volumes.slice(0, compact ? 3 : 8).join(" / ");
  return (
    <span className="fr-badge is-pm" title={`${pm.rows} ${plural(pm.rows, "строка", "строки", "строк")} поставщиков${pm.volumes.length ? `, объёмы ${pm.volumes.join(", ")} мл` : ""}`}>
      <Check size={12} /> в PriceMaster{pm.minUsd ? ` от ${usd(pm.minUsd)}` : ""}{vols ? ` · ${vols} мл` : ""}
    </span>
  );
}

function ShopChips({ exported }: { exported: ExportedChip[] }) {
  const shops = [...new Set(exported.filter((e) => e.status !== "failed").map((e) => e.account))];
  if (!shops.length) return null;
  return <span className="fr-badge is-shop">{shops.join(", ")}</span>;
}

// ─── Страница ───────────────────────────────────────────────────────────────

export function FragranticaPage() {
  const [q, setQ] = useState("");
  const [gender, setGender] = useState("");
  const [yearFrom, setYearFrom] = useState("");
  const [yearTo, setYearTo] = useState("");
  const [sort, setSort] = useState("popular");
  const [exported, setExported] = useState("");
  const [pm, setPm] = useState("");
  const [importUrl, setImportUrl] = useState("");
  const [openId, setOpenId] = useState<number | null>(null);
  const queryClient = useQueryClient();
  const debouncedQ = useDebounced(q, 350);

  const params = useMemo(() => {
    const p = new URLSearchParams({ sort, limit: "60" });
    if (debouncedQ.trim()) p.set("q", debouncedQ.trim());
    if (gender) p.set("gender", gender);
    if (Number(yearFrom)) p.set("yearFrom", yearFrom);
    if (Number(yearTo)) p.set("yearTo", yearTo);
    if (exported) p.set("exported", exported);
    if (pm) p.set("pm", pm);
    return p.toString();
  }, [debouncedQ, gender, yearFrom, yearTo, sort, exported, pm]);

  const list = useInfiniteQuery({
    queryKey: ["fragrantica", "catalog", params],
    queryFn: ({ pageParam }) => apiJson<ListResponse>(`/api/fragrantica/catalog?${params}&page=${pageParam}`),
    initialPageParam: 1,
    getNextPageParam: (last) => (last.hasMore ? last.page + 1 : undefined),
  });

  const crawler = useQuery({
    queryKey: ["fragrantica", "crawler"],
    queryFn: () => apiJson<CrawlerResponse>("/api/fragrantica/crawler"),
    refetchInterval: 30_000,
  });

  const toggleCrawler = useMutation({
    mutationFn: (paused: boolean) => apiJson<{ paused: boolean }>("/api/fragrantica/crawler", mutationBody({ paused })),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["fragrantica", "crawler"] }),
    onError: (error) => toast.error(errorMessage(error)),
  });

  const importByUrl = useMutation({
    mutationFn: (url: string) => apiJson<{ id: number }>("/api/fragrantica/catalog/import", mutationBody({ url })),
    onSuccess: (data) => {
      setImportUrl("");
      setOpenId(data.id);
      queryClient.invalidateQueries({ queryKey: ["fragrantica", "catalog"] });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });

  const items = list.data?.pages.flatMap((page) => page.items) ?? [];
  const total = list.data?.pages[0]?.total ?? 0;
  const stats = crawler.data?.stats;
  const blocked = Boolean(crawler.data?.status?.blockedUntil && Date.parse(crawler.data.status.blockedUntil) > Date.now());
  const loadedShare = stats && stats.brands ? Math.round((stats.brandsCrawled / stats.brands) * 100) : 0;

  return (
    <div className="page-shell fr-page">
      <PageHeader
        title="Фрагрантика"
        subtitle="Каталог ароматов fragrantica.ru: выберите аромат и создайте карточку в своих магазинах"
        action={
          stats ? (
            <div className="fr-status">
              <div className="fr-status-line">
                <i className={`fr-dot${crawler.data?.paused ? " is-paused" : blocked ? " is-blocked" : ""}`} />
                <b>{stats.perfumes.toLocaleString("ru")}</b> ароматов
                {stats.inPm ? <> · <b>{stats.inPm.toLocaleString("ru")}</b> есть в PriceMaster</> : null}
              </div>
              <div className="fr-status-progress" title={`Бренды загружены: ${stats.brandsCrawled} из ${stats.brands}`}>
                <span style={{ width: `${loadedShare}%` }} />
              </div>
              <button
                className="secondary-action compact"
                type="button"
                disabled={toggleCrawler.isPending}
                onClick={() => toggleCrawler.mutate(!crawler.data?.paused)}
                title={crawler.data?.paused ? "Продолжить фоновую загрузку каталога" : "Приостановить фоновую загрузку каталога"}
              >
                {crawler.data?.paused ? <Play size={14} /> : <Pause size={14} />}
                {crawler.data?.paused ? "Продолжить загрузку" : "Пауза загрузки"}
              </button>
              {blocked ? <span className="fr-hint">Фрагрантика не отвечает, повтор {formatDateTime(crawler.data?.status?.blockedUntil)}</span> : null}
            </div>
          ) : null
        }
      />

      <div className="fr-filters">
        <label className="fr-search">
          <Search size={16} />
          <input placeholder="Бренд или название, например dior sauvage" value={q} onChange={(e) => setQ(e.target.value)} />
        </label>
        <select value={pm} onChange={(e) => setPm(e.target.value)} aria-label="Наличие у поставщиков">
          <option value="">Все ароматы</option>
          <option value="yes">Есть в PriceMaster</option>
          <option value="no">Нет в PriceMaster</option>
        </select>
        <select value={exported} onChange={(e) => setExported(e.target.value)} aria-label="Магазины">
          <option value="">В магазинах и нет</option>
          <option value="no">Ещё не добавлены</option>
          <option value="yes">Уже в магазинах</option>
        </select>
        <select value={gender} onChange={(e) => setGender(e.target.value)} aria-label="Пол">
          <option value="">Любой пол</option>
          <option value="female">Женские</option>
          <option value="male">Мужские</option>
          <option value="unisex">Унисекс</option>
        </select>
        <div className="fr-years">
          <input inputMode="numeric" placeholder="год от" value={yearFrom} onChange={(e) => setYearFrom(e.target.value.replace(/\D/g, ""))} aria-label="Год от" />
          <input inputMode="numeric" placeholder="до" value={yearTo} onChange={(e) => setYearTo(e.target.value.replace(/\D/g, ""))} aria-label="Год до" />
        </div>
        <select value={sort} onChange={(e) => setSort(e.target.value)} aria-label="Сортировка">
          <option value="popular">Сначала популярные</option>
          <option value="pm">Больше всего у поставщиков</option>
          <option value="new">Сначала новинки</option>
          <option value="name">По алфавиту</option>
        </select>
      </div>

      <form
        className="fr-import"
        onSubmit={(e) => {
          e.preventDefault();
          if (importUrl.trim()) importByUrl.mutate(importUrl.trim());
        }}
      >
        <input placeholder="Или вставьте ссылку на аромат с fragrantica.ru" value={importUrl} onChange={(e) => setImportUrl(e.target.value)} />
        <button className="secondary-action" type="submit" disabled={importByUrl.isPending || !importUrl.trim()}>
          {importByUrl.isPending ? <Loader2 size={14} className="spin" /> : null}Открыть по ссылке
        </button>
      </form>

      <div className="fr-count">
        {list.isLoading ? "Загрузка…" : `${total.toLocaleString("ru")} ${plural(total, "аромат", "аромата", "ароматов")}`}
      </div>
      {list.isError ? <div className="inline-error">{errorMessage(list.error)}</div> : null}

      <div className="fr-grid">
        {items.map((item) => (
          <button key={item.id} type="button" className={`fr-card${item.exported.length ? " is-added" : ""}`} onClick={() => setOpenId(item.id)}>
            <div className="fr-card-img">
              <img src={item.thumb} alt="" loading="lazy" />
            </div>
            <AccordSpectrum accords={item.accords} />
            <div className="fr-card-body">
              <div className="fr-card-brand">{item.brand}</div>
              <div className="fr-card-name">{item.name}</div>
              <div className="fr-card-meta">
                {[item.year, item.gender ? GENDER_LABEL[item.gender] : ""].filter(Boolean).join(", ")}
              </div>
              <div className="fr-card-badges">
                <PmBadge pm={item.pm} compact />
                <ShopChips exported={item.exported} />
              </div>
            </div>
          </button>
        ))}
      </div>
      {!list.isLoading && !items.length ? (
        <div className="fr-empty">
          Под эти фильтры ароматов нет. Сбросьте фильтры или откройте аромат по ссылке с fragrantica.ru.
        </div>
      ) : null}
      {list.hasNextPage ? (
        <div className="fr-more">
          <button className="secondary-action" type="button" disabled={list.isFetchingNextPage} onClick={() => list.fetchNextPage()}>
            {list.isFetchingNextPage ? <Loader2 size={14} className="spin" /> : null}Показать ещё
          </button>
        </div>
      ) : null}

      {openId ? <PerfumeDrawer id={openId} onClose={() => setOpenId(null)} /> : null}
    </div>
  );
}

// ─── Карточка аромата ───────────────────────────────────────────────────────

function NotesTier({ title, notes }: { title: string; notes: Note[] }) {
  if (!notes.length) return null;
  return (
    <div className="fr-tier">
      <div className="fr-tier-title">{title}</div>
      <div className="fr-notes-row">
        {notes.map((note) => (
          <div key={note.name} className="fr-note">
            {note.icon ? <img src={note.icon} alt="" loading="lazy" /> : <span className="fr-note-letter">{note.name.charAt(0)}</span>}
            {note.name}
          </div>
        ))}
      </div>
    </div>
  );
}

function PerfumeDrawer({ id, onClose }: { id: number; onClose: () => void }) {
  const [adding, setAdding] = useState(false);
  const [showDescription, setShowDescription] = useState(false);
  const perfume = useQuery({
    queryKey: ["fragrantica", "perfume", id],
    queryFn: () => apiJson<{ perfume: Perfume }>(`/api/fragrantica/catalog/${id}`).then((r) => r.perfume),
  });

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const p = perfume.data;
  return (
    <div className="fr-drawer-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <aside className="fr-drawer" role="dialog" aria-modal="true" aria-label={p ? `${p.brand} ${p.name}` : "Аромат"}>
        <button className="icon-action fr-drawer-close" type="button" onClick={onClose} aria-label="Закрыть"><X size={18} /></button>
        {perfume.isLoading ? <div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем страницу аромата…</div> : null}
        {perfume.isError ? <div className="inline-error">{errorMessage(perfume.error)}</div> : null}

        {p ? (
          <>
            <div className="fr-hero">
              <div className="fr-hero-img"><img src={p.image} alt={`${p.brand} ${p.name}`} /></div>
              <div className="fr-hero-text">
                <div className="fr-hero-brand">{p.brand}</div>
                <h2>{p.name}</h2>
                <p className="fr-hero-meta">
                  {[p.year, p.gender ? GENDER_LABEL[p.gender] : "", p.family, p.perfumers.length ? `парфюмер ${p.perfumers.join(", ")}` : ""].filter(Boolean).join(", ")}
                </p>
                <AccordSpectrum accords={p.accords} tall />
                <div className="fr-accord-names">
                  {p.accords.slice(0, 6).map((a) => <span key={a.name}><i style={{ background: a.background }} />{a.name}</span>)}
                </div>
                <div className="fr-card-badges">
                  <PmBadge pm={p.pm} />
                  {p.rating ? <span className="fr-badge is-muted">★ {p.rating} на Фрагрантике, {(p.votes || 0).toLocaleString("ru")} голосов</span> : null}
                  <a className="fr-badge is-link" href={p.url} target="_blank" rel="noreferrer">Открыть на Фрагрантике <ExternalLink size={12} /></a>
                </div>
              </div>
            </div>

            <div className="fr-pyramid">
              <NotesTier title="Верхние ноты" notes={p.notes.top} />
              <NotesTier title="Ноты сердца" notes={p.notes.middle} />
              <NotesTier title="Базовые ноты" notes={p.notes.base} />
              <NotesTier title="Ноты" notes={p.notes.flat} />
            </div>

            {p.description ? (
              <div className="fr-description-box">
                <p className={`fr-description${showDescription ? "" : " is-clamped"}`}>{p.description}</p>
                <button className="fr-link-button" type="button" onClick={() => setShowDescription((v) => !v)}>
                  {showDescription ? "Свернуть описание" : "Показать описание целиком"}
                </button>
              </div>
            ) : null}

            {adding ? (
              <AddToShopsForm perfume={p} onCancel={() => setAdding(false)} />
            ) : (
              <div className="fr-cta">
                <button className="primary-action" type="button" onClick={() => setAdding(true)}><Send size={15} /> Добавить в магазины</button>
                <span className="fr-hint">Magic Stick, AURA, Яндекс Маркет — выберете на следующем шаге</span>
              </div>
            )}
          </>
        ) : null}
      </aside>
    </div>
  );
}

// ─── Добавление: шаг 1 — тип и объём ────────────────────────────────────────

function AddToShopsForm({ perfume, onCancel }: { perfume: Perfume; onCancel: () => void }) {
  const [typeKey, setTypeKey] = useState("");
  const [volume, setVolume] = useState("");
  const [tester, setTester] = useState(false);
  const [step, setStep] = useState<"params" | "card">("params");
  const options = useQuery({
    queryKey: ["fragrantica", "ozon-options", perfume.id],
    queryFn: () => apiJson<OzonForm>(`/api/fragrantica/ozon/form?perfumeId=${perfume.id}`),
    staleTime: 5 * 60_000,
  });
  useEffect(() => {
    if (options.data && !typeKey) setTypeKey(options.data.typeKey);
  }, [options.data, typeKey]);

  const ready = Number(volume.replace(",", ".")) > 0 && Boolean(typeKey);
  if (step === "card") {
    return (
      <CardStep
        perfume={perfume}
        typeKey={typeKey}
        volume={volume.replace(",", ".")}
        tester={tester}
        onBack={() => setStep("params")}
      />
    );
  }
  return (
    <section className="fr-form">
      <h3>Что добавляем</h3>
      {options.isError ? <div className="inline-error">{errorMessage(options.error)}</div> : null}
      <div className="fr-form-grid">
        <div className="fr-field">
          <span>Тип</span>
          <select value={typeKey} onChange={(e) => setTypeKey(e.target.value)}>
            {(options.data?.types || []).map((t) => <option key={t.key} value={t.key}>{t.nameLabel}</option>)}
          </select>
        </div>
        <div className="fr-field">
          <span>Объём, мл<span className="fr-req">*</span></span>
          <input inputMode="decimal" value={volume} onChange={(e) => setVolume(e.target.value.replace(/[^\d.,]/g, ""))} placeholder="100" autoFocus />
        </div>
        <label className="fr-field fr-check">
          <input type="checkbox" checked={tester} onChange={(e) => setTester(e.target.checked)} />
          <span>Тестер</span>
        </label>
      </div>
      <div className="fr-volumes">
        {VOLUMES.map((v) => (
          <button key={v} type="button" className={Number(volume) === v ? "is-active" : ""} onClick={() => setVolume(String(v))}>{v} мл</button>
        ))}
      </div>
      <div className="fr-actions">
        <button className="primary-action" type="button" disabled={!ready || options.isLoading} onClick={() => setStep("card")}>
          {options.isLoading ? <Loader2 size={14} className="spin" /> : null}Дальше: магазины и карточка
        </button>
        <button className="secondary-action" type="button" onClick={onCancel}>Отмена</button>
      </div>
      {options.data?.exports.length ? <ExportHistory rows={options.data.exports} /> : null}
    </section>
  );
}

function ExportHistory({ rows }: { rows: ExportRow[] }) {
  return (
    <div className="fr-history">
      <div className="fr-subtitle">Уже создавали</div>
      {rows.map((row) => (
        <div key={row.id} className="fr-history-row">
          <b>{row.accountName}</b>
          <span>{row.offerId}{row.volume ? `, ${row.volume} мл` : ""}{row.tester ? ", тестер" : ""}</span>
          <span className={`fr-status-pill is-${row.status}`}>{STATUS_LABEL[row.status] || row.status}</span>
          {row.error ? <span className="fr-hint">{row.error}</span> : null}
        </div>
      ))}
    </div>
  );
}

// ─── Поля атрибутов Ozon ────────────────────────────────────────────────────

function DictionaryField({ attr, value, onChange, accountId, typeId }: {
  attr: FormAttribute; value: DictValue[]; onChange: (next: DictValue[]) => void; accountId: string; typeId: number;
}) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const debounced = useDebounced(query, 300);
  const box = useRef<HTMLDivElement>(null);
  const results = useQuery({
    queryKey: ["fragrantica", "dict", accountId, typeId, attr.id, debounced],
    queryFn: () => apiJson<{ items: Array<{ id: number; value: string }> }>(
      `/api/fragrantica/ozon/attribute-values?accountId=${encodeURIComponent(accountId)}&typeId=${typeId}&attributeId=${attr.id}&q=${encodeURIComponent(debounced)}`,
    ),
    enabled: open,
    staleTime: 10 * 60_000,
  });
  useEffect(() => {
    const onDoc = (e: MouseEvent) => { if (box.current && !box.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);
  const max = attr.collection ? (attr.maxValues || 30) : 1;
  const pick = (item: { id: number; value: string }) => {
    const entry = { dictionary_value_id: item.id, value: item.value };
    if (value.some((v) => v.dictionary_value_id === item.id)) return;
    onChange(max === 1 ? [entry] : [...value, entry].slice(0, max));
    setQuery("");
    if (max === 1) setOpen(false);
  };
  return (
    <div className="fr-dict" ref={box}>
      <div className="fr-dict-selected">
        {value.map((v) => (
          <button key={`${v.dictionary_value_id}-${v.value}`} type="button" title="Убрать" onClick={() => onChange(value.filter((x) => x !== v))}>
            {v.value} <X size={11} />
          </button>
        ))}
      </div>
      <input
        value={query}
        placeholder={value.length >= max ? "" : "Найти в справочнике Ozon"}
        onFocus={() => setOpen(true)}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
      />
      {open ? (
        <div className="fr-dict-results">
          {results.isLoading ? <div className="fr-hint fr-pad">Ищем…</div> : null}
          {(results.data?.items || []).map((item) => (
            <button key={item.id} type="button" onClick={() => pick(item)}>{item.value}</button>
          ))}
          {!results.isLoading && !(results.data?.items || []).length ? (
            <div className="fr-hint fr-pad">{debounced.trim().length < 2 ? "Введите хотя бы 2 буквы" : "Ничего не найдено"}</div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function AttributeInput({ attr, value, onChange, accountId, typeId }: {
  attr: FormAttribute; value: DictValue[]; onChange: (next: DictValue[]) => void; accountId: string; typeId: number;
}) {
  if (attr.dictionaryId) return <DictionaryField attr={attr} value={value} onChange={onChange} accountId={accountId} typeId={typeId} />;
  const text = value.map((v) => v.value).join(attr.collection ? "; " : "");
  const set = (next: string) => onChange(next.trim() === "" ? [] : attr.collection ? next.split(";").map((s) => ({ value: s.trim() })).filter((v) => v.value) : [{ value: next }]);
  if (attr.type === "Boolean") {
    return (
      <select value={text} onChange={(e) => set(e.target.value)}>
        <option value="">—</option>
        <option value="true">Да</option>
        <option value="false">Нет</option>
      </select>
    );
  }
  if (TEXTAREA_ATTRS.has(attr.id)) return <textarea value={text} onChange={(e) => set(e.target.value)} />;
  return <input value={text} inputMode={attr.type === "Integer" || attr.type === "Decimal" ? "decimal" : undefined} onChange={(e) => set(e.target.value)} />;
}

// ─── Добавление: шаг 2 — магазины, привязка, карточка ───────────────────────

function CardStep({ perfume, typeKey, volume, tester, onBack }: {
  perfume: Perfume; typeKey: string; volume: string; tester: boolean; onBack: () => void;
}) {
  const queryClient = useQueryClient();
  const form = useQuery({
    queryKey: ["fragrantica", "ozon-form", perfume.id, typeKey, volume, tester],
    queryFn: () => apiJson<OzonForm>(`/api/fragrantica/ozon/form?perfumeId=${perfume.id}&typeKey=${typeKey}&volume=${encodeURIComponent(volume)}&tester=${tester ? 1 : 0}`),
    staleTime: Infinity,
  });
  const data = form.data;
  const targets = data?.targets || [];

  // Магазины: по умолчанию отмечены все; у каждого своя «Пирамида аромата», её нужно одобрить
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [approved, setApproved] = useState<Record<string, boolean>>({});
  useEffect(() => {
    if (!targets.length) return;
    setSelected((prev) => (Object.keys(prev).length ? prev : Object.fromEntries(targets.map((t) => [t.key, true]))));
  }, [targets]);
  const chosen = targets.filter((t) => selected[t.key]);
  const hasOzon = chosen.some((t) => t.kind === "ozon");
  const hasYandex = chosen.some((t) => t.kind === "yandex");

  const [name, setName] = useState("");
  const [offerId, setOfferId] = useState("");
  const [price, setPrice] = useState("");
  const [oldPrice, setOldPrice] = useState("");
  const [yandexPrice, setYandexPrice] = useState("");
  const [vat, setVat] = useState("0.05");
  const [description, setDescription] = useState("");
  const [dims, setDims] = useState({ depth: "", width: "", height: "", weight: "" });
  const [values, setValues] = useState<Record<number, DictValue[]>>({});
  const [showOptional, setShowOptional] = useState(false);
  const [missing, setMissing] = useState<string[]>([]);
  const [exportIds, setExportIds] = useState<number[]>([]);

  useEffect(() => {
    if (!data) return;
    setName(data.name);
    setOfferId(data.offerId);
    setVat(data.vat);
    setDims({ depth: String(data.dims.depth), width: String(data.dims.width), height: String(data.dims.height), weight: String(data.dims.weight) });
    setValues(Object.fromEntries(data.attributes.map((a) => [a.id, a.values])));
    setDescription(data.attributes.find((a) => a.id === 4191)?.values?.[0]?.value || "");
  }, [data]);

  // Фото: флакон с премиум-фоном и пирамиды для всех магазинов разом (parfumdeclaration)
  const styles = useMemo(() => [...new Set(targets.map((t) => t.style))], [targets]);
  const [jobId, setJobId] = useState<string | null>(null);
  const startImages = useMutation({
    mutationFn: (refresh: boolean) => apiJson<{ jobId: string }>("/api/fragrantica/ozon/images", mutationBody({ perfumeId: perfume.id, styles, refresh })),
    onSuccess: (res) => { setJobId(res.jobId); setApproved({}); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const startRef = useRef(startImages.mutate);
  startRef.current = startImages.mutate;
  const stylesKey = styles.join(",");
  useEffect(() => { if (stylesKey) startRef.current(false); }, [perfume.id, stylesKey]);
  const images = useQuery({
    queryKey: ["fragrantica", "images", jobId],
    queryFn: () => apiJson<ImageJob>(`/api/fragrantica/ozon/images/${jobId}`),
    enabled: Boolean(jobId),
    refetchInterval: (query) => (query.state.data?.status === "running" ? 2500 : false),
  });
  const imageResult = images.data?.status === "done" ? images.data.result : null;
  const imagesBusy = startImages.isPending || images.data?.status === "running" || (Boolean(jobId) && !images.data);
  const mainImage = imageResult?.main || data?.sourceImage || "";

  // Привязка к поставщикам и цена
  const [linkQuery, setLinkQuery] = useState("");
  const debouncedLinkQuery = useDebounced(linkQuery, 400);
  const [selectedLinks, setSelectedLinks] = useState<Record<string, LinkRow>>({});
  const [priceTouched, setPriceTouched] = useState(false);
  const suggestions = useQuery({
    queryKey: ["fragrantica", "links", perfume.id, typeKey, volume, tester, debouncedLinkQuery.trim()],
    queryFn: () => apiJson<LinkSuggestions>(`/api/fragrantica/ozon/link-suggestions?perfumeId=${perfume.id}&typeKey=${typeKey}&volume=${encodeURIComponent(volume)}&tester=${tester ? 1 : 0}&q=${encodeURIComponent(debouncedLinkQuery.trim())}`),
    staleTime: 5 * 60_000,
  });
  const suggestedApplied = useRef(false);
  useEffect(() => {
    if (suggestedApplied.current || !suggestions.data || debouncedLinkQuery.trim()) return;
    suggestedApplied.current = true;
    const ids = suggestions.data.suggested;
    setSelectedLinks(Object.fromEntries(suggestions.data.rows.filter((r) => ids.includes(r.id)).map((r) => [r.id, r])));
  }, [suggestions.data, debouncedLinkQuery]);
  const selectedRows = Object.values(selectedLinks);
  const selectionKey = selectedRows.map((r) => r.id).sort().join(",");
  const pricePreview = useQuery({
    queryKey: ["fragrantica", "price-preview", selectionKey],
    queryFn: () => apiJson<PricePreview>("/api/fragrantica/ozon/price-preview", mutationBody({ rows: selectedRows.map((r) => ({ price: r.price, priceCurrency: r.priceCurrency, supplierName: r.supplierName })) })),
    enabled: selectedRows.length > 0,
  });
  const applyPreview = (preview: PricePreview) => {
    setPrice(String(preview.price));
    setOldPrice(String(preview.oldPrice || ""));
    setYandexPrice(String(preview.yandexPrice || preview.price));
  };
  useEffect(() => {
    if (!priceTouched && pricePreview.data?.price) applyPreview(pricePreview.data);
  }, [pricePreview.data, priceTouched]);
  const toggleLink = (row: LinkRow) => setSelectedLinks((prev) => {
    const next = { ...prev };
    if (next[row.id]) delete next[row.id];
    else next[row.id] = row;
    return next;
  });

  const submit = useMutation({
    mutationFn: () => {
      const d = data!;
      const attributes = d.attributes
        .filter((a) => !AUTO_ATTRS.has(a.id))
        .map((a) => ({ id: a.id, values: values[a.id] || [] }))
        .filter((a) => a.values.length);
      attributes.push({ id: 4180, values: [{ value: name }] });
      attributes.push({ id: 9024, values: [{ value: offerId }] });
      attributes.push({ id: 8229, values: [{ dictionary_value_id: d.typeId, value: d.types.find((t) => t.key === d.typeKey)?.label || "" }] });
      if (description.trim()) attributes.push({ id: 4191, values: [{ value: description }] });
      if (Number(dims.weight)) attributes.push({ id: 4497, values: [{ value: dims.weight }] });
      return apiJson<{ exports: ExportRow[] }>("/api/fragrantica/ozon/export", mutationBody({
        perfumeId: perfume.id,
        typeId: d.typeId,
        typeKey: d.typeKey,
        targets: chosen.map((t) => ({ key: t.key, notes: approved[t.key] ? imageResult?.notes?.[t.style] || null : null })),
        offerId, name, price, oldPrice, yandexPrice, vat,
        depth: dims.depth, width: dims.width, height: dims.height, weight: dims.weight, tester,
        images: [mainImage],
        attributes,
        links: selectedRows.map((r) => ({ rowId: r.rowId, article: r.article, name: r.name, supplierName: r.supplierName, partnerId: r.partnerId, priceCurrency: r.priceCurrency })),
      }));
    },
    onSuccess: (res) => {
      setMissing([]);
      setExportIds(res.exports.map((e) => e.id));
      queryClient.invalidateQueries({ queryKey: ["fragrantica", "catalog"] });
    },
    onError: (error) => {
      const detail = (error as { detail?: { missing?: string[] } }).detail;
      if (detail?.missing) setMissing(detail.missing);
      toast.error(errorMessage(error));
    },
  });

  if (form.isLoading) return <section className="fr-form"><div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем характеристики Ozon…</div></section>;
  if (form.isError || !data) return <section className="fr-form"><div className="inline-error">{errorMessage(form.error)}</div><button className="secondary-action compact" type="button" onClick={onBack}>Назад</button></section>;

  const required = data.attributes.filter((a) => a.required && !AUTO_ATTRS.has(a.id));
  const optional = data.attributes.filter((a) => !a.required && !AUTO_ATTRS.has(a.id));
  const isMissing = (label: string) => missing.includes(label);
  const unapproved = chosen.filter((t) => imageResult?.notes?.[t.style] && !approved[t.key]);
  const sent = exportIds.length > 0;

  const renderAttr = (attr: FormAttribute) => (
    <div key={attr.id} className={`fr-field${TEXTAREA_ATTRS.has(attr.id) ? " is-wide" : ""}${isMissing(attr.name) ? " is-missing" : ""}`}>
      <span title={attr.description}>{attr.name}{attr.required ? <span className="fr-req">*</span> : null}</span>
      <AttributeInput attr={attr} value={values[attr.id] || []} onChange={(next) => setValues((prev) => ({ ...prev, [attr.id]: next }))} accountId={data.account.id} typeId={data.typeId} />
    </div>
  );

  return (
    <section className="fr-form">
      <div className="fr-form-head">
        <h3>{name || "Карточка"}</h3>
        <button className="secondary-action compact" type="button" onClick={onBack} disabled={submit.isPending}>Изменить тип или объём</button>
      </div>

      <div className="fr-step"><span className="fr-step-n">1</span>Магазины и фото</div>
      <div className="fr-shops">
        <div className="fr-shop-bottle">
          <div className="fr-photo-frame">
            {imageResult?.main ? <img src={imageResult.main} alt="Флакон" /> : imagesBusy ? <span><Loader2 size={16} className="spin" /> Премиум фон…</span> : <img src={data.sourceImage} alt="Флакон" />}
          </div>
          <span className="fr-hint">Фото флакона: общее для всех магазинов</span>
        </div>
        {targets.map((t) => {
          const notes = imageResult?.notes?.[t.style];
          const on = Boolean(selected[t.key]);
          return (
            <div key={t.key} className={`fr-shop${on ? " is-on" : ""}`}>
              <label className="fr-shop-head">
                <input type="checkbox" checked={on} onChange={(e) => setSelected((prev) => ({ ...prev, [t.key]: e.target.checked }))} />
                <span><b>{t.label}</b><small>{t.marketplace}</small></span>
              </label>
              <div className="fr-photo-frame">
                {notes ? <img src={notes} alt={`Пирамида аромата ${t.label}`} /> : imagesBusy ? <span><Loader2 size={16} className="spin" /> Рисуем пирамиду…</span> : <span>Пирамиды нет</span>}
              </div>
              <label className={`fr-approve${approved[t.key] ? " is-ok" : ""}`}>
                <input type="checkbox" disabled={!notes || !on} checked={Boolean(approved[t.key])} onChange={(e) => setApproved((prev) => ({ ...prev, [t.key]: e.target.checked }))} />
                Пирамида одобрена
              </label>
            </div>
          );
        })}
      </div>
      <div className="fr-actions is-tight">
        <button className="secondary-action compact" type="button" disabled={imagesBusy} onClick={() => startImages.mutate(true)}>
          <RefreshCw size={13} /> Обработать фото заново
        </button>
        <span className="fr-hint">«Секреты нанесения» и «Спасибо» parfumdeclaration добавит в конец карточки сам, в течение часа.</span>
      </div>
      {images.data?.status === "failed" ? <div className="fr-warn">{images.data.error}</div> : null}
      {(imageResult?.warnings || []).map((w) => <div key={w} className="fr-warn">{w}</div>)}

      <div className="fr-step"><span className="fr-step-n">2</span>Привязка к поставщикам и цена</div>
      <p className="fr-hint">Отмеченные строки PriceMaster привяжутся к товару на складе сразу после создания карточек, цены ниже считаются по ним.</p>
      <label className="fr-search is-small">
        <Search size={14} />
        <input value={linkQuery} onChange={(e) => setLinkQuery(e.target.value)} placeholder={`Свой поиск, например ${perfume.brand} ${perfume.name} ${volume}`} />
      </label>
      {suggestions.isLoading ? <div className="fr-hint"><Loader2 size={13} className="spin" /> Ищем у поставщиков…</div> : null}
      {suggestions.isError ? <div className="inline-error">{errorMessage(suggestions.error)}</div> : null}
      {suggestions.data && !suggestions.data.rows.length ? <div className="fr-hint">Подходящих строк нет. Карточку можно создать без привязки и привязать позже на складе.</div> : null}
      <div className="fr-links">
        {[...selectedRows.filter((r) => !(suggestions.data?.rows || []).some((x) => x.id === r.id)), ...(suggestions.data?.rows || [])].map((row) => (
          <label key={row.id} className={`fr-link-row${selectedLinks[row.id] ? " is-on" : ""}${row.recommended ? " is-recommended" : ""}`}>
            <input type="checkbox" checked={Boolean(selectedLinks[row.id])} onChange={() => toggleLink(row)} />
            <span className="fr-link-name">
              {row.name}
              <small>{row.supplierName}{row.article ? `, арт. ${row.article}` : ""}{(row.issues || []).length ? ` · ${(row.issues || []).join(", ")}` : ""}</small>
            </span>
            <span className="fr-link-price">
              <small>{row.price.toLocaleString("ru")} {row.priceCurrency === "RUB" ? "₽" : "$"}</small>
              <b>{row.ozonPrice.toLocaleString("ru")} ₽</b>
            </span>
          </label>
        ))}
      </div>

      <div className="fr-form-grid">
        {hasOzon ? (
          <>
            <div className={`fr-field${isMissing("Цена") ? " is-missing" : ""}`}>
              <span>Цена на Ozon, ₽<span className="fr-req">*</span></span>
              <input inputMode="numeric" value={price} onChange={(e) => { setPriceTouched(true); setPrice(e.target.value.replace(/[^\d]/g, "")); }} />
            </div>
            <div className="fr-field">
              <span>Цена до скидки, ₽</span>
              <input inputMode="numeric" value={oldPrice} onChange={(e) => { setPriceTouched(true); setOldPrice(e.target.value.replace(/[^\d]/g, "")); }} />
            </div>
          </>
        ) : null}
        {hasYandex ? (
          <div className="fr-field">
            <span>Цена на Маркете, ₽</span>
            <input inputMode="numeric" value={yandexPrice} onChange={(e) => { setPriceTouched(true); setYandexPrice(e.target.value.replace(/[^\d]/g, "")); }} />
          </div>
        ) : null}
        <div className="fr-field">
          <span>НДС</span>
          <select value={vat} onChange={(e) => setVat(e.target.value)}>
            {VAT_OPTIONS.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
          </select>
        </div>
      </div>
      {pricePreview.data?.price ? (
        <div className="fr-hint">
          По привязке: Ozon {pricePreview.data.price.toLocaleString("ru")} ₽ (наценка ×{pricePreview.data.markup}, {pricePreview.data.supplierName})
          {pricePreview.data.yandexPrice ? `, Маркет ${pricePreview.data.yandexPrice.toLocaleString("ru")} ₽` : ""}.
          {priceTouched ? <button type="button" className="fr-link-button" onClick={() => { setPriceTouched(false); applyPreview(pricePreview.data!); }}>Вернуть цены по привязке</button> : null}
        </div>
      ) : null}

      <div className="fr-step"><span className="fr-step-n">3</span>Карточка</div>
      <div className="fr-form-grid">
        <div className={`fr-field is-wide${isMissing("Название") ? " is-missing" : ""}`}>
          <span>Название<span className="fr-req">*</span></span>
          <input value={name} onChange={(e) => setName(e.target.value)} />
        </div>
        <div className={`fr-field${isMissing("Артикул") ? " is-missing" : ""}`}>
          <span>Артикул<span className="fr-req">*</span></span>
          <input value={offerId} onChange={(e) => setOfferId(e.target.value)} />
        </div>
        {(["depth", "width", "height"] as const).map((key) => (
          <div key={key} className={`fr-field${isMissing({ depth: "Длина", width: "Ширина", height: "Высота" }[key]) ? " is-missing" : ""}`}>
            <span>{{ depth: "Длина", width: "Ширина", height: "Высота" }[key]} упаковки, мм</span>
            <input inputMode="numeric" value={dims[key]} onChange={(e) => setDims((d) => ({ ...d, [key]: e.target.value.replace(/[^\d]/g, "") }))} />
          </div>
        ))}
        <div className={`fr-field${isMissing("Вес") ? " is-missing" : ""}`}>
          <span>Вес с упаковкой, г</span>
          <input inputMode="numeric" value={dims.weight} onChange={(e) => setDims((d) => ({ ...d, weight: e.target.value.replace(/[^\d]/g, "") }))} />
        </div>
        <div className="fr-field is-wide">
          <span>Описание</span>
          <textarea value={description} onChange={(e) => setDescription(e.target.value)} />
        </div>
      </div>
      {!data.brandMatched ? (
        <div className="fr-warn">
          Бренда «{perfume.brand}» нет в справочнике Ozon в точности. Выберите его в поле «Бренд»
          {data.brandCandidates.length ? `, похожие: ${data.brandCandidates.slice(0, 5).map((c) => c.value).join(", ")}` : ""}.
        </div>
      ) : null}

      <div className="fr-step"><span className="fr-step-n">4</span>Характеристики Ozon</div>
      <div className="fr-form-grid">{required.map(renderAttr)}</div>
      <button className="fr-link-button" type="button" onClick={() => setShowOptional((v) => !v)}>
        {showOptional ? "Скрыть необязательные характеристики" : `Показать необязательные характеристики (${optional.length})`}
      </button>
      {showOptional ? <div className="fr-form-grid">{optional.map(renderAttr)}</div> : null}

      <div className="fr-footer">
        <div className="fr-footer-text">
          {chosen.length ? (
            <>Создать <b>{offerId}</b> в {chosen.map((t) => t.label).join(", ")}</>
          ) : "Отметьте хотя бы один магазин"}
          {unapproved.length ? <span className="fr-warn"> Пирамида не одобрена для {unapproved.map((t) => t.label).join(", ")}: карточка уйдёт без неё.</span> : null}
          {missing.length ? <span className="fr-warn"> Не заполнено: {missing.join(", ")}</span> : null}
        </div>
        <button className="primary-action" type="button" disabled={!chosen.length || submit.isPending || sent} onClick={() => submit.mutate()}>
          {submit.isPending ? <Loader2 size={14} className="spin" /> : <Send size={14} />}
          {chosen.length > 1 ? `Создать в ${chosen.length} ${plural(chosen.length, "магазине", "магазинах", "магазинах")}` : "Создать карточку"}
        </button>
      </div>

      {sent ? (
        <div className="fr-results">
          {exportIds.map((id) => <ExportStatus key={id} id={id} />)}
          <button className="secondary-action compact" type="button" onClick={() => setExportIds([])}>Отправить ещё раз с правками</button>
        </div>
      ) : null}
    </section>
  );
}

function ExportStatus({ id }: { id: number }) {
  const status = useQuery({
    queryKey: ["fragrantica", "export", id],
    queryFn: () => apiJson<{ export: ExportRow }>(`/api/fragrantica/ozon/exports/${id}`).then((r) => r.export),
    refetchInterval: (query) => {
      const row = query.state.data;
      if (!row) return 3000;
      const r = row.result;
      return row.status === "pending" || row.status === "new" || (row.status === "imported" && (r?.barcode === "pending" || r?.links === "pending")) ? 3000 : false;
    },
  });
  const row = status.data;
  if (!row) return <div className="fr-result is-wait"><Loader2 size={14} className="spin" /> Отправляем…</div>;
  const shop = <b>{row.accountName}</b>;
  if (row.status === "imported") {
    return (
      <div className="fr-result is-ok">
        {shop}: карточка {row.offerId} создана{row.productId ? ` (product_id ${row.productId})` : ""}.
        {row.result?.links === "linked" ? ` Привязано поставщиков: ${row.result.linksAdded ?? 0}, цена и остаток пойдут автоматически.` : row.result?.links === "pending" ? " Привязываем поставщиков…" : ""}
        {row.result?.barcode === "generated" ? " Штрихкод сгенерирован." : ""}
        {row.result?.linksError ? <div className="fr-hint">Привязка: {row.result.linksError}. Повторим автоматически.</div> : null}
        {row.result?.warnings ? <div className="fr-hint">Замечания Ozon: {row.result.warnings}</div> : null}
      </div>
    );
  }
  if (row.status === "queued_limit") {
    return <div className="fr-result is-wait">{shop}: дневной лимит Ozon исчерпан, карточка уйдёт сама{row.nextAttemptAt ? ` около ${formatDateTime(row.nextAttemptAt)}` : " после 03:00 МСК"}.</div>;
  }
  if (row.status === "failed") {
    return <div className="fr-result is-bad">{shop}: карточку {row.offerId} не приняли. {row.error}</div>;
  }
  return <div className="fr-result is-wait"><Loader2 size={14} className="spin" /> {shop}: {STATUS_LABEL[row.status] || row.status}…</div>;
}
