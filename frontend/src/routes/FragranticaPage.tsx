import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, CheckSquare, ExternalLink, Loader2, Pause, Play, Plus, RefreshCw, Ruler, Search, Send, Sparkles, Square, Store, Trash2, Workflow, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { errorMessage, useDebounced } from "../lib/common";
import { toast } from "../lib/toast";
import { openPhotoLightbox } from "../components/PhotoLightbox";
import { ConveyorBar, ConveyorPanel, useAddToConveyor, VolumePicker, WorkShopsPicker, type PickedVolume } from "./FragranticaConveyor";
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
type ExportedChip = { offerId: string; status: string; account: string; accountId?: string; marketplace?: string; volume: number | null; tester?: boolean; error?: string | null };
type Shop = { id: string; kind: "ozon" | "yandex"; label: string; marketplace: string; added: number; pending: number; failed: number; offers: number; stock?: number };
type StockMap = Record<string, { n: number; v: number[] }>;
type ListItem = {
  id: number; url: string; brand: string; name: string; gender: Gender; year: number | null; votes: number | null;
  thumb: string; hasDetail: boolean; accords: Accord[]; exported: ExportedChip[]; pm: PmInfo;
  /** Cards already on the shops before Fragrantica (warehouse), by shop id */
  stock?: Record<string, { n: number; v: number[] }>;
};
type ListResponse = { items: ListItem[]; hasMore: boolean; total: number; page: number };
type CrawlerResponse = {
  shops?: Shop[];
  stats: { brands: number; brandsCrawled: number; perfumes: number; details: number; exports: number; inPm?: number };
  paused: boolean;
  status: { lastError?: string | null; blockedUntil?: string | null };
};
type Perfume = {
  id: number; url: string; brand: string; name: string; gender: Gender; year: number | null; votes: number | null; rating: number | null;
  family: string; perfumers: string[]; notes: { top: Note[]; middle: Note[]; base: Note[]; flat: Note[] }; accords: Accord[];
  description: string; image: string; pm: PmInfo; detailMissing?: boolean; fetchError?: string | null;
};
type DictValue = { dictionary_value_id?: number; value: string };
type FormAttribute = {
  id: number; name: string; description: string; type: string; required: boolean; collection: boolean;
  dictionaryId: number; maxValues: number; group: string; values: DictValue[];
};
type ExportRow = {
  id: number; marketplace?: string; offerId: string; accountName: string; volume: number | null; tester: boolean; status: string;
  productId: number | null; error: string | null; nextAttemptAt?: string | null; createdAt?: string;
  result?: { barcode?: string; warnings?: string | null; links?: string; linksAdded?: number; linksError?: string; market?: string; docs?: string; docsInfo?: string } | null;
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
  vatByTarget?: Record<string, string>;
  country?: { source: string; ozon: string | null } | null;
};
type DimsRow = { volume: number | string; depth: number | string; width: number | string; height: number | string; weight: number | string };
type LinkRow = {
  id: string; rowId: string; article: string; name: string; supplierName: string; partnerId: string;
  price: number; priceCurrency: string; volumeOk: boolean; nameOk: boolean; recommended: boolean; issues?: string[];
  markup: number; ozonPrice: number; yandexPrice?: number;
};
type LinkSuggestions = { productName: string; rows: LinkRow[]; suggested: string[] };
type PricePreview = { price: number; oldPrice: number; supplierName: string; markup: number; yandexPrice?: number; yandexMarkup?: number };
type ImageJob = {
  status: "running" | "done" | "failed"; error?: string | null;
  result?: { main: string | null; notes: Record<string, string>; video?: Record<string, string>; source: string; warnings: string[] } | null;
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
  const vols = pm.volumes.filter((v) => v > 3);
  const shown = compact ? vols.slice(0, 4) : vols;
  return (
    <span className="fr-pm" title={`${pm.rows} ${plural(pm.rows, "строка", "строки", "строк")} поставщиков${vols.length ? `, объёмы ${vols.join(", ")} мл` : ""}`}>
      <span className="fr-badge is-pm"><Check size={12} /> PriceMaster{pm.minUsd ? ` от ${usd(pm.minUsd)}` : ""}</span>
      {shown.length ? <span className="fr-pm-vols">{shown.join(" · ")} мл{vols.length > shown.length ? " …" : ""}</span> : null}
    </span>
  );
}

const PENDING = new Set(["new", "pending", "running", "queued_limit"]);
type ShopState = "ok" | "stock" | "pending" | "failed" | "none";
const STATE_TEXT: Record<ShopState, string> = { ok: "добавлен через Фрагрантику", stock: "уже был в магазине (склад)", pending: "в очереди", failed: "ошибка", none: "не добавлен" };

/** One shop's state for a perfume: our card > a card that was there before (warehouse) > queue > error. */
function shopState(exported: ExportedChip[], shop: Shop, stock?: StockMap): { state: ShopState; rows: ExportedChip[] } {
  const rows = exported.filter((e) => (e.accountId ? e.accountId === shop.id : e.account === shop.label));
  if (rows.some((e) => e.status !== "failed" && !PENDING.has(e.status))) return { state: "ok", rows };
  if (stock?.[shop.id]?.n) return { state: "stock", rows };
  if (rows.some((e) => PENDING.has(e.status))) return { state: "pending", rows };
  if (rows.length) return { state: "failed", rows };
  return { state: "none", rows };
}

const shortShop = (label: string) => (label.length <= 9 ? label : label.split(/\s+/)[0].slice(0, 9));

/** Every shop on the card: added (with volumes), waiting, failed or not added. */
function ShopChips({ exported, shops, stock }: { exported: ExportedChip[]; shops: Shop[]; stock?: StockMap }) {
  if (!shops.length) return null;
  return (
    <span className="fr-shops-row">
      {shops.map((shop) => {
        const { state, rows } = shopState(exported, shop, stock);
        const had = stock?.[shop.id];
        const vols = state === "stock" && had
          ? had.v.map(String)
          : [...new Set(rows.filter((r) => r.status !== "failed").map((r) => `${r.volume ?? "?"}${r.tester ? "T" : ""}`))];
        const title = `${shop.label} (${shop.marketplace}): ${STATE_TEXT[state]}`
          + (had ? `\nНа складе: ${had.n} ${plural(had.n, "карточка", "карточки", "карточек")}${had.v.length ? `, ${had.v.join(" / ")} мл` : ""}` : "")
          + (rows.length ? `\n${rows.map((r) => `${r.offerId} — ${r.status}${r.error ? `: ${r.error}` : ""}`).join("\n")}` : "");
        return (
          <span key={shop.id} className={`fr-shop-chip is-${state} is-${shop.kind}`} title={title}>
            <i aria-hidden="true" />{shortShop(shop.label)}{(state === "ok" || state === "stock") && vols.length ? <small>{vols.join("·")}</small> : null}
          </span>
        );
      })}
    </span>
  );
}

/** Control bar: per shop — added / waiting / failed; a click filters the list. */
function ShopsBar({ shops, value, onPick }: { shops: Shop[]; value: string; onPick: (v: string) => void }) {
  if (!shops.length) return null;
  const pick = (v: string) => onPick(value === v ? "" : v);
  return (
    <div className="fr-shops-bar">
      {shops.map((s) => (
        <div key={s.id} className={`fr-shops-card is-${s.kind}`}>
          <div className="fr-shops-name">{s.label}<span>{s.marketplace}</span></div>
          <div className="fr-shops-nums">
            <button type="button" className={value === `in:${s.id}` ? "is-on" : ""} onClick={() => pick(`in:${s.id}`)} title="Ароматы, которые есть в магазине: были на складе раньше или добавлены через Фрагрантику">
              <b>{((s.stock || 0) + s.added).toLocaleString("ru")}</b> в магазине
            </button>
            {s.added ? <span className="is-ours" title="Добавлено через Фрагрантику">{s.added} через Фрагрантику</span> : null}
            <button type="button" className={value === `out:${s.id}` ? "is-on" : ""} onClick={() => pick(`out:${s.id}`)} title="Показать ароматы, которых в этом магазине ещё нет">
              ещё нет
            </button>
            {s.pending ? <span className="is-pending">{s.pending} в очереди</span> : null}
            {s.failed ? <span className="is-failed">{s.failed} с ошибкой</span> : null}
          </div>
        </div>
      ))}
    </div>
  );
}

/** Card frame: everywhere / partly / failed. */
function cardClass(exported: ExportedChip[], shops: Shop[], stock?: StockMap): string {
  if (!shops.length || (!exported.length && !Object.keys(stock || {}).length)) return "";
  const states = shops.map((s) => shopState(exported, s, stock).state);
  const present = (s: ShopState) => s === "ok" || s === "stock";
  if (states.every(present)) return " is-added is-everywhere";
  if (states.some(present)) return " is-added";
  if (states.some((s) => s === "failed")) return " is-failed";
  return "";
}

// ─── Магазины, в которые грузим (выбор при первом входе, меняется в любой момент) ─

const WORK_SHOPS_KEY = "fragrantica.workShops";
const shopKey = (s: { kind: string; id: string }) => `${s.kind}:${s.id}`;

function readWorkShops(): string[] | null {
  try {
    const raw = window.localStorage.getItem(WORK_SHOPS_KEY);
    const list = raw ? JSON.parse(raw) : null;
    return Array.isArray(list) ? list.map(String) : null;
  } catch {
    return null;
  }
}

function useWorkShops(shops: Shop[]) {
  const [saved, setSaved] = useState<string[] | null>(() => readWorkShops());
  const all = shops.map(shopKey);
  // a shop that disappeared from settings drops out; nothing saved yet → every shop
  const keys = saved ? saved.filter((k) => all.includes(k)) : all;
  const save = (next: string[]) => {
    setSaved(next);
    try { window.localStorage.setItem(WORK_SHOPS_KEY, JSON.stringify(next)); } catch { /* private mode */ }
  };
  return { keys: keys.length ? keys : all, chosen: saved !== null, save };
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
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [conveyorOpen, setConveyorOpen] = useState(false);
  const [picked, setPicked] = useState<number[]>([]);
  // perfume → volumes chosen in «Какие объёмы?»; the window opens when a perfume is picked
  const [pickedVolumes, setPickedVolumes] = useState<Record<number, PickedVolume[]>>({});
  const [volumeFor, setVolumeFor] = useState<{ id: number; title: string } | null>(null);
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

  useBrowserPageReceiver((id) => {
    setOpenId(id);
    queryClient.invalidateQueries({ queryKey: ["fragrantica"] });
  });

  const items = list.data?.pages.flatMap((page) => page.items) ?? [];
  const total = list.data?.pages[0]?.total ?? 0;
  const stats = crawler.data?.stats;
  const shops = crawler.data?.shops ?? [];
  const blocked = Boolean(crawler.data?.status?.blockedUntil && Date.parse(crawler.data.status.blockedUntil) > Date.now());
  const loadedShare = stats && stats.brands ? Math.round((stats.brandsCrawled / stats.brands) * 100) : 0;
  const work = useWorkShops(shops);
  const workShops = shops.filter((s) => work.keys.includes(shopKey(s)));
  const addToConveyor = useAddToConveyor();
  const pickerShops = shops.map((s) => ({ key: shopKey(s), kind: s.kind, label: s.label, marketplace: s.marketplace, count: (s.stock || 0) + s.added }));
  const togglePick = (id: number, title = "") => {
    if (picked.includes(id)) {
      setPicked((prev) => prev.filter((x) => x !== id));
      setPickedVolumes((prev) => { const next = { ...prev }; delete next[id]; return next; });
      return;
    }
    setVolumeFor({ id, title });
  };
  // «Выбрать все»: ароматы по текущим фильтрам, которые есть у поставщиков и которых нет хотя бы в одном из моих магазинов
  const selectAllQuery = () => {
    const query: Record<string, string> = Object.fromEntries(new URLSearchParams(params));
    delete query.limit;
    query.pm = "yes";
    if (!query.exported) query.exported = `missing:${workShops.map((s) => s.id).join(",")}`;
    return query;
  };
  const sendToConveyor = (body: { perfumeIds?: number[]; query?: Record<string, string> }) =>
    addToConveyor.mutate(
      { ...body, targets: work.keys, volumes: body.perfumeIds ? Object.fromEntries(body.perfumeIds.filter((id) => pickedVolumes[id]).map((id) => [id, pickedVolumes[id]])) : undefined },
      { onSuccess: () => { setPicked([]); setPickedVolumes({}); setConveyorOpen(true); } },
    );

  return (
    <div className="page-shell fr-page">
      <PageHeader
        title="Фрагрантика"
        subtitle="Каталог ароматов fragrantica.ru: выберите аромат и создайте карточку в своих магазинах"
        action={
          <div className="fr-head-actions">
          <button className="secondary-action compact fr-work-btn" type="button" onClick={() => setPickerOpen(true)} title="Магазины, в которые грузим">
            <Store size={14} /> {workShops.length === shops.length ? "Все магазины" : workShops.map((s) => s.label).join(", ") || "Магазины"}
          </button>
          <button className="secondary-action compact" type="button" onClick={() => setConveyorOpen(true)}>
            <Workflow size={14} /> Конвейер
          </button>
          <button className="secondary-action compact" type="button" onClick={() => setSettingsOpen(true)}>
            <Ruler size={14} /> Шаблоны габаритов
          </button>
          {stats ? (
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
          ) : null}
          </div>
        }
      />
      {settingsOpen ? <DimsTemplatesPanel onClose={() => setSettingsOpen(false)} /> : null}
      {shops.length && (pickerOpen || !work.chosen) ? (
        <WorkShopsPicker
          shops={pickerShops}
          value={work.chosen ? work.keys : []}
          onSave={(keys) => { work.save(keys); setPickerOpen(false); }}
          onClose={work.chosen ? () => setPickerOpen(false) : undefined}
        />
      ) : null}
      {conveyorOpen ? <ConveyorPanel onClose={() => setConveyorOpen(false)} /> : null}
      {volumeFor ? (
        <VolumePicker
          perfumeId={volumeFor.id}
          title={volumeFor.title}
          targets={work.keys}
          initial={pickedVolumes[volumeFor.id]}
          onClose={() => setVolumeFor(null)}
          onSave={(volumes) => {
            setPickedVolumes((prev) => ({ ...prev, [volumeFor.id]: volumes }));
            setPicked((prev) => (prev.includes(volumeFor.id) ? prev : [...prev, volumeFor.id]));
            setVolumeFor(null);
          }}
        />
      ) : null}

      <ShopsBar shops={shops} value={exported} onPick={setExported} />

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
          <option value="no">Нигде не добавлены</option>
          <option value="yes">Есть хоть в одном магазине</option>
          {workShops.length ? <option value={`missing:${workShops.map((s) => s.id).join(",")}`}>Нет хотя бы в одном из моих магазинов</option> : null}
          <option value="stock">Были в магазинах до Фрагрантики</option>
          <option value="ozon">Есть на Ozon</option>
          <option value="yandex">Есть на Яндекс Маркете</option>
          {shops.map((s) => <option key={`in-${s.id}`} value={`in:${s.id}`}>Есть в {s.label}</option>)}
          {shops.map((s) => <option key={`out-${s.id}`} value={`out:${s.id}`}>Нет в {s.label}</option>)}
          <option value="pending">В очереди на отправку</option>
          <option value="failed">С ошибкой отправки</option>
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

      <div className="fr-count fr-select-bar">
        <span>{list.isLoading ? "Загрузка…" : `${total.toLocaleString("ru")} ${plural(total, "аромат", "аромата", "ароматов")}`}</span>
        <span className="fr-select-actions">
          {picked.length ? (
            <>
              <button className="primary-action compact" type="button" disabled={addToConveyor.isPending} onClick={() => sendToConveyor({ perfumeIds: picked })}>
                {addToConveyor.isPending ? <Loader2 size={13} className="spin" /> : <Workflow size={13} />} В конвейер: {picked.length}
              </button>
              <button className="secondary-action compact" type="button" onClick={() => setPicked([])}>Снять выбор</button>
            </>
          ) : null}
          <button
            className="secondary-action compact"
            type="button"
            disabled={addToConveyor.isPending || !total || !workShops.length}
            onClick={() => sendToConveyor({ query: selectAllQuery() })}
            title="Все ароматы под текущими фильтрами, которые есть у поставщиков и которых нет хотя бы в одном из выбранных магазинов (до 300 за раз). Объёмы возьмутся из PriceMaster."
          >
            <CheckSquare size={13} /> Выбрать все в конвейер
          </button>
        </span>
      </div>
      {list.isError ? <div className="inline-error">{errorMessage(list.error)}</div> : null}

      <div className="fr-grid">
        {list.isLoading ? Array.from({ length: 12 }, (_, i) => <div key={`sk-${i}`} className="fr-card is-skeleton" aria-hidden="true"><div className="fr-card-img" /><div className="fr-card-body"><i /><i /><i /></div></div>) : null}
        {items.map((item, index) => (
          <div key={item.id} role="button" tabIndex={0} className={`fr-card fr-appear${cardClass(item.exported, shops, item.stock)}${picked.includes(item.id) ? " is-picked" : ""}`} style={{ ["--i" as string]: index % 60 }}
            onClick={() => setOpenId(item.id)} onKeyDown={(e) => { if (e.key === "Enter") setOpenId(item.id); }}>
            <div className="fr-card-img">
              <img src={item.thumb} alt="" loading="lazy" />
              <button type="button" className="fr-card-pick" aria-pressed={picked.includes(item.id)} title={picked.includes(item.id) ? "Убрать из выбора" : "Выбрать для конвейера"}
                onClick={(e) => { e.stopPropagation(); togglePick(item.id, `${item.brand} ${item.name}`); }}>
                {picked.includes(item.id) ? <CheckSquare size={18} /> : <Square size={18} />}
              </button>
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
                <ShopChips exported={item.exported} shops={shops} stock={item.stock} />
              </div>
            </div>
          </div>
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

      <ConveyorBar onOpen={() => setConveyorOpen(true)} />
      {openId ? <PerfumeDrawer id={openId} workShops={work.keys} onClose={() => setOpenId(null)} onConveyor={() => sendToConveyor({ perfumeIds: [openId] })} /> : null}
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

function PerfumeDrawer({ id, workShops, onClose, onConveyor }: { id: number; workShops: string[]; onClose: () => void; onConveyor: () => void }) {
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

  // The drawer is portalled to <body> (an animated ancestor would pin a fixed overlay to the top of the page)
  useEffect(() => {
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { document.body.style.overflow = prev; };
  }, []);

  const p = perfume.data;
  return createPortal(
    <div className="fr-drawer-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <aside className="fr-drawer" role="dialog" aria-modal="true" aria-label={p ? `${p.brand} ${p.name}` : "Аромат"}>
        <button className="icon-action fr-drawer-close" type="button" onClick={onClose} aria-label="Закрыть"><X size={18} /></button>
        {perfume.isLoading ? <div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем страницу аромата…</div> : null}
        {perfume.isError ? <div className="inline-error">{errorMessage(perfume.error)}</div> : null}

        {p ? (
          <>
            <div className="fr-hero">
              <div className="fr-hero-img"><img src={p.image} alt={`${p.brand} ${p.name}`} style={{ cursor: "zoom-in" }} onClick={() => openPhotoLightbox([p.image], 0, `${p.brand} ${p.name}`)} /></div>
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

            {p.detailMissing ? <BrowserFetchHelp perfume={p} onRetry={() => perfume.refetch()} retrying={perfume.isFetching} /> : null}

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
              <AddToShopsForm perfume={p} workShops={workShops} onCancel={() => setAdding(false)} />
            ) : (
              <div className="fr-cta">
                <button className="primary-action" type="button" onClick={() => setAdding(true)}><Send size={15} /> Добавить в магазины</button>
                <button className="secondary-action" type="button" onClick={onConveyor} title="Все объёмы из PriceMaster соберутся сами — останется проверить и одобрить"><Workflow size={15} /> Все объёмы в конвейер</button>
                <span className="fr-hint">Магазины уже выбраны вверху страницы, в карточке их можно поменять</span>
              </div>
            )}
          </>
        ) : null}
      </aside>
    </div>,
    document.body,
  );
}

// ─── Страница аромата через браузер ─────────────────────────────────────────
// Cloudflare время от времени не пускает наши серверы на fragrantica.ru, а браузер пускает.
// Закладка «→ Склад» на странице аромата берёт её HTML (тот же запрос, что делает сервер),
// открывает эту страницу с ?receive=1 и передаёт HTML через postMessage; дальше — обычный парсер.

const RECEIVE_PARAM = "receive";

function fragranticaBookmarklet(origin: string) {
  const target = JSON.stringify(`${origin}/app/fragrantica?${RECEIVE_PARAM}=1`);
  const from = JSON.stringify(origin);
  const code = String.raw`(async()=>{if(!/fragrantica\.[a-z.]+\/(perfume|parfum)\/.+-\d+\.html/i.test(location.href)){alert("Откройте страницу аромата на Фрагрантике и нажмите закладку ещё раз.");return}`
    + String.raw`var w=window.open(${target},"ds_fragrantica");var h="";`
    + String.raw`try{var r=await fetch(location.href,{credentials:"include"});h=r.ok?await r.text():""}catch(e){}`
    + String.raw`if(!h)h=document.documentElement.outerHTML;`
    + String.raw`h=h.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,"").replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi,"").replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi,"<svg></svg>").replace(/<!--[\s\S]*?-->/g,"").replace(/\s{2,}/g," ");`
    + String.raw`var sent=false;addEventListener("message",function(e){if(e.origin===${from}&&e.data&&e.data.type==="ds-fragrantica-ready"&&!sent){sent=true;w.postMessage({type:"ds-fragrantica-page",url:location.href,html:h},${from})}})})()`;
  return `javascript:${encodeURIComponent(code)}`;
}

function BookmarkletLink() {
  // React не ставит javascript:-ссылки через props, поэтому href выставляем сами.
  const href = useMemo(() => fragranticaBookmarklet(window.location.origin), []);
  return (
    <a className="fr-bookmarklet" ref={(el) => { el?.setAttribute("href", href); }} onClick={(e) => { e.preventDefault(); toast.info("Перетащите эту кнопку на панель закладок браузера"); }} title="Перетащите на панель закладок">
      → Склад
    </a>
  );
}

function BrowserFetchHelp({ perfume, onRetry, retrying }: { perfume: Perfume; onRetry: () => void; retrying: boolean }) {
  return (
    <div className="fr-fetch-help">
      <strong>Пирамиды и описания пока нет: Фрагрантика не пустила наш сервер</strong>
      <p>
        Её защита (Cloudflare) иногда блокирует серверы, а ваш браузер пускает. Загрузите страницу через браузер:
        откройте аромат на Фрагрантике и нажмите там закладку <b>«→ Склад»</b>. Склад откроется в новой вкладке,
        и аромат появится с пирамидой и описанием.
      </p>
      <ol>
        <li>Один раз перетащите кнопку <BookmarkletLink /> на панель закладок (Ctrl+Shift+B — показать панель).</li>
        <li><a href={perfume.url} target="_blank" rel="noreferrer">Откройте «{perfume.name}» на Фрагрантике <ExternalLink size={12} /></a> и нажмите закладку.</li>
      </ol>
      <div className="fr-actions">
        <button className="secondary-action compact" type="button" disabled={retrying} onClick={onRetry}>
          {retrying ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />} Попробовать с сервера ещё раз
        </button>
      </div>
      {perfume.fetchError ? <span className="fr-hint">Ответ сервера: {perfume.fetchError}</span> : null}
    </div>
  );
}

const FRAGRANTICA_ORIGIN = /^https:\/\/(www\.)?fragrantica\.[a-z.]{2,8}$/;

function useBrowserPageReceiver(onImported: (id: number) => void) {
  const handler = useRef(onImported);
  handler.current = onImported;
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get(RECEIVE_PARAM) !== "1" || !window.opener) return;
    params.delete(RECEIVE_PARAM);
    const rest = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${rest ? `?${rest}` : ""}`);
    const opener = window.opener as Window;
    let done = false;
    toast("Получаем страницу аромата из вкладки Фрагрантики…", "info", 5000);
    const onMessage = async (event: MessageEvent) => {
      if (done || !FRAGRANTICA_ORIGIN.test(event.origin) || event.data?.type !== "ds-fragrantica-page") return;
      done = true;
      try {
        const res = await apiJson<{ id: number; name: string; brand: string }>(
          "/api/fragrantica/catalog/import-html",
          mutationBody({ url: String(event.data.url || ""), html: String(event.data.html || "") }),
        );
        toast.success(`Загружено с Фрагрантики: ${res.brand} ${res.name}`);
        handler.current(res.id);
      } catch (error) {
        toast.error(errorMessage(error));
      }
    };
    window.addEventListener("message", onMessage);
    // Вкладка Фрагрантики начинает слушать «готов» только после того, как скачает страницу, — повторяем.
    const ping = window.setInterval(() => { if (!done) opener.postMessage({ type: "ds-fragrantica-ready" }, "*"); }, 500);
    const stop = window.setTimeout(() => {
      window.clearInterval(ping);
      if (!done) toast.error("Вкладка Фрагрантики не ответила. Нажмите закладку ещё раз.");
    }, 60_000);
    return () => { window.removeEventListener("message", onMessage); window.clearInterval(ping); window.clearTimeout(stop); };
  }, []);
}

// ─── Добавление: шаг 1 — тип и объём ────────────────────────────────────────

function AddToShopsForm({ perfume, workShops, onCancel }: { perfume: Perfume; workShops: string[]; onCancel: () => void }) {
  const [typeKey, setTypeKey] = useState("");
  // Описание общее для всех объёмов: сменили объём — текст остаётся тем же
  const [sharedDescription, setSharedDescription] = useState("");
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
        workShops={workShops}
        description={sharedDescription}
        onDescription={setSharedDescription}
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

function CardStep({ perfume, typeKey, volume, tester, workShops, description, onDescription, onBack }: {
  perfume: Perfume; typeKey: string; volume: string; tester: boolean; workShops: string[];
  description: string; onDescription: (text: string) => void; onBack: () => void;
}) {
  const setDescription = onDescription;
  const queryClient = useQueryClient();
  const form = useQuery({
    queryKey: ["fragrantica", "ozon-form", perfume.id, typeKey, volume, tester],
    queryFn: () => apiJson<OzonForm>(`/api/fragrantica/ozon/form?perfumeId=${perfume.id}&typeKey=${typeKey}&volume=${encodeURIComponent(volume)}&tester=${tester ? 1 : 0}`),
    staleTime: Infinity,
  });
  const data = form.data;
  const targets = data?.targets || [];

  // Магазины: отмечены выбранные вверху страницы; «Пирамида аромата» каждого магазина идёт в карточку сама
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  useEffect(() => {
    if (!targets.length) return;
    const work = targets.filter((t) => workShops.includes(t.key));
    setSelected((prev) => (Object.keys(prev).length ? prev : Object.fromEntries(targets.map((t) => [t.key, work.length ? work.some((w) => w.key === t.key) : true]))));
  }, [targets, workShops]);
  const chosen = targets.filter((t) => selected[t.key]);
  const hasOzon = chosen.some((t) => t.kind === "ozon");
  const hasYandex = chosen.some((t) => t.kind === "yandex");

  const [name, setName] = useState("");
  const [offerId, setOfferId] = useState("");
  const [barcode, setBarcode] = useState("");
  const [price, setPrice] = useState("");
  const [oldPrice, setOldPrice] = useState("");
  const [yandexPrice, setYandexPrice] = useState("");
  const [dims, setDims] = useState({ depth: "", width: "", height: "", weight: "" });
  const [values, setValues] = useState<Record<number, DictValue[]>>({});
  const [showOptional, setShowOptional] = useState(false);
  const [missing, setMissing] = useState<string[]>([]);
  const [exportIds, setExportIds] = useState<number[]>([]);

  useEffect(() => {
    if (!data) return;
    setName(data.name);
    setOfferId(data.offerId);
    setDims({ depth: String(data.dims.depth), width: String(data.dims.width), height: String(data.dims.height), weight: String(data.dims.weight) });
    setValues(Object.fromEntries(data.attributes.map((a) => [a.id, a.values])));
    // keep the text of the previous volume; otherwise the shared one from the server (or Fragrantica's)
    if (!description.trim()) setDescription(data.attributes.find((a) => a.id === 4191)?.values?.[0]?.value || "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);

  // Фото: флакон с премиум-фоном и пирамиды для всех магазинов разом (parfumdeclaration)
  const styles = useMemo(() => [...new Set(targets.map((t) => t.style))], [targets]);
  const [jobId, setJobId] = useState<string | null>(null);
  const startImages = useMutation({
    mutationFn: (refresh: boolean) => apiJson<{ jobId: string }>("/api/fragrantica/ozon/images", mutationBody({ perfumeId: perfume.id, styles, refresh })),
    onSuccess: (res) => setJobId(res.jobId),
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

  // «Написать описание ИИ»: DeepSeek по фактам Фрагрантики (ноты, семейство, парфюмер, год)
  const describe = useMutation({
    mutationFn: () => apiJson<{ description: string; bulletPoints: string[]; model: string }>("/api/fragrantica/ozon/describe", mutationBody({
      perfumeId: perfume.id, typeKey, volume, tester, name, marketplace: hasYandex ? "yandex" : "ozon",
    })),
    onSuccess: (res) => {
      setDescription(res.description);
      toast.success(`Описание готово: ${res.description.length.toLocaleString("ru")} знаков`);
    },
    onError: (error) => toast.error(errorMessage(error)),
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
        targets: chosen.map((t) => ({ key: t.key, notes: imageResult?.notes?.[t.style] || null })),
        offerId, name, price, oldPrice, yandexPrice, barcode,
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
            {imageResult?.main ? <img src={imageResult.main} alt="Флакон" style={{ cursor: "zoom-in" }} onClick={() => openPhotoLightbox([imageResult.main as string], 0)} /> : imagesBusy ? <span><Loader2 size={16} className="spin" /> Премиум фон…</span> : <img src={data.sourceImage} alt="Флакон" />}
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
                {notes ? <img src={notes} alt={`Пирамида аромата ${t.label}`} style={{ cursor: "zoom-in" }} onClick={() => openPhotoLightbox([notes], 0, `Пирамида аромата · ${t.label}`)} /> : imagesBusy ? <span><Loader2 size={16} className="spin" /> Рисуем пирамиду…</span> : <span>Пирамиды нет</span>}
              </div>
              {notes && on ? <span className="fr-approve is-ok"><Check size={13} /> Пирамида пойдёт в карточку</span> : null}
              {t.kind === "ozon" && imageResult?.video?.[t.style] ? (
                <div className="fr-video-cover" title="Видеообложка Ozon — соберётся из фото карточки, крутится по кругу">
                  <video src={imageResult.video[t.style]} muted autoPlay loop playsInline />
                  <span>Видеообложка + Rich-контент</span>
                </div>
              ) : null}
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
          <div className="fr-vat">
            {chosen.map((t) => {
              const v = data.vatByTarget?.[t.key];
              return <span key={t.key} className="fr-chip">{t.label}: {v === "0" ? "без НДС" : `${Math.round(Number(v || 0.05) * 100)}%`}</span>;
            })}
          </div>
          <span className="fr-hint">Ставится по магазину автоматически</span>
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
        <div className="fr-field">
          <span>Штрихкод производителя (EAN)</span>
          <input inputMode="numeric" placeholder="3348901250146" value={barcode} onChange={(e) => setBarcode(e.target.value.replace(/[^\d]/g, ""))} />
          <span className="fr-hint">{barcode && ![8, 12, 13, 14].includes(barcode.length) ? "EAN — 8, 12, 13 или 14 цифр" : hasYandex ? "Маркету нужен для маркированного товара; без него Ozon сделает свой, а Маркет не опубликует" : "Пусто — Ozon сгенерирует штрихкод сам"}</span>
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
          <span className="fr-field-head">
            Описание <small>{description.length.toLocaleString("ru")} знаков</small>
            <button className="secondary-action compact" type="button" disabled={describe.isPending} onClick={() => describe.mutate()}>
              {describe.isPending ? <Loader2 size={13} className="spin" /> : <Sparkles size={13} />}
              {describe.isPending ? "Пишем описание…" : "Написать описание ИИ"}
            </button>
          </span>
          <textarea className="fr-description-input" value={description} onChange={(e) => setDescription(e.target.value)} />
          <span className="fr-hint">ИИ пишет 1500–2500 знаков по нотам и фактам Фрагрантики{hasYandex ? " по правилам Маркета, они строже, так что текст подходит и Ozon" : ""}. Абзацы сохранятся в карточке.</span>
        </div>
      </div>
      {!data.brandMatched ? (
        <div className="fr-warn">
          Бренда «{perfume.brand}» нет в справочнике Ozon в точности. Выберите его в поле «Бренд»
          {data.brandCandidates.length ? `, похожие: ${data.brandCandidates.slice(0, 5).map((c) => c.value).join(", ")}` : ""}.
        </div>
      ) : null}

      <div className="fr-step"><span className="fr-step-n">4</span>Характеристики Ozon</div>
      <div className="fr-autofill">
        <Sparkles size={14} />
        <span>
          Заполнено автоматически: {data.attributes.filter((a) => (values[a.id] || []).length).length} из {data.attributes.length} характеристик
          {data.country?.ozon ? `, страна ${data.country.ozon}` : data.country?.source ? `, страна с Фрагрантики «${data.country.source}» не найдена в справочнике Ozon — выберите вручную` : ", страну бренда Фрагрантика не указала"}.
          На Маркет уходят тип, пол, семейство, год, ноты, вес, срок годности 900 дней, ТН ВЭД и ОКПД2.
        </span>
      </div>
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
        {row.result?.docs ? <div className="fr-hint">Документы: {row.result.docs === "done" ? row.result.docsInfo || "привязаны" : row.result.docs === "pending" ? "привязываем декларацию…" : `не привязаны — ${row.result.docsInfo || "нет подходящей декларации"}`}</div> : null}
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

// ─── Шаблоны габаритов ──────────────────────────────────────────────────────

const DEFAULT_DIMS: DimsRow[] = [
  { volume: 10, depth: 150, width: 125, height: 60, weight: 50 },
  { volume: 30, depth: 195, width: 125, height: 115, weight: 300 },
  { volume: 50, depth: 140, width: 120, height: 110, weight: 300 },
  { volume: 60, depth: 190, width: 120, height: 110, weight: 350 },
  { volume: 100, depth: 200, width: 130, height: 120, weight: 450 },
  { volume: 200, depth: 220, width: 140, height: 130, weight: 600 },
];

function DimsTemplatesPanel({ onClose }: { onClose: () => void }) {
  const queryClient = useQueryClient();
  const settings = useQuery({
    queryKey: ["fragrantica", "settings"],
    queryFn: () => apiJson<{ dimsTemplates: DimsRow[] }>("/api/fragrantica/settings"),
  });
  const [rows, setRows] = useState<DimsRow[] | null>(null);
  useEffect(() => {
    if (settings.data && rows === null) setRows(settings.data.dimsTemplates.length ? settings.data.dimsTemplates : DEFAULT_DIMS);
  }, [settings.data, rows]);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  const save = useMutation({
    mutationFn: () => apiJson<{ dimsTemplates: DimsRow[] }>("/api/fragrantica/settings", { method: "PUT", body: JSON.stringify({ dimsTemplates: rows || [] }) }),
    onSuccess: (res) => {
      setRows(res.dimsTemplates);
      queryClient.invalidateQueries({ queryKey: ["fragrantica", "settings"] });
      queryClient.invalidateQueries({ queryKey: ["fragrantica", "ozon-form"] });
      toast.success(`Шаблоны сохранены: ${res.dimsTemplates.length}`);
    },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const update = (index: number, key: keyof DimsRow, value: string) =>
    setRows((prev) => (prev || []).map((row, i) => (i === index ? { ...row, [key]: value.replace(/[^\d.,]/g, "") } : row)));
  const cols: Array<[keyof DimsRow, string]> = [["volume", "Объём, мл"], ["depth", "Длина, мм"], ["width", "Ширина, мм"], ["height", "Высота, мм"], ["weight", "Вес, г"]];
  return createPortal(
    <div className="fr-drawer-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <aside className="fr-drawer fr-settings" role="dialog" aria-modal="true" aria-label="Шаблоны габаритов">
        <button className="icon-action fr-drawer-close" type="button" onClick={onClose} aria-label="Закрыть"><X size={18} /></button>
        <h2>Шаблоны габаритов</h2>
        <p className="fr-hint">
          Габариты коробки и вес с упаковкой подставляются в карточку по объёму: берётся строка с этим объёмом,
          а если её нет — ближайшая большая. Их можно поправить в самой карточке перед отправкой.
        </p>
        {settings.isLoading ? <div className="empty-state"><Loader2 size={16} className="spin" /> Загрузка…</div> : null}
        {rows ? (
          <div className="fr-dims">
            <div className="fr-dims-row is-head">{cols.map(([, label]) => <span key={label}>{label}</span>)}<span /></div>
            {rows.map((row, index) => (
              <div key={index} className="fr-dims-row fr-appear" style={{ ["--i" as string]: index }}>
                {cols.map(([key, label]) => (
                  <input key={key} aria-label={label} inputMode="decimal" value={String(row[key] ?? "")} onChange={(e) => update(index, key, e.target.value)} />
                ))}
                <button className="icon-action" type="button" aria-label="Удалить строку" onClick={() => setRows((prev) => (prev || []).filter((_, i) => i !== index))}><Trash2 size={15} /></button>
              </div>
            ))}
          </div>
        ) : null}
        <div className="fr-actions">
          <button className="secondary-action compact" type="button" onClick={() => setRows((prev) => [...(prev || []), { volume: "", depth: "", width: "", height: "", weight: "" }])}><Plus size={14} /> Добавить объём</button>
          <button className="secondary-action compact" type="button" onClick={() => setRows(DEFAULT_DIMS)}>Вернуть стандартные</button>
          <button className="primary-action" type="button" disabled={save.isPending || !rows} onClick={() => save.mutate()}>
            {save.isPending ? <Loader2 size={14} className="spin" /> : <Check size={14} />} Сохранить
          </button>
        </div>
      </aside>
    </div>,
    document.body,
  );
}
