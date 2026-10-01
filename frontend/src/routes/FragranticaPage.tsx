import { useEffect, useMemo, useRef, useState } from "react";
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ExternalLink, Loader2, Pause, Play, RefreshCw, Send, X } from "lucide-react";
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

// ─── Типы ответов /api/fragrantica/* ────────────────────────────────────────

type Gender = "male" | "female" | "unisex" | "";
type Note = { name: string; icon?: string };
type Accord = { name: string; share?: number; color?: string; background?: string };
type ListItem = {
  id: number; url: string; brand: string; name: string; gender: Gender; year: number | null; votes: number | null;
  thumb: string; hasDetail: boolean; accords: Accord[]; exported: Array<{ offerId: string; status: string; account: string; volume: number | null }>;
};
type ListResponse = { items: ListItem[]; hasMore: boolean; total: number; page: number };
type CrawlerResponse = {
  stats: { brands: number; brandsCrawled: number; perfumes: number; details: number; exports: number };
  paused: boolean;
  indexPages: number | null;
  status: { lastError?: string | null; blockedUntil?: string | null; lastStepAt?: string | null; enabled?: boolean };
};
type Perfume = {
  id: number; url: string; brand: string; name: string; gender: Gender; year: number | null; votes: number | null; rating: number | null;
  family: string; perfumers: string[]; notes: { top: Note[]; middle: Note[]; base: Note[]; flat: Note[] }; accords: Accord[];
  description: string; image: string;
};
type DictValue = { dictionary_value_id?: number; value: string };
type FormAttribute = {
  id: number; name: string; description: string; type: string; required: boolean; collection: boolean;
  dictionaryId: number; maxValues: number; group: string; values: DictValue[];
};
type ExportRow = {
  id: number; offerId: string; accountName: string; volume: number | null; tester: boolean; status: string;
  productId: number | null; error: string | null; nextAttemptAt?: string | null; createdAt?: string;
  result?: { barcode?: string; barcodeError?: string; warnings?: string | null } | null;
};
type OzonForm = {
  perfume: { id: number; brand: string; name: string };
  accounts: Array<{ id: string; name: string; style: string }>;
  account: { id: string; name: string; style: string };
  types: Array<{ key: string; typeId: number; label: string; nameLabel: string }>;
  typeKey: string; typeId: number; volume: string; tester: boolean; offerId: string; name: string; vat: string;
  dims: { depth: number; width: number; height: number; weight: number };
  attributes: FormAttribute[];
  brandMatched: boolean;
  brandCandidates: Array<{ id: number; value: string }>;
  sourceImage: string;
  exports: ExportRow[];
};
type ImageJob = { status: "running" | "done" | "failed"; stage?: string; error?: string | null; result?: { main: string | null; notes: string | null; source: string; warnings: string[] } | null };

const GENDER_LABEL: Record<string, string> = { male: "мужской", female: "женский", unisex: "унисекс" };
const VOLUMES = [2, 5, 10, 30, 50, 75, 90, 100, 125, 200];
const VAT_OPTIONS = [["0", "Без НДС"], ["0.05", "5%"], ["0.07", "7%"], ["0.1", "10%"], ["0.2", "20%"], ["0.22", "22%"]];
// Атрибуты, которые форма заполняет сама из полей выше
const AUTO_ATTRS = new Set([4180, 9024, 8229, 4497]);
const TEXTAREA_ATTRS = new Set([4191, 8050, 11254]);
const STATUS_LABEL: Record<string, string> = {
  new: "создаётся", pending: "Ozon обрабатывает", imported: "создана", failed: "ошибка", queued_limit: "ждёт лимита Ozon",
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

// ─── Страница ───────────────────────────────────────────────────────────────

export function FragranticaPage() {
  const [q, setQ] = useState("");
  const [gender, setGender] = useState("");
  const [yearFrom, setYearFrom] = useState("");
  const [yearTo, setYearTo] = useState("");
  const [sort, setSort] = useState("popular");
  const [exported, setExported] = useState("");
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
    return p.toString();
  }, [debouncedQ, gender, yearFrom, yearTo, sort, exported]);

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

  return (
    <div className="page-shell">
      <PageHeader
        title="Фрагрантика"
        subtitle="Каталог ароматов fragrantica.ru — карточка на Ozon одним нажатием"
        action={
          <div className="fr-status">
            {stats ? (
              <>
                <span>
                  <i className={`fr-dot${crawler.data?.paused ? " is-paused" : blocked ? " is-blocked" : ""}`} />
                  Ароматов: <b>{stats.perfumes.toLocaleString("ru")}</b>
                </span>
                <span>с деталями: <b>{stats.details.toLocaleString("ru")}</b></span>
                <span>брендов обойдено: <b>{stats.brandsCrawled.toLocaleString("ru")}</b> из {stats.brands.toLocaleString("ru")}</span>
                <button
                  className="secondary-action compact"
                  type="button"
                  disabled={toggleCrawler.isPending}
                  onClick={() => toggleCrawler.mutate(!crawler.data?.paused)}
                  title={crawler.data?.paused ? "Продолжить фоновую загрузку каталога" : "Приостановить фоновую загрузку каталога"}
                >
                  {crawler.data?.paused ? <Play size={14} /> : <Pause size={14} />}
                  {crawler.data?.paused ? "Продолжить" : "Пауза"}
                </button>
              </>
            ) : null}
            {blocked && crawler.data?.status?.lastError ? (
              <span title={crawler.data.status.lastError}>Фрагрантика не отвечает — повтор {formatDateTime(crawler.data.status.blockedUntil)}</span>
            ) : null}
          </div>
        }
      />

      <div className="fr-toolbar">
        <input className="fr-search" placeholder="Бренд или название: dior sauvage" value={q} onChange={(e) => setQ(e.target.value)} />
        <select value={gender} onChange={(e) => setGender(e.target.value)} aria-label="Пол">
          <option value="">Любой пол</option>
          <option value="female">Женские</option>
          <option value="male">Мужские</option>
          <option value="unisex">Унисекс</option>
        </select>
        <input className="fr-year" inputMode="numeric" placeholder="Год от" value={yearFrom} onChange={(e) => setYearFrom(e.target.value.replace(/\D/g, ""))} />
        <input className="fr-year" inputMode="numeric" placeholder="Год до" value={yearTo} onChange={(e) => setYearTo(e.target.value.replace(/\D/g, ""))} />
        <select value={sort} onChange={(e) => setSort(e.target.value)} aria-label="Сортировка">
          <option value="popular">Популярные</option>
          <option value="new">Новинки</option>
          <option value="name">По алфавиту</option>
        </select>
        <select value={exported} onChange={(e) => setExported(e.target.value)} aria-label="Добавленные">
          <option value="">Все</option>
          <option value="no">Ещё не добавлены</option>
          <option value="yes">Уже добавлены</option>
        </select>
        <form
          className="fr-import"
          onSubmit={(e) => {
            e.preventDefault();
            if (importUrl.trim()) importByUrl.mutate(importUrl.trim());
          }}
        >
          <input placeholder="Ссылка на аромат fragrantica.ru" value={importUrl} onChange={(e) => setImportUrl(e.target.value)} />
          <button className="secondary-action" type="submit" disabled={importByUrl.isPending || !importUrl.trim()}>
            {importByUrl.isPending ? <Loader2 size={14} className="spin" /> : null}Открыть
          </button>
        </form>
      </div>

      <div className="muted-note">
        {list.isLoading ? "Загрузка…" : `Найдено ${total.toLocaleString("ru")} ${plural(total, "аромат", "аромата", "ароматов")}`}
        {stats && stats.perfumes === 0 ? " — каталог ещё загружается в фоне, а любой аромат можно открыть по ссылке." : ""}
      </div>
      {list.isError ? <div className="inline-error">{errorMessage(list.error)}</div> : null}

      <div className="fr-grid" style={{ marginTop: 12 }}>
        {items.map((item) => (
          <button key={item.id} type="button" className="fr-card" onClick={() => setOpenId(item.id)}>
            {item.exported.length ? <span className="fr-added">На Ozon</span> : null}
            <div className="fr-card-img">
              <img src={item.thumb} alt="" loading="lazy" />
            </div>
            <div className="fr-card-brand">{item.brand}</div>
            <div className="fr-card-name">{item.name}</div>
            <div className="fr-card-meta">
              {item.year ? <span>{item.year}</span> : null}
              {item.gender ? <span>· {GENDER_LABEL[item.gender]}</span> : null}
              {item.votes ? <span>· {item.votes.toLocaleString("ru")} голосов</span> : null}
            </div>
            {item.accords.length ? (
              <div className="fr-card-meta">
                {item.accords.slice(0, 3).map((a) => (
                  <span key={a.name} className="fr-chip is-accord" style={{ background: a.background, color: a.color }}>{a.name}</span>
                ))}
              </div>
            ) : null}
          </button>
        ))}
      </div>
      {!list.isLoading && !items.length ? <div className="fr-empty">Ничего не найдено. Попробуйте другой запрос или вставьте ссылку на аромат.</div> : null}
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

function NotesRow({ title, notes }: { title: string; notes: Note[] }) {
  if (!notes.length) return null;
  return (
    <>
      <div className="fr-section-title">{title}</div>
      <div className="fr-notes-row">
        {notes.map((note) => (
          <div key={note.name} className="fr-note">
            {note.icon ? <img src={note.icon} alt="" loading="lazy" /> : null}
            {note.name}
          </div>
        ))}
      </div>
    </>
  );
}

function PerfumeDrawer({ id, onClose }: { id: number; onClose: () => void }) {
  const [adding, setAdding] = useState(false);
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
        <div className="fr-drawer-head">
          <div>
            <h2>{p ? `${p.brand} — ${p.name}` : "Загрузка…"}</h2>
            {p ? (
              <p>
                {[p.year, p.gender ? GENDER_LABEL[p.gender] : "", p.family, p.perfumers.length ? `парфюмер: ${p.perfumers.join(", ")}` : "", p.rating ? `★ ${p.rating} (${(p.votes || 0).toLocaleString("ru")})` : ""].filter(Boolean).join(" · ")}
                {" · "}
                <a href={p.url} target="_blank" rel="noreferrer">Фрагрантика <ExternalLink size={12} /></a>
              </p>
            ) : null}
          </div>
          <button className="icon-action" type="button" onClick={onClose} aria-label="Закрыть"><X size={18} /></button>
        </div>

        {perfume.isLoading ? <div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем страницу аромата…</div> : null}
        {perfume.isError ? <div className="inline-error">{errorMessage(perfume.error)}</div> : null}

        {p ? (
          <>
            <div className="fr-detail">
              <div className="fr-detail-img"><img src={p.image} alt={`${p.brand} ${p.name}`} /></div>
              <div>
                <NotesRow title="Верхние ноты" notes={p.notes.top} />
                <NotesRow title="Ноты сердца" notes={p.notes.middle} />
                <NotesRow title="Базовые ноты" notes={p.notes.base} />
                <NotesRow title="Ноты" notes={p.notes.flat} />
                {p.accords.length ? (
                  <>
                    <div className="fr-section-title">Аккорды</div>
                    <div className="fr-accords">
                      {p.accords.slice(0, 8).map((a) => (
                        <div key={a.name} className="fr-accord" style={{ width: `${Math.max(30, a.share || 50)}%`, background: a.background, color: a.color }}>{a.name}</div>
                      ))}
                    </div>
                  </>
                ) : null}
              </div>
            </div>
            {p.description ? (
              <>
                <div className="fr-section-title">Описание</div>
                <div className="fr-description">{p.description}</div>
              </>
            ) : null}
            {adding ? (
              <OzonExportForm perfume={p} onCancel={() => setAdding(false)} />
            ) : (
              <div className="fr-actions">
                <button className="primary-action" type="button" onClick={() => setAdding(true)}><Send size={15} /> Добавить на Ozon</button>
              </div>
            )}
          </>
        ) : null}
      </aside>
    </div>
  );
}

// ─── Форма Ozon ─────────────────────────────────────────────────────────────

function OzonExportForm({ perfume, onCancel }: { perfume: Perfume; onCancel: () => void }) {
  const [accountId, setAccountId] = useState("");
  const [typeKey, setTypeKey] = useState("");
  const [volume, setVolume] = useState("");
  const [tester, setTester] = useState(false);
  const [step, setStep] = useState<"params" | "card">("params");
  // Шаг 1 — параметры; форма с атрибутами загружается под них (тип = категория атрибутов Ozon)
  const options = useQuery({
    queryKey: ["fragrantica", "ozon-options", perfume.id, accountId],
    queryFn: () => apiJson<OzonForm>(`/api/fragrantica/ozon/form?perfumeId=${perfume.id}${accountId ? `&accountId=${encodeURIComponent(accountId)}` : ""}`),
    staleTime: 5 * 60_000,
  });
  useEffect(() => {
    if (!options.data) return;
    if (!accountId) setAccountId(options.data.account.id);
    if (!typeKey) setTypeKey(options.data.typeKey);
  }, [options.data, accountId, typeKey]);

  const ready = Number(volume.replace(",", ".")) > 0 && typeKey && accountId;
  return (
    <section className="fr-form">
      <h3>Добавить на Ozon</h3>
      <div className="muted-note">Остаток будет 0 — дальше цена и наличие идут по обычной схеме через привязку к поставщику.</div>
      {options.isError ? <div className="inline-error">{errorMessage(options.error)}</div> : null}
      {step === "params" ? (
        <>
          <div className="fr-form-grid">
            <div className="fr-field">
              <span>Кабинет Ozon</span>
              <select value={accountId} onChange={(e) => setAccountId(e.target.value)}>
                {(options.data?.accounts || []).map((a) => <option key={a.id} value={a.id}>{a.name}</option>)}
              </select>
            </div>
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
            <label className="fr-field" style={{ justifyContent: "flex-end" }}>
              <span><input type="checkbox" checked={tester} onChange={(e) => setTester(e.target.checked)} style={{ width: "auto", marginRight: 6 }} />Тестер</span>
            </label>
          </div>
          <div className="fr-volumes" style={{ marginTop: 10 }}>
            {VOLUMES.map((v) => (
              <button key={v} type="button" className={Number(volume) === v ? "is-active" : ""} onClick={() => setVolume(String(v))}>{v} мл</button>
            ))}
          </div>
          <div className="fr-actions">
            <button className="primary-action" type="button" disabled={!ready || options.isLoading} onClick={() => setStep("card")}>
              {options.isLoading ? <Loader2 size={14} className="spin" /> : null}Заполнить карточку
            </button>
            <button className="secondary-action" type="button" onClick={onCancel}>Отмена</button>
          </div>
          {options.data?.exports.length ? <ExportHistory rows={options.data.exports} /> : null}
        </>
      ) : (
        <OzonCardStep
          perfume={perfume}
          accountId={accountId}
          typeKey={typeKey}
          volume={volume.replace(",", ".")}
          tester={tester}
          onBack={() => setStep("params")}
        />
      )}
    </section>
  );
}

function ExportHistory({ rows }: { rows: ExportRow[] }) {
  return (
    <>
      <div className="fr-section-title">Уже создавали</div>
      <div className="fr-history">
        {rows.map((row) => (
          <div key={row.id} className="fr-history-row">
            <span className="fr-chip">{row.offerId}</span>
            <span>{row.accountName}</span>
            {row.volume ? <span>{row.volume} мл{row.tester ? " · тестер" : ""}</span> : null}
            <b>{STATUS_LABEL[row.status] || row.status}</b>
            {row.productId ? <span>product_id {row.productId}</span> : null}
            {row.error ? <span className="fr-hint">{row.error}</span> : null}
          </div>
        ))}
      </div>
    </>
  );
}

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
            {v.value} ×
          </button>
        ))}
      </div>
      <input
        value={query}
        placeholder={value.length >= max ? "" : "Поиск в справочнике Ozon…"}
        onFocus={() => setOpen(true)}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
      />
      {open ? (
        <div className="fr-dict-results">
          {results.isLoading ? <div className="fr-hint" style={{ padding: 8 }}>Ищем…</div> : null}
          {(results.data?.items || []).map((item) => (
            <button key={item.id} type="button" onClick={() => pick(item)}>{item.value}</button>
          ))}
          {!results.isLoading && !(results.data?.items || []).length ? (
            <div className="fr-hint" style={{ padding: 8 }}>{debounced.trim().length < 2 ? "Введите хотя бы 2 буквы" : "Ничего не найдено"}</div>
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

function OzonCardStep({ perfume, accountId, typeKey, volume, tester, onBack }: {
  perfume: Perfume; accountId: string; typeKey: string; volume: string; tester: boolean; onBack: () => void;
}) {
  const queryClient = useQueryClient();
  const form = useQuery({
    queryKey: ["fragrantica", "ozon-form", perfume.id, accountId, typeKey, volume, tester],
    queryFn: () => apiJson<OzonForm>(`/api/fragrantica/ozon/form?perfumeId=${perfume.id}&accountId=${encodeURIComponent(accountId)}&typeKey=${typeKey}&volume=${encodeURIComponent(volume)}&tester=${tester ? 1 : 0}`),
    staleTime: Infinity,
  });
  const [name, setName] = useState("");
  const [offerId, setOfferId] = useState("");
  const [price, setPrice] = useState("");
  const [oldPrice, setOldPrice] = useState("");
  const [vat, setVat] = useState("0.05");
  const [dims, setDims] = useState({ depth: "", width: "", height: "", weight: "" });
  const [values, setValues] = useState<Record<number, DictValue[]>>({});
  const [useNotesCard, setUseNotesCard] = useState(true);
  const [showOptional, setShowOptional] = useState(false);
  const [missing, setMissing] = useState<string[]>([]);
  const [exportId, setExportId] = useState<number | null>(null);

  useEffect(() => {
    const data = form.data;
    if (!data) return;
    setName(data.name);
    setOfferId(data.offerId);
    setVat(data.vat);
    setDims({ depth: String(data.dims.depth), width: String(data.dims.width), height: String(data.dims.height), weight: String(data.dims.weight) });
    setValues(Object.fromEntries(data.attributes.map((a) => [a.id, a.values])));
  }, [form.data]);

  // Фото: флакон с премиум-фоном + «Пирамида аромата» (parfumdeclaration)
  const [jobId, setJobId] = useState<string | null>(null);
  const startImages = useMutation({
    mutationFn: (refresh: boolean) => apiJson<{ jobId: string }>("/api/fragrantica/ozon/images", mutationBody({ perfumeId: perfume.id, accountId, refresh })),
    onSuccess: (data) => setJobId(data.jobId),
    onError: (error) => toast.error(errorMessage(error)),
  });
  const startImagesRef = useRef(startImages.mutate);
  startImagesRef.current = startImages.mutate;
  useEffect(() => { startImagesRef.current(false); }, [perfume.id, accountId]);
  const images = useQuery({
    queryKey: ["fragrantica", "images", jobId],
    queryFn: () => apiJson<ImageJob>(`/api/fragrantica/ozon/images/${jobId}`),
    enabled: Boolean(jobId),
    refetchInterval: (query) => (query.state.data?.status === "running" ? 2500 : false),
  });
  const imageResult = images.data?.status === "done" ? images.data.result : null;
  const imagesBusy = startImages.isPending || images.data?.status === "running" || (Boolean(jobId) && !images.data);

  const submit = useMutation({
    mutationFn: () => {
      const data = form.data!;
      const photoList = [imageResult?.main || data.sourceImage, useNotesCard ? imageResult?.notes : null].filter(Boolean);
      const attributes = data.attributes
        .filter((a) => !AUTO_ATTRS.has(a.id))
        .map((a) => ({ id: a.id, values: values[a.id] || [] }))
        .filter((a) => a.values.length);
      attributes.push({ id: 4180, values: [{ value: name }] });
      attributes.push({ id: 9024, values: [{ value: offerId }] });
      attributes.push({ id: 8229, values: [{ dictionary_value_id: data.typeId, value: data.types.find((t) => t.key === data.typeKey)?.label || "" }] });
      if (Number(dims.weight)) attributes.push({ id: 4497, values: [{ value: dims.weight }] });
      return apiJson<{ export: ExportRow }>("/api/fragrantica/ozon/export", mutationBody({
        perfumeId: perfume.id, accountId, typeId: data.typeId, typeKey: data.typeKey, offerId, name, price, oldPrice, vat,
        depth: dims.depth, width: dims.width, height: dims.height, weight: dims.weight, tester, images: photoList, attributes,
      }));
    },
    onSuccess: (data) => {
      setMissing([]);
      setExportId(data.export.id);
      queryClient.invalidateQueries({ queryKey: ["fragrantica", "catalog"] });
    },
    onError: (error) => {
      const detail = (error as { detail?: { missing?: string[] } }).detail;
      if (detail?.missing) setMissing(detail.missing);
      toast.error(errorMessage(error));
    },
  });

  const status = useQuery({
    queryKey: ["fragrantica", "export", exportId],
    queryFn: () => apiJson<{ export: ExportRow }>(`/api/fragrantica/ozon/exports/${exportId}`).then((r) => r.export),
    enabled: Boolean(exportId),
    refetchInterval: (query) => {
      const s = query.state.data?.status;
      return s === "pending" || s === "new" || (s === "imported" && query.state.data?.result?.barcode === "pending") ? 3000 : false;
    },
  });

  if (form.isLoading) return <div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем характеристики Ozon…</div>;
  if (form.isError || !form.data) return <div className="inline-error">{errorMessage(form.error)} <button className="secondary-action compact" type="button" onClick={onBack}>Назад</button></div>;
  const data = form.data;
  const required = data.attributes.filter((a) => a.required && !AUTO_ATTRS.has(a.id));
  const optional = data.attributes.filter((a) => !a.required && !AUTO_ATTRS.has(a.id));
  const description = data.attributes.find((a) => a.id === 4191);
  const isMissing = (label: string) => missing.includes(label);
  const result = status.data;

  const renderAttr = (attr: FormAttribute) => (
    <div key={attr.id} className={`fr-field${TEXTAREA_ATTRS.has(attr.id) ? " is-wide" : ""}${isMissing(attr.name) ? " is-missing" : ""}`}>
      <span title={attr.description}>{attr.name}{attr.required ? <span className="fr-req">*</span> : null}</span>
      <AttributeInput attr={attr} value={values[attr.id] || []} onChange={(next) => setValues((prev) => ({ ...prev, [attr.id]: next }))} accountId={accountId} typeId={data.typeId} />
    </div>
  );

  return (
    <>
      <div className="fr-section-title">Фото</div>
      <div className="fr-photos">
        <div className="fr-photo">
          <div className="fr-photo-frame">
            {imageResult?.main ? <img src={imageResult.main} alt="Фото флакона" /> : imagesBusy ? <span><Loader2 size={16} className="spin" /><br />Премиум фон…</span> : <img src={data.sourceImage} alt="Исходное фото" />}
          </div>
          1. Флакон {imageResult?.main && imageResult.main !== imageResult.source ? "(премиум фон)" : ""}
        </div>
        <div className={`fr-photo${useNotesCard ? "" : " is-off"}`}>
          <div className="fr-photo-frame">
            {imageResult?.notes ? <img src={imageResult.notes} alt="Пирамида аромата" /> : imagesBusy ? <span><Loader2 size={16} className="spin" /><br />Рисуем ноты…</span> : <span>Нет картинки</span>}
          </div>
          <label><input type="checkbox" checked={useNotesCard} onChange={(e) => setUseNotesCard(e.target.checked)} /> 2. Пирамида аромата</label>
        </div>
        <div className="fr-photo fr-photo-auto">
          <div className="fr-photo-frame">«Секреты нанесения» и «Спасибо» добавит parfumdeclaration сам в течение часа</div>
          3–4. Фото в конце
        </div>
      </div>
      <div className="fr-actions" style={{ marginTop: 8 }}>
        <button className="secondary-action compact" type="button" disabled={imagesBusy} onClick={() => startImages.mutate(true)}>
          <RefreshCw size={13} /> Обработать фото заново
        </button>
      </div>
      {images.data?.status === "failed" ? <div className="fr-warn">{images.data.error}</div> : null}
      {(imageResult?.warnings || []).map((w) => <div key={w} className="fr-warn">{w}</div>)}

      <div className="fr-section-title">Основное</div>
      <div className="fr-form-grid">
        <div className={`fr-field is-wide${isMissing("Название") ? " is-missing" : ""}`}>
          <span>Название<span className="fr-req">*</span></span>
          <input value={name} onChange={(e) => setName(e.target.value)} />
        </div>
        <div className={`fr-field${isMissing("Артикул") ? " is-missing" : ""}`}>
          <span>Артикул<span className="fr-req">*</span></span>
          <input value={offerId} onChange={(e) => setOfferId(e.target.value)} />
          <span className="fr-hint">Если занят — добавим -2, -3…</span>
        </div>
        <div className={`fr-field${isMissing("Цена") ? " is-missing" : ""}`}>
          <span>Цена, ₽<span className="fr-req">*</span></span>
          <input inputMode="numeric" value={price} onChange={(e) => setPrice(e.target.value.replace(/[^\d]/g, ""))} />
        </div>
        <div className="fr-field">
          <span>Цена до скидки, ₽</span>
          <input inputMode="numeric" value={oldPrice} onChange={(e) => setOldPrice(e.target.value.replace(/[^\d]/g, ""))} />
        </div>
        <div className="fr-field">
          <span>НДС</span>
          <select value={vat} onChange={(e) => setVat(e.target.value)}>
            {VAT_OPTIONS.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
          </select>
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
      </div>
      {!data.brandMatched ? (
        <div className="fr-warn">
          Бренд «{perfume.brand}» не нашёлся в справочнике Ozon дословно — выберите его в поле «Бренд»
          {data.brandCandidates.length ? `: похожие — ${data.brandCandidates.slice(0, 5).map((c) => c.value).join(", ")}` : ""}.
        </div>
      ) : null}

      {description ? (
        <>
          <div className="fr-section-title">Описание (аннотация)</div>
          {renderAttr(description)}
        </>
      ) : null}

      <div className="fr-section-title">Обязательные характеристики Ozon</div>
      <div className="fr-form-grid">{required.map(renderAttr)}</div>

      <button className="fr-collapse" type="button" onClick={() => setShowOptional((v) => !v)}>
        {showOptional ? "Скрыть" : "Показать"} остальные характеристики ({optional.length - (description ? 1 : 0)})
      </button>
      {showOptional ? <div className="fr-form-grid">{optional.filter((a) => a.id !== 4191).map(renderAttr)}</div> : null}

      <div className="fr-actions">
        <button className="primary-action" type="button" disabled={submit.isPending || Boolean(exportId && result?.status !== "failed")} onClick={() => submit.mutate()}>
          {submit.isPending ? <Loader2 size={14} className="spin" /> : <Send size={14} />} Создать карточку на Ozon
        </button>
        <button className="secondary-action" type="button" onClick={onBack} disabled={submit.isPending}>Назад к параметрам</button>
        <span className="fr-hint">Кабинет: {data.account.name}</span>
      </div>
      {missing.length ? <div className="fr-warn">Не заполнено: {missing.join(", ")}</div> : null}

      {result ? <ExportResult row={result} /> : exportId ? <div className="fr-result is-wait"><Loader2 size={14} className="spin" /> Отправляем…</div> : null}
    </>
  );
}

function ExportResult({ row }: { row: ExportRow }) {
  if (row.status === "imported") {
    return (
      <div className="fr-result is-ok">
        Карточка создана: <b>{row.offerId}</b>{row.productId ? ` (product_id ${row.productId})` : ""}.
        {row.result?.barcode === "generated" ? " Штрихкод сгенерирован." : row.result?.barcode === "pending" ? " Штрихкод сгенерируем, как только Ozon позволит." : ""}
        {row.result?.warnings ? <div className="fr-hint">Замечания Ozon: {row.result.warnings}</div> : null}
        <div className="fr-hint">Фото в конце добавит parfumdeclaration в течение часа; дальше — привязка к поставщику на складе.</div>
      </div>
    );
  }
  if (row.status === "queued_limit") {
    return (
      <div className="fr-result is-wait">
        Дневной лимит Ozon на создание карточек исчерпан — карточка <b>{row.offerId}</b> в очереди и уйдёт автоматически
        {row.nextAttemptAt ? ` около ${formatDateTime(row.nextAttemptAt)}` : " после 03:00 МСК"}.
      </div>
    );
  }
  if (row.status === "failed") {
    return <div className="fr-result is-bad">Ozon не принял карточку <b>{row.offerId}</b>: {row.error}. Исправьте поля и отправьте ещё раз.</div>;
  }
  return <div className="fr-result is-wait"><Loader2 size={14} className="spin" /> {STATUS_LABEL[row.status] || row.status}: {row.offerId}…</div>;
}
