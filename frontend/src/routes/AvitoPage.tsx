import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckSquare, ClipboardCopy, Eye, Link2, Loader2, PackageCheck, RefreshCw, Save, Send, Trash2, Upload } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

type FeedDefaults = {
  category: string;
  goodsType: string;
  adType: string;
  condition: string;
  address: string;
  description: string;
};

type ImportRules = {
  enabled: boolean;
  minVolumeMl: number;
  skipWithoutVolume: boolean;
  excludeTitleWords: string[];
  includeTitleWords: string[];
  excludeBrands: string[];
  includeBrands: string[];
  minPriceRub: number;
  maxPriceRub: number;
  requireImages: boolean;
  skipArchived: boolean;
  minStock: number;
  priceCoefficient: number;
  priceRules: Array<{ minPriceRub: number; coefficient: number }>;
  autoUpdatePrices: boolean;
  hideOutOfStock: boolean;
  maxItems: number;
  feedDefaults: FeedDefaults;
};

type MatchedListing = {
  adId: string;
  sourceOfferId: string;
  title: string;
  brand: string;
  volumeMl: number;
  priceRub: number;
  imageUrls: string[];
};

type SkippedItem = { id: string; offerId: string; name: string; reasons: string[] };

type PreviewResponse = {
  rules: ImportRules;
  totalOzonProducts: number;
  matchedCount: number;
  skippedCount: number;
  skippedByReason: Record<string, number>;
  matchedSample: MatchedListing[];
  skippedSample: SkippedItem[];
};

type ApplyResponse = {
  ok: boolean;
  totalOzonProducts: number;
  matchedCount: number;
  skippedCount: number;
  created: number;
  updated: number;
  totalListings: number;
};

type Listing = {
  adId: string;
  source: string;
  title: string;
  brand: string;
  volumeMl: number;
  priceRub: number;
  imageUrls: string[];
  enabled: boolean;
  outOfStock?: boolean;
  lastSyncedAt?: string | null;
  updatedAt: string;
};

type ListingsResponse = { updatedAt: string | null; total: number; items: Listing[] };
type FeedRefreshResult = {
  status: string;
  updatedPrices?: number;
  outOfStock?: number;
  total?: number;
  at?: string;
  error?: string;
};
type FeedInfoResponse = {
  feedUrl: string;
  enabledCount: number;
  totalListings: number;
  hiddenOutOfStock?: number;
  liveSource?: string;
  autoRefresh?: {
    enabled: boolean;
    intervalMinutes: number;
    nextRunAt: string | null;
    lastResult: FeedRefreshResult | null;
  };
};
type AvitoUpload = {
  upload_id: number;
  status: string;
  started_at?: string;
  source?: string;
  stats?: { count?: number };
  events?: Array<{ description?: string; type?: string }>;
};
type UploadsResponse = { uploads?: AvitoUpload[] };
type AccountsResponse = { accounts: Array<{ id: string; marketplace: string; name: string; configured: boolean }> };

const SKIP_REASON_LABELS: Record<string, string> = {
  title_word: "Стоп-слово в названии",
  title_not_in_include_list: "Нет обязательного слова",
  brand: "Бренд в чёрном списке",
  brand_not_in_include_list: "Бренд не в белом списке",
  volume_below_min: "Объём меньше порога",
  volume_unknown: "Объём не распознан",
  no_price: "Нет цены",
  price_below_min: "Цена ниже минимума",
  price_above_max: "Цена выше максимума",
  no_images: "Нет фото",
  archived: "В архиве",
  stock_below_min: "Мало остатков",
  no_title: "Нет названия",
};

function reasonLabel(reason: string): string {
  const key = reason.split(":")[0];
  const detail = reason.includes(":") ? ` (${reason.split(":").slice(1).join(":")})` : "";
  return `${SKIP_REASON_LABELS[key] || key}${detail}`;
}

const UPLOAD_STATUS_LABELS: Record<string, string> = {
  processing: "обрабатывается",
  success: "успешно",
  success_warning: "успешно, с предупреждениями",
  error: "ошибка",
  canceled: "отменена",
};

async function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...(init || {}),
    headers: { ...(init?.body ? { "Content-Type": "application/json" } : {}), ...(init?.headers || {}) },
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error((data as { error?: string })?.error || `HTTP ${response.status}`);
  return data as T;
}

function wordsToText(words: string[]): string {
  return (words || []).join(", ");
}

function textToWords(text: string): string[] {
  return text.split(/[\n,;]+/).map((word) => word.trim()).filter(Boolean);
}

function numberInput(value: string, fallback = 0): number {
  const parsed = Number(value.replace(",", "."));
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function AvitoPage() {
  const queryClient = useQueryClient();
  const [rules, setRules] = useState<ImportRules | null>(null);
  const [copied, setCopied] = useState(false);

  const accountsQuery = useQuery({
    queryKey: ["marketplace-accounts"],
    queryFn: () => apiJson<AccountsResponse>("/api/marketplace-accounts"),
  });
  const rulesQuery = useQuery({
    queryKey: ["avito-import-rules"],
    queryFn: () => apiJson<ImportRules>("/api/avito/import/rules"),
  });
  const listingsQuery = useQuery({
    queryKey: ["avito-listings"],
    queryFn: () => apiJson<ListingsResponse>("/api/avito/listings"),
  });
  const feedInfoQuery = useQuery({
    queryKey: ["avito-feed-info"],
    queryFn: () => apiJson<FeedInfoResponse>("/api/avito/feed-info"),
  });

  const avitoAccount = (accountsQuery.data?.accounts || []).find((account) => account.marketplace === "avito");
  const avitoConfigured = Boolean(avitoAccount?.configured);

  const uploadsQuery = useQuery({
    queryKey: ["avito-uploads"],
    queryFn: () => apiJson<UploadsResponse>("/api/avito/uploads?perPage=10"),
    enabled: avitoConfigured,
    retry: false,
  });

  useEffect(() => {
    if (rulesQuery.data && !rules) setRules(rulesQuery.data);
  }, [rulesQuery.data, rules]);

  const saveRules = useMutation({
    mutationFn: (next: ImportRules) => apiJson<ImportRules>("/api/avito/import/rules", { method: "PUT", body: JSON.stringify(next) }),
    onSuccess: (saved) => {
      setRules(saved);
      void queryClient.invalidateQueries({ queryKey: ["avito-import-rules"] });
    },
  });
  const preview = useMutation({
    mutationFn: (next: ImportRules) => apiJson<PreviewResponse>("/api/avito/import/preview", { method: "POST", body: JSON.stringify(next) }),
  });
  const applyImport = useMutation({
    mutationFn: (next: ImportRules) => apiJson<ApplyResponse>("/api/avito/import/apply", { method: "POST", body: JSON.stringify(next) }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["avito-listings"] });
      void queryClient.invalidateQueries({ queryKey: ["avito-feed-info"] });
    },
  });
  const removeListing = useMutation({
    mutationFn: (adId: string) => apiJson("/api/avito/listings", { method: "DELETE", body: JSON.stringify({ adIds: [adId] }) }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["avito-listings"] });
      void queryClient.invalidateQueries({ queryKey: ["avito-feed-info"] });
    },
  });
  const triggerUpload = useMutation({
    mutationFn: () => apiJson("/api/avito/upload", { method: "POST", body: JSON.stringify({}) }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["avito-uploads"] });
    },
  });
  const [adjustDirection, setAdjustDirection] = useState<"decrease" | "increase">("decrease");
  const [adjustPercent, setAdjustPercent] = useState(2);
  const adjustPrices = useMutation({
    mutationFn: () => apiJson<{ ok: boolean; rules: ImportRules }>("/api/avito/price/adjust-percent", {
      method: "POST",
      body: JSON.stringify({ direction: adjustDirection, percent: adjustPercent }),
    }),
    onSuccess: (data) => {
      if (data.rules) setRules(data.rules);
      void queryClient.invalidateQueries({ queryKey: ["avito-import-rules"] });
      void queryClient.invalidateQueries({ queryKey: ["avito-listings"] });
      void queryClient.invalidateQueries({ queryKey: ["avito-feed-info"] });
    },
  });
  const refreshFeed = useMutation({
    mutationFn: () => apiJson<FeedRefreshResult>("/api/avito/feed/refresh", { method: "POST", body: JSON.stringify({}) }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["avito-listings"] });
      void queryClient.invalidateQueries({ queryKey: ["avito-feed-info"] });
    },
  });

  const copyFeedUrl = async () => {
    const url = feedInfoQuery.data?.feedUrl;
    if (!url) return;
    try {
      await navigator.clipboard.writeText(url);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard недоступен — url виден в поле */
    }
  };

  const updateRules = (patch: Partial<ImportRules>) => {
    setRules((current) => (current ? { ...current, ...patch } : current));
  };
  const updateFeedDefaults = (patch: Partial<FeedDefaults>) => {
    setRules((current) => (current ? { ...current, feedDefaults: { ...current.feedDefaults, ...patch } } : current));
  };

  const listings = listingsQuery.data?.items || [];
  const previewData = preview.data;

  return (
    <section className="page-section import-page avito-page">
      <PageHeader
        title="Импорт на Avito"
        subtitle="Перенос товаров с Ozon в фид Автозагрузки Avito по гибким правилам: объём, стоп-слова, бренды, цены."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" disabled={!rules || preview.isPending} onClick={() => rules && preview.mutate(rules)}>
              {preview.isPending ? <Loader2 className="spin" size={16} /> : <Eye size={16} />} Предпросмотр
            </button>
            <button className="primary-action" type="button" disabled={!rules || applyImport.isPending} onClick={() => rules && applyImport.mutate(rules)}>
              {applyImport.isPending ? <Loader2 className="spin" size={16} /> : <Upload size={16} />} Импортировать в фид
            </button>
            <button className="secondary-action" type="button" disabled={!avitoConfigured || triggerUpload.isPending} onClick={() => triggerUpload.mutate()} title="Avito скачает фид и обновит объявления. Не чаще раза в час.">
              {triggerUpload.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Запустить автозагрузку
            </button>
          </div>
        )}
      />

      {!accountsQuery.isFetching && !avitoConfigured ? (
        <div className="info-strip warn">
          Кабинет Avito не настроен. Добавьте Client ID и Client Secret на вкладке «Кабинеты» в классическом интерфейсе — <a href="/">открыть настройки кабинетов</a>. Правила и фид можно готовить и без ключей.
        </div>
      ) : null}

      <section className="dashboard-metrics">
        <Stat label="Товаров Ozon" value={previewData?.totalOzonProducts ?? "—"} tone="accent" icon={<PackageCheck size={18} />} />
        <Stat label="Пройдут фильтр" value={previewData?.matchedCount ?? "—"} tone="success" icon={<CheckSquare size={18} />} />
        <Stat label="Будут пропущены" value={previewData?.skippedCount ?? "—"} tone="warn" icon={<Trash2 size={18} />} />
        <Stat label="Объявлений в фиде" value={feedInfoQuery.data?.enabledCount ?? listings.length} tone="accent" icon={<Link2 size={18} />} />
      </section>

      {applyImport.data ? (
        <div className="info-strip success">
          Импорт применён: новых {applyImport.data.created} · обновлено {applyImport.data.updated} · всего в фиде {applyImport.data.totalListings} · пропущено {applyImport.data.skippedCount}
        </div>
      ) : null}
      {applyImport.error ? <div className="inline-error">{String((applyImport.error as Error).message)}</div> : null}
      {preview.error ? <div className="inline-error">{String((preview.error as Error).message)}</div> : null}
      {triggerUpload.data ? <div className="info-strip success">Автозагрузка запущена — Avito скачает фид в течение нескольких минут.</div> : null}
      {triggerUpload.error ? <div className="inline-error">{String((triggerUpload.error as Error).message)}</div> : null}

      <div className="settings-grid">
        <section className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div><span>Правила импорта</span><h3>Фильтры Ozon → Avito</h3></div>
            <button className="secondary-action" type="button" disabled={!rules || saveRules.isPending} onClick={() => rules && saveRules.mutate(rules)}>
              {saveRules.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить правила
            </button>
          </div>
          {!rules ? (
            <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю правила…</div>
          ) : (
            <>
              <div className="settings-form-row">
                <label className="field-label">
                  Мин. объём, мл (0 = выкл)
                  <input type="number" min={0} value={String(rules.minVolumeMl)} onChange={(event) => updateRules({ minVolumeMl: numberInput(event.target.value) })} />
                </label>
                <label className="field-label">
                  Коэффициент цены Avito
                  <input type="number" min={0.01} step={0.01} value={String(rules.priceCoefficient)} onChange={(event) => updateRules({ priceCoefficient: numberInput(event.target.value, 1) })} />
                </label>
                <label className="field-label">
                  Мин. цена, ₽ (0 = выкл)
                  <input type="number" min={0} value={String(rules.minPriceRub)} onChange={(event) => updateRules({ minPriceRub: numberInput(event.target.value) })} />
                </label>
                <label className="field-label">
                  Макс. цена, ₽ (0 = выкл)
                  <input type="number" min={0} value={String(rules.maxPriceRub)} onChange={(event) => updateRules({ maxPriceRub: numberInput(event.target.value) })} />
                </label>
                <label className="field-label">
                  Мин. остаток (0 = выкл)
                  <input type="number" min={0} value={String(rules.minStock)} onChange={(event) => updateRules({ minStock: numberInput(event.target.value) })} />
                </label>
                <label className="field-label">
                  Лимит объявлений (0 = все)
                  <input type="number" min={0} value={String(rules.maxItems)} onChange={(event) => updateRules({ maxItems: numberInput(event.target.value) })} />
                </label>
              </div>
              <div className="settings-form-row">
                <label className="settings-toggle">
                  <input type="checkbox" checked={rules.skipWithoutVolume} onChange={(event) => updateRules({ skipWithoutVolume: event.target.checked })} />
                  Пропускать без распознанного объёма
                </label>
                <label className="settings-toggle">
                  <input type="checkbox" checked={rules.requireImages} onChange={(event) => updateRules({ requireImages: event.target.checked })} />
                  Только с фото
                </label>
                <label className="settings-toggle">
                  <input type="checkbox" checked={rules.skipArchived} onChange={(event) => updateRules({ skipArchived: event.target.checked })} />
                  Пропускать архивные
                </label>
                <label className="settings-toggle" title="При каждом скачивании фида Avito получает актуальную цену склада × коэффициент">
                  <input type="checkbox" checked={rules.autoUpdatePrices} onChange={(event) => updateRules({ autoUpdatePrices: event.target.checked })} />
                  Автообновлять цены в фиде
                </label>
                <label className="settings-toggle" title="Товары без остатков или в архиве исключаются из фида — Avito снимет объявление">
                  <input type="checkbox" checked={rules.hideOutOfStock} onChange={(event) => updateRules({ hideOutOfStock: event.target.checked })} />
                  Скрывать без остатков
                </label>
              </div>
              <div className="section-title compact-title">
                <div><span>Цена</span><h3>Гибкие правила наценки Avito</h3></div>
                <button className="secondary-action" type="button" onClick={() => updateRules({ priceRules: [...rules.priceRules, { minPriceRub: 0, coefficient: rules.priceCoefficient || 1 }] })}>
                  Добавить правило
                </button>
              </div>
              <p className="form-hint">Коэффициент к рублёвой цене Ozon: берётся правило с наибольшим подходящим порогом «от цены». Если правил нет — действует базовый коэффициент выше.</p>
              <div className="avito-price-rules">
                <div className="avito-price-rule-head"><span>От цены, ₽</span><span>Коэффициент</span><span></span></div>
                {rules.priceRules
                  .map((rule, index) => ({ rule, index }))
                  .sort((a, b) => a.rule.minPriceRub - b.rule.minPriceRub || a.index - b.index)
                  .map(({ rule, index }) => (
                    <div className="avito-price-rule-row" key={`price-rule-${index}`}>
                      <input type="number" min={0} value={String(rule.minPriceRub)} onChange={(event) => updateRules({ priceRules: rules.priceRules.map((item, itemIndex) => (itemIndex === index ? { ...item, minPriceRub: numberInput(event.target.value) } : item)) })} />
                      <input type="number" min={0.01} step={0.01} value={String(rule.coefficient)} onChange={(event) => updateRules({ priceRules: rules.priceRules.map((item, itemIndex) => (itemIndex === index ? { ...item, coefficient: numberInput(event.target.value, 1) } : item)) })} />
                      <button className="icon-action danger" type="button" title="Удалить правило" onClick={() => updateRules({ priceRules: rules.priceRules.filter((_, itemIndex) => itemIndex !== index) })}>×</button>
                    </div>
                  ))}
                {!rules.priceRules.length ? <div className="form-hint">Правил нет — используется базовый коэффициент {rules.priceCoefficient}.</div> : null}
              </div>
              <div className="avito-price-adjust">
                <strong>Быстрая корректировка цен</strong>
                <div className="avito-price-adjust-row">
                  <select value={adjustDirection} onChange={(event) => setAdjustDirection(event.target.value as "decrease" | "increase")}>
                    <option value="decrease">Снизить</option>
                    <option value="increase">Поднять</option>
                  </select>
                  <input type="number" min={0.01} max={90} step={0.01} value={String(adjustPercent)} onChange={(event) => setAdjustPercent(numberInput(event.target.value, 0))} />
                  <span>%</span>
                  <button className="primary-action" type="button" disabled={adjustPrices.isPending || !(adjustPercent > 0)} onClick={() => adjustPrices.mutate()}>
                    {adjustPrices.isPending ? <Loader2 className="spin" size={15} /> : null} Применить
                  </button>
                </div>
                <small className="form-hint">Меняет базовый коэффициент и все правила на указанный процент и сразу обновляет цены в фиде.</small>
                {adjustPrices.isSuccess ? <div className="info-strip success compact">Коэффициенты обновлены, цены в фиде пересчитаны.</div> : null}
                {adjustPrices.error ? <div className="inline-error">{String((adjustPrices.error as Error).message)}</div> : null}
              </div>
              <div className="settings-form-row">
                <label className="field-label field-label-wide">
                  Стоп-слова в названии (через запятую)
                  <textarea rows={2} value={wordsToText(rules.excludeTitleWords)} onChange={(event) => updateRules({ excludeTitleWords: textToWords(event.target.value) })} placeholder="дубль, удаленый, удаленный" />
                </label>
                <label className="field-label field-label-wide">
                  Обязательные слова (пусто = все)
                  <textarea rows={2} value={wordsToText(rules.includeTitleWords)} onChange={(event) => updateRules({ includeTitleWords: textToWords(event.target.value) })} placeholder="например: туалетная вода" />
                </label>
              </div>
              <div className="settings-form-row">
                <label className="field-label field-label-wide">
                  Исключить бренды
                  <textarea rows={2} value={wordsToText(rules.excludeBrands)} onChange={(event) => updateRules({ excludeBrands: textToWords(event.target.value) })} />
                </label>
                <label className="field-label field-label-wide">
                  Только бренды (пусто = все)
                  <textarea rows={2} value={wordsToText(rules.includeBrands)} onChange={(event) => updateRules({ includeBrands: textToWords(event.target.value) })} />
                </label>
              </div>
              <div className="section-title compact-title"><div><span>Фид</span><h3>Значения по умолчанию для объявлений</h3></div></div>
              <div className="settings-form-row">
                <label className="field-label">
                  Категория
                  <input value={rules.feedDefaults.category} onChange={(event) => updateFeedDefaults({ category: event.target.value })} />
                </label>
                <label className="field-label">
                  Вид товара (GoodsType)
                  <input value={rules.feedDefaults.goodsType} onChange={(event) => updateFeedDefaults({ goodsType: event.target.value })} />
                </label>
                <label className="field-label">
                  Вид объявления (AdType)
                  <input value={rules.feedDefaults.adType} onChange={(event) => updateFeedDefaults({ adType: event.target.value })} />
                </label>
                <label className="field-label">
                  Состояние
                  <input value={rules.feedDefaults.condition} onChange={(event) => updateFeedDefaults({ condition: event.target.value })} />
                </label>
              </div>
              <div className="settings-form-row">
                <label className="field-label field-label-wide">
                  Адрес (обязателен для публикации на Avito)
                  <input value={rules.feedDefaults.address} onChange={(event) => updateFeedDefaults({ address: event.target.value })} placeholder="Москва, ул. Примерная, 1" />
                </label>
                <label className="field-label field-label-wide">
                  Шаблон описания ({"{title}"} и {"{brand}"} подставятся)
                  <textarea rows={3} value={rules.feedDefaults.description} onChange={(event) => updateFeedDefaults({ description: event.target.value })} placeholder="{title} — оригинальная продукция. Быстрая отправка." />
                </label>
              </div>
            </>
          )}
        </section>

        <section className="settings-panel">
          <div className="section-title"><div><span>Фид Автозагрузки</span><h3>Ссылка для Avito</h3></div></div>
          <p className="form-hint">Укажите эту ссылку в профиле автозагрузки Avito (или сохраните через API-профиль). Avito будет скачивать XML по расписанию.</p>
          {feedInfoQuery.data ? (
            <>
              <div className="feed-url-row">
                <input readOnly value={feedInfoQuery.data.feedUrl} onFocus={(event) => event.target.select()} />
                <button className="secondary-action" type="button" onClick={copyFeedUrl}>
                  <ClipboardCopy size={16} /> {copied ? "Скопировано" : "Копировать"}
                </button>
              </div>
              <p className="form-hint">
                В фиде: {feedInfoQuery.data.enabledCount} активных из {feedInfoQuery.data.totalListings} объявлений
                {feedInfoQuery.data.hiddenOutOfStock ? ` · скрыто без остатков: ${feedInfoQuery.data.hiddenOutOfStock}` : ""}.
                {" "}<a href="/api/avito/feed.xml" target="_blank" rel="noopener">Открыть XML</a>
              </p>
              <p className="form-hint">
                Цены и остатки подставляются из склада при каждом скачивании фида
                {feedInfoQuery.data.autoRefresh?.enabled
                  ? `; фоновая сверка каждые ${feedInfoQuery.data.autoRefresh.intervalMinutes} мин`
                  : ""}
                {feedInfoQuery.data.autoRefresh?.lastResult?.at
                  ? ` · последняя: ${new Date(feedInfoQuery.data.autoRefresh.lastResult.at).toLocaleString("ru-RU")}`
                  : ""}.
              </p>
              <div className="row-actions">
                <button className="secondary-action" type="button" disabled={refreshFeed.isPending} onClick={() => refreshFeed.mutate()} title="Перенести актуальные цены и остатки склада в объявления прямо сейчас">
                  {refreshFeed.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить цены/остатки
                </button>
              </div>
              {refreshFeed.data ? (
                <p className="form-hint">
                  Обновлено цен: {refreshFeed.data.updatedPrices ?? 0} · без остатков: {refreshFeed.data.outOfStock ?? 0} из {refreshFeed.data.total ?? 0}.
                </p>
              ) : null}
              {refreshFeed.error ? <div className="inline-error">{String((refreshFeed.error as Error).message)}</div> : null}
            </>
          ) : (
            <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю…</div>
          )}
          <div className="section-title compact-title"><div><span>Загрузки Avito</span><h3>Последние запуски</h3></div>
            <button className="secondary-action" type="button" disabled={!avitoConfigured} onClick={() => uploadsQuery.refetch()}><RefreshCw size={16} /></button>
          </div>
          {!avitoConfigured ? <p className="form-hint">Появятся после настройки кабинета Avito.</p> : null}
          {uploadsQuery.error ? <p className="form-hint">История загрузок недоступна: {String((uploadsQuery.error as Error).message)}</p> : null}
          {(uploadsQuery.data?.uploads || []).slice(0, 6).map((upload) => (
            <div className="upload-status-row" key={upload.upload_id}>
              <span className={`pill ${upload.status === "success" ? "ok" : upload.status === "processing" ? "muted" : "warn"}`}>
                {UPLOAD_STATUS_LABELS[upload.status] || upload.status}
              </span>
              <span>{upload.started_at ? new Date(upload.started_at).toLocaleString("ru-RU") : "—"}</span>
              <span>{upload.stats?.count ?? 0} объявл.</span>
            </div>
          ))}
        </section>
      </div>

      {previewData ? (
        <section className="settings-panel settings-panel-wide">
          <div className="section-title"><div><span>Предпросмотр</span><h3>Пройдут: {previewData.matchedCount} · пропущены: {previewData.skippedCount}</h3></div></div>
          {Object.keys(previewData.skippedByReason).length ? (
            <div className="brand-chips">
              {Object.entries(previewData.skippedByReason).map(([reason, count]) => (
                <span className="brand-chip" key={reason}>{reasonLabel(reason)}: {count}</span>
              ))}
            </div>
          ) : null}
          <div className="preview-columns">
            <div>
              <h4>Примеры прошедших ({previewData.matchedSample.length})</h4>
              <div className="table-panel">
                {previewData.matchedSample.slice(0, 20).map((item) => (
                  <div className="table-row avito-preview-row" key={item.adId}>
                    <span className="import-name">{item.title}</span>
                    <span>{item.volumeMl ? `${item.volumeMl} мл` : "—"}</span>
                    <span>{item.priceRub} ₽</span>
                  </div>
                ))}
                {!previewData.matchedSample.length ? <div className="empty-state">Ни один товар не прошёл фильтры.</div> : null}
              </div>
            </div>
            <div>
              <h4>Примеры пропущенных ({previewData.skippedSample.length})</h4>
              <div className="table-panel">
                {previewData.skippedSample.slice(0, 20).map((item) => (
                  <div className="table-row avito-preview-row" key={item.id}>
                    <span className="import-name">{item.name || item.offerId}</span>
                    <span className="pill warn">{reasonLabel(item.reasons[0] || "unknown")}</span>
                  </div>
                ))}
                {!previewData.skippedSample.length ? <div className="empty-state">Пропущенных нет.</div> : null}
              </div>
            </div>
          </div>
        </section>
      ) : null}

      <section className="settings-panel settings-panel-wide">
        <div className="section-title">
          <div><span>Объявления фида</span><h3>Всего: {listingsQuery.data?.total ?? 0}</h3></div>
          <button className="secondary-action" type="button" onClick={() => listingsQuery.refetch()}><RefreshCw size={16} /> Обновить</button>
        </div>
        <div className="table-panel import-table">
          <div className="table-head avito-listing-row">
            <span>Объявление</span>
            <span>Артикул</span>
            <span>Объём</span>
            <span>Цена</span>
            <span>Источник</span>
            <span></span>
          </div>
          {listings.slice(0, 200).map((item) => (
            <div className="table-row avito-listing-row" key={item.adId}>
              <span className="import-product">
                {item.imageUrls[0] ? <img src={item.imageUrls[0]} alt="" loading="lazy" /> : <span className="import-noimg" />}
                <span className="import-name">{item.title}</span>
              </span>
              <span data-label="Артикул">{item.adId}</span>
              <span data-label="Объём">{item.volumeMl ? `${item.volumeMl} мл` : "—"}</span>
              <span data-label="Цена">{item.priceRub ? `${item.priceRub} ₽` : "—"}</span>
              <span data-label="Источник">
                {item.outOfStock ? (
                  <span className="pill warn" title="Нет остатков на складе — объявление скрыто из фида">нет остатков</span>
                ) : (
                  <span className={`pill ${item.source === "ozon" ? "ok" : "muted"}`}>{item.source === "ozon" ? "Ozon" : "вручную"}</span>
                )}
              </span>
              <span>
                <button className="icon-action danger" type="button" disabled={removeListing.isPending} onClick={() => removeListing.mutate(item.adId)} title="Убрать из фида">
                  <Trash2 size={15} />
                </button>
              </span>
            </div>
          ))}
          {!listings.length && !listingsQuery.isFetching ? (
            <div className="empty-state">Фид пуст. Настрой правила и нажми «Импортировать в фид».</div>
          ) : null}
        </div>
        {listings.length > 200 ? <p className="form-hint">Показаны первые 200 объявлений из {listings.length}.</p> : null}
      </section>
    </section>
  );
}
