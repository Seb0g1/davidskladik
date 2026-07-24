import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Archive, Image, Loader2, Package, RefreshCw, Save, Send, Warehouse, Zap } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { useDebounced } from "../lib/common";

type WbCard = {
  nmID: number;
  vendorCode: string;
  subjectName?: string;
  brand?: string;
  title?: string;
  photos?: Array<{ big?: string; c246x328?: string; square?: string }>;
  sizes?: Array<{ techSize?: string; skus?: string[] }>;
  updatedAt?: string;
};

type WbCardsResponse = { total: number; cards: WbCard[] };
type WbErrorItem = { object?: string; vendorCode?: string; errors?: string[] };
type WbErrorsResponse = { total: number; errors: WbErrorItem[] };

type WbImportRules = {
  subjectId: number;
  subjectName: string;
  tnved: string;
  minSupplierPriceRub: number;
  maxWbPriceRub: number;
  skipArchived: boolean;
  excludeTitleWords: string[];
  defaultStock: number;
  updatedAt: string | null;
};

type WbWarehouse = { id: number; name: string; officeId?: number };
type WbWarehousesResponse = { warehouses: WbWarehouse[] };

type WbPricesSendResult = {
  ok: boolean;
  dryRun?: boolean;
  cards: number;
  prepared: number;
  sent?: number;
  skippedBelowMin: number;
  skippedAboveMax: number;
  skippedNoSupplier: number;
  skippedNotLinked: number;
  maxWbPriceRub: number;
  minSupplierPriceRub: number;
  sample?: Array<{ nmID: number; price: number }>;
};

type WbStocksSyncResult = {
  ok: boolean;
  dryRun?: boolean;
  warehouseId: number;
  cards: number;
  inStock: number;
  zeroed: number;
  sent?: number;
};

type AccountsResponse = {
  accounts: Array<{ id: string; marketplace: string; name: string; configured: boolean }>;
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
function cardPhotoUrl(card: WbCard): string | null {
  const photos = card.photos || [];
  return photos[0]?.c246x328 || photos[0]?.big || null;
}

export function WbPage() {
  const queryClient = useQueryClient();
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounced(search, 400);
  const [rules, setRules] = useState<WbImportRules | null>(null);
  const [warehouseId, setWarehouseId] = useState<number>(0);
  const [pricePreview, setPricePreview] = useState<WbPricesSendResult | null>(null);
  const [stockPreview, setStockPreview] = useState<WbStocksSyncResult | null>(null);

  const accountsQuery = useQuery({
    queryKey: ["marketplace-accounts"],
    queryFn: () => apiJson<AccountsResponse>("/api/marketplace-accounts"),
  });
  const wbConfigured = Boolean(
    (accountsQuery.data?.accounts || []).find((a) => a.marketplace === "wb")?.configured
  );

  const cardsQuery = useQuery({
    queryKey: ["wb-cards", debouncedSearch],
    queryFn: () => apiJson<WbCardsResponse>(`/api/wb/cards?limit=2000${debouncedSearch ? `&query=${encodeURIComponent(debouncedSearch)}` : ""}`),
    enabled: wbConfigured,
  });
  const errorsQuery = useQuery({
    queryKey: ["wb-card-errors"],
    queryFn: () => apiJson<WbErrorsResponse>("/api/wb/cards/errors"),
    enabled: wbConfigured,
    retry: false,
  });
  const rulesQuery = useQuery({
    queryKey: ["wb-import-rules"],
    queryFn: () => apiJson<WbImportRules>("/api/wb/import/rules"),
  });
  const warehousesQuery = useQuery({
    queryKey: ["wb-warehouses"],
    queryFn: () => apiJson<WbWarehousesResponse>("/api/wb/warehouses"),
    enabled: wbConfigured,
    retry: false,
  });

  useEffect(() => {
    if (rulesQuery.data && !rules) setRules(rulesQuery.data);
  }, [rulesQuery.data, rules]);

  useEffect(() => {
    const warehouses = warehousesQuery.data?.warehouses || [];
    if (warehouses.length && !warehouseId) setWarehouseId(warehouses[0].id);
  }, [warehousesQuery.data, warehouseId]);

  const saveRules = useMutation({
    mutationFn: (next: WbImportRules) => apiJson<WbImportRules>("/api/wb/import/rules", { method: "PUT", body: JSON.stringify(next) }),
    onSuccess: (saved) => {
      setRules(saved);
      void queryClient.invalidateQueries({ queryKey: ["wb-import-rules"] });
    },
  });

  const previewPrices = useMutation({
    mutationFn: () => apiJson<WbPricesSendResult>("/api/wb/prices/send", { method: "POST", body: JSON.stringify({ dryRun: true }) }),
    onSuccess: (data) => setPricePreview(data),
  });
  const sendPrices = useMutation({
    mutationFn: () => apiJson<WbPricesSendResult>("/api/wb/prices/send", { method: "POST", body: JSON.stringify({ dryRun: false }) }),
    onSuccess: (data) => {
      setPricePreview(data);
      void queryClient.invalidateQueries({ queryKey: ["wb-cards"] });
    },
  });

  const archiveAboveLimit = useMutation({
    mutationFn: () => apiJson<{ ok: boolean; async: boolean; message: string }>("/api/wb/cards/archive-above-limit", { method: "POST", body: JSON.stringify({}) }),
  });

  const previewStocks = useMutation({
    mutationFn: () => apiJson<WbStocksSyncResult>("/api/wb/stocks/sync", { method: "POST", body: JSON.stringify({ dryRun: true, warehouseId }) }),
    onSuccess: (data) => setStockPreview(data),
  });
  const syncStocks = useMutation({
    mutationFn: () => apiJson<WbStocksSyncResult>("/api/wb/stocks/sync", { method: "POST", body: JSON.stringify({ dryRun: false, warehouseId }) }),
    onSuccess: (data) => setStockPreview(data),
  });

  const backfillMedia = useMutation({
    mutationFn: () => apiJson<{ ok: boolean; sent: number; skippedNoImages: number; cardsWithoutPhoto: number }>("/api/wb/media/backfill", { method: "POST", body: JSON.stringify({ limit: 200 }) }),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["wb-cards"] }),
  });
  const enrichCards = useMutation({
    mutationFn: () => apiJson<{ ok: boolean; updated: number; prepared: number }>("/api/wb/cards/enrich", { method: "POST", body: JSON.stringify({ limit: 20000 }) }),
  });
  const tnvedBackfill = useMutation({
    mutationFn: () => apiJson<{ ok: boolean; updated: number; missingTnved: number }>("/api/wb/tnved/backfill", { method: "POST", body: JSON.stringify({ limit: 20000 }) }),
  });

  const cards = cardsQuery.data?.cards || [];
  const withPhoto = cards.filter((c) => (c.photos || []).length > 0).length;
  const withoutPhoto = cards.length - withPhoto;
  const cardErrors = errorsQuery.data?.errors || [];
  const warehouses = warehousesQuery.data?.warehouses || [];

  return (
    <section className="page-section wb-page">
      <PageHeader
        title="Wildberries"
        subtitle="Карточки, цены и остатки FBS на WB"
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" disabled={!wbConfigured || previewPrices.isPending} onClick={() => previewPrices.mutate()}>
              {previewPrices.isPending ? <Loader2 className="spin" size={16} /> : <Zap size={16} />} Проверить цены
            </button>
            <button className="primary-action" type="button" disabled={!wbConfigured || sendPrices.isPending} onClick={() => sendPrices.mutate()}>
              {sendPrices.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Отправить цены
            </button>
            <button className="secondary-action" type="button" disabled={!wbConfigured || archiveAboveLimit.isPending} onClick={() => { if (window.confirm("Отправить в архив WB все карточки с ценой > 20 000 ₽?")) archiveAboveLimit.mutate(); }}>
              {archiveAboveLimit.isPending ? <Loader2 className="spin" size={16} /> : <Archive size={16} />} Архив выше лимита
            </button>
          </div>
        )}
      />

      {!accountsQuery.isFetching && !wbConfigured ? (
        <div className="info-strip warn">
          Кабинет WB не настроен. Добавьте API-токен в настройках кабинетов. Настройки правил импорта доступны и без токена.
        </div>
      ) : null}

      <section className="dashboard-metrics">
        <Stat label="Карточек WB" value={cardsQuery.isFetching ? "…" : cards.length} tone="accent" icon={<Package size={18} />} />
        <Stat label="С фото" value={cardsQuery.isFetching ? "…" : withPhoto} tone="success" icon={<Image size={18} />} />
        <Stat label="Без фото" value={cardsQuery.isFetching ? "…" : withoutPhoto} tone={withoutPhoto > 0 ? "warn" : undefined} icon={<Image size={18} />} />
        <Stat label="Ошибки создания" value={errorsQuery.isFetching ? "…" : cardErrors.length} tone={cardErrors.length > 0 ? "warn" : undefined} icon={<AlertTriangle size={18} />} />
      </section>

      {sendPrices.data && !sendPrices.data.dryRun ? (
        <div className="info-strip success">
          Цены отправлены: {sendPrices.data.sent ?? sendPrices.data.prepared} из {sendPrices.data.cards} карточек
          {sendPrices.data.skippedAboveMax > 0 ? ` · выше лимита ${sendPrices.data.maxWbPriceRub} ₽: ${sendPrices.data.skippedAboveMax}` : ""}
          {sendPrices.data.skippedNoSupplier > 0 ? ` · нет поставщика: ${sendPrices.data.skippedNoSupplier}` : ""}.
        </div>
      ) : null}
      {sendPrices.error ? <div className="inline-error">{String((sendPrices.error as Error).message)}</div> : null}

      <div className="settings-grid">
        {/* Карточки */}
        <section className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div><span>Каталог</span><h3>Карточки Wildberries</h3></div>
            <div className="row-actions">
              <input
                type="search"
                placeholder="Поиск по артикулу"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="table-search-input"
              />
              <button className="secondary-action" type="button" onClick={() => void queryClient.invalidateQueries({ queryKey: ["wb-cards"] })} disabled={cardsQuery.isFetching}>
                <RefreshCw size={16} />
              </button>
            </div>
          </div>

          {cardsQuery.isFetching ? (
            <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю карточки…</div>
          ) : !wbConfigured ? (
            <div className="table-note">Кабинет WB не настроен — карточки недоступны.</div>
          ) : !cards.length ? (
            <div className="table-note">{debouncedSearch ? "Карточки не найдены." : "Карточек на WB нет. Создайте через «Импорт Ozon → WB» в настройках."}</div>
          ) : (
            <div className="wb-cards-table">
              <div className="wb-cards-head">
                <span>Фото</span><span>NM ID</span><span>Артикул</span><span>Название</span><span>Предмет</span>
              </div>
              {cards.slice(0, 500).map((card) => {
                const photo = cardPhotoUrl(card);
                return (
                  <div className="wb-card-row" key={card.nmID}>
                    <span className="wb-card-photo">
                      {photo ? (
                        <img src={photo} alt="" loading="lazy" className="wb-card-thumb" />
                      ) : (
                        <span className="wb-card-no-photo"><Image size={14} /></span>
                      )}
                    </span>
                    <span className="wb-card-nmid">
                      <a href={`https://www.wildberries.ru/catalog/${card.nmID}/detail.aspx`} target="_blank" rel="noreferrer" className="link-plain">
                        {card.nmID}
                      </a>
                    </span>
                    <span className="wb-card-vendor">{card.vendorCode}</span>
                    <span className="wb-card-title">{card.title || "—"}</span>
                    <span className="wb-card-subject muted">{card.subjectName || "—"}</span>
                  </div>
                );
              })}
              {cards.length > 500 ? <div className="table-note">Показано 500 из {cards.length}. Используйте поиск для фильтрации.</div> : null}
            </div>
          )}

          {cardErrors.length > 0 ? (
            <>
              <div className="section-title compact-title"><div><span>Ошибки</span><h3>Ошибки создания карточек</h3></div></div>
              {cardErrors.slice(0, 20).map((err, index) => (
                <div className="wb-card-error" key={index}>
                  <span className="pill warn">{err.vendorCode || err.object || "—"}</span>
                  <span>{(err.errors || []).join("; ")}</span>
                </div>
              ))}
              {cardErrors.length > 20 ? <p className="form-hint">…и ещё {cardErrors.length - 20} ошибок.</p> : null}
            </>
          ) : null}
        </section>

        {/* Правая панель: операции */}
        <section className="settings-panel">
          {/* Цены */}
          <div className="section-title compact-title"><div><span>Цены WB</span><h3>Отправка цен</h3></div></div>
          <p className="form-hint">Цена = закупка поставщика × наценка WB. Выше {rules?.maxWbPriceRub ?? "…"} ₽ — не отправляется.</p>
          <div className="row-actions">
            <button className="secondary-action" type="button" disabled={!wbConfigured || previewPrices.isPending} onClick={() => previewPrices.mutate()}>
              {previewPrices.isPending ? <Loader2 className="spin" size={16} /> : <Zap size={16} />} Предпросмотр
            </button>
            <button className="primary-action" type="button" disabled={!wbConfigured || sendPrices.isPending} onClick={() => sendPrices.mutate()}>
              {sendPrices.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Отправить
            </button>
          </div>
          {pricePreview ? (
            <div className="info-strip success compact" style={{ marginTop: 8 }}>
              {pricePreview.dryRun ? "Предпросмотр: " : "Отправлено: "}
              готово {pricePreview.prepared} из {pricePreview.cards}
              {pricePreview.skippedAboveMax > 0 ? ` · выше лимита: ${pricePreview.skippedAboveMax}` : ""}
              {pricePreview.skippedNoSupplier > 0 ? ` · нет поставщика: ${pricePreview.skippedNoSupplier}` : ""}
              {pricePreview.skippedNotLinked > 0 ? ` · нет привязки: ${pricePreview.skippedNotLinked}` : ""}
              .
            </div>
          ) : null}
          {previewPrices.error ? <div className="inline-error">{String((previewPrices.error as Error).message)}</div> : null}

          {/* Остатки */}
          <div className="section-title compact-title"><div><span>Остатки FBS</span><h3>Синхронизация</h3></div></div>
          <p className="form-hint">Выставляет defaultStock ({rules?.defaultStock ?? "…"}) для продаваемых товаров, 0 для остальных.</p>
          {warehouses.length ? (
            <label className="field-label">
              Склад WB
              <select value={String(warehouseId)} onChange={(e) => setWarehouseId(Number(e.target.value))}>
                {warehouses.map((wh) => (
                  <option key={wh.id} value={String(wh.id)}>{wh.name} (#{wh.id})</option>
                ))}
              </select>
            </label>
          ) : wbConfigured ? (
            <p className="form-hint muted">Склады не загружены. Введите warehouseId вручную или настройте Campaign ID кабинета WB.</p>
          ) : null}
          <div className="row-actions">
            <button className="secondary-action" type="button" disabled={!wbConfigured || !warehouseId || previewStocks.isPending} onClick={() => previewStocks.mutate()}>
              {previewStocks.isPending ? <Loader2 className="spin" size={16} /> : <Warehouse size={16} />} Предпросмотр
            </button>
            <button className="primary-action" type="button" disabled={!wbConfigured || !warehouseId || syncStocks.isPending} onClick={() => syncStocks.mutate()}>
              {syncStocks.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Синхронизировать
            </button>
          </div>
          {stockPreview ? (
            <div className="info-strip success compact" style={{ marginTop: 8 }}>
              {stockPreview.dryRun ? "Предпросмотр: " : "Синхронизировано: "}
              в продаже {stockPreview.inStock} · обнулено {stockPreview.zeroed} из {stockPreview.cards}.
            </div>
          ) : null}
          {previewStocks.error ? <div className="inline-error">{String((previewStocks.error as Error).message)}</div> : null}
          {syncStocks.error ? <div className="inline-error">{String((syncStocks.error as Error).message)}</div> : null}

          {/* Обслуживание */}
          <div className="section-title compact-title"><div><span>Обслуживание</span><h3>Дозаполнение данных</h3></div></div>
          <div className="column-actions">
            <button className="secondary-action" type="button" disabled={!wbConfigured || backfillMedia.isPending} onClick={() => backfillMedia.mutate()} title="Отправить фото на карточки без фото (порция до 200; ~1 фото/15 мин лимит WB)">
              {backfillMedia.isPending ? <Loader2 className="spin" size={16} /> : <Image size={16} />} Досылка фото
            </button>
            <button className="secondary-action" type="button" disabled={!wbConfigured || enrichCards.isPending} onClick={() => enrichCards.mutate()} title="Обновить бренд, описание, объём и артикул Ozon в существующих карточках WB">
              {enrichCards.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обогатить с Ozon
            </button>
            <button className="secondary-action" type="button" disabled={!wbConfigured || tnvedBackfill.isPending} onClick={() => tnvedBackfill.mutate()} title="Заполнить ТН ВЭД в карточках без этой характеристики">
              {tnvedBackfill.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Заполнить ТН ВЭД
            </button>
          </div>
          {backfillMedia.data ? (
            <p className="form-hint">Фото: отправлено {backfillMedia.data.sent} из {backfillMedia.data.cardsWithoutPhoto} без фото · пропущено (нет фото на складе) {backfillMedia.data.skippedNoImages}.</p>
          ) : null}
          {backfillMedia.error ? <div className="inline-error">{String((backfillMedia.error as Error).message)}</div> : null}
          {enrichCards.data ? (
            <p className="form-hint">Обогащение: обновлено {enrichCards.data.updated} из {enrichCards.data.prepared} карточек.</p>
          ) : null}
          {enrichCards.error ? <div className="inline-error">{String((enrichCards.error as Error).message)}</div> : null}
          {tnvedBackfill.data ? (
            <p className="form-hint">ТН ВЭД: обновлено {tnvedBackfill.data.updated} карточек · без кода оставалось {tnvedBackfill.data.missingTnved}.</p>
          ) : null}
          {tnvedBackfill.error ? <div className="inline-error">{String((tnvedBackfill.error as Error).message)}</div> : null}

          {/* Настройки */}
          <div className="section-title compact-title">
            <div><span>Настройки</span><h3>Правила WB</h3></div>
            <button className="secondary-action" type="button" disabled={!rules || saveRules.isPending} onClick={() => rules && saveRules.mutate(rules)}>
              {saveRules.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить
            </button>
          </div>
          {!rules ? (
            <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю…</div>
          ) : (
            <>
              <div className="settings-form-row">
                <label className="field-label" title="Товары с ценой выше этого лимита не отображаются на WB и не получают остатков">
                  Лимит цены WB, ₽
                  <input type="number" min={0} value={String(rules.maxWbPriceRub)} onChange={(e) => setRules((r) => r ? { ...r, maxWbPriceRub: numberInput(e.target.value, 20000) } : r)} />
                </label>
                <label className="field-label" title="Товары с закупкой ниже порога не отправляются на WB. 0 — отключено.">
                  Мин. закупка, ₽ (0 = выкл)
                  <input type="number" min={0} value={String(rules.minSupplierPriceRub)} onChange={(e) => setRules((r) => r ? { ...r, minSupplierPriceRub: numberInput(e.target.value) } : r)} />
                </label>
                <label className="field-label" title="Остаток FBS, который выставляется для продаваемых товаров">
                  Остаток FBS (шт.)
                  <input type="number" min={0} max={999} value={String(rules.defaultStock)} onChange={(e) => setRules((r) => r ? { ...r, defaultStock: Math.max(0, Math.min(999, numberInput(e.target.value, 3))) } : r)} />
                </label>
              </div>
              <label className="settings-toggle">
                <input type="checkbox" checked={rules.skipArchived} onChange={(e) => setRules((r) => r ? { ...r, skipArchived: e.target.checked } : r)} />
                Пропускать архивные при импорте
              </label>
              <div className="settings-form-row" style={{ marginTop: 8 }}>
                <label className="field-label field-label-wide">
                  Стоп-слова в названии при импорте
                  <textarea rows={2} value={wordsToText(rules.excludeTitleWords)} onChange={(e) => setRules((r) => r ? { ...r, excludeTitleWords: textToWords(e.target.value) } : r)} placeholder="дубль, отливант, тестер" />
                </label>
              </div>
              {saveRules.isSuccess ? <div className="info-strip success compact">Сохранено.</div> : null}
              {saveRules.error ? <div className="inline-error">{String((saveRules.error as Error).message)}</div> : null}
            </>
          )}
        </section>
      </div>
    </section>
  );
}
