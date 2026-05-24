import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { BadgeDollarSign, CheckCircle2, Clock3, Loader2, RefreshCcw, Send, Zap } from "lucide-react";
import { useMemo, useState } from "react";
import { fetchJson, mutationBody } from "../api";
import { MutationProductResponseSchema, PricePreviewSchema } from "../types";

const text = (value: unknown) => String(value ?? "").trim();

const money = (value: unknown) => {
  const number = Number(value || 0);
  return number > 0 ? `${Math.round(number).toLocaleString("ru-RU")} ₽` : "-";
};

const numberValue = (value: unknown) => Number(value || 0) || 0;

const reasonLabel = (reason: unknown) => {
  const value = text(reason);
  const labels: Record<string, string> = {
    unchanged: "цена уже совпадает",
    no_pricemaster_link: "нет PM-привязки",
    not_ready: "поставщик не готов",
    stock_only_manual_price_missing: "нужна ручная цена склада",
    ozon_price_delayed: "ожидает лимит Ozon",
    no_next_price: "нет расчетной цены",
  };
  return labels[value] || value || "-";
};

export function PricesPage() {
  const [marketplace, setMarketplace] = useState("all");
  const [onlyChanged, setOnlyChanged] = useState(true);
  const queryClient = useQueryClient();
  const preview = useQuery({
    queryKey: ["warehouse", "prices", "preview", marketplace, onlyChanged],
    queryFn: () => fetchJson(
      `/api/warehouse/prices/preview?marketplace=${encodeURIComponent(marketplace)}&onlyChanged=${onlyChanged ? "true" : "false"}&limit=500&refreshMarketplacePrices=true&livePriceMaster=true`,
      PricePreviewSchema,
    ),
  });
  const send = useMutation({
    mutationFn: (payload: { marketplace: string; force?: boolean }) => fetchJson("/api/warehouse/prices/send", MutationProductResponseSchema, mutationBody({
      confirmed: true,
      marketplace: payload.marketplace,
      onlyChanged,
      force: Boolean(payload.force),
      livePriceMaster: true,
      refreshMarketplacePrices: true,
      limit: 1000,
    })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse", "prices"] });
    },
  });
  const data = preview.data;
  const items = data?.items || [];
  const skipped = data?.skipped || [];
  const skippedStats = useMemo(() => {
    const result = new Map<string, number>();
    for (const row of skipped) {
      const reason = reasonLabel(row.reason);
      result.set(reason, (result.get(reason) || 0) + 1);
    }
    return Array.from(result.entries()).slice(0, 5);
  }, [skipped]);
  const ozonItems = items.filter((item) => text(item.marketplace) === "ozon").length;
  const yandexItems = items.filter((item) => text(item.marketplace) === "yandex").length;

  return (
    <section className="page-section price-control-page">
      <div className="section-title">
        <div>
          <span>Price control</span>
          <h2>Автоцены Ozon и Yandex</h2>
        </div>
        <button className="secondary-action" type="button" onClick={() => preview.refetch()} disabled={preview.isFetching}>
          {preview.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Обновить контроль
        </button>
      </div>

      <div className="price-automation-hero">
        <div>
          <span className="eyebrow">Автоматический режим</span>
          <h3>Цены отправляются сами после изменения PriceMaster, курса, наценки или привязки.</h3>
          <p>
            Эта страница нужна для контроля: увидеть, что сейчас отличается от кабинетов, повторить ошибки и вручную ускорить отправку.
            Обычная работа не требует нажимать кнопку.
          </p>
        </div>
        <div className="price-automation-badge">
          <CheckCircle2 size={22} />
          <strong>auto on</strong>
          <span>live PM + cabinet check</span>
        </div>
      </div>

      <div className="control-grid price-controls">
        <label>
          Маркетплейс
          <select value={marketplace} onChange={(event) => setMarketplace(event.target.value)}>
            <option value="all">Ozon + Yandex</option>
            <option value="yandex">Только Yandex</option>
            <option value="ozon">Только Ozon</option>
          </select>
        </label>
        <label className="toggle-row">
          <input type="checkbox" checked={onlyChanged} onChange={(event) => setOnlyChanged(event.target.checked)} />
          Только измененные
        </label>
        <button className="primary-action" type="button" onClick={() => send.mutate({ marketplace })} disabled={send.isPending || !items.length}>
          {send.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Отправить сейчас
        </button>
        <button className="secondary-action" type="button" onClick={() => send.mutate({ marketplace: "yandex", force: true })} disabled={send.isPending}>
          <BadgeDollarSign size={16} /> Сверить Yandex force
        </button>
      </div>

      <div className="summary-grid price-summary-grid">
        <div><span>К отправке</span><strong>{data?.readyToSend ?? 0}</strong></div>
        <div><span>Ozon</span><strong>{ozonItems}</strong></div>
        <div><span>Yandex</span><strong>{yandexItems}</strong></div>
        <div><span>Пропущено</span><strong>{skipped.length}</strong></div>
      </div>

      {skippedStats.length ? (
        <div className="price-skip-strip">
          <Clock3 size={16} />
          <span>Почему часть товаров не отправляется:</span>
          {skippedStats.map(([reason, count]) => <b key={reason}>{reason}: {count}</b>)}
        </div>
      ) : null}

      {preview.error ? <div className="inline-error">{String((preview.error as Error).message || preview.error)}</div> : null}
      {send.error ? <div className="inline-error">{String((send.error as Error).message || send.error)}</div> : null}
      {send.data ? (
        <div className="success-strip">
          Отправлено: {numberValue(send.data.sent)}. Ozon: {numberValue(send.data.ozonSent)}, Yandex: {numberValue(send.data.yandexSent)}, ошибок: {numberValue(send.data.failed)}.
        </div>
      ) : null}

      <div className="table-panel price-table">
        <div className="table-head">
          <span>Маркет</span><span>Артикул</span><span>Сейчас</span><span>Новая</span><span>Поставщик</span><span>Статус</span>
        </div>
        {items.map((item) => (
          <div className="table-row" key={`${text(item.marketplace)}-${text(item.id || item.offerId)}`}>
            <span data-label="Маркет"><Zap size={14} /> {text(item.marketplace)}</span>
            <span data-label="Артикул">{text(item.offerId)}</span>
            <span data-label="Сейчас">{money(item.oldPrice)}</span>
            <span data-label="Новая"><strong>{money(item.price)}</strong></span>
            <span data-label="Поставщик">{text((item.supplier as { supplierName?: string })?.supplierName || (item.supplier as { name?: string })?.name || (item.supplier as { partnerName?: string })?.partnerName || "") || "-"}</span>
            <span data-label="Статус">изменено</span>
          </div>
        ))}
        {!items.length && !preview.isLoading ? <div className="empty-state">Новых цен к отправке нет. Автоотправка продолжит работать в фоне.</div> : null}
      </div>
    </section>
  );
}
