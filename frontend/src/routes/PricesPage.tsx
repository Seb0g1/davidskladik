import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { BadgeDollarSign, Loader2, RefreshCcw, Send } from "lucide-react";
import { useState } from "react";
import { fetchJson, mutationBody } from "../api";
import { MutationProductResponseSchema, PricePreviewSchema } from "../types";

const text = (value: unknown) => String(value ?? "").trim();
const money = (value: unknown) => {
  const number = Number(value || 0);
  return number > 0 ? `${Math.round(number).toLocaleString("ru-RU")} ₽` : "-";
};

export function PricesPage() {
  const [marketplace, setMarketplace] = useState("all");
  const [onlyChanged, setOnlyChanged] = useState(true);
  const queryClient = useQueryClient();
  const preview = useQuery({
    queryKey: ["warehouse", "prices", "preview", marketplace, onlyChanged],
    queryFn: () => fetchJson(`/api/warehouse/prices/preview?marketplace=${encodeURIComponent(marketplace)}&onlyChanged=${onlyChanged ? "true" : "false"}&limit=500`, PricePreviewSchema),
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
  return (
    <section className="page-section">
      <div className="section-title">
        <div>
          <span>Price control</span>
          <h2>Цены к отправке</h2>
        </div>
        <button className="secondary-action" type="button" onClick={() => preview.refetch()} disabled={preview.isFetching}>
          {preview.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Обновить
        </button>
      </div>
      <div className="control-grid">
        <select value={marketplace} onChange={(event) => setMarketplace(event.target.value)}>
          <option value="all">Ozon + Yandex</option>
          <option value="yandex">Только Yandex</option>
          <option value="ozon">Только Ozon</option>
        </select>
        <label className="toggle-row">
          <input type="checkbox" checked={onlyChanged} onChange={(event) => setOnlyChanged(event.target.checked)} />
          Только измененные
        </label>
        <button className="primary-action" type="button" onClick={() => send.mutate({ marketplace })} disabled={send.isPending || !items.length}>
          {send.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />} Отправить измененные
        </button>
        <button className="secondary-action" type="button" onClick={() => send.mutate({ marketplace: "yandex", force: true })} disabled={send.isPending}>
          <BadgeDollarSign size={16} /> Force Yandex
        </button>
      </div>
      <div className="summary-grid">
        <div><span>К отправке</span><strong>{data?.readyToSend ?? 0}</strong></div>
        <div><span>Ozon</span><strong>{items.filter((item) => text(item.marketplace) === "ozon").length}</strong></div>
        <div><span>Yandex</span><strong>{items.filter((item) => text(item.marketplace) === "yandex").length}</strong></div>
        <div><span>Пропущено</span><strong>{skipped.length}</strong></div>
      </div>
      {preview.error ? <div className="inline-error">{String((preview.error as Error).message || preview.error)}</div> : null}
      {send.error ? <div className="inline-error">{String((send.error as Error).message || send.error)}</div> : null}
      <div className="table-panel">
        <div className="table-head">
          <span>Маркет</span><span>Артикул</span><span>Текущая</span><span>Новая</span><span>Поставщик</span><span>Статус</span>
        </div>
        {items.map((item) => (
          <div className="table-row" key={`${text(item.marketplace)}-${text(item.id || item.offerId)}`}>
            <span>{text(item.marketplace)}</span>
            <span>{text(item.offerId)}</span>
            <span>{money(item.oldPrice)}</span>
            <span>{money(item.price)}</span>
            <span>{text((item.supplier as { supplierName?: string })?.supplierName || (item.supplier as { name?: string })?.name || "") || "-"}</span>
            <span>changed</span>
          </div>
        ))}
        {!items.length && !preview.isLoading ? <div className="empty-state">Новых цен к отправке нет.</div> : null}
      </div>
    </section>
  );
}
