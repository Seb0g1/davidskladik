import { useQuery } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { fetchJson } from "../api";
import { SupplierAlternativesSchema } from "../types";
import type { SupplierAltOption } from "./SupplierAltPicker";

/**
 * Suggestions right in a skipped cart line: the best orderable suppliers for the offer (same product first, another
 * volume last and marked), one click to take one. «Предложений нет» when PriceMaster has nobody.
 */
export function SupplierAltInline({ offerId, currentPartnerId, busy, onPick }: {
  offerId: string;
  currentPartnerId?: string;
  busy: boolean;
  onPick: (option: SupplierAltOption) => void;
}) {
  const alternatives = useQuery({
    queryKey: ["supplier-alternatives", offerId],
    queryFn: () => fetchJson(`/api/supplier-cart/alternatives?offerId=${encodeURIComponent(offerId)}`, SupplierAlternativesSchema),
    staleTime: 60_000,
  });
  const current = (currentPartnerId || "").toLowerCase();
  const options = (alternatives.data?.options || [])
    .filter((o) => String(o.partnerId || "").toLowerCase() !== current && !o.blocked && (o.orderable || o.inactivePm))
    .sort((a, b) => Number(Boolean(a.otherVolume)) - Number(Boolean(b.otherVolume)) || (b.score || 0) - (a.score || 0))
    .slice(0, 3);

  if (alternatives.isLoading) return <div className="cart-sugg is-muted"><Loader2 className="spin" size={12} /> Ищу других поставщиков…</div>;
  if (alternatives.error) return <div className="cart-sugg is-muted">Предложения не загрузились — откройте «Заменить».</div>;
  if (!options.length) return <div className="cart-sugg is-muted">Предложений нет: у других поставщиков в PriceMaster этого товара нет.</div>;
  return (
    <div className="cart-sugg">
      <span className="cart-sugg-title">Можно заказать:</span>
      {options.map((o) => (
        <button key={`${o.partnerId}-${o.rowId}`} type="button" className={`cart-sugg-opt${o.otherVolume ? " is-other" : ""}`} disabled={busy}
          title={o.rowName || undefined} onClick={() => onPick(o)}>
          <b>{o.supplierName || o.partnerId}</b>
          <span>{o.price ? `${o.price} ${o.priceCurrency}` : "цена не указана"}{o.orderCutoffTime ? ` · до ${o.orderCutoffTime}` : ""}</span>
          {o.otherVolume ? <em>другой объём {o.otherVolume}</em> : null}
          {o.inactivePm ? <em>неактивная строка PM</em> : null}
        </button>
      ))}
    </div>
  );
}
