import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Clock3, Loader2, ShieldCheck, Truck, Unlock, X } from "lucide-react";
import { z } from "zod";
import { fetchJson } from "../api";
import { SupplierAlternativesSchema } from "../types";

export type SupplierAltOption = z.infer<typeof SupplierAlternativesSchema>["options"][number];

const SKIP_REASON_LABELS: Record<string, string> = {
  product_not_found: "Товар не найден на складе",
  no_pricemaster_link: "У товара нет привязки PriceMaster",
  offer_required: "Не указан SKU",
};

// Live PriceMaster supplier options for one offer: used on Автокорзине (замена
// поставщика до формирования корзины) and на Сборке (перезаказ после «Не было»).
export function SupplierAltPicker({
  offerId,
  currentPartnerId,
  busy,
  actionLabel,
  onPick,
  onClose,
}: {
  offerId: string;
  currentPartnerId?: string;
  busy: boolean;
  actionLabel: string;
  onPick: (option: SupplierAltOption) => void;
  onClose: () => void;
}) {
  const qc = useQueryClient();
  const alternatives = useQuery({
    queryKey: ["supplier-alternatives", offerId],
    queryFn: () => fetchJson(`/api/supplier-cart/alternatives?offerId=${encodeURIComponent(offerId)}`, SupplierAlternativesSchema),
    staleTime: 30_000,
  });
  const unblock = useMutation({
    mutationFn: (partnerId: string) =>
      fetchJson("/api/supplier-cart/blocks", z.object({ ok: z.boolean().optional(), unblocked: z.string().optional() }), {
        method: "DELETE",
        body: JSON.stringify({ offerId, partnerId }),
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["supplier-alternatives", offerId] });
    },
  });
  const current = (currentPartnerId || "").toLowerCase();
  const options = (alternatives.data?.options || []).filter((option) => String(option.partnerId || "").toLowerCase() !== current);
  const skipReason = String(alternatives.data?.skipReason || "");

  return (
    <div className="supplier-alt-panel">
      <div className="supplier-alt-head">
        <strong><Truck size={14} /> Замена поставщика · {offerId}</strong>
        <button className="icon-action" type="button" aria-label="Закрыть" onClick={onClose}><X size={15} /></button>
      </div>
      {alternatives.isLoading ? <div className="soft-empty compact"><Loader2 className="spin" size={14} /> Пробиваю наличие по PriceMaster…</div> : null}
      {alternatives.error ? <div className="inline-error">Не удалось получить поставщиков: {String((alternatives.error as Error).message || alternatives.error)}</div> : null}
      {!alternatives.isLoading && !options.length ? (
        <div className="soft-empty compact">
          {SKIP_REASON_LABELS[skipReason] || "Других поставщиков с этой позицией в PriceMaster нет."}
        </div>
      ) : null}
      {options.map((option) => {
        const disabled = busy || option.blocked || (!option.orderable && !option.inactivePm);
        return (
          <div className={`supplier-alt-row${option.blocked || (!option.orderable && !option.inactivePm) ? " is-disabled" : ""}${option.inactivePm ? " is-inactive-pm" : ""}`} key={`${option.partnerId}-${option.rowId}`}>
            <div className="supplier-alt-info">
              <strong>{option.supplierName || option.partnerId}</strong>
              <span>
                {option.price ? `${option.price} ${option.priceCurrency}` : "цена не указана"}
                {" · "}доверие {option.trustFactor}/100
                {option.orderCutoffTime ? ` · заказы до ${option.orderCutoffTime}` : ""}
              </span>
              <span className="supplier-alt-badges">
                {option.blocked ? (
                  <em className="danger-text">
                    <AlertTriangle size={12} /> заблокирован после «не было»
                  </em>
                ) : null}
                {option.inactivePm ? <em style={{ color: "#f59e0b" }}><AlertTriangle size={12} /> неактивен в PM (Active=0)</em> : (!option.available ? <em className="danger-text">нет наличия</em> : null)}
                {option.cutoffPassed && !option.blocked ? <em><Clock3 size={12} /> приём заказов на сегодня закрыт</em> : null}
                {option.stockOnly ? <em>Наш склад</em> : null}
                {option.reseller ? <em>перекупщик</em> : null}
                {!option.blocked && option.orderable && !option.cutoffPassed && !option.stockOnly ? <em className="success-text"><ShieldCheck size={12} /> можно заказать</em> : null}
              </span>
            </div>
            {option.blocked ? (
              <button
                className="secondary-action"
                type="button"
                disabled={unblock.isPending}
                title="Снять блок поставщика для этого SKU"
                onClick={() => unblock.mutate(option.partnerId)}
              >
                {unblock.isPending && unblock.variables === option.partnerId
                  ? <Loader2 className="spin" size={14} />
                  : <Unlock size={14} />}
                {" "}Разблокировать
              </button>
            ) : (
              <button
                className="secondary-action"
                type="button"
                disabled={disabled}
                title={option.inactivePm ? "Поставщик неактивен в PM — заказ всё равно будет создан" : (!option.orderable ? "Нет наличия или цены" : "")}
                onClick={() => onPick(option)}
              >
                {busy ? <Loader2 className="spin" size={14} /> : null} {actionLabel}
              </button>
            )}
          </div>
        );
      })}
    </div>
  );
}
