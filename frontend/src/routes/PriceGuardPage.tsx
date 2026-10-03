import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, Loader2, ShieldAlert, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { errorMessage } from "../lib/common";
import { toast } from "../lib/toast";
import "./price-guard.css";

// «Проверка цен»: цены, которые сервер не отправил сам (заглушка поставщика, рубли вместо долларов, рост в 3+ раза).
// «Отправить» — цена уходит на маркетплейс; «Не отправлять» — остаётся задержанной, пока цена не изменится.

function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

type Hold = {
  product_id: string; marketplace: string; target: string | null; offer_id: string; name: string;
  current_price: number | null; next_price: number; supplier: string | null; supplier_price: string | number | null;
  supplier_currency: string | null; reason: string; status: string; updated_at: string;
};
type HoldsResponse = { items: Hold[]; counts: Record<string, number> };

const TABS: Array<[string, string]> = [["pending", "Ждут решения"], ["rejected", "Не отправлять"], ["approved", "Отправлены"], ["cleared", "Исправились сами"]];
const rub = (v: number | null | undefined) => (v ? `${Math.round(v).toLocaleString("ru")} ₽` : "—");

export function PriceGuardPage() {
  const [tab, setTab] = useState("pending");
  const [picked, setPicked] = useState<string[]>([]);
  const queryClient = useQueryClient();
  const holds = useQuery({
    queryKey: ["price-guard", tab],
    queryFn: () => apiJson<HoldsResponse>(`/api/price-guard/holds?status=${tab}`),
    refetchInterval: 30_000,
  });
  const decide = useMutation({
    mutationFn: (body: { productIds: string[]; decision: "approve" | "reject" }) => apiJson<{ approved?: number; rejected?: number; sent?: number }>("/api/price-guard/holds/decide", mutationBody(body)),
    onSuccess: (data, body) => {
      toast.success(body.decision === "approve" ? `Отправлено: ${data.approved ?? body.productIds.length}` : `Не отправляем: ${data.rejected ?? body.productIds.length}`);
      setPicked([]);
      queryClient.invalidateQueries({ queryKey: ["price-guard"] });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const items = holds.data?.items || [];
  const counts = holds.data?.counts || {};
  const allPicked = items.length > 0 && picked.length === items.length;
  const editable = tab === "pending" || tab === "rejected";
  const ratio = (h: Hold) => (h.current_price ? h.next_price / h.current_price : 0);
  const sorted = useMemo(() => [...items].sort((a, b) => b.next_price - a.next_price), [items]);

  return (
    <div className="page-shell pg-page">
      <PageHeader
        title="Проверка цен"
        subtitle="Цены, которые склад не отправил на Ozon и Маркет сам: цена поставщика похожа на заглушку или на рубли вместо долларов, либо цена выросла в 3 раза и больше. Без вашего решения такая цена никуда не уходит."
      />
      <div className="pg-tabs" role="tablist">
        {TABS.map(([key, label]) => (
          <button key={key} type="button" role="tab" aria-selected={tab === key} className={tab === key ? "is-on" : ""} onClick={() => { setTab(key); setPicked([]); }}>
            {label}{counts[key] ? <b>{counts[key]}</b> : null}
          </button>
        ))}
      </div>

      {editable && items.length ? (
        <div className="pg-bulk">
          <label><input type="checkbox" checked={allPicked} onChange={() => setPicked(allPicked ? [] : items.map((h) => h.product_id))} /> Выбрать все ({items.length})</label>
          <button className="secondary-action" type="button" disabled={!picked.length || decide.isPending} onClick={() => decide.mutate({ productIds: picked, decision: "reject" })}><X size={14} /> Не отправлять</button>
          <button className="primary-action" type="button" disabled={!picked.length || decide.isPending} onClick={() => decide.mutate({ productIds: picked, decision: "approve" })}>
            {decide.isPending ? <Loader2 size={14} className="spin" /> : <Check size={14} />} Отправить выбранные ({picked.length})
          </button>
        </div>
      ) : null}

      {holds.isLoading ? <div className="empty-state"><Loader2 size={16} className="spin" /> Загружаем…</div> : null}
      {holds.isError ? <div className="inline-error">{errorMessage(holds.error)}</div> : null}
      {!holds.isLoading && !items.length ? (
        <div className="pg-empty"><ShieldAlert size={18} /> {tab === "pending" ? "Подозрительных цен нет — все цены уходят сами." : "Здесь пусто."}</div>
      ) : null}

      <div className="pg-list">
        {sorted.map((h) => (
          <div key={h.product_id} className={`pg-row${picked.includes(h.product_id) ? " is-picked" : ""}`}>
            {editable ? <input type="checkbox" checked={picked.includes(h.product_id)} onChange={() => setPicked((p) => (p.includes(h.product_id) ? p.filter((x) => x !== h.product_id) : [...p, h.product_id]))} aria-label={`Выбрать ${h.offer_id}`} /> : <span />}
            <div className="pg-main">
              <div className="pg-name">{h.name}</div>
              <div className="pg-meta">
                <span className={`pg-mp is-${h.marketplace}`}>{h.marketplace === "yandex" ? "Маркет" : "Ozon"}</span>
                {h.offer_id}{h.supplier ? ` · ${h.supplier}: ${h.supplier_price ?? "?"} ${h.supplier_currency === "RUB" ? "₽" : "$"}` : ""}
              </div>
              <div className="pg-reason"><AlertTriangle size={13} /> {h.reason}</div>
            </div>
            <div className="pg-prices">
              <span>сейчас {rub(h.current_price)}</span>
              <b className={ratio(h) >= 3 || ratio(h) === 0 ? "is-bad" : ""}>→ {rub(h.next_price)}</b>
            </div>
            {editable ? (
              <div className="pg-actions">
                {tab === "pending" ? <button className="icon-action" type="button" title="Не отправлять" onClick={() => decide.mutate({ productIds: [h.product_id], decision: "reject" })}><X size={15} /></button> : null}
                <button className="secondary-action compact" type="button" onClick={() => decide.mutate({ productIds: [h.product_id], decision: "approve" })}><Check size={13} /> Отправить</button>
              </div>
            ) : <span className="pg-when">{new Date(h.updated_at).toLocaleString("ru")}</span>}
          </div>
        ))}
      </div>
    </div>
  );
}
