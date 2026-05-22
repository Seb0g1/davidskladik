import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, RefreshCcw } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { OzonUnarchiveQueueSchema } from "../types";

const text = (value: unknown) => String(value ?? "").trim();

function moneyDate(value: unknown) {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
}

function numberValue(value: unknown) {
  return Number(value || 0) || 0;
}

export function RecoveryQueuePage() {
  const queryClient = useQueryClient();
  const queue = useQuery({
    queryKey: ["ozon", "unarchive-queue"],
    queryFn: () => fetchJson("/api/ozon/unarchive-queue?limit=1000", OzonUnarchiveQueueSchema),
  });
  const process = useMutation({
    mutationFn: () => fetchJson("/api/ozon/unarchive-queue/process", OzonUnarchiveQueueSchema, mutationBody({ limit: 100 })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["ozon", "unarchive-queue"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });
  const data = queue.data;
  const items = data?.items || [];
  return (
    <section className="page-section">
      <div className="section-title">
        <div>
          <span>Ozon autoarchive</span>
          <h2>Очередь восстановления</h2>
        </div>
        <button className="primary-action" type="button" onClick={() => process.mutate()} disabled={process.isPending || queue.isLoading}>
          {process.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Продолжить очередь Ozon
        </button>
      </div>
      <div className="summary-grid">
        <div><span>Всего в очереди</span><strong>{data?.total ?? 0}</strong></div>
        <div><span>Можно сейчас</span><strong>{data?.due ?? 0}</strong></div>
        <div><span>Осталось лимита</span><strong>{data?.availableToday ?? 0}</strong></div>
        <div><span>Следующая попытка</span><strong>{moneyDate(data?.nextRetryAt)}</strong></div>
      </div>
      {process.error ? <div className="inline-error">{String((process.error as Error).message || process.error)}</div> : null}
      {queue.error ? <div className="inline-error">{String((queue.error as Error).message || queue.error)}</div> : null}
      <div className="table-panel">
        <div className="table-head">
          <span>SKU</span><span>OfferId</span><span>Цель</span><span>Статус</span><span>Попытки</span><span>Когда</span>
        </div>
        {items.length ? items.map((item) => (
          <div className="table-row" key={text(item.queueKey || item.id || item.offerId)}>
            <span>{text(item.id) || "-"}</span>
            <span>{text(item.offerId) || "-"}</span>
            <span>{text(item.target) || "-"}</span>
            <span>{item.due ? "можно запускать" : text(item.warning || item.status || "pending")}</span>
            <span>{numberValue(item.attempts)}</span>
            <span>{moneyDate(item.nextRetryAt)}</span>
          </div>
        )) : <div className="empty-state">Очередь восстановления Ozon пуста.</div>}
      </div>
    </section>
  );
}
