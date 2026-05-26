import { useQuery } from "@tanstack/react-query";
import { Activity, AlertCircle, Database, RefreshCcw } from "lucide-react";
import { fetchJson } from "../api";
import { SystemStatusSchema } from "../types";

const asRecord = (value: unknown): Record<string, unknown> => value && typeof value === "object" ? value as Record<string, unknown> : {};
const text = (value: unknown) => String(value ?? "").trim();
const dateText = (value: unknown) => {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
};

function StatusCard({ label, value, detail, tone = "neutral" }: { label: string; value: string | number; detail?: string; tone?: "success" | "warn" | "danger" | "neutral" }) {
  return (
    <div className={`system-card ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
    </div>
  );
}

export function SystemPage() {
  const status = useQuery({
    queryKey: ["system", "status"],
    queryFn: () => fetchJson("/api/system/status", SystemStatusSchema),
    refetchInterval: 30000,
  });
  const health = asRecord(status.data?.health);
  const components = asRecord(health.components);
  const queues = asRecord(status.data?.queues);
  const priceRetry = asRecord(queues.priceRetry);
  const ozonQueue = asRecord(queues.ozonUnarchive);
  const daily = asRecord(status.data?.dailySync);
  const operations = asRecord(status.data?.operations);
  const failed = Array.isArray(operations.failed) ? operations.failed : [];
  return (
    <section className="page-section">
      <div className="section-title">
        <div>
          <span>System dashboard</span>
          <h2>Состояние ДавидСклад</h2>
        </div>
        <button className="secondary-action" type="button" onClick={() => status.refetch()} disabled={status.isFetching}>
          <RefreshCcw size={16} /> Обновить
        </button>
      </div>
      {status.error ? <div className="inline-error">{String((status.error as Error).message || status.error)}</div> : null}
      <div className="summary-grid">
        <StatusCard label="Health" value={status.data?.ok ? "green" : "check"} tone={status.data?.ok ? "success" : "warn"} detail={dateText(status.data?.time)} />
        <StatusCard label="PostgreSQL" value={asRecord(components.postgres).ok === false ? "error" : "ok"} tone={asRecord(components.postgres).ok === false ? "danger" : "success"} />
        <StatusCard label="Redis/BullMQ" value={asRecord(components.redis).ok === false ? "error" : "ok"} tone={asRecord(components.redis).ok === false ? "danger" : "success"} />
        <StatusCard label="PriceMaster" value={asRecord(components.pricemaster).ok === false ? "error" : "ok"} tone={asRecord(components.pricemaster).ok === false ? "danger" : "success"} />
        <StatusCard label="Daily sync" value={text(daily.status) || "-"} detail={dateText(daily.lastRunAt)} />
        <StatusCard label="Retry цен" value={Number(priceRetry.total || 0)} />
        <StatusCard label="Ozon recovery" value={Number(ozonQueue.total || 0)} detail={`due: ${Number(ozonQueue.due || 0)}`} />
        <StatusCard label="Ошибки операций" value={failed.length} tone={failed.length ? "warn" : "success"} />
      </div>
      <div className="table-panel system-table">
        <div className="table-head"><span>Компонент</span><span>Статус</span><span>Детали</span></div>
        {Object.entries(components).map(([name, value]) => {
          const row = asRecord(value);
          const missing = Array.isArray(row.missing) ? row.missing.join(", ") : "";
          return (
            <div className="table-row" key={name}>
              <span data-label="Компонент"><Database size={14} /> {name}</span>
              <span data-label="Статус">{row.ok === false ? <AlertCircle size={14} /> : <Activity size={14} />} {row.ok === false ? "error" : "ok"}</span>
              <span data-label="Детали">{missing || text(row.error || row.mode || row.queueMode || row.accounts || row.shops) || "-"}</span>
            </div>
          );
        })}
      </div>
    </section>
  );
}
