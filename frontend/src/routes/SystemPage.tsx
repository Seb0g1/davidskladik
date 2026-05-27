import { useQuery } from "@tanstack/react-query";
import { Activity, AlertCircle, Database, RefreshCcw } from "lucide-react";
import { fetchJson } from "../api";
import { SystemStatusSchema } from "../types";

const asRecord = (value: unknown): Record<string, unknown> => value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const text = (value: unknown) => String(value ?? "").trim();
const list = (value: unknown): Array<Record<string, unknown>> => Array.isArray(value) ? value.filter((item) => item && typeof item === "object") as Array<Record<string, unknown>> : [];
const dateText = (value: unknown) => {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
};
const numberValue = (value: unknown) => Number(value || 0) || 0;

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
  const marketplaceQueue = asRecord(queues.marketplace);
  const runtime = asRecord(status.data?.runtime || asRecord(components.runtime));
  const runtimeMemory = asRecord(status.data?.memory || runtime.memory);
  const salesAutomation = asRecord(status.data?.salesAutomation);
  const daily = asRecord(status.data?.dailySync);
  const operations = asRecord(status.data?.operations);
  const failed = list(operations.failed);
  const active = list(operations.active);
  const slowEndpoints = asRecord(status.data?.slowEndpoints);
  const slowRequests = list(slowEndpoints.recent || status.data?.slowRequests);
  const slowByPath = list(slowEndpoints.byPath);
  const stateWarnings = list(status.data?.stateWarnings);
  const postgresTables = asRecord(status.data?.postgresTables || asRecord(components.postgresTables));
  const missingTables = Array.isArray(postgresTables.missing) ? postgresTables.missing.map(String) : [];
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
        <StatusCard label="PostgreSQL" value={asRecord(components.postgres).ok === false ? "error" : "ok"} tone={asRecord(components.postgres).ok === false ? "danger" : "success"} detail={missingTables.length ? `missing: ${missingTables.length}` : "tables ok"} />
        <StatusCard label="Redis/BullMQ" value={asRecord(components.redis).ok === false ? "error" : "ok"} tone={asRecord(components.redis).ok === false ? "danger" : "success"} />
        <StatusCard label="PriceMaster" value={asRecord(components.pricemaster).ok === false ? "error" : "ok"} tone={asRecord(components.pricemaster).ok === false ? "danger" : "success"} />
        <StatusCard label="Memory" value={`${Number(runtimeMemory.heapUsedMb || 0)} MB`} detail={`rss: ${Number(runtimeMemory.rssMb || 0)} MB uptime: ${Number(runtime.uptimeSec || 0)}s`} tone={Number(runtimeMemory.heapUsedMb || 0) > 1200 ? "warn" : "neutral"} />
        <StatusCard label="BullMQ jobs" value={Number(marketplaceQueue.active || 0)} detail={`waiting: ${Number(marketplaceQueue.waiting || 0)} delayed: ${Number(marketplaceQueue.delayed || 0)} failed: ${Number(marketplaceQueue.failed || 0)}`} tone={Number(marketplaceQueue.failed || 0) ? "warn" : "neutral"} />
        <StatusCard label="Daily sync" value={text(daily.status) || "-"} detail={dateText(daily.lastRunAt)} />
        <StatusCard label="Auto prices" value={Number(salesAutomation.total || 0)} detail={`queued: ${Number(salesAutomation.queued || 0)} verify: ${Number(salesAutomation.verificationPending || 0)}`} />
        <StatusCard label="PM timeout" value={Number(salesAutomation.pmTimeout || 0)} tone={Number(salesAutomation.pmTimeout || 0) ? "warn" : "success"} />
        <StatusCard label="Retry цен" value={Number(priceRetry.total || 0)} />
        <StatusCard
          label="Ozon recovery"
          value={numberValue(ozonQueue.total)}
          detail={`due: ${numberValue(ozonQueue.due)} verify: ${numberValue(ozonQueue.verificationPending)} limit: ${numberValue(ozonQueue.availableToday)}`}
          tone={numberValue(ozonQueue.verificationPending) || numberValue(ozonQueue.due) ? "warn" : "success"}
        />
        <StatusCard label="Slow requests" value={slowRequests.length} detail={`threshold: ${numberValue(slowEndpoints.thresholdMs)} ms`} tone={slowRequests.length ? "warn" : "success"} />
        <StatusCard label="State warnings" value={stateWarnings.length} tone={stateWarnings.length ? "warn" : "success"} />
        <StatusCard label="Active jobs" value={active.length} tone={active.length ? "warn" : "success"} />
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

      <div className="table-panel system-table">
        <div className="table-head"><span>Медленный endpoint</span><span>Сколько</span><span>Последний / максимум</span></div>
        {slowByPath.map((row) => (
          <div className="table-row" key={text(row.path)}>
            <span data-label="Endpoint">{text(row.method)} {text(row.path)}</span>
            <span data-label="Сколько">{numberValue(row.count)}</span>
            <span data-label="Время">{numberValue(row.lastMs)} ms / {numberValue(row.maxMs)} ms · {dateText(row.lastAt)}</span>
          </div>
        ))}
        {!slowByPath.length ? <div className="empty-state">Медленных запросов сейчас нет.</div> : null}
      </div>

      <div className="table-panel system-table">
        <div className="table-head"><span>State warning</span><span>Когда</span><span>Детали</span></div>
        {stateWarnings.slice().reverse().map((row, index) => (
          <div className="table-row" key={`${text(row.source)}-${index}`}>
            <span data-label="Источник">{text(row.source)}</span>
            <span data-label="Когда">{dateText(row.at)}</span>
            <span data-label="Детали">{text(row.detail) || "-"}</span>
          </div>
        ))}
        {!stateWarnings.length ? <div className="empty-state">Переходов на fallback и таймаутов записи состояния нет.</div> : null}
      </div>
    </section>
  );
}
