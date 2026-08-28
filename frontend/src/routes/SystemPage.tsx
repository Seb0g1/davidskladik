import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, AlertCircle, AlertTriangle, CheckCircle, Database, HeartPulse, ListChecks, RefreshCcw, RotateCcw } from "lucide-react";
import { useState } from "react";
import { fetchJson } from "../api";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
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
    <section className="page-section system-page">
      <PageHeader
        title="Состояние ДавидСклад"
        subtitle="Health, очереди, кэши и технический статус системы в реальном времени."
        action={(
          <button className="secondary-action" type="button" onClick={() => status.refetch()} disabled={status.isFetching}>
            <RefreshCcw size={16} /> Обновить
          </button>
        )}
      />
      {status.error ? <div className="inline-error">{String((status.error as Error).message || status.error)}</div> : null}
      <section className="dashboard-metrics">
        <Stat label="Health" value={status.data?.ok ? "ok" : "check"} tone={status.data?.ok ? "success" : "warn"} icon={<HeartPulse size={18} />} />
        <Stat label="Активные задачи" value={active.length} tone={active.length ? "warn" : "success"} icon={<ListChecks size={18} />} />
        <Stat label="Ошибки операций" value={failed.length} tone={failed.length ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
        <Stat label="Retry цен" value={Number(priceRetry.total || 0)} tone={Number(priceRetry.total || 0) ? "warn" : "success"} icon={<RotateCcw size={18} />} />
      </section>
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
          detail={`due: ${numberValue(ozonQueue.due)} verify: ${numberValue(ozonQueue.verificationPending)} limit: ${ozonQueue.dailyLimit == null ? "off" : numberValue(ozonQueue.availableToday)}`}
          tone={numberValue(ozonQueue.verificationPending) || numberValue(ozonQueue.due) ? "warn" : "success"}
        />
        <StatusCard label="Slow requests" value={slowRequests.length} detail={`threshold: ${numberValue(slowEndpoints.thresholdMs)} ms`} tone={slowRequests.length ? "warn" : "success"} />
        <StatusCard label="State warnings" value={stateWarnings.length} tone={stateWarnings.length ? "warn" : "success"} />
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

      {status.data?.supplierLedger ? (
        <div className="card" style={{marginTop: 12}}>
          <div className="section-title"><h3>Журнал поставщиков</h3></div>
          <div style={{display:"flex", gap:16, padding:8}}>
            {(() => {
              const sl = asRecord(status.data?.supplierLedger);
              return (<>
                <span>Строк закупки: {numberValue(sl.pickedRows)}</span>
                <span>Долгов: {numberValue(sl.debtEntries)}</span>
                <span>Пропущено: {numberValue(sl.missingDebtEntries)}</span>
              </>);
            })()}
          </div>
        </div>
      ) : null}

      <AppErrorJournal />
    </section>
  );
}

type AppError = {
  id: string;
  type: string;
  source: string;
  message: string;
  context?: Record<string, unknown>;
  resolvedAt: string | null;
  createdAt: string;
};

const SINCE_OPTIONS = [
  { label: "1ч", value: "1h" },
  { label: "6ч", value: "6h" },
  { label: "24ч", value: "24h" },
  { label: "7д", value: "7d" },
];

function AppErrorJournal() {
  const qc = useQueryClient();
  const [since, setSince] = useState("24h");

  const errorsQ = useQuery({
    queryKey: ["system-errors", since],
    queryFn: () => fetch(`/api/system/errors?since=${since}&limit=50`).then((r) => r.json()) as Promise<{ ok: boolean; errors: AppError[]; total: number }>,
    refetchInterval: 60_000,
  });

  const resolveMut = useMutation({
    mutationFn: (id: string) => fetch(`/api/system/errors/${id}/resolve`, { method: "PATCH" }).then((r) => r.json()),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["system-errors"] }),
  });

  const errors: AppError[] = errorsQ.data?.errors ?? [];

  return (
    <div className="card" style={{ marginTop: 16 }}>
      <div className="section-title" style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px" }}>
        <h3 style={{ margin: 0, display: "flex", alignItems: "center", gap: 6 }}>
          <AlertTriangle size={16} /> Журнал ошибок
          {errorsQ.data?.total ? <span className="badge warn" style={{ marginLeft: 4 }}>{errorsQ.data.total}</span> : null}
        </h3>
        <div style={{ display: "flex", gap: 4 }}>
          {SINCE_OPTIONS.map((opt) => (
            <button
              key={opt.value}
              type="button"
              className={since === opt.value ? "action-button active" : "secondary-action"}
              style={{ padding: "2px 8px", fontSize: 12 }}
              onClick={() => setSince(opt.value)}
            >
              {opt.label}
            </button>
          ))}
          <button type="button" className="secondary-action" style={{ padding: "2px 8px", fontSize: 12 }} onClick={() => errorsQ.refetch()} disabled={errorsQ.isFetching}>
            <RefreshCcw size={12} />
          </button>
        </div>
      </div>
      {errorsQ.error ? <div className="inline-error" style={{ margin: 8 }}>{String((errorsQ.error as Error).message)}</div> : null}
      <div className="table-panel system-table">
        <div className="table-head">
          <span>Время</span>
          <span>Тип</span>
          <span>Источник</span>
          <span>Сообщение</span>
          <span>Действие</span>
        </div>
        {errors.map((err) => (
          <div className="table-row" key={err.id}>
            <span data-label="Время" style={{ fontSize: 12, whiteSpace: "nowrap" }}>{dateText(err.createdAt)}</span>
            <span data-label="Тип"><code style={{ fontSize: 11 }}>{err.type}</code></span>
            <span data-label="Источник" style={{ fontSize: 11, opacity: 0.8 }}>{err.source}</span>
            <span data-label="Сообщение" style={{ fontSize: 12 }}>{err.message}</span>
            <span data-label="Действие">
              <button
                type="button"
                className="secondary-action"
                style={{ padding: "2px 8px", fontSize: 11 }}
                disabled={resolveMut.isPending}
                onClick={() => resolveMut.mutate(err.id)}
                title="Отметить решённой"
              >
                <CheckCircle size={12} /> Решено
              </button>
            </span>
          </div>
        ))}
        {!errors.length && !errorsQ.isFetching ? (
          <div className="empty-state">Ошибок за выбранный период нет.</div>
        ) : null}
      </div>
    </div>
  );
}
