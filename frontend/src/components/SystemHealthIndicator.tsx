import { useEffect, useRef, useState } from "react";
import { Activity, AlertTriangle, CheckCircle2, Server, XCircle } from "lucide-react";
import { fetchJson } from "../api";
import { LiveStatusSchema } from "../types";

type Health = "ok" | "warn" | "error";
type LiveStatus = ReturnType<typeof LiveStatusSchema.parse>;

const POLL_INTERVAL_MS = 30_000;
const SYNC_STALE_HOURS = 36;
const PRICE_MASTER_STALE_HOURS = 24;

function ageHours(iso?: string | null): number {
  if (!iso) return Infinity;
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return Infinity;
  return (Date.now() - date.getTime()) / 3_600_000;
}

function relativeTime(iso?: string | null): string {
  if (!iso) return "нет данных";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "нет данных";
  const minutes = Math.round((Date.now() - date.getTime()) / 60_000);
  if (minutes < 1) return "только что";
  if (minutes < 60) return `${minutes} мин назад`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} ч назад`;
  return `${Math.round(hours / 24)} дн назад`;
}

function HealthIcon({ health, size = 14 }: { health: Health; size?: number }) {
  if (health === "ok") return <CheckCircle2 size={size} className="health-ok" />;
  if (health === "warn") return <AlertTriangle size={size} className="health-warn" />;
  return <XCircle size={size} className="health-error" />;
}

function worstHealth(values: Health[]): Health {
  if (values.includes("error")) return "error";
  if (values.includes("warn")) return "warn";
  return "ok";
}

export function SystemHealthIndicator() {
  const [data, setData] = useState<LiveStatus | null>(null);
  const [loadError, setLoadError] = useState(false);
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    const refresh = () => {
      fetchJson("/api/live-status", LiveStatusSchema)
        .then((payload) => {
          if (cancelled) return;
          setData(payload);
          setLoadError(false);
        })
        .catch(() => {
          if (!cancelled) setLoadError(true);
        });
    };
    refresh();
    const timer = window.setInterval(refresh, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    const onDocClick = (event: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, []);

  const queue = data?.queue;
  const workerHealth: Health = loadError || !data
    ? "error"
    : queue?.enabled
      ? (queue.error || !queue.consumerReady ? "error" : "ok")
      : "ok";

  const queueHealth: Health = loadError || !data
    ? "error"
    : queue?.enabled
      ? (queue.degraded ? "error" : (Number(queue.counts?.failed || 0) > 0 ? "warn" : "ok"))
      : "ok";

  const lastSyncAt = data?.dailySync?.lastRunAt || data?.warehouse?.updatedAt || null;
  const syncHealth: Health = loadError || !data
    ? "error"
    : data.dailySync?.error
      ? "error"
      : ageHours(lastSyncAt) > SYNC_STALE_HOURS ? "warn" : "ok";

  const priceMasterHealth: Health = loadError || !data
    ? "error"
    : (data.priceMaster?.error || !data.priceMaster?.items)
      ? "error"
      : ageHours(data.priceMaster?.updatedAt) > PRICE_MASTER_STALE_HOURS ? "warn" : "ok";

  const overall = worstHealth([workerHealth, queueHealth, syncHealth, priceMasterHealth]);

  const queueDetail = !data
    ? "нет данных"
    : queue?.enabled
      ? (queue.error || `ожидает ${queue.counts?.waiting ?? 0}, активно ${queue.counts?.active ?? 0}, ошибок ${queue.counts?.failed ?? 0}`)
      : "встроенный режим (без очереди)";

  const workerDetail = !data
    ? "нет данных"
    : queue?.enabled
      ? (queue.consumerReady ? "обработчик активен" : (queue.error || "обработчик недоступен"))
      : "встроенный режим";

  const priceMasterDetail = !data
    ? "нет данных"
    : data.priceMaster?.error
      ? data.priceMaster.error
      : data.priceMaster?.items
        ? `${data.priceMaster.items.toLocaleString("ru-RU")} позиций · обновлён ${relativeTime(data.priceMaster.updatedAt)}`
        : "нет данных";

  return (
    <div className="health-wrap" ref={wrapRef}>
      <button className={`health-button is-${overall}`} type="button" onClick={() => setOpen((value) => !value)} title="Здоровье системы">
        <Activity size={18} />
        <span className={`health-dot is-${overall}`} aria-hidden="true" />
      </button>
      {open ? (
        <div className="health-dropdown">
          <div className="health-head">
            <strong><Server size={14} /> Здоровье системы</strong>
            {data?.now ? <small>{relativeTime(data.now)}</small> : null}
          </div>
          <div className="health-list">
            <div className="health-row">
              <HealthIcon health={workerHealth} />
              <span>Воркер</span>
              <small>{workerDetail}</small>
            </div>
            <div className="health-row">
              <HealthIcon health={queueHealth} />
              <span>Очередь</span>
              <small>{queueDetail}</small>
            </div>
            <div className="health-row">
              <HealthIcon health={syncHealth} />
              <span>Последний синк</span>
              <small>{relativeTime(lastSyncAt)}</small>
            </div>
            <div className="health-row">
              <HealthIcon health={priceMasterHealth} />
              <span>PriceMaster</span>
              <small>{priceMasterDetail}</small>
            </div>
          </div>
          {loadError ? <div className="health-error-banner">Не удалось получить /api/live-status</div> : null}
        </div>
      ) : null}
    </div>
  );
}
