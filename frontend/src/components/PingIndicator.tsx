import { useEffect, useState } from "react";

// Пинг до сервера для любого сотрудника: время ответа от его браузера до davidsklad.ru.
// Меряем HEAD-запросом к маленькому статичному файлу — без тела ответа и без запросов
// к базам, раз в 30 с и только пока вкладка открыта, поэтому нагрузки на сервер нет.
const PING_URL = "/login.js";
const INTERVAL_MS = 30_000;
const TIMEOUT_MS = 8_000;
const HISTORY = 10;

type Level = "good" | "ok" | "bad" | "down" | "idle";

function levelOf(ms: number | null, failed: boolean): Level {
  if (failed) return "down";
  if (ms == null) return "idle";
  if (ms < 150) return "good";
  if (ms < 400) return "ok";
  return "bad";
}

const LEVEL_TEXT: Record<Level, string> = {
  good: "Связь отличная",
  ok: "Связь нормальная, есть задержка",
  bad: "Связь медленная — страницы будут открываться дольше",
  down: "Нет связи с сервером",
  idle: "Измеряю…",
};

async function measure(): Promise<number> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const started = performance.now();
    const response = await fetch(`${PING_URL}?ping=${Date.now()}`, { method: "HEAD", cache: "no-store", credentials: "same-origin", signal: controller.signal });
    const elapsed = performance.now() - started;
    if (!response.ok && response.status !== 304) throw new Error(`HTTP ${response.status}`);
    return Math.round(elapsed);
  } finally {
    window.clearTimeout(timer);
  }
}

export function PingIndicator() {
  const [ms, setMs] = useState<number | null>(null);
  const [failed, setFailed] = useState(false);
  const [history, setHistory] = useState<number[]>([]);

  useEffect(() => {
    let cancelled = false;
    let busy = false;
    const run = async () => {
      if (busy || document.hidden) return;
      busy = true;
      try {
        // Первый запрос после простоя открывает соединение (DNS/TLS) и завышает цифру —
        // поэтому берём лучшее из двух быстрых замеров.
        const first = await measure();
        const second = await measure().catch(() => first);
        const value = Math.min(first, second);
        if (cancelled) return;
        setMs(value);
        setFailed(false);
        setHistory((prev) => [...prev, value].slice(-HISTORY));
      } catch {
        if (!cancelled) setFailed(true);
      } finally {
        busy = false;
      }
    };
    void run();
    const id = window.setInterval(() => void run(), INTERVAL_MS);
    const onVisible = () => { if (!document.hidden) void run(); };
    const onOffline = () => setFailed(true);
    const onOnline = () => void run();
    document.addEventListener("visibilitychange", onVisible);
    window.addEventListener("offline", onOffline);
    window.addEventListener("online", onOnline);
    return () => {
      cancelled = true;
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVisible);
      window.removeEventListener("offline", onOffline);
      window.removeEventListener("online", onOnline);
    };
  }, []);

  const level = levelOf(ms, failed);
  const sorted = history.slice().sort((a, b) => a - b);
  const median = sorted.length ? sorted[Math.floor(sorted.length / 2)] : null;
  const worst = sorted.length ? sorted[sorted.length - 1] : null;
  const title = [
    `Пинг до сервера: ${failed ? "нет ответа" : ms != null ? `${ms} мс` : "—"}`,
    LEVEL_TEXT[level],
    median != null ? `За последние ${history.length} замеров: обычно ${median} мс, худший ${worst} мс` : "",
    "Если медленно только у вас — проблема в вашем интернете; если у всех — в сервере.",
  ].filter(Boolean).join("\n");

  return (
    <div className={`ping-indicator ping-${level}`} title={title} role="status" aria-label={title}>
      <span className="ping-dot" />
      <span className="ping-value">{failed ? "нет связи" : ms != null ? `${ms} мс` : "…"}</span>
    </div>
  );
}
