// Cross-process sweep heartbeats (PLAN-HARDENING.md 4: "застывшие" detectors).
//
// The background sweeps (price/stock/zero-stock) run in the WORKER process, but the
// dashboard is served by the API process, so the in-process *SweepNextRunAt timers are
// invisible to it. Each sweep records a heartbeat row here after every tick; the API
// dashboard reads them and flags a sweep that has not completed within ~2.5 intervals
// (e.g. its event loop is pinned, or the scheduler stopped rescheduling itself).
//
// Storage is a plain lazily-created PG table (no prisma migration), mirroring
// app_notifications in 02f-notifications-core.js.

let sweepHeartbeatTableReady = false;
async function ensureSweepHeartbeatTable() {
  if (sweepHeartbeatTableReady) return true;
  const prisma = getPrisma();
  if (!prisma) return false;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS sweep_heartbeats (
      name TEXT PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'ok',
      interval_ms BIGINT NOT NULL DEFAULT 0,
      detail JSONB NOT NULL DEFAULT '{}'::jsonb,
      last_run_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  sweepHeartbeatTableReady = true;
  return true;
}

// Called by each sweep at the end of every tick (success or handled error). Best-effort:
// a heartbeat write must never break the sweep itself.
async function recordSweepHeartbeat(name, { status = "ok", intervalMs = 0, detail = {} } = {}) {
  const cleanName = cleanText(name);
  if (!cleanName) return false;
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return false;
  try {
    if (!(await ensureSweepHeartbeatTable())) return false;
    await prisma.$executeRawUnsafe(
      `INSERT INTO sweep_heartbeats (name, status, interval_ms, detail, last_run_at)
       VALUES ($1, $2, $3, $4::jsonb, now())
       ON CONFLICT (name) DO UPDATE
         SET status = EXCLUDED.status,
             interval_ms = EXCLUDED.interval_ms,
             detail = EXCLUDED.detail,
             last_run_at = now()`,
      cleanName,
      cleanText(status) || "ok",
      Math.max(0, Math.round(Number(intervalMs) || 0)),
      JSON.stringify(detail && typeof detail === "object" ? detail : {}),
    );
    return true;
  } catch (error) {
    logger.warn("sweep heartbeat write failed", { name: cleanName, detail: error?.message || String(error) });
    return false;
  }
}

// Pure: a sweep is "stale" once it has gone longer than staleFactor× its own interval
// without a heartbeat. Default 2.5× tolerates one missed tick + scheduling jitter.
function sweepHeartbeatStaleness(row, { now = Date.now(), staleFactor = 2.5 } = {}) {
  const intervalMs = Math.max(0, Number(row?.intervalMs ?? row?.interval_ms ?? 0) || 0);
  const lastRunMs = row?.lastRunAt || row?.last_run_at ? new Date(row.lastRunAt || row.last_run_at).getTime() : 0;
  const ageMs = lastRunMs > 0 ? Math.max(0, now - lastRunMs) : Number.MAX_SAFE_INTEGER;
  const thresholdMs = intervalMs > 0 ? Math.round(intervalMs * staleFactor) : 0;
  const stale = thresholdMs > 0 ? ageMs > thresholdMs : false;
  return { ageMs: ageMs === Number.MAX_SAFE_INTEGER ? null : ageMs, thresholdMs, stale };
}

async function readSweepHeartbeats({ now = Date.now() } = {}) {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return [];
  try {
    if (!(await ensureSweepHeartbeatTable())) return [];
    const rows = await prisma.$queryRawUnsafe(
      `SELECT name, status, interval_ms AS "intervalMs", detail, last_run_at AS "lastRunAt" FROM sweep_heartbeats ORDER BY name`,
    );
    return (rows || []).map((row) => {
      const staleness = sweepHeartbeatStaleness(row, { now });
      return {
        name: row.name,
        status: row.status,
        intervalMs: Number(row.intervalMs || 0),
        lastRunAt: row.lastRunAt ? new Date(row.lastRunAt).toISOString() : null,
        ageMs: staleness.ageMs,
        stale: staleness.stale,
        detail: row.detail && typeof row.detail === "object" ? row.detail : {},
      };
    });
  } catch (error) {
    logger.warn("sweep heartbeat read failed", { detail: error?.message || String(error) });
    return [];
  }
}
