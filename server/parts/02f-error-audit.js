// Structured error audit (PLAN-HARDENING.md 4: «единый формат ошибок + еженедельный
// авто-отчёт топ-N классов ошибок»).
//
// Every server-side request error and background-warning is classified into a stable
// "class" (variable parts — ids, numbers, quotes, urls — collapsed) and appended to a
// lazily-created PG table. `/api/dashboard/error-report` aggregates the top classes over a
// window; a weekly job sends the same digest to Telegram and prunes old rows. Best-effort:
// a failed audit write must never affect the request or the background job.

const errorAuditEnabled = process.env.ERROR_AUDIT_ENABLED !== "false";
const errorAuditRetentionDays = Math.max(7, Number(process.env.ERROR_AUDIT_RETENTION_DAYS || 30) || 30);
const errorAuditDigestIntervalMs = Math.max(60 * 60_000, Number(process.env.ERROR_AUDIT_DIGEST_INTERVAL_HOURS || 168) * 3_600_000 || 168 * 3_600_000);
let errorAuditDigestTimer = null;
let errorAuditTableReady = false;

// Pure: collapse the variable parts of a message so the same error shape clusters into one
// class. Unit-tested — keep deterministic.
function classifyErrorMessage(message) {
  let text = cleanText(message).toLowerCase();
  if (!text) return "unknown";
  text = text
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, "<uuid>")
    .replace(/0x[0-9a-f]+/g, "<hex>")
    .replace(/\b[0-9a-f]{16,}\b/g, "<hex>")
    .replace(/https?:\/\/\S+/g, "<url>")
    .replace(/["'`][^"'`]*["'`]/g, "<str>")
    .replace(/\b\d[\d.,:]*\b/g, "<n>")
    .replace(/\s+/g, " ")
    .trim();
  return text.slice(0, 120) || "unknown";
}

async function ensureErrorEventsTable() {
  if (errorAuditTableReady) return true;
  const prisma = getPrisma();
  if (!prisma) return false;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS error_events (
      id BIGSERIAL PRIMARY KEY,
      class TEXT NOT NULL,
      source TEXT NOT NULL DEFAULT '',
      message TEXT,
      code TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS error_events_created ON error_events (created_at DESC)`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS error_events_class ON error_events (class)`);
  errorAuditTableReady = true;
  return true;
}

function recordErrorEvent({ source = "", message = "", code = "" } = {}) {
  if (!errorAuditEnabled) return;
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return;
  const cls = classifyErrorMessage(message || code);
  // Fire-and-forget: never block the caller (request handler / scheduler) on the audit write.
  Promise.resolve()
    .then(async () => {
      if (!(await ensureErrorEventsTable())) return;
      await prisma.$executeRawUnsafe(
        `INSERT INTO error_events (class, source, message, code) VALUES ($1, $2, $3, $4)`,
        cls,
        cleanText(source).slice(0, 80),
        cleanText(message).slice(0, 500),
        cleanText(code).slice(0, 80) || null,
      );
    })
    .catch((error) => logger.warn("error audit write failed", { detail: error?.message || String(error) }));
}

async function readErrorReport({ days = 7, limit = 15 } = {}) {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { days, total: 0, classes: [] };
  try {
    if (!(await ensureErrorEventsTable())) return { days, total: 0, classes: [] };
    const windowDays = Math.max(1, Math.min(90, Math.round(Number(days) || 7)));
    const topLimit = Math.max(1, Math.min(100, Math.round(Number(limit) || 15)));
    const rows = await prisma.$queryRawUnsafe(`
      SELECT class, COUNT(*)::int AS count, MAX(created_at) AS "lastAt",
             (ARRAY_AGG(DISTINCT source))[1:3] AS sources
      FROM error_events
      WHERE created_at > now() - interval '${windowDays} days'
      GROUP BY class
      ORDER BY count DESC
      LIMIT ${topLimit}
    `);
    const totalRow = await prisma.$queryRawUnsafe(`SELECT COUNT(*)::int AS n FROM error_events WHERE created_at > now() - interval '${windowDays} days'`);
    return {
      days: windowDays,
      total: Number(totalRow?.[0]?.n || 0),
      classes: (rows || []).map((row) => ({
        class: row.class,
        count: Number(row.count || 0),
        lastAt: row.lastAt ? new Date(row.lastAt).toISOString() : null,
        sources: Array.isArray(row.sources) ? row.sources.filter(Boolean) : [],
      })),
    };
  } catch (error) {
    logger.warn("error report read failed", { detail: error?.message || String(error) });
    return { days, total: 0, classes: [] };
  }
}

app.get("/api/dashboard/error-report", requireAdmin, async (request, response, next) => {
  try {
    const report = await readErrorReport({ days: request.query.days, limit: request.query.limit });
    response.json({ ok: true, ...report });
  } catch (error) {
    next(error);
  }
});

async function runErrorAuditDigest({ source = "schedule" } = {}) {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  try {
    if (!(await ensureErrorEventsTable())) return { status: "no_table" };
    // Prune old rows first so the table stays bounded.
    await prisma.$executeRawUnsafe(`DELETE FROM error_events WHERE created_at < now() - interval '${errorAuditRetentionDays} days'`).catch(() => null);
    const report = await readErrorReport({ days: 7, limit: 10 });
    logger.info("error_audit_digest", { source, total: report.total, top: report.classes.slice(0, 5).map((c) => `${c.class}=${c.count}`) });
    if (report.total > 0 && typeof healthAlertTelegramConfigured === "function" && healthAlertTelegramConfigured()) {
      const lines = report.classes.slice(0, 10).map((c, i) => `${i + 1}. ${c.count}× ${c.class}`);
      await sendHealthAlertTelegram(`DavidSklad · отчёт об ошибках за 7д (всего ${report.total})\n${lines.join("\n")}`);
    }
    return { status: "ok", total: report.total };
  } catch (error) {
    logger.warn("error audit digest failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  }
}

function scheduleErrorAuditDigest(delayMs = errorAuditDigestIntervalMs) {
  if (!errorAuditEnabled) return;
  if (errorAuditDigestTimer) clearTimeout(errorAuditDigestTimer);
  const normalizedDelay = Math.max(60_000, Number(delayMs) || errorAuditDigestIntervalMs);
  errorAuditDigestTimer = setTimeout(async () => {
    try {
      await runErrorAuditDigest({ source: "schedule" });
    } catch (error) {
      logger.warn("error audit digest tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleErrorAuditDigest(errorAuditDigestIntervalMs);
    }
  }, normalizedDelay);
  errorAuditDigestTimer.unref?.();
}
