// Non-blocking centralized error journal. Call recordAppError() from catch blocks
// so errors are queryable from /api/system/errors without SSH.

const { randomUUID } = require("crypto");

function recordAppError(type, source, message, context = {}) {
  const prisma = getPrisma?.();
  if (!prisma) return;
  prisma.appError
    .create({
      data: {
        id: randomUUID(),
        type: String(type || "unknown").slice(0, 100),
        source: String(source || "").slice(0, 200),
        message: String(message || "").slice(0, 1000),
        context: context && typeof context === "object" ? context : {},
      },
    })
    .catch((e) => logger.warn("error_tracker_write_failed", { detail: e?.message || String(e) }));
}

async function pruneOldAppErrors() {
  const prisma = getPrisma?.();
  if (!prisma) return;
  const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  const deleted = await prisma.appError.deleteMany({ where: { createdAt: { lt: cutoff } } });
  logger.info("app_errors_pruned", { count: deleted.count });
}
