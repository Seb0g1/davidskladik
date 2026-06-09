function ozonUnarchiveDateKey(date = new Date()) {
  const value = date instanceof Date ? date : new Date(date);
  const ms = Number.isFinite(value.getTime()) ? value.getTime() : Date.now();
  return new Date(ms + 3 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

function nextOzonUnarchiveScheduledRunAt(date = new Date()) {
  const value = date instanceof Date ? date : new Date(date);
  const base = Number.isFinite(value.getTime()) ? value : new Date();
  const moscowOffsetMs = 3 * 60 * 60 * 1000;
  const moscowNow = new Date(base.getTime() + moscowOffsetMs);
  const scheduledMoscow = new Date(Date.UTC(
    moscowNow.getUTCFullYear(),
    moscowNow.getUTCMonth(),
    moscowNow.getUTCDate(),
    3,
    0,
    0,
    0,
  ));
  if (moscowNow.getTime() >= scheduledMoscow.getTime()) {
    scheduledMoscow.setUTCDate(scheduledMoscow.getUTCDate() + 1);
  }
  return new Date(scheduledMoscow.getTime() - moscowOffsetMs);
}

function nextOzonUnarchiveRetryAt(date = new Date()) {
  return nextOzonUnarchiveScheduledRunAt(date).toISOString();
}

function nextOzonUnarchiveVisibilityRetryAt(date = new Date()) {
  const value = date instanceof Date ? date : new Date(date);
  const base = Number.isFinite(value.getTime()) ? value : new Date();
  return new Date(base.getTime() + ozonUnarchiveVisibilityRetryMinutes * 60 * 1000).toISOString();
}

function ozonUnarchiveQueueKey(item = {}) {
  const warehouseProductId = cleanText(item.warehouseProductId || item.warehouse_product_id || item.id || item.productUuid);
  return [
    cleanText(item.target),
    warehouseProductId,
    cleanText(item.offerId || item.offer_id),
  ].filter(Boolean).join(":");
}

function ozonNumericProductId(value) {
  const text = cleanText(value);
  if (!/^\d+$/.test(text)) return "";
  return Number(text) > 0 ? text : "";
}

function normalizeOzonUnarchiveQueueItem(item = {}, fallback = {}) {
  const now = new Date().toISOString();
  const id = cleanText(item.id || item.warehouseProductId || item.warehouse_product_id || fallback.id || fallback.warehouseProductId);
  const ozonProductId = ozonNumericProductId(item.ozonProductId || item.ozon_product_id || item.productId || item.product_id || fallback.ozonProductId || fallback.productId);
  return {
    id,
    warehouseProductId: id,
    productId: ozonProductId,
    ozonProductId,
    offerId: cleanText(item.offerId || item.offer_id || fallback.offerId),
    target: cleanText(item.target || fallback.target),
    marketplace: "ozon",
    status: cleanText(item.status || "pending") || "pending",
    queuedAt: cleanText(item.queuedAt || item.queued_at) || now,
    nextRetryAt: cleanText(item.nextRetryAt || item.next_retry_at) || nextOzonUnarchiveRetryAt(),
    lastAttemptAt: cleanText(item.lastAttemptAt || item.last_attempt_at),
    attempts: Math.max(0, Number(item.attempts || 0) || 0),
    warning: cleanText(item.warning || "ozon_unarchive_daily_limit_queued"),
    error: cleanText(item.error),
  };
}

function normalizeOzonUnarchiveQueue(queue = {}) {
  const items = Array.isArray(queue.items) ? queue.items : [];
  const daily = queue.daily && typeof queue.daily === "object" ? queue.daily : {};
  const byKey = new Map();
  for (const item of items) {
    const normalized = normalizeOzonUnarchiveQueueItem(item);
    const key = ozonUnarchiveQueueKey(normalized);
    if (key && normalized.status !== "done") byKey.set(key, normalized);
  }
  return {
    updatedAt: cleanText(queue.updatedAt),
    daily,
    items: Array.from(byKey.values()),
  };
}

async function readOzonUnarchiveDailyState() {
  try {
    const text = await fs.readFile(ozonUnarchiveDailyStatePath, "utf8");
    const parsed = JSON.parse(text || "{}");
    return parsed.daily && typeof parsed.daily === "object" ? parsed.daily : {};
  } catch (error) {
    if (error.code !== "ENOENT" && !(error instanceof SyntaxError)) {
      logger.warn("read ozon unarchive daily state failed", { detail: error?.message || String(error) });
    }
  }
  try {
    const text = await fs.readFile(ozonUnarchiveQueuePath, "utf8");
    const parsed = JSON.parse(text || "{}");
    return parsed.daily && typeof parsed.daily === "object" ? parsed.daily : {};
  } catch (_error) {
    return {};
  }
}

async function writeOzonUnarchiveDailyState(daily = {}) {
  await fs.mkdir(dataDir, { recursive: true });
  const payload = {
    updatedAt: new Date().toISOString(),
    daily: daily && typeof daily === "object" ? daily : {},
  };
  const tmpPath = `${ozonUnarchiveDailyStatePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmpPath, ozonUnarchiveDailyStatePath);
}

function ozonUnarchiveQueueItemToPostgres(item = {}) {
  const normalized = normalizeOzonUnarchiveQueueItem(item);
  return {
    queueKey: ozonUnarchiveQueueKey(normalized) || crypto.randomUUID(),
    productId: cleanText(normalized.warehouseProductId || normalized.id) || null,
    offerId: cleanText(normalized.offerId || normalized.id) || "unknown",
    target: cleanText(normalized.target) || null,
    status: normalized.status === "done" || normalized.status === "success" ? "success" : (["pending", "processing", "failed", "delayed"].includes(normalized.status) ? normalized.status : "pending"),
    queuedAt: toDateOrNull(normalized.queuedAt) || new Date(),
    nextRetryAt: toDateOrNull(normalized.nextRetryAt),
    lastAttemptAt: toDateOrNull(normalized.lastAttemptAt),
    attempts: Math.max(0, Number(normalized.attempts || 0) || 0),
    warning: cleanText(normalized.warning) || null,
    error: cleanText(normalized.error) || null,
    raw: normalized,
  };
}

function ozonUnarchiveQueueItemUpdateData(data = {}) {
  return {
    productId: data.productId,
    offerId: data.offerId,
    target: data.target,
    status: data.status,
    queuedAt: data.queuedAt,
    nextRetryAt: data.nextRetryAt,
    lastAttemptAt: data.lastAttemptAt,
    attempts: data.attempts,
    warning: data.warning,
    error: data.error,
    raw: data.raw,
  };
}

function ozonUnarchiveQueueItemFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeOzonUnarchiveQueueItem({
    ...raw,
    id: raw.id || row.productId || row.offerId,
    warehouseProductId: raw.warehouseProductId || raw.warehouse_product_id || row.productId,
    productId: raw.productId || raw.product_id || raw.ozonProductId || raw.ozon_product_id,
    ozonProductId: raw.ozonProductId || raw.ozon_product_id || raw.productId || raw.product_id,
    offerId: row.offerId,
    target: row.target,
    status: row.status === "success" ? "done" : row.status,
    queuedAt: row.queuedAt?.toISOString?.() || raw.queuedAt,
    nextRetryAt: row.nextRetryAt?.toISOString?.() || raw.nextRetryAt,
    lastAttemptAt: row.lastAttemptAt?.toISOString?.() || raw.lastAttemptAt,
    attempts: row.attempts,
    warning: row.warning || raw.warning,
    error: row.error || raw.error,
  });
}
