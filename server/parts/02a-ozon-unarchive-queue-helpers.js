// Key of Ozon's auto-archive restore quota window. Ozon resets the 100/day limit at 03:00 Moscow
// time = 00:00 UTC, so the window is the UTC calendar day. The old Moscow-date key rolled over at
// 00:00 MSK: restores sent between 00:00 and 03:00 hit Ozon's previous window but were charged to
// the new local day, so the day after ended ~10 short of 100.
function ozonUnarchiveDateKey(date = new Date()) {
  const value = date instanceof Date ? date : new Date(date);
  const ms = Number.isFinite(value.getTime()) ? value.getTime() : Date.now();
  return new Date(ms).toISOString().slice(0, 10);
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
  const base = date instanceof Date ? date : new Date(date);
  const ms = Number.isFinite(base.getTime()) ? base.getTime() : Date.now();
  const retryHours = Math.max(1, Number(process.env.OZON_UNARCHIVE_RETRY_HOURS || 5) || 5);
  return new Date(ms + retryHours * 60 * 60 * 1000).toISOString();
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

// Restore quota usage per Ozon account and window. The api, the worker and the queue jobs all
// restore products, and every queue write used to overwrite this counter with the absolute value
// read at the start of that run, losing the others' increments. Usage now only changes through
// recordOzonUnarchiveUsage (Redis INCRBY, shared by all processes) and closeOzonUnarchiveWindow
// (set when Ozon itself answers "restore limit exceeded" — the only reliable end-of-day signal).
// daily[windowKey][target] = used (or the limit once closed), daily[windowKey]["<target>#closed"].
const ozonUnarchiveUsageTtlSeconds = 3 * 24 * 60 * 60;
let ozonUnarchiveUsageRedisClient = null;

function ozonUnarchiveUsageRedis() {
  if (ozonUnarchiveUsageRedisClient === false) return null;
  if (ozonUnarchiveUsageRedisClient) return ozonUnarchiveUsageRedisClient;
  if (!redisUrl) {
    ozonUnarchiveUsageRedisClient = false;
    return null;
  }
  try {
    const Redis = require("ioredis");
    ozonUnarchiveUsageRedisClient = new Redis(redisUrl, { maxRetriesPerRequest: 2, enableReadyCheck: false });
    ozonUnarchiveUsageRedisClient.on("error", () => {});
    return ozonUnarchiveUsageRedisClient;
  } catch {
    ozonUnarchiveUsageRedisClient = false;
    return null;
  }
}

function ozonUnarchiveUsageRedisKeys(target = "", windowKey = ozonUnarchiveDateKey()) {
  const targetKey = cleanText(target) || "default";
  return {
    used: `ozon:unarchive:used:${windowKey}:${targetKey}`,
    closed: `ozon:unarchive:closed:${windowKey}:${targetKey}`,
  };
}

function ozonUnarchiveUsageTargets() {
  const ids = (typeof getOzonAccounts === "function" ? getOzonAccounts() : []).map((account) => cleanText(account.id)).filter(Boolean);
  return Array.from(new Set([...ids, "ozon", "default"]));
}

async function readOzonUnarchiveUsageFile() {
  try {
    const parsed = JSON.parse(await fs.readFile(ozonUnarchiveDailyStatePath, "utf8") || "{}");
    return parsed.daily && typeof parsed.daily === "object" ? parsed.daily : {};
  } catch {
    return {};
  }
}

async function writeOzonUnarchiveUsageFile(daily = {}) {
  await fs.mkdir(dataDir, { recursive: true });
  const windowKey = ozonUnarchiveDateKey();
  const kept = Object.fromEntries(Object.entries(daily).filter(([key]) => key >= windowKey));
  const tmpPath = `${ozonUnarchiveDailyStatePath}.${process.pid}.${Date.now()}.${crypto.randomUUID()}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify({ updatedAt: new Date().toISOString(), daily: kept }, null, 2), "utf8");
  try {
    await fs.rename(tmpPath, ozonUnarchiveDailyStatePath);
  } catch (renameError) {
    if (renameError?.code !== "ENOENT") throw renameError;
    await fs.unlink(tmpPath).catch(() => {});
  }
}

// Count auto-archived products Ozon accepted for restore (every caller, bypass or not).
async function recordOzonUnarchiveUsage(target = "", count = 0) {
  const amount = Math.max(0, Math.round(Number(count) || 0));
  if (!amount) return null;
  const windowKey = ozonUnarchiveDateKey();
  const redis = ozonUnarchiveUsageRedis();
  if (redis) {
    try {
      const keys = ozonUnarchiveUsageRedisKeys(target, windowKey);
      const [[, used]] = await redis.multi().incrby(keys.used, amount).expire(keys.used, ozonUnarchiveUsageTtlSeconds).exec();
      return Number(used) || 0;
    } catch (error) {
      logger.warn("ozon unarchive usage redis incr failed", { detail: error?.message || String(error) });
    }
  }
  const daily = await readOzonUnarchiveUsageFile();
  const targetKey = cleanText(target) || "default";
  daily[windowKey] = daily[windowKey] && typeof daily[windowKey] === "object" ? daily[windowKey] : {};
  daily[windowKey][targetKey] = (Number(daily[windowKey][targetKey]) || 0) + amount;
  await writeOzonUnarchiveUsageFile(daily);
  return daily[windowKey][targetKey];
}

// Ozon answered "restore limit exceeded" for a single product: nothing more fits this window.
async function closeOzonUnarchiveWindow(target = "", detail = "") {
  const windowKey = ozonUnarchiveDateKey();
  const redis = ozonUnarchiveUsageRedis();
  logger.info("ozon_unarchive_window_closed", { target, window: windowKey, detail: cleanText(detail).slice(0, 200) });
  if (redis) {
    try {
      const keys = ozonUnarchiveUsageRedisKeys(target, windowKey);
      await redis.set(keys.closed, "1", "EX", ozonUnarchiveUsageTtlSeconds);
      return;
    } catch (error) {
      logger.warn("ozon unarchive window close redis failed", { detail: error?.message || String(error) });
    }
  }
  const daily = await readOzonUnarchiveUsageFile();
  const targetKey = cleanText(target) || "default";
  daily[windowKey] = daily[windowKey] && typeof daily[windowKey] === "object" ? daily[windowKey] : {};
  daily[windowKey][`${targetKey}#closed`] = true;
  await writeOzonUnarchiveUsageFile(daily);
}

async function readOzonUnarchiveDailyState() {
  const windowKey = ozonUnarchiveDateKey();
  const limit = Number.isFinite(ozonUnarchiveDailyLimit) ? ozonUnarchiveDailyLimit : 0;
  const redis = ozonUnarchiveUsageRedis();
  if (redis) {
    try {
      const targets = ozonUnarchiveUsageTargets();
      const keys = targets.flatMap((target) => {
        const pair = ozonUnarchiveUsageRedisKeys(target, windowKey);
        return [pair.used, pair.closed];
      });
      const values = await redis.mget(keys);
      const day = {};
      targets.forEach((target, index) => {
        const used = Number(values[index * 2]) || 0;
        const closed = values[index * 2 + 1] === "1";
        if (!used && !closed) return;
        day[target] = closed ? Math.max(used, limit) : used;
        if (closed) day[`${target}#closed`] = true;
      });
      return { [windowKey]: day };
    } catch (error) {
      logger.warn("ozon unarchive usage redis read failed", { detail: error?.message || String(error) });
    }
  }
  const daily = await readOzonUnarchiveUsageFile();
  const day = { ...(daily[windowKey] || {}) };
  for (const key of Object.keys(day)) {
    if (key.endsWith("#closed") && day[key]) {
      const target = key.slice(0, -"#closed".length);
      day[target] = Math.max(Number(day[target]) || 0, limit);
    }
  }
  return { [windowKey]: day };
}

// Queue writes no longer persist usage (it lives in recordOzonUnarchiveUsage /
// closeOzonUnarchiveWindow); writing the snapshot back was what lost concurrent increments.
async function writeOzonUnarchiveDailyState(_daily = {}) {
  return undefined;
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
