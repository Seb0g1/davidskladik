function yandexUnarchiveQueueKey(item = {}) {
  return [
    cleanText(item.target),
    cleanText(item.id || item.warehouseProductId),
    cleanText(item.offerId || item.offer_id),
  ].filter(Boolean).join(":");
}

function nextYandexUnarchiveRetryAt(attempts = 0, date = new Date()) {
  const value = date instanceof Date ? date : new Date(date);
  const base = Number.isFinite(value.getTime()) ? value : new Date();
  const idx = Math.min(Math.max(0, Number(attempts || 0) || 0), yandexUnarchiveRetryMinutes.length - 1);
  const minutes = yandexUnarchiveRetryMinutes[idx] || 1440;
  return new Date(base.getTime() + minutes * 60 * 1000).toISOString();
}

function normalizeYandexUnarchiveQueueItem(item = {}, fallback = {}) {
  const now = new Date().toISOString();
  const id = cleanText(item.id || item.warehouseProductId || item.warehouse_product_id || fallback.id || fallback.warehouseProductId);
  return {
    id,
    warehouseProductId: id,
    offerId: cleanText(item.offerId || item.offer_id || fallback.offerId),
    target: cleanText(item.target || fallback.target),
    marketplace: "yandex",
    status: cleanText(item.status || "pending") || "pending",
    queuedAt: cleanText(item.queuedAt || item.queued_at) || now,
    nextRetryAt: cleanText(item.nextRetryAt || item.next_retry_at) || nextYandexUnarchiveRetryAt(0),
    lastAttemptAt: cleanText(item.lastAttemptAt || item.last_attempt_at),
    attempts: Math.max(0, Number(item.attempts || 0) || 0),
    warning: cleanText(item.warning || "yandex_unarchive_pending"),
    error: cleanText(item.error),
  };
}

function normalizeYandexUnarchiveQueue(queue = {}) {
  const items = Array.isArray(queue.items) ? queue.items : [];
  const byKey = new Map();
  for (const item of items) {
    const normalized = normalizeYandexUnarchiveQueueItem(item);
    const key = yandexUnarchiveQueueKey(normalized);
    if (key && normalized.status !== "done") byKey.set(key, normalized);
  }
  return {
    updatedAt: cleanText(queue.updatedAt),
    items: Array.from(byKey.values()),
  };
}

async function readYandexUnarchiveQueue() {
  try {
    const text = await fs.readFile(yandexUnarchiveQueuePath, "utf8");
    if (!text.trim()) return { updatedAt: null, items: [] };
    return normalizeYandexUnarchiveQueue(JSON.parse(text));
  } catch (error) {
    if (error.code === "ENOENT") return { updatedAt: null, items: [] };
    if (error instanceof SyntaxError) {
      logger.warn("yandex unarchive queue is invalid, resetting in memory", { detail: error.message });
      return { updatedAt: null, items: [] };
    }
    throw error;
  }
}

async function writeYandexUnarchiveQueue(queue = {}) {
  const payload = normalizeYandexUnarchiveQueue({
    ...queue,
    updatedAt: new Date().toISOString(),
  });
  await fs.mkdir(dataDir, { recursive: true });
  const tmpPath = `${yandexUnarchiveQueuePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmpPath, yandexUnarchiveQueuePath);
  return payload;
}

function queueYandexUnarchiveItems(queue = {}, products = [], { nextRetryAt = nextYandexUnarchiveRetryAt(0), warning = "yandex_unarchive_pending", error = "", attempted = false } = {}) {
  const normalizedQueue = normalizeYandexUnarchiveQueue(queue);
  const byKey = new Map(normalizedQueue.items.map((item) => [yandexUnarchiveQueueKey(item), item]));
  const now = new Date().toISOString();
  for (const product of Array.isArray(products) ? products : []) {
    const item = normalizeYandexUnarchiveQueueItem({
      id: product.id,
      warehouseProductId: product.id,
      offerId: product.offerId,
      target: product.target,
      nextRetryAt,
      warning,
      error,
      status: "pending",
    });
    const key = yandexUnarchiveQueueKey(item);
    if (!key) continue;
    const existing = byKey.get(key);
    const attempts = attempted
      ? Math.max(0, Number(existing?.attempts || 0) || 0) + 1
      : (existing?.attempts || item.attempts);
    byKey.set(key, {
      ...(existing || {}),
      ...item,
      attempts,
      queuedAt: existing?.queuedAt || item.queuedAt,
      lastAttemptAt: attempted ? now : (existing?.lastAttemptAt || item.lastAttemptAt),
      nextRetryAt: nextRetryAt || nextYandexUnarchiveRetryAt(attempts),
    });
  }
  normalizedQueue.items = Array.from(byKey.values());
  return normalizedQueue;
}

function removeYandexUnarchiveQueueItems(queue = {}, products = []) {
  const removeKeys = new Set((Array.isArray(products) ? products : []).map(yandexUnarchiveQueueKey).filter(Boolean));
  const normalizedQueue = normalizeYandexUnarchiveQueue(queue);
  normalizedQueue.items = normalizedQueue.items.filter((item) => !removeKeys.has(yandexUnarchiveQueueKey(item)));
  return normalizedQueue;
}

async function writeYandexUnarchiveQueueDelta(queue = {}, { upsertProducts = [], removeProducts = [] } = {}) {
  let payload = normalizeYandexUnarchiveQueue({
    ...queue,
    updatedAt: new Date().toISOString(),
  });
  if (removeProducts.length) payload = removeYandexUnarchiveQueueItems(payload, removeProducts);
  if (upsertProducts.length) payload = queueYandexUnarchiveItems(payload, upsertProducts);
  return writeYandexUnarchiveQueue(payload);
}

function yandexUnarchiveQueuePublic(queue = {}, { limit = 1000 } = {}) {
  const normalized = normalizeYandexUnarchiveQueue(queue);
  const now = Date.now();
  const items = normalized.items
    .filter((item) => item.status !== "done")
    .map((item) => {
      const nextMs = item.nextRetryAt ? new Date(item.nextRetryAt).getTime() : 0;
      const due = !nextMs || !Number.isFinite(nextMs) || nextMs <= now;
      return { ...item, due };
    })
    .slice(0, Math.max(1, Number(limit || 1000) || 1000));
  return {
    updatedAt: normalized.updatedAt,
    total: normalized.items.length,
    due: items.filter((item) => item.due).length,
    items,
  };
}

function ozonUnarchiveQueuedActions(products = [], queue = {}, { warning = "ozon_unarchive_daily_limit_queued", nextRetryAt = nextOzonUnarchiveRetryAt() } = {}) {
  const normalizedQueue = normalizeOzonUnarchiveQueue(queue);
  const dailyLimit = Number.isFinite(ozonUnarchiveDailyLimit) ? ozonUnarchiveDailyLimit : null;
  return (Array.isArray(products) ? products : []).map((item) => ({
    id: item.id,
    warehouseProductId: item.id,
    ozonProductId: ozonNumericProductId(item.ozonProductId || item.productId),
    type: "unarchive",
    target: item.target,
    offerId: item.offerId,
    ok: true,
    pending: true,
    warning,
    queuedByDailyLimit: true,
    nextRetryAt,
    dailyLimit,
    queueSize: normalizedQueue.items.length,
  }));
}

function ozonUnarchiveQueuePublic(queue = {}, { limit = 1000 } = {}) {
  const normalized = normalizeOzonUnarchiveQueue(queue);
  const now = Date.now();
  const dailyLimit = Number.isFinite(ozonUnarchiveDailyLimit) ? ozonUnarchiveDailyLimit : null;
  const targets = new Map();
  const items = normalized.items
    .filter((item) => item.status !== "done")
    .map((item) => {
      const target = cleanText(item.target) || "default";
      const nextMs = item.nextRetryAt ? new Date(item.nextRetryAt).getTime() : 0;
      const due = !nextMs || !Number.isFinite(nextMs) || nextMs <= now;
      const existing = targets.get(target) || {
        target,
        dailyLimit,
        dailyUsed: ozonUnarchiveDailyUsed(normalized, target),
        availableToday: dailyLimit === null ? null : Math.max(0, dailyLimit - ozonUnarchiveDailyUsed(normalized, target)),
        due: 0,
        future: 0,
        total: 0,
      };
      existing.total += 1;
      if (due) existing.due += 1;
      else existing.future += 1;
      targets.set(target, existing);
      return {
        ...item,
        warehouseProductId: item.warehouseProductId || item.id,
        ozonProductId: item.ozonProductId || item.productId || "",
        queueKey: ozonUnarchiveQueueKey(item),
        due,
        dailyLimit,
        dailyUsed: ozonUnarchiveDailyUsed(normalized, target),
        availableToday: dailyLimit === null ? null : Math.max(0, dailyLimit - ozonUnarchiveDailyUsed(normalized, target)),
      };
    })
    .sort((a, b) => {
      if (a.due !== b.due) return a.due ? -1 : 1;
      return new Date(a.nextRetryAt || a.queuedAt || 0) - new Date(b.nextRetryAt || b.queuedAt || 0);
    });
  const warnings = items.reduce((acc, item) => {
    const warning = cleanText(item.warning || item.status || "pending") || "pending";
    acc[warning] = (acc[warning] || 0) + 1;
    return acc;
  }, {});
  return {
    ok: true,
    updatedAt: normalized.updatedAt,
    dailyLimit,
    total: items.length,
    due: items.filter((item) => item.due).length,
    future: items.filter((item) => !item.due).length,
    verificationPending: Number(warnings.ozon_unarchive_verify_pending || 0),
    warningCounts: warnings,
    availableToday: dailyLimit === null ? null : Array.from(targets.values()).reduce((sum, item) => sum + item.availableToday, 0),
    nextRetryAt: items.filter((item) => !item.due).map((item) => item.nextRetryAt).filter(Boolean).sort()[0] || null,
    targets: Array.from(targets.values()),
    items: items.slice(0, cleanLimit(limit, 1000, 5000)),
  };
}


