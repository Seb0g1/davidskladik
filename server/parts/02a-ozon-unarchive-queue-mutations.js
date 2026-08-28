function ozonUnarchiveDailyUsed(queue = {}, target = "", date = new Date()) {
  const key = ozonUnarchiveDateKey(date);
  const targetKey = cleanText(target) || "default";
  return Math.max(0, Number(queue.daily?.[key]?.[targetKey] || 0) || 0);
}

function setOzonUnarchiveDailyUsed(queue = {}, target = "", used = 0, date = new Date()) {
  const key = ozonUnarchiveDateKey(date);
  const targetKey = cleanText(target) || "default";
  queue.daily = queue.daily && typeof queue.daily === "object" ? queue.daily : {};
  queue.daily[key] = queue.daily[key] && typeof queue.daily[key] === "object" ? queue.daily[key] : {};
  queue.daily[key][targetKey] = Math.max(0, Math.round(Number(used || 0) || 0));
  for (const dateKey of Object.keys(queue.daily)) {
    if (dateKey < key) delete queue.daily[dateKey];
  }
  return queue;
}

function queueOzonUnarchiveItems(queue = {}, products = [], { nextRetryAt = nextOzonUnarchiveRetryAt(), warning = "pending", error = "", attempted = false } = {}) {
  const normalizedQueue = normalizeOzonUnarchiveQueue(queue);
  const byKey = new Map(normalizedQueue.items.map((item) => [ozonUnarchiveQueueKey(item), item]));
  const now = new Date().toISOString();
  for (const product of Array.isArray(products) ? products : []) {
    const item = normalizeOzonUnarchiveQueueItem({
      id: product.id,
      warehouseProductId: product.id,
      productId: product.ozonProductId || product.productId,
      ozonProductId: product.ozonProductId || product.productId,
      offerId: product.offerId,
      target: product.target,
      nextRetryAt,
      warning,
      error,
      status: "pending",
    });
    const key = ozonUnarchiveQueueKey(item);
    if (!key) continue;
    const existing = byKey.get(key);
    byKey.set(key, {
      ...(existing || {}),
      ...item,
      queuedAt: existing?.queuedAt || item.queuedAt,
      attempts: attempted ? Math.max(0, Number(existing?.attempts || 0) || 0) + 1 : (existing?.attempts || item.attempts),
      lastAttemptAt: attempted ? now : (existing?.lastAttemptAt || item.lastAttemptAt),
    });
  }
  normalizedQueue.items = Array.from(byKey.values());
  return normalizedQueue;
}

function removeOzonUnarchiveQueueItems(queue = {}, products = []) {
  const removeKeys = new Set((Array.isArray(products) ? products : []).map(ozonUnarchiveQueueKey).filter(Boolean));
  const normalizedQueue = normalizeOzonUnarchiveQueue(queue);
  normalizedQueue.items = normalizedQueue.items.filter((item) => !removeKeys.has(ozonUnarchiveQueueKey(item)));
  return normalizedQueue;
}


