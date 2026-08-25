function ozonUnarchiveQueueAutomationPublic() {
  return {
    autoEnabled: ozonUnarchiveQueueAutoEnabled,
    autoRunning: ozonUnarchiveQueueAutoRunning || ozonUnarchiveQueueProcessQueued,
    autoQueued: ozonUnarchiveQueueProcessQueued,
    lastAutoRunAt: ozonUnarchiveQueueAutoLastRunAt,
    nextAutoRunAt: ozonUnarchiveQueueAutoNextRunAt,
    lastAutoResult: ozonUnarchiveQueueAutoLastResult,
  };
}

function normalizeMarketplaceEnum(value) {
  const text = cleanText(value).toLowerCase();
  return text === "yandex" ? "yandex" : "ozon";
}

function normalizeQueueStatusEnum(value, item = {}) {
  const text = cleanText(value).toLowerCase();
  if (["pending", "processing", "success", "failed", "delayed"].includes(text)) return text;
  if (item.nextRetryAt && new Date(item.nextRetryAt).getTime() > Date.now()) return "delayed";
  return item.error ? "failed" : "pending";
}

function priceRetryQueueItemToPostgres(item = {}) {
  const queueKey = priceRetryQueueKey(item) || crypto.randomUUID();
  const offerId = cleanText(item.offerId || item.offer_id || item.sku || item.id || item.productId || queueKey);
  const price = roundPrice(item.price ?? item.newPrice ?? item.targetPrice ?? 0) || 0;
  return {
    queueKey,
    marketplace: normalizeMarketplaceEnum(item.marketplace || item.target || "ozon"),
    target: cleanText(item.target || item.account || item.marketplace) || null,
    productId: cleanText(item.productId || item.id) || null,
    offerId,
    price,
    oldPrice: item.oldPrice === undefined && item.old_price === undefined ? null : (roundPrice(item.oldPrice ?? item.old_price) || 0),
    status: normalizeQueueStatusEnum(item.status, item),
    attempts: Math.max(0, Number(item.attempts || 0) || 0),
    error: cleanText(item.error || item.detail || ""),
    payload: cloneAuditValue(item) || {},
    nextRetryAt: toDateOrNull(item.nextRetryAt),
    lastAttemptAt: toDateOrNull(item.lastAttemptAt),
    createdAt: toDateOrNull(item.queuedAt || item.createdAt) || new Date(),
    updatedAt: toDateOrNull(item.updatedAt) || new Date(),
  };
}

function priceRetryQueueItemUpdateData(data = {}) {
  return {
    marketplace: data.marketplace,
    target: data.target,
    productId: data.productId,
    offerId: data.offerId,
    price: data.price,
    oldPrice: data.oldPrice,
    status: data.status,
    attempts: data.attempts,
    error: data.error,
    payload: data.payload,
    nextRetryAt: data.nextRetryAt,
    lastAttemptAt: data.lastAttemptAt,
  };
}

async function writePrismaStateChunks(rows = [], chunkSize = 100, buildOperation) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return;
  for (const chunk of chunkArray(list, chunkSize)) {
    try {
      await getPrisma().$transaction(
        chunk.map((item) => buildOperation(item)),
        { maxWait: stateWriteTransactionMaxWaitMs, timeout: stateWriteTransactionTimeoutMs },
      );
    } catch (error) {
      const detail = error?.message || String(error);
      if (!/Transaction already closed|expired transaction|timeout|timed out|deadlock|40P01/i.test(detail)) throw error;
      logger.warn("state postgres transaction failed (deadlock/timeout), retrying writes one by one", {
        rows: chunk.length,
        detail: detail.slice(0, 300),
      });
      rememberStateWarning("state_postgres_transaction_deadlock_or_timeout", error, { rows: chunk.length });
      for (const item of chunk) {
        await buildOperation(item);
      }
    }
  }
}

function priceRetryQueueItemFromPostgres(row = {}) {
  const payload = row.payload && typeof row.payload === "object" && !Array.isArray(row.payload) ? row.payload : {};
  return {
    ...payload,
    id: payload.id || row.productId || row.offerId,
    productId: row.productId || payload.productId || payload.id || null,
    offerId: row.offerId || payload.offerId || null,
    marketplace: row.marketplace || payload.marketplace || "ozon",
    target: row.target || payload.target || row.marketplace || "ozon",
    price: row.price ?? payload.price ?? null,
    oldPrice: row.oldPrice ?? payload.oldPrice ?? null,
    status: row.status || payload.status || "pending",
    attempts: row.attempts || 0,
    error: row.error || payload.error || "",
    queueKey: row.queueKey || payload.queueKey || null,
    queuedAt: payload.queuedAt || (row.createdAt ? row.createdAt.toISOString() : null),
    lastAttemptAt: row.lastAttemptAt ? row.lastAttemptAt.toISOString() : (payload.lastAttemptAt || null),
    nextRetryAt: row.nextRetryAt ? row.nextRetryAt.toISOString() : (payload.nextRetryAt || null),
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : (payload.updatedAt || null),
  };
}

function priceRetryQueueKey(item = {}) {
  return cleanText(item.queueKey || `${item.id || item.productId || item.offerId}:${item.target || item.marketplace || "ozon"}`);
}

function isActiveDelayedPriceRetry(item = {}, now = new Date()) {
  const nextRetryAt = item.nextRetryAt ? new Date(item.nextRetryAt).getTime() : 0;
  if (!nextRetryAt || !Number.isFinite(nextRetryAt) || nextRetryAt <= now.getTime()) return false;
  const status = cleanText(item.status).toLowerCase();
  return (status === "delayed" || status === "pending")
    && (item.retryReason === "ozon_per_item_price_limit" || isOzonPerItemPriceLimitError({ message: item.error }));
}

function findActiveDelayedPriceRetry(queueItems = [], item = {}, now = new Date()) {
  const keys = new Set([
    priceRetryQueueKey(item),
    priceRetryQueueKey({ ...item, id: item.productId }),
    priceRetryQueueKey({ ...item, id: item.offerId }),
  ].filter(Boolean));
  return (Array.isArray(queueItems) ? queueItems : []).find((queueItem) =>
    keys.has(priceRetryQueueKey(queueItem)) && isActiveDelayedPriceRetry(queueItem, now)
  ) || null;
}

function priceRetryDelayMs(attempts = 1, error = null) {
  if (isOzonPerItemPriceLimitError(error)) {
    return Math.max(3_600_000, Number(process.env.OZON_PRICE_ITEM_LIMIT_RETRY_MS || 3_900_000) || 3_900_000);
  }
  if (needsOzonOldPriceEscalation(error)) {
    return Math.max(5_000, Number(process.env.OZON_OLD_PRICE_RETRY_MS || 15_000) || 15_000);
  }
  const base = Math.max(30_000, Number(process.env.OZON_PRICE_RETRY_BASE_DELAY_MS || 180_000) || 180_000);
  const max = Math.max(base, Number(process.env.OZON_PRICE_RETRY_MAX_DELAY_MS || 1_800_000) || 1_800_000);
  const attempt = Math.max(1, Number(attempts || 1) || 1);
  return Math.min(max, base * attempt * attempt);
}

function buildPriceRetryItem(item = {}, error = null, now = new Date()) {
  const attempts = Number(item.attempts || 0) + 1;
  const delayedByLimitCheck = isOzonPerItemPriceLimitError(error);
  // Drop general failures after 20 attempts to prevent the queue from getting stuck forever.
  // Per-item-price-limit errors are exempt — Ozon unlocks them after ~65 min and they will succeed.
  const MAX_ATTEMPTS = Number(process.env.PRICE_RETRY_MAX_ATTEMPTS || 20) || 20;
  if (!delayedByLimitCheck && attempts > MAX_ATTEMPTS) return null;
  const delayMs = priceRetryDelayMs(attempts, error);
  const nextRetryAt = new Date(now.getTime() + delayMs).toISOString();
  const delayedByLimit = delayedByLimitCheck;
  const discountQuarantine = isOzonPriceDiscountQuarantineError(error);
  const oldPriceLess = isOzonOldPriceLessError(error);
  const oldPriceAdjusted = oldPriceLess || discountQuarantine;
  const targetPrice = roundPrice(item.finalTargetPrice ?? item.targetPrice ?? item.price);
  const cabinetPrice = roundPrice(item.cabinetPrice ?? item.oldPrice ?? 0);
  const stepPrice = discountQuarantine
    ? computeOzonQuarantineNextPrice(cabinetPrice || roundPrice(item.price), targetPrice)
    : roundPrice(item.price);
  const price = stepPrice || targetPrice;
  const stagedQuarantine = discountQuarantine && price > targetPrice;
  return {
    ...item,
    price,
    finalTargetPrice: targetPrice,
    cabinetPrice: cabinetPrice || item.cabinetPrice,
    error: error?.message || item.error || "retry_failed",
    oldPrice: oldPriceLess
      ? resolveOzonOldPrice(price, { ...item, oldPrice: Math.max(cabinetPrice, item.oldPrice || 0) })
      : resolveOzonOldPrice(price, item),
    forceOldPrice: oldPriceAdjusted ? true : item.forceOldPrice,
    queueKey: priceRetryQueueKey(item),
    status: delayedByLimit ? "delayed" : (oldPriceAdjusted ? "pending" : "failed"),
    queuedAt: item.queuedAt || now.toISOString(),
    lastAttemptAt: now.toISOString(),
    attempts,
    nextRetryAt,
    retryReason: delayedByLimit
      ? "ozon_per_item_price_limit"
      : (stagedQuarantine ? "ozon_quarantine_step" : (oldPriceLess ? "ozon_old_price_adjusted" : "send_failed")),
  };
}

