function ozonInfoLooksArchived(info = {}) {
  const visibility = cleanText(info.visibility).toUpperCase();
  const state = cleanText(info.status?.state || info.state || info.state_name || info.status_name).toUpperCase();
  return Boolean(info.archived || visibility === "ARCHIVED" || state === "ARCHIVED");
}

async function verifyOzonUnarchiveActions(products = [], actions = [], options = {}) {
  const verified = (Array.isArray(actions) ? actions : []).map((action) => ({ ...action }));
  const productsById = new Map((Array.isArray(products) ? products : [])
    .map((product) => [String(product.id), product]));
  const pendingByTarget = new Map();
  for (const action of verified) {
    if (!action?.ok || action.type !== "unarchive") continue;
    const product = productsById.get(String(action.id));
    if (!product || product.marketplace !== "ozon") continue;
    const target = cleanText(action.target || product.target);
    if (!target) continue;
    if (!pendingByTarget.has(target)) pendingByTarget.set(target, []);
    pendingByTarget.get(target).push({
      action,
      product,
      productId: ozonNumericProductId(action.ozonProductId || product.ozonProductId || product.productId || action.productId),
      offerId: cleanText(action.offerId || product.offerId),
    });
  }
  if (!pendingByTarget.size) return verified;

  const attempts = Math.max(1, Math.min(10, Math.round(Number(options.attempts || ozonUnarchiveVerifyAttempts) || ozonUnarchiveVerifyAttempts)));
  const delayMs = Math.max(0, Math.round(Number(options.delayMs ?? ozonUnarchiveVerifyDelayMs) || 0));
  const activeKeys = new Set();
  const archivedKeys = new Set();
  const visibleKeys = new Set();
  const failedTargets = new Map();

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    activeKeys.clear();
    archivedKeys.clear();
    visibleKeys.clear();
    failedTargets.clear();

    for (const [target, rows] of pendingByTarget.entries()) {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        failedTargets.set(target, "ozon_account_not_found");
        continue;
      }
      try {
        const productIds = rows.map((row) => row.productId).filter(Boolean);
        const offerIds = rows.map((row) => row.offerId).filter(Boolean);
        const byProductId = productIds.length
          ? await getOzonProductInfoMapByProductIds(productIds, account, { continueOnError: true })
          : new Map();
        const byOfferId = offerIds.length
          ? await getOzonProductInfoMap(offerIds, account, { continueOnError: true })
          : new Map();

        for (const row of rows) {
          const info = (row.productId && byProductId.get(row.productId))
            || (row.offerId && getOzonOfferMapValue(byOfferId, row.offerId))
            || null;
          if (!info) continue;
          const key = String(row.action.id);
          visibleKeys.add(key);
          if (ozonInfoLooksArchived(info)) archivedKeys.add(key);
          else activeKeys.add(key);
        }
      } catch (error) {
        failedTargets.set(target, error?.message || "ozon_unarchive_verify_failed");
      }
    }

    const remaining = [];
    for (const rows of pendingByTarget.values()) {
      for (const row of rows) {
        if (!activeKeys.has(String(row.action.id))) remaining.push(row);
      }
    }
    if (!remaining.length) break;
    if (attempt < attempts && delayMs > 0) await sleep(delayMs);
  }

  const retryProducts = [];
  const nextRetryAt = nextOzonUnarchiveVisibilityRetryAt();
  for (const [target, rows] of pendingByTarget.entries()) {
    const targetError = failedTargets.get(target);
    for (const row of rows) {
      const key = String(row.action.id);
      if (activeKeys.has(key)) {
        row.action.verified = true;
        row.action.pending = false;
        continue;
      }
      row.action.verified = false;
      row.action.ok = true;
      row.action.pending = true;
      row.action.nextRetryAt = nextRetryAt;
      row.action.warning = targetError
        ? `ozon_unarchive_verify_pending: ${targetError}`
        : (archivedKeys.has(key) ? "still_archived_after_unarchive" : (visibleKeys.has(key) ? "still_archived_after_unarchive" : "unarchive_not_visible_after_api"));
      delete row.action.error;
      retryProducts.push(row.product);
    }
  }

  if (retryProducts.length) {
    try {
      const currentQueue = await readOzonUnarchiveQueue();
      const attemptsByKey = new Map((currentQueue.items || []).map((item) => [ozonUnarchiveQueueKey(item), item]));
      const toQueue = [];
      const exhausted = [];
      for (const product of retryProducts) {
        const key = ozonUnarchiveQueueKey(product);
        const existing = attemptsByKey.get(key);
        const currentAttempts = Math.max(0, Number(existing?.attempts || 0) || 0) + 1;
        if (currentAttempts >= ozonUnarchiveMaxAttempts) {
          exhausted.push(product);
        } else {
          toQueue.push(product);
        }
      }
      if (exhausted.length) {
        logger.warn("ozon_unarchive_verify_exhausted", {
          count: exhausted.length,
          offerIds: exhausted.slice(0, 10).map((p) => p.offerId),
        });
      }
      if (toQueue.length) {
        const queueState = queueOzonUnarchiveItems(currentQueue, toQueue, {
          nextRetryAt,
          warning: "ozon_unarchive_verify_pending",
          attempted: true,
        });
        await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: toQueue });
        Promise.resolve(rescheduleOzonUnarchiveQueueAutoSoon("visibility_pending")).catch((error) => {
          logger.warn("ozon unarchive queue reschedule failed", { detail: error?.message || String(error) });
        });
      }
    } catch (error) {
      logger.warn("ozon unarchive pending requeue failed", { detail: error?.message || String(error) });
    }
  }

  return verified;
}

async function verifyMarketplaceUnarchiveActions(products = [], actions = [], options = {}) {
  return verifyYandexUnarchiveActions(
    products,
    await verifyOzonUnarchiveActions(products, actions, options),
    options,
  );
}

async function verifyYandexUnarchiveActions(products = [], actions = [], options = {}) {
  const verified = (Array.isArray(actions) ? actions : []).map((action) => ({ ...action }));
  const productsById = new Map((Array.isArray(products) ? products : [])
    .map((product) => [String(product.id), product]));
  const pendingByTarget = new Map();
  for (const action of verified) {
    if (!action?.ok || action.type !== "unarchive") continue;
    const product = productsById.get(String(action.id));
    if (!product || product.marketplace !== "yandex") continue;
    const offerId = cleanText(action.offerId || product.offerId);
    const target = cleanText(action.target || product.target);
    if (!offerId || !target) continue;
    const key = target;
    if (!pendingByTarget.has(key)) pendingByTarget.set(key, []);
    pendingByTarget.get(key).push({ action, offerId });
  }
  if (!pendingByTarget.size) return verified;

  const attempts = Math.max(1, Math.min(5, Math.round(Number(options.attempts || process.env.YANDEX_UNARCHIVE_VERIFY_ATTEMPTS || 3) || 3)));
  const delayMs = Math.max(0, Math.min(10000, Math.round(Number(options.delayMs ?? process.env.YANDEX_UNARCHIVE_VERIFY_DELAY_MS ?? 1500) || 1500)));
  const activeOfferIds = new Set();
  const archivedOfferIds = new Set();
  const failedTargets = new Map();

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    activeOfferIds.clear();
    archivedOfferIds.clear();
    failedTargets.clear();

    for (const [target, rows] of pendingByTarget.entries()) {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        failedTargets.set(target, "shop_not_found");
        continue;
      }
      const offerIds = rows.map((row) => row.offerId);
      try {
        const mappings = await getYandexOfferMappingsByOfferIds(shop, offerIds);
        for (const item of mappings) {
          const offerId = yandexOfferIdFromMapping(item).toLowerCase();
          if (!offerId) continue;
          const offer = pickYandexOfferFromMapping(item);
          const state = pickYandexState(item, offer);
          const key = `${target}:${offerId}`;
          if (state.code === "archived") archivedOfferIds.add(key);
          else activeOfferIds.add(key);
        }
      } catch (error) {
        failedTargets.set(target, error?.message || "yandex_unarchive_verify_failed");
      }
    }

    const remaining = [];
    for (const [target, rows] of pendingByTarget.entries()) {
      for (const row of rows) {
        const key = `${target}:${row.offerId.toLowerCase()}`;
        if (!activeOfferIds.has(key)) remaining.push(row);
      }
    }
    if (!remaining.length) break;
    if (attempt < attempts && delayMs > 0) await sleep(delayMs);
  }

  for (const [target, rows] of pendingByTarget.entries()) {
    const targetError = failedTargets.get(target);
    for (const row of rows) {
      const key = `${target}:${row.offerId.toLowerCase()}`;
      if (activeOfferIds.has(key)) {
        row.action.verified = true;
        row.action.pending = false;
        row.action.ok = true;
        continue;
      }
      row.action.verified = false;
      row.action.ok = false;
      row.action.pending = true;
      row.action.status = "retry_scheduled";
      row.action.warning = targetError
        ? `unarchive_verify_pending: ${targetError}`
        : (archivedOfferIds.has(key) ? "still_archived_after_unarchive" : "unarchive_not_visible_after_api");
      delete row.action.error;
    }
  }

  const retryProducts = [];
  for (const [target, rows] of pendingByTarget.entries()) {
    for (const row of rows) {
      if (!row.action.pending) continue;
      const product = productsById.get(String(row.action.id));
      if (product) retryProducts.push(product);
    }
  }
  if (retryProducts.length) {
    try {
      const queueState = await readYandexUnarchiveQueue();
      const attemptsByKey = new Map(queueState.items.map((item) => [yandexUnarchiveQueueKey(item), item]));
      const toQueue = [];
      const exhausted = [];
      for (const product of retryProducts) {
        const key = yandexUnarchiveQueueKey(product);
        const existing = attemptsByKey.get(key);
        const attempts = Math.max(0, Number(existing?.attempts || 0) || 0) + 1;
        if (attempts >= yandexUnarchiveMaxAttempts) {
          exhausted.push(product);
          continue;
        }
        toQueue.push({ ...product, attempts });
      }
      if (toQueue.length) {
        const nextRetryAt = nextYandexUnarchiveRetryAt(Math.max(...toQueue.map((item) => item.attempts || 1)));
        const updated = queueYandexUnarchiveItems(queueState, toQueue, {
          nextRetryAt,
          warning: "yandex_unarchive_verify_pending",
          attempted: true,
        });
        await writeYandexUnarchiveQueue(updated);
        Promise.resolve(rescheduleYandexUnarchiveQueueAutoSoon("verify_pending")).catch((error) => {
          logger.warn("yandex unarchive queue reschedule failed", { detail: error?.message || String(error) });
        });
      }
      if (exhausted.length) {
        logger.warn("yandex unarchive retries exhausted", {
          count: exhausted.length,
          offerIds: exhausted.slice(0, 10).map((item) => item.offerId),
        });
      }
    } catch (error) {
      logger.warn("yandex unarchive pending requeue failed", { detail: error?.message || String(error) });
    }
  }

  return verified;
}


