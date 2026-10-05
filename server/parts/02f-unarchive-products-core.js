async function unarchiveProductsOnMarketplaces(products = [], options = {}) {
  const actions = [];
  const byTarget = new Map();
  // Market duplicates archived on purpose («Дубль варианта» → one offer kept) stay archived
  const deduped = typeof marketDedupeOfferKeys === "function" ? await marketDedupeOfferKeys().catch(() => new Set()) : new Set();
  for (const product of products) {
    if (product?.marketplace === "yandex" && deduped.has(`${cleanText(product.target)}|${cleanText(product.offerId).toLowerCase()}`)) {
      actions.push({ id: product.id, type: "unarchive", offerId: product.offerId, target: product.target, ok: false, skipped: true, error: "market_duplicate_archived" });
      continue;
    }
    if (!product?.id || !product?.target || (product.marketplace === "yandex" && !cleanText(product.offerId))) {
      actions.push({
        id: product?.id || "",
        type: "unarchive",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.target ? "missing_target" : "missing_offer_id"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "unarchive", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      let queueState = await readOzonUnarchiveQueue();
      const resolvedRows = await resolveOzonUnarchiveProductIds(items, account);
      const missingRows = resolvedRows.filter((row) => !row.productId);
      if (missingRows.length) {
        const missingItems = missingRows.map((row) => row.item);
        const nextRetryAt = nextOzonUnarchiveVisibilityRetryAt();
        queueState = queueOzonUnarchiveItems(queueState, missingItems, {
          nextRetryAt,
          warning: "ozon_product_id_missing",
          error: "ozon_product_id_missing",
          attempted: true,
        });
        await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: missingItems });
        rescheduleOzonUnarchiveQueueAutoSoon("missing_ozon_product_id").catch((error) => {
          logger.warn("ozon unarchive queue reschedule failed", { detail: error?.message || String(error) });
        });
        actions.push(...missingItems.map((item) => ({
          id: item.id,
          type: "unarchive",
          target,
          offerId: item.offerId,
          ok: false,
          pending: true,
          error: "ozon_product_id_missing",
          warning: "ozon_product_id_missing",
          nextRetryAt,
          queueSize: queueState.items.length,
        })));
      }
      const resolvedItems = resolvedRows
        .filter((row) => row.productId)
        .map((row) => ({
          ...row.item,
          productId: row.productId,
          ozonProductId: row.productId,
        }));
      let usedToday = ozonUnarchiveDailyUsed(queueState, target);
      // Ozon limits restores of AUTO-archived products to 100 per window (03:00 MSK → 03:00 MSK)
      // and rejects a whole /v1/product/unarchive request that would cross it. Manually archived
      // products (isAutoArchived === false) have no limit and are always sent, separately.
      // The local counter (shared by every process, see recordOzonUnarchiveUsage) gates the
      // queue; the real end of the day is Ozon's "restore limit exceeded", after which the window
      // is closed for every caller. Callers with forceOzonDailyLimit (reconciler, link activation,
      // manual force) skip the local gate but are counted and stop once the window is closed.
      const enforceDailyLimit = options.forceOzonDailyLimit !== true && Number.isFinite(ozonUnarchiveDailyLimit);
      const windowClosed = Number.isFinite(ozonUnarchiveDailyLimit) && ozonUnarchiveWindowClosed(queueState, target);
      const regularArchiveItems = resolvedItems.filter((item) => item.marketplaceState?.isAutoArchived === false);
      const limitedItems = resolvedItems.filter((item) => item.marketplaceState?.isAutoArchived !== false);
      // the counter at the limit is not the end of the day: a few are probed (Ozon may still take them —
      // the seller restored 2–5 by hand after our «limit»); Ozon's refusal closes the window for the probe pause
      const availableToday = windowClosed
        ? 0
        : (enforceDailyLimit ? (ozonUnarchiveDailyLimit - usedToday > 0 ? ozonUnarchiveDailyLimit - usedToday : OZON_UNARCHIVE_PROBE_SIZE) : limitedItems.length);
      const runnableLimited = limitedItems.slice(0, availableToday);
      const overflowItems = limitedItems.slice(runnableLimited.length);
      const deferToNextWindow = async (items, { attempted = false, error = "" } = {}) => {
        if (!items.length) return;
        const nextRetryAt = windowClosed || attempted
          ? nextOzonUnarchiveProbeAt().toISOString()
          : nextOzonUnarchiveRetryAt();
        queueState = queueOzonUnarchiveItems(queueState, items, {
          nextRetryAt,
          warning: "ozon_unarchive_daily_limit_queued",
          attempted,
          ...(error ? { error } : {}),
        });
        await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: items });
        actions.push(...ozonUnarchiveQueuedActions(items, queueState, {
          warning: "ozon_unarchive_daily_limit_queued",
          nextRetryAt,
        }));
      };
      await deferToNextWindow(overflowItems);

      const numericIds = (items) => items
        .map((item) => Number(ozonNumericProductId(item.ozonProductId || item.productId)))
        .filter((id) => id > 0);
      const isRestoreLimitError = (detail) => {
        const text = String(detail || "");
        if (/too.?many.?request|rate.?limit|429/i.test(text)) return false; // API throttling, not the quota
        return /restore limit|daily|суточ|лимит|limit|quota|auto.?archive|автоархив/i.test(text);
      };
      const markAccepted = async (items, { limited }) => {
        queueState = removeOzonUnarchiveQueueItems(queueState, items);
        if (limited) {
          const autoArchiveCount = items.filter((item) => item.marketplaceState?.isAutoArchived !== false).length;
          usedToday += autoArchiveCount;
          setOzonUnarchiveDailyUsed(queueState, target, usedToday);
          await recordOzonUnarchiveUsage(target, autoArchiveCount).catch((error) => {
            logger.warn("ozon unarchive usage record failed", { detail: error?.message || String(error) });
          });
        }
        await writeOzonUnarchiveQueueDelta(queueState, { removeProducts: items });
        actions.push(...items.map((item) => ({
          id: item.id,
          type: "unarchive",
          target,
          offerId: item.offerId,
          ozonProductId: ozonNumericProductId(item.ozonProductId || item.productId),
          ok: true,
          dailyLimit: Number.isFinite(ozonUnarchiveDailyLimit) ? ozonUnarchiveDailyLimit : null,
          dailyUsed: usedToday,
          queueSize: queueState.items.length,
        })));
      };
      const markFailed = async (items, detail) => {
        const nextRetryAt = nextOzonUnarchiveVisibilityRetryAt();
        queueState = queueOzonUnarchiveItems(queueState, items, {
          nextRetryAt,
          warning: "ozon_unarchive_api_error",
          error: detail,
          attempted: true,
        });
        await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: items });
        rescheduleOzonUnarchiveQueueAutoSoon("api_error_queue").catch((rescheduleError) => {
          logger.warn("ozon unarchive queue reschedule failed", { detail: rescheduleError?.message || String(rescheduleError) });
        });
        actions.push(...items.map((item) => ({
          id: item.id,
          type: "unarchive",
          target,
          offerId: item.offerId,
          ozonProductId: ozonNumericProductId(item.ozonProductId || item.productId),
          ok: false,
          pending: true,
          error: detail,
          warning: "ozon_unarchive_api_error",
          nextRetryAt,
          queueSize: queueState.items.length,
        })));
      };
      const markMissingProductId = async (items) => {
        const nextRetryAt = nextOzonUnarchiveVisibilityRetryAt();
        queueState = queueOzonUnarchiveItems(queueState, items, {
          nextRetryAt,
          warning: "ozon_product_id_missing",
          error: "ozon_product_id_missing",
          attempted: true,
        });
        await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: items });
        actions.push(...items.map((item) => ({
          id: item.id,
          type: "unarchive",
          target,
          offerId: item.offerId,
          ok: false,
          pending: true,
          error: "ozon_product_id_missing",
          warning: "ozon_product_id_missing",
          nextRetryAt,
          queueSize: queueState.items.length,
        })));
      };

      // Send one request; on "restore limit exceeded" split the batch in halves until a single
      // product is refused, so the last few slots of the window are used instead of lost.
      // Returns true once Ozon has refused a single product (the window is full).
      let windowFull = false;
      const sendLimited = async (items) => {
        if (!items.length || windowFull) return;
        try {
          await ozonRequest("/v1/product/unarchive", { product_id: numericIds(items) }, account);
          await markAccepted(items, { limited: true });
        } catch (error) {
          const detail = error?.message || "unarchive_failed";
          if (!isRestoreLimitError(detail)) {
            await markFailed(items, detail);
            return;
          }
          if (items.length > 1) {
            const middle = Math.ceil(items.length / 2);
            await sendLimited(items.slice(0, middle));
            if (windowFull) {
              await deferToNextWindow(items.slice(middle), { attempted: false });
              return;
            }
            await sendLimited(items.slice(middle));
            return;
          }
          windowFull = true;
          usedToday = Math.max(usedToday, Number.isFinite(ozonUnarchiveDailyLimit) ? ozonUnarchiveDailyLimit : usedToday);
          setOzonUnarchiveDailyUsed(queueState, target, usedToday);
          await closeOzonUnarchiveWindow(target, detail).catch((closeError) => {
            logger.warn("ozon unarchive window close failed", { detail: closeError?.message || String(closeError) });
          });
          const nextRetryAt = nextOzonUnarchiveProbeAt().toISOString();
          queueState = queueOzonUnarchiveItems(queueState, items, { nextRetryAt, warning: "ozon_unarchive_daily_limit_queued", attempted: false, error: detail });
          await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: items });
          rescheduleOzonUnarchiveQueueAutoSoon("api_limit_queue").catch((rescheduleError) => {
            logger.warn("ozon unarchive queue reschedule failed", { detail: rescheduleError?.message || String(rescheduleError) });
          });
          actions.push(...ozonUnarchiveQueuedActions(items, queueState, {
            warning: "ozon_unarchive_daily_limit_queued",
            nextRetryAt,
          }));
        }
      };

      // Manually archived products: no quota, sent as they are.
      for (const chunk of chunkArray(regularArchiveItems, 100)) {
        const withIds = chunk.filter((item) => numericIds([item]).length);
        const withoutIds = chunk.filter((item) => !numericIds([item]).length);
        if (withoutIds.length) await markMissingProductId(withoutIds);
        if (!withIds.length) continue;
        try {
          await ozonRequest("/v1/product/unarchive", { product_id: numericIds(withIds) }, account);
          await markAccepted(withIds, { limited: false });
        } catch (error) {
          await markFailed(withIds, error?.message || "unarchive_failed");
        }
      }

      const limitedChunks = chunkArray(runnableLimited, 100);
      for (let chunkIndex = 0; chunkIndex < limitedChunks.length; chunkIndex += 1) {
        const chunk = limitedChunks[chunkIndex];
        if (windowFull) {
          await deferToNextWindow(limitedChunks.slice(chunkIndex).flat(), { attempted: false });
          break;
        }
        const withoutIds = chunk.filter((item) => !numericIds([item]).length);
        if (withoutIds.length) await markMissingProductId(withoutIds);
        await sendLimited(chunk.filter((item) => numericIds([item]).length));
      }
      continue;
    }
    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "unarchive", target, offerId: item.offerId, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 200)) {
        const unarchiveResults = await sendYandexOfferArchiveState(shop, chunk.map((item) => item.offerId), false);
        const byOfferId = new Map(unarchiveResults.map((item) => [cleanText(item.offerId).toLowerCase(), item]));
        actions.push(...chunk.map((item) => {
          const result = byOfferId.get(cleanText(item.offerId).toLowerCase());
          return {
            id: item.id,
            type: "unarchive",
            target: shop.id,
            offerId: item.offerId,
            ok: Boolean(result?.ok),
            error: result?.ok ? undefined : (result?.error || "unarchive_failed"),
          };
        }));
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "unarchive", target, offerId: item.offerId, ok: false, error: "unsupported_marketplace" })));
    }
  }
  return actions;
}

