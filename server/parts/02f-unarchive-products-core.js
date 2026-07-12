async function unarchiveProductsOnMarketplaces(products = [], options = {}) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
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
      // Cap the send by the remaining local daily quota. Ozon rejects a whole /v1/product/unarchive
      // request when it crosses the daily limit, so an ungated batch wastes the remaining quota
      // (e.g. 88 used + a batch of 100 → whole batch rejected, day ends at 88 instead of 100).
      // Over-quota items are deferred to the next scheduled window without an API call.
      const enforceDailyLimit = options.forceOzonDailyLimit !== true && Number.isFinite(ozonUnarchiveDailyLimit);
      const availableToday = enforceDailyLimit ? Math.max(0, ozonUnarchiveDailyLimit - usedToday) : resolvedItems.length;
      const runnableItems = resolvedItems.slice(0, availableToday);
      const overflowItems = resolvedItems.slice(runnableItems.length);
      if (overflowItems.length) {
        const nextRetryAt = nextOzonUnarchiveRetryAt();
        queueState = queueOzonUnarchiveItems(queueState, overflowItems, {
          nextRetryAt,
          warning: "ozon_unarchive_daily_limit_queued",
          attempted: false,
        });
        await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: overflowItems });
        actions.push(...ozonUnarchiveQueuedActions(overflowItems, queueState, {
          warning: "ozon_unarchive_daily_limit_queued",
          nextRetryAt,
        }));
      }
      const chunks = chunkArray(runnableItems, 100);
      for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex += 1) {
        const chunk = chunks[chunkIndex];
        const productIds = chunk.map((item) => Number(ozonNumericProductId(item.ozonProductId || item.productId))).filter((id) => id > 0);
        if (!productIds.length) {
          const nextRetryAt = nextOzonUnarchiveVisibilityRetryAt();
          queueState = queueOzonUnarchiveItems(queueState, chunk, {
            nextRetryAt,
            warning: "ozon_product_id_missing",
            error: "ozon_product_id_missing",
            attempted: true,
          });
          await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: chunk });
          actions.push(...chunk.map((item) => ({
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
          continue;
        }
        try {
          await ozonRequest("/v1/product/unarchive", { product_id: productIds }, account);
          usedToday += productIds.length;
          queueState = removeOzonUnarchiveQueueItems(queueState, chunk);
          setOzonUnarchiveDailyUsed(queueState, target, usedToday);
          await writeOzonUnarchiveQueueDelta(queueState, { removeProducts: chunk });
          actions.push(...chunk.map((item) => ({
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
        } catch (error) {
          const detail = error?.message || "unarchive_failed";
          if (/daily|суточ|лимит|limit|quota|auto.?archive|автоархив/i.test(detail)) {
            const nextRetryAt = nextOzonUnarchiveRetryAt();
            const remainingItems = chunks.slice(chunkIndex + 1).flat();
            queueState = queueOzonUnarchiveItems(queueState, chunk, { nextRetryAt, attempted: true, error: detail });
            if (remainingItems.length) {
              queueState = queueOzonUnarchiveItems(queueState, remainingItems, {
                nextRetryAt,
                warning: "ozon_unarchive_daily_limit_queued",
                attempted: false,
              });
            }
            // Ozon says the daily limit is reached — trust it over the local counter so the rest
            // of today's runs defer immediately instead of burning more rejected API calls.
            if (Number.isFinite(ozonUnarchiveDailyLimit)) {
              usedToday = Math.max(usedToday, ozonUnarchiveDailyLimit);
              setOzonUnarchiveDailyUsed(queueState, target, usedToday);
            }
            await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: [...chunk, ...remainingItems] });
            rescheduleOzonUnarchiveQueueAutoSoon("api_limit_queue").catch((rescheduleError) => {
              logger.warn("ozon unarchive queue reschedule failed", { detail: rescheduleError?.message || String(rescheduleError) });
            });
            actions.push(...ozonUnarchiveQueuedActions([...chunk, ...remainingItems], queueState, {
              warning: "ozon_unarchive_daily_limit_queued",
              nextRetryAt,
            }));
            break;
          } else {
            const nextRetryAt = nextOzonUnarchiveVisibilityRetryAt();
            queueState = queueOzonUnarchiveItems(queueState, chunk, {
              nextRetryAt,
              warning: "ozon_unarchive_api_error",
              error: detail,
              attempted: true,
            });
            await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: chunk });
            rescheduleOzonUnarchiveQueueAutoSoon("api_error_queue").catch((rescheduleError) => {
              logger.warn("ozon unarchive queue reschedule failed", { detail: rescheduleError?.message || String(rescheduleError) });
            });
            actions.push(...chunk.map((item) => ({
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
          }
        }
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

