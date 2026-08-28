  const results = [];
  const failed = [];
  const ozonVerifiedById = new Map();
  touchAutoPricePushHeartbeat({
    phase: "start",
    selected: selected.length,
    readyToSend: items.length,
    reason: cleanText(reason) || "auto-price-push",
  });
  const pricePushHeartbeatTimer = items.length
    ? setInterval(() => {
      touchAutoPricePushHeartbeat({
        phase: "sending",
        readyToSend: items.length,
        results: results.length,
        failed: failed.length,
        reason: cleanText(reason) || "auto-price-push",
      });
    }, autoPricePushHeartbeatLogMs)
    : null;
  try {
  for (const account of getOzonAccounts()) {
    const targetItems = items.filter((item) => item.marketplace === "ozon" && matchesOzonTarget(item.target, account.id));
    const ozonItems = targetItems
      .map((item) => ({ item, payload: buildOzonPricePayload(item) }))
      .filter((entry) => entry.payload.offer_id && Number(entry.payload.price) > 0);
    if (!ozonItems.length) continue;
    const sent = await sendOzonPricePayloadChunks(account, ozonItems.map((entry) => entry.payload));
    results.push(...sent.results.map((entry) => ({ target: account.id, response: entry.response, count: entry.count })));
    const failedOfferIds = new Map(sent.failed.map((entry) => [String(entry.payload.offer_id), entry.error]));
    const acceptedOzonItems = ozonItems.filter((entry) => !failedOfferIds.has(String(entry.payload.offer_id)));
    if (acceptedOzonItems.length) {
      await upsertSalesAutomationSkuStates(acceptedOzonItems.map((entry) => ({
        ...entry.item,
        productId: entry.item.productId || entry.item.id,
        currentPrice: entry.item.oldPrice,
        targetPrice: entry.item.price,
        priceStatus: "pending",
        stockStatus: "pending",
        unarchiveStatus: "pending",
        reason: verify === false ? "api_accepted" : "verification_pending",
        lastPriceSentAt: sentAt,
        lastCalculatedAt: sentAt,
        priceIntentId,
        priceApplyStatus: verify === false ? "api_accepted" : "verification_pending",
        lastRequestedPrice: roundPrice(entry.item.price),
        lastPriceRequestAt: sentAt,
        selectedSupplier: entry.item.supplier || entry.item.selectedSupplier || null,
        supplierPurchasePrice: Number(entry.item.supplier?.price || entry.item.supplier?.supplierPrice || 0) || null,
        supplierPurchaseCurrency: cleanText(entry.item.supplier?.priceCurrency || entry.item.supplier?.currency) || null,
        effectiveFinalPrice: Number(entry.item.supplier?.effectiveFinalPrice || entry.item.supplier?.calculatedPrice || entry.item.price || 0) || null,
        sourceEvent: entry.item.sourceEvent || sourceEvent || reason,
        priceSource: entry.item.priceSource || entry.item.supplier?.priceSource || null,
        raw: {
          ...entry.item,
          payload: entry.payload,
          priceIntentId,
          priceApplyStatus: verify === false ? "api_accepted" : "verification_pending",
          lastRequestedPrice: roundPrice(entry.item.price),
          lastPriceRequestAt: sentAt,
          selectedSupplier: entry.item.supplier || entry.item.selectedSupplier || null,
          supplierPurchasePrice: Number(entry.item.supplier?.price || entry.item.supplier?.supplierPrice || 0) || null,
          supplierPurchaseCurrency: cleanText(entry.item.supplier?.priceCurrency || entry.item.supplier?.currency) || null,
          effectiveFinalPrice: Number(entry.item.supplier?.effectiveFinalPrice || entry.item.supplier?.calculatedPrice || entry.item.price || 0) || null,
          sourceEvent: entry.item.sourceEvent || sourceEvent || reason,
          priceSource: entry.item.priceSource || entry.item.supplier?.priceSource || null,
        },
      }))).catch((error) => logger.warn("sales automation ozon accepted update failed", { detail: error?.message || String(error) }));
    }
    if (acceptedOzonItems.length && verify !== false && ozonPriceVerifyDeferred !== true) {
      const verification = await verifyOzonPriceApplied(account, acceptedOzonItems, { priceIntentId, sentAt });
      for (const entry of verification.verified) {
        ozonVerifiedById.set(String(entry.item.id), entry);
      }
      failed.push(...verification.failed.map((entry) => ({
        ...entry.item,
        error: entry.error?.message || "ozon_price_not_applied",
        errorCode: entry.error?.code || "ozon_price_not_applied",
        marketplace: "ozon",
        verificationAttempts: entry.verificationAttempts,
      })));
    } else {
      // Accepted-by-API counts as success for the pipeline; real verification runs in the
      // background and force-requeues prices Ozon did not apply (see scheduleDeferred...).
      for (const entry of acceptedOzonItems) ozonVerifiedById.set(String(entry.item.id), {
        ...entry,
        verifiedPrice: roundPrice(entry.item.price),
        verifiedAt: sentAt,
        verificationAttempts: 0,
      });
      if (acceptedOzonItems.length && verify !== false && ozonPriceVerifyDeferred === true) {
        scheduleDeferredOzonPriceVerification(account, acceptedOzonItems, { priceIntentId, sentAt });
      }
    }
    failed.push(...ozonItems
      .filter((entry) => failedOfferIds.has(String(entry.payload.offer_id)))
      .map((entry) => ({
        ...entry.item,
        error: failedOfferIds.get(String(entry.payload.offer_id))?.message || "send_failed",
        errorCode: failedOfferIds.get(String(entry.payload.offer_id))?.code || "send_failed",
        marketplace: "ozon",
      })));
  }

  for (const shop of getYandexShops()) {
    const targetItems = items.filter((item) => item.marketplace === "yandex" && matchesYandexTarget(item.target, shop.id));
    const yandexItems = targetItems
      .map((item) => ({
        offerId: String(item.offerId || "").trim(),
        price: { value: roundPrice(item.price), currencyId: "RUR" },
      }))
      .filter((item) => item.offerId && item.price.value > 0);
    if (!yandexItems.length) continue;
    const yandexOfferIdToItem = new Map(
      targetItems.map((item) => [String(item.offerId || "").trim(), item]).filter(([k]) => k),
    );
    for (const chunk of chunkArray(yandexItems, 500)) {
      try {
        results.push({
          target: shop.id,
          response: await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, { offers: chunk }),
        });
      } catch (error) {
        const detail = error?.message || "send_failed";
        failed.push(...chunk
          .map((y) => yandexOfferIdToItem.get(y.offerId))
          .filter(Boolean)
          .map((item) => ({ ...item, error: detail, marketplace: "yandex" })));
      }
    }
  }
  } finally {
    if (pricePushHeartbeatTimer) clearInterval(pricePushHeartbeatTimer);
    touchAutoPricePushHeartbeat({
      phase: "done",
      readyToSend: items.length,
      results: results.length,
      failed: failed.length,
      reason: cleanText(reason) || "auto-price-push",
    });
  }
