app.get("/api/warehouse/yandex-quality-candidates", requireAdmin, async (request, response, next) => {
  try {
    const threshold = Math.max(0, Math.min(100, Math.round(Number(request.query.threshold ?? 40) || 40)));
    const limit = cleanLimit(request.query.limit, 30000, 50000);
    const resultLimit = cleanLimit(request.query.resultLimit, 300, 1000);
    const warehouse = await readWarehouse();
    const yandexProducts = (warehouse.products || [])
      .map((product) => normalizeWarehouseProduct(product))
      .filter((product) => product.marketplace === "yandex" && cleanText(product.offerId))
      .slice(0, limit);

    if (parseBooleanSetting(request.query.cached || request.query.cacheOnly, false)) {
      const lowQuality = (warehouse.products || [])
        .filter((product) => {
          const normalized = normalizeWarehouseProduct(product);
          const rating = Number(normalized.yandex?.extra?.cardQuality?.contentRating);
          return normalized.marketplace === "yandex" && Number.isFinite(rating) && rating <= threshold;
        })
        .sort((a, b) => {
          const qa = Number(normalizeWarehouseProduct(a).yandex?.extra?.cardQuality?.contentRating || 0);
          const qb = Number(normalizeWarehouseProduct(b).yandex?.extra?.cardQuality?.contentRating || 0);
          return qa - qb || String(a.offerId || "").localeCompare(String(b.offerId || ""));
        });
      return response.json({
        ok: true,
        cached: true,
        threshold,
        checked: yandexProducts.length,
        qualityLoaded: (warehouse.products || []).filter((product) => Number.isFinite(Number(normalizeWarehouseProduct(product).yandex?.extra?.cardQuality?.contentRating))).length,
        total: lowQuality.length,
        errors: [],
        products: lowQuality.slice(0, resultLimit).map(buildAiQualityReviewRow),
      });
    }

    const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
    if (!shops.length) return response.status(400).json({ error: "Yandex Market is not configured." });

    const qualityByTargetOffer = new Map();
    const errors = [];

    for (const shop of shops) {
      const offerIds = yandexProducts
        .filter((product) => matchesYandexTarget(product.target, shop.id))
        .map((product) => product.offerId);
      for (const chunk of chunkArray(offerIds, 200)) {
        try {
          const rows = await getYandexOfferCardsContentStatus(shop, chunk, { withRecommendations: true });
          for (const row of rows) qualityByTargetOffer.set(yandexTargetOfferKey(shop.id, row.offerId), row);
        } catch (error) {
          errors.push({ target: shop.id, error: error?.message || "yandex_card_quality_failed" });
        }
      }
    }

    const now = new Date().toISOString();
    const changedProducts = [];
    const lowQuality = [];
    for (const product of warehouse.products || []) {
      const normalized = normalizeWarehouseProduct(product);
      if (normalized.marketplace !== "yandex" || !normalized.offerId) continue;
      const shop = getYandexShopByTarget(normalized.target);
      const quality = shop ? qualityByTargetOffer.get(yandexTargetOfferKey(shop.id, normalized.offerId)) : null;
      if (!quality) continue;
      product.yandex = normalizeYandexDraft({
        ...(product.yandex || {}),
        extra: {
          ...(product.yandex?.extra || {}),
          cardQuality: quality,
        },
      });
      product.updatedAt = now;
      changedProducts.push(product);
      if (Number(quality.contentRating || 0) <= threshold) lowQuality.push(product);
    }
    if (changedProducts.length) {
      await writeWarehouseProductPatch(changedProducts, { reason: "yandex_card_quality_find", writeLinks: false });
    }

    lowQuality.sort((a, b) => {
      const qa = Number(normalizeWarehouseProduct(a).yandex?.extra?.cardQuality?.contentRating || 0);
      const qb = Number(normalizeWarehouseProduct(b).yandex?.extra?.cardQuality?.contentRating || 0);
      return qa - qb || String(a.offerId || "").localeCompare(String(b.offerId || ""));
    });

    response.json({
      ok: true,
      threshold,
      checked: yandexProducts.length,
      qualityLoaded: qualityByTargetOffer.size,
      total: lowQuality.length,
      errors,
      products: lowQuality.slice(0, resultLimit).map(buildAiQualityReviewRow),
    });
  } catch (error) {
    next(error);
  }
});
