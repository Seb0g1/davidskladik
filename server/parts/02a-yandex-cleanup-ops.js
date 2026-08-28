async function buildYandexCleanupPreview({ protectedBrands = [], limit = 50000 } = {}) {
  const seenBusinesses = new Set();
  const shops = getYandexShops().filter((shop) => {
    if (!shop.apiKey || !shop.businessId) return false;
    const businessKey = String(shop.businessId);
    if (seenBusinesses.has(businessKey)) return false;
    seenBusinesses.add(businessKey);
    return true;
  });
  const maxItems = Math.max(1, Math.min(50000, Number(limit || 50000) || 50000));
  const warnings = [];
  const rows = [];
  if (!shops.length) {
    return { ok: true, warnings: ["Yandex Market не настроен."], rows, summary: summarizeYandexCleanupPreview(rows) };
  }

  for (const shop of shops) {
    const remaining = Math.max(0, maxItems - rows.length);
    if (!remaining) break;
    try {
      const active = await getYandexOfferMappings(shop, remaining, { archived: false });
      const archivedRemaining = Math.max(0, maxItems - rows.length - active.length);
      const archived = archivedRemaining > 0
        ? await getYandexOfferMappings(shop, archivedRemaining, { archived: true })
        : [];
      const byOfferId = new Map();
      for (const item of [...active, ...archived]) {
        const offer = pickYandexOfferFromMapping(item);
        const offerId = cleanText(offer.offerId || item.offerId || item.mapping?.offerId);
        if (offerId) byOfferId.set(`${shop.businessId}:${offerId}`, item);
      }
      for (const item of byOfferId.values()) {
        rows.push(buildYandexCleanupCandidate(item, shop, protectedBrands));
        if (rows.length >= maxItems) break;
      }
    } catch (error) {
      const label = error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex «${shop.name || shop.id}»: не удалось загрузить товары (${label})`);
      logger.warn("yandex cleanup preview failed", { shop: shop.id, detail: label });
    }
  }

  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    protectedBrands: parseProtectedBrandList(protectedBrands),
    warnings,
    summary: summarizeYandexCleanupPreview(rows),
    rows,
  };
}

async function deleteYandexOfferIds(shop, offerIds = []) {
  const results = [];
  for (const chunk of chunkArray(offerIds.map(cleanText).filter(Boolean), 500)) {
    if (!chunk.length) continue;
    try {
      const payload = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-mappings/delete`, { offerIds: chunk });
      const notDeleted = new Set(
        [
          ...(Array.isArray(payload?.notDeletedOfferIds) ? payload.notDeletedOfferIds : []),
          ...(Array.isArray(payload?.result?.notDeletedOfferIds) ? payload.result.notDeletedOfferIds : []),
        ].map(cleanText).filter(Boolean),
      );
      results.push(...chunk.map((offerId) => (
        notDeleted.has(offerId)
          ? { offerId, ok: false, method: "delete", error: "not_deleted_by_yandex" }
          : { offerId, ok: true, method: "delete" }
      )));
    } catch (error) {
      const detail = error?.message || "delete_failed";
      results.push(...chunk.map((offerId) => ({ offerId, ok: false, method: "delete", error: detail })));
    }
  }
  return results;
}

async function deleteYandexCleanupRows(rows = []) {
  const byShop = new Map();
  for (const row of rows) {
    if (row.action !== "delete" || !row.offerId || !row.shopId) continue;
    if (!byShop.has(row.shopId)) byShop.set(row.shopId, []);
    byShop.get(row.shopId).push(row.offerId);
  }
  const results = [];
  for (const [shopId, offerIds] of byShop.entries()) {
    const shop = getYandexShopByTarget(shopId);
    if (!shop) {
      results.push(...offerIds.map((offerId) => ({ offerId, shopId, ok: false, error: "shop_not_found" })));
      continue;
    }
    const deleted = await deleteYandexOfferIds(shop, offerIds);
    results.push(...deleted.map((item) => ({ ...item, shopId })));
  }
  return results;
}

