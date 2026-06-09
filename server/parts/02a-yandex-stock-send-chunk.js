async function sendYandexStockChunk(shop, rows = []) {
  if (!shop?.campaignId) {
    const error = new Error("Yandex campaignId is required for stock updates");
    error.statusCode = 400;
    throw error;
  }
  const payload = buildYandexStockUpdatePayload(rows);
  if (!payload.skus.length) return null;
  const result = await yandexRequest(shop, "PUT", `/v2/campaigns/${shop.campaignId}/offers/stocks`, payload);
  if (apiPayloadHasErrors(result)) {
    const error = new Error(summarizeApiErrorPayload(result, "Yandex stock update failed"));
    error.statusCode = 400;
    error.yandex = result;
    throw error;
  }
  return result;
}

async function sendYandexStockRowsWithFallback(shop, rows = []) {
  const chunk = (Array.isArray(rows) ? rows : []).filter((row) => cleanText(row.offerId || row.sku));
  if (!chunk.length) return { sent: 0, failed: 0, results: [] };
  const target = shop.id;
  const targetName = shop.name || "Yandex Market";
  const targetCampaignId = cleanText(shop.campaignId || shop.campaign_id);
  try {
    await sendYandexStockChunk(shop, chunk);
    return {
      sent: chunk.length,
      failed: 0,
      results: [{ target, targetName, targetCampaignId, sent: chunk.length, ok: true }],
    };
  } catch (error) {
    const chunkError = error?.message || error?.code || "Yandex stock update failed";
    if (chunk.length === 1) {
      return {
        sent: 0,
        failed: 1,
        fallbackError: chunkError,
        results: chunk.map((item) => ({
          stage: "stock",
          offerId: item.offerId,
          target,
          targetName,
          targetCampaignId,
          stock: item.stock,
          ok: false,
          error: chunkError,
        })),
      };
    }
    const results = [];
    let sent = 0;
    let failed = 0;
    for (const item of chunk) {
      try {
        await sendYandexStockChunk(shop, [item]);
        sent += 1;
        results.push({
          stage: "stock",
          offerId: item.offerId,
          target,
          targetName,
          targetCampaignId,
          stock: item.stock,
          sent: 1,
          ok: true,
        });
      } catch (itemError) {
        failed += 1;
        results.push({
          stage: "stock",
          offerId: item.offerId,
          target,
          targetName,
          targetCampaignId,
          stock: item.stock,
          ok: false,
          error: itemError?.message || itemError?.code || chunkError,
        });
      }
    }
    return { sent, failed, fallbackError: chunkError, results };
  }
}

async function sendYandexOfferArchiveState(shop, offerIds = [], archived = false) {
  const ids = [...new Set((Array.isArray(offerIds) ? offerIds : [])
    .map(cleanText)
    .filter(Boolean))];
  if (!ids.length) return [];
  const endpoint = `/v2/businesses/${shop.businessId}/offer-mappings/${archived ? "archive" : "unarchive"}`;
  const type = archived ? "archive" : "unarchive";
  const results = [];
  for (const chunk of chunkArray(ids, 200)) {
    try {
      const result = await yandexRequest(shop, "POST", endpoint, { offerIds: chunk });
      if (apiPayloadHasErrors(result)) {
        const error = new Error(summarizeApiErrorPayload(result, `Yandex ${type} failed`));
        error.statusCode = 400;
        throw error;
      }
      results.push(...chunk.map((offerId) => ({ offerId, ok: true })));
    } catch (error) {
      const detail = error?.message || `${type}_failed`;
      results.push(...chunk.map((offerId) => ({ offerId, ok: false, error: detail })));
    }
  }
  return results;
}
