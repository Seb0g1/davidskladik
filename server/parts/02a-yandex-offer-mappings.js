async function getYandexOfferMappings(shop, limit = Number.POSITIVE_INFINITY, options = {}) {
  const maxItems = Number.isFinite(Number(limit)) && Number(limit) > 0 ? Number(limit) : Number.MAX_SAFE_INTEGER;
  const items = [];
  let pageToken = "";

  while (items.length < maxItems) {
    const pageLimit = Math.min(100, maxItems - items.length);
    const params = new URLSearchParams({ limit: String(pageLimit) });
    if (pageToken) params.set("pageToken", pageToken);
    const body = options.archived === true || options.archived === false
      ? { archived: options.archived }
      : undefined;

    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-mappings?${params.toString()}`,
      body,
    );
    const pageItems = data.result?.offerMappings || data.result?.offers || data.offerMappings || [];
    items.push(...pageItems);

    pageToken =
      data.result?.paging?.nextPageToken ||
      data.result?.nextPageToken ||
      data.paging?.nextPageToken ||
      data.nextPageToken ||
      "";
    if (!pageToken || !pageItems.length) break;
  }

  return items.slice(0, maxItems);
}

async function getYandexOfferMappingsByOfferIds(shop, offerIds = []) {
  const ids = [...new Set((Array.isArray(offerIds) ? offerIds : [])
    .map(cleanText)
    .filter(Boolean))];
  const items = [];
  if (!ids.length) return items;

  for (const chunk of chunkArray(ids, 100)) {
    const body = { offerIds: chunk };
    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-mappings`,
      body,
    );
    items.push(...(data.result?.offerMappings || data.result?.offers || data.offerMappings || []));
  }

  return items;
}

function normalizeYandexOfferCardQuality(item = {}, shop = {}) {
  const offerId = cleanText(item.offerId || item.offer_id || item.shopSku || item.sku);
  const contentRating = Number(item.contentRating ?? item.content_rating ?? item.rating ?? 0);
  const averageContentRating = Number(item.averageContentRating ?? item.average_content_rating ?? 0);
  const recommendations = Array.isArray(item.recommendations) ? item.recommendations : [];
  const errors = Array.isArray(item.errors) ? item.errors : [];
  const warnings = Array.isArray(item.warnings) ? item.warnings : [];
  return compactObject({
    offerId,
    target: shop.id || "",
    contentRating: Number.isFinite(contentRating) ? contentRating : 0,
    averageContentRating: Number.isFinite(averageContentRating) ? averageContentRating : undefined,
    contentRatingStatus: cleanText(item.contentRatingStatus || item.content_rating_status),
    cardStatus: cleanText(item.cardStatus || item.card_status),
    recommendations,
    errors,
    warnings,
    updatedAt: new Date().toISOString(),
  });
}

async function getYandexOfferCardsContentStatus(shop, offerIds = [], options = {}) {
  const ids = [...new Set((Array.isArray(offerIds) ? offerIds : [])
    .map(cleanText)
    .filter(Boolean))];
  const items = [];
  if (!ids.length) return items;
  const withRecommendations = options.withRecommendations !== false;

  for (const chunk of chunkArray(ids, 200)) {
    const params = new URLSearchParams({ limit: String(chunk.length) });
    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-cards?${params.toString()}`,
      { offerIds: chunk, withRecommendations },
    );
    const rows = data.result?.offerCards || data.result?.offers || data.offerCards || [];
    items.push(...rows.map((item) => normalizeYandexOfferCardQuality(item, shop)).filter((item) => item.offerId));
  }

  return items;
}

