function findWarehouseProductForMarketplace(warehouse = {}, seedProduct = {}, marketplace = "") {
  const wanted = cleanText(marketplace || seedProduct.marketplace || "").toLowerCase();
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const siblings = expandWarehouseProductsToGroups(products, [seedProduct]);
  return siblings.find((item) => cleanText(item.marketplace).toLowerCase() === wanted)
    || (cleanText(seedProduct.marketplace).toLowerCase() === wanted ? seedProduct : null);
}

function applyAiImageDraftToProduct(product = {}, draftId = "", options = {}) {
  const next = { ...normalizeWarehouseProduct(product) };
  next.aiImages = normalizeAiImageDrafts(next.aiImages || []);
  const draft = next.aiImages.find((item) => item.id === draftId);
  if (!draft) {
    const error = new Error("AI image draft not found.");
    error.statusCode = 404;
    error.code = "ai_image_draft_not_found";
    throw error;
  }
  if (!draft.resultUrl) {
    const error = new Error("AI image draft has no result URL.");
    error.statusCode = 400;
    error.code = "ai_image_result_url_missing";
    throw error;
  }
  const batchDrafts = draft.batchId
    ? next.aiImages.filter((item) => item.batchId === draft.batchId && item.resultUrl)
    : [draft];
  const batchUrls = [
    draft.resultUrl,
    ...batchDrafts.map((item) => item.resultUrl).filter((url) => url && url !== draft.resultUrl),
  ];
  const now = new Date().toISOString();
  draft.status = "approved";
  draft.reviewedAt = now;
  if (options.sentMarketplace) {
    draft.sentAt = now;
    draft.sentMarketplace = cleanText(options.sentMarketplace);
  }
  batchDrafts.forEach((item) => {
    if (item.status === "pending") {
      item.status = "approved";
      item.reviewedAt = now;
    }
    if (options.sentMarketplace) {
      item.sentAt = now;
      item.sentMarketplace = cleanText(options.sentMarketplace);
    }
  });

  if (next.marketplace === "yandex") {
    const yandex = next.yandex || {};
    const pictures = splitList(yandex.pictures);
    next.yandex = normalizeYandexDraft({
      ...yandex,
      pictures: [...batchUrls, ...pictures.filter((url) => !batchUrls.includes(url))],
    });
  } else {
    const ozon = next.ozon || {};
    const images = splitList(ozon.images);
    next.ozon = normalizeOzonDraft({
      ...ozon,
      primaryImage: draft.resultUrl,
      images: [...batchUrls, ...images.filter((url) => !batchUrls.includes(url))],
    });
  }
  next.imageUrl = draft.resultUrl;
  next.updatedAt = now;
  return { product: normalizeWarehouseProduct(next), draft, batchDrafts, batchUrls };
}

async function normalizeAiImageDraftUrlsForMarketplaceSend(product = {}, draftId = "", request = null) {
  product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
  const draft = product.aiImages.find((item) => item.id === draftId);
  if (!draft) {
    const error = new Error("AI image draft not found.");
    error.statusCode = 404;
    error.code = "ai_image_draft_not_found";
    throw error;
  }
  const batchDrafts = draft.batchId
    ? product.aiImages.filter((item) => item.batchId === draft.batchId && item.resultUrl)
    : [draft];
  if (!batchDrafts.length || !draft.resultUrl) {
    throw marketplaceImageError("ai_image_result_url_missing", "В AI-черновике нет URL фото для отправки.");
  }
  for (const item of batchDrafts) {
    item.resultUrl = await normalizeMarketplaceImageUrlForSend(item.resultUrl, request);
  }
  draft.resultUrl = await normalizeMarketplaceImageUrlForSend(draft.resultUrl, request);
  return { draft, batchDrafts };
}
