async function runYandexCardQualityAiDraftOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const threshold = Math.max(0, Math.min(100, Math.round(Number(payload?.threshold ?? 40) || 40)));
  const draftLimit = Math.max(0, Math.min(100, Math.round(Number(payload?.draftLimit ?? 20) || 20)));
  const generateImages = payload?.generateImages !== false;
  const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  if (!shops.length) {
    const error = new Error("Yandex Market is not configured.");
    error.statusCode = 400;
    throw error;
  }

  const warehouse = await readWarehouse();
  const yandexProducts = (warehouse.products || [])
    .map((product) => normalizeWarehouseProduct(product))
    .filter((product) => product.marketplace === "yandex" && cleanText(product.offerId))
    .slice(0, limit);
  await options.onProgress?.({ progress: 8, summary: `Checking Yandex card quality for ${yandexProducts.length} products.` });

  const qualityByTargetOffer = new Map();
  const qualityErrors = [];
  for (const shop of shops) {
    const offerIds = yandexProducts
      .filter((product) => matchesYandexTarget(product.target, shop.id))
      .map((product) => product.offerId);
    for (const chunk of chunkArray(offerIds, 200)) {
      try {
        const rows = await getYandexOfferCardsContentStatus(shop, chunk, { withRecommendations: true });
        for (const row of rows) {
          qualityByTargetOffer.set(yandexTargetOfferKey(shop.id, row.offerId), row);
        }
      } catch (error) {
        qualityErrors.push({
          target: shop.id,
          type: "quality",
          error: error?.message || "yandex_card_quality_failed",
        });
      }
    }
  }

  const now = new Date().toISOString();
  const changedProducts = [];
  const lowQualityProducts = [];
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
    if (Number(quality.contentRating || 0) < threshold) {
      lowQualityProducts.push({ product, quality });
    }
  }
  if (changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "yandex_card_quality_sync", writeLinks: false });
  }
  await options.onProgress?.({ progress: 35, summary: `Low quality cards: ${lowQualityProducts.length}. Creating AI drafts.` });

  const draftResults = [];
  const draftProducts = [];
  let stoppedByBillingLimit = false;
  let imageGenerationStoppedReason = "";
  for (const { product, quality } of lowQualityProducts.slice(0, draftLimit)) {
    const normalized = normalizeWarehouseProduct(product);
    try {
      const draft = await generateAiProductContentDraft(normalized, { marketplace: "yandex" });
      const savedDraft = normalizeAiContentDraft({
        ...draft,
        marketplace: "yandex",
        source: "yandex_card_quality",
        qualityBefore: quality.contentRating,
        recommendations: quality.recommendations,
        model: (await readEffectiveAiSettings()).textModel || openaiTextModel,
      });
      if (savedDraft) {
        product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
      }
      let imageDraftCreated = false;
      let imageError = "";
      if (generateImages && !imageGenerationStoppedReason) {
        try {
          const imageDraft = await generateOzonAiImageDraft(normalized, {
            prompt: `Create a clean marketplace product photo for ${normalized.name || normalized.offerId}. White background, realistic perfume product image, no text overlays.`,
          });
          product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), imageDraft]);
          imageDraftCreated = true;
        } catch (imageDraftError) {
          imageError = imageDraftError?.message || "ai_image_draft_failed";
          if (isOpenAiBillingLimitError(imageDraftError)) imageGenerationStoppedReason = imageError;
        }
      }
      product.updatedAt = new Date().toISOString();
      draftProducts.push(product);
      draftResults.push({
        id: product.id,
        offerId: normalized.offerId,
        target: normalized.target,
        contentRating: quality.contentRating,
        ok: Boolean(savedDraft),
        contentDraft: Boolean(savedDraft),
        imageDraft: imageDraftCreated,
        warning: imageError || undefined,
        error: savedDraft ? undefined : (imageError || "ai_content_draft_empty"),
      });
    } catch (error) {
      draftResults.push({
        id: product.id,
        offerId: normalized.offerId,
        target: normalized.target,
        contentRating: quality.contentRating,
        ok: false,
        error: error?.message || "ai_draft_failed",
        fatal: isOpenAiBillingLimitError(error) || undefined,
      });
      if (isOpenAiBillingLimitError(error)) {
        stoppedByBillingLimit = true;
        break;
      }
    }
  }
  if (draftProducts.length) {
    await writeWarehouseProductPatch(draftProducts, { reason: "yandex_card_quality_ai_drafts", writeLinks: false });
  }

  const warnings = draftResults
    .filter((item) => item.ok && item.warning)
    .map((item) => ({ id: item.id, offerId: item.offerId, type: "image", error: item.warning }));
  const failed = [...qualityErrors, ...draftResults.filter((item) => !item.ok)];
  const result = {
    ok: failed.length === 0,
    partial: Boolean(failed.length && draftResults.some((item) => item.ok)),
    limit,
    threshold,
    checked: yandexProducts.length,
    qualityLoaded: qualityByTargetOffer.size,
    lowQuality: lowQualityProducts.length,
    draftsCreated: draftResults.filter((item) => item.ok).length,
    imageDraftsCreated: draftResults.filter((item) => item.imageDraft).length,
    stoppedByBillingLimit,
    imageGenerationStoppedReason,
    warnings,
    failed: failed.length,
    results: draftResults,
    errors: failed,
    summary: `Yandex quality checked ${yandexProducts.length}; below ${threshold}: ${lowQualityProducts.length}; AI drafts: ${draftResults.filter((item) => item.ok).length}; image drafts: ${draftResults.filter((item) => item.imageDraft).length}; warnings: ${warnings.length}; errors: ${failed.length}.`,
  };
  logger.info("yandex card quality ai drafts complete", {
    checked: result.checked,
    qualityLoaded: result.qualityLoaded,
    lowQuality: result.lowQuality,
    draftsCreated: result.draftsCreated,
    imageDraftsCreated: result.imageDraftsCreated,
    stoppedByBillingLimit: result.stoppedByBillingLimit,
    imageGenerationStopped: Boolean(result.imageGenerationStoppedReason),
    warnings: warnings.length,
    failed: result.failed,
    sampleErrors: failed.slice(0, 5).map((item) => ({
      id: item.id,
      offerId: item.offerId,
      error: item.error,
    })),
    sampleWarnings: warnings.slice(0, 5),
  });
  return result;
}

async function runPriceMasterGroupLinksRepairOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 0);
  const limit = Math.max(0, Math.min(100000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 0));
  if (shouldUsePostgresStorage()) {
    const prisma = getPrisma();
    if (prisma) {
      const result = await repairLinkedWarehouseCatalogPostgres(prisma, {
        dryRun: false,
        batchSize: Math.max(50, Math.min(2000, Number(payload?.batchSize) || 500)),
        limit,
        onProgress: options.onProgress,
      });
      return {
        ...result,
        processedGroups: result.processedGroups || result.linkSync?.processedGroups || 0,
        repairedGroups: result.repairedGroups || result.linkSync?.repairedGroups || 0,
        changedProducts: result.changedProducts || result.linkSync?.changedProducts || 0,
        changedProductIds: result.changedProductIds || result.linkSync?.changedProductIds || [],
        skippedGroups: result.skippedGroups || result.linkSync?.skippedGroups || 0,
        groups: [],
      };
    }
  }

  const warehouse = await readWarehouse();
  const groups = collectWarehouseLinkRepairGroups(warehouse.products || []);
  const candidates = Array.from(groups.entries())
    .filter(([, products]) => products.some((product) => (product.links || []).length))
    .slice(0, limit || 50000);
  const changedProducts = [];
  const changedIds = [];
  const repairedGroups = [];
  let skippedGroups = 0;
  const now = new Date().toISOString();
  let processed = 0;

  for (const [groupKey, products] of candidates) {
    processed += 1;
    const before = warehouseGroupLinkSignature(products);
    const pairPatches = applyOzonYandexPairGroupIds(products);
    const mergedProducts = Array.from(new Map(
      [...products, ...pairPatches].map((product) => [String(product.id), product]),
    ).values());
    if (before.ok && !pairPatches.length) {
      skippedGroups += 1;
    } else {
      const syncResult = syncWarehouseProductGroupLinks(mergedProducts, { now, username: "operation" });
      const updates = Array.from(new Map(
        [...pairPatches, ...(syncResult.changedProducts || [])].map((product) => [String(product.id), product]),
      ).values());
      if (updates.length) {
        changedProducts.push(...updates);
        changedIds.push(...updates.map((product) => product.id));
        repairedGroups.push({
          groupKey,
          products: mergedProducts.map((product) => product.id),
          before,
          after: warehouseGroupLinkSignature(mergedProducts),
        });
      } else {
        skippedGroups += 1;
      }
    }
    if (processed % 50 === 0) {
      await options.onProgress?.({
        progress: 10 + (processed / Math.max(1, candidates.length)) * 80,
        summary: `Checked ${processed} of ${candidates.length} PriceMaster groups.`,
      });
    }
  }

  const uniqueChanged = Array.from(new Map(changedProducts.map((product) => [String(product.id), product])).values());
  if (uniqueChanged.length) {
    for (const chunk of chunkArray(uniqueChanged, 200)) {
      await writeWarehouseProductPatch(chunk, { reason: "warehouse_links_repair_group" });
    }
    const uniqueIds = Array.from(new Set(changedIds.map(String)));
    await queueLinkedProductActivation(uniqueIds, "link_repair_group", { username: "operation" });
  }

  return {
    ok: true,
    processedGroups: candidates.length,
    repairedGroups: repairedGroups.length,
    changedProducts: uniqueChanged.length,
    changedProductIds: uniqueChanged.map((product) => product.id),
    skippedGroups,
    groups: repairedGroups.slice(0, 200),
    summary: `PriceMaster groups checked ${candidates.length}; repaired ${repairedGroups.length}; changed products ${uniqueChanged.length}; skipped ${skippedGroups}.`,
  };
}

