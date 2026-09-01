app.post("/api/warehouse/products/:id/yandex-quality-draft/generate", requireAdmin, async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const normalized = normalizeWarehouseProduct(product);
    if (normalized.marketplace !== "yandex") return response.status(400).json({ error: "Генерация качества доступна только для Yandex Market." });

    const count = Math.min(5, Math.max(1, Math.floor(Number(request.body.imagesCount || request.body.count || 5) || 5)));
    const quality = normalized.yandex?.extra?.cardQuality || {};
    const before = cloneAuditValue({
      id: product.id,
      aiContentDrafts: product.aiContentDrafts || [],
      aiImages: product.aiImages || [],
      updatedAt: product.updatedAt,
    });
    let savedDraft = null;
    const errors = [];

    try {
      const draft = await generateAiProductContentDraft(normalized, { marketplace: "yandex" });
      savedDraft = normalizeAiContentDraft({
        ...draft,
        marketplace: "yandex",
        source: "yandex_card_quality_manual",
        qualityBefore: quality.contentRating,
        recommendations: quality.recommendations,
        model: (await readEffectiveAiSettings()).textModel || openaiTextModel,
      });
      if (savedDraft) {
        product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
      }
    } catch (error) {
      errors.push({ type: "content", error: error?.message || "ai_content_draft_failed" });
      if (isOpenAiBillingLimitError(error)) {
        return response.status(400).json({ error: error.message, code: error.code || "ai_billing_limit", errors });
      }
    }

    const batchId = crypto.randomUUID();
    const imageDrafts = [];
    for (let index = 1; index <= count; index += 1) {
      try {
        const imageDraft = await generateOzonAiImageDraft(normalized, {
          prompt: `Create exactly one marketplace-ready product photo ${index} of ${count} for ${normalized.name || normalized.offerId}. This is one image only; do not create a collage, grid, contact sheet, split-screen, multi-panel layout, or multiple bottles in one image. Clean square ecommerce packshot, realistic perfume bottle only, full bottle visible from cap to base, centered, upright, not cropped, 10-15% empty margin on every side. No box, no outer packaging, no cartons, no props, no headline, no typography, no text overlays, no extra objects. Text is allowed only if it already exists on the original bottle label.`,
          sourceImageUrl: request.body.sourceImageUrl || "",
          batchId,
          variantIndex: index,
          variantTotal: count,
        }, request);
        imageDrafts.push(imageDraft);
      } catch (error) {
        errors.push({ type: "image", index, error: error?.message || "ai_image_draft_failed" });
        if (isOpenAiBillingLimitError(error)) break;
      }
    }
    if (imageDrafts.length) {
      product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), ...imageDrafts]);
    }
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "yandex_card_quality_manual_draft", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const row = buildAiQualityReviewRow(savedProduct);
    response.json({
      ok: Boolean(savedDraft && imageDrafts.length === count),
      partial: Boolean(errors.length && (savedDraft || imageDrafts.length)),
      contentDraft: savedDraft,
      imageDrafts,
      batchId,
      errors,
      row,
      product: savedProduct,
    });
    appendAudit(request, "yandex.card_quality.manual_generate", {
      productId: product.id,
      offerId: normalized.offerId,
      batchId,
      imagesRequested: count,
      imagesCreated: imageDrafts.length,
      oldValue: before,
      newValue: { id: savedProduct.id, aiContentDrafts: savedProduct.aiContentDrafts || [], aiImages: savedProduct.aiImages || [] },
    }).catch((auditError) => logger.warn("yandex quality manual generate audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/yandex-quality-draft/send", requireAdmin, async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const normalizedBefore = normalizeWarehouseProduct(product);
    if (normalizedBefore.marketplace !== "yandex") return response.status(400).json({ error: "Отправка качества доступна только для Yandex Market." });

    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    const contentDraftId = cleanText(request.body.contentDraftId);
    const imageBatchId = cleanText(request.body.imageBatchId);
    const primaryImageDraftId = cleanText(request.body.primaryImageDraftId);
    const contentDraft = contentDraftId
      ? product.aiContentDrafts.find((draft) => draft.id === contentDraftId)
      : latestAiContentDraft(product, "pending");
    let imageBatch = imageBatchId
      ? product.aiImages.filter((draft) => draft.batchId === imageBatchId && draft.resultUrl)
      : latestAiImageBatch(product, "pending").drafts;
    let primaryImageDraft = primaryImageDraftId
      ? imageBatch.find((draft) => draft.id === primaryImageDraftId)
      : imageBatch[0];
    if (!contentDraft && !imageBatch.length) return response.status(400).json({ error: "Нет черновика текста или фото для отправки." });

    const before = cloneAuditValue({
      id: product.id,
      yandex: product.yandex || {},
      aiContentDrafts: product.aiContentDrafts || [],
      aiImages: product.aiImages || [],
      imageUrl: product.imageUrl || "",
      updatedAt: product.updatedAt,
    });
    if (contentDraft) {
      const enhanced = applyAiContentDraftToProduct(product, contentDraft, "yandex");
      Object.assign(product, enhanced);
      product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
      const draftToApprove = product.aiContentDrafts.find((draft) => draft.id === contentDraft.id);
      if (draftToApprove) {
        draftToApprove.status = "approved";
        draftToApprove.reviewedAt = new Date().toISOString();
      }
      product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
      imageBatch = imageBatchId
        ? product.aiImages.filter((draft) => draft.batchId === imageBatchId && draft.resultUrl)
        : latestAiImageBatch(product, "pending").drafts;
      primaryImageDraft = primaryImageDraftId
        ? imageBatch.find((draft) => draft.id === primaryImageDraftId)
        : imageBatch[0];
    }
    let yandexPicturesForSend = [];
    if (imageBatch.length) {
      for (const draft of imageBatch) {
        draft.resultUrl = await normalizeMarketplaceImageUrlForSend(draft.resultUrl, request);
      }
      if (primaryImageDraft) {
        primaryImageDraft.resultUrl = await normalizeMarketplaceImageUrlForSend(primaryImageDraft.resultUrl, request);
      }
      const primaryUrl = primaryImageDraft?.resultUrl || imageBatch[0]?.resultUrl;
      if (!primaryUrl) return response.status(400).json({ error: "В выбранных AI-фото нет URL результата." });
      const reviewedAt = new Date().toISOString();
      imageBatch.forEach((draft) => {
        if (draft.status === "pending") {
          draft.status = "approved";
          draft.reviewedAt = reviewedAt;
        }
      });
      const batchUrls = [
        primaryUrl,
        ...imageBatch.map((draft) => draft.resultUrl).filter((url) => url && url !== primaryUrl),
      ].slice(0, 10);
      const extraCardUrls = await marketplaceExtraCardUrls("yandex", request, { product });
      yandexPicturesForSend = appendUniqueImages(batchUrls, extraCardUrls).slice(0, 10);
      const yandex = product.yandex || {};
      const currentPictures = splitList(yandex.pictures);
      product.yandex = normalizeYandexDraft({
        ...yandex,
        pictures: [...yandexPicturesForSend, ...currentPictures.filter((url) => !yandexPicturesForSend.includes(url))],
      });
      product.imageUrl = primaryUrl;
    }
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "yandex_card_quality_manual_send", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const mode = contentDraft && imageBatch.length ? "both" : (imageBatch.length ? "image" : "content");
    const yandexSend = await sendApprovedYandexProductContent(savedProduct, { mode, pictures: yandexPicturesForSend });
    response.json({ ok: Boolean(yandexSend.ok), product: savedProduct, row: buildAiQualityReviewRow(savedProduct), yandexSend });
    appendAudit(request, "yandex.card_quality.manual_send", {
      productId: product.id,
      offerId: normalizedBefore.offerId,
      mode,
      yandexSend,
      oldValue: before,
      newValue: { id: savedProduct.id, yandex: savedProduct.yandex || {}, aiContentDrafts: savedProduct.aiContentDrafts || [], aiImages: savedProduct.aiImages || [] },
    }).catch((auditError) => logger.warn("yandex quality manual send audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});
