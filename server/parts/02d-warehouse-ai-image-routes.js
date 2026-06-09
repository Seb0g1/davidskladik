app.post("/api/warehouse/products/:id/ai-images/:draftId/approve", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], yandex: product.yandex || {}, ozon: product.ozon || {}, imageUrl: product.imageUrl || "", updatedAt: product.updatedAt });

    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    let draft = product.aiImages.find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI-черновик изображения не найден." });
    if (!draft.resultUrl) return response.status(400).json({ error: "В AI-черновике нет URL результата." });

    let batchDrafts = draft.batchId
      ? product.aiImages.filter((item) => item.batchId === draft.batchId && item.resultUrl)
      : [draft];
    if (normalizeWarehouseProduct(product).marketplace === "yandex") {
      const prepared = await normalizeAiImageDraftUrlsForMarketplaceSend(product, draft.id, request);
      draft = prepared.draft;
      batchDrafts = prepared.batchDrafts;
    }
    draft.status = "approved";
    draft.reviewedAt = new Date().toISOString();
    batchDrafts.forEach((item) => {
      if (item.status === "pending") {
        item.status = "approved";
        item.reviewedAt = draft.reviewedAt;
      }
    });
    const batchUrls = [draft.resultUrl, ...batchDrafts.map((item) => item.resultUrl).filter((url) => url && url !== draft.resultUrl)];
    const marketplace = normalizeWarehouseProduct(product).marketplace === "yandex" ? "yandex" : "ozon";
    const extraCardUrls = await marketplaceExtraCardUrls(marketplace, request);
    const sendBatchUrls = appendUniqueImages(batchUrls, extraCardUrls).slice(0, 10);
    if (normalizeWarehouseProduct(product).marketplace === "yandex") {
      const yandex = product.yandex || {};
      const pictures = splitList(yandex.pictures);
      product.yandex = normalizeYandexDraft({
        ...yandex,
        pictures: [...sendBatchUrls, ...pictures.filter((url) => !sendBatchUrls.includes(url))],
      });
    } else {
      const ozon = product.ozon || {};
      const images = splitList(ozon.images);
      product.ozon = normalizeOzonDraft({
        ...ozon,
        primaryImage: draft.resultUrl,
        images: [...sendBatchUrls, ...images.filter((url) => !sendBatchUrls.includes(url))],
      });
    }
    product.imageUrl = draft.resultUrl;
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_approve", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const yandexSend = normalizeWarehouseProduct(savedProduct).marketplace === "yandex"
      ? await sendApprovedYandexProductContent(savedProduct, { mode: "image", pictures: sendBatchUrls })
      : { ok: true, skipped: true, reason: "not_yandex" };
    response.json({ ok: true, draft, product: savedProduct, yandexSend });
    appendAudit(request, "warehouse.ai_image.approve", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      oldValue: before,
      yandexSend,
      newValue: { id: savedProduct.id, aiImages: savedProduct.aiImages || [], yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {}, imageUrl: savedProduct.imageUrl || "", updatedAt: savedProduct.updatedAt },
    }).catch((auditError) => logger.warn("ai image approve audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-images/:draftId/send", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const seedProduct = warehouse.products.find((item) => item.id === request.params.id);
    if (!seedProduct) return response.status(404).json({ error: "Product not found." });
    const sourceDraft = (normalizeAiImageDrafts(seedProduct.aiImages || [])).find((item) => item.id === request.params.draftId);
    if (!sourceDraft) return response.status(404).json({ error: "AI image draft not found." });
    if (!sourceDraft.resultUrl) return response.status(400).json({ error: "AI image draft has no result URL.", code: "ai_image_result_url_missing" });
    if (sourceDraft.status === "rejected") return response.status(400).json({ error: "AI image draft is rejected." });
    const marketplace = cleanText(request.body?.marketplace || seedProduct.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const product = findWarehouseProductForMarketplace(warehouse, seedProduct, marketplace);
    if (!product) return response.status(404).json({ error: `Marketplace product not found: ${marketplace}.`, code: "marketplace_product_not_found", marketplace });

    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    if (!product.aiImages.some((item) => item.id === sourceDraft.id)) {
      const sourceBatch = sourceDraft.batchId
        ? normalizeAiImageDrafts(seedProduct.aiImages || []).filter((item) => item.batchId === sourceDraft.batchId)
        : [sourceDraft];
      product.aiImages.push(...sourceBatch.map((item) => normalizeAiImageDraft(item)).filter(Boolean));
    }
    await normalizeAiImageDraftUrlsForMarketplaceSend(product, sourceDraft.id, request);
    const applied = applyAiImageDraftToProduct(product, sourceDraft.id, { sentMarketplace: marketplace });
    const extraCardUrls = await marketplaceExtraCardUrls(marketplace, request);
    if (extraCardUrls.length) {
      if (marketplace === "yandex") {
        const yandex = applied.product.yandex || {};
        applied.product.yandex = normalizeYandexDraft({
          ...yandex,
          pictures: appendUniqueImages(splitList(yandex.pictures), extraCardUrls),
        });
      } else {
        const ozon = applied.product.ozon || {};
        applied.product.ozon = normalizeOzonDraft({
          ...ozon,
          images: appendUniqueImages(splitList(ozon.images), extraCardUrls),
        });
      }
    }
    const sendResult = marketplace === "yandex"
      ? await sendApprovedYandexProductContent(applied.product, { mode: "image", pictures: appendUniqueImages(applied.batchUrls, extraCardUrls) })
      : await sendApprovedOzonProductContent(applied.product, { mode: "image" });
    const sendError = marketplaceSendResultError(sendResult, `${marketplace}_image_send_failed`);
    if (sendError) return next(sendError);

    Object.assign(product, applied.product);
    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    const sentIds = new Set([sourceDraft.id, ...applied.batchDrafts.map((item) => item.id)]);
    product.aiImages.forEach((item) => {
      if (!sentIds.has(item.id)) return;
      item.status = "approved";
      item.reviewedAt = item.reviewedAt || new Date().toISOString();
      item.sentAt = new Date().toISOString();
      item.sentMarketplace = marketplace;
      item.sendResult = sendResult;
    });
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_send", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const savedDraft = (savedProduct.aiImages || []).find((item) => item.id === sourceDraft.id) || sourceDraft;
    response.json({ ok: true, marketplace, mode: "image", target: savedProduct.target || marketplace, offerId: savedProduct.offerId, sentFields: sendResult.fields || ["images"], result: sendResult, product: savedProduct, draft: savedDraft });
    appendAudit(request, "warehouse.ai_image.send", {
      productId: product.id,
      offerId: product.offerId,
      draftId: sourceDraft.id,
      marketplace,
      sendResult,
      newValue: cloneAuditValue({ id: savedProduct.id, yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {}, aiImages: savedProduct.aiImages || [] }),
    }).catch((auditError) => logger.warn("ai image send audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-images/:draftId/reject", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], updatedAt: product.updatedAt });

    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    const draft = product.aiImages.find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI-черновик изображения не найден." });

    draft.status = "rejected";
    draft.reviewedAt = new Date().toISOString();
    if (draft.batchId) {
      product.aiImages
        .filter((item) => item.batchId === draft.batchId && item.status === "pending")
        .forEach((item) => {
          item.status = "rejected";
          item.reviewedAt = draft.reviewedAt;
        });
    }
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_reject", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, draft, product: savedProduct });
    appendAudit(request, "warehouse.ai_image.reject", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      oldValue: before,
      newValue: { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt },
    }).catch((auditError) => logger.warn("ai image reject audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

