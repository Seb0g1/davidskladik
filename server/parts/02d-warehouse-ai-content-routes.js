app.post("/api/warehouse/products/:id/ai-content/:draftId/approve", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Product not found." });
    const draft = (product.aiContentDrafts || []).find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI content draft not found." });
    if (draft.status !== "pending") return response.status(400).json({ error: "AI content draft is already reviewed." });
    const marketplace = cleanText(draft.marketplace || product.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const enhanced = applyAiContentDraftToProduct(product, draft, marketplace);
    Object.assign(product, enhanced);
    draft.status = "approved";
    draft.reviewedAt = new Date().toISOString();
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_approve", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const yandexSend = marketplace === "yandex"
      ? await sendApprovedYandexProductContent(savedProduct, { mode: "content" })
      : { ok: true, skipped: true, reason: "not_yandex" };
    response.json({ ok: true, draft, product: savedProduct, yandexSend });
    appendAudit(request, "warehouse.ai_content.approve", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      marketplace,
      yandexSend,
      newValue: cloneAuditValue({ id: savedProduct.id, yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {} }),
    }).catch((auditError) => logger.warn("ai content approve audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-content/:draftId/send", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const seedProduct = warehouse.products.find((item) => item.id === request.params.id);
    if (!seedProduct) return response.status(404).json({ error: "Product not found." });
    const sourceDraft = (seedProduct.aiContentDrafts || []).find((item) => item.id === request.params.draftId);
    if (!sourceDraft) return response.status(404).json({ error: "AI content draft not found." });
    if (sourceDraft.status === "rejected") return response.status(400).json({ error: "AI content draft is rejected." });
    const marketplace = cleanText(request.body?.marketplace || sourceDraft.marketplace || seedProduct.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const product = findWarehouseProductForMarketplace(warehouse, seedProduct, marketplace);
    if (!product) return response.status(404).json({ error: `Marketplace product not found: ${marketplace}.`, code: "marketplace_product_not_found", marketplace });

    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    if (!product.aiContentDrafts.some((item) => item.id === sourceDraft.id)) {
      product.aiContentDrafts.push(normalizeAiContentDraft({ ...sourceDraft, marketplace }));
    }
    const draft = product.aiContentDrafts.find((item) => item.id === sourceDraft.id) || sourceDraft;
    const enhanced = applyAiContentDraftToProduct(product, draft, marketplace);
    const sendResult = marketplace === "yandex"
      ? await sendApprovedYandexProductContent(enhanced, { mode: "content" })
      : await sendApprovedOzonProductContent(enhanced, { mode: "content" });
    const sendError = marketplaceSendResultError(sendResult, `${marketplace}_content_send_failed`);
    if (sendError) return next(sendError);

    Object.assign(product, enhanced);
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    const savedDraft = product.aiContentDrafts.find((item) => item.id === sourceDraft.id);
    if (savedDraft) {
      savedDraft.status = "approved";
      savedDraft.reviewedAt = savedDraft.reviewedAt || new Date().toISOString();
      savedDraft.sentAt = new Date().toISOString();
      savedDraft.sentMarketplace = marketplace;
      savedDraft.sendResult = sendResult;
    }
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_send", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, marketplace, mode: "content", target: savedProduct.target || marketplace, offerId: savedProduct.offerId, sentFields: sendResult.fields || ["name", "description"], result: sendResult, product: savedProduct, draft: savedDraft });
    appendAudit(request, "warehouse.ai_content.send", {
      productId: product.id,
      offerId: product.offerId,
      draftId: sourceDraft.id,
      marketplace,
      sendResult,
      newValue: cloneAuditValue({ id: savedProduct.id, yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {}, aiContentDrafts: savedProduct.aiContentDrafts || [] }),
    }).catch((auditError) => logger.warn("ai content send audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-content/:draftId/reject", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Product not found." });
    const draft = (product.aiContentDrafts || []).find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI content draft not found." });
    draft.status = "rejected";
    draft.reviewedAt = new Date().toISOString();
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_reject", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, draft, product: savedProduct });
  } catch (error) {
    next(error);
  }
});

