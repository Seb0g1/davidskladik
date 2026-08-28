function buildAiAssistantChecklist(product = {}, validation = {}, draft = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const hasSourceImage = Boolean(firstImageUrl(normalized.ozon?.primaryImage || normalized.ozon?.images || normalized.imageUrl));
  return [
    {
      id: "description",
      label: "Проверить новое описание",
      ok: cleanText(draft.description).length >= 300,
      detail: cleanText(draft.description).length ? `${cleanText(draft.description).length} символов` : "описание не создано",
    },
    {
      id: "seo",
      label: "Проверить SEO-фразы",
      ok: Array.isArray(draft.seoKeywords) && draft.seoKeywords.length >= 5,
      detail: `${Array.isArray(draft.seoKeywords) ? draft.seoKeywords.length : 0} фраз`,
    },
    {
      id: "photo",
      label: "Сгенерировать 5 фото по studio presets",
      ok: hasSourceImage,
      detail: hasSourceImage ? "исходное фото есть" : "нет исходного фото",
    },
    {
      id: "marketplace",
      label: "Проверить требования маркетплейса",
      ok: Boolean(validation.ready),
      detail: (validation.missing || validation.reasons || []).join(", ") || "критичных замечаний нет",
    },
    {
      id: "manual-review",
      label: "Одобрить вручную перед отправкой",
      ok: false,
      detail: "AI ничего не публикует без кнопки подтверждения",
    },
  ];
}

app.post("/api/warehouse/products/:id/ai-assistant", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const marketplace = cleanText(request.body.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const beforeValidation = productContentQuality(product, marketplace);
    const draft = await generateAiProductContentDraft(product, { marketplace });
    const previewProduct = applyAiContentDraftToProduct(product, draft, marketplace);
    const afterValidation = productContentQuality(previewProduct, marketplace);
    const aiSettings = await readEffectiveAiSettings();
    const savedDraft = normalizeAiContentDraft({
      ...draft,
      marketplace,
      source: "assistant",
      model: draft.model || aiSettings.textModel || openaiTextModel,
    });
    let savedProduct = normalizeWarehouseProduct(product);
    if (savedDraft) {
      product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
      product.updatedAt = new Date().toISOString();
      const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_assistant_draft", writeLinks: false });
      savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    }
    response.json({
      ok: true,
      productId: product.id,
      offerId: product.offerId,
      marketplace,
      draft: savedDraft || draft,
      product: savedProduct,
      before: beforeValidation,
      after: afterValidation,
      reasons: beforeValidation.reasons || [],
      checklist: buildAiAssistantChecklist(previewProduct, afterValidation, draft),
      photoPresets: publicAiImageStudioPresets(),
      provider: {
        providerId: aiSettings.providerId,
        baseUrl: normalizeOpenAiCompatibleBaseUrl(aiSettings.baseUrl),
        textModel: aiSettings.textModel || openaiTextModel,
        imageModel: effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings),
      },
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-content/generate", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const apply = request.body.apply !== false;
    if (apply) {
      const conflict = productConflict(product, request.body.expectedUpdatedAt);
      if (conflict) return conflictResponse(response, [conflict]);
    }
    const marketplace = cleanText(request.body.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const before = cloneAuditValue({
      id: product.id,
      marketplace: product.marketplace,
      offerId: product.offerId,
      yandex: product.yandex || {},
      updatedAt: product.updatedAt,
    });
    const draft = await generateAiProductContentDraft(product, { marketplace });
    const enhancedProduct = applyAiContentDraftToProduct(product, draft, marketplace);
    const validation = productContentQuality(enhancedProduct, marketplace);
    const built = marketplace === "yandex" ? buildYandexOfferMapping(enhancedProduct) : { ready: true, missing: [] };

    if (!apply) {
      if (request.body.saveDraft === true) {
        const savedDraft = normalizeAiContentDraft({
          ...draft,
          marketplace,
          source: cleanText(request.body.source || "manual"),
          model: (await readEffectiveAiSettings()).textModel || openaiTextModel,
        });
        if (savedDraft) {
          product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
          product.updatedAt = new Date().toISOString();
          const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_draft", writeLinks: false });
          const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
          return response.json({
            ok: true,
            applied: false,
            saved: true,
            draft: savedDraft,
            validation: { ...validation, yandexReady: Boolean(built.ready), missing: built.missing || validation.missing || [] },
            product: savedProduct,
          });
        }
      }
      return response.json({
        ok: true,
        applied: false,
        draft,
        validation: { ...validation, yandexReady: Boolean(built.ready), missing: built.missing || validation.missing || [] },
      });
    }

    Object.assign(product, enhancedProduct, { updatedAt: new Date().toISOString() });
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_generate", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const savedValidation = productContentQuality(savedProduct, marketplace);
    response.json({
      ok: true,
      applied: true,
      draft,
      validation: { ...savedValidation, yandexReady: Boolean(buildYandexOfferMapping(savedProduct).ready) },
      product: savedProduct,
    });
    appendAudit(request, "warehouse.ai_content.generate", {
      productId: product.id,
      offerId: product.offerId,
      marketplace,
      oldValue: before,
      newValue: cloneAuditValue({
        id: savedProduct.id,
        marketplace: savedProduct.marketplace,
        offerId: savedProduct.offerId,
        yandex: savedProduct.yandex || {},
        updatedAt: savedProduct.updatedAt,
      }),
    }).catch((auditError) => logger.warn("ai content generate audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});
