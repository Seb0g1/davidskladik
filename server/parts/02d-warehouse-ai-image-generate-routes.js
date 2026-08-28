app.post("/api/warehouse/products/:id/premium-images/generate", async (request, response, next) => {
  try {
    const product = await findWarehouseProductById(request.params.id);
    if (!product) return response.status(404).json({ error: "Warehouse product not found.", code: "warehouse_product_not_found" });
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], updatedAt: product.updatedAt });
    const generation = await generatePremiumPerfumeImageDrafts(product, {
      count: request.body?.count,
      marketplace: request.body?.marketplace,
      useLogo: request.body?.useLogo !== false,
    }, request);
    const savedProduct = await appendAiImageDraftsToProduct(product.id, generation.drafts, "warehouse_premium_image_generate");
    appendAudit(request, "warehouse.premium_image.generate", {
      productId: product.id,
      offerId: product.offerId,
      draftIds: generation.drafts.map((draft) => draft.id),
      batchId: generation.batchId,
      count: generation.drafts.length,
      warnings: generation.warnings,
      oldValue: before,
      newValue: savedProduct ? { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt } : null,
    }).catch((auditError) => logger.warn("premium image generate audit failed", { detail: auditError?.message || String(auditError) }));
    response.json({
      ok: true,
      product: savedProduct || product,
      drafts: generation.drafts,
      warnings: generation.warnings,
      batchId: generation.batchId,
    });
  } catch (error) {
    logger.warn("premium image generation failed", {
      productId: request.params.id,
      detail: error?.message || String(error),
      code: error?.code,
      statusCode: error?.statusCode || error?.status,
    });
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-images/generate", async (request, response, next) => {
  try {
    const product = await findWarehouseProductById(request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден.", code: "warehouse_product_not_found" });
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], updatedAt: product.updatedAt });
    const generation = aiImageGenerationRequestFromBody(product, request.body);
    if (!generation.sourceImageUrl) {
      return response.status(400).json({
        error: "Р”Р»СЏ РіРµРЅРµСЂР°С†РёРё С‡РµСЂРµР· Codex РЅСѓР¶РЅРѕ РёСЃС…РѕРґРЅРѕРµ С„РѕС‚Рѕ С‚РѕРІР°СЂР°.",
        code: "source_image_required",
      });
    }
    assertImageGenerationConfigured(forceCodexSaleAiImageSettings(await readEffectiveAiSettings()));

    const syncMode = request.body?.sync === true || cleanText(request.body?.mode).toLowerCase() === "sync";
    if (syncMode) {
      const batchId = crypto.randomUUID();
      const drafts = await generateAiImageDraftsSynchronously(product, generation, request, batchId);
      const draft = drafts[drafts.length - 1];
      let savedProduct = null;
      for (const item of drafts) savedProduct = await appendAiImageDraftToProduct(product.id, item);
      response.json({ ok: true, draft, drafts, batchId, product: savedProduct || product });
      appendAudit(request, "warehouse.ai_image.generate", {
        productId: product.id,
        offerId: product.offerId,
        draftId: draft.id,
        batchId,
        count: generation.count,
        oldValue: before,
        newValue: savedProduct ? { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt } : null,
      }).catch((auditError) => logger.warn("ai image generate audit failed", { detail: auditError?.message || String(auditError) }));
      return;
    }

    const activeJob = await findActiveAiImageJob(product.id);
    if (activeJob) {
      return response.status(202).json({
        ok: true,
        jobId: activeJob.id,
        status: activeJob.status,
        productId: product.id,
        batchId: activeJob.batchId,
        job: publicAiImageJob(activeJob),
      });
    }

    const job = await upsertAiImageJob({
      productId: product.id,
      offerId: product.offerId,
      target: product.target,
      batchId: crypto.randomUUID(),
      status: "queued",
      progress: 0,
      variantTotal: generation.count,
      model: "gpt-image-2",
      endpoint: "https://codex.sale/v1/images/edits",
      sourceImageUrl: generation.sourceImageUrl,
      prompt: generation.prompt,
      createdBy: requestUsername(request),
    });
    activeAiImageJobs.set(job.id, true);
    response.status(202).json({ ok: true, jobId: job.id, status: "queued", productId: product.id, batchId: job.batchId, job: publicAiImageJob(job) });
    setImmediate(() => {
      runAiImageGenerationJob(job, generation, {
        session: { username: requestUsername(request), role: request.session?.role || "admin" },
        headers: { host: request.headers.host, "x-forwarded-proto": request.headers["x-forwarded-proto"] },
        protocol: request.protocol,
        get: (name) => request.get(name),
        username: requestUsername(request),
        role: request.session?.role || "admin",
        before,
      }).catch((error) => logger.warn("ai image background job launcher failed", { jobId: job.id, detail: error?.message || String(error) }));
    });
  } catch (error) {
    logger.warn("ai image drafts generation failed", {
      productId: request.params.id,
      detail: error?.message || String(error),
      code: error?.code,
      statusCode: error?.statusCode || error?.status,
    });
    next(error);
  }
});

app.get("/api/warehouse/products/:id/ai-images/jobs/:jobId", async (request, response, next) => {
  try {
    const jobs = await readAiImageJobs();
    const job = jobs.find((item) => item.id === request.params.jobId && item.productId === request.params.id);
    if (!job) return response.status(404).json({ error: "AI image job not found.", code: "ai_image_job_not_found" });
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id) || null;
    response.json({ ok: true, job: publicAiImageJob(job), product: product ? normalizeWarehouseProduct(product) : null });
  } catch (error) {
    next(error);
  }
});
