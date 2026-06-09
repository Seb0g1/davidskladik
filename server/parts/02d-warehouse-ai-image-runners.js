async function generatePremiumPerfumeImageDrafts(product, options = {}, request) {
  const count = Math.max(1, Math.min(5, Math.floor(Number(options.count || 5) || 5)));
  const marketplace = cleanText(options.marketplace || "ozon").toLowerCase() === "yandex" ? "yandex" : "ozon";
  const useLogo = options.useLogo !== false;
  const settings = await readAppSettings();
  const logoBuffer = useLogo ? await readBrandingLogoBuffer(settings, marketplace, request) : null;
  const sources = productImageUrlsForPremium(product);
  if (!sources.length) {
    const error = new Error("Product has no source images for premium card generation.");
    error.statusCode = 400;
    error.code = "source_image_required";
    throw error;
  }
  const themes = premiumImageBackgrounds();
  const targetSize = ozonAiImageTargetPx || 1000;
  await fs.mkdir(aiImageDir, { recursive: true });
  const batchId = crypto.randomUUID();
  const drafts = [];
  const warnings = [];
  for (let index = 1; index <= count; index += 1) {
    const sourceUrl = sources[(index - 1) % sources.length];
    const theme = themes[(index - 1) % themes.length];
    let sourceBuffer = null;
    try {
      sourceBuffer = await readProductSourceImageBuffer(sourceUrl, request);
    } catch (error) {
      if (index === 1) throw error;
      warnings.push({ sourceImageUrl: sourceUrl, code: error?.code || "source_image_not_ready", detail: error?.message || String(error) });
      sourceBuffer = await readProductSourceImageBuffer(sources[0], request);
    }
    const outBuffer = await buildPremiumPerfumeImageBuffer(sourceBuffer, { theme, logoBuffer, size: targetSize });
    const fileName = `${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}.png`;
    const filePath = path.join(aiImageDir, fileName);
    await fs.writeFile(filePath, outBuffer);
    const relativeUrl = `/uploads/ai-images/${fileName}`;
    drafts.push(normalizeAiImageDraft({
      status: "pending",
      prompt: `Premium deterministic perfume card: ${theme.label}`,
      productName: product.name || product.ozon?.name,
      sourceImageUrl: sourceUrl,
      resultUrl: `${uploadBaseUrl(request)}${relativeUrl}`,
      batchId,
      variantIndex: index,
      variantTotal: count,
      presetId: `premium-${theme.id}`,
      presetLabel: theme.label,
      layout: "premium-template",
      model: "premium-template",
      size: `${targetSize}x${targetSize}`,
      quality: "deterministic",
      format: "png",
    }));
  }
  return { drafts, warnings, batchId };
}

async function generateAiImageDraftsSynchronously(product, generation, request, batchId = crypto.randomUUID()) {
  const drafts = [];
  for (let index = 1; index <= generation.count; index += 1) {
    const preset = generation.studioPresets[(index - 1) % generation.studioPresets.length];
    const prompt = [generation.prompt, preset?.prompt].filter(Boolean).join("\n\n");
    const draft = await generateOzonAiImageDraftWithRetry(product, {
      prompt,
      sourceImageUrl: generation.sourceImageUrl,
      batchId,
      variantIndex: index,
      variantTotal: generation.count,
      presetId: preset?.id,
      presetLabel: preset?.label,
      requireSourceImage: true,
      allowGenerationFallback: false,
      forceCodexSale: true,
    }, request, {
      productId: product.id,
      offerId: product.offerId,
      variantIndex: index,
      variantTotal: generation.count,
    });
    drafts.push(draft);
    if (index < generation.count && aiImageGenerationSequenceDelayMs > 0) await sleep(aiImageGenerationSequenceDelayMs);
  }
  return drafts;
}

async function runAiImageGenerationJob(jobInput, generation, requestContext = {}) {
  let job = normalizeAiImageJob({ ...jobInput, status: "running", progress: 2, startedAt: new Date().toISOString(), lastError: null });
  activeAiImageJobs.set(job.id, true);
  await upsertAiImageJob(job);
  const auditRequest = {
    session: {
      username: cleanText(requestContext.username || job.createdBy || "system") || "system",
      role: cleanText(requestContext.role || "admin") || "admin",
    },
  };
  let savedProduct = null;
  try {
    const initialWarehouse = await readWarehouse();
    const product = initialWarehouse.products.find((item) => item.id === job.productId);
    if (!product) {
      const error = new Error("Warehouse product not found.");
      error.statusCode = 404;
      error.code = "warehouse_product_not_found";
      throw error;
    }
    logger.info("ai image drafts job started", {
      jobId: job.id,
      productId: product.id,
      offerId: product.offerId,
      target: product.target,
      count: generation.count,
      provider: "codexsale",
      sequential: true,
      attempts: aiImageGenerationAttempts,
    });
    for (let index = 1; index <= generation.count; index += 1) {
      const preset = generation.studioPresets[(index - 1) % generation.studioPresets.length];
      job = await upsertAiImageJob({
        ...job,
        status: "running",
        variantIndex: index,
        variantTotal: generation.count,
        progress: Math.max(5, Math.floor(((index - 1) / generation.count) * 90)),
        presetId: preset?.id,
        presetLabel: preset?.label,
        lastError: null,
      });
      const prompt = [generation.prompt, preset?.prompt].filter(Boolean).join("\n\n");
      const draft = await generateOzonAiImageDraftWithRetry(product, {
        prompt,
        sourceImageUrl: generation.sourceImageUrl,
        batchId: job.batchId,
        variantIndex: index,
        variantTotal: generation.count,
        presetId: preset?.id,
        presetLabel: preset?.label,
        requireSourceImage: true,
        allowGenerationFallback: false,
        forceCodexSale: true,
      }, requestContext, {
        jobId: job.id,
        productId: product.id,
        offerId: product.offerId,
        variantIndex: index,
        variantTotal: generation.count,
      });
      savedProduct = await appendAiImageDraftToProduct(product.id, draft);
      job = await upsertAiImageJob({
        ...job,
        draftIds: Array.from(new Set([...(job.draftIds || []), draft.id])),
        progress: Math.floor((index / generation.count) * 100),
        variantIndex: index,
        variantTotal: generation.count,
        status: "running",
      });
      if (index < generation.count && aiImageGenerationSequenceDelayMs > 0) await sleep(aiImageGenerationSequenceDelayMs);
    }
    job = await upsertAiImageJob({
      ...job,
      status: "completed",
      progress: 100,
      variantIndex: generation.count,
      variantTotal: generation.count,
      finishedAt: new Date().toISOString(),
      lastError: null,
    });
    logger.info("ai image drafts job complete", {
      jobId: job.id,
      productId: job.productId,
      offerId: job.offerId,
      drafts: (job.draftIds || []).length,
      batchId: job.batchId,
    });
    appendAudit(auditRequest, "warehouse.ai_image.generate", {
      productId: job.productId,
      offerId: job.offerId,
      draftIds: job.draftIds,
      batchId: job.batchId,
      count: generation.count,
      jobId: job.id,
      oldValue: requestContext.before || null,
      newValue: savedProduct ? { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt } : null,
    }).catch((auditError) => logger.warn("ai image generate audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    const errorPayload = aiImageJobErrorPayload(error);
    job = await upsertAiImageJob({
      ...job,
      status: (job.draftIds || []).length ? "partial" : "failed",
      progress: (job.draftIds || []).length ? job.progress : 0,
      lastError: errorPayload,
      finishedAt: new Date().toISOString(),
    });
    logger.warn("ai image drafts job failed", {
      jobId: job.id,
      productId: job.productId,
      offerId: job.offerId,
      drafts: (job.draftIds || []).length,
      ...errorPayload,
    });
  } finally {
    activeAiImageJobs.delete(job.id);
  }
  return job;
}


