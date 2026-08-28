function isRetryableAiImageGenerationError(error = {}) {
  if (isOpenAiBillingLimitError(error)) return false;
  const status = Number(error.statusCode || error.status || 0);
  const code = cleanText(error.code || error.error?.code || error.cause?.code || error.cause?.cause?.code).toLowerCase();
  const detail = cleanText(
    error.message
      || error.error?.message
      || error.detail
      || error.cause?.message
      || error.cause?.cause?.message
      || String(error),
  ).toLowerCase();
  if ([408, 409, 425, 429].includes(status)) return true;
  if (status >= 500 && status < 600) return true;
  return Boolean(
    code.includes("timeout")
      || code.includes("rate_limit")
      || code.includes("temporar")
      || code.includes("overload")
      || code.includes("request_failed")
      || ["eai_again", "enotfound", "econnreset", "econnrefused", "etimedout", "und_err_connect_timeout", "und_err_headers_timeout", "und_err_body_timeout"].includes(code)
      || detail.includes("fetch failed")
      || detail.includes("connection error")
      || detail.includes("network")
      || detail.includes("dns")
      || detail.includes("eai_again")
      || detail.includes("temporar")
  );
}

async function generateOzonAiImageDraftWithRetry(product, options, request, loggerContext = {}) {
  let lastError = null;
  for (let attempt = 1; attempt <= aiImageGenerationAttempts; attempt += 1) {
    try {
      return await generateOzonAiImageDraft(product, options, request);
    } catch (error) {
      lastError = error;
      const retryable = isRetryableAiImageGenerationError(error);
      logger.warn("ai image draft generation attempt failed", {
        ...loggerContext,
        attempt,
        attempts: aiImageGenerationAttempts,
        retryable,
        detail: error?.message || String(error),
        code: error?.code,
        statusCode: error?.statusCode || error?.status,
      });
      if (!retryable || attempt >= aiImageGenerationAttempts) break;
      await sleep(aiImageGenerationRetryDelayMs * attempt);
    }
  }
  throw lastError;
}

const activeAiImageJobs = new Map();

function aiImageJobErrorPayload(error = {}) {
  return compactObject({
    detail: cleanText(
      error?.message
        || error?.error?.message
        || error?.detail
        || error?.cause?.message
        || error?.cause?.cause?.message
        || String(error),
    ),
    code: cleanText(error?.code || error?.error?.code || error?.cause?.code || error?.cause?.cause?.code),
    status: Number(error?.statusCode || error?.status || 0) || undefined,
    model: cleanText(error?.model || "gpt-image-2"),
    endpoint: cleanText(error?.endpoint || "https://codex.sale/v1/images/edits"),
  });
}

function normalizeAiImageJob(input = {}) {
  const status = cleanText(input.status || "queued").toLowerCase();
  const allowedStatus = new Set(["queued", "running", "completed", "failed", "partial"]);
  const variantTotal = Math.max(1, Math.min(5, Number(input.variantTotal || input.variant_total || 5) || 5));
  const draftIds = Array.isArray(input.draftIds || input.draft_ids)
    ? (input.draftIds || input.draft_ids).map(cleanText).filter(Boolean)
    : [];
  const job = compactObject({
    id: cleanText(input.id || input.jobId || input.job_id) || crypto.randomUUID(),
    productId: cleanText(input.productId || input.product_id),
    offerId: cleanText(input.offerId || input.offer_id),
    target: cleanText(input.target),
    batchId: cleanText(input.batchId || input.batch_id) || crypto.randomUUID(),
    status: allowedStatus.has(status) ? status : "queued",
    progress: Math.max(0, Math.min(100, Number(input.progress || 0) || 0)),
    variantIndex: Math.max(0, Math.min(variantTotal, Number(input.variantIndex || input.variant_index || 0) || 0)),
    variantTotal,
    draftIds,
    lastError: input.lastError || input.last_error || null,
    model: cleanText(input.model || "gpt-image-2"),
    endpoint: cleanText(input.endpoint || "https://codex.sale/v1/images/edits"),
    presetId: cleanText(input.presetId || input.preset_id),
    presetLabel: cleanText(input.presetLabel || input.preset_label),
    sourceImageUrl: cleanText(input.sourceImageUrl || input.source_image_url),
    prompt: cleanText(input.prompt),
    createdBy: cleanText(input.createdBy || input.created_by || "system"),
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    startedAt: input.startedAt || input.started_at || null,
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
    finishedAt: input.finishedAt || input.finished_at || null,
  });
  job.draftIds = draftIds;
  return job;
}

async function readAiImageJobs() {
  try {
    const payload = JSON.parse(await fs.readFile(aiImageJobsPath, "utf8"));
    const jobs = Array.isArray(payload?.jobs) ? payload.jobs : (Array.isArray(payload) ? payload : []);
    return jobs.map(normalizeAiImageJob).filter((job) => job.id && job.productId);
  } catch (error) {
    if (error.code === "ENOENT") return [];
    logger.warn("read ai image jobs failed", { detail: error?.message || String(error) });
    return [];
  }
}

async function commitAtomicJsonFile(targetPath, payload, temporaryPath) {
  const content = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
  await fs.writeFile(temporaryPath, content, "utf8");
  try {
    await fs.rename(temporaryPath, targetPath);
  } catch (error) {
    if (error?.code === "ENOENT") throw error;
    if (["EPERM", "EACCES", "EBUSY", "EEXIST"].includes(error?.code)) {
      await fs.copyFile(temporaryPath, targetPath);
      await fs.unlink(temporaryPath).catch(() => {});
      return;
    }
    throw error;
  }
}

async function writeAiImageJobs(jobs = []) {
  await fs.mkdir(dataDir, { recursive: true });
  const normalized = (Array.isArray(jobs) ? jobs : [])
    .map(normalizeAiImageJob)
    .filter((job) => job.id && job.productId)
    .sort((left, right) => String(right.updatedAt || "").localeCompare(String(left.updatedAt || "")))
    .slice(0, 250);
  const payload = { updatedAt: new Date().toISOString(), jobs: normalized };
  const maxAttempts = 6;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const temporaryPath = `${aiImageJobsPath}.${process.pid}.${Date.now()}.${attempt}.tmp`;
    try {
      await commitAtomicJsonFile(aiImageJobsPath, payload, temporaryPath);
      return normalized;
    } catch (error) {
      await fs.unlink(temporaryPath).catch(() => {});
      if (attempt >= maxAttempts - 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, 40 * (attempt + 1)));
    }
  }
  return normalized;
}

async function upsertAiImageJob(jobInput = {}) {
  const job = normalizeAiImageJob({ ...jobInput, updatedAt: new Date().toISOString() });
  const jobs = await readAiImageJobs();
  const index = jobs.findIndex((item) => item.id === job.id);
  if (index >= 0) jobs[index] = job;
  else jobs.unshift(job);
  await writeAiImageJobs(jobs);
  return job;
}

async function findActiveAiImageJob(productId) {
  const id = cleanText(productId);
  if (!id) return null;
  const jobs = await readAiImageJobs();
  return jobs.find((job) => job.productId === id && ["queued", "running"].includes(job.status) && activeAiImageJobs.has(job.id)) || null;
}

function publicAiImageJob(jobInput = {}) {
  const job = normalizeAiImageJob(jobInput);
  return compactObject({
    id: job.id,
    jobId: job.id,
    productId: job.productId,
    offerId: job.offerId,
    target: job.target,
    batchId: job.batchId,
    status: job.status,
    progress: job.progress,
    variantIndex: job.variantIndex,
    variantTotal: job.variantTotal,
    draftIds: job.draftIds || [],
    lastError: job.lastError || null,
    model: job.model,
    endpoint: job.endpoint,
    presetId: job.presetId,
    presetLabel: job.presetLabel,
    sourceImageUrl: job.sourceImageUrl,
    startedAt: job.startedAt || null,
    updatedAt: job.updatedAt || null,
    finishedAt: job.finishedAt || null,
  });
}

function aiImageGenerationRequestFromBody(product, body = {}) {
  return {
    sourceImageUrl: cleanText(body.sourceImageUrl) || firstImageUrl(product.ozon?.primaryImage || product.ozon?.images || product.imageUrl),
    count: Math.min(5, Math.max(1, Math.floor(Number(body.count || body.imagesCount || 5) || 5))),
    studioPresets: normalizeAiImageStudioPresetList(body.photoPresets || body.presets),
    prompt: cleanText(body.prompt),
  };
}

async function appendAiImageDraftToProduct(productId, draft) {
  const warehouse = await readWarehouse();
  const product = warehouse.products.find((item) => item.id === productId);
  if (!product) {
    const error = new Error("Warehouse product not found.");
    error.statusCode = 404;
    error.code = "warehouse_product_not_found";
    throw error;
  }
  product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), draft]);
  product.updatedAt = new Date().toISOString();
  const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_generate", writeLinks: false });
  return saved?.products?.find((item) => item.id === productId) || normalizeWarehouseProduct(product);
}

async function appendAiImageDraftsToProduct(productId, drafts = [], reason = "warehouse_premium_image_generate") {
  const normalizedDrafts = (Array.isArray(drafts) ? drafts : [drafts]).map(normalizeAiImageDraft).filter(Boolean);
  if (!normalizedDrafts.length) return null;
  const warehouse = await readWarehouse();
  const product = warehouse.products.find((item) => item.id === productId);
  if (!product) {
    const error = new Error("Warehouse product not found.");
    error.statusCode = 404;
    error.code = "warehouse_product_not_found";
    throw error;
  }
  product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), ...normalizedDrafts]);
  product.updatedAt = new Date().toISOString();
  const saved = await writeWarehouseProductPatch([product], { reason, writeLinks: false });
  return saved?.products?.find((item) => item.id === productId) || normalizeWarehouseProduct(product);
}

