function registerOperationsRoutes(app, deps) {
  const {
    requireAdmin,
    cleanLimit,
    cleanText,
    crypto,
    readOperationJobs,
    upsertOperationJob,
    operationJobPublic,
    operationTitle,
    startOperationJob,
    activeOperationJobs,
  } = deps;

// Список отдаёт только скаляры result: полные результаты завершённых задач
// (массивы statuses/results на тысячи строк) раздували ответ до ~4 МБ, а
// строкам списка нужны лишь счётчики и summary. Детали (ошибки, построчные
// результаты) грузятся отдельно из GET /api/operations/:id.
function operationJobListPublic(job) {
  const publicJob = operationJobPublic(job);
  if (publicJob.result && typeof publicJob.result === "object") {
    const compact = {};
    for (const [key, value] of Object.entries(publicJob.result)) {
      if (value == null || typeof value === "number" || typeof value === "boolean") compact[key] = value;
      else if (typeof value === "string") compact[key] = value.slice(0, 300);
    }
    publicJob.result = compact;
  }
  return publicJob;
}

app.get("/api/operations", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 50, 300);
    const jobs = await readOperationJobs(limit);
    response.json({ ok: true, jobs: jobs.map(operationJobListPublic), total: jobs.length });
  } catch (error) {
    next(error);
  }
});

app.get("/api/operations/:id", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const jobs = await readOperationJobs(300);
    const job = jobs.find((item) => item.id === id) || activeOperationJobs.get(id);
    if (!job) return response.status(404).json({ error: "Operation not found." });
    response.json({ ok: true, job: operationJobPublic(job) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/operations", requireAdmin, async (request, response, next) => {
  try {
    const type = cleanText(request.body?.type);
    if (!["yandex-import-send", "yandex-stock-sync", "yandex-price-push", "linked-supplier-recovery", "ozon-linked-unarchive", "restore-archived-stock", "yandex-card-quality-ai-drafts", "repair-pricemaster-group-links", "marketplace-supplier-cart-preview", "marketplace-supplier-cart-commit", "ozon-unarchive-queue-process", "sales-automation-run", "problem-products-repair", "brand-index-rebuild", "health-deep"].includes(type)) {
      return response.status(400).json({ error: "Unsupported operation type." });
    }
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type,
      title: operationTitle(type),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: request.body?.payload && typeof request.body.payload === "object" ? request.body.payload : {},
      progress: 0,
    });
    startOperationJob(job);
    response.status(202).json({ ok: true, job: operationJobPublic(job) });
  } catch (error) {
    next(error);
  }
});
}

module.exports = {
  registerOperationsRoutes,
};
