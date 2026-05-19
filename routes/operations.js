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

app.get("/api/operations", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 50, 300);
    const jobs = await readOperationJobs(limit);
    response.json({ ok: true, jobs: jobs.map(operationJobPublic), total: jobs.length });
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
    if (!["yandex-import-send", "yandex-stock-sync", "linked-supplier-recovery", "restore-archived-stock", "yandex-card-quality-ai-drafts", "health-deep"].includes(type)) {
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
