app.post("/api/yandex-cleanup/preview", async (request, response, next) => {
  try {
    const protectedBrands = parseProtectedBrandList(request.body?.protectedBrands || request.body?.brands || "");
    const requestedLimit = Number(request.body?.limit || 50000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 50000));
    const preview = await buildYandexCleanupPreview({ protectedBrands, limit });
    const toDelete = (preview.rows || []).filter((row) => row.action === "delete").length;
    response.json({
      ...preview,
      summary: {
        ...(preview.summary || {}),
        deleteLimit: yandexCleanupDeleteLimit,
        deletePlannedNow: Math.min(toDelete, yandexCleanupDeleteLimit),
        deleteSkippedByLimit: Math.max(0, toDelete - yandexCleanupDeleteLimit),
      },
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon-yandex-import/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 20, 100);
    const audit = await readAuditFiltered({ action: "yandex.import.send" }, limit);
    response.json({ ok: true, history: audit.map(publicYandexImportAuditEntry), total: audit.length });
  } catch (error) {
    next(error);
  }
});

app.get("/api/yandex-cleanup/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 20, 100);
    const audit = await readAuditFiltered({ action: "yandex.cleanup.delete" }, limit);
    response.json({ ok: true, history: audit.map(publicYandexCleanupAuditEntry), total: audit.length });
  } catch (error) {
    next(error);
  }
});
