app.post("/api/yandex-cleanup/archive", async (request, response, next) => {
  try {
    response.status(410).json({ error: "Архивация отключена. Используйте удаление: /api/yandex-cleanup/delete." });
  } catch (error) {
    next(error);
  }
});

app.post("/api/yandex-cleanup/delete", async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun === true;
    if (!dryRun && (request.body?.confirmed !== true || cleanText(request.body?.confirmationText) !== "УДАЛИТЬ ЯНДЕКС")) {
      return response.status(400).json({ error: "Для удаления товаров Яндекса нужно подтверждение: УДАЛИТЬ ЯНДЕКС." });
    }
    const protectedBrands = parseProtectedBrandList(request.body?.protectedBrands || request.body?.brands || "");
    if (!protectedBrands.length) {
      return response.status(400).json({ error: "Укажите хотя бы один бренд, который нельзя удалять." });
    }
    const requestedLimit = Number(request.body?.limit || 50000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 50000));
    const preview = await buildYandexCleanupPreview({ protectedBrands, limit });
    const toDelete = (preview.rows || []).filter((row) => row.action === "delete");
    const limitedToDelete = toDelete.slice(0, yandexCleanupDeleteLimit);
    const deleteSummary = {
      ...(preview.summary || {}),
      toDelete: toDelete.length,
      toArchive: toDelete.length,
      deleteLimit: yandexCleanupDeleteLimit,
      deletePlannedNow: limitedToDelete.length,
      deleteSkippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
    };
    if (dryRun) {
      return response.json({
        ok: true,
        dryRun: true,
        generatedAt: new Date().toISOString(),
        protectedBrands,
        summary: deleteSummary,
        planned: toDelete.length,
        plannedNow: limitedToDelete.length,
        skippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
        deleted: 0,
        failed: 0,
        notDeleted: 0,
        warnings: preview.warnings || [],
        rows: preview.rows || [],
      });
    }
    const results = await deleteYandexCleanupRows(limitedToDelete);
    const deleted = results.filter((item) => item.ok).length;
    const failedRows = results.filter((item) => !item.ok);
    await appendAudit(request, "yandex.cleanup.delete", {
      entityType: "yandex_cleanup",
      entityId: "business_catalog",
      protectedBrands,
      limit,
      summary: deleteSummary,
      planned: toDelete.length,
      plannedNow: limitedToDelete.length,
      skippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
      deleted,
      failed: failedRows.length,
      notDeleted: failedRows.filter((item) => item.error === "not_deleted_by_yandex").length,
      failedOfferIds: failedRows.map((item) => item.offerId).filter(Boolean).slice(0, 500),
      newValue: {
        planned: toDelete.length,
        plannedNow: limitedToDelete.length,
        deleted,
        failed: failedRows.length,
        protectedBrands,
      },
    });
    response.json({
      ok: failedRows.length === 0,
      generatedAt: new Date().toISOString(),
      protectedBrands,
      summary: deleteSummary,
      planned: toDelete.length,
      plannedNow: limitedToDelete.length,
      skippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
      deleted,
      failed: failedRows.length,
      notDeleted: failedRows.filter((item) => item.error === "not_deleted_by_yandex").length,
      warnings: preview.warnings || [],
      results,
    });
  } catch (error) {
    next(error);
  }
});


