// Targeted delete: removes only products matching keyword (Тестер/Отливант) or volume <20ml.
// Safer than the general /delete endpoint — no protectedBrands list needed.
app.post("/api/yandex-cleanup/delete-filtered", async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun !== false;
    if (!dryRun && request.body?.confirmed !== true) {
      return response.status(400).json({ error: "Передайте confirmed:true для удаления." });
    }
    const preview = await buildYandexCleanupPreview({ protectedBrands: [], limit: 50000 });
    const toDelete = (preview.rows || []).filter((row) => row.hasBlockedKeyword || row.smallVolume);
    const limitedToDelete = toDelete.slice(0, yandexCleanupDeleteLimit);
    const summary = {
      total: (preview.rows || []).length,
      toDelete: toDelete.length,
      plannedNow: limitedToDelete.length,
      skippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
    };
    if (dryRun) {
      return response.json({
        ok: true,
        dryRun: true,
        generatedAt: new Date().toISOString(),
        summary,
        rows: toDelete,
        warnings: preview.warnings || [],
      });
    }
    const results = await deleteYandexCleanupRows(limitedToDelete);
    const deleted = results.filter((item) => item.ok).length;
    const failedRows = results.filter((item) => !item.ok);
    await appendAudit(request, "yandex.cleanup.delete_filtered", {
      entityType: "yandex_cleanup",
      entityId: "business_catalog",
      summary,
      deleted,
      failed: failedRows.length,
    });
    response.json({
      ok: failedRows.length === 0,
      generatedAt: new Date().toISOString(),
      summary,
      deleted,
      failed: failedRows.length,
      warnings: preview.warnings || [],
      results,
    });
  } catch (error) {
    next(error);
  }
});

// Fast variant of delete-filtered: builds the candidate list from the local DB instead of
// paging the whole Yandex catalog through the partner API (which takes 10+ minutes and can
// stall the API process). Same filter: keyword Тестер/Отливант or volume < 20ml.
app.post("/api/yandex-cleanup/delete-filtered-local", async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun !== false;
    if (!dryRun && request.body?.confirmed !== true) {
      return response.status(400).json({ error: "Передайте confirmed:true для удаления." });
    }
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "Postgres недоступен." });
    // Lightweight select: no raw JSON, no links — pulling 50k full rows hangs the API.
    const rows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      select: { id: true, offerId: true, target: true, name: true },
      take: 50000,
    });
    // Yandex rows created from exports often store only the offerId as name; the real
    // name (with volume) lives on the Ozon sibling — use it as assessment fallback.
    const ozonNameRows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "ozon" },
      select: { offerId: true, name: true },
      take: 50000,
    });
    const ozonNameByOffer = new Map(ozonNameRows
      .map((row) => [cleanText(row.offerId).toLowerCase(), cleanText(row.name)])
      .filter(([key, value]) => key && value));
    const toDelete = [];
    for (const row of rows) {
      const offerId = cleanText(row.offerId);
      const shopId = cleanText(row.target);
      if (!offerId || !shopId) continue;
      let name = cleanText(row.name || offerId);
      if (!name || name === offerId) {
        name = ozonNameByOffer.get(offerId.toLowerCase()) || name;
      }
      const lowerName = name.toLowerCase();
      const hasBlockedKeyword = lowerName.includes("отливант")
        || lowerName.includes("тестер")
        || isTrashNameProduct(lowerName)
        || isYandexNoBoxProduct(name);
      const volumeAssessment = assessYandexSmallVolume(name);
      // Delete only when the LARGEST mentioned volume is under 20ml — "50 мл + 10 мл"
      // sets must not be deleted. Named sets ("Парфюмерный набор" …) are always kept.
      const volumes = Array.isArray(volumeAssessment.volumesMl) ? volumeAssessment.volumesMl : [];
      const smallVolume = !isYandexSetProduct(name) && volumes.length > 0 && Math.max(...volumes) < YANDEX_MIN_VOLUME_ML;
      if (!hasBlockedKeyword && !smallVolume) continue;
      toDelete.push({
        action: "delete",
        id: row.id,
        offerId,
        shopId,
        name,
        hasBlockedKeyword,
        smallVolume,
        minVolumeMl: volumeAssessment.minVolumeMl,
        maxVolumeMl: volumes.length ? Math.max(...volumes) : null,
      });
    }
    const limitedToDelete = toDelete.slice(0, yandexCleanupDeleteLimit);
    const summary = {
      total: rows.length,
      toDelete: toDelete.length,
      byKeyword: toDelete.filter((r) => r.hasBlockedKeyword).length,
      bySmallVolume: toDelete.filter((r) => r.smallVolume).length,
      plannedNow: limitedToDelete.length,
      skippedByLimit: Math.max(0, toDelete.length - yandexCleanupDeleteLimit),
    };
    if (dryRun) {
      return response.json({
        ok: true,
        dryRun: true,
        generatedAt: new Date().toISOString(),
        summary,
        rows: toDelete.slice(0, 500),
      });
    }
    const results = await deleteYandexCleanupRows(limitedToDelete);
    const deleted = results.filter((item) => item.ok).length;
    const failedRows = results.filter((item) => !item.ok);
    // Remove successfully deleted offers from the local DB too, so price/stock automation
    // stops targeting products that no longer exist on Yandex.
    const okOfferIds = new Set(results.filter((item) => item.ok).map((item) => cleanText(item.offerId)).filter(Boolean));
    const okProductIds = limitedToDelete.filter((r) => okOfferIds.has(r.offerId)).map((r) => r.id).filter(Boolean);
    let localDeleted = 0;
    let localArchived = 0;
    if (okProductIds.length) {
      try {
        await prisma.productLink.deleteMany({ where: { productId: { in: okProductIds } } }).catch(() => {});
        const res = await prisma.warehouseProduct.deleteMany({ where: { id: { in: okProductIds } } });
        localDeleted = res.count || 0;
      } catch (error) {
        logger.warn("yandex cleanup local delete failed, archiving instead", { detail: error?.message || String(error) });
        const res = await prisma.warehouseProduct.updateMany({
          where: { id: { in: okProductIds } },
          data: { archived: true },
        }).catch(() => ({ count: 0 }));
        localArchived = res.count || 0;
      }
    }
    await appendAudit(request, "yandex.cleanup.delete_filtered_local", {
      entityType: "yandex_cleanup",
      entityId: "business_catalog",
      summary,
      deleted,
      failed: failedRows.length,
      localDeleted,
      localArchived,
    });
    response.json({
      ok: failedRows.length === 0,
      generatedAt: new Date().toISOString(),
      summary,
      deleted,
      failed: failedRows.length,
      localDeleted,
      localArchived,
      failedSample: failedRows.slice(0, 30),
    });
  } catch (error) {
    next(error);
  }
});

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


