app.get("/api/system/status", requireAdmin, async (_request, response, next) => {
  try {
    const supplierLedgerDiagnostics = async () => {
      if (!shouldUsePostgresStorage()) return { source: "disabled", pickedRows: 0, debtEntries: 0, missingDebtEntries: 0, recentPayments: [] };
      try {
        const prisma = getPrisma();
        const [pickedRows, debtEntries, recentPayments, picked] = await Promise.all([
          prisma.supplierPickingRow.count({ where: { status: "picked" } }),
          prisma.supplierLedgerEntry.count({ where: { entryType: "purchase_debt", status: "active" } }),
          prisma.supplierLedgerEntry.findMany({ where: { entryType: "payment", status: "active" }, orderBy: { occurredAt: "desc" }, take: 10 }),
          prisma.supplierPickingRow.findMany({ where: { status: "picked" }, select: { pickingKey: true }, take: 5000 }),
        ]);
        const sourceKeys = picked.map((row) => supplierLedgerSourceKeyForPicking({ key: row.pickingKey }));
        const linkedDebts = sourceKeys.length
          ? await prisma.supplierLedgerEntry.count({ where: { sourceKey: { in: sourceKeys }, entryType: "purchase_debt", status: "active" } })
          : 0;
        return {
          source: "postgres",
          pickedRows,
          debtEntries,
          sampledPickedRows: picked.length,
          sampledLinkedDebts: linkedDebts,
          missingDebtEntries: Math.max(0, picked.length - linkedDebts),
          recentPayments: recentPayments.map(supplierLedgerEntryFromPostgres),
        };
      } catch (error) {
        return { source: "postgres", error: error?.message || String(error), pickedRows: 0, debtEntries: 0, missingDebtEntries: 0, recentPayments: [] };
      }
    };
    const [health, dailySync, priceRetry, ozonQueue, operations, salesAutomation, bullmq, supplierLedger] = await Promise.all([
      collectHealthDetails({ deep: true }).catch((error) => ({ ok: false, error: error?.message || String(error) })),
      readDailySyncState().catch((error) => ({ status: "error", error: error?.message || String(error) })),
      readPriceRetryQueue().catch((error) => ({ items: [], error: error?.message || String(error) })),
      readOzonUnarchiveQueue().catch((error) => ({ items: [], error: error?.message || String(error) })),
      readOperationJobs(20).catch((error) => ({ jobs: [], error: error?.message || String(error) })),
      readSalesAutomationSystemSummary(),
      marketplaceQueueCounts(),
      supplierLedgerDiagnostics(),
    ]);
    const jobs = Array.isArray(operations.jobs) ? operations.jobs : Array.isArray(operations) ? operations : [];
    const activeJobs = Array.from(activeOperationJobs.values()).map(operationJobPublic).slice(0, 20);
    const components = health.components || {};
    const postgresTables = components.postgresTables || null;
    const runtime = components.runtime || null;
    const ozonPublicQueue = ozonUnarchiveQueuePublic(ozonQueue, { limit: 20 });
    const ozonFullQueue = ozonUnarchiveQueuePublic(ozonQueue, { limit: 5000 });
    const ozonUnarchiveQueueStatus = {
      total: Number(ozonFullQueue.total || 0),
      due: Number(ozonFullQueue.due || 0),
      missingOzonProductId: (ozonFullQueue.items || [])
        .filter((item) => !ozonNumericProductId(item.ozonProductId || item.productId)).length,
      nextAutoRunAt: ozonUnarchiveQueueAutoNextRunAt,
      lastAutoResult: ozonUnarchiveQueueAutoLastResult || null,
    };
    const slowEndpoints = summarizeRecentSlowRequests(30);
    const lastOzonVerification = ozonPublicQueue.items
      ?.map((item) => item.lastAttemptAt || item.updatedAt || null)
      .filter(Boolean)
      .sort()
      .at(-1) || ozonUnarchiveQueueAutoLastRunAt || null;
    response.json({
      ok: health.ok !== false,
      time: new Date().toISOString(),
      health,
      postgresTables,
      redis: components.redis || bullmq,
      bullmq,
      jobs: {
        marketplace: bullmq.counts || components.redis?.counts || null,
        active: activeJobs,
        latest: jobs.slice(0, 10),
        failed: jobs.filter((job) => ["failed", "error"].includes(cleanText(job.status).toLowerCase())).slice(0, 10),
      },
      slowEndpoints,
      memory: runtime?.memory || null,
      runtime,
      lastPriceRun: salesAutomation.latest || null,
      lastOzonVerification,
      lastAutoarchiveRun: ozonUnarchiveQueueAutoLastResult || null,
      ozonUnarchiveQueue: ozonUnarchiveQueueStatus,
      dailySync,
      salesAutomation,
      supplierLedger,
      queues: {
        priceRetry: { total: Array.isArray(priceRetry.items) ? priceRetry.items.length : 0, updatedAt: priceRetry.updatedAt || null, error: priceRetry.error || "" },
        ozonUnarchive: ozonPublicQueue,
        marketplace: bullmq.counts || components.redis?.counts || null,
      },
      operations: {
        active: activeJobs,
        latest: jobs.slice(0, 10),
        failed: jobs.filter((job) => ["failed", "error"].includes(cleanText(job.status).toLowerCase())).slice(0, 10),
        error: operations.error || "",
      },
      slowRequests: slowEndpoints.recent,
      stateWarnings: recentStateWarnings.slice(-30),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const limit = request.query.limit ? Number(request.query.limit) : Number.POSITIVE_INFINITY;
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    const refreshPrices = request.query.refreshPrices === "true";
    const data = await buildWarehouseViewCached({ sync, limit, usdRate, refreshPrices });
    if (!sync && !refreshPrices) queueChangedWarehousePrices(data.products, "warehouse_view_detected_changed_prices");
    response.json(data);
  } catch (error) {
    logger.warn("warehouse view failed, serving snapshot if available", { detail: error?.message || String(error) });
    if (lastWarehouseViewSnapshot) {
      return response.json({
        ...lastWarehouseViewSnapshot,
        sourceError: error?.message || String(error),
        stale: true,
      });
    }
    next(error);
  }
});

app.get("/api/warehouse/brands", async (request, response, next) => {
  try {
    if (shouldUsePostgresStorage()) {
      const brands = await getWarehouseBrandListFromPostgres(getPrisma());
      return response.json({ brands, source: "postgres", count: brands.length });
    }
    const warehouse = await readWarehouse();
    const unique = new Map();
    for (const product of warehouse.products || []) {
      const b = resolveWarehouseBrand(product);
      if (!b) continue;
      const key = b.toLowerCase();
      if (!unique.has(key)) unique.set(key, b);
    }
    const brands = Array.from(unique.values()).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
    response.json({ brands, count: brands.length });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/brands/refresh", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.body?.limit || request.query?.limit, 300, 2000);
    clearWarehouseViewCache();
    warehouseBrandListCache = null;
    warehousePostgresBrandBackfillDone = false;
    let scanned = 0;
    let weakBrand = 0;
    if (shouldUsePostgresStorage()) {
      const prisma = getPrisma();
      const backfill = await ensureWarehousePostgresBrandsBackfilled(prisma, { force: true });
      const rows = await prisma.warehouseProduct.findMany({
        where: enabledWarehouseTargetWhere(),
        select: { id: true, name: true, brand: true, raw: true, marketplace: true, target: true, offerId: true, productId: true },
        take: limit,
        orderBy: [{ updatedAt: "desc" }],
      });
      scanned = rows.length;
      weakBrand = rows.filter((row) => !resolveWarehouseBrand({ ...(row.raw || {}), name: row.name, brand: row.brand, marketplace: row.marketplace, target: row.target, offerId: row.offerId, productId: row.productId })).length;
      const brands = await getWarehouseBrandListFromPostgres(prisma);
      return response.json({ ok: true, source: "postgres", scanned, weakBrand, brands, brandCount: brands.length, refreshed: Number(backfill.updated || 0), backfill, note: "brand_cache_cleared" });
    }
    const warehouse = await readWarehouse();
    scanned = Math.min(limit, (warehouse.products || []).length);
    weakBrand = (warehouse.products || []).slice(0, limit).filter((product) => !resolveWarehouseBrand(product)).length;
    const unique = new Map();
    for (const product of warehouse.products || []) {
      for (const brand of resolveWarehouseBrandCandidates(product)) {
        const key = brand.toLowerCase();
        if (!unique.has(key)) unique.set(key, brand);
      }
    }
    return response.json({ ok: true, source: "json", scanned, weakBrand, brands: Array.from(unique.values()).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" })), brandCount: unique.size, refreshed: 0, note: "brand_cache_cleared" });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/brands/index-status", requireAdmin, async (_request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.json({ ok: true, source: "json", indexed: 0, products: 0, ready: false });
    }
    const prisma = getPrisma();
    const [indexed, products] = await Promise.all([
      prisma.brandIndexItem.count().catch(() => 0),
      prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }).catch(() => 0),
    ]);
    response.json({ ok: true, source: "postgres", indexed, products, ready: indexed > 0, stale: indexed === 0 && products > 0 });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/yandex/restore-markups", requireAdmin, async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun !== false;
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type: "restore-yandex-markups",
      title: operationTitle("restore-yandex-markups"),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: {
        dryRun,
        minMarkup: Number(request.body?.minMarkup || 1.0) || 1.0,
        maxMarkup: Number(request.body?.maxMarkup || 6.0) || 6.0,
      },
      progress: 0,
    });
    startOperationJob(job);
    response.status(202).json({ ok: true, accepted: true, jobId: job.id, statusUrl: `/api/operations/${job.id}`, job: operationJobPublic(job) });
  } catch (error) { next(error); }
});

app.post("/api/warehouse/brands/rebuild-index", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(400).json({ error: "Brand index requires PostgreSQL storage.", code: "postgres_required" });
    }
    const limit = cleanLimit(request.body?.limit || request.query?.limit, 100000, 200000);
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type: "brand-index-rebuild",
      title: operationTitle("brand-index-rebuild"),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: { limit },
      progress: 0,
    });
    startOperationJob(job);
    response.status(202).json({ ok: true, accepted: true, jobId: job.id, statusUrl: `/api/operations/${job.id}`, job: operationJobPublic(job), source: "postgres" });
  } catch (error) {
    next(error);
  }
});

// ─── Журнал ошибок приложения ─────────────────────────────────────────────────

function sinceMs(since) {
  switch (String(since || "").trim()) {
    case "1h": return 60 * 60 * 1000;
    case "6h": return 6 * 60 * 60 * 1000;
    case "7d": return 7 * 24 * 60 * 60 * 1000;
    default: return 24 * 60 * 60 * 1000; // 24h
  }
}

app.get("/api/system/errors", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const sinceDate = new Date(Date.now() - sinceMs(request.query.since));
    const type = cleanText(request.query.type || "");
    const limit = Math.min(200, Math.max(1, Number(request.query.limit) || 50));
    const errors = await prisma.appError.findMany({
      where: {
        createdAt: { gte: sinceDate },
        ...(type ? { type } : {}),
        resolvedAt: null,
      },
      orderBy: { createdAt: "desc" },
      take: limit,
    });
    const total = await prisma.appError.count({
      where: { createdAt: { gte: sinceDate }, ...(type ? { type } : {}), resolvedAt: null },
    });
    response.json({ ok: true, errors, total });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/system/errors/:id/resolve", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const id = cleanText(request.params.id);
    if (!id) return response.status(400).json({ error: "Missing id." });
    await prisma.appError.update({ where: { id }, data: { resolvedAt: new Date() } });
    response.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

