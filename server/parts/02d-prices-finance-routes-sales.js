app.post("/api/warehouse/prices/send", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Prices were not sent because manual confirmation is required." });
    }
    response.json(await sendWarehousePrices({
      productIds: Array.isArray(request.body.productIds) ? request.body.productIds : [],
      usdRate: Number(request.body.usdRate || 0) || undefined,
      minDiffRub: Number(request.body.minDiffRub || 0),
      minDiffPct: Number(request.body.minDiffPct || 0),
      dryRun: request.body.dryRun === true,
      force: request.body.force === true,
      refreshMarketplacePrices: request.body.refreshMarketplacePrices === true,
      marketplace: request.body.marketplace || "all",
      onlyChanged: request.body.onlyChanged === true,
      livePriceMaster: request.body.livePriceMaster === true,
      limit: request.body.limit,
      verify: request.body.verify !== false,
      priceIntentId: cleanText(request.body.priceIntentId || crypto.randomUUID()),
      sourceEvent: "warehouse_prices_send",
    }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/prices/preview", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.query.marketplace || "all").toLowerCase();
    const onlyChanged = String(request.query.onlyChanged ?? "true") !== "false";
    const limit = cleanLimit(request.query.limit, 200, 2000);
    response.json(await sendWarehousePrices({
      dryRun: true,
      marketplace,
      onlyChanged,
      limit,
      refreshMarketplacePrices: request.query.refreshMarketplacePrices === "true",
      livePriceMaster: request.query.livePriceMaster !== "false",
      force: request.query.force === "true",
    }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/sales-automation/summary", requireAdmin, async (_request, response, next) => {
  try {
    const [retryQueue, ozonQueue] = await Promise.all([
      readPriceRetryQueue().catch(() => ({ items: [] })),
      readOzonUnarchiveQueue().catch(() => ({ items: [] })),
    ]);
    if (shouldUsePostgresStorage()) {
      try {
        const prisma = getPrisma();
        const [total, reasons, statuses, latest] = await Promise.all([
          prisma.salesAutomationSkuState.count(),
          prisma.salesAutomationSkuState.groupBy({ by: ["reason"], _count: { _all: true } }),
          prisma.salesAutomationSkuState.groupBy({ by: ["marketplace", "priceStatus"], _count: { _all: true } }),
          prisma.salesAutomationSkuState.findFirst({ orderBy: { updatedAt: "desc" } }),
        ]);
        return response.json({
          ok: true,
          source: "postgres",
          autoEnabled: true,
          total,
          updatedAt: latest?.updatedAt?.toISOString?.() || null,
          retryTotal: retryQueue.items?.length || 0,
          ozonUnarchiveQueued: normalizeOzonUnarchiveQueue(ozonQueue).items.length,
          reasons: Object.fromEntries(reasons.map((row) => [row.reason || "unknown", row._count?._all || 0])),
          statuses: statuses.map((row) => ({ marketplace: row.marketplace, priceStatus: row.priceStatus, count: row._count?._all || 0 })),
        });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        rememberStateWarning("sales_automation_summary_postgres", error);
        logger.warn("sales automation summary postgres failed, using live preview fallback", { detail: error?.message || String(error) });
      }
    }
    const preview = await sendWarehousePrices({ dryRun: true, marketplace: "all", onlyChanged: false, limit: 500, livePriceMaster: true });
    response.json({
      ok: true,
      source: "preview",
      autoEnabled: true,
      total: Number(preview.selected || 0),
      updatedAt: new Date().toISOString(),
      retryTotal: retryQueue.items?.length || 0,
      ozonUnarchiveQueued: normalizeOzonUnarchiveQueue(ozonQueue).items.length,
      reasons: (preview.skipped || []).reduce((acc, row) => {
        const key = salesAutomationReason(row.reason || "unknown");
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {}),
      statuses: [],
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/sales-automation/items", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.query.marketplace || "all").toLowerCase();
    const reason = cleanText(request.query.reason || "");
    const statusFilter = cleanText(request.query.status || request.query.applyStatus || "").toLowerCase();
    const q = cleanText(request.query.q || "");
    const limit = cleanLimit(request.query.limit, 200, 2000);
    if (shouldUsePostgresStorage()) {
      try {
        const where = {
          AND: [
            marketplace !== "all" && ["ozon", "yandex"].includes(marketplace) ? { marketplace } : {},
            reason ? { reason } : {},
            q ? {
              OR: [
                { offerId: { contains: q, mode: "insensitive" } },
                { productId: { contains: q, mode: "insensitive" } },
                { lastError: { contains: q, mode: "insensitive" } },
              ],
            } : {},
          ].filter((item) => Object.keys(item || {}).length),
        };
        const take = statusFilter ? Math.max(limit, 2000) : limit;
        const [databaseTotal, items] = await Promise.all([
          statusFilter ? Promise.resolve(0) : getPrisma().salesAutomationSkuState.count({ where }),
          getPrisma().salesAutomationSkuState.findMany({ where, orderBy: { updatedAt: "desc" }, take }),
        ]);
        const publicItems = items.map((item) => {
          const raw = item.raw && typeof item.raw === "object" && !Array.isArray(item.raw) ? item.raw : {};
          const selectedSupplier = raw.selectedSupplier || raw.supplier || null;
          return {
            id: item.id,
            productId: item.productId,
            marketplace: item.marketplace,
            target: item.target,
            offerId: item.offerId,
            currentPrice: item.currentPrice,
            targetPrice: item.targetPrice,
            targetStock: item.targetStock,
            priceStatus: item.priceStatus,
            stockStatus: item.stockStatus,
            unarchiveStatus: item.unarchiveStatus,
            reason: salesAutomationReason(item.reason || raw.reason),
            lastCalculatedAt: item.lastCalculatedAt?.toISOString?.() || null,
            lastPriceSentAt: item.lastPriceSentAt?.toISOString?.() || null,
            lastStockSentAt: item.lastStockSentAt?.toISOString?.() || null,
            lastError: item.lastError || "",
            updatedAt: item.updatedAt?.toISOString?.() || null,
            priceIntentId: raw.priceIntentId || null,
            lastRequestedPrice: raw.lastRequestedPrice ?? raw.requestedPrice ?? null,
            lastVerifiedPrice: raw.lastVerifiedPrice ?? raw.verifiedPrice ?? null,
            lastPriceRequestAt: raw.lastPriceRequestAt || raw.requestedAt || null,
            lastPriceVerifiedAt: raw.lastPriceVerifiedAt || raw.verifiedAt || null,
            priceApplyStatus: raw.priceApplyStatus || raw.applyStatus || item.priceStatus,
            verificationAttempts: raw.verificationAttempts || 0,
            selectedSupplier,
            supplierPurchasePrice: raw.supplierPurchasePrice ?? selectedSupplier?.price ?? selectedSupplier?.supplierPrice ?? null,
            supplierPurchaseCurrency: raw.supplierPurchaseCurrency || selectedSupplier?.priceCurrency || selectedSupplier?.currency || null,
            effectiveFinalPrice: raw.effectiveFinalPrice ?? selectedSupplier?.effectiveFinalPrice ?? selectedSupplier?.calculatedPrice ?? item.targetPrice ?? null,
            sourceEvent: raw.sourceEvent || raw.reason || "",
            priceSource: raw.priceSource || selectedSupplier?.priceSource || null,
            raw,
          };
        });
        const filteredItems = statusFilter
          ? publicItems.filter((item) => cleanText(item.priceApplyStatus).toLowerCase() === statusFilter)
          : publicItems;
        return response.json({
          ok: true,
          source: "postgres",
          total: statusFilter ? filteredItems.length : databaseTotal,
          items: filteredItems.slice(0, limit),
        });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        rememberStateWarning("sales_automation_items_postgres", error);
        logger.warn("sales automation items postgres failed, using live preview fallback", { detail: error?.message || String(error) });
      }
    }
    const preview = await sendWarehousePrices({ dryRun: true, marketplace, onlyChanged: false, limit, livePriceMaster: true });
    const items = [
      ...(preview.items || []).map((item) => ({ ...item, reason: "ok", priceStatus: "pending" })),
      ...(preview.skipped || []).map((item) => ({ ...item, reason: salesAutomationReason(item.reason), priceStatus: item.reason === "unchanged" ? "success" : "pending" })),
    ].filter((item) => !reason || salesAutomationReason(item.reason) === reason);
    response.json({ ok: true, source: "preview", total: items.length, items: items.slice(0, limit) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/sales-automation/run", requireAdmin, async (request, response, next) => {
  try {
    const marketplaceValue = cleanText(request.body?.marketplace || "all").toLowerCase();
    const marketplace = ["ozon", "yandex"].includes(marketplaceValue) ? marketplaceValue : "all";
    const force = request.body?.force === true;
    const onlyChanged = request.body?.onlyChanged === true;
    const productIds = Array.isArray(request.body?.productIds)
      ? request.body.productIds.map(cleanText).filter(Boolean).slice(0, 5000)
      : undefined;
    const result = await queueAuthoritativePriceReprice({
      productIds,
      marketplace,
      reason: cleanText(request.body?.reason || "sales_automation_manual"),
      sourceEvent: "sales_automation_manual",
      force,
      onlyChanged,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: request.body?.verify !== false,
      limit: productIds?.length ? 0 : cleanLimit(request.body?.limit, 1000, 50000),
      priority: 1,
    });
    response.status(202).json({ ok: true, accepted: true, statusUrl: "/api/sales-automation/items", ...result });
  } catch (error) {
    next(error);
  }
});

function problemProductCategoryFromAutomation(row = {}) {
  const reason = salesAutomationReason(row.reason || row.raw?.reason || "");
  if (reason === "no_pricemaster_link") return "no_supplier";
  if (reason === "no_supplier") return "no_supplier";
  if (reason === "no_price") return "no_price";
  if (reason === "stock_only_manual_price_missing") return "stock_only_manual_price_missing";
  if (reason === "api_error") return "api_error";
  if (reason === "ozon_price_not_applied") return "api_error";
  if (reason === "pm_live_timeout") return "api_error";
  if (reason === "verification_pending" || reason === "api_accepted" || reason === "queued") return "price_retry";
  if (reason === "in_retry") return "price_retry";
  if (reason === "ozon_limit") return "ozon_autoarchive";
  if (row.priceStatus === "failed" || row.stockStatus === "failed") return "api_error";
  return reason && reason !== "ok" && reason !== "unchanged" ? reason : "";
}

app.get("/api/problem-products", requireAdmin, async (request, response, next) => {
  try {
    const category = cleanText(request.query.category || "all");
    const q = cleanText(request.query.q || "");
    const allowPreviewFallback = ["1", "true", "yes"].includes(cleanText(request.query.previewFallback || request.query.liveFallback).toLowerCase());
    const limit = cleanLimit(request.query.limit, 200, 2000);
    let items = [];
    let source = shouldUsePostgresStorage() ? "postgres" : "preview";
    let postgresFailed = false;
    if (shouldUsePostgresStorage()) {
      try {
        const rows = await getPrisma().salesAutomationSkuState.findMany({
          where: {
            AND: [
              q ? {
                OR: [
                  { offerId: { contains: q, mode: "insensitive" } },
                  { productId: { contains: q, mode: "insensitive" } },
                  { lastError: { contains: q, mode: "insensitive" } },
                ],
              } : {},
              category !== "all" ? { reason: category === "price_retry" ? "in_retry" : category } : {},
            ].filter((item) => Object.keys(item || {}).length),
          },
          orderBy: { updatedAt: "desc" },
          take: limit,
        });
        items = rows
          .map((row) => ({
            id: row.id,
            productId: row.productId,
            marketplace: row.marketplace,
            target: row.target,
            offerId: row.offerId,
            category: problemProductCategoryFromAutomation(row),
            reason: row.reason,
            currentPrice: row.currentPrice,
            targetPrice: row.targetPrice,
            targetStock: row.targetStock,
            lastError: row.lastError || "",
            updatedAt: row.updatedAt?.toISOString?.() || null,
          }))
          .filter((row) => row.category);
      } catch (error) {
        postgresFailed = true;
        if (!jsonFallbackEnabled()) throw error;
        rememberStateWarning("problem_products_postgres", error);
        logger.warn("problem products postgres failed, using preview fallback", { detail: error?.message || String(error) });
      }
    }
    if (!shouldUsePostgresStorage() || (postgresFailed && jsonFallbackEnabled()) || allowPreviewFallback) {
      source = postgresFailed ? "preview_postgres_failed" : "preview";
      const preview = await sendWarehousePrices({ dryRun: true, marketplace: "all", onlyChanged: false, limit, livePriceMaster: true });
      items = (preview.skipped || [])
        .map((row) => ({
          ...row,
          productId: row.productId || row.id,
          category: problemProductCategoryFromAutomation(row),
          reason: salesAutomationReason(row.reason),
        }))
        .filter((row) => row.category && (category === "all" || row.category === category));
    }
    const summary = items.reduce((acc, row) => {
      acc[row.category] = (acc[row.category] || 0) + 1;
      return acc;
    }, {});
    response.json({ ok: true, source, total: items.length, summary, items: items.slice(0, limit) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/problem-products/repair", requireAdmin, async (request, response, next) => {
  try {
    const productIds = Array.isArray(request.body?.productIds) ? request.body.productIds.map(cleanText).filter(Boolean) : [];
    if (!productIds.length) return response.status(400).json({ error: "No productIds selected.", code: "problem_products_empty" });
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type: "problem-products-repair",
      title: operationTitle("problem-products-repair"),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: { productIds: productIds.slice(0, 100) },
      progress: 0,
    });
    startOperationJob(job);
    response.status(202).json({ ok: true, accepted: true, jobId: job.id, statusUrl: `/api/operations/${job.id}`, job: operationJobPublic(job) });
  } catch (error) {
    next(error);
  }
});

