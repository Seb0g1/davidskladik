app.get("/api/pricemaster/search", async (request, response, next) => {
  try {
    const q = cleanText(request.query.q || request.query.search || "");
    const supplier = cleanText(request.query.supplier || "");
    const limit = cleanLimit(request.query.limit, 30, 100);
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const cacheKey = `pricemaster-search:${q.toLowerCase()}:${supplier.toLowerCase()}:${limit}:${usdRate.toFixed(4)}`;
    const cached = getPriceMasterSearchCache(cacheKey);
    if (cached) return response.json(cached);

    let source = "live";
    let rows = [];

    const snapshotRows = await searchPriceMasterSnapshotOffers({
      search: q,
      partner: supplier,
      limit,
      usdRate,
    });
    if (snapshotRows?.length) {
      source = "postgres_snapshot";
      rows = snapshotRows.map((row) => mapPriceMasterSearchResponseRow(row, usdRate));
    }

    if (!rows.length) {
      try {
        const params = [];
        const conditions = ["r.Ignored = 0"];
        if (q) {
          conditions.push("(r.NativeID LIKE ? OR r.NativeName LIKE ? OR r.BarCode LIKE ? OR p.PartnerName LIKE ?)");
          const like = likeSearch(q);
          params.push(like, like, like, like);
        }
        if (supplier) {
          conditions.push("p.PartnerName LIKE ?");
          params.push(likeSearch(supplier));
        }
        params.push(limit);
        const [liveRows] = await pool.query(
          `
          SELECT
            r.NativeID AS article,
            r.NativeName AS name,
            r.NativePrice AS price,
            r.Active AS active,
            r.RowID AS rowId,
            d.DocDate AS docDate,
            d.PartnerID AS partnerId,
            p.PartnerName AS partnerName
          FROM OfferRows r
          JOIN OfferDocs d ON d.DocID = r.DocID
          LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
          WHERE ${conditions.join(" AND ")}
          ORDER BY d.DocDate DESC, r.RowID DESC
          LIMIT ?
          `,
          params,
        );
        source = "live";
        rows = liveRows.map((row) => mapPriceMasterSearchResponseRow(row, usdRate));
      } catch (error) {
        logger.warn("PriceMaster search live query failed, using json snapshot", { detail: error?.message || String(error) });
        const indexes = await getPriceMasterSnapshotIndexes();
        source = "json_snapshot";
        rows = searchPriceMasterSnapshotJsonRows(indexes.rows || [], { q, supplier, limit, usdRate });
      }
    }

    if (!rows.length && source !== "json_snapshot") {
      const indexes = await getPriceMasterSnapshotIndexes();
      const fallbackRows = searchPriceMasterSnapshotJsonRows(indexes.rows || [], { q, supplier, limit, usdRate });
      if (fallbackRows.length) {
        source = "json_snapshot";
        rows = fallbackRows;
      }
    }

    // Linking UX: cheapest rows first, testers ALWAYS at the bottom (so a tester can't be
    // linked by accident), inactive rows after active within each group.
    const isTesterRow = (row) => /тестер|tester/iu.test(cleanText(row?.name || ""));
    const rubPrice = (row) => {
      const price = Number(row?.price || 0) || 0;
      return cleanText(row?.priceCurrency || row?.currency).toUpperCase() === "USD" ? price * usdRate : price;
    };
    const sortedRows = rows
      .map((row) => ({ ...row, isTester: isTesterRow(row) }))
      .sort((a, b) => {
        if (a.isTester !== b.isTester) return a.isTester ? 1 : -1;
        if (Boolean(a.active) !== Boolean(b.active)) return a.active ? -1 : 1;
        return rubPrice(a) - rubPrice(b);
      });

    const payload = {
      ok: true,
      rows: sortedRows.slice(0, limit),
      total: sortedRows.length,
      source,
    };
    setPriceMasterSearchCache(cacheKey, payload);
    response.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get("/api/live-status", async (_request, response, next) => {
  try {
    const [warehouseMeta, dailySync, priceMaster, queueStatus] = await Promise.all([
      getWarehouseMetaFast(),
      getDailySyncStatus().catch((error) => ({ error: error?.message || String(error) })),
      getPriceMasterSnapshotMetaFast().catch((error) => {
        logger.warn("live status PriceMaster meta failed", { detail: error?.message || String(error) });
        return { syncId: null, updatedAt: null, items: 0, changes: 0, error: error?.message || String(error) };
      }),
      marketplaceQueueCounts().catch((error) => ({
        enabled: bullmqEnabled,
        mode: "bullmq",
        ok: false,
        error: error?.message || String(error),
      })),
    ]);
    response.json({
      ok: true,
      now: new Date().toISOString(),
      warehouse: {
        updatedAt: warehouseMeta.updatedAt || warehouseMeta.createdAt || null,
        createdAt: warehouseMeta.createdAt || null,
        products: Number(warehouseMeta.products || 0),
        suppliers: Number(warehouseMeta.suppliers || 0),
        source: warehouseMeta.source || null,
      },
      priceMaster,
      dailySync: {
        updatedAt: dailySync.updatedAt || dailySync.lastRunAt || null,
        status: dailySync.status || "idle",
        running: Boolean(dailySync.running),
        lastRunAt: dailySync.lastRunAt || null,
        nextRunAt: dailySync.nextRunAt || null,
        error: dailySync.error || null,
      },
      autoSync: {
        running: Boolean(autoSyncRunning),
        nextRunAt: autoSyncNextRunAt || null,
      },
      marketplaceMaintenance: {
        enabled: marketplaceMaintenanceEnabled,
        running: Boolean(marketplaceMaintenanceRunning),
        everyHours: marketplaceMaintenanceHours,
        nextRunAt: marketplaceMaintenanceNextRunAt || null,
      },
      queue: {
        enabled: bullmqEnabled && Boolean(redisUrl),
        degraded: bullmqEnabled && Boolean(redisUrl) && (!marketplaceQueue || queueStatus?.ok === false),
        producerReady: marketplaceJobsCanEnqueue(),
        consumerReady: Boolean(marketplaceWorker),
        counts: queueStatus?.counts || null,
        error: queueStatus?.error || null,
      },
    });
  } catch (error) {
    next(error);
  }
});

