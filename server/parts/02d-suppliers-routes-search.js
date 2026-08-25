// Слова из имён PriceMaster для автодополнения (по префиксу, затем по вхождению)
app.get("/api/pricemaster/words", async (request, response, next) => {
  try {
    const prefix = cleanText(request.query.prefix || "").toLowerCase();
    if (!prefix || prefix.length < 1) return response.json({ ok: true, words: [] });
    const words = await getPmWordIndex();
    const exact = words.filter((w) => w.startsWith(prefix));
    const contains = words.filter((w) => !w.startsWith(prefix) && w.includes(prefix));
    response.json({ ok: true, words: [...exact, ...contains].slice(0, 40) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/pricemaster/search", async (request, response, next) => {
  try {
    const q = cleanText(request.query.q || request.query.search || "");
    const supplier = cleanText(request.query.supplier || "");
    const limit = cleanLimit(request.query.limit, 50, 300);
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const cacheKey = `pricemaster-search:${q.toLowerCase()}:${supplier.toLowerCase()}:${limit}:${usdRate.toFixed(4)}`;
    const cached = getPriceMasterSearchCache(cacheKey);
    if (cached) return response.json(cached);

    // OR-across-tokens approach (matches GingerPM word search): WHERE uses any-word OR,
    // post-filter enforces minimum match count so 1-word false positives are excluded.
    const tokenGroups = q ? pmQueryToTokenGroups(q) : null;
    const minMatchCount = tokenGroups ? (tokenGroups.length <= 2 ? tokenGroups.length : tokenGroups.length - 1) : 0;

    // Live PriceMaster is the source of truth — query it FIRST so the linking dialog
    // sees every current offer. The snapshot only supplements (it lags syncs and hides
    // inactive rows, which used to make articles "lose" suppliers in the search).
    let source = "live";
    let rows = [];
    let liveOk = false;

    try {
      const params = [];
      const conditions = ["r.Ignored = 0"];
      if (offerDocsActiveColumn) conditions.push(`d.${offerDocsActiveColumn}${offerDocsActiveFilterSuffix}`);
      if (q) {
        if (tokenGroups && tokenGroups.length) {
          // Single OR condition across all token synonyms — post-filter enforces minMatchCount
          const nameConds = tokenGroups.flatMap((group) => group.map(() => "r.NativeName LIKE ?"));
          const extraConds = tokenGroups[0].flatMap(() => ["r.NativeID LIKE ?", "r.BarCode LIKE ?", "p.PartnerName LIKE ?"]);
          conditions.push(`(${[...nameConds, ...extraConds].join(" OR ")})`);
          params.push(
            ...tokenGroups.flatMap((group) => group.map(likeSearch)),
            ...tokenGroups[0].flatMap((t) => [likeSearch(t), likeSearch(t), likeSearch(t)]),
          );
        } else {
          conditions.push("(r.NativeID LIKE ? OR r.NativeName LIKE ? OR r.BarCode LIKE ? OR p.PartnerName LIKE ?)");
          const like = likeSearch(q);
          params.push(like, like, like, like);
        }
      }
      if (supplier) {
        conditions.push("p.PartnerName LIKE ?");
        params.push(likeSearch(supplier));
      }
      // Extra headroom: OR-query pulls in more candidates; JS post-filter trims to minMatchCount.
      params.push(limit * 15);
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
      // Keep only the newest row per partner+article+name (docs repeat daily).
      const seenOffer = new Set();
      for (const row of liveRows) {
        const offerKey = `${cleanText(row.partnerId)}|${cleanText(row.article).toLowerCase()}|${cleanText(row.name).toLowerCase()}`;
        if (seenOffer.has(offerKey)) continue;
        seenOffer.add(offerKey);
        rows.push(mapPriceMasterSearchResponseRow(row, usdRate));
      }
      // Post-filter: each row must match at least minMatchCount token groups in name+article
      if (tokenGroups && tokenGroups.length >= 2) {
        rows = rows.filter((row) => {
          const hay = [cleanText(row.name || ""), cleanText(row.article || "")].join(" ");
          return pmWordMatchScore(hay, tokenGroups) >= minMatchCount;
        });
      }
      liveOk = true;
    } catch (error) {
      logger.warn("PriceMaster search live query failed, using snapshot", { detail: error?.message || String(error) });
    }

    // Supplement with snapshot rows the live answer didn't contain (e.g. live briefly
    // unavailable rows), and use it as the full fallback when live is down.
    const snapshotRows = await searchPriceMasterSnapshotOffers({
      search: q,
      partner: supplier,
      limit: limit * 2,
      usdRate,
      tokenGroups: q ? pmQueryToTokenGroups(q) : null,
    });
    if (snapshotRows?.length) {
      const seenRowIds = new Set(rows.map((row) => cleanText(row.rowId)));
      const seenOfferKeys = new Set(rows.map((row) => `${cleanText(row.partnerId)}|${cleanText(row.article).toLowerCase()}|${cleanText(row.name).toLowerCase()}`));
      for (const snapshotRow of snapshotRows) {
        const mapped = mapPriceMasterSearchResponseRow(snapshotRow, usdRate);
        const offerKey = `${cleanText(mapped.partnerId)}|${cleanText(mapped.article).toLowerCase()}|${cleanText(mapped.name).toLowerCase()}`;
        if (seenRowIds.has(cleanText(mapped.rowId)) || seenOfferKeys.has(offerKey)) continue;
        seenRowIds.add(cleanText(mapped.rowId));
        seenOfferKeys.add(offerKey);
        rows.push(mapped);
      }
      source = liveOk ? "live+snapshot" : "postgres_snapshot";
    }

    if (!rows.length) {
      const indexes = await getPriceMasterSnapshotIndexes();
      const fallbackRows = searchPriceMasterSnapshotJsonRows(indexes.rows || [], { q, supplier, limit, usdRate });
      if (fallbackRows.length) {
        source = "json_snapshot";
        rows = fallbackRows;
      }
    }

    // Linking UX: relevance first, cheapest rows within same relevance;
    // testers AND otlivants ALWAYS at the bottom, inactive rows after active.
    const tokenGroupsForScore = tokenGroups;
    const qLower = q.toLowerCase();
    const rowMarker = (row) => {
      const name = cleanText(row?.name || "").toLowerCase();
      if (name.includes("отливант")) return "otlivant";
      if (
        name.includes("тест")
        || name.includes("test")
        || name.includes("-tst")
        || /(^|[^a-z0-9])tst([^a-z0-9]|$)/u.test(name)
      ) return "tester";
      return null;
    };
    const rubPrice = (row) => {
      const price = Number(row?.price || 0) || 0;
      return cleanText(row?.priceCurrency || row?.currency).toUpperCase() === "USD" ? price * usdRate : price;
    };
    // Relevance tiers (higher = better match):
    //   1000 exact article | 900 article prefix | 800 article contains
    //   matchScore*10 + wordBoundaryBonus*5 for name-based matches
    // Word-boundary bonus: token appears as whole word in name (not mid-word like "oud" in "cloud")
    const computeRelevance = (row) => {
      const article = cleanText(row?.article || "").toLowerCase();
      const name = cleanText(row?.name || "").toLowerCase();
      if (qLower && article === qLower) return 1000;
      if (qLower && article.startsWith(qLower)) return 900;
      if (qLower && article.includes(qLower)) return 800;
      const matchScore = tokenGroupsForScore
        ? pmWordMatchScore([name, article].join(" "), tokenGroupsForScore)
        : 0;
      const wbScore = tokenGroupsForScore
        ? tokenGroupsForScore.reduce((n, group) => {
            const hasWB = group.some(token => {
              const idx = name.indexOf(token);
              if (idx === -1) return false;
              const c0 = name[idx - 1];
              const c1 = name[idx + token.length];
              return (!c0 || !/[a-zа-яё0-9]/i.test(c0)) && (!c1 || !/[a-zа-яё0-9]/i.test(c1));
            });
            return n + (hasWB ? 1 : 0);
          }, 0)
        : 0;
      return matchScore * 10 + wbScore * 5;
    };
    const sortedRows = rows
      .map((row) => {
        const marker = rowMarker(row);
        return { ...row, isTester: marker === "tester", isOtlivant: marker === "otlivant", matchScore: computeRelevance(row) };
      })
      .sort((a, b) => {
        const aSink = a.isTester || a.isOtlivant;
        const bSink = b.isTester || b.isOtlivant;
        if (aSink !== bSink) return aSink ? 1 : -1;
        if (Boolean(a.active) !== Boolean(b.active)) return a.active ? -1 : 1;
        if (b.matchScore !== a.matchScore) return b.matchScore - a.matchScore;
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
        consumerReady: Boolean(marketplaceWorker) || Number(queueStatus?.workers || 0) > 0 || Boolean(queueStatus?.consumerHeartbeatAt),
        counts: queueStatus?.counts || null,
        error: queueStatus?.error || null,
      },
    });
  } catch (error) {
    next(error);
  }
});

