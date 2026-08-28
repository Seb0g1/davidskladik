async function appendPriceHistoryRows(rows = []) {
  if (!shouldUsePostgresStorage()) return 0;
  const normalizedRows = (Array.isArray(rows) ? rows : [])
    .map((row) => {
      // Store price breakdown in response so we can diagnose "where did this price come from"
      // without a schema migration. Fields: pmPriceUsd, usdRate, markup, supplierName, supplierArticle, reason.
      const breakdown = {};
      if (row.pmPriceUsd != null) breakdown.pmPriceUsd = Number(row.pmPriceUsd);
      if (row.usdRate != null) breakdown.usdRate = Number(row.usdRate);
      if (row.markup != null) breakdown.markup = Number(row.markup);
      if (row.supplierName) breakdown.supplierName = cleanText(row.supplierName);
      if (row.supplierArticle) breakdown.supplierArticle = cleanText(row.supplierArticle);
      if (row.reason) breakdown.reason = cleanText(row.reason);
      const existingResponse = cloneAuditValue(row.response || row.result || null);
      const mergedResponse = Object.keys(breakdown).length
        ? { ...(existingResponse && typeof existingResponse === "object" ? existingResponse : {}), ...breakdown }
        : existingResponse;
      return {
        productId: cleanText(row.productId || row.id) || null,
        marketplace: normalizeMarketplaceEnum(row.marketplace || "ozon"),
        target: cleanText(row.target || row.marketplace) || null,
        offerId: cleanText(row.offerId || row.offer_id),
        oldPrice: row.oldPrice === undefined || row.oldPrice === null ? null : (roundPrice(row.oldPrice) || 0),
        newPrice: roundPrice(row.newPrice ?? row.price ?? 0) || 0,
        status: normalizeQueueStatusEnum(row.status || (row.error ? "failed" : "success")),
        response: mergedResponse,
        error: cleanText(row.error || ""),
        createdAt: toDateOrNull(row.createdAt || row.at) || new Date(),
      };
    })
    .filter((row) => row.offerId && row.newPrice > 0);
  if (!normalizedRows.length) return 0;
  try {
    const windowMs = priceHistoryDedupeWindowMs();
    const dedupedRows = [];
    const seenRows = new Set();
    for (const row of normalizedRows) {
      const createdAt = row.createdAt || new Date();
      const recentSince = new Date(createdAt.getTime() - windowMs);
      const rowKey = [
        row.productId || "",
        row.marketplace,
        row.target || "",
        row.offerId,
        row.oldPrice ?? "",
        row.newPrice,
        row.status,
        row.error || "",
        Math.floor(createdAt.getTime() / windowMs),
      ].join("|");
      if (seenRows.has(rowKey)) continue;
      seenRows.add(rowKey);
      const existing = await getPrisma().priceHistory.findFirst({
        where: {
          productId: row.productId || null,
          marketplace: row.marketplace,
          target: row.target || null,
          offerId: row.offerId,
          oldPrice: row.oldPrice === undefined ? null : row.oldPrice,
          newPrice: row.newPrice,
          status: row.status,
          OR: [{ error: row.error || "" }, { error: null }],
          createdAt: {
            gte: recentSince,
            lte: new Date(createdAt.getTime() + windowMs),
          },
        },
        select: { id: true },
      });
      if (!existing) dedupedRows.push(row);
    }
    if (!dedupedRows.length) return 0;
    const result = await getPrisma().priceHistory.createMany({
      data: dedupedRows,
      skipDuplicates: true,
    });
    return result.count || 0;
  } catch (error) {
    logger.warn("postgres price history append failed", { detail: error?.message || String(error), rows: normalizedRows.length });
    return 0;
  }
}

function priceHistoryRowFromPostgres(row = {}) {
  const resp = row.response && typeof row.response === "object" ? row.response : null;
  return {
    id: row.id || null,
    productId: row.productId || null,
    marketplace: row.marketplace || "ozon",
    target: row.target || null,
    offerId: row.offerId || null,
    oldPrice: row.oldPrice ?? null,
    newPrice: row.newPrice ?? null,
    status: row.status || "pending",
    response: row.response || null,
    error: row.error || "",
    // Price breakdown (stored in response field to avoid schema migration)
    pmPriceUsd: resp?.pmPriceUsd ?? null,
    usdRate: resp?.usdRate ?? null,
    markup: resp?.markup ?? null,
    supplierName: resp?.supplierName ?? null,
    supplierArticle: resp?.supplierArticle ?? null,
    reason: resp?.reason ?? null,
    at: row.createdAt ? row.createdAt.toISOString() : null,
    createdAt: row.createdAt ? row.createdAt.toISOString() : null,
  };
}

async function readPriceHistory({ productId, offerId, marketplace, status, dateFrom, dateTo, limit = 100, offset = 0 } = {}) {
  const productIds = splitList(productId);
  const offerIds = splitList(offerId);
  const statuses = splitList(status)
    .map((item) => item.toLowerCase() === "error" ? "failed" : item.toLowerCase())
    .filter((item) => item !== "all");
  const marketplaceFilter = cleanText(marketplace).toLowerCase();
  const from = toDateOrNull(dateFrom);
  const to = toDateOrNull(dateTo);
  const safeLimit = Math.max(1, Math.min(500, Number(limit || 100) || 100));
  const safeOffset = Math.max(0, Number(offset || 0) || 0);

  if (shouldUsePostgresStorage()) {
    try {
      const where = {};
      if (productIds.length) where.productId = { in: productIds };
      if (offerIds.length) where.offerId = { in: offerIds };
      if (marketplaceFilter && marketplaceFilter !== "all") where.marketplace = normalizeMarketplaceEnum(marketplaceFilter);
      if (statuses.length) where.status = { in: statuses.map((item) => normalizeQueueStatusEnum(item)) };
      if (from || to) {
        where.createdAt = {};
        if (from) where.createdAt.gte = from;
        if (to) where.createdAt.lte = to;
      }
      const [total, rows] = await Promise.all([
        getPrisma().priceHistory.count({ where }),
        getPrisma().priceHistory.findMany({
          where,
          orderBy: { createdAt: "desc" },
          skip: safeOffset,
          take: safeLimit,
        }),
      ]);
      return {
        source: "postgres",
        total,
        limit: safeLimit,
        offset: safeOffset,
        items: rows.map(priceHistoryRowFromPostgres),
      };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read price history postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }

  const warehouse = await readWarehouse();
  const rows = [];
  for (const product of warehouse.products || []) {
    if (productIds.length && !productIds.includes(String(product.id))) continue;
    if (offerIds.length && !offerIds.includes(String(product.offerId))) continue;
    if (marketplaceFilter && marketplaceFilter !== "all" && cleanText(product.marketplace) !== marketplaceFilter) continue;
    for (const entry of product.priceHistory || []) {
      const at = toDateOrNull(entry.at || entry.createdAt);
      const normalizedStatus = normalizeQueueStatusEnum(entry.status === "error" ? "failed" : entry.status);
      if (statuses.length && !statuses.includes(normalizedStatus)) continue;
      if (from && (!at || at < from)) continue;
      if (to && (!at || at > to)) continue;
      rows.push({
        productId: product.id,
        marketplace: product.marketplace,
        target: entry.target || product.target || product.marketplace,
        offerId: entry.offerId || product.offerId,
        oldPrice: entry.oldPrice ?? null,
        newPrice: entry.newPrice ?? null,
        status: normalizedStatus,
        response: null,
        error: entry.error || "",
        supplierName: entry.supplierName || "",
        supplierArticle: entry.supplierArticle || "",
        reason: entry.reason || "",
        at: at ? at.toISOString() : null,
        createdAt: at ? at.toISOString() : null,
      });
    }
  }
  rows.sort((a, b) => new Date(b.at || 0) - new Date(a.at || 0));
  return {
    source: "json",
    total: rows.length,
    limit: safeLimit,
    offset: safeOffset,
    items: rows.slice(safeOffset, safeOffset + safeLimit),
  };
}
