function linkedWarehouseProductsForReprice(warehouse = {}, { productIds, marketplace = "all", limit = 0 } = {}) {
  const idSet = Array.isArray(productIds) && productIds.length
    ? new Set(productIds.map((id) => cleanText(id)).filter(Boolean))
    : null;
  const marketplaceFilter = cleanText(marketplace || "all").toLowerCase();
  const rows = (Array.isArray(warehouse.products) ? warehouse.products : [])
    .filter((product) => !idSet || idSet.has(String(product.id)))
    .filter((product) => marketplaceFilter === "all" || cleanText(product.marketplace).toLowerCase() === marketplaceFilter)
    .filter((product) => Array.isArray(product.links) && product.links.length)
    .filter((product) => isWarehouseProductTargetEnabled(product));
  const normalizedLimit = Math.max(0, Math.round(Number(limit || 0) || 0));
  return normalizedLimit > 0 ? rows.slice(0, normalizedLimit) : rows;
}

async function readLinkedProductsForReprice({ productIds, marketplace = "all", limit = 0 } = {}) {
  const warehouse = await readWarehouse();
  const fromMemory = linkedWarehouseProductsForReprice(warehouse, { productIds, marketplace, limit });
  // API server holds a partial in-memory warehouse (individual product updates only, not the full set).
  // Always fall through to Postgres on the API server so reprice covers all linked products.
  if (!isApiServer && (fromMemory.length || !shouldUsePostgresStorage())) return fromMemory;
  const prisma = getPrisma();
  if (!prisma) return fromMemory;
  const idSet = Array.isArray(productIds) && productIds.length
    ? productIds.map((id) => cleanText(id)).filter(Boolean)
    : null;
  const marketplaceFilter = cleanText(marketplace || "all").toLowerCase();
  const normalizedLimit = Math.max(0, Math.round(Number(limit || 0) || 0));
  const where = {
    AND: [
      enabledWarehouseTargetWhere(),
      { links: { some: {} } },
      idSet?.length ? { id: { in: idSet } } : {},
      marketplaceFilter !== "all" ? { marketplace: marketplaceFilter } : {},
    ].filter((item) => Object.keys(item || {}).length),
  };
  // Fetch in pages of 5 000 to avoid loading all data into Prisma's Rust heap at once.
  // A single unlimited findMany on a 20k-product dataset with includes can push the API
  // process over its memory ceiling, triggering the "Failed to convert rust String into
  // napi string" error even when the data itself is clean.
  const PAGE = 5000;
  let rows = [];
  try {
    let skip = 0;
    for (;;) {
      const take = normalizedLimit > 0 ? Math.min(PAGE, normalizedLimit - rows.length) : PAGE;
      const page = await prisma.warehouseProduct.findMany({
        where,
        include: { links: true },
        orderBy: { updatedAt: "desc" },
        take,
        skip,
      });
      rows.push(...page);
      if (page.length < PAGE) break;
      if (normalizedLimit > 0 && rows.length >= normalizedLimit) break;
      skip += PAGE;
    }
  } catch (pgError) {
    logger.warn("readLinkedProductsForReprice postgres failed, using memory fallback", {
      detail: pgError?.message || String(pgError),
    });
    return fromMemory;
  }
  return rows
    .map(productFromPostgres)
    .filter((product) => Array.isArray(product.links) && product.links.length)
    .filter((product) => isWarehouseProductTargetEnabled(product));
}

async function markSalesAutomationPriceQueued(products = [], {
  priceIntentId,
  reason = "queued",
  marketplace = "all",
  sourceEvent = "",
  queuedAt = new Date().toISOString(),
} = {}) {
  const rows = (Array.isArray(products) ? products : [])
    .filter((product) => cleanText(product.offerId))
    .map((product) => ({
      productId: product.id,
      marketplace: cleanText(product.marketplace).toLowerCase() === "yandex" ? "yandex" : "ozon",
      target: cleanText(product.target) || "default",
      offerId: cleanText(product.offerId),
      currentPrice: Number(product.currentPrice || product.marketplacePrice || 0) || null,
      targetPrice: Number(product.targetPrice || product.nextPrice || 0) || null,
      targetStock: Number(product.targetStock || product.marketplaceState?.stock || 0) || null,
      priceStatus: "pending",
      stockStatus: "pending",
      unarchiveStatus: "pending",
      reason: "queued",
      lastCalculatedAt: queuedAt,
      raw: {
        productId: product.id,
        marketplace: product.marketplace,
        target: product.target,
        offerId: product.offerId,
        priceIntentId,
        sourceEvent,
        requestedMarketplace: marketplace,
        queuedAt,
        reason,
        priceApplyStatus: "queued",
      },
    }));
  return upsertSalesAutomationSkuStates(rows);
}

