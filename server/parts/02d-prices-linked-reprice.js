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
  if (fromMemory.length || !shouldUsePostgresStorage()) return fromMemory;
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
  const rows = await prisma.warehouseProduct.findMany({
    where,
    include: { links: true },
    orderBy: { updatedAt: "desc" },
    ...(normalizedLimit > 0 ? { take: normalizedLimit } : {}),
  });
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

