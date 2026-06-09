function warehouseGroupKeyParts(groupKey = "") {
  const text = cleanText(groupKey);
  const [kind, ...rest] = text.split(":");
  return { kind: cleanText(kind).toLowerCase(), value: cleanText(rest.join(":")).toLowerCase() };
}

async function buildWarehouseGroupDetailFromPostgres(groupKey, { usdRate, filters = {}, refreshPrices = false } = {}) {
  const prisma = getPrisma();
  if (!prisma) return null;
  const { kind, value } = warehouseGroupKeyParts(groupKey);
  if (!value) return null;
  void kickWarehousePostgresLinksBackfill(prisma);
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const cacheKey = `group:${groupKey}:${rate}:all-marketplaces:${refreshPrices ? "live" : "stored"}`;
  return warehousePostgresCachedDetail(cacheKey, async () => {
    const baseWhere = warehousePagePostgresWhere({ marketplace: "all", state: "all", q: "", linked: "all", brand: "" });
    let rows = [];
    if (kind === "offer") {
      rows = await prisma.warehouseProduct.findMany({
        where: { AND: [baseWhere, { offerId: { equals: value, mode: "insensitive" } }] },
        include: { links: true },
        orderBy: warehousePagePostgresOrderBy(),
      });
      const crossRows = await fetchCrossMarketplaceSiblingRows(prisma, baseWhere, rows);
      rows = mergeWarehousePostgresRows(rows, crossRows);
    } else if (kind === "pair") {
      rows = await prisma.warehouseProduct.findMany({
        where: {
          AND: [
            baseWhere,
            {
              OR: [
                { marketplace: "ozon", id: value },
                { raw: { path: ["yandex", "extra", "sourceProductId"], equals: value } },
                { raw: { path: ["manualGroupId"], equals: `auto-pair-${value}` } },
              ],
            },
          ],
        },
        include: { links: true },
        orderBy: warehousePagePostgresOrderBy(),
      });
      const crossRows = await fetchCrossMarketplaceSiblingRows(prisma, baseWhere, rows);
      rows = mergeWarehousePostgresRows(rows, crossRows);
    } else if (kind === "manual") {
      rows = await prisma.warehouseProduct.findMany({
        where: {
          AND: [
            baseWhere,
            { raw: { path: ["manualGroupId"], equals: value } },
          ],
        },
        include: { links: true },
        orderBy: warehousePagePostgresOrderBy(),
      }).catch(async (error) => {
        logger.warn("warehouse manual group postgres direct lookup failed, using fallback scan", { detail: error?.message || String(error) });
        const candidates = await prisma.warehouseProduct.findMany({
          where: baseWhere,
          include: { links: true },
          orderBy: warehousePagePostgresOrderBy(),
        });
        const groupContext = buildWarehouseCatalogGroupContext(candidates.map(productFromPostgres));
        return candidates.filter((row) => warehouseProductPageGroupKey(productFromPostgres(row), groupContext) === groupKey);
      });
    }
    if (!rows.length) return null;
    const normalizedSuppliers = await getWarehousePostgresSuppliers(prisma);
    const detailProducts = await buildWarehouseDetailProductsFromPageWarehouse(
      { products: rows.map(productFromPostgres), suppliers: normalizedSuppliers },
      { refreshPrices, usdRate: rate },
    );
    return {
      products: detailProducts,
      suppliers: normalizedSuppliers,
      groupLinkSignature: warehouseGroupLinkSignature(detailProducts),
      marketplacePriceBreakdown: marketplacePriceBreakdown(detailProducts),
    };
  });
}

async function buildWarehouseGroupDetail(groupKey, { usdRate, filters = {}, refreshPrices = false } = {}) {
  if (shouldUsePostgresStorage()) {
    const postgresDetail = await buildWarehouseGroupDetailFromPostgres(groupKey, { usdRate, filters, refreshPrices });
    if (postgresDetail) return postgresDetail;
  }
  const warehouse = await readWarehouse();
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const enabledProducts = (Array.isArray(warehouse.products) ? warehouse.products : []).filter(isWarehouseProductTargetEnabled);
  const groupContext = buildWarehouseCatalogGroupContext(enabledProducts);
  const products = enabledProducts.filter((product) => warehouseProductPageGroupKey(product, groupContext) === groupKey);
  if (!products.length) return null;
  const pageProducts = await enrichWeakOzonProductsForPage(products);
  const built = await buildFreshWarehouseProductsForWarehouse(
    { ...warehouse, products: pageProducts },
    pageProducts.map((product) => product.id),
    {
      refreshPrices,
      livePriceMaster: refreshPrices,
      batchPriceMaster: refreshPrices,
      usdRate: rate,
      priceMasterTimeoutMs: refreshPrices ? autoPricePmTimeoutMs : undefined,
    },
  );
  const builtMap = new Map(built.map((product) => [product.id, product]));
  const detailProducts = pageProducts.map((product) => normalizeWarehouseDetailProduct(builtMap.get(product.id) || normalizeWarehouseProduct(product)));
  return {
    products: detailProducts,
    suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers : [],
    groupLinkSignature: warehouseGroupLinkSignature(detailProducts),
    marketplacePriceBreakdown: marketplacePriceBreakdown(detailProducts),
  };
}
