async function buildWarehouseProductDetailFromPostgres(productId, { usdRate } = {}) {
  const prisma = getPrisma();
  if (!prisma) return null;
  kickWarehousePostgresLinksBackfill(prisma);
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  return warehousePostgresCachedDetail(`product:${productId}:${rate}`, async () => {
    const row = await prisma.warehouseProduct.findFirst({
      where: { AND: [enabledWarehouseTargetWhere(), { id: productId }] },
      include: { links: true },
    });
    if (!row) return null;
    const normalizedSuppliers = await getWarehousePostgresSuppliers(prisma);
    const warehouse = {
      createdAt: row.createdAt?.toISOString?.() || null,
      updatedAt: row.updatedAt?.toISOString?.() || null,
      products: [productFromPostgres(row)],
      suppliers: normalizedSuppliers,
    };
    const built = await buildFreshWarehouseProductsForWarehouse(
      warehouse,
      [row.id],
      { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
    );
    const product = built[0] || warehouse.products[0];
    queueLinkedUnavailableSupplierZeroStock(built, { source: "warehouse_product_detail" });
    return {
      createdAt: warehouse.createdAt,
      product: {
        ...product,
        autoPriceEnabled: product.autoPriceEnabled !== false,
        links: Array.isArray(product.links) ? product.links : [],
        suppliers: Array.isArray(product.suppliers) ? product.suppliers : [],
        selectedSupplier: product.selectedSupplier || null,
        noSupplierAutomation: product.noSupplierAutomation || {},
        marketplaceState: product.marketplaceState || {},
        partial: (product.links || []).length > 0 && !product.selectedSupplier && !product.stockOnlyFallbackActive,
      },
    };
  });
}

function normalizeWarehouseDetailProduct(product = {}) {
  return {
    ...product,
    autoPriceEnabled: product.autoPriceEnabled !== false,
    links: Array.isArray(product.links) ? product.links : [],
    suppliers: Array.isArray(product.suppliers) ? product.suppliers : [],
    selectedSupplier: product.selectedSupplier || null,
    noSupplierAutomation: product.noSupplierAutomation || {},
    marketplaceState: product.marketplaceState || {},
    partial: false,
  };
}

function publicSelectedSupplierDiagnostics(supplier = null) {
  if (!supplier) return null;
  return {
    supplierName: supplier.supplierName || supplier.name || "",
    article: supplier.article || supplier.supplierArticle || "",
    partnerId: supplier.partnerId || "",
    available: supplier.available !== false,
    price: supplier.price ?? supplier.calculatedPrice ?? null,
    priceRub: supplier.priceRub ?? supplier.calculatedPriceRub ?? null,
    calculatedPrice: supplier.calculatedPrice ?? supplier.priceRub ?? supplier.calculatedPriceRub ?? null,
    markupCoefficient: supplier.markupCoefficient ?? null,
    baseMarkupCoefficient: supplier.baseMarkupCoefficient ?? null,
    currency: supplier.currency || supplier.priceCurrency || "",
  };
}

function latestProductCommand(product = {}, kind = "stock") {
  return kind === "archive"
    ? normalizeLastMarketplaceCommand(product.lastArchiveSend || {})
    : normalizeLastMarketplaceCommand(product.lastStockSend || {});
}



