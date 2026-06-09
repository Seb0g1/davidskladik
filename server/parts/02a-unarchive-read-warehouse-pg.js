async function readWarehouseFromPostgres(prisma) {
  kickWarehousePostgresLinksBackfill(prisma);
  const startedAt = Date.now();
  const [products, suppliers] = await Promise.all([
    prisma.warehouseProduct.findMany({
      include: { links: true },
      orderBy: { updatedAt: "desc" },
    }),
    prisma.managedSupplier.findMany({ orderBy: { name: "asc" } }),
  ]);
  logger.info("warehouse postgres full load complete", {
    products: products.length,
    suppliers: suppliers.length,
    elapsedMs: Date.now() - startedAt,
  });
  if (!products.length && !suppliers.length) return null;
  const updatedAtMs = Math.max(
    ...products.map((item) => item.updatedAt?.getTime() || 0),
    ...suppliers.map((item) => item.updatedAt?.getTime() || 0),
    0,
  );
  const warehouse = {
    createdAt: products[0]?.createdAt?.toISOString() || new Date().toISOString(),
    updatedAt: updatedAtMs ? new Date(updatedAtMs).toISOString() : null,
    products: products.map(productFromPostgres),
    suppliers: suppliers.map(supplierFromPostgres),
  };
  refreshWarehouseHashCache(warehouse);
  return warehouse;
}

async function getWarehouseMetaFast() {
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const [productCount, supplierCount, productAgg, supplierAgg] = await Promise.all([
        prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }),
        prisma.managedSupplier.count(),
        prisma.warehouseProduct.aggregate({
          where: enabledWarehouseTargetWhere(),
          _max: { updatedAt: true },
          _min: { createdAt: true },
        }),
        prisma.managedSupplier.aggregate({
          _max: { updatedAt: true },
          _min: { createdAt: true },
        }),
      ]);
      const updatedAtMs = Math.max(
        productAgg._max.updatedAt?.getTime?.() || 0,
        supplierAgg._max.updatedAt?.getTime?.() || 0,
      );
      const createdAtMs = Math.min(
        ...[
          productAgg._min.createdAt?.getTime?.() || 0,
          supplierAgg._min.createdAt?.getTime?.() || 0,
        ].filter(Boolean),
      );
      return {
        updatedAt: updatedAtMs ? new Date(updatedAtMs).toISOString() : null,
        createdAt: Number.isFinite(createdAtMs) ? new Date(createdAtMs).toISOString() : null,
        products: productCount,
        suppliers: supplierCount,
        source: "postgres",
      };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("warehouse postgres meta failed, using memory/json fallback", { detail: error?.message || String(error) });
    }
  }
  const warehouse = await readWarehouse();
  return {
    updatedAt: warehouse.updatedAt || warehouse.createdAt || null,
    createdAt: warehouse.createdAt || null,
    products: Array.isArray(warehouse.products) ? warehouse.products.length : 0,
    suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.length : 0,
    source: shouldUsePostgresStorage() ? "fallback" : "json",
  };
}

