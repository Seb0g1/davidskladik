function supplierFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeManagedSupplier({
    ...raw,
    id: raw.id || row.partnerId || row.id,
    partnerId: row.partnerId,
    name: row.name,
    stopped: row.active === false,
    priceCurrency: row.defaultCurrency,
    stopReason: row.stopReason,
    note: row.note,
    createdAt: row.createdAt ? row.createdAt.toISOString() : raw.createdAt,
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : raw.updatedAt,
  });
}

function refreshWarehouseHashCache(warehouse = {}) {
  warehousePostgresHashCache = new Map();
  warehousePostgresUpdatedAtCache = new Map();
  for (const product of warehouse.products || []) {
    warehousePostgresHashCache.set(product.id, true);
    warehousePostgresUpdatedAtCache.set(product.id, cleanText(product.updatedAt));
  }
}

function markWarehousePostgresProductsWritten(products = []) {
  for (const product of products || []) {
    if (!product?.id) continue;
    warehousePostgresHashCache.set(product.id, true);
    warehousePostgresUpdatedAtCache.set(product.id, cleanText(product.updatedAt));
  }
}

async function readWarehousePostgresStub(prisma) {
  const suppliers = await prisma.managedSupplier.findMany({ orderBy: { name: "asc" } });
  const meta = await getWarehouseMetaFast().catch(() => null);
  return {
    createdAt: meta?.createdAt || new Date().toISOString(),
    updatedAt: meta?.updatedAt || null,
    products: [],
    suppliers: suppliers.map(supplierFromPostgres),
    postgresOnly: true,
  };
}

