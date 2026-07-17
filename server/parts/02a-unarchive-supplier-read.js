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

// Кэш «что уже записано в PG» жил только в памяти: каждый рестарт worker
// считал изменившимся ВЕСЬ каталог и один раз переписывал 8-11k товаров
// (тяжёлая волна upsert'ов + блокировки event loop). Снимок карты
// id → updatedAt персистится в data/ (дебаунс 60 с) и прогревается на старте.
const warehousePgWrittenCachePath = path.join(dataDir, "warehouse-pg-written-cache.json");
let warehousePgWrittenCacheLoadPromise = null;
let warehousePgWrittenCachePersistTimer = null;

function loadWarehousePgWrittenCache() {
  if (!warehousePgWrittenCacheLoadPromise) {
    warehousePgWrittenCacheLoadPromise = (async () => {
      try {
        const saved = JSON.parse(await fs.readFile(warehousePgWrittenCachePath, "utf8"));
        for (const [id, updatedAt] of Object.entries(saved.items || {})) {
          if (warehousePostgresUpdatedAtCache.has(id)) continue;
          warehousePostgresHashCache.set(id, true);
          warehousePostgresUpdatedAtCache.set(id, cleanText(updatedAt));
        }
      } catch {
        // нет файла / битый — прогреется живыми записями
      }
    })();
  }
  return warehousePgWrittenCacheLoadPromise;
}

function persistWarehousePgWrittenCacheSoon() {
  if (warehousePgWrittenCachePersistTimer) return;
  warehousePgWrittenCachePersistTimer = setTimeout(async () => {
    warehousePgWrittenCachePersistTimer = null;
    try {
      const items = {};
      for (const [id, updatedAt] of warehousePostgresUpdatedAtCache) items[id] = updatedAt;
      await fs.writeFile(warehousePgWrittenCachePath, JSON.stringify({ savedAt: new Date().toISOString(), items }));
    } catch (error) {
      logger.warn("warehouse pg written cache persist failed", { detail: error?.message || String(error) });
    }
  }, 60_000);
  warehousePgWrittenCachePersistTimer.unref?.();
}

function markWarehousePostgresProductsWritten(products = []) {
  for (const product of products || []) {
    if (!product?.id) continue;
    warehousePostgresHashCache.set(product.id, true);
    warehousePostgresUpdatedAtCache.set(product.id, cleanText(product.updatedAt));
  }
  persistWarehousePgWrittenCacheSoon();
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

