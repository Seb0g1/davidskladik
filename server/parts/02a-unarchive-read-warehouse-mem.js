function warehouseMemoryCacheIsHydratedStub(cache = null) {
  return Boolean(cache?.postgresOnly && Array.isArray(cache.products) && cache.products.length > 0);
}

async function loadWarehouseMemory({ forceFull = false } = {}) {
  if (warehouseMemoryCache) {
    if (!warehouseMemoryCache.postgresOnly || forceFull || warehouseMemoryCacheIsHydratedStub(warehouseMemoryCache)) {
      return warehouseMemoryCache;
    }
    warehouseMemoryCache = null;
    warehouseMemoryProductIndexCache = { products: null, byId: new Map() };
  }
  if (shouldUsePostgresStorage()) {
    if (!warehouseFullMemoryLoadEnabled) {
      const stub = await readWarehousePostgresStub(getPrisma());
      warehouseMemoryCache = stub;
      if (forceFull) {
        logger.warn("warehouse full memory load blocked on postgres", {
          hint: "set WAREHOUSE_FULL_MEMORY_LOAD_ENABLED=true only on 32GB+ servers",
        });
      }
      return warehouseMemoryCache;
    }
    try {
      const warehouse = await readWarehouseFromPostgres(getPrisma());
      if (warehouse) {
        const materialized = materializeYandexExportedProductsForWarehouse(warehouse);
        warehouseMemoryCache = materialized.warehouse;
        if (materialized.added > 0) {
          await writeWarehouse(materialized.warehouse);
        }
        return warehouseMemoryCache;
      }
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read warehouse postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const jsonStat = await fs.stat(warehousePath).catch(() => null);
    if (jsonStat?.size > warehouseJsonMaxLoadBytes) {
      if (shouldUsePostgresStorage()) {
        const stub = await readWarehousePostgresStub(getPrisma());
        warehouseMemoryCache = stub;
        logger.warn("warehouse json file too large for memory load, using postgres stub", {
          bytes: jsonStat.size,
          maxBytes: warehouseJsonMaxLoadBytes,
        });
        return warehouseMemoryCache;
      }
      throw new Error(`warehouse_json_too_large:${jsonStat.size}`);
    }
    const warehouse = JSON.parse(await fs.readFile(warehousePath, "utf8"));
    const normalized = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: warehouse.updatedAt || null,
      products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    };
    const materialized = materializeYandexExportedProductsForWarehouse(normalized);
    warehouseMemoryCache = materialized.warehouse;
    if (materialized.added > 0) {
      await writeWarehouse(materialized.warehouse, { writePostgres: false });
    }
    refreshWarehouseHashCache(warehouseMemoryCache);
    return warehouseMemoryCache;
  } catch (error) {
    if (error.code === "ENOENT") {
      warehouseMemoryCache = { createdAt: new Date().toISOString(), updatedAt: null, products: [], suppliers: [] };
      refreshWarehouseHashCache(warehouseMemoryCache);
      return warehouseMemoryCache;
    }
    throw error;
  }
}

async function readWarehouse(options = {}) {
  const forceFull = options?.forceFull === true;
  if (warehouseMemoryCache) {
    if (!warehouseMemoryCache.postgresOnly || forceFull || warehouseMemoryCacheIsHydratedStub(warehouseMemoryCache)) {
      return warehouseMemoryCache;
    }
    warehouseMemoryCache = null;
    warehouseMemoryProductIndexCache = { products: null, byId: new Map() };
  }
  if (forceFull) return loadWarehouseMemory({ forceFull: true });
  if (!warehouseMemoryLoadPromise) {
    warehouseMemoryLoadPromise = loadWarehouseMemory({ forceFull: false }).finally(() => {
      warehouseMemoryLoadPromise = null;
    });
  }
  return warehouseMemoryLoadPromise;
}

async function readWarehouseFull() {
  return readWarehouse({ forceFull: true });
}



