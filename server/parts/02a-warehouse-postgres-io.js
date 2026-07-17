function autoSyncShouldImportMarketplaces(trigger = "auto") {
  const normalized = cleanText(trigger).toLowerCase();
  return normalized === "manual" || normalized === "daily" || normalized === "full";
}

async function findWarehouseProductById(productId = "") {
  const id = cleanText(productId);
  if (!id) return null;
  const [hydrated] = await hydrateWarehouseProductsForIds([id], { expandGroups: false });
  if (hydrated && String(hydrated.id) === id) return hydrated;
  const warehouse = await readWarehouse();
  const cached = (warehouse.products || []).find((item) => String(item.id) === id);
  if (cached) return cached;
  let [fromPostgres] = await readWarehouseProductsFromPostgresByIds([id], { includeDisabledTargets: true });
  if (!fromPostgres) {
    [fromPostgres] = await readWarehouseProductsFromPostgresByIds([id]);
  }
  if (fromPostgres) {
    mergeWarehouseProductsIntoMemory([fromPostgres]);
    return fromPostgres;
  }
  return null;
}

async function readWarehouseProductsFromPostgresByIds(productIds = [], { includeDisabledTargets = false } = {}) {
  const ids = Array.from(new Set((Array.isArray(productIds) ? productIds : [productIds])
    .map((id) => cleanText(id))
    .filter(Boolean)));
  if (!ids.length || !shouldUsePostgresStorage()) return [];
  const prisma = getPrisma();
  if (!prisma) return [];
  const where = includeDisabledTargets
    ? { id: { in: ids } }
    : { AND: [enabledWarehouseTargetWhere(), { id: { in: ids } }] };
  const rows = await prisma.warehouseProduct.findMany({
    where,
    include: { links: true },
  });
  return rows.map(productFromPostgres);
}

async function readWarehouseGroupSiblingsFromPostgres(seedProducts = []) {
  const seeds = (Array.isArray(seedProducts) ? seedProducts : []).filter((product) => product?.id);
  if (!seeds.length || !shouldUsePostgresStorage()) return seeds;
  const prisma = getPrisma();
  if (!prisma) return seeds;
  const byId = new Map(seeds.map((product) => [String(product.id), product]));
  const offerIds = Array.from(new Set(seeds.map((product) => cleanText(product.offerId)).filter(Boolean)));
  const manualGroupIds = Array.from(new Set(seeds.map((product) => {
    const raw = product?.raw && typeof product.raw === "object" && !Array.isArray(product.raw) ? product.raw : {};
    return cleanText(raw.manualGroupId || raw.manual_group_id);
  }).filter(Boolean)));
  const pairOzonIds = Array.from(new Set(seeds.map((product) => resolveWarehouseProductPairOzonId(product)).filter(Boolean)));
  for (const seed of seeds) {
    if (cleanText(seed.marketplace).toLowerCase() === "ozon" && seed.id) {
      pairOzonIds.push(cleanText(seed.id));
    }
  }
  const uniquePairOzonIds = Array.from(new Set(pairOzonIds.map((id) => cleanText(id)).filter(Boolean)));
  const baseWhere = enabledWarehouseTargetWhere();
  const clauses = offerIds.map((offerId) => ({ offerId: { equals: offerId, mode: "insensitive" } }));
  for (const manualGroupId of manualGroupIds) {
    if (!manualGroupId.startsWith("auto-pair-")) {
      clauses.push({ raw: { path: ["manualGroupId"], equals: manualGroupId } });
    }
  }
  for (const ozonId of uniquePairOzonIds) {
    clauses.push({ marketplace: "ozon", id: ozonId });
    clauses.push({ raw: { path: ["yandex", "extra", "sourceProductId"], equals: ozonId } });
    clauses.push({ raw: { path: ["manualGroupId"], equals: `auto-pair-${ozonId}` } });
  }
  if (!clauses.length) return Array.from(byId.values());
  const clauseChunkSize = Math.max(50, Math.min(8000, Number(process.env.WAREHOUSE_POSTGRES_OR_CHUNK || 4000) || 4000));
  let rows = [];
  try {
    for (const clauseChunk of chunkArray(clauses, clauseChunkSize)) {
      const chunkRows = await prisma.warehouseProduct.findMany({
        where: { AND: [baseWhere, { OR: clauseChunk }] },
        include: { links: true },
      });
      rows.push(...chunkRows);
    }
  } catch (error) {
    if (!offerIds.length) return Array.from(byId.values());
    logger.warn("warehouse postgres group sibling lookup failed, using offerId fallback", { detail: error?.message || String(error) });
    for (const offerChunk of chunkArray(offerIds, clauseChunkSize)) {
      const chunkRows = await prisma.warehouseProduct.findMany({
        where: {
          AND: [
            baseWhere,
            { OR: offerChunk.map((offerId) => ({ offerId: { equals: offerId, mode: "insensitive" } })) },
          ],
        },
        include: { links: true },
      });
      rows.push(...chunkRows);
    }
  }
  for (const row of rows) {
    const product = productFromPostgres(row);
    byId.set(String(product.id), product);
  }
  return Array.from(byId.values());
}

function mergeWarehouseProductsIntoMemory(products = [], { suppliers = null } = {}) {
  const normalized = (Array.isArray(products) ? products : []).map(normalizeWarehouseProduct).filter((product) => product?.id);
  if (!normalized.length) return warehouseMemoryCache || { products: [], suppliers: [] };
  const existing = warehouseMemoryCache || { createdAt: new Date().toISOString(), updatedAt: null, products: [], suppliers: [] };
  if (!Array.isArray(existing.products)) existing.products = [];
  // Раньше каждый hydrate (батч реконсайлера — каждые ~40 с) строил здесь
  // новую Map и новый массив на все ~30k товаров: аллокационный шторм давал
  // GC-паузы 7-10 с (event_loop_blocked) и рестарты worker watchdog'ом.
  // Теперь массив мутируется на месте: идентичность products сохраняется,
  // позиционный индекс warehouseProductIndexFor остаётся валидным и лишь
  // дополняется для новых товаров.
  const list = existing.products;
  const index = warehouseProductIndexFor(existing);
  for (const product of normalized) {
    const key = String(product.id);
    const position = index.get(key);
    if (position === undefined) {
      index.set(key, list.length);
      list.push(product);
    } else {
      list[position] = product;
    }
  }
  if (Array.isArray(suppliers) && suppliers.length) existing.suppliers = suppliers;
  else if (!Array.isArray(existing.suppliers)) existing.suppliers = [];
  existing.postgresOnly = shouldUsePostgresStorage() && !warehouseFullMemoryLoadEnabled;
  warehouseMemoryCache = existing;
  return existing;
}

async function hydrateWarehouseProductsForIds(productIds = [], { expandGroups = true, forceRefresh = false } = {}) {
  const ids = Array.from(new Set((Array.isArray(productIds) ? productIds : [productIds])
    .map((id) => cleanText(id))
    .filter(Boolean)));
  if (!ids.length) return [];
  const warehouse = await readWarehouse();
  // forceRefresh: reload ALL requested IDs from Postgres, not just missing ones.
  // Required in the worker process which has a separate in-memory cache: after the API
  // process writes a change (e.g. snooze removal), the worker cache is stale and
  // hydrateWarehouseProductsForIds must go to Postgres to get the updated state.
  const missingIds = forceRefresh
    ? ids
    : (() => {
      // O(1) по позиционному индексу вместо O(ids × 30k) прохода по каталогу.
      const index = warehouseProductIndexFor(warehouse);
      return ids.filter((id) => !index.has(id));
    })();
  if (missingIds.length && shouldUsePostgresStorage()) {
    let loaded = await readWarehouseProductsFromPostgresByIds(missingIds, { includeDisabledTargets: true });
    if (!loaded.length) {
      loaded = await readWarehouseProductsFromPostgresByIds(missingIds);
    }
    if (loaded.length) {
      // Не предпочитаем in-memory список поставщиков: в worker-процессе он может быть
      // устаревшим (валюту/статус меняют через api-процесс). getWarehousePostgresSuppliers
      // кэширован по TTL, так что это дешёвое обновление.
      const suppliers = await getWarehousePostgresSuppliers(getPrisma()).catch(() => warehouse.suppliers || []);
      mergeWarehouseProductsIntoMemory(loaded, { suppliers });
    }
  }
  const refreshed = await readWarehouse();
  const seeds = (refreshed.products || []).filter((product) => ids.includes(String(product.id)));
  if (!expandGroups) return seeds;
  if (shouldUsePostgresStorage()) {
    const groupProducts = await readWarehouseGroupSiblingsFromPostgres(seeds.length ? seeds : await readWarehouseProductsFromPostgresByIds(ids));
    if (groupProducts.length) mergeWarehouseProductsIntoMemory(groupProducts, { suppliers: refreshed.suppliers });
    const finalWarehouse = await readWarehouse();
    return expandWarehouseProductsToGroups(finalWarehouse.products || [], seeds.length ? seeds : groupProducts);
  }
  return expandWarehouseProductsToGroups(refreshed.products || [], seeds);
}

async function writeWarehouseJsonPayload(payload) {
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${warehousePath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload), "utf8");
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await fs.rename(temporaryPath, warehousePath);
      break;
    } catch (error) {
      if (attempt === 4 || !["EPERM", "EBUSY", "EACCES"].includes(error.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 80 * (attempt + 1)));
    }
  }
}

function normalizeWarehousePayload(warehouse) {
  return {
    createdAt: warehouse.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
    suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    // На postgres-стабе products — всегда частичный список. Без флага writeWarehouse
    // превращает кэш в «полный» каталог: readWarehouse отдаёт его как есть, hydrate-хелперы
    // пропускают догрузку из Postgres, и автокорзина метит все офферы вне кэша product_not_found.
    postgresOnly: shouldUsePostgresStorage() && !warehouseFullMemoryLoadEnabled,
  };
}

async function writeWarehouse(warehouse, { writePostgres = true } = {}) {
  // PLAN-HARDENING.md 1.4: invalidate AFTER the write commits (via withWarehouseMutation),
  // not before — see withWarehouseMutation in 01-bootstrap-helpers.js.
  return withWarehouseMutation(async () => {
    warehouseWritePromise = warehouseWritePromise.then(async () => {
      const closeMarker = setEventLoopBlockMarker("warehouse_write_normalize");
      let materialized;
      let payload;
      try {
        materialized = materializeYandexExportedProductsForWarehouse(warehouse);
        payload = normalizeWarehousePayload(materialized.warehouse);
      } finally {
        closeMarker();
      }
      if (materialized.added > 0) {
        logger.info("materialized yandex exported products into warehouse", { added: materialized.added });
      }
      warehouseMemoryCache = payload;
      if (writePostgres && shouldUsePostgresStorage()) {
        scheduleWarehousePostgresWrite(getPrisma(), payload);
      } else if (!shouldUsePostgresStorage()) {
        refreshWarehouseHashCache(payload);
      }
      // personal-warehouse.json (вырос до 181 МБ) — JSON-fallback: в
      // postgres-режиме loadWarehouseMemory его не читает вовсе (склад идёт из
      // Postgres), а сериализация файла блокировала event loop на 5-10 с при
      // КАЖДОМ writeWarehouse (батч реконсайлера — каждые ~40 с). Пишем файл
      // только вне postgres-режима, где он действительно источник данных.
      if (!shouldUsePostgresStorage()) {
        await writeWarehouseJsonPayload(payload);
      }
      return payload;
    });
    return warehouseWritePromise;
  });
}

async function writeWarehouseProductPatch(products = [], { reason = "warehouse_product_patch", writeLinks = true } = {}) {
  const normalizedProducts = (Array.isArray(products) ? products : [products])
    .filter((product) => product && product.id)
    .map(normalizeWarehouseProduct);
  if (!normalizedProducts.length) return null;
  // PLAN-HARDENING.md 1.4: invalidate AFTER the write commits (via withWarehouseMutation),
  // not before — see withWarehouseMutation in 01-bootstrap-helpers.js.
  return withWarehouseMutation(async () => {
  const warehouse = await readWarehouse();
  const byId = new Map(normalizedProducts.map((product) => [String(product.id), product]));
  let changed = 0;
  const productIndex = warehouseProductIndexFor(warehouse);
  if (normalizedProducts.length <= Math.max(50, Math.floor((warehouse.products || []).length / 10))) {
    for (const product of normalizedProducts) {
      const index = productIndex.get(String(product.id));
      if (index === undefined) continue;
      warehouse.products[index] = product;
      changed += 1;
    }
  } else {
    warehouse.products = (warehouse.products || []).map((product) => {
      const replacement = byId.get(String(product.id));
      if (!replacement) return product;
      changed += 1;
      return replacement;
    });
    warehouseMemoryProductIndexCache = { products: null, byId: new Map() };
  }
  if (!changed && shouldUsePostgresStorage()) {
    if (writeLinks) {
      await replaceProductLinksInPostgres(getPrisma(), normalizedProducts);
    } else {
      const chunkSize = Math.max(25, Math.min(250, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CHUNK_SIZE || 100) || 100));
      const writeConcurrency = warehousePostgresWriteConcurrency();
      for (const productChunk of chunkArray(normalizedProducts, chunkSize)) {
        await runWithLimitedConcurrency(productChunk, writeConcurrency, async (product) => {
          await upsertWarehouseProductPostgres(getPrisma(), product);
        });
        markWarehousePostgresProductsWritten(productChunk);
      }
    }
    mergeWarehouseProductsIntoMemory(normalizedProducts);
    return warehouseMemoryCache || warehouse;
  }
  if (!changed) return warehouseMemoryCache || warehouse;
  const payload = normalizeWarehousePayload(warehouse);
  warehouseMemoryCache = payload;
  if (shouldUsePostgresStorage()) {
    if (writeLinks) {
      await replaceProductLinksInPostgres(getPrisma(), normalizedProducts);
    } else {
      const chunkSize = Math.max(25, Math.min(250, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CHUNK_SIZE || 100) || 100));
      const writeConcurrency = warehousePostgresWriteConcurrency();
      for (const productChunk of chunkArray(normalizedProducts, chunkSize)) {
        await runWithLimitedConcurrency(productChunk, writeConcurrency, async (product) => {
          await upsertWarehouseProductPostgres(getPrisma(), product);
        });
        markWarehousePostgresProductsWritten(productChunk);
      }
    }
  } else {
    refreshWarehouseHashCache(payload);
  }
  writeWarehouse(payload, { writePostgres: false }).catch((error) => {
    logger.warn("warehouse product patch JSON fallback write failed", { reason, detail: error?.message || String(error) });
  });
  return payload;
  });
}

function writeWarehouseInBackground(warehouse, reason = "warehouse_background_write") {
  writeWarehouse(warehouse).catch((error) => {
    logger.error("warehouse background write failed", { reason, detail: error?.message || String(error) });
  });
}

