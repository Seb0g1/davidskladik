async function getWarehouseBrandListFromPostgres(prisma) {
  if (
    warehouseBrandListCache
    && Date.now() - warehouseBrandListCache.at < warehouseBrandListCacheTtlMs
  ) {
    return warehouseBrandListCache.value.slice();
  }
  if (prisma?.brandIndexItem) {
    try {
      let rows = await prisma.brandIndexItem.findMany({
        select: { normalizedBrand: true, displayBrand: true },
        distinct: ["normalizedBrand"],
        orderBy: { displayBrand: "asc" },
      });
      if (rows.length) {
        const brands = rows.map((row) => cleanText(row.displayBrand)).filter(Boolean).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
        warehouseBrandListCache = { at: Date.now(), value: brands };
        return brands.slice();
      }
    } catch (error) {
      rememberStateWarning("warehouse_brand_index_list", error);
      logger.warn("warehouse brand index list failed, using product brands", { detail: error?.message || String(error) });
    }
  }
  const rows = await prisma.warehouseProduct.findMany({
    where: {
      AND: [
        enabledWarehouseTargetWhere(),
        { brand: { not: null } },
        { brand: { not: "" } },
      ],
    },
    select: {
      brand: true,
    },
    distinct: ["brand"],
    orderBy: { brand: "asc" },
  });
  const unique = new Map();
  for (const row of rows) {
    const brand = cleanText(row.brand);
    if (!brand) continue;
    const key = brand.toLowerCase();
    if (!unique.has(key)) unique.set(key, brand);
  }
  const brands = Array.from(unique.values()).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
  warehouseBrandListCache = { at: Date.now(), value: brands };
  return brands.slice();
}

function resolveWarehouseBrandFromPostgresRow(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  // Keep `raw` as an explicit field so resolveWarehouseBrandCandidates can access
  // product.raw?.vendor / product.raw?.brand and extractBrandFromNestedAttributes(product.raw).
  // Without it, { ...raw } spreads the contents but product.raw becomes raw.raw (undefined).
  return resolveWarehouseBrand({
    ...raw,
    raw,
    id: row.id,
    marketplace: row.marketplace,
    target: row.target,
    offerId: row.offerId,
    productId: row.productId,
    name: row.name || raw.name,
    brand: row.brand || raw.brand,
  });
}

async function ensureWarehousePostgresBrandsBackfilled(prisma, { force = false } = {}) {
  if (!prisma) return { updated: 0, scanned: 0, skipped: true };
  if (warehousePostgresBrandBackfillDone && !force) return { updated: 0, scanned: 0, skipped: true };
  if (warehousePostgresBrandBackfillPromise && !force) return warehousePostgresBrandBackfillPromise;
  const maxRows = Math.max(1000, Math.min(100000, Number(process.env.WAREHOUSE_BRAND_BACKFILL_LIMIT || 50000) || 50000));
  warehousePostgresBrandBackfillPromise = (async () => {
    const rows = await prisma.warehouseProduct.findMany({
      where: {
        AND: [
          enabledWarehouseTargetWhere(),
          force ? {} : {
            OR: [
              { brand: null },
              { brand: "" },
            ],
          },
        ].filter((item) => Object.keys(item || {}).length),
      },
      select: { id: true, name: true, brand: true, raw: true, marketplace: true, target: true, offerId: true, productId: true },
      take: maxRows,
      orderBy: [{ updatedAt: "desc" }],
    });
    let updated = 0;
    const updates = [];
    for (const row of rows) {
      const brand = cleanText(resolveWarehouseBrandFromPostgresRow(row));
      if (!brand) continue;
      if (cleanText(row.brand).toLowerCase() === brand.toLowerCase()) continue;
      updates.push({ id: row.id, brand });
    }
    for (const batch of chunkArray(updates, 250)) {
      await runWithLimitedConcurrency(batch, 2, async (item) => {
        await prisma.warehouseProduct.update({
          where: { id: item.id },
          data: { brand: item.brand },
        });
        updated += 1;
      });
    }
    warehousePostgresBrandBackfillDone = rows.length < maxRows;
    warehouseBrandListCache = null;
    if (updated || rows.length) {
      logger.info("warehouse postgres brands backfilled from raw", {
        scanned: rows.length,
        updated,
        complete: warehousePostgresBrandBackfillDone,
      });
    }
    // If brand column was updated for any products, rebuild the brand index in the background
    // so the new brands appear in the dropdown and filter immediately, without waiting for the
    // next maintenance cycle.
    if (updated > 0) {
      rebuildWarehouseBrandIndexPostgres(prisma).catch((error) => {
        logger.warn("brand index rebuild after backfill failed", { detail: error?.message || String(error) });
      });
    }
    return { updated, scanned: rows.length, skipped: false, complete: warehousePostgresBrandBackfillDone };
  })().catch((error) => {
    logger.warn("warehouse postgres brand backfill failed", { detail: error?.message || String(error) });
    return { updated: 0, scanned: 0, skipped: false, error: error?.message || String(error) };
  }).finally(() => {
    warehousePostgresBrandBackfillPromise = null;
  });
  return warehousePostgresBrandBackfillPromise;
}


