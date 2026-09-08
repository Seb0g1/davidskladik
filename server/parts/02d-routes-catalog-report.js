// Отчёт «Бренды / ТН ВЭД» — агрегация по атрибутам Ozon
// Бренд: attribute_id=85, ТН ВЭД: attribute_id=22232
// Данные кешируются 24 ч в data/brands-tnved-cache.json

const brandsTnvedCachePath = path.join(dataDir, "brands-tnved-cache.json");
const BRANDS_TNVED_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const BRAND_ATTR_ID = 85;
const TNVED_ATTR_ID = 22232;

let brandsTnvedBuildRunning = false;

async function readBrandsTnvedCache() {
  try {
    const raw = await fs.readFile(brandsTnvedCachePath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function writeBrandsTnvedCache(data) {
  await fs.writeFile(brandsTnvedCachePath, JSON.stringify(data));
}

async function buildBrandsTnvedReport(accounts) {
  const brandCounts = new Map();
  const tnvedCounts = new Map();
  const brandTnvedMap = new Map();
  let total = 0;
  let withBrand = 0;
  let withTnved = 0;
  const seenOfferIds = new Set();

  for (const account of accounts) {
    // Step 1: collect all offer_ids via /v3/product/list (reliable cursor pagination)
    const allOfferIds = [];
    for (const visibility of ["ALL", "ARCHIVED"]) {
      let lastId = "";
      for (;;) {
        let data;
        try {
          data = await ozonRequest("/v3/product/list", {
            filter: { visibility }, last_id: lastId, limit: 1000,
          }, account);
        } catch (err) {
          logger.warn("brands-tnved list interrupted", { account: account.id, visibility, detail: err?.message });
          break;
        }
        const items = (data.result && data.result.items) || [];
        for (const i of items) {
          const oid = cleanText(i.offer_id || "");
          if (oid) allOfferIds.push(oid);
        }
        lastId = cleanText((data.result && data.result.last_id) || "");
        if (items.length < 1000 || !lastId) break;
      }
    }
    logger.info("brands-tnved offer_ids collected", { account: account.id, count: allOfferIds.length });

    // Step 2: fetch attributes by offer_id chunks (no cursor = no stale-cursor bug)
    for (const chunk of chunkArray(allOfferIds, 100)) {
      let data;
      try {
        data = await ozonRequest("/v4/product/info/attributes", {
          filter: { offer_id: chunk, visibility: "ALL" },
          limit: 100, sort_by: "id", sort_dir: "asc",
        }, account);
      } catch (err) {
        logger.warn("brands-tnved attrs chunk error", { account: account.id, detail: err?.message });
        continue;
      }
      for (const item of (data.result || [])) {
        const offerId = cleanText(item.offer_id || String(item.id || ""));
        if (!offerId || seenOfferIds.has(offerId)) continue;
        seenOfferIds.add(offerId);
        total++;
        const attrs = Array.isArray(item.attributes) ? item.attributes : [];

        const brandAttr = attrs.find((a) => Number(a.attribute_id || a.id) === BRAND_ATTR_ID);
        const brand = cleanText(brandAttr?.values?.[0]?.value || "");
        if (brand && !isBrandGarbageValue(brand)) {
          withBrand++;
          if (!brandCounts.has(brand)) brandCounts.set(brand, { brand, count: 0, sample: [] });
          const entry = brandCounts.get(brand);
          entry.count++;
          if (entry.sample.length < 3) entry.sample.push(offerId);
        }

        const tnvedAttr = attrs.find((a) => Number(a.attribute_id || a.id) === TNVED_ATTR_ID);
        const tnvedFull = cleanText(tnvedAttr?.values?.[0]?.value || "");
        if (tnvedFull) {
          withTnved++;
          const code = tnvedFull.match(/^(\d{7,10})/)?.[1] || tnvedFull.slice(0, 10);
          if (!tnvedCounts.has(code)) tnvedCounts.set(code, { code, fullValue: tnvedFull, count: 0, sample: [] });
          const entry = tnvedCounts.get(code);
          entry.count++;
          if (entry.sample.length < 3) entry.sample.push(offerId);

          if (brand && !isBrandGarbageValue(brand)) {
            if (!brandTnvedMap.has(brand)) brandTnvedMap.set(brand, new Map());
            const codesMap = brandTnvedMap.get(brand);
            if (!codesMap.has(code)) codesMap.set(code, { code, fullValue: tnvedFull, count: 0 });
            codesMap.get(code).count++;
          }
        }
      }
    }
  }

  const brands = [...brandCounts.values()].sort((a, b) => b.count - a.count);
  const tnveds = [...tnvedCounts.values()].sort((a, b) => b.count - a.count);
  const brandTnveds = [...brandTnvedMap.entries()]
    .map(([brand, codesMap]) => ({
      brand,
      totalCount: brandCounts.get(brand)?.count || 0,
      tnvedCodes: [...codesMap.values()].sort((a, b) => b.count - a.count),
    }))
    .sort((a, b) => a.brand.localeCompare(b.brand, "ru"));

  return {
    summary: { total, withBrand, missingBrand: total - withBrand, withTnved, missingTnved: total - withTnved },
    brands,
    tnveds,
    brandTnveds,
    cachedAt: new Date().toISOString(),
  };
}

app.get("/api/catalog/brands-tnved", requireAdmin, async (req, res, next) => {
  try {
    if (brandsTnvedBuildRunning) {
      return res.json({ building: true, cachedAt: null });
    }
    const cached = await readBrandsTnvedCache();
    if (cached) {
      const age = Date.now() - new Date(cached.cachedAt).getTime();
      return res.json({ ...cached, fromCache: true, stale: age > BRANDS_TNVED_CACHE_TTL_MS });
    }
    res.json({ noData: true, building: false });
  } catch (err) {
    next(err);
  }
});

app.post("/api/catalog/brands-tnved/refresh", requireAdmin, async (req, res, next) => {
  try {
    if (brandsTnvedBuildRunning) {
      return res.json({ ok: true, building: true, alreadyRunning: true });
    }
    const accounts = getOzonAccounts({ includeSyncDisabled: true });
    if (!accounts.length) return res.status(503).json({ error: "Ozon аккаунт не настроен" });

    brandsTnvedBuildRunning = true;
    res.json({ ok: true, building: true });

    setImmediate(async () => {
      try {
        const report = await buildBrandsTnvedReport(accounts);
        await writeBrandsTnvedCache(report);
        logger.info("brands-tnved report built", {
          total: report.summary.total,
          brands: report.brands.length,
          tnveds: report.tnveds.length,
        });
      } catch (err) {
        logger.warn("brands-tnved report build failed", { detail: err?.message || String(err) });
      } finally {
        brandsTnvedBuildRunning = false;
      }
    });
  } catch (err) {
    next(err);
  }
});

// ─── Excel export: brand → TN VED codes ──────────────────────────────────────

app.get("/api/catalog/brands-tnved/export-excel", requireAdmin, async (req, res, next) => {
  try {
    const cached = await readBrandsTnvedCache();
    if (!cached || !cached.brandTnveds) {
      return res.status(404).json({ error: "Данные ещё не загружены. Нажмите «Обновить данные Ozon» на странице Бренды / ТН ВЭД." });
    }

    const ExcelJS = require("exceljs");
    const workbook = new ExcelJS.Workbook();
    workbook.creator = "Magic Vibes Склад";
    workbook.created = new Date();

    const sheet = workbook.addWorksheet("Бренды ТН ВЭД", {
      views: [{ state: "frozen", ySplit: 1 }],
    });

    sheet.columns = [
      { header: "Бренд", key: "brand", width: 36 },
      { header: "Код ТН ВЭД", key: "code", width: 16 },
      { header: "Описание категории", key: "description", width: 50 },
      { header: "SKU с кодом", key: "count", width: 14 },
    ];

    // Bold header row
    const headerRow = sheet.getRow(1);
    headerRow.font = { bold: true };
    headerRow.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFD9E1F2" } };
    headerRow.alignment = { vertical: "middle" };
    headerRow.height = 18;

    const brandTnveds = cached.brandTnveds;
    for (const entry of brandTnveds) {
      if (!entry.tnvedCodes || entry.tnvedCodes.length === 0) {
        // Brand exists but no tnved code assigned
        sheet.addRow({
          brand: entry.brand,
          code: "",
          description: "— код не назначен —",
          count: entry.totalCount,
        });
        continue;
      }
      for (let i = 0; i < entry.tnvedCodes.length; i++) {
        const tc = entry.tnvedCodes[i];
        const description = tc.fullValue.replace(/^\d+\s*[-–]\s*/, "");
        sheet.addRow({
          brand: i === 0 ? entry.brand : "",
          code: tc.code,
          description,
          count: tc.count,
        });
      }
    }

    // Style data rows: alternate fill, monospace for code column
    sheet.eachRow((row, rowNumber) => {
      if (rowNumber === 1) return;
      const fill = rowNumber % 2 === 0
        ? { type: "pattern", pattern: "solid", fgColor: { argb: "FFF7F7F7" } }
        : undefined;
      row.eachCell({ includeEmpty: true }, (cell, colNum) => {
        if (fill) cell.fill = fill;
        if (colNum === 2) cell.font = { name: "Courier New", size: 10 }; // ТН ВЭД code
        if (colNum === 4) cell.alignment = { horizontal: "right" };
        cell.border = {
          bottom: { style: "thin", color: { argb: "FFE0E0E0" } },
        };
      });
    });

    // Autofilter on headers
    sheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: 4 } };

    const dateStr = new Date().toISOString().slice(0, 10);
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''tnved-brands-${dateStr}.xlsx`);

    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    next(err);
  }
});

// ─── Yandex brands report (from local PostgreSQL) ────────────────────────────

const brandsYandexCachePath = path.join(dataDir, "brands-yandex-cache.json");
const BRANDS_YANDEX_CACHE_TTL_MS = 60 * 60 * 1000;

async function readBrandsYandexCache() {
  try {
    return JSON.parse(await fs.readFile(brandsYandexCachePath, "utf8"));
  } catch {
    return null;
  }
}

async function buildBrandsYandexReport() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("БД недоступна");

  const [brandRows, catRows, brandCatRows, total] = await Promise.all([
    prisma.$queryRawUnsafe(`
      SELECT NULLIF(TRIM(raw->'yandex'->>'vendor'), '') AS vendor, COUNT(*)::int AS count
      FROM warehouse_products WHERE marketplace = 'yandex'
      GROUP BY vendor ORDER BY count DESC
    `),
    prisma.$queryRawUnsafe(`
      SELECT
        NULLIF(TRIM(raw->'yandex'->>'marketCategoryId'), '') AS cat_id,
        NULLIF(TRIM(raw->'yandex'->>'marketCategoryName'), '') AS cat_name,
        COUNT(*)::int AS count
      FROM warehouse_products WHERE marketplace = 'yandex'
      GROUP BY cat_id, cat_name ORDER BY count DESC
    `),
    prisma.$queryRawUnsafe(`
      SELECT
        NULLIF(TRIM(raw->'yandex'->>'vendor'), '') AS vendor,
        NULLIF(TRIM(raw->'yandex'->>'marketCategoryId'), '') AS cat_id,
        COUNT(*)::int AS count
      FROM warehouse_products WHERE marketplace = 'yandex'
      GROUP BY vendor, cat_id ORDER BY count DESC
    `),
    prisma.warehouseProduct.count({ where: { marketplace: "yandex" } }),
  ]);

  const withVendor = brandRows.filter((r) => r.vendor && !isBrandGarbageValue(r.vendor)).reduce((s, r) => s + Number(r.count), 0);

  const settings = await readAppSettings().catch(() => null);
  const tnvedCodeConfigured = cleanText(settings?.tnved?.code || "");
  const totalNum = Number(total);
  const withTnved = tnvedCodeConfigured ? totalNum : 0;

  // Category names: first from stored raw.yandex.marketCategoryName,
  // then resolve missing via offer-mappings pagination (stops early when all resolved).
  // The /v2/categories/{id} endpoint is not available in the Partner API.
  const catNameMap = new Map();
  const catIdsToFetch = new Set();
  for (const row of catRows) {
    if (!row.cat_id) continue;
    const storedName = cleanText(row.cat_name || "");
    if (storedName) catNameMap.set(row.cat_id, storedName);
    else catIdsToFetch.add(row.cat_id);
  }

  const [shop] = getYandexShops();
  if (shop && catIdsToFetch.size > 0) {
    try {
      const remaining = new Set(catIdsToFetch);
      let pageToken = "";
      do {
        const params = new URLSearchParams({ limit: "100" });
        if (pageToken) params.set("pageToken", pageToken);
        const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-mappings?${params}`, undefined);
        for (const item of (data.result?.offerMappings || [])) {
          const catId = String(item.mapping?.marketCategoryId || "");
          const catName = cleanText(item.mapping?.marketCategoryName || "");
          if (catId && catName && remaining.has(catId)) {
            catNameMap.set(catId, catName);
            remaining.delete(catId);
          }
        }
        pageToken = data.result?.paging?.nextPageToken || "";
        if (remaining.size === 0) break;
      } while (pageToken);
    } catch {
      // category names are optional
    }
  }

  // Build brand → categories map for Excel export
  const brandCatMap = new Map();
  for (const row of brandCatRows) {
    if (!row.vendor || !row.cat_id || isBrandGarbageValue(row.vendor)) continue;
    if (!brandCatMap.has(row.vendor)) brandCatMap.set(row.vendor, []);
    brandCatMap.get(row.vendor).push({ catId: String(row.cat_id), catName: catNameMap.get(row.cat_id) || "", count: Number(row.count) });
  }

  const brandCountMap = new Map(brandRows.filter((r) => r.vendor).map((r) => [String(r.vendor), Number(r.count)]));
  const brandCategories = [...brandCatMap.entries()]
    .map(([brand, cats]) => ({
      brand,
      totalCount: brandCountMap.get(brand) || 0,
      categories: cats.sort((a, b) => b.count - a.count),
    }))
    .sort((a, b) => a.brand.localeCompare(b.brand, "ru"));

  return {
    summary: {
      total: totalNum,
      withVendor,
      missingVendor: totalNum - withVendor,
      withTnved,
      missingTnved: totalNum - withTnved,
    },
    brands: brandRows.filter((r) => r.vendor && !isBrandGarbageValue(r.vendor)).map((r) => ({ brand: String(r.vendor), count: Number(r.count) })),
    categories: catRows.filter((r) => r.cat_id).map((r) => ({
      catId: String(r.cat_id),
      count: Number(r.count),
      catName: catNameMap.get(r.cat_id) || "",
    })),
    brandCategories,
    cachedAt: new Date().toISOString(),
  };
}

app.get("/api/catalog/brands-tnved/yandex", requireAdmin, async (req, res, next) => {
  try {
    const cached = await readBrandsYandexCache();
    if (cached) {
      const age = Date.now() - new Date(cached.cachedAt).getTime();
      return res.json({ ...cached, fromCache: true, stale: age > BRANDS_YANDEX_CACHE_TTL_MS });
    }
    const report = await buildBrandsYandexReport();
    await fs.writeFile(brandsYandexCachePath, JSON.stringify(report));
    res.json({ ...report, fromCache: false, stale: false });
  } catch (err) {
    next(err);
  }
});

app.post("/api/catalog/brands-tnved/yandex/refresh", requireAdmin, async (req, res, next) => {
  try {
    const report = await buildBrandsYandexReport();
    await fs.writeFile(brandsYandexCachePath, JSON.stringify(report));
    res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

// ─── Excel export: Yandex brands → categories ────────────────────────────────

app.get("/api/catalog/brands-tnved/yandex/export-excel", requireAdmin, async (req, res, next) => {
  try {
    const cached = await readBrandsYandexCache();
    if (!cached?.brandCategories) {
      return res.status(404).json({ error: "Данные ещё не загружены или устарели. Нажмите «Обновить данные Яндекс»." });
    }

    const ExcelJS = require("exceljs");
    const workbook = new ExcelJS.Workbook();
    workbook.creator = "Magic Vibes Склад";
    workbook.created = new Date();

    const sheet = workbook.addWorksheet("Бренды Яндекс", { views: [{ state: "frozen", ySplit: 1 }] });

    sheet.columns = [
      { header: "Бренд", key: "brand", width: 36 },
      { header: "Категория ЯМ", key: "catName", width: 44 },
      { header: "ID категории", key: "catId", width: 14 },
      { header: "SKU", key: "count", width: 10 },
    ];

    const headerRow = sheet.getRow(1);
    headerRow.font = { bold: true };
    headerRow.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFE2EFDA" } };
    headerRow.alignment = { vertical: "middle" };
    headerRow.height = 18;

    for (const entry of cached.brandCategories) {
      if (!entry.categories || entry.categories.length === 0) {
        sheet.addRow({ brand: entry.brand, catName: "— категория не определена —", catId: "", count: entry.totalCount });
        continue;
      }
      for (let i = 0; i < entry.categories.length; i++) {
        const cat = entry.categories[i];
        sheet.addRow({
          brand: i === 0 ? entry.brand : "",
          catName: cat.catName || `— ID: ${cat.catId} —`,
          catId: cat.catId,
          count: cat.count,
        });
      }
    }

    sheet.eachRow((row, rowNumber) => {
      if (rowNumber === 1) return;
      const fill = rowNumber % 2 === 0
        ? { type: "pattern", pattern: "solid", fgColor: { argb: "FFF7F7F7" } }
        : undefined;
      row.eachCell({ includeEmpty: true }, (cell, colNum) => {
        if (fill) cell.fill = fill;
        if (colNum === 3) cell.font = { name: "Courier New", size: 10 };
        if (colNum === 4) cell.alignment = { horizontal: "right" };
        cell.border = { bottom: { style: "thin", color: { argb: "FFE0E0E0" } } };
      });
    });

    sheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: 4 } };

    const dateStr = new Date().toISOString().slice(0, 10);
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''yandex-brands-${dateStr}.xlsx`);
    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    next(err);
  }
});

// ─── Excel export: combined Ozon + Yandex brands ─────────────────────────────

app.get("/api/catalog/brands-tnved/combined/export-excel", requireAdmin, async (req, res, next) => {
  try {
    const [ozonCached, yandexCached] = await Promise.all([readBrandsTnvedCache(), readBrandsYandexCache()]);

    if (!ozonCached?.brandTnveds) {
      return res.status(404).json({ error: "Нет данных Ozon. Нажмите «Обновить данные Ozon» на вкладке Ozon." });
    }
    if (!yandexCached?.brands) {
      return res.status(404).json({ error: "Нет данных Яндекс. Нажмите «Обновить данные Яндекс» на вкладке Яндекс." });
    }

    // Build Yandex brand count map (lowercase key for case-insensitive join)
    const yandexMap = new Map();
    for (const b of yandexCached.brands) {
      yandexMap.set(b.brand.toLowerCase().trim(), Number(b.count));
    }

    const matchedYandexKeys = new Set();
    const rows = [];

    for (const entry of ozonCached.brandTnveds) {
      if (isBrandGarbageValue(entry.brand)) continue;
      const key = entry.brand.toLowerCase().trim();
      const yandexCount = yandexMap.get(key) || 0;
      if (yandexCount > 0) matchedYandexKeys.add(key);

      if (!entry.tnvedCodes || entry.tnvedCodes.length === 0) {
        rows.push({ brand: entry.brand, code: "", description: "— код не назначен —", ozonCount: entry.totalCount, yandexCount });
      } else {
        const tc = entry.tnvedCodes[0]; // primary (most common) ТН ВЭД code
        rows.push({ brand: entry.brand, code: tc.code, description: tc.fullValue.replace(/^\d+\s*[-–]\s*/, ""), ozonCount: entry.totalCount, yandexCount });
      }
    }

    // Yandex-only brands (not in Ozon)
    for (const b of yandexCached.brands) {
      if (isBrandGarbageValue(b.brand)) continue;
      if (!matchedYandexKeys.has(b.brand.toLowerCase().trim())) {
        rows.push({ brand: b.brand, code: "", description: "— только Яндекс —", ozonCount: 0, yandexCount: Number(b.count) });
      }
    }

    rows.sort((a, b) => a.brand.localeCompare(b.brand, "ru"));

    const ExcelJS = require("exceljs");
    const workbook = new ExcelJS.Workbook();
    workbook.creator = "Magic Vibes Склад";
    workbook.created = new Date();

    const sheet = workbook.addWorksheet("Бренды Ozon+Яндекс", { views: [{ state: "frozen", ySplit: 1 }] });

    sheet.columns = [
      { header: "Бренд", key: "brand", width: 36 },
      { header: "Код ТН ВЭД", key: "code", width: 16 },
      { header: "Описание ТН ВЭД", key: "description", width: 46 },
      { header: "SKU Ozon", key: "ozonCount", width: 12 },
      { header: "SKU Яндекс", key: "yandexCount", width: 12 },
    ];

    const headerRow = sheet.getRow(1);
    headerRow.font = { bold: true };
    headerRow.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFCE4D6" } };
    headerRow.alignment = { vertical: "middle" };
    headerRow.height = 18;

    for (const row of rows) {
      sheet.addRow(row);
    }

    sheet.eachRow((row, rowNumber) => {
      if (rowNumber === 1) return;
      const fill = rowNumber % 2 === 0
        ? { type: "pattern", pattern: "solid", fgColor: { argb: "FFF7F7F7" } }
        : undefined;
      row.eachCell({ includeEmpty: true }, (cell, colNum) => {
        if (fill) cell.fill = fill;
        if (colNum === 2) cell.font = { name: "Courier New", size: 10 };
        if (colNum === 4 || colNum === 5) cell.alignment = { horizontal: "right" };
        cell.border = { bottom: { style: "thin", color: { argb: "FFE0E0E0" } } };
      });
    });

    sheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: 5 } };

    const dateStr = new Date().toISOString().slice(0, 10);
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''combined-brands-${dateStr}.xlsx`);
    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    next(err);
  }
});
