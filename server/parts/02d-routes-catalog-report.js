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

async function buildBrandsTnvedReport(account) {
  const brandCounts = new Map();
  const tnvedCounts = new Map();
  // brand -> Map<code, {fullValue, count}>
  const brandTnvedMap = new Map();
  let total = 0;
  let withBrand = 0;
  let withTnved = 0;

  const seenOfferIds = new Set();
  for (const visibility of ["ALL", "ARCHIVED"]) {
    let lastId = "";
    for (;;) {
      const data = await ozonRequest("/v4/product/info/attributes", {
        filter: { visibility },
        last_id: lastId,
        limit: 100,
      }, account);

      const items = Array.isArray(data.result) ? data.result : [];
      for (const item of items) {
        const offerId = cleanText(item.offer_id || item.offerId || String(item.id || ""));
        if (seenOfferIds.has(offerId)) continue;
        seenOfferIds.add(offerId);
        total++;
        const attrs = Array.isArray(item.attributes) ? item.attributes : [];

        const brandAttr = attrs.find((a) => Number(a.attribute_id || a.id) === BRAND_ATTR_ID);
        const brand = cleanText(brandAttr?.values?.[0]?.value || "");
        if (brand) {
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

          // Track brand → tnved link
          if (brand) {
            if (!brandTnvedMap.has(brand)) brandTnvedMap.set(brand, new Map());
            const codesMap = brandTnvedMap.get(brand);
            if (!codesMap.has(code)) codesMap.set(code, { code, fullValue: tnvedFull, count: 0 });
            codesMap.get(code).count++;
          }
        }
      }

      lastId = cleanText(data.last_id || "");
      if (items.length < 100 || !lastId) break;
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
    summary: {
      total,
      withBrand,
      missingBrand: total - withBrand,
      withTnved,
      missingTnved: total - withTnved,
    },
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
    const [account] = getOzonAccounts();
    if (!account) return res.status(503).json({ error: "Ozon аккаунт не настроен" });

    brandsTnvedBuildRunning = true;
    res.json({ ok: true, building: true });

    setImmediate(async () => {
      try {
        const report = await buildBrandsTnvedReport(account);
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

  const [brandRows, catRows, total] = await Promise.all([
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
    prisma.warehouseProduct.count({ where: { marketplace: "yandex" } }),
  ]);

  const withVendor = brandRows.filter((r) => r.vendor).reduce((s, r) => s + Number(r.count), 0);

  // TN VED: бэкфилл ЯМ отправляет код в Yandex API без сохранения локально.
  // Используем настройки: если код задан — он применён ко всем товарам.
  const settings = await readAppSettings().catch(() => null);
  const tnvedCodeConfigured = cleanText(settings?.tnved?.code || "");
  const totalNum = Number(total);
  const withTnved = tnvedCodeConfigured ? totalNum : 0;

  // Имена категорий: сначала из raw.yandex.marketCategoryName (если сохранено),
  // затем добираем недостающие через API пакетами по 5 с задержкой.
  const catNameMap = new Map();
  const catIdsToFetch = [];
  for (const row of catRows) {
    if (!row.cat_id) continue;
    const storedName = cleanText(row.cat_name || "");
    if (storedName) {
      catNameMap.set(row.cat_id, storedName);
    } else {
      catIdsToFetch.push(row.cat_id);
    }
  }

  const [shop] = getYandexShops();
  if (shop && catIdsToFetch.length) {
    const BATCH = 5;
    for (let i = 0; i < catIdsToFetch.length; i += BATCH) {
      const chunk = catIdsToFetch.slice(i, i + BATCH);
      await Promise.allSettled(
        chunk.map(async (catId) => {
          try {
            const data = await yandexRequest(shop, "GET", `/v2/categories/${catId}`, undefined);
            const name = cleanText(data?.result?.name || data?.category?.name || data?.name || "");
            if (name) catNameMap.set(catId, name);
          } catch {
            // category name is optional — don't fail the report
          }
        }),
      );
      if (i + BATCH < catIdsToFetch.length) await sleep(300);
    }
  }

  return {
    summary: {
      total: totalNum,
      withVendor,
      missingVendor: totalNum - withVendor,
      withTnved,
      missingTnved: totalNum - withTnved,
    },
    brands: brandRows.filter((r) => r.vendor).map((r) => ({ brand: String(r.vendor), count: Number(r.count) })),
    categories: catRows.filter((r) => r.cat_id).map((r) => ({
      catId: String(r.cat_id),
      count: Number(r.count),
      catName: catNameMap.get(r.cat_id) || "",
    })),
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
