// Repair missing brand (vendor) and weight/dimensions for existing Yandex products.
// Finds products where yandex.vendor is empty and/or weightDimensions are missing,
// resolves them from Ozon sibling data or name extraction, and re-sends content to Yandex.
app.post("/api/ozon-yandex-import/repair-yandex-content", requireAdmin, async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun !== false;
    const requestedLimit = Number(request.body?.limit || 5000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 5000));
    const repairVendor = request.body?.repairVendor !== false;
    const repairDimensions = request.body?.repairDimensions !== false;

    // Page through Postgres directly — readWarehouse() does not return products in PG mode.
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "Postgres недоступен." });
    const yandexProducts = [];
    let cursorId = null;
    while (yandexProducts.length < limit) {
      const page = await prisma.warehouseProduct.findMany({
        where: { marketplace: "yandex" },
        select: { id: true, raw: true },
        orderBy: { id: "asc" },
        take: 1000,
        ...(cursorId ? { cursor: { id: cursorId }, skip: 1 } : {}),
      });
      if (!page.length) break;
      cursorId = page[page.length - 1].id;
      for (const row of page) {
        const product = row.raw && typeof row.raw === "object" ? row.raw : null;
        if (product && cleanText(product.offerId)) yandexProducts.push(product);
        if (yandexProducts.length >= limit) break;
      }
      if (page.length < 1000) break;
    }

    const repairPictures = request.body?.repairPictures !== false;
    const candidates = yandexProducts.filter((product) => {
      const normalized = normalizeWarehouseProduct(product);
      const missingVendor = repairVendor && !cleanText(normalized.yandex?.vendor);
      const dims = normalized.yandex?.extra?.weightDimensions;
      const missingDims = repairDimensions && !(Number(dims?.length) > 0 && Number(dims?.weight) > 0);
      const missingPictures = repairPictures
        && !splitList(normalized.yandex?.pictures).length
        && !splitList(normalized.yandex?.images).length;
      return missingVendor || missingDims || missingPictures;
    });

    if (dryRun) {
      return response.json({
        ok: true,
        dryRun: true,
        total: yandexProducts.length,
        candidates: candidates.length,
        sample: candidates.slice(0, 20).map((product) => {
          const normalized = normalizeWarehouseProduct(product);
          const built = buildYandexOfferMapping(normalized);
          return {
            id: product.id,
            offerId: product.offerId,
            name: normalized.name,
            resolvedVendor: built.offer?.vendor || "",
            hasDimensions: Boolean(built.offer?.weightDimensions),
          };
        }),
      });
    }

    const shops = uniqueYandexShopsByBusiness ? uniqueYandexShopsByBusiness() : getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
    if (!shops.length) return response.status(400).json({ error: "Yandex Market не настроен." });

    const shopByTarget = new Map(shops.map((shop) => [shop.id, shop]));
    const batchByShop = new Map();
    let skipped = 0;

    // Photo fallback: yandex rows created from exports often have no images of their own —
    // borrow them from the Ozon sibling with the same offerId (images is a light column).
    const ozonImagesByOfferId = new Map();
    {
      let ozonCursor = null;
      while (true) {
        const page = await prisma.warehouseProduct.findMany({
          where: { marketplace: "ozon" },
          select: { id: true, offerId: true, images: true },
          orderBy: { id: "asc" },
          take: 2000,
          ...(ozonCursor ? { cursor: { id: ozonCursor }, skip: 1 } : {}),
        });
        if (!page.length) break;
        ozonCursor = page[page.length - 1].id;
        for (const row of page) {
          const key = cleanText(row.offerId).toLowerCase();
          const images = Array.isArray(row.images) ? row.images.filter(Boolean) : [];
          if (key && images.length && !ozonImagesByOfferId.has(key)) ozonImagesByOfferId.set(key, images);
        }
        if (page.length < 2000) break;
      }
    }

    for (const product of candidates) {
      const normalized = normalizeWarehouseProduct(product);
      const ownPictures = [
        cleanText(normalized.imageUrl),
        ...splitList(normalized.yandex?.pictures),
        ...splitList(normalized.yandex?.images),
      ].filter(Boolean);
      const fallbackPictures = ownPictures.length
        ? []
        : (ozonImagesByOfferId.get(cleanText(normalized.offerId).toLowerCase()) || []);
      const built = buildYandexOfferMapping(
        normalized,
        fallbackPictures.length ? { yandex: { pictures: fallbackPictures } } : {},
      );
      const vendor = cleanText(built.offer?.vendor);
      if (!vendor || vendor === "Без бренда") {
        skipped += 1;
        continue;
      }
      const target = cleanText(normalized.target);
      const shop = shopByTarget.get(target) || shops.find((s) => matchesYandexTarget(target, s.id));
      if (!shop) {
        skipped += 1;
        continue;
      }
      if (!batchByShop.has(shop.id)) batchByShop.set(shop.id, { shop, offers: [] });
      // Send the FULL built offer: pictures, barcodes, marketCategoryId and basicPrice
      // included — sending a partial payload left created cards without photos and price.
      batchByShop.get(shop.id).offers.push(built.offer);
    }

    const results = [];
    for (const { shop, offers } of batchByShop.values()) {
      const sent = await sendYandexOfferMappings(shop, offers);
      results.push(...sent.map((item) => ({ ...item, target: shop.id })));
    }

    const sentCount = results.filter((item) => item.ok).length;
    const failedCount = results.filter((item) => !item.ok).length;
    response.json({
      ok: failedCount === 0,
      total: yandexProducts.length,
      candidates: candidates.length,
      skipped,
      sent: sentCount,
      failed: failedCount,
      errors: results.filter((item) => !item.ok).slice(0, 50),
    });
  } catch (error) {
    next(error);
  }
});

// --- Исправление категорий карточек Яндекса ---
// Карточки создавались без marketCategoryId, Яндекс подбирал категорию по
// названию и часто промахивался: «Костюмы спортивные детские» у набора духов,
// «Вина игристые» у парфюмерного сета. Отсюда требования чужих обязательных
// полей («Пол», «Размерная сетка»). Категории явно не из бьюти-сегмента
// переотправляются с правильным marketCategoryId; id целевой категории берётся
// из карточек, где уже лежит большинство таких товаров (без хардкода id).

const YANDEX_WRONG_CATEGORY_PATTERNS = [
  /сумк/i, /костюм/i, /юбк/i, /худи/i, /игрушк/i, /(^|\s)вина(\s|$)/i, /аквариум/i,
  /автомобильн/i, /диффузор/i, /одежд/i, /обув/i, /плать/i, /футболк/i,
  /брюк/i, /куртк/i, /носк/i, /детск/i, /спортивн/i, /канцеляр/i, /посуд/i,
];

function isWrongYandexBeautyCategory(categoryName = "") {
  const name = cleanText(categoryName);
  if (!name) return false;
  return YANDEX_WRONG_CATEGORY_PATTERNS.some((pattern) => pattern.test(name));
}

// Целевая категория по названию товара. Пустая строка = не уверены, не трогаем.
// Свечи/диффузоры/ароматизаторы пропускаем: для них «Ароматические диффузоры»
// и «Декоративные свечи» могут быть верными.
function resolveYandexTargetCategoryName(productName = "") {
  const text = cleanText(productName).toLowerCase().replace(/ё/g, "е");
  if (!text) return "";
  if (/свеч|диффузор|ароматизатор|косметичк/.test(text)) return "";
  if (/дезодорант|deodorant/.test(text)) return "Дезодоранты";
  if (/шампунь|shampoo/.test(text)) return "Шампуни";
  if (/для душа|shower gel|body cleanser/.test(text)) return "Для душа";
  if (/парфюм|духи|туалетн|eau de|edp|edt|parfum|аромат|discovery/.test(text)) return "Парфюмерия";
  return "";
}

app.post("/api/ozon-yandex-import/fix-yandex-categories", requireAdmin, async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun !== false;
    const withErrorsReport = request.body?.errorsReport !== false;
    const shops = uniqueYandexShopsByBusiness
      ? uniqueYandexShopsByBusiness()
      : getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
    if (!shops.length) return response.status(400).json({ error: "Yandex Market не настроен." });

    const errorCounts = new Map();
    const warningCounts = new Map();
    const planned = [];
    const results = [];
    let totalOffers = 0;
    let cardsWithErrors = 0;

    for (const shop of shops) {
      const mappings = await getYandexOfferMappings(shop);
      const rows = mappings.map((item) => ({
        offerId: cleanText(item.offer?.offerId || item.offerId),
        name: cleanText(item.offer?.name),
        categoryId: Number(item.mapping?.marketCategoryId || item.offer?.marketCategoryId || 0) || 0,
        categoryName: cleanText(item.mapping?.marketCategoryName || item.offer?.marketCategoryName),
      })).filter((row) => row.offerId);
      totalOffers += rows.length;

      // Гистограмма id по имени категории: у правильных категорий большинство.
      const idsByCategoryName = new Map();
      for (const row of rows) {
        if (!row.categoryId || !row.categoryName) continue;
        const key = row.categoryName.toLowerCase();
        const counter = idsByCategoryName.get(key) || new Map();
        counter.set(row.categoryId, (counter.get(row.categoryId) || 0) + 1);
        idsByCategoryName.set(key, counter);
      }
      const majorityCategoryId = (name) => {
        const counter = idsByCategoryName.get(cleanText(name).toLowerCase());
        if (!counter || !counter.size) return 0;
        return [...counter.entries()].sort((a, b) => b[1] - a[1])[0][0];
      };

      for (const row of rows) {
        if (!isWrongYandexBeautyCategory(row.categoryName)) continue;
        const targetName = resolveYandexTargetCategoryName(row.name);
        if (!targetName) continue;
        const targetId = majorityCategoryId(targetName);
        if (!targetId || targetId === row.categoryId) continue;
        planned.push({
          shopId: shop.id,
          offerId: row.offerId,
          name: row.name,
          fromCategory: row.categoryName,
          toCategory: targetName,
          toCategoryId: targetId,
        });
      }

      // Полный отчёт ошибок карточек — точные размеры классов проблем.
      if (withErrorsReport) {
        const cards = await getYandexOfferCardsContentStatus(shop, rows.map((row) => row.offerId));
        for (const card of cards) {
          if (card.errors?.length) cardsWithErrors += 1;
          for (const error of card.errors || []) {
            const key = cleanText(error.message || error.comment || error.type || "unknown");
            errorCounts.set(key, (errorCounts.get(key) || 0) + 1);
          }
          for (const warning of card.warnings || []) {
            const key = cleanText(warning.message || warning.comment || warning.type || "unknown");
            warningCounts.set(key, (warningCounts.get(key) || 0) + 1);
          }
        }
      }

      if (!dryRun) {
        const byShopPlanned = planned.filter((item) => item.shopId === shop.id);
        for (const chunk of chunkArray(byShopPlanned, 500)) {
          const sent = await sendYandexOfferMappings(
            shop,
            chunk.map((item) => ({ offerId: item.offerId, marketCategoryId: item.toCategoryId })),
          );
          results.push(...sent.map((item) => ({ ...item, target: shop.id })));
        }
      }
    }

    const topEntries = (map) => [...map.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
      .map(([message, count]) => ({ message, count }));
    const fixesByTransition = new Map();
    for (const item of planned) {
      const key = `${item.fromCategory} → ${item.toCategory}`;
      fixesByTransition.set(key, (fixesByTransition.get(key) || 0) + 1);
    }

    response.json({
      ok: dryRun || results.every((item) => item.ok),
      dryRun,
      totalOffers,
      cardsWithErrors,
      topErrors: topEntries(errorCounts),
      topWarnings: topEntries(warningCounts),
      plannedCategoryFixes: planned.length,
      fixesByTransition: [...fixesByTransition.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([transition, count]) => ({ transition, count })),
      sample: planned.slice(0, 30).map(({ shopId, ...rest }) => rest),
      sent: results.filter((item) => item.ok).length,
      failed: results.filter((item) => !item.ok).length,
      errors: results.filter((item) => !item.ok).slice(0, 30),
    });
  } catch (error) {
    next(error);
  }
});

// Sync names (and vendor/description) from Ozon products to their Yandex counterparts
// where the stored name differs. Sends a content-mode partial update to the Yandex API.
app.post("/api/ozon-yandex-import/sync-names", requireAdmin, async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun === true;
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "Postgres недоступен." });

    const { Prisma } = require("@prisma/client");
    // JOIN yandex products with their ozon siblings; compare stored names.
    const mismatches = await prisma.$queryRaw`
      SELECT
        yp.id          AS "yandexId",
        yp.offer_id    AS "offerId",
        yp.name        AS "yandexName",
        yp.raw         AS "yandexRaw",
        op.name        AS "ozonName",
        op.raw         AS "ozonRaw"
      FROM warehouse_products yp
      JOIN warehouse_products op
        ON op.marketplace = 'ozon'
       AND LOWER(op.offer_id) = LOWER(yp.offer_id)
       AND op.archived = false
      WHERE yp.marketplace = 'yandex'
        AND LOWER(TRIM(yp.name)) != LOWER(TRIM(op.name))
      LIMIT 5000
    `;

    if (dryRun) {
      return response.json({
        ok: true,
        dryRun: true,
        mismatched: mismatches.length,
        sample: mismatches.slice(0, 20).map((r) => ({
          offerId: r.offerId,
          yandexName: r.yandexName,
          ozonName: r.ozonName,
        })),
      });
    }

    const shops = uniqueYandexShopsByBusiness();
    if (!shops.length) return response.status(400).json({ error: "Yandex Market не настроен." });

    const batchByShop = new Map();
    const updateIds = [];

    for (const row of mismatches) {
      const yandexProduct = normalizeWarehouseProduct(
        row.yandexRaw && typeof row.yandexRaw === "object" ? row.yandexRaw : { offerId: row.offerId },
      );
      const ozonProduct = normalizeWarehouseProduct(
        row.ozonRaw && typeof row.ozonRaw === "object" ? row.ozonRaw : { offerId: row.offerId },
      );
      const newName = cleanText(ozonProduct.name || ozonProduct.ozon?.name || row.ozonName);
      if (!newName) continue;

      const target = cleanText(yandexProduct.target);
      const shop = shops.find((s) => s.id === target) || shops[0];
      if (!shop) continue;

      // Build full offer from the yandex product but override name/vendor/description
      // from the Ozon product so all text content stays in sync.
      const built = buildYandexOfferMapping(yandexProduct, { name: newName });

      if (!batchByShop.has(shop.id)) batchByShop.set(shop.id, { shop, offers: [] });
      batchByShop.get(shop.id).offers.push(compactObject({
        offerId: cleanText(row.offerId),
        name: newName,
        vendor: built.offer?.vendor,
        description: built.offer?.description,
      }));
      updateIds.push({ id: row.yandexId, name: newName });
    }

    const results = [];
    for (const { shop, offers } of batchByShop.values()) {
      const sent = await sendYandexOfferMappings(shop, offers);
      results.push(...sent.map((item) => ({ ...item, target: shop.id })));
    }

    const sentCount = results.filter((item) => item.ok).length;
    const failedCount = results.filter((item) => !item.ok).length;

    // Update local DB names for successfully synced products.
    const sentOfferIds = new Set(results.filter((item) => item.ok).map((item) => cleanText(item.offerId).toLowerCase()));
    const toUpdate = updateIds.filter((u) => {
      const offerId = cleanText(
        mismatches.find((m) => m.yandexId === u.id)?.offerId || "",
      ).toLowerCase();
      return sentOfferIds.has(offerId);
    });
    if (toUpdate.length) {
      await Promise.all(
        toUpdate.map(({ id, name }) =>
          prisma.warehouseProduct.update({ where: { id }, data: { name, updatedAt: new Date() } }).catch(() => null),
        ),
      );
    }

    logger.info("ozon_yandex_sync_names_done", {
      mismatched: mismatches.length,
      sent: sentCount,
      failed: failedCount,
    });
    response.json({
      ok: failedCount === 0,
      mismatched: mismatches.length,
      sent: sentCount,
      failed: failedCount,
      errors: results.filter((item) => !item.ok).slice(0, 20),
    });
  } catch (error) {
    next(error);
  }
});

// Backfill descriptions for Yandex products that lack them, sourcing from their Ozon
// counterparts in Postgres. Run after an Ozon refresh (to populate ozon.description in DB).
app.post("/api/ozon-yandex-import/repair-yandex-descriptions", requireAdmin, async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun !== false;
    const requestedLimit = Math.max(1, Math.min(50000, Number(request.body?.limit || 5000) || 5000));
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "Postgres недоступен." });
    const { Prisma } = require("@prisma/client");

    // Find Yandex products missing descriptions where an Ozon sibling exists.
    // Note: we do NOT filter by op.raw->'ozon'->>'description' here because descriptions
    // are not stored by the regular Ozon import (/v3/product/info/list omits them).
    // We fetch them on-demand below via /v1/product/info/description.
    const rows = await prisma.$queryRaw`
      SELECT
        yp.id   AS "yandexId",
        yp.offer_id AS "offerId",
        yp.raw  AS "yandexRaw",
        op.raw  AS "ozonRaw"
      FROM warehouse_products yp
      JOIN warehouse_products op
        ON op.marketplace = 'ozon'
        AND LOWER(op.offer_id) = LOWER(yp.offer_id)
        AND op.archived = false
      WHERE yp.marketplace = 'yandex'
        AND COALESCE(TRIM(yp.raw->'yandex'->>'description'), '') = ''
      LIMIT ${requestedLimit}
    `;

    if (dryRun) {
      return response.json({
        ok: true,
        dryRun: true,
        candidates: rows.length,
        sample: rows.slice(0, 10).map((r) => ({
          offerId: r.offerId,
          storedOzonDesc: String(r.ozonRaw?.ozon?.description || "").slice(0, 80),
        })),
      });
    }

    const shops = uniqueYandexShopsByBusiness();
    if (!shops.length) return response.status(400).json({ error: "Yandex Market не настроен." });
    const shopByTarget = new Map(shops.map((shop) => [shop.id, shop]));

    const batchByShop = new Map();
    const updatedRecords = [];
    let apiCalls = 0;
    let apiErrors = 0;

    const descDelayMs = 60; // stay well within Ozon rate limits (~15 req/s)
    for (const row of rows) {
      const yandexProduct = normalizeWarehouseProduct(
        row.yandexRaw && typeof row.yandexRaw === "object" ? row.yandexRaw : { offerId: row.offerId },
      );
      const ozonProduct = normalizeWarehouseProduct(
        row.ozonRaw && typeof row.ozonRaw === "object" ? row.ozonRaw : { offerId: row.offerId },
      );

      // Use description stored in DB if available; otherwise fetch from Ozon API.
      let description = cleanText(ozonProduct.ozon?.description || "");
      if (!description) {
        const ozonAccount = getOzonAccountByTarget(cleanText(ozonProduct.target || "ozon"));
        if (ozonAccount) {
          try {
            const body = ozonProduct.productId
              ? { product_id: Number(ozonProduct.productId) || undefined, offer_id: cleanText(row.offerId) }
              : { offer_id: cleanText(row.offerId) };
            const data = await ozonRequest("/v1/product/info/description", body, ozonAccount);
            description = cleanText(data?.result?.description || "");
            apiCalls += 1;
            // Persist back to Ozon product DB record so future runs skip the API call.
            if (description) {
              await prisma.$executeRaw`
                UPDATE warehouse_products
                SET raw = jsonb_set(raw, '{ozon,description}', ${JSON.stringify(description)}::jsonb, true),
                    updated_at = NOW()
                WHERE marketplace = 'ozon'
                  AND LOWER(offer_id) = LOWER(${cleanText(row.offerId)})
                  AND archived = false
              `.catch(() => {});
            }
          } catch (e) {
            apiErrors += 1;
            if (apiErrors >= 10) break; // abort on repeated failures to protect rate limits
          }
          if (descDelayMs > 0) await new Promise((resolve) => setTimeout(resolve, descDelayMs));
        }
      }

      if (!description) continue;

      const target = cleanText(yandexProduct.target);
      const shop = shopByTarget.get(target) || shops[0];
      if (!shop) continue;

      const built = buildYandexOfferMapping({ ...yandexProduct, ozon: { ...(yandexProduct.ozon || {}), description } });
      if (!batchByShop.has(shop.id)) batchByShop.set(shop.id, { shop, offers: [] });
      batchByShop.get(shop.id).offers.push(compactObject({
        offerId: cleanText(row.offerId),
        description: built.offer?.description,
        vendor: built.offer?.vendor,
      }));
      updatedRecords.push({ id: row.yandexId, description });
    }

    const results = [];
    for (const { shop, offers } of batchByShop.values()) {
      for (const chunk of chunkArray(offers, 500)) {
        const sent = await sendYandexOfferMappings(shop, chunk);
        results.push(...sent.map((item) => ({ ...item, target: shop.id })));
      }
    }

    // Persist description into Yandex product records in Postgres.
    const sentOfferIds = new Set(results.filter((item) => item.ok).map((item) => cleanText(item.offerId).toLowerCase()).filter(Boolean));
    const toPersist = updatedRecords.filter((rec) => {
      const yandexRaw = rows.find((r) => r.yandexId === rec.id)?.yandexRaw;
      return yandexRaw && sentOfferIds.has(cleanText(yandexRaw.offerId).toLowerCase());
    });
    for (const rec of toPersist) {
      await prisma.warehouseProduct.update({
        where: { id: rec.id },
        data: { raw: { ...(rows.find((r) => r.yandexId === rec.id)?.yandexRaw || {}), yandex: { ...(rows.find((r) => r.yandexId === rec.id)?.yandexRaw?.yandex || {}), description: rec.description } } },
      }).catch(() => {});
    }

    response.json({
      ok: results.every((item) => item.ok),
      dryRun: false,
      candidates: rows.length,
      apiCalls,
      apiErrors,
      sent: results.filter((item) => item.ok).length,
      failed: results.filter((item) => !item.ok).length,
      persisted: toPersist.length,
    });
  } catch (error) {
    next(error);
  }
});

// Manual trigger for the Postgres-backed auto import (also runs on schedule).
app.post("/api/ozon-yandex-import/auto-run", requireAdmin, async (request, response, next) => {
  try {
    const limit = Math.max(1, Math.min(5000, Number(request.body?.limit || 0) || ozonYandexAutoImportPerRunLimit));
    const onlyLinked = request.body?.onlyLinked === true;
    const result = await runOzonYandexAutoImport({ limit, source: "manual", onlyLinked });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon-yandex-import/archive-blocked", requireAdmin, async (request, response, next) => {
  try {
    if (request.body?.confirmed !== true) {
      return response.status(400).json({ error: "Нужно подтверждение archived-blocked confirmed=true." });
    }
    const requestedLimit = Number(request.body?.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const warehouse = await readWarehouse();
    const products = (warehouse.products || [])
      .filter((product) => product.marketplace === "ozon")
      .slice(0, limit);
    const candidates = products.map(buildOzonYandexImportCandidate);
    const blockedIds = new Set(candidates
      .filter((row) => row.blockReasons?.length)
      .map((row) => row.id)
      .filter(Boolean));
    const blockedProducts = products.filter((product) => blockedIds.has(product.id));
    const actions = await archiveProductsOnMarketplaces(blockedProducts);
    response.json({
      ok: true,
      requested: blockedProducts.length,
      archived: actions.filter((item) => item.ok).length,
      failed: actions.filter((item) => !item.ok).length,
      actions,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon-yandex-import/sync-stocks", requireAdmin, async (request, response, next) => {
  try {
    const requestedLimit = Number(request.body?.limit || request.query.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const warehouse = await readWarehouse();
    const products = (warehouse.products || [])
      .filter((product) => product.marketplace === "ozon")
      .slice(0, limit);
    const existingOfferIds = getLocalYandexExportedOfferIdSet(warehouse.products || []);
    const result = await sendYandexStocksFromOzonProducts(products, {
      dryRun: request.body?.dryRun === true,
      warehouseProducts: warehouse.products || [],
      existingOfferIds,
    });
    response.json({ ok: result.ok, limit, ...result });
  } catch (error) {
    next(error);
  }
});
