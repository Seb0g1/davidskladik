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

app.post("/api/ozon-yandex-import/archive-blocked", async (request, response, next) => {
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

app.post("/api/ozon-yandex-import/sync-stocks", async (request, response, next) => {
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
