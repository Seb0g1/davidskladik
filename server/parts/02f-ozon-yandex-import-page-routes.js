// Manual Ozon -> Yandex import page (Postgres-backed, selective).
//
// Why this exists: the legacy /preview and /send routes read readWarehouse(), which in
// Postgres mode only returns a recent in-memory subset — so newly added Ozon products were
// invisible ("товар не импортировался"). These routes page Postgres directly, let the
// operator search by article and pick specific products, and reuse the shared export
// pipeline (cards + price + stock + persist) from the auto-import.

let ozonImportRefreshRunning = false;
let ozonImportRefreshLastResult = null;

// Refresh: pull the fresh Ozon catalog (names/prices/stocks) into Postgres in the
// background, so the candidate list reflects what's actually in the Ozon cabinet now.
app.post("/api/ozon-yandex-import/refresh", requireAdmin, async (request, response, next) => {
  try {
    if (ozonImportRefreshRunning) {
      return response.json({ ok: true, started: false, running: true });
    }
    const limit = Math.max(1, Math.min(50000, Number(request.body?.limit || 30000) || 30000));
    ozonImportRefreshRunning = true;
    const startedAt = Date.now();
    void (async () => {
      try {
        const warehouse = await readWarehouse().catch(() => ({ products: [] }));
        const imported = await importOzonWarehouseProducts(limit, warehouse.products || [], {
          detailRefreshLimit: Math.min(limit, Number(process.env.OZON_YANDEX_IMPORT_DETAIL_LIMIT || 25000) || 25000),
        });
        if (Array.isArray(imported.imported) && imported.imported.length) {
          await writeWarehouseProductPatch(imported.imported, { reason: "ozon_import_page_refresh", writeLinks: false })
            .catch((error) => logger.warn("ozon import refresh persist failed", { detail: error?.message || String(error) }));
        }
        ozonImportRefreshLastResult = {
          at: new Date().toISOString(),
          imported: Array.isArray(imported.imported) ? imported.imported.length : 0,
          warnings: imported.warnings || [],
          elapsedMs: Date.now() - startedAt,
        };
        logger.info("ozon_import_page_refresh_complete", ozonImportRefreshLastResult);
      } catch (error) {
        ozonImportRefreshLastResult = { at: new Date().toISOString(), error: error?.message || String(error) };
        logger.warn("ozon import page refresh failed", { detail: error?.message || String(error) });
      } finally {
        ozonImportRefreshRunning = false;
      }
    })();
    response.status(202).json({ ok: true, started: true, running: true });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon-yandex-import/refresh/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json({ ok: true, running: ozonImportRefreshRunning, lastResult: ozonImportRefreshLastResult });
  } catch (error) {
    next(error);
  }
});

// Candidate list: Ozon products with import flags (eligible / blocked+reasons / exists on
// Yandex). Searchable by article or name, paged. Postgres-direct so new products show up.
app.get("/api/ozon-yandex-import/candidates", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma || !shouldUsePostgresStorage()) return response.status(503).json({ error: "Postgres недоступен." });
    const q = cleanText(request.query.q || "");
    const page = Math.max(1, Number(request.query.page || 1) || 1);
    const pageSize = Math.max(1, Math.min(100, Number(request.query.pageSize || 40) || 40));
    const onlyEligible = String(request.query.onlyEligible || "") === "true";
    const brand = cleanText(request.query.brand || "");

    const { Prisma } = require("@prisma/client");

    // Known Yandex offerIds (to flag "already exists"). Lightweight column scan.
    const yandexRows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      select: { offerId: true },
    });
    const existingOfferIds = new Set(yandexRows.map((row) => cleanText(row.offerId).toLowerCase()).filter(Boolean));

    const where = { marketplace: "ozon", archived: false };
    if (q) {
      where.OR = [
        { offerId: { contains: q, mode: "insensitive" } },
        { name: { contains: q, mode: "insensitive" } },
        { productId: { contains: q, mode: "insensitive" } },
      ];
    }
    if (brand) where.brand = { contains: brand, mode: "insensitive" };

    // When showing only eligible candidates, use a SQL anti-join to skip Ozon products
    // already on Yandex before JS evaluation — with 10K+ products this drops response
    // time from 15-25s to under 1s when most of the catalog is already imported.
    let rows;
    let total;
    let scanCapped = false;
    if (onlyEligible) {
      const scanLimit = 50000;
      const qFilter = q
        ? Prisma.sql`AND (LOWER(wp.offer_id) LIKE ${`%${q.toLowerCase()}%`} OR LOWER(wp.name) LIKE ${`%${q.toLowerCase()}%`})`
        : Prisma.empty;
      const brandFilter = brand
        ? Prisma.sql`AND LOWER(wp.brand) LIKE ${`%${brand.toLowerCase()}%`}`
        : Prisma.empty;
      const rawRows = await prisma.$queryRaw`
        SELECT wp.id, wp.offer_id as "offerId", wp.name, wp.raw
        FROM warehouse_products wp
        WHERE wp.marketplace = 'ozon' AND wp.archived = false
          AND NOT EXISTS (
            SELECT 1 FROM warehouse_products yp
            WHERE yp.marketplace = 'yandex'
              AND LOWER(yp.offer_id) = LOWER(wp.offer_id)
          )
          ${qFilter}
          ${brandFilter}
        ORDER BY wp.updated_at DESC
        LIMIT ${scanLimit}
      `;
      rows = rawRows;
      total = rows.length;
      scanCapped = rows.length >= scanLimit;
    } else {
      total = await prisma.warehouseProduct.count({ where });
      rows = await prisma.warehouseProduct.findMany({
        where,
        select: { id: true, offerId: true, name: true, raw: true },
        orderBy: { updatedAt: "desc" },
        skip: (page - 1) * pageSize,
        take: pageSize,
      });
    }

    const mapped = rows.map((row) => {
      const product = row.raw && typeof row.raw === "object" ? normalizeWarehouseProduct(row.raw) : normalizeWarehouseProduct(row);
      // This page IS manual operator import — use manual leniency (only hard/business blocks +
      // technical readiness), so soft heuristics don't make products unselectable here. Matches
      // the send-selected route, which also runs in manual mode.
      const candidate = buildOzonYandexImportCandidate(product, { yandexExistingOfferIds: existingOfferIds, manual: true });
      return {
        id: row.id,
        offerId: candidate.offerId || row.offerId,
        name: candidate.name || cleanText(row.name),
        vendor: candidate.vendor || "",
        imageUrl: cleanText(product.imageUrl || product.ozon?.primaryImage || ""),
        eligible: Boolean(candidate.eligible),
        existsOnYandex: Boolean(candidate.existingInYandex),
        yandexReady: Boolean(candidate.yandexReady),
        blockReasons: candidate.blockReasons || [],
        missing: Array.isArray(candidate.missing) ? candidate.missing : [],
      };
    });

    let items = mapped;
    let effectiveTotal = total;
    if (onlyEligible) {
      const eligible = mapped.filter((item) => item.eligible);
      effectiveTotal = eligible.length;
      items = eligible.slice((page - 1) * pageSize, (page - 1) * pageSize + pageSize);
    }

    response.json({
      ok: true,
      page,
      pageSize,
      total: effectiveTotal,
      scanCapped,
      items,
    });
  } catch (error) {
    next(error);
  }
});

// Returns IDs of all eligible (not yet on Yandex) Ozon candidates for a given brand.
// Used by "Select all by brand" UI action.
app.get("/api/ozon-yandex-import/candidates/eligible-ids", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma || !shouldUsePostgresStorage()) return response.status(503).json({ error: "Postgres недоступен." });
    const brand = cleanText(request.query.brand || "");
    if (!brand) return response.json({ ids: [], eligible: 0, total: 0 });

    const { Prisma } = require("@prisma/client");
    const brandLike = `%${brand.toLowerCase()}%`;
    const rawRows = await prisma.$queryRaw`
      SELECT wp.id, wp.offer_id as "offerId", wp.name, wp.raw
      FROM warehouse_products wp
      WHERE wp.marketplace = 'ozon' AND wp.archived = false
        AND NOT EXISTS (
          SELECT 1 FROM warehouse_products yp
          WHERE yp.marketplace = 'yandex' AND LOWER(yp.offer_id) = LOWER(wp.offer_id)
        )
        AND LOWER(wp.brand) LIKE ${brandLike}
      ORDER BY wp.updated_at DESC
      LIMIT 5000
    `;

    const yandexRows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      select: { offerId: true },
    });
    const existingOfferIds = new Set(yandexRows.map((row) => cleanText(row.offerId).toLowerCase()).filter(Boolean));

    const ids = [];
    for (const row of rawRows) {
      const product = row.raw && typeof row.raw === "object" ? normalizeWarehouseProduct(row.raw) : normalizeWarehouseProduct(row);
      const candidate = buildOzonYandexImportCandidate(product, { yandexExistingOfferIds: existingOfferIds, manual: true });
      if (candidate.eligible) ids.push(row.id);
    }

    response.json({ ids, eligible: ids.length, total: rawRows.length });
  } catch (error) {
    next(error);
  }
});

// Send an explicit list of selected Ozon products to Yandex (by id or offerId).
app.post("/api/ozon-yandex-import/send-selected", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma || !shouldUsePostgresStorage()) return response.status(503).json({ error: "Postgres недоступен." });
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(cleanText).filter(Boolean);
    const offerIds = (Array.isArray(request.body?.offerIds) ? request.body.offerIds : []).map(cleanText).filter(Boolean);
    if (!ids.length && !offerIds.length) return response.status(400).json({ error: "Передайте ids или offerIds выбранных товаров." });

    const shops = uniqueYandexShopsByBusiness();
    if (!shops.length) return response.status(400).json({ error: "Yandex Market не настроен." });

    const [yandexRows, rows] = await Promise.all([
      prisma.warehouseProduct.findMany({ where: { marketplace: "yandex" }, select: { offerId: true } }),
      prisma.warehouseProduct.findMany({
        where: {
          marketplace: "ozon",
          OR: [
            ...(ids.length ? [{ id: { in: ids } }] : []),
            ...(offerIds.length ? [{ offerId: { in: offerIds } }] : []),
          ],
        },
        include: { links: true },
        take: 2000,
      }),
    ]);
    const existingYandexOfferIds = new Set(yandexRows.map((row) => cleanText(row.offerId).toLowerCase()).filter(Boolean));

    const products = [];
    const skipped = [];
    for (const row of rows) {
      const product = productFromPostgres(row);
      // manual: operator explicitly selected this product, so only the hard/business blocks
      // and the technical readiness check apply (soft quality heuristics are bypassed).
      const candidate = buildOzonYandexImportCandidate(product, { yandexExistingOfferIds: existingYandexOfferIds, manual: true });
      if (candidate.existingInYandex) {
        skipped.push({ offerId: candidate.offerId || row.offerId, reasons: ["Уже существует на Yandex Market"] });
        continue;
      }
      if (candidate.blockReasons?.length || !candidate.yandexReady) {
        const reasons = candidate.blockReasons?.length
          ? candidate.blockReasons
          : [`Карточка не готова к выгрузке (не хватает: ${(candidate.missing || []).join(", ") || "обязательных полей"})`];
        skipped.push({ offerId: candidate.offerId || row.offerId, reasons });
        continue;
      }
      products.push(product);
    }

    if (!products.length) {
      return response.json({ ok: true, requested: rows.length, sent: 0, failed: 0, skipped });
    }

    const exportResult = await exportOzonProductsToYandex(products, shops, { reason: "ozon_yandex_import_manual" });
    await appendAudit(request, "ozon_yandex_import.send_selected", {
      entityType: "ozon_yandex_import",
      entityId: "manual",
      requested: products.length,
      sent: exportResult.sentOfferIds.size,
    });
    response.json({
      ok: exportResult.failed === 0,
      requested: rows.length,
      sent: exportResult.sentOfferIds.size,
      failed: exportResult.failed,
      priceSent: Number(exportResult.priceStage?.sent || 0),
      stockSent: Number(exportResult.stockStage?.sent || 0),
      skipped,
      errors: exportResult.results.filter((item) => !item.ok).slice(0, 30),
    });
  } catch (error) {
    next(error);
  }
});
