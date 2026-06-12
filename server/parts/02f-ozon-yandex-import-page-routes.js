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

    // We must compute candidate flags in JS (block reasons, build readiness), so when
    // onlyEligible is set we scan a bounded window and filter; otherwise we page directly.
    const total = await prisma.warehouseProduct.count({ where });
    const scanTake = onlyEligible ? Math.min(2000, total) : pageSize;
    const skip = onlyEligible ? 0 : (page - 1) * pageSize;
    const rows = await prisma.warehouseProduct.findMany({
      where,
      select: { id: true, offerId: true, name: true, raw: true },
      orderBy: { updatedAt: "desc" },
      skip,
      take: scanTake,
    });

    const mapped = rows.map((row) => {
      const product = row.raw && typeof row.raw === "object" ? normalizeWarehouseProduct(row.raw) : normalizeWarehouseProduct(row);
      const candidate = buildOzonYandexImportCandidate(product, { yandexExistingOfferIds: existingOfferIds });
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
      scanCapped: onlyEligible && total > scanTake,
      items,
    });
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

    const rows = await prisma.warehouseProduct.findMany({
      where: {
        marketplace: "ozon",
        OR: [
          ...(ids.length ? [{ id: { in: ids } }] : []),
          ...(offerIds.length ? [{ offerId: { in: offerIds } }] : []),
        ],
      },
      select: { id: true, offerId: true, raw: true },
      take: 2000,
    });

    const products = [];
    const skipped = [];
    for (const row of rows) {
      const product = row.raw && typeof row.raw === "object" ? normalizeWarehouseProduct(row.raw) : normalizeWarehouseProduct(row);
      const candidate = buildOzonYandexImportCandidate(product);
      if (candidate.blockReasons?.length || !candidate.yandexReady) {
        skipped.push({ offerId: candidate.offerId || row.offerId, reasons: candidate.blockReasons || ["not_ready"] });
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
