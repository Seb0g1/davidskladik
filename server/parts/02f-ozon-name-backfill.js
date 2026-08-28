// One-shot / scheduled backfill of Ozon product names.
//
// /v3/product/list returns no names, and the regular import refreshes details for only
// OZON_SYNC_DETAIL_LIMIT products per run — over 10k rows were stuck with name=offerId,
// invisible to word search (testers included). This backfills name/brand/images/description
// for every weak-named row in big chunks through the request pool.

let ozonNameBackfillRunning = false;

async function runOzonNameBackfill({ limit = 20000, source = "manual" } = {}) {
  if (ozonNameBackfillRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  ozonNameBackfillRunning = true;
  const startedAt = Date.now();
  try {
    const weakRows = await prisma.$queryRawUnsafe(`
      SELECT id, offer_id AS "offerId", target
      FROM warehouse_products
      WHERE marketplace = 'ozon' AND (name = offer_id OR name = '' OR name IS NULL)
      LIMIT ${Math.max(1, Math.min(50000, Number(limit) || 20000))}
    `);
    if (!weakRows.length) return { status: "ok", updated: 0, weak: 0 };

    let updated = 0;
    let missing = 0;
    const byTarget = new Map();
    for (const row of weakRows) {
      const key = cleanText(row.target) || "ozon";
      if (!byTarget.has(key)) byTarget.set(key, []);
      byTarget.get(key).push(row);
    }

    for (const [target, rows] of byTarget.entries()) {
      const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
      if (!account) continue;
      for (const chunk of chunkArray(rows, 500)) {
        const infoMap = await getOzonProductInfoMap(chunk.map((row) => row.offerId), account, { continueOnError: true })
          .catch(() => new Map());
        for (const row of chunk) {
          const info = getOzonOfferMapValue(infoMap, row.offerId) || {};
          const name = cleanText(info.name);
          if (!name || name === cleanText(row.offerId)) {
            missing += 1;
            continue;
          }
          const vendor = cleanText(info.brand || info.vendor || "");
          const primaryImage = firstImageUrl(info.primary_image || info.primaryImage || info.images);
          try {
            const current = await prisma.warehouseProduct.findUnique({ where: { id: row.id }, select: { raw: true } });
            const raw = current?.raw && typeof current.raw === "object" ? current.raw : {};
            const nextRaw = {
              ...raw,
              name,
              imageUrl: raw.imageUrl || primaryImage || undefined,
              ozon: {
                ...(raw.ozon && typeof raw.ozon === "object" ? raw.ozon : {}),
                name,
                vendor: vendor || raw.ozon?.vendor,
                description: cleanText(info.description) || raw.ozon?.description,
                primaryImage: primaryImage || raw.ozon?.primaryImage,
                images: Array.isArray(info.images) && info.images.length ? info.images : raw.ozon?.images,
                barcodes: Array.isArray(info.barcodes) && info.barcodes.length ? info.barcodes : raw.ozon?.barcodes,
                categoryId: info.description_category_id || info.category_id || raw.ozon?.categoryId,
              },
            };
            await prisma.warehouseProduct.update({
              where: { id: row.id },
              data: {
                name,
                ...(vendor ? { brand: vendor } : {}),
                raw: nextRaw,
              },
            });
            updated += 1;
          } catch (error) {
            logger.warn("ozon name backfill row failed", { offerId: row.offerId, detail: error?.message || String(error) });
          }
        }
        logger.info("ozon_name_backfill_progress", { target, updated, missing, of: weakRows.length });
      }
    }
    logger.info("ozon_name_backfill_complete", {
      source,
      weak: weakRows.length,
      updated,
      missing,
      elapsedMs: Date.now() - startedAt,
    });
    return { status: "ok", weak: weakRows.length, updated, missing, elapsedMs: Date.now() - startedAt };
  } catch (error) {
    logger.warn("ozon name backfill failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    ozonNameBackfillRunning = false;
  }
}

app.post("/api/ozon/backfill-names", requireAdmin, async (request, response, next) => {
  try {
    const limit = Math.max(1, Math.min(50000, Number(request.body?.limit || 20000) || 20000));
    // Long job: respond immediately, run in background, watch logs for completion.
    if (request.body?.wait === true) {
      return response.json(await runOzonNameBackfill({ limit, source: "manual" }));
    }
    void runOzonNameBackfill({ limit, source: "manual" });
    response.status(202).json({ ok: true, started: true, limit });
  } catch (error) {
    next(error);
  }
});
