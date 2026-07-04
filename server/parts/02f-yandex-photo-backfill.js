// Scheduled backfill of pictures for Yandex products that have none.
//
// Yandex cards created by old exports (and some manual imports) went out without photos.
// Every interval this job selects yandex rows with no local pictures, reads the actual
// card from the Yandex API (local rows can be stale), self-heals local rows when the
// card already has photos, and otherwise sends the Ozon sibling images (same offerId)
// to Yandex and persists them locally. The manual repair route stays for bulk one-shots;
// this keeps "товар на Yandex без фото" fixed automatically day to day.

const yandexPhotoBackfillEnabled = process.env.YANDEX_PHOTO_BACKFILL_ENABLED !== "false";
const yandexPhotoBackfillIntervalHours = Math.max(1, Math.min(48, Number(process.env.YANDEX_PHOTO_BACKFILL_INTERVAL_HOURS || 6) || 6));
const yandexPhotoBackfillPerRunLimit = Math.max(10, Math.min(5000, Number(process.env.YANDEX_PHOTO_BACKFILL_PER_RUN || 500) || 500));
let yandexPhotoBackfillTimer = null;
let yandexPhotoBackfillRunning = false;
let yandexPhotoBackfillNextRunAt = null;

function collectOzonRowPicturesForYandex(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const ozon = raw.ozon && typeof raw.ozon === "object" ? raw.ozon : {};
  return Array.from(new Set([
    ...(Array.isArray(row.images) ? row.images : []),
    ...splitList(ozon.primaryImage),
    ...splitList(ozon.images),
    cleanText(raw.imageUrl),
  ].map(cleanText).filter(Boolean)));
}

async function runYandexPhotoBackfill({ limit = yandexPhotoBackfillPerRunLimit, source = "auto" } = {}) {
  if (yandexPhotoBackfillRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) return { status: "no_yandex_shops" };
  yandexPhotoBackfillRunning = true;
  const startedAt = Date.now();
  try {
    // 1. Yandex rows without any local pictures.
    const candidates = [];
    let scanned = 0;
    let cursorId = null;
    while (candidates.length < limit) {
      const page = await prisma.warehouseProduct.findMany({
        where: { marketplace: "yandex", archived: false },
        select: { id: true, offerId: true, target: true, raw: true },
        orderBy: { id: "asc" },
        take: 1000,
        ...(cursorId ? { cursor: { id: cursorId }, skip: 1 } : {}),
      });
      if (!page.length) break;
      cursorId = page[page.length - 1].id;
      for (const row of page) {
        scanned += 1;
        const product = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : null;
        if (!product) continue;
        if (!product.id) product.id = row.id;
        const normalized = normalizeWarehouseProduct(product);
        const offerId = cleanText(normalized.offerId || row.offerId);
        if (!offerId) continue;
        const hasPictures = Boolean(cleanText(normalized.imageUrl))
          || splitList(normalized.yandex?.pictures).length > 0
          || splitList(normalized.yandex?.images).length > 0;
        if (hasPictures) continue;
        const shop = shops.find((s) => s.id === cleanText(normalized.target))
          || shops.find((s) => matchesYandexTarget(normalized.target, s.id));
        if (!shop) continue;
        candidates.push({ product, normalized, offerId, offerKey: offerId.toLowerCase(), shop });
        if (candidates.length >= limit) break;
      }
      if (page.length < 1000) break;
    }
    if (!candidates.length) {
      logger.info("yandex photo backfill: nothing to do", { source, scanned });
      return { status: "ok", scanned, candidates: 0, sent: 0, selfHealed: 0 };
    }

    // 2. What the Yandex card actually has: cards with photos only need a local patch,
    // and sending pictures to them would needlessly overwrite curated images.
    const remotePicturesByKey = new Map();
    for (const shop of shops) {
      const offerIds = candidates.filter((c) => c.shop.id === shop.id).map((c) => c.offerId);
      if (!offerIds.length) continue;
      try {
        const items = await getYandexOfferMappingsByOfferIds(shop, offerIds);
        for (const item of items) {
          const offer = item?.offer || {};
          const key = cleanText(offer.offerId).toLowerCase();
          if (!key) continue;
          const pictures = splitList(offer.pictures);
          if (pictures.length) remotePicturesByKey.set(key, pictures);
        }
      } catch (error) {
        logger.warn("yandex photo backfill: offer-mappings read failed", { target: shop.id, detail: error?.message || String(error) });
      }
    }

    // 3. Ozon sibling images (same offerId) for the cards that truly lack photos.
    const needPhotos = candidates.filter((c) => !remotePicturesByKey.has(c.offerKey));
    const ozonPicturesByKey = new Map();
    for (const chunk of chunkArray([...new Set(needPhotos.map((c) => c.offerKey))], 500)) {
      if (!chunk.length) continue;
      const rows = await prisma.$queryRaw`
        SELECT offer_id AS "offerId", images, raw
        FROM warehouse_products
        WHERE marketplace = 'ozon' AND archived = false AND LOWER(offer_id) = ANY(${chunk})
      `;
      for (const row of rows) {
        const key = cleanText(row.offerId).toLowerCase();
        if (!key || ozonPicturesByKey.has(key)) continue;
        const pictures = collectOzonRowPicturesForYandex(row);
        if (pictures.length) ozonPicturesByKey.set(key, pictures);
      }
    }

    // 4. Send Ozon pictures to Yandex, partial offer: offerId + pictures only.
    const batchByShop = new Map();
    const pendingPicturesByKey = new Map();
    let skippedNoOzonPhoto = 0;
    for (const candidate of needPhotos) {
      const pictures = ozonPicturesByKey.get(candidate.offerKey) || [];
      if (!pictures.length) {
        skippedNoOzonPhoto += 1;
        continue;
      }
      if (!batchByShop.has(candidate.shop.id)) batchByShop.set(candidate.shop.id, { shop: candidate.shop, offers: [] });
      batchByShop.get(candidate.shop.id).offers.push({ offerId: candidate.offerId, pictures });
      pendingPicturesByKey.set(candidate.offerKey, pictures);
    }
    const results = [];
    for (const { shop, offers } of batchByShop.values()) {
      const sent = await sendYandexOfferMappings(shop, offers);
      results.push(...sent.map((item) => ({ ...item, target: shop.id })));
    }
    const sentOfferKeys = new Set(results
      .filter((item) => item.ok)
      .map((item) => cleanText(item.offerId).toLowerCase())
      .filter(Boolean));

    // 5. Persist pictures locally (sent from Ozon or self-healed from the remote card)
    // so the rows drop out of the candidate scan on the next run.
    const now = new Date().toISOString();
    const patched = [];
    let selfHealed = 0;
    for (const candidate of candidates) {
      let pictures = null;
      if (sentOfferKeys.has(candidate.offerKey)) {
        pictures = pendingPicturesByKey.get(candidate.offerKey);
      } else if (remotePicturesByKey.has(candidate.offerKey)) {
        pictures = remotePicturesByKey.get(candidate.offerKey);
        selfHealed += 1;
      }
      if (!pictures?.length) continue;
      const product = candidate.product;
      product.yandex = normalizeYandexDraft({
        ...(product.yandex || {}),
        offerId: product.yandex?.offerId || candidate.offerId,
        pictures,
      });
      if (!cleanText(product.imageUrl)) product.imageUrl = pictures[0];
      product.updatedAt = now;
      patched.push(product);
    }
    if (patched.length) {
      await writeWarehouseProductPatch(patched, { reason: "yandex_photo_backfill", writeLinks: false })
        .catch((error) => logger.warn("yandex photo backfill persist failed", { detail: error?.message || String(error) }));
    }

    const failed = results.filter((item) => !item.ok).length;
    logger.info("yandex_photo_backfill_complete", {
      source,
      scanned,
      candidates: candidates.length,
      sent: sentOfferKeys.size,
      failed,
      selfHealed,
      skippedNoOzonPhoto,
      elapsedMs: Date.now() - startedAt,
    });
    return {
      status: "ok",
      scanned,
      candidates: candidates.length,
      sent: sentOfferKeys.size,
      failed,
      selfHealed,
      skippedNoOzonPhoto,
      errors: results.filter((item) => !item.ok).slice(0, 20),
    };
  } catch (error) {
    logger.warn("yandex photo backfill failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    yandexPhotoBackfillRunning = false;
  }
}

function scheduleYandexPhotoBackfill(delayMs = null) {
  if (!yandexPhotoBackfillEnabled) {
    yandexPhotoBackfillNextRunAt = null;
    return;
  }
  if (yandexPhotoBackfillTimer) clearTimeout(yandexPhotoBackfillTimer);
  const intervalMs = yandexPhotoBackfillIntervalHours * 60 * 60 * 1000;
  const normalizedDelay = Math.max(60_000, Number(delayMs ?? intervalMs) || intervalMs);
  yandexPhotoBackfillNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  yandexPhotoBackfillTimer = setTimeout(async () => {
    try {
      if (heavyBackgroundWorkShouldDefer("yandex_photo_backfill")) {
        logger.info("yandex photo backfill deferred under load");
        scheduleYandexPhotoBackfill(15 * 60 * 1000);
        return;
      }
      await runYandexPhotoBackfill({ source: "schedule" });
    } catch (error) {
      logger.warn("yandex photo backfill tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleYandexPhotoBackfill(intervalMs);
    }
  }, normalizedDelay);
  yandexPhotoBackfillTimer.unref?.();
}

// Manual trigger (the scheduled run also covers this; useful for a first bulk pass).
app.post("/api/yandex/photo-backfill", requireAdmin, async (request, response, next) => {
  try {
    const limit = Math.max(1, Math.min(5000, Number(request.body?.limit || 0) || yandexPhotoBackfillPerRunLimit));
    if (request.body?.wait === true) {
      return response.json(await runYandexPhotoBackfill({ limit, source: "manual" }));
    }
    void runYandexPhotoBackfill({ limit, source: "manual" });
    response.status(202).json({ ok: true, started: true, limit });
  } catch (error) {
    next(error);
  }
});
