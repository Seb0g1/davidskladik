// Фото объявлений Avito. Объявление без фото Avito не публикует (и может
// завалить отчёт загрузки), поэтому фид их скрывает, а этот бэкфилл возвращает
// их в фид: сначала дешёвый проход по warehouse_products.images (Postgres),
// затем товары без фото и в Postgres добираются с Ozon API /v3/product/info/list;
// найденные фото персистятся обратно в колонку images, чтобы импорт и карточка
// склада тоже их видели.

async function loadWarehouseImagesUrlsMap(productIds = []) {
  const prisma = getPrisma();
  const ids = [...new Set(productIds.map((value) => cleanText(value)).filter(Boolean))];
  if (!prisma || !ids.length) return new Map();
  const map = new Map();
  const chunkSize = 5000;
  for (let index = 0; index < ids.length; index += chunkSize) {
    const chunk = ids.slice(index, index + chunkSize);
    const rows = await prisma.$queryRaw`
      SELECT id, images, COALESCE(raw->'avitoImages', '[]'::jsonb) AS avito_images
      FROM warehouse_products
      WHERE id = ANY(${chunk})
    `;
    for (const row of rows) {
      // Avito-specific photos take priority over Ozon images
      const avitoUrls = Array.isArray(row.avito_images) ? row.avito_images.map((url) => cleanText(url)).filter(Boolean) : [];
      const urls = avitoUrls.length ? avitoUrls : extractAvitoImageUrls(row.images);
      if (urls.length) map.set(cleanText(row.id), urls);
    }
  }
  return map;
}

function extractOzonInfoImageUrls(info = {}) {
  const primary = Array.isArray(info.primary_image) ? info.primary_image : [info.primary_image || info.primaryImage];
  const rest = Array.isArray(info.images) ? info.images : [];
  return extractAvitoImageUrls([...primary, ...rest]);
}

const avitoImageBackfillPerRun = Math.max(0, Number(process.env.AVITO_IMAGE_BACKFILL_PER_RUN || 300) || 300);
let avitoImageBackfillRunning = false;
let avitoImageBackfillLastResult = null;

// Добирает фото для объявлений фида без imageUrls. Возвращает
// { status, updatedFromPostgres, updatedFromOzon, remaining }.
async function backfillAvitoListingImages({ limit = avitoImageBackfillPerRun, source = "schedule" } = {}) {
  if (avitoImageBackfillRunning) return { status: "already_running" };
  if (!(limit > 0)) return { status: "disabled" };
  avitoImageBackfillRunning = true;
  try {
    const state = await readAvitoListingsFile();
    const pending = state.items.filter((item) => item.enabled !== false && !item.imageUrls.length && cleanText(item.sourceProductId));
    if (!pending.length) {
      const result = { status: "done", source, updatedFromPostgres: 0, updatedFromOzon: 0, remaining: 0, at: new Date().toISOString() };
      avitoImageBackfillLastResult = result;
      return result;
    }
    const prisma = getPrisma();
    if (!prisma) return { status: "postgres_unavailable", remaining: pending.length };

    // Проход 1: фото уже есть на складе — просто переносим в объявления.
    const storedImages = await loadWarehouseImagesUrlsMap(pending.map((item) => item.sourceProductId));
    const updates = [];
    const missing = [];
    for (const item of pending) {
      const urls = storedImages.get(cleanText(item.sourceProductId));
      if (urls?.length) updates.push({ adId: item.adId, imageUrls: urls });
      else missing.push(item);
    }
    const updatedFromPostgres = updates.length;

    // Проход 2: фото нет и в Postgres — добираем с Ozon API порцией за запуск
    // и персистим в колонку images, чтобы следующий импорт их тоже увидел.
    let updatedFromOzon = 0;
    let apiErrors = 0;
    let lastError = "";
    const batch = missing.slice(0, Math.max(1, Number(limit) || 1));
    if (batch.length) {
      const rows = await prisma.warehouseProduct.findMany({
        where: { id: { in: batch.map((item) => cleanText(item.sourceProductId)) } },
        select: { id: true, offerId: true, target: true },
      });
      const listingByProductId = new Map(batch.map((item) => [cleanText(item.sourceProductId), item]));
      const rowsByTarget = new Map();
      for (const row of rows) {
        const target = cleanText(row.target) || "ozon";
        if (!rowsByTarget.has(target)) rowsByTarget.set(target, []);
        rowsByTarget.get(target).push(row);
      }
      for (const [target, targetRows] of rowsByTarget) {
        const account = getOzonAccountByTarget(target);
        if (!account) continue;
        let infoMap;
        try {
          infoMap = await getOzonProductInfoMap(targetRows.map((row) => row.offerId), account, { continueOnError: true });
        } catch (error) {
          apiErrors += 1;
          lastError = error?.message || String(error);
          continue;
        }
        for (const row of targetRows) {
          const info = getOzonOfferMapValue(infoMap, row.offerId);
          if (!info) continue;
          const urls = extractOzonInfoImageUrls(info);
          if (!urls.length) continue;
          const listing = listingByProductId.get(cleanText(row.id));
          if (!listing) continue;
          updates.push({ adId: listing.adId, imageUrls: urls });
          updatedFromOzon += 1;
          try {
            await prisma.warehouseProduct.update({
              where: { id: row.id },
              data: { images: { imageUrl: urls[0], images: urls } },
            });
          } catch (error) {
            logger.warn("avito image backfill postgres persist failed", { productId: row.id, detail: error?.message || String(error) });
          }
        }
      }
    }

    if (updates.length) await upsertAvitoListings(updates, { source: undefined });
    const result = {
      status: "ok",
      source,
      updatedFromPostgres,
      updatedFromOzon,
      apiErrors,
      ...(lastError ? { lastError } : {}),
      remaining: Math.max(0, pending.length - updates.length),
      at: new Date().toISOString(),
    };
    avitoImageBackfillLastResult = result;
    if (updates.length || apiErrors) logger.info("avito image backfill", result);
    return result;
  } finally {
    avitoImageBackfillRunning = false;
  }
}
