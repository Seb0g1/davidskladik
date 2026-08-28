// Описания объявлений Avito берутся с Ozon: при импорте — из сохранённого
// raw склада (warehouse_products.raw->ozon->description), недостающие
// добираются через Ozon API /v1/product/info/description (один товар на
// запрос) фоновым дозаполнением и ручным роутом.

async function loadStoredOzonDescriptionsMap(productIds = []) {
  const prisma = getPrisma();
  const ids = [...new Set(productIds.map((value) => cleanText(value)).filter(Boolean))];
  if (!prisma || !ids.length) return new Map();
  const map = new Map();
  const chunkSize = 5000;
  for (let index = 0; index < ids.length; index += chunkSize) {
    const chunk = ids.slice(index, index + chunkSize);
    const rows = await prisma.$queryRaw`
      SELECT id, raw#>>'{ozon,description}' AS description
      FROM warehouse_products
      WHERE id = ANY(${chunk})
    `;
    for (const row of rows) {
      const description = cleanText(row.description || "");
      if (description) map.set(cleanText(row.id), description);
    }
  }
  return map;
}

const avitoDescriptionBackfillPerRun = Math.max(0, Number(process.env.AVITO_DESCRIPTION_BACKFILL_PER_RUN || 300) || 300);
const avitoDescriptionBackfillDelayMs = Math.max(50, Number(process.env.AVITO_DESCRIPTION_BACKFILL_DELAY_MS || 150) || 150);
let avitoDescriptionBackfillRunning = false;
let avitoDescriptionBackfillLastResult = null;

// Добирает описания с Ozon для объявлений без description. Возвращает
// { status, updated, remaining } — remaining показывает, сколько ещё осталось.
async function backfillAvitoListingDescriptionsFromOzon({ limit = avitoDescriptionBackfillPerRun, delayMs = avitoDescriptionBackfillDelayMs, source = "schedule" } = {}) {
  if (avitoDescriptionBackfillRunning) return { status: "already_running" };
  if (!(limit > 0)) return { status: "disabled" };
  avitoDescriptionBackfillRunning = true;
  try {
    const state = await readAvitoListingsFile();
    const pending = state.items.filter((item) => item.enabled !== false && !cleanText(item.description) && cleanText(item.sourceProductId));
    if (!pending.length) {
      const result = { status: "done", source, updated: 0, remaining: 0, at: new Date().toISOString() };
      avitoDescriptionBackfillLastResult = result;
      return result;
    }
    const prisma = getPrisma();
    if (!prisma) return { status: "postgres_unavailable", updated: 0, remaining: pending.length };

    const batch = pending.slice(0, Math.max(1, Number(limit) || 1));
    const rows = await prisma.warehouseProduct.findMany({
      where: { id: { in: batch.map((item) => cleanText(item.sourceProductId)) } },
      select: { id: true, offerId: true, productId: true, target: true },
    });
    const rowById = new Map(rows.map((row) => [row.id, row]));

    const updates = [];
    let apiErrors = 0;
    let lastError = "";
    for (const item of batch) {
      const row = rowById.get(cleanText(item.sourceProductId));
      if (!row) continue;
      const account = getOzonAccountByTarget(row.target || "ozon");
      if (!account) continue;
      try {
        const body = row.productId ? { product_id: Number(row.productId) || undefined, offer_id: row.offerId } : { offer_id: row.offerId };
        const data = await ozonRequest("/v1/product/info/description", body, account);
        const description = cleanText(data?.result?.description || "");
        // Пустое описание на Ozon помечаем пробелом-заглушкой? Нет: оставляем
        // пустым — фид подставит шаблон из feedDefaults.description.
        if (description) updates.push({ adId: item.adId, description });
      } catch (error) {
        apiErrors += 1;
        lastError = error?.message || String(error);
        // Серия ошибок подряд — прерываемся, чтобы не жечь лимиты API.
        if (apiErrors >= 5) break;
      }
      if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
    }

    if (updates.length) await upsertAvitoListings(updates, { source: undefined });
    const result = {
      status: apiErrors >= 5 ? "error" : "ok",
      source,
      updated: updates.length,
      apiErrors,
      ...(lastError ? { lastError } : {}),
      remaining: Math.max(0, pending.length - updates.length),
      at: new Date().toISOString(),
    };
    avitoDescriptionBackfillLastResult = result;
    if (updates.length || apiErrors) logger.info("avito description backfill", result);
    return result;
  } finally {
    avitoDescriptionBackfillRunning = false;
  }
}
