// Per-product ETA: when the rolling reconciler will next touch the product (price/stock
// refresh) and when an archived product is expected to be unarchived. Powers the live
// countdown chips in the product card.
app.get("/api/warehouse/products/:id/eta", async (request, response, next) => {
  try {
    const productId = cleanText(request.params.id);
    if (!productId) return response.status(400).json({ error: "id required" });
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "Postgres недоступен." });

    const product = await prisma.warehouseProduct.findUnique({
      where: { id: productId },
      select: { id: true, marketplace: true, archived: true, offerId: true },
    });
    if (!product) return response.status(404).json({ error: "Товар не найден." });

    const state = await readLinkedReconcilerState().catch(() => ({}));
    const cursorId = cleanText(state.lastProductId);
    const tickMs = linkedReconcilerIntervalMinutes * 60 * 1000;
    const perTick = Math.max(1, linkedReconcilerMaxProductsPerTick);

    // Products between the cursor and this product (keyset order by id asc).
    const linkedWhere = { AND: [{ links: { some: {} } }] };
    let ahead = 0;
    if (!cursorId || productId > cursorId) {
      ahead = await prisma.warehouseProduct.count({
        where: { ...linkedWhere, id: { gt: cursorId || "", lt: productId } },
      });
    } else {
      // Cursor already passed this product: remainder of this cycle + position in the next.
      const [restOfCycle, position] = await Promise.all([
        prisma.warehouseProduct.count({ where: { ...linkedWhere, id: { gt: cursorId } } }),
        prisma.warehouseProduct.count({ where: { ...linkedWhere, id: { lt: productId } } }),
      ]);
      ahead = restOfCycle + position;
    }
    const ticksAway = Math.floor(ahead / perTick);
    const nextRunAtMs = linkedReconcilerNextRunAt ? new Date(linkedReconcilerNextRunAt).getTime() : Date.now() + tickMs;
    let priceEtaMs = Math.max(0, nextRunAtMs - Date.now()) + ticksAway * tickMs;
    // Changed prices are picked up by the fast price sweep (every ~2 min), not the cursor.
    const priceColumns = await prisma.warehouseProduct.findUnique({
      where: { id: productId },
      select: { currentPrice: true, targetPrice: true },
    }).catch(() => null);
    const priceDiffers = priceColumns
      && Number(priceColumns.targetPrice || 0) > 0
      && Number(priceColumns.currentPrice || 0) !== Number(priceColumns.targetPrice || 0);
    if (priceDiffers && typeof priceSweepNextRunAt === "string" && priceSweepNextRunAt) {
      const sweepMs = Math.max(0, new Date(priceSweepNextRunAt).getTime() - Date.now()) + 30_000;
      priceEtaMs = Math.min(priceEtaMs, sweepMs);
    }

    // Unarchive ETA.
    let unarchiveEtaMs = null;
    let unarchiveNote = null;
    if (product.archived) {
      if (product.marketplace === "yandex") {
        // Fast bulk unarchive pass runs every minute (Medium: 10k offers/min).
        const fastAtMs = yandexFastUnarchiveNextRunAt ? new Date(yandexFastUnarchiveNextRunAt).getTime() : Date.now() + 60_000;
        unarchiveEtaMs = Math.max(0, fastAtMs - Date.now()) + 15_000;
        unarchiveNote = "yandex_fast_unarchive";
      } else {
        try {
          const queue = await readOzonUnarchiveQueue();
          const items = (queue.items || []).filter((item) => item.status !== "done");
          const index = items.findIndex((item) => cleanText(item.id) === productId);
          if (index >= 0) {
            const daysAway = Math.floor(index / Math.max(1, ozonUnarchiveDailyLimit));
            const resetAt = new Date();
            resetAt.setHours(3, 5, 0, 0);
            if (resetAt.getTime() < Date.now()) resetAt.setDate(resetAt.getDate() + 1);
            unarchiveEtaMs = Math.max(0, resetAt.getTime() - Date.now()) + daysAway * 24 * 60 * 60 * 1000;
            unarchiveNote = `ozon_queue_position_${index + 1}`;
          } else {
            unarchiveEtaMs = Math.max(0, nextRunAtMs - Date.now()) + ticksAway * tickMs;
            unarchiveNote = "ozon_not_queued_yet";
          }
        } catch {
          unarchiveNote = "ozon_queue_unavailable";
        }
      }
    }

    response.json({
      ok: true,
      id: product.id,
      archived: product.archived,
      priceEtaSeconds: Math.round(priceEtaMs / 1000),
      priceEtaAt: new Date(Date.now() + priceEtaMs).toISOString(),
      unarchiveEtaSeconds: unarchiveEtaMs !== null ? Math.round(unarchiveEtaMs / 1000) : null,
      unarchiveEtaAt: unarchiveEtaMs !== null ? new Date(Date.now() + unarchiveEtaMs).toISOString() : null,
      unarchiveNote,
      ahead,
      perTick,
      tickMinutes: linkedReconcilerIntervalMinutes,
    });
  } catch (error) {
    next(error);
  }
});
