// Periodically retry stock push for YM offers that were created via Ozon→Yandex
// import but weren't yet PUBLISHED when stock was first attempted. Runs every
// 10 minutes on the worker process only.

const ymPendingStockIntervalMs = Math.max(
  60_000,
  Number(process.env.YM_PENDING_STOCK_INTERVAL_SECONDS || 600) * 1000 || 600_000,
);
const ymPendingStockMaxAttempts = Math.max(1, Number(process.env.YM_PENDING_STOCK_MAX_ATTEMPTS || 72) || 72);

let ymPendingStockTimer = null;

async function runYandexPendingStockCheck({ source = "schedule" } = {}) {
  const queue = await readYandexPendingStockQueue();
  if (!queue.items.length) return { status: "ok", pending: 0, sent: 0, removed: 0 };

  const allShops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  if (!allShops.length) return { status: "no_shops", pending: queue.items.length };

  const allOfferIds = queue.items.map((item) => cleanText(item.offerId)).filter(Boolean);
  let publishedSet;
  try {
    publishedSet = await getExistingYandexOfferIdSet(allOfferIds);
  } catch (error) {
    logger.warn("yandex_pending_stock_check: could not fetch offer-mappings", { detail: error?.message || String(error) });
    return { status: "api_error", pending: queue.items.length };
  }

  const nowReady = queue.items.filter((item) => publishedSet.has(cleanText(item.offerId).toLowerCase()));
  const stillPending = queue.items.filter((item) => !publishedSet.has(cleanText(item.offerId).toLowerCase()));

  const tooManyAttempts = stillPending.filter((item) => Number(item.attempts || 0) >= ymPendingStockMaxAttempts);
  if (tooManyAttempts.length) {
    logger.warn("yandex_pending_stock_expired", { count: tooManyAttempts.length, offerIds: tooManyAttempts.map((i) => i.offerId).slice(0, 20) });
  }

  const toRemoveIds = [
    ...nowReady.map((item) => item.offerId),
    ...tooManyAttempts.map((item) => item.offerId),
  ];

  let sent = 0;
  if (nowReady.length) {
    const shops = allShops.filter((shop) => shop.campaignId);
    if (shops.length) {
      const rows = nowReady.map((item) => ({ offerId: cleanText(item.offerId), stock: Number(item.stock) || 5 }));
      const existingSet = new Set(rows.map((r) => r.offerId.toLowerCase()));
      try {
        const result = await sendYandexStocksFromOzonProducts(
          rows.map((r) => ({ offerId: r.offerId, id: r.offerId, stock: r.stock })),
          { existingOfferIds: existingSet },
        );
        sent = Number(result.sent || 0);
        if (result.warnings?.length) logger.warn("yandex_pending_stock_sent warnings", { warnings: result.warnings });
      } catch (error) {
        logger.warn("yandex_pending_stock_sent failed", { detail: error?.message || String(error) });
      }
    }
  }

  // Bump attempt counter for items still pending
  const updatedItems = stillPending
    .filter((item) => Number(item.attempts || 0) < ymPendingStockMaxAttempts)
    .map((item) => ({ ...item, attempts: Number(item.attempts || 0) + 1, lastCheckedAt: new Date().toISOString() }));

  await writeYandexPendingStockQueue({ items: updatedItems });

  if (nowReady.length || tooManyAttempts.length) {
    logger.info("yandex_pending_stock_check", { source, ready: nowReady.length, sent, expired: tooManyAttempts.length, remaining: updatedItems.length });
  }

  return { status: "ok", pending: updatedItems.length, ready: nowReady.length, sent, expired: tooManyAttempts.length };
}

function startYandexPendingStockScheduler() {
  if (!backgroundJobsEnabled) return;

  async function tick() {
    try {
      await runYandexPendingStockCheck({ source: "schedule" });
    } catch (error) {
      logger.warn("yandex pending stock scheduler error", { detail: error?.message || String(error) });
    } finally {
      ymPendingStockTimer = setTimeout(tick, ymPendingStockIntervalMs);
    }
  }

  ymPendingStockTimer = setTimeout(tick, ymPendingStockIntervalMs);
  logger.info("yandex pending stock scheduler started", { intervalMs: ymPendingStockIntervalMs });
}
