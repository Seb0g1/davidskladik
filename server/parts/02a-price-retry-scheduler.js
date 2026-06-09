function priceRetryAutoSchedulerEnabled() {
  if (process.env.PRICE_RETRY_AUTO_ENABLED === "false") return false;
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return false;
  if (!backgroundJobsEnabled) return false;
  return true;
}

function schedulePriceRetryProcessing(delayMs = null) {
  if (!priceRetryAutoSchedulerEnabled()) return;
  if (priceRetryTimer) return;
  const waitMs = Math.max(5_000, Number(delayMs ?? process.env.OZON_PRICE_RETRY_POLL_MS ?? 60_000) || 60_000);
  priceRetryTimer = setTimeout(async () => {
    priceRetryTimer = null;
    try {
      const result = await processPriceRetryQueue({
        limit: Math.max(1, Number(process.env.OZON_PRICE_RETRY_AUTO_LIMIT || 25) || 25),
        respectNextRetryAt: true,
        trigger: "auto",
      });
      if (result.processed || result.failed) {
        logger.info("price retry auto run complete", {
          processed: result.processed,
          retried: result.retried,
          failed: result.failed,
          remaining: result.remaining,
        });
      }
    } catch (error) {
      logger.warn("price retry auto run failed", { detail: error?.message || String(error) });
    } finally {
      const queue = await readPriceRetryQueue().catch(() => ({ items: [] }));
      if ((queue.items || []).length) {
        const nextAt = Math.min(...queue.items.map((item) => new Date(item.nextRetryAt || 0).getTime()).filter(Number.isFinite));
        const nextDelay = Number.isFinite(nextAt) ? Math.max(5_000, nextAt - Date.now()) : null;
        schedulePriceRetryProcessing(nextDelay);
      }
    }
  }, waitMs);
}

function schedulePriceRetryItems(items = []) {
  if (!priceRetryAutoSchedulerEnabled()) return;
  const retryItems = Array.isArray(items) ? items : [];
  if (!retryItems.length) return;
  const nextAt = Math.min(...retryItems
    .map((item) => new Date(item.nextRetryAt || 0).getTime())
    .filter(Number.isFinite));
  const delayMs = Number.isFinite(nextAt) ? Math.max(5_000, nextAt - Date.now()) : null;
  schedulePriceRetryProcessing(delayMs);
}

