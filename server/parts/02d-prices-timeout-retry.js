function schedulePmTimeoutRepriceRetry(productIds = [], options = {}) {
  const ids = Array.from(new Set((Array.isArray(productIds) ? productIds : []).map(cleanText).filter(Boolean)));
  if (!ids.length) return;
  const attempt = Math.max(0, Number(options.repriceAttempt || 0) || 0);
  if (attempt >= autoPricePmTimeoutRetryAttempts) return;
  setTimeout(() => {
    queueAuthoritativePriceReprice({
      productIds: ids,
      marketplace: options.marketplace || "all",
      reason: "pm_live_timeout_retry",
      sourceEvent: options.sourceEvent || options.reason || "pm_live_timeout",
      force: true,
      onlyChanged: false,
      verify: true,
      repriceAttempt: attempt + 1,
      priority: 2,
    }).catch((error) => logger.warn("pm timeout reprice retry failed", { detail: error?.message || String(error), count: ids.length }));
  }, autoPricePmTimeoutRetryMs).unref?.();
}

