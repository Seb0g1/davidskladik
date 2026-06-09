async function shutdownForTests() {
  for (const timer of [
    dailySyncTimer,
    autoSyncTimer,
    marketplaceMaintenanceTimer,
    ozonUnarchiveQueueAutoTimer,
    immediateAutoPushTimer,
    priceRetryTimer,
    supplierCartAutoTimer,
  ]) {
    if (timer) clearTimeout(timer);
  }
  dailySyncTimer = null;
  autoSyncTimer = null;
  marketplaceMaintenanceTimer = null;
  ozonUnarchiveQueueAutoTimer = null;
  immediateAutoPushTimer = null;
  immediateAutoPushRunning = false;
  immediateAutoPushAll = false;
  immediateAutoPushIds.clear();
  immediateAutoPushReasons.clear();
  priceRetryTimer = null;
  supplierCartAutoTimer = null;
  await Promise.allSettled([
    marketplaceWorker?.close?.(),
    marketplaceQueue?.close?.(),
    pool?.end?.(),
    closePrisma(),
  ]);
  marketplaceWorker = null;
  marketplaceQueue = null;
}
