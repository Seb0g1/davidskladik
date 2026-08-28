// Persistent JSON queue for YM offers that were created via import but weren't
// PUBLISHED yet when stock was first sent. The scheduler retries them every 10
// minutes until they appear in offer-mappings, then sends stock and removes them.

async function readYandexPendingStockQueue() {
  try {
    const text = await fs.readFile(yandexPendingStockQueuePath, "utf8");
    if (!text.trim()) return { updatedAt: null, items: [] };
    const data = JSON.parse(text);
    return {
      updatedAt: cleanText(data.updatedAt),
      items: Array.isArray(data.items) ? data.items.filter((item) => item && cleanText(item.offerId)) : [],
    };
  } catch (error) {
    if (error.code === "ENOENT") return { updatedAt: null, items: [] };
    if (error instanceof SyntaxError) {
      logger.warn("yandex pending stock queue is invalid, resetting", { detail: error.message });
      return { updatedAt: null, items: [] };
    }
    throw error;
  }
}

async function writeYandexPendingStockQueue(queue = {}) {
  const payload = {
    updatedAt: new Date().toISOString(),
    items: Array.isArray(queue.items) ? queue.items.filter((item) => item && cleanText(item.offerId)) : [],
  };
  await fs.mkdir(dataDir, { recursive: true });
  const tmpPath = `${yandexPendingStockQueuePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmpPath, yandexPendingStockQueuePath);
  return payload;
}

async function addYandexPendingStockItems(offerIds = [], { shopId = "", stock = 0 } = {}) {
  if (!offerIds.length) return;
  const queue = await readYandexPendingStockQueue();
  const existingKeys = new Set(queue.items.map((item) => cleanText(item.offerId).toLowerCase()));
  const now = new Date().toISOString();
  for (const rawOfferId of offerIds) {
    const offerId = cleanText(rawOfferId);
    if (!offerId) continue;
    const key = offerId.toLowerCase();
    if (existingKeys.has(key)) continue;
    existingKeys.add(key);
    queue.items.push({ offerId, shopId: cleanText(shopId), stock: Number(stock) || 0, queuedAt: now, attempts: 0 });
  }
  await writeYandexPendingStockQueue(queue);
  logger.info("yandex_pending_stock_queued", { added: offerIds.length, total: queue.items.length });
}

async function removeYandexPendingStockItems(resolvedOfferIds = []) {
  if (!resolvedOfferIds.length) return;
  const lowerSet = new Set(resolvedOfferIds.map((id) => cleanText(id).toLowerCase()).filter(Boolean));
  const queue = await readYandexPendingStockQueue();
  const before = queue.items.length;
  queue.items = queue.items.filter((item) => !lowerSet.has(cleanText(item.offerId).toLowerCase()));
  await writeYandexPendingStockQueue(queue);
  logger.info("yandex_pending_stock_removed", { removed: before - queue.items.length, remaining: queue.items.length });
}
