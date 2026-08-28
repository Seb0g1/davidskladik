async function rebuildOzonUnarchiveQueue({ source = "ozon_unarchive_queue_rebuild", limit = 20000 } = {}) {
  const max = Math.max(1, Math.min(50000, Math.round(Number(limit || 20000) || 20000)));
  const prisma = getPrisma();
  if (!prisma) throw new Error("postgres_not_configured");

  const supplierRows = await prisma.managedSupplier.findMany({ orderBy: { name: "asc" } });
  const suppliers = supplierRows.map(supplierFromPostgres);
  const rows = await prisma.warehouseProduct.findMany({
    where: {
      AND: [
        enabledWarehouseTargetWhere(),
        { marketplace: "ozon" },
        { links: { some: {} } },
      ],
    },
    include: { links: true },
    orderBy: { updatedAt: "desc" },
    take: max,
  });
  const products = rows.map(productFromPostgres);
  const built = await buildFreshWarehouseProductsFromKnownProducts(
    { suppliers },
    products,
    { persistMutations: false, livePriceMaster: false, batchPriceMaster: false },
  );
  const candidates = built.filter((product) =>
    product?.id
    && product.hasLinks
    && product.selectedSupplier
    && productLooksArchived(product),
  );

  const existingQueue = await readOzonUnarchiveQueue();
  const nextRetryAt = nextOzonUnarchiveRetryAt();
  let queueState = normalizeOzonUnarchiveQueue({
    updatedAt: existingQueue.updatedAt,
    daily: existingQueue.daily || {},
    items: [],
  });
  queueState = queueOzonUnarchiveItems(queueState, candidates, {
    nextRetryAt,
    warning: "queue_rebuilt",
    attempted: false,
  });
  await writeOzonUnarchiveQueue(queueState);
  await rescheduleOzonUnarchiveQueueAutoSoon(source).catch((error) => {
    logger.warn("ozon unarchive queue reschedule after rebuild failed", { detail: error?.message || String(error) });
  });

  const publicQueue = ozonUnarchiveQueuePublic(queueState, { limit: 5000 });
  const report = {
    ok: true,
    source,
    scanned: built.length,
    candidates: candidates.length,
    queued: publicQueue.total,
    due: publicQueue.due,
    future: publicQueue.future,
    sample: candidates.slice(0, 15).map((product) => ({
      id: product.id,
      offerId: product.offerId,
      target: product.target,
      supplier: product.selectedSupplier?.partnerName || null,
      effectiveFinalPrice: product.selectedSupplier?.effectiveFinalPrice || product.selectedSupplier?.calculatedPrice || null,
      supplierUsd: product.selectedSupplier?.price || null,
    })),
  };
  logger.info("ozon unarchive queue rebuilt", report);
  return report;
}

async function queueOzonUnarchiveForLinkedProducts(products = [], options = {}) {
  const archivedOzon = (Array.isArray(products) ? products : [])
    .filter((product) => product?.id && product.marketplace === "ozon" && productLooksArchived(product));
  if (!archivedOzon.length) return [];
  let queueState = await readOzonUnarchiveQueue();
  const nextRetryAt = nextOzonUnarchiveRetryAt();
  const warning = cleanText(options.warning) || "linked_activation_queued";
  queueState = queueOzonUnarchiveItems(queueState, archivedOzon, { nextRetryAt, warning, attempted: false });
  await writeOzonUnarchiveQueueDelta(queueState, { upsertProducts: archivedOzon });
  rescheduleOzonUnarchiveQueueAutoSoon(options.source || "linked_activation").catch((error) => {
    logger.warn("ozon unarchive queue reschedule failed", { detail: error?.message || String(error) });
  });
  return archivedOzon.map((item) => ({
    id: item.id,
    type: "unarchive",
    target: item.target,
    offerId: item.offerId,
    ok: true,
    pending: true,
    queuedByDailyLimit: true,
    warning,
    nextRetryAt,
    queueSize: queueState.items.length,
  }));
}

