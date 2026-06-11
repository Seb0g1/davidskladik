function parseLinkedOnlyQuery(requestOrValue) {
  const value = requestOrValue && typeof requestOrValue === "object" && "query" in requestOrValue
    ? requestOrValue.query?.linkedOnly
    : requestOrValue;
  if (value === undefined || value === null || value === "") return true;
  const normalized = cleanText(value).toLowerCase();
  return !["0", "false", "no", "all"].includes(normalized);
}

function extractMarketplaceSaleUnitRub(line = {}) {
  const raw = line.raw && typeof line.raw === "object" ? line.raw : {};
  const product = raw.product && typeof raw.product === "object" ? raw.product : {};
  const item = raw.item && typeof raw.item === "object" ? raw.item : {};
  const candidates = [
    line.saleUnitPrice,
    line.salePrice,
    line.priceForBuyer,
    product.price,
    product.price_for_buyer,
    product.offer_price,
    product.currency_price,
    item.price?.value,
    item.buyerPrice?.value,
    item.subsidy?.value,
    item.price,
    item.buyerPrice,
  ];
  for (const candidate of candidates) {
    const value = typeof candidate === "object" && candidate !== null
      ? Number(candidate.value ?? candidate.amount ?? candidate.price ?? 0)
      : Number(candidate);
    if (Number.isFinite(value) && value > 0) return normalizeFinanceMoney(value, 0);
  }
  return null;
}

function computeMarketplaceSaleAmountRub(line = {}) {
  const direct = Number(line.saleAmount ?? line.sale_amount ?? 0);
  if (Number.isFinite(direct) && direct > 0) return normalizeFinanceMoney(direct, 0);
  const unit = extractMarketplaceSaleUnitRub(line);
  if (!(unit > 0)) return null;
  const quantity = Math.max(1, Math.round(Number(line.quantity || line.count || 1) || 1));
  return normalizeFinanceMoney(unit * quantity, 0);
}

function isWarehouseProductLinked(product = {}, _warehouse = {}) {
  const normalized = normalizeWarehouseProduct(product);
  return (Array.isArray(normalized.links) && normalized.links.length > 0) || normalized.everHadLinks === true;
}

function financeOrderWarehouseProductIds(order = {}) {
  const raw = order.raw && typeof order.raw === "object" ? order.raw : {};
  const picking = raw.picking && typeof raw.picking === "object" ? raw.picking : {};
  return [
    order.warehouseProductId,
    order.productId,
    raw.warehouseProductId,
    raw.productId,
    picking.warehouseProductId,
    picking.productId,
  ].map(cleanText).filter(Boolean);
}

function isFinanceOrderLinked(order = {}, warehouse = {}) {
  const products = Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [];
  if (!products.length) return false;
  const idSet = new Set(financeOrderWarehouseProductIds(order).map((id) => id.toLowerCase()));
  for (const product of products) {
    if (idSet.has(cleanText(product.id).toLowerCase())) return isWarehouseProductLinked(product, warehouse);
  }
  const offerId = cleanText(order.offerId).toLowerCase();
  if (!offerId) return false;
  const marketplace = cleanText(order.marketplace).toLowerCase();
  const target = cleanText(order.target).toLowerCase();
  return products.some((product) => {
    if (cleanText(product.offerId).toLowerCase() !== offerId) return false;
    if (marketplace && cleanText(product.marketplace).toLowerCase() !== marketplace) return false;
    if (target) {
      const productTarget = cleanText(product.target).toLowerCase();
      const productTargetName = cleanText(product.targetName || product.accountName).toLowerCase();
      if (productTarget && productTarget !== target && productTargetName !== target) return false;
    }
    return isWarehouseProductLinked(product, warehouse);
  });
}

// Postgres-backed linked check: readWarehouse() only holds a recent in-memory subset in
// PG mode, which made linkedOnly drop almost every marketplace-synced order.
async function filterFinanceOrdersByLinkedPg(orders = []) {
  const rows = Array.isArray(orders) ? orders : [];
  const prisma = getPrisma();
  if (!prisma || !rows.length) return rows;
  const offerIds = Array.from(new Set(rows.map((order) => cleanText(order.offerId)).filter(Boolean)));
  if (!offerIds.length) return [];
  const linkedRows = await prisma.warehouseProduct.findMany({
    where: { offerId: { in: offerIds }, links: { some: {} } },
    select: { offerId: true, marketplace: true },
  }).catch(() => []);
  const linkedKeys = new Set();
  for (const row of linkedRows) {
    const offer = cleanText(row.offerId).toLowerCase();
    linkedKeys.add(offer); // any marketplace counts: the group shares the link
    linkedKeys.add(`${cleanText(row.marketplace).toLowerCase()}|${offer}`);
  }
  return rows.filter((order) => {
    const offer = cleanText(order.offerId).toLowerCase();
    if (!offer) return false;
    return linkedKeys.has(offer);
  });
}

function filterFinanceOrdersByLinked(orders = [], warehouse = {}, linkedOnly = true) {
  const rows = Array.isArray(orders) ? orders : [];
  if (!linkedOnly) return rows;
  return rows.filter((order) => isFinanceOrderLinked(order, warehouse));
}
