// Automatic consignment sales from marketplace orders (worker).
//
// Every CONSIGNMENT_MP_SYNC_MINUTES (10m) scans recent FinanceOrder rows
// (ingested by the finance orders sync from Ozon/Yandex) and books a
// consignment sale for warehouse items matched by article:
//   - direct match: consignment item article == marketplace offerId
//   - link match: the warehouse product with that offerId has a PriceMaster
//     link whose supplierArticle equals the consignment item article.
// Idempotency: each booked sale stores sourceKey = "finorder:<financeOrder.id>",
// so an order is never sold twice. Orders without enough consignment stock are
// left pending and retried on the next pass (they book once the приход is done).
// Marketplace cancellations are NOT auto-reversed — use the manual "Возврат".

const consignmentMpSyncEnabled = process.env.CONSIGNMENT_MP_SYNC_ENABLED !== "false";
const consignmentMpSyncIntervalMs = Math.max(5 * 60_000, Number(process.env.CONSIGNMENT_MP_SYNC_MINUTES || 10) * 60_000 || 10 * 60_000);
const consignmentMpSyncLookbackDays = Math.max(1, Math.min(60, Number(process.env.CONSIGNMENT_MP_SYNC_LOOKBACK_DAYS || 14) || 14));
let consignmentMpSyncTimer = null;
let consignmentMpSyncRunning = false;
let consignmentMpSyncLastResult = null;

function consignmentArticleKey(value) {
  return cleanText(value).toLowerCase();
}

// offerId -> Set(link supplierArticle, lowercased) for marketplace offer ids.
async function consignmentOfferLinkArticles(offerIds = []) {
  const map = new Map();
  const unique = [...new Set(offerIds.map(cleanText).filter(Boolean))];
  for (let start = 0; start < unique.length; start += 200) {
    const chunk = unique.slice(start, start + 200);
    const rows = await getPrisma().warehouseProduct.findMany({
      where: { offerId: { in: chunk } },
      select: { offerId: true, links: { select: { supplierArticle: true } } },
    }).catch(() => []);
    for (const row of rows) {
      const key = consignmentArticleKey(row.offerId);
      if (!map.has(key)) map.set(key, new Set());
      for (const link of row.links || []) {
        const article = consignmentArticleKey(link.supplierArticle);
        if (article) map.get(key).add(article);
      }
    }
  }
  return map;
}

async function bookConsignmentSaleFromOrder(order, itemId) {
  const sourceKey = `finorder:${order.id}`;
  const quantity = Math.max(1, Math.round(Number(order.quantity || 1) || 1));
  return getPrisma().$transaction(async (tx) => {
    const existing = await tx.consignmentOperation.findUnique({ where: { sourceKey }, select: { id: true } });
    if (existing) return { status: "already_booked" };
    const item = await tx.consignmentItem.findUnique({ where: { id: itemId } });
    if (!item || item.archived) return { status: "item_missing" };
    if (item.quantity < quantity) return { status: "no_stock", itemName: item.name, have: item.quantity, need: quantity };
    const unitPurchase = normalizeFinanceMoney(item.purchasePrice, 0);
    const saleAmount = normalizeFinanceMoney(order.saleAmount, 0);
    const unitSale = saleAmount > 0 ? normalizeFinanceMoney(saleAmount / quantity, 0) : normalizeFinanceMoney(item.salePrice, 0);
    const profit = normalizeFinanceMoney((unitSale - unitPurchase) * quantity, 0);
    const sponsorHalf = normalizeFinanceMoney(profit / 2, 0);
    const myHalf = normalizeFinanceMoney(profit - sponsorHalf, 0);
    await tx.consignmentItem.update({ where: { id: item.id }, data: { quantity: { decrement: quantity } } });
    const operation = await tx.consignmentOperation.create({
      data: {
        sourceKey,
        itemId: item.id,
        itemName: item.name,
        type: "sale",
        quantity,
        unitPurchase,
        unitSale,
        balanceDelta: normalizeFinanceMoney(unitPurchase * quantity, 0),
        sponsorDelta: sponsorHalf,
        myDelta: myHalf,
        note: `Авто: ${cleanText(order.marketplace) || "маркетплейс"} заказ ${cleanText(order.orderId) || order.id}`,
        createdBy: "mp-sync",
        raw: { financeOrderId: order.id, marketplace: order.marketplace, orderId: order.orderId, offerId: order.offerId },
      },
    });
    return { status: "created", operation };
  });
}

async function runConsignmentMarketplaceSync({ source = "schedule" } = {}) {
  if (consignmentMpSyncRunning) return { status: "already_running" };
  if (!shouldUsePostgresStorage() || !getPrisma()) return { status: "postgres_disabled" };
  consignmentMpSyncRunning = true;
  const startedAt = Date.now();
  try {
    const items = (await getPrisma().consignmentItem.findMany({ where: { archived: false } }))
      .filter((item) => consignmentArticleKey(item.article));
    const result = { status: "ok", at: new Date().toISOString(), source, itemsWithArticle: items.length, matched: 0, created: 0, noStock: 0, alreadyBooked: 0, errors: 0 };
    if (!items.length) {
      consignmentMpSyncLastResult = { ...result, elapsedMs: Date.now() - startedAt };
      return consignmentMpSyncLastResult;
    }
    const itemByArticle = new Map();
    for (const item of items) {
      const key = consignmentArticleKey(item.article);
      if (!itemByArticle.has(key)) itemByArticle.set(key, item);
    }
    const sinceDate = new Date(Date.now() - consignmentMpSyncLookbackDays * 24 * 60 * 60 * 1000);
    const orders = await getPrisma().financeOrder.findMany({
      where: {
        source: "marketplace_sync",
        createdAt: { gte: sinceDate },
        offerId: { not: null },
        NOT: { status: { in: ["cancelled", "not_accepted", "returned", "unpaid"] } },
      },
      orderBy: { createdAt: "asc" },
      take: 5000,
    });
    const directMatches = [];
    const unmatchedOffers = [];
    for (const order of orders) {
      const offerKey = consignmentArticleKey(order.offerId);
      if (!offerKey) continue;
      if (itemByArticle.has(offerKey)) directMatches.push({ order, item: itemByArticle.get(offerKey) });
      else unmatchedOffers.push(order);
    }
    const linkArticles = unmatchedOffers.length
      ? await consignmentOfferLinkArticles(unmatchedOffers.map((order) => order.offerId))
      : new Map();
    const linkMatches = [];
    for (const order of unmatchedOffers) {
      const articles = linkArticles.get(consignmentArticleKey(order.offerId));
      if (!articles) continue;
      for (const article of articles) {
        if (itemByArticle.has(article)) {
          linkMatches.push({ order, item: itemByArticle.get(article) });
          break;
        }
      }
    }
    const matches = [...directMatches, ...linkMatches];
    result.matched = matches.length;
    for (const { order, item } of matches) {
      try {
        const booked = await bookConsignmentSaleFromOrder(order, item.id);
        if (booked.status === "created") {
          result.created += 1;
          logger.info("consignment auto sale booked", {
            financeOrderId: order.id,
            item: item.name,
            quantity: booked.operation.quantity,
            unitSale: Number(booked.operation.unitSale),
          });
        } else if (booked.status === "no_stock") {
          result.noStock += 1;
        } else if (booked.status === "already_booked") {
          result.alreadyBooked += 1;
        }
      } catch (error) {
        result.errors += 1;
        logger.warn("consignment auto sale failed", { financeOrderId: order.id, detail: error?.message || String(error) });
      }
    }
    result.elapsedMs = Date.now() - startedAt;
    consignmentMpSyncLastResult = result;
    if (result.created || result.noStock || result.errors) {
      logger.info("consignment_mp_sync_complete", result);
    }
    return result;
  } catch (error) {
    logger.warn("consignment marketplace sync failed", { detail: error?.message || String(error) });
    consignmentMpSyncLastResult = { status: "error", at: new Date().toISOString(), error: error?.message || String(error) };
    return consignmentMpSyncLastResult;
  } finally {
    consignmentMpSyncRunning = false;
  }
}

function scheduleConsignmentMarketplaceSync(delayMs = consignmentMpSyncIntervalMs) {
  if (!consignmentMpSyncEnabled) return;
  if (consignmentMpSyncTimer) clearTimeout(consignmentMpSyncTimer);
  consignmentMpSyncTimer = setTimeout(async () => {
    try {
      await runConsignmentMarketplaceSync({ source: "schedule" });
    } finally {
      scheduleConsignmentMarketplaceSync(consignmentMpSyncIntervalMs);
    }
  }, Math.max(30_000, Number(delayMs) || consignmentMpSyncIntervalMs));
  consignmentMpSyncTimer.unref?.();
}

app.post("/api/consignment/sync-marketplace", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await runConsignmentMarketplaceSync({ source: "manual" }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/consignment/sync-marketplace/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json({
      ok: true,
      enabled: consignmentMpSyncEnabled,
      running: consignmentMpSyncRunning,
      intervalMinutes: Math.round(consignmentMpSyncIntervalMs / 60000),
      lookbackDays: consignmentMpSyncLookbackDays,
      lastResult: consignmentMpSyncLastResult,
    });
  } catch (error) {
    next(error);
  }
});
