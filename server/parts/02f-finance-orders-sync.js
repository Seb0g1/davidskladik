// Automatic finance orders ingestion from both marketplaces (worker).
//
// Every FINANCE_ORDERS_SYNC_MINUTES (20m) pulls recent orders:
//   - Ozon: /v3/posting/fbs/list with financial_data (sale, payout, commissions per SKU)
//   - Yandex: /v2/campaigns/{campaignId}/orders (sale amount per item; fees TODO via stats)
// and upserts FinanceOrder rows. Purchase cost comes from the product's current selected
// supplier (PriceMaster link) at ingestion time. Linked-only filtering happens at read
// time (listFinanceOrders linkedOnly), so unlinked products are stored but flagged.

const financeOrdersSyncEnabled = process.env.FINANCE_ORDERS_SYNC_ENABLED !== "false";
const financeOrdersSyncIntervalMs = Math.max(5 * 60_000, Number(process.env.FINANCE_ORDERS_SYNC_MINUTES || 20) * 60_000 || 20 * 60_000);
const financeOrdersLookbackDays = Math.max(1, Math.min(30, Number(process.env.FINANCE_ORDERS_LOOKBACK_DAYS || 7) || 7));
let financeOrdersSyncTimer = null;
let financeOrdersSyncRunning = false;
let financeOrdersSyncLastResult = null;

async function financeUpsertMarketplaceOrder(financeOrder) {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return false;
  const payload = {
    marketplace: financeOrder.marketplace || null,
    target: financeOrder.target || null,
    orderId: financeOrder.orderId,
    postingNumber: financeOrder.postingNumber || null,
    offerId: financeOrder.offerId || null,
    productName: financeOrder.productName || null,
    quantity: financeOrder.quantity,
    saleAmount: financeOrder.saleAmount,
    payoutAmount: financeOrder.payoutAmount,
    purchaseCost: financeOrder.purchaseCost,
    feesAmount: financeOrder.feesAmount ?? 0,
    profitAmount: financeOrderProfit(financeOrder),
    supplierName: financeOrder.supplierName || null,
    partnerId: financeOrder.partnerId || null,
    source: financeOrder.source,
    status: financeOrder.status,
    soldAt: toDateOrNull(financeOrder.soldAt),
    receivedAt: toDateOrNull(financeOrder.receivedAt),
    raw: financeOrder.raw,
  };
  await prisma.financeOrder.upsert({
    where: { id: financeOrder.id },
    create: { id: financeOrder.id, ...payload },
    update: payload,
  });
  return true;
}

// Current supplier purchase cost for a marketplace offer (RUB, for the given quantity).
async function financeSupplierCostForOffer(marketplace, offerId, quantity) {
  const prisma = getPrisma();
  if (!prisma) return { purchaseCost: null, supplierName: "", partnerId: "" };
  // The selected supplier snapshot often lives only on the Ozon sibling (links are shared
  // per group), so look across all marketplaces and prefer the requested one.
  const rows = await prisma.warehouseProduct.findMany({
    where: { offerId: cleanText(offerId) },
    select: { marketplace: true, raw: true },
  }).catch(() => []);
  const pickSupplier = (row) => (row?.raw && typeof row.raw === "object" ? row.raw.selectedSupplier : null);
  let supplier = null;
  for (const row of rows) {
    const candidate = pickSupplier(row);
    if (!candidate || typeof candidate !== "object" || !(Number(candidate.price ?? candidate.supplierPrice) > 0)) continue;
    supplier = candidate;
    if (cleanText(row.marketplace) === cleanText(marketplace)) break;
  }
  if (!supplier) {
    // Fallback: the sales automation state stores the supplier used on the last price send.
    const stateRows = await prisma.$queryRawUnsafe(
      `SELECT raw FROM sales_automation_sku_states
       WHERE offer_id = $1 AND raw -> 'selectedSupplier' IS NOT NULL
       ORDER BY updated_at DESC LIMIT 1`,
      cleanText(offerId),
    ).catch(() => []);
    const stateSupplier = stateRows[0]?.raw?.selectedSupplier;
    if (stateSupplier && typeof stateSupplier === "object" && Number(stateSupplier.price ?? stateSupplier.supplierPrice) > 0) {
      supplier = stateSupplier;
    }
  }
  if (!supplier || typeof supplier !== "object") return { purchaseCost: null, supplierName: "", partnerId: "" };
  const purchaseCost = await financePurchaseCostRubFromPicking({
    price: supplier.price ?? supplier.supplierPrice,
    priceCurrency: supplier.priceCurrency || supplier.currency || "USD",
    quantity,
  });
  return {
    purchaseCost,
    supplierName: cleanText(supplier.partnerName || supplier.supplierName),
    partnerId: cleanText(supplier.partnerId),
  };
}

async function syncOzonFinanceOrders() {
  let imported = 0;
  for (const account of getOzonAccounts()) {
    try {
      const since = new Date(Date.now() - financeOrdersLookbackDays * 24 * 60 * 60 * 1000).toISOString();
      let offset = 0;
      for (let pageIndex = 0; pageIndex < 10; pageIndex += 1) {
        const data = await ozonRequest("/v3/posting/fbs/list", {
          dir: "DESC",
          filter: { since, to: new Date().toISOString() },
          limit: 100,
          offset,
          with: { financial_data: true, analytics_data: false },
        }, account);
        const postings = data?.result?.postings || [];
        for (const posting of postings) {
          if (["cancelled", "not_accepted"].includes(cleanText(posting.status))) continue;
          const financialProducts = posting.financial_data?.products || [];
          const finByProduct = new Map(financialProducts.map((item) => [String(item.product_id), item]));
          for (const product of posting.products || []) {
            const quantity = Math.max(1, Number(product.quantity || 1) || 1);
            const saleAmount = normalizeFinanceMoney(Number(product.price || 0) * quantity, 0);
            const fin = finByProduct.get(String(product.sku || product.product_id)) || financialProducts.find((item) => cleanText(item.offer_id) === cleanText(product.offer_id)) || null;
            const payoutAmount = fin ? normalizeFinanceMoney(Number(fin.payout || 0) * quantity, 0) || saleAmount : saleAmount;
            const feesAmount = fin ? normalizeFinanceMoney(Number(fin.commission_amount || 0) * quantity, 0) : 0;
            const cost = await financeSupplierCostForOffer("ozon", product.offer_id, quantity);
            const ok = await financeUpsertMarketplaceOrder(normalizeFinanceOrder({
              id: `mp:ozon:${cleanText(posting.posting_number)}:${cleanText(product.offer_id)}`,
              marketplace: "ozon",
              target: account.id,
              orderId: cleanText(posting.order_number || posting.order_id || posting.posting_number),
              postingNumber: cleanText(posting.posting_number),
              offerId: cleanText(product.offer_id),
              productName: cleanText(product.name),
              quantity,
              saleAmount,
              payoutAmount,
              feesAmount,
              purchaseCost: cost.purchaseCost,
              supplierName: cost.supplierName,
              partnerId: cost.partnerId,
              source: "marketplace_sync",
              status: cleanText(posting.status) || "unknown",
              soldAt: posting.in_process_at || posting.created_at || null,
              raw: { posting_number: posting.posting_number, status: posting.status },
            })).catch((error) => {
              logger.warn("finance ozon order upsert failed", { detail: error?.message || String(error) });
              return false;
            });
            if (ok) imported += 1;
          }
        }
        if (postings.length < 100) break;
        offset += 100;
      }
    } catch (error) {
      logger.warn("finance ozon orders sync failed", { account: account.id, detail: error?.message || String(error) });
    }
  }
  return imported;
}

async function syncYandexFinanceOrders() {
  let imported = 0;
  for (const shop of getYandexShops()) {
    if (!shop.campaignId) continue;
    try {
      const fromDate = new Date(Date.now() - financeOrdersLookbackDays * 24 * 60 * 60 * 1000);
      const dd = String(fromDate.getDate()).padStart(2, "0");
      const mm = String(fromDate.getMonth() + 1).padStart(2, "0");
      const yyyy = fromDate.getFullYear();
      for (let page = 1; page <= 10; page += 1) {
        const data = await yandexRequest(shop, "GET", `/v2/campaigns/${shop.campaignId}/orders?pageSize=50&page=${page}&fromDate=${dd}-${mm}-${yyyy}`);
        const orders = data?.orders || [];
        for (const order of orders) {
          const status = cleanText(order.status).toUpperCase();
          if (["CANCELLED", "RETURNED", "UNPAID"].includes(status)) continue;
          for (const item of order.items || []) {
            const quantity = Math.max(1, Number(item.count || 1) || 1);
            const unitPrice = Number(item.price ?? item.buyerPrice ?? item.priceBeforeDiscount ?? 0) || 0;
            const saleAmount = normalizeFinanceMoney(unitPrice * quantity, 0);
            const subsidyTotal = (item.subsidies || []).reduce((sum, subsidy) => sum + (Number(subsidy.amount || 0) || 0), 0);
            const payoutAmount = normalizeFinanceMoney(saleAmount + subsidyTotal, 0);
            const cost = await financeSupplierCostForOffer("yandex", item.offerId, quantity);
            const ok = await financeUpsertMarketplaceOrder(normalizeFinanceOrder({
              id: `mp:yandex:${order.id}:${cleanText(item.offerId)}`,
              marketplace: "yandex",
              target: shop.id,
              orderId: String(order.id),
              offerId: cleanText(item.offerId),
              productName: cleanText(item.offerName),
              quantity,
              saleAmount,
              payoutAmount,
              feesAmount: 0,
              purchaseCost: cost.purchaseCost,
              supplierName: cost.supplierName,
              partnerId: cost.partnerId,
              source: "marketplace_sync",
              status: status.toLowerCase() || "unknown",
              soldAt: order.creationDate ? new Date(order.creationDate.split(" ")[0].split("-").reverse().join("-")).toISOString() : null,
              raw: { orderId: order.id, status: order.status, substatus: order.substatus },
            })).catch((error) => {
              logger.warn("finance yandex order upsert failed", { detail: error?.message || String(error) });
              return false;
            });
            if (ok) imported += 1;
          }
        }
        if (orders.length < 50) break;
      }
    } catch (error) {
      logger.warn("finance yandex orders sync failed", { shop: shop.id, detail: error?.message || String(error) });
    }
  }
  return imported;
}

async function runFinanceOrdersSync({ source = "schedule" } = {}) {
  if (financeOrdersSyncRunning) return { status: "already_running" };
  if (!shouldUsePostgresStorage() || !getPrisma()) return { status: "postgres_disabled" };
  financeOrdersSyncRunning = true;
  const startedAt = Date.now();
  try {
    const ozon = await syncOzonFinanceOrders();
    const yandex = await syncYandexFinanceOrders();
    financeOrdersSyncLastResult = {
      at: new Date().toISOString(),
      ozon,
      yandex,
      elapsedMs: Date.now() - startedAt,
    };
    logger.info("finance_orders_sync_complete", { source, ...financeOrdersSyncLastResult });
    return { status: "ok", ...financeOrdersSyncLastResult };
  } catch (error) {
    logger.warn("finance orders sync failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    financeOrdersSyncRunning = false;
  }
}

function scheduleFinanceOrdersSync(delayMs = financeOrdersSyncIntervalMs) {
  if (!financeOrdersSyncEnabled) return;
  if (financeOrdersSyncTimer) clearTimeout(financeOrdersSyncTimer);
  financeOrdersSyncTimer = setTimeout(async () => {
    try {
      await runFinanceOrdersSync({ source: "schedule" });
    } finally {
      scheduleFinanceOrdersSync(financeOrdersSyncIntervalMs);
    }
  }, Math.max(30_000, Number(delayMs) || financeOrdersSyncIntervalMs));
  financeOrdersSyncTimer.unref?.();
}

app.post("/api/finance/sync-orders", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await runFinanceOrdersSync({ source: "manual" }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/finance/sync-orders/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json({
      ok: true,
      enabled: financeOrdersSyncEnabled,
      running: financeOrdersSyncRunning,
      intervalMinutes: Math.round(financeOrdersSyncIntervalMs / 60000),
      lookbackDays: financeOrdersLookbackDays,
      lastResult: financeOrdersSyncLastResult,
    });
  } catch (error) {
    next(error);
  }
});
