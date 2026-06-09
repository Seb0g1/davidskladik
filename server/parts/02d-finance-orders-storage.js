async function readFinanceJsonFallback() {
  try {
    const parsed = JSON.parse(await fs.readFile(financeStatePath, "utf8"));
    return {
      orders: Array.isArray(parsed.orders) ? parsed.orders.map(normalizeFinanceOrder) : [],
      expenses: Array.isArray(parsed.expenses) ? parsed.expenses.map(normalizeFinanceExpense) : [],
      updatedAt: parsed.updatedAt || null,
    };
  } catch (error) {
    if (error.code === "ENOENT") return { orders: [], expenses: [], updatedAt: null };
    throw error;
  }
}

async function writeFinanceJsonFallback(state = {}) {
  const payload = {
    updatedAt: new Date().toISOString(),
    orders: Array.isArray(state.orders) ? state.orders.map(normalizeFinanceOrder) : [],
    expenses: Array.isArray(state.expenses) ? state.expenses.map(normalizeFinanceExpense) : [],
  };
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${financeStatePath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(temporaryPath, financeStatePath);
  return payload;
}

async function upsertFinanceOrderFromPickingRow(row = {}, request = null) {
  const normalized = normalizeSupplierPickingRow(row);
  const purchaseCost = await financePurchaseCostRubFromPicking(normalized);
  const saleAmount = normalized.saleAmount === null || normalized.saleAmount === undefined
    ? computeMarketplaceSaleAmountRub(normalized)
    : normalizeFinanceMoney(normalized.saleAmount, 0);
  const payoutAmount = normalized.payoutAmount === null || normalized.payoutAmount === undefined
    ? saleAmount
    : normalizeFinanceMoney(normalized.payoutAmount, 0);
  const financeOrder = normalizeFinanceOrder({
    id: financeOrderIdForPicking(normalized),
    marketplace: normalized.marketplace,
    target: normalized.accountName || normalized.marketplace,
    orderId: normalized.orderId || normalized.postingNumber || normalized.key,
    postingNumber: normalized.postingNumber,
    offerId: normalized.offerId,
    productName: normalized.productName,
    quantity: normalized.quantity,
    saleAmount,
    payoutAmount,
    purchaseCost,
    supplierName: normalized.supplierName,
    partnerId: normalized.partnerId,
    source: "supplier_picking",
    status: "purchase_confirmed",
    soldAt: normalized.soldAt,
    receivedAt: normalized.pickedAt || new Date().toISOString(),
    raw: {
      pickingKey: normalized.key,
      requestDocId: normalized.requestDocId,
      requestRowId: normalized.requestRowId,
      warehouseProductId: normalized.warehouseProductId,
      picking: normalized,
    },
  });
  if (shouldUsePostgresStorage()) {
    try {
      const row = await getPrisma().financeOrder.upsert({
        where: { id: financeOrder.id },
        create: {
          id: financeOrder.id,
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
          profitAmount: financeOrderProfit(financeOrder),
          supplierName: financeOrder.supplierName || null,
          partnerId: financeOrder.partnerId || null,
          source: financeOrder.source,
          status: financeOrder.status,
          soldAt: toDateOrNull(financeOrder.soldAt),
          receivedAt: toDateOrNull(financeOrder.receivedAt),
          raw: financeOrder.raw,
        },
        update: {
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
          profitAmount: financeOrderProfit(financeOrder),
          supplierName: financeOrder.supplierName || null,
          partnerId: financeOrder.partnerId || null,
          source: financeOrder.source,
          status: financeOrder.status,
          soldAt: toDateOrNull(financeOrder.soldAt),
          receivedAt: toDateOrNull(financeOrder.receivedAt),
          raw: financeOrder.raw,
        },
      });
      await appendAudit(request || { session: { username: "system", role: "admin" } }, "finance.order.purchase_from_picking", {
        entityType: "finance_order",
        entityId: financeOrder.id,
        newValue: financeOrder,
      }).catch((error) => logger.warn("finance picking audit failed", { detail: error?.message || String(error) }));
      return financeOrderFromPostgres(row);
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("finance picking postgres write failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  const state = await readFinanceJsonFallback();
  const nextOrders = [financeOrder, ...state.orders.filter((item) => item.id !== financeOrder.id)];
  await writeFinanceJsonFallback({ ...state, orders: nextOrders });
  return financeOrder;
}

async function removeFinanceOrderForPickingRow(row = {}) {
  const financeOrderId = financeOrderIdForPicking(row);
  if (shouldUsePostgresStorage()) {
    try {
      await getPrisma().financeOrder.delete({ where: { id: financeOrderId } });
      return { removed: true, source: "postgres", id: financeOrderId };
    } catch (error) {
      if (error?.code === "P2025") return { removed: false, source: "postgres", id: financeOrderId };
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("finance picking postgres delete failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  const state = await readFinanceJsonFallback();
  const nextOrders = state.orders.filter((item) => item.id !== financeOrderId);
  if (nextOrders.length !== state.orders.length) await writeFinanceJsonFallback({ ...state, orders: nextOrders });
  return { removed: nextOrders.length !== state.orders.length, source: "json", id: financeOrderId };
}
