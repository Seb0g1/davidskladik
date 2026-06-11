function financePeriodWhere(period = "30d", field = "createdAt") {
  const normalized = cleanText(period || "30d").toLowerCase();
  if (normalized === "all") return {};
  const days = normalized === "7d" ? 7 : normalized === "90d" ? 90 : 30;
  return { [field]: { gte: new Date(Date.now() - days * 24 * 60 * 60 * 1000) } };
}

function financeRowMatchesPeriod(row = {}, period = "30d", field = "createdAt") {
  const normalized = cleanText(period || "30d").toLowerCase();
  if (normalized === "all") return true;
  const days = normalized === "7d" ? 7 : normalized === "90d" ? 90 : 30;
  const date = toDateOrNull(row[field] || row.createdAt);
  if (!date) return false;
  return date.getTime() >= Date.now() - days * 24 * 60 * 60 * 1000;
}

function financeSummaryFromRows(orders = [], expenses = []) {
  const orderIncome = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.payoutAmount ?? row.saleAmount, 0), 0);
  const purchaseCost = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.purchaseCost, 0), 0);
  const fees = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.feesAmount, 0), 0);
  const tax = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.taxAmount, 0), 0);
  const penalties = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.penaltiesAmount, 0), 0);
  const refunds = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.refundsAmount, 0), 0);
  const manualExpenses = expenses.reduce((sum, row) => sum + normalizeFinanceMoney(row.amount, 0), 0);
  const orderProfit = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.profitAmount ?? financeOrderProfit(row), 0), 0);
  return {
    orders: orders.length,
    expenses: expenses.length,
    orderIncome: normalizeFinanceMoney(orderIncome, 0),
    purchaseCost: normalizeFinanceMoney(purchaseCost, 0),
    fees: normalizeFinanceMoney(fees, 0),
    tax: normalizeFinanceMoney(tax, 0),
    penalties: normalizeFinanceMoney(penalties, 0),
    refunds: normalizeFinanceMoney(refunds, 0),
    manualExpenses: normalizeFinanceMoney(manualExpenses, 0),
    orderProfit: normalizeFinanceMoney(orderProfit, 0),
    netProfit: normalizeFinanceMoney(orderProfit - manualExpenses, 0),
  };
}

async function listFinanceOrders({ period = "30d", q = "", limit = 200, linkedOnly = true } = {}) {
  const normalizedLimit = Math.max(1, Math.min(2000, Number(limit || 200) || 200));
  const shouldFilterLinked = linkedOnly !== false;
  if (shouldUsePostgresStorage()) {
    try {
      const where = {
        AND: [
          financePeriodWhere(period, "createdAt"),
          q ? {
            OR: [
              { orderId: { contains: q, mode: "insensitive" } },
              { postingNumber: { contains: q, mode: "insensitive" } },
              { offerId: { contains: q, mode: "insensitive" } },
              { productName: { contains: q, mode: "insensitive" } },
              { supplierName: { contains: q, mode: "insensitive" } },
            ],
          } : {},
        ].filter((item) => Object.keys(item || {}).length),
      };
      const take = shouldFilterLinked ? Math.max(normalizedLimit, 2000) : normalizedLimit;
      const [unfilteredTotal, rows] = await Promise.all([
        shouldFilterLinked ? Promise.resolve(null) : getPrisma().financeOrder.count({ where }),
        getPrisma().financeOrder.findMany({ where, orderBy: { createdAt: "desc" }, take }),
      ]);
      let orders = rows.map(financeOrderFromPostgres);
      if (shouldFilterLinked) {
        orders = await filterFinanceOrdersByLinkedPg(orders);
      }
      return { source: "postgres", total: shouldFilterLinked ? orders.length : unfilteredTotal, orders: orders.slice(0, normalizedLimit), linkedOnly: shouldFilterLinked };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("finance orders postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  const state = await readFinanceJsonFallback();
  const needle = cleanText(q).toLowerCase();
  let orders = state.orders
    .filter((row) => financeRowMatchesPeriod(row, period, "createdAt"))
    .filter((row) => !needle || [row.orderId, row.postingNumber, row.offerId, row.productName, row.supplierName].join(" ").toLowerCase().includes(needle))
    .sort((left, right) => String(right.createdAt || "").localeCompare(String(left.createdAt || "")));
  if (shouldFilterLinked) {
    const warehouse = await readWarehouse();
    orders = filterFinanceOrdersByLinked(orders, warehouse, true);
  }
  return { source: "json", total: orders.length, orders: orders.slice(0, normalizedLimit), linkedOnly: shouldFilterLinked };
}

async function listFinanceExpenses({ period = "30d", q = "", limit = 200 } = {}) {
  const normalizedLimit = Math.max(1, Math.min(2000, Number(limit || 200) || 200));
  if (shouldUsePostgresStorage()) {
    try {
      const where = {
        AND: [
          financePeriodWhere(period, "spentAt"),
          q ? {
            OR: [
              { offerId: { contains: q, mode: "insensitive" } },
              { productName: { contains: q, mode: "insensitive" } },
              { supplierName: { contains: q, mode: "insensitive" } },
              { note: { contains: q, mode: "insensitive" } },
            ],
          } : {},
        ].filter((item) => Object.keys(item || {}).length),
      };
      const [total, rows] = await Promise.all([
        getPrisma().financeExpense.count({ where }),
        getPrisma().financeExpense.findMany({ where, orderBy: { spentAt: "desc" }, take: normalizedLimit }),
      ]);
      return { source: "postgres", total, expenses: rows.map(financeExpenseFromPostgres) };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("finance expenses postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  const state = await readFinanceJsonFallback();
  const needle = cleanText(q).toLowerCase();
  const expenses = state.expenses
    .filter((row) => financeRowMatchesPeriod(row, period, "spentAt"))
    .filter((row) => !needle || [row.offerId, row.productName, row.supplierName, row.note].join(" ").toLowerCase().includes(needle))
    .slice(0, normalizedLimit);
  return { source: "json", total: expenses.length, expenses };
}
