// Aggregated data for the admin dashboard (PLAN.md item D.1): sales today/week, profit,
// top suppliers, price queue size, archive backlog, unread chats/questions/reviews.
function topSuppliersFromFinanceOrders(orders = [], limit = 5) {
  const byName = new Map();
  for (const order of orders) {
    const name = cleanText(order.supplierName) || "Без поставщика";
    const entry = byName.get(name) || { supplierName: name, orders: 0, income: 0, profit: 0 };
    entry.orders += 1;
    entry.income += normalizeFinanceMoney(order.payoutAmount ?? order.saleAmount, 0);
    entry.profit += normalizeFinanceMoney(order.profitAmount ?? financeOrderProfit(order), 0);
    byName.set(name, entry);
  }
  return Array.from(byName.values())
    .sort((a, b) => b.profit - a.profit)
    .slice(0, Math.max(1, Number(limit || 5) || 5))
    .map((entry) => ({
      supplierName: entry.supplierName,
      orders: entry.orders,
      income: normalizeFinanceMoney(entry.income, 0),
      profit: normalizeFinanceMoney(entry.profit, 0),
    }));
}

app.get("/api/dashboard/summary", requireAdmin, async (_request, response, next) => {
  try {
    const prisma = getPrisma();
    const usePg = Boolean(prisma) && shouldUsePostgresStorage();

    const [todayResult, weekResult, weekExpensesResult, priceQueueCount, archivedLinkedYandex, ozonQueue, unreadByTypeRows] = await Promise.all([
      listFinanceOrders({ period: "today", limit: 2000, linkedOnly: true }),
      listFinanceOrders({ period: "7d", limit: 2000, linkedOnly: true }),
      listFinanceExpenses({ period: "7d", limit: 2000 }),
      usePg
        ? prisma.salesAutomationSkuState.count({ where: { priceStatus: { in: ["pending", "queued"] } } }).catch(() => 0)
        : Promise.resolve(0),
      usePg
        ? prisma.warehouseProduct.count({ where: { marketplace: "yandex", archived: true, links: { some: {} } } }).catch(() => 0)
        : Promise.resolve(0),
      readOzonUnarchiveQueue().catch(() => ({ items: [] })),
      usePg
        ? ensureNotificationsTable()
          .then((ready) => (ready
            ? prisma.$queryRawUnsafe("SELECT type, COUNT(*)::int AS n FROM app_notifications WHERE read_at IS NULL GROUP BY type")
            : []))
          .catch(() => [])
        : Promise.resolve([]),
    ]);

    const today = financeSummaryFromRows(todayResult.orders, []);
    const week = financeSummaryFromRows(weekResult.orders, weekExpensesResult.expenses);
    const ozonBacklog = ozonUnarchiveQueuePublic(ozonQueue, { limit: 1 });
    const unreadByType = Object.fromEntries((unreadByTypeRows || []).map((row) => [row.type, Number(row.n || 0)]));
    const unreadTotal = Object.values(unreadByType).reduce((sum, n) => sum + n, 0);

    response.json({
      ok: true,
      updatedAt: new Date().toISOString(),
      salesToday: {
        orders: today.orders,
        income: today.orderIncome,
        profit: today.orderProfit,
      },
      salesWeek: {
        orders: week.orders,
        income: week.orderIncome,
        profit: week.netProfit,
      },
      topSuppliers: topSuppliersFromFinanceOrders(weekResult.orders, 5),
      priceQueue: Number(priceQueueCount || 0),
      archiveBacklog: {
        yandex: Number(archivedLinkedYandex || 0),
        ozon: Number(ozonBacklog.total || 0),
        ozonDue: Number(ozonBacklog.due || 0),
      },
      notifications: {
        unread: unreadTotal,
        byType: unreadByType,
      },
    });
  } catch (error) {
    next(error);
  }
});
