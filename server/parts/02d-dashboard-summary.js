// Aggregated data for the admin dashboard (PLAN.md item D.1): sales today/week, profit,
// top suppliers, price queue size, archive backlog, unread chats/questions/reviews.

// A linked, non-archived product whose current_price still differs from target_price an
// hour after price_sweep should have reconciled it (sweep runs every ~2 min, see
// 02f-price-sweep.js) means the push is stuck (repeated failure, oscillation, etc.) — not
// just "queued". Alert if too many SKUs are stuck like this.
// The query must mirror the sweep's own exclusions so we only count SKUs the sweep
// actually intends to process — otherwise recently-verified or duplicate-name SKUs inflate
// the count with false positives (verified-cooldown products re-diverge on every USD rate
// recompute but the sweep correctly skips them for PRICE_SWEEP_VERIFIED_COOLDOWN_HOURS).
const stalePriceLinkedHours = Math.max(0, Number(process.env.STALE_PRICE_LINKED_HOURS || 1) || 1);
const stalePriceLinkedAlertThreshold = Math.max(0, Number(process.env.STALE_PRICE_LINKED_ALERT_THRESHOLD || 50) || 50);
const stalePriceExcludeVerifiedHours = Math.max(0, Number(process.env.PRICE_SWEEP_VERIFIED_COOLDOWN_HOURS || 6) || 6);

// PLAN-HARDENING.md 1.3: alert if the oldest pending "auto-price-push" job has been
// waiting longer than this — a sign that recovery/unarchive jobs are starving price
// pushes despite the QUEUE_PRIORITY contract (see lib/queue-priorities.js).
const priceQueueOldestJobAlertMs = Math.max(60_000, Number(process.env.PRICE_QUEUE_OLDEST_JOB_ALERT_MS || 10 * 60_000) || 10 * 60_000);

// PLAN-HARDENING.md "Высокий": Детектор расхождений с МП — the rolling linked reconciler
// (02f-linked-reconciler-scheduler.js) compares our stored marketplaceState.code against
// the live Ozon/Yandex state on every product it refreshes. Alert if the last completed
// reconciler cycle found more drifted products than this.
const marketplaceStateMismatchAlertThreshold = Math.max(0, Number(process.env.MARKETPLACE_STATE_MISMATCH_ALERT_THRESHOLD || 60) || 60);
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

    const [todayResult, weekResult, weekExpensesResult, priceQueueCount, archivedLinkedYandex, ozonQueue, unreadByTypeRows, stalePriceLinkedCount, oldestPriceJobAgeMs, linkedSoldBelowTarget, sweepHeartbeats, reconcilerState] = await Promise.all([
      listFinanceOrders({ period: "today", limit: 2000, linkedOnly: true }),
      listFinanceOrders({ period: "7d", limit: 2000, linkedOnly: true }),
      listFinanceExpenses({ period: "7d", limit: 2000 }),
      usePg
        ? prisma.$queryRawUnsafe(
            `SELECT COUNT(*)::int AS n FROM sales_automation_sku_states WHERE price_status = 'pending' AND last_calculated_at > now() - interval '30 minutes'`
          ).then((rows) => Number(rows?.[0]?.n || 0)).catch(() => 0)
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
      usePg
        ? prisma.$queryRawUnsafe(`
            SELECT COUNT(*)::int AS n
            FROM warehouse_products p
            WHERE p.archived = false
              AND p.target_price IS NOT NULL AND p.target_price > 0
              AND (p.current_price IS NULL OR p.current_price <> p.target_price)
              AND p.updated_at < now() - interval '${stalePriceLinkedHours} hours'
              AND (
                p.raw->>'priceVerifiedAt' IS NULL
                OR (p.raw->>'priceVerifiedAt')::timestamptz < now() - interval '${stalePriceExcludeVerifiedHours} hours'
              )
              AND ${duplicateNameSqlExclusion("p")}
              AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
          `).then((rows) => Number(rows?.[0]?.n || 0)).catch(() => 0)
        : Promise.resolve(0),
      oldestPendingPriceJobAgeMs().catch(() => 0),
      usePg
        ? prisma.$queryRawUnsafe(`
            SELECT COUNT(*)::int AS n
            FROM warehouse_products p
            WHERE p.archived = false
              AND COALESCE(p.target_stock, 0) > 0
              AND COALESCE(NULLIF(p.raw -> 'marketplaceState' ->> 'stock', '')::numeric, 0) < p.target_stock
              AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
          `).then((rows) => Number(rows?.[0]?.n || 0)).catch(() => 0)
        : Promise.resolve(0),
      readSweepHeartbeats().catch(() => []),
      readLinkedReconcilerState().catch(() => null),
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
      priceHealth: {
        stalePriceLinked: Number(stalePriceLinkedCount || 0),
        staleHours: stalePriceLinkedHours,
        alertThreshold: stalePriceLinkedAlertThreshold,
        alert: Number(stalePriceLinkedCount || 0) > stalePriceLinkedAlertThreshold,
        oldestPriceJobAgeMs: Number(oldestPriceJobAgeMs || 0),
        oldestPriceJobAlertThresholdMs: priceQueueOldestJobAlertMs,
        oldestPriceJobAlert: Number(oldestPriceJobAgeMs || 0) > priceQueueOldestJobAlertMs,
      },
      stockHealth: {
        linkedSoldBelowTarget: Number(linkedSoldBelowTarget || 0),
      },
      sweepHealth: {
        sweeps: Array.isArray(sweepHeartbeats) ? sweepHeartbeats : [],
        staleSweeps: (Array.isArray(sweepHeartbeats) ? sweepHeartbeats : []).filter((s) => s.stale).map((s) => s.name),
        alert: (Array.isArray(sweepHeartbeats) ? sweepHeartbeats : []).some((s) => s.stale),
      },
      marketplaceHealth: {
        // total churn (info; mostly active -> out_of_stock depletion the reconciler corrects)
        lastCycleMismatches: Number(reconcilerState?.lastCycleStateMismatches || 0),
        lastCycleMismatchAt: reconcilerState?.lastCycleMismatchAt || null,
        cycleMismatchesSoFar: Number(reconcilerState?.cycleStateMismatches || 0),
        // actionable: we hid products the marketplace actually sells (lost sales) — this is what alerts
        lastCycleWronglyHidden: Number(reconcilerState?.lastCycleWronglyHidden || 0),
        cycleWronglyHiddenSoFar: Number(reconcilerState?.cycleWronglyHidden || 0),
        alertThreshold: marketplaceStateMismatchAlertThreshold,
        alert: Number(reconcilerState?.lastCycleWronglyHidden || 0) > marketplaceStateMismatchAlertThreshold,
      },
    });
  } catch (error) {
    next(error);
  }
});
