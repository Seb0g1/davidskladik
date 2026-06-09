app.get("/api/finance/summary", requireAdmin, async (request, response, next) => {
  try {
    const period = cleanText(request.query.period || "30d").toLowerCase();
    const linkedOnly = parseLinkedOnlyQuery(request);
    const [ordersResult, expensesResult, supplierLedgerResult] = await Promise.all([
      listFinanceOrders({ period, limit: 2000, linkedOnly }),
      listFinanceExpenses({ period, limit: 2000 }),
      listSupplierLedgerEntries({ period: "all", status: "active", limit: 2000 }).catch((error) => ({ summary: supplierLedgerSummaryFromEntries([]), error: error?.message || String(error) })),
    ]);
    response.json({
      ok: true,
      period,
      linkedOnly,
      source: ordersResult.source === expensesResult.source ? ordersResult.source : "mixed",
      summary: {
        ...financeSummaryFromRows(ordersResult.orders, expensesResult.expenses),
        supplierBalance: supplierLedgerResult.summary?.balance || 0,
        supplierDebt: supplierLedgerResult.summary?.debtTotal || 0,
        supplierPaid: supplierLedgerResult.summary?.paidTotal || 0,
      },
      updatedAt: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/finance/orders", requireAdmin, async (request, response, next) => {
  try {
    const result = await listFinanceOrders({
      period: cleanText(request.query.period || "30d").toLowerCase(),
      q: cleanText(request.query.q || ""),
      limit: cleanLimit(request.query.limit, 200, 2000),
      linkedOnly: parseLinkedOnlyQuery(request),
    });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/finance/orders/:id", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const patch = normalizeFinanceOrder({ ...request.body, id });
    if (shouldUsePostgresStorage()) {
      try {
        const row = await getPrisma().financeOrder.upsert({
          where: { id },
          create: {
            id,
            marketplace: patch.marketplace || null,
            target: patch.target || null,
            orderId: patch.orderId,
            postingNumber: patch.postingNumber || null,
            offerId: patch.offerId || null,
            productName: patch.productName || null,
            quantity: patch.quantity,
            saleAmount: patch.saleAmount,
            payoutAmount: patch.payoutAmount,
            purchaseCost: patch.purchaseCost,
            feesAmount: patch.feesAmount,
            taxAmount: patch.taxAmount,
            penaltiesAmount: patch.penaltiesAmount,
            refundsAmount: patch.refundsAmount,
            profitAmount: financeOrderProfit(patch),
            supplierName: patch.supplierName || null,
            partnerId: patch.partnerId || null,
            source: patch.source,
            status: patch.status,
            soldAt: toDateOrNull(patch.soldAt),
            receivedAt: toDateOrNull(patch.receivedAt),
            raw: patch,
          },
          update: {
            saleAmount: patch.saleAmount,
            payoutAmount: patch.payoutAmount,
            purchaseCost: patch.purchaseCost,
            feesAmount: patch.feesAmount,
            taxAmount: patch.taxAmount,
            penaltiesAmount: patch.penaltiesAmount,
            refundsAmount: patch.refundsAmount,
            profitAmount: financeOrderProfit(patch),
            supplierName: patch.supplierName || null,
            partnerId: patch.partnerId || null,
            status: patch.status,
            soldAt: toDateOrNull(patch.soldAt),
            receivedAt: toDateOrNull(patch.receivedAt),
            raw: patch,
          },
        });
        await appendAudit(request, "finance.order.update", { entityType: "finance_order", entityId: id, newValue: patch });
        return response.json({ ok: true, order: financeOrderFromPostgres(row) });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("finance order postgres write failed, using JSON fallback", { detail: error?.message || String(error) });
      }
    }
    const state = await readFinanceJsonFallback();
    const nextOrders = [...state.orders.filter((row) => row.id !== id), patch];
    await writeFinanceJsonFallback({ ...state, orders: nextOrders });
    response.json({ ok: true, source: "json", order: patch });
  } catch (error) {
    next(error);
  }
});

app.get("/api/finance/expenses", requireAdmin, async (request, response, next) => {
  try {
    const result = await listFinanceExpenses({
      period: cleanText(request.query.period || "30d").toLowerCase(),
      q: cleanText(request.query.q || ""),
      limit: cleanLimit(request.query.limit, 200, 2000),
    });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/finance/expenses", requireAdmin, async (request, response, next) => {
  try {
    const expense = normalizeFinanceExpense(request.body || {});
    if (!(expense.amount > 0)) return response.status(400).json({ error: "Expense amount must be greater than zero.", code: "finance_amount_required" });
    if (shouldUsePostgresStorage()) {
      try {
        const row = await getPrisma().financeExpense.create({
          data: {
            id: expense.id,
            type: expense.type,
            supplierName: expense.supplierName || null,
            partnerId: expense.partnerId || null,
            offerId: expense.offerId || null,
            productName: expense.productName || null,
            quantity: expense.quantity,
            amount: expense.amount,
            currency: expense.currency,
            note: expense.note || null,
            source: expense.source,
            status: expense.status,
            spentAt: toDateOrNull(expense.spentAt) || new Date(),
            raw: expense,
          },
        });
        await appendAudit(request, "finance.expense.create", { entityType: "finance_expense", entityId: row.id, newValue: expense });
        return response.status(201).json({ ok: true, expense: financeExpenseFromPostgres(row) });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("finance expense postgres write failed, using JSON fallback", { detail: error?.message || String(error) });
      }
    }
    const state = await readFinanceJsonFallback();
    await writeFinanceJsonFallback({ ...state, expenses: [expense, ...state.expenses] });
    response.status(201).json({ ok: true, source: "json", expense });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/prices/retry", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Retry was not sent because manual confirmation is required." });
    }
    const result = await processPriceRetryQueue({
      queueKeys: Array.isArray(request.body.queueKeys) ? request.body.queueKeys : [],
      limit: 1000,
      respectNextRetryAt: false,
      trigger: "manual",
    });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/prices/retry-queue", requireAdmin, async (_request, response, next) => {
  try {
    const queue = await readPriceRetryQueue();
    const items = (queue.items || [])
      .map((item) => ({
        ...item,
        queueKey: priceRetryQueueKey(item),
      }))
      .sort((a, b) => new Date(b.queuedAt || 0) - new Date(a.queuedAt || 0));
    response.json({ ok: true, updatedAt: queue.updatedAt, total: items.length, items });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/prices/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 100, 500);
    const offset = Math.max(0, Number.parseInt(request.query.offset || "0", 10) || 0);
    response.json({
      ok: true,
      ...await readPriceHistory({
        productId: request.query.productId,
        offerId: request.query.offerId,
        marketplace: request.query.marketplace,
        status: request.query.status,
        dateFrom: request.query.dateFrom,
        dateTo: request.query.dateTo,
        limit,
        offset,
      }),
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/prices/retry-queue", requireAdmin, async (request, response, next) => {
  try {
    const queueKeys = new Set((Array.isArray(request.body?.queueKeys) ? request.body.queueKeys : [])
      .map((key) => String(key || "").trim())
      .filter(Boolean));
    if (!queueKeys.size) {
      await writePriceRetryQueue({ items: [] });
      return response.json({ ok: true, removed: "all" });
    }
    const queue = await readPriceRetryQueue();
    const items = (queue.items || []).filter((item) => !queueKeys.has(String(priceRetryQueueKey(item))));
    await writePriceRetryQueue({ items });
    response.json({ ok: true, removed: queueKeys.size, remaining: items.length });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon-yandex-import/preview", async (request, response, next) => {
  try {
    const requestedLimit = Number(request.query.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const refresh = String(request.query.refresh || "") === "true";
    const warehouse = await readWarehouse();
    let products = (warehouse.products || []).filter((product) => product.marketplace === "ozon");
    const warnings = [];

    if (refresh) {
      const requestedDetailLimit = Number(process.env.OZON_YANDEX_IMPORT_DETAIL_LIMIT || 1000);
      const imported = await importOzonWarehouseProducts(limit, warehouse.products || [], {
        detailRefreshLimit: Math.min(limit, Number.isFinite(requestedDetailLimit) ? requestedDetailLimit : 1000),
      });
      products = imported.imported || [];
      warnings.push(...(imported.warnings || []));
    } else {
      products = products.slice(0, limit);
    }

    const initialRows = products.map((product) => buildOzonYandexImportCandidate(product));
    const checkableOfferIds = initialRows
      .filter((row) => !row.blockReasons?.length && row.yandexReady)
      .map((row) => row.offerId)
      .map(cleanText)
      .filter(Boolean);
    const yandexExistingOfferIds = await getKnownYandexExistingOfferIds(checkableOfferIds, {
      products: warehouse.products || [],
      warnings,
      allowCatalogRefresh: refresh,
      allowDirectCheck: false,
    });

    const rows = products.map((product) => buildOzonYandexImportCandidate(product, { yandexExistingOfferIds }));
    response.json({
      ok: true,
      generatedAt: new Date().toISOString(),
      source: refresh ? "ozon_api" : "warehouse",
      limit,
      summary: summarizeOzonYandexImportPreview(rows),
      warnings,
      rows,
    });
  } catch (error) {
    next(error);
  }
});

