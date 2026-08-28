app.get("/api/suppliers", async (request, response, next) => {
  try {
    const refresh = request.query.refresh === "true";
    if (!refresh && suppliersListCache && Date.now() - suppliersListCache.at < suppliersListCacheTtlMs) {
      return response.json(cloneAuditValue(suppliersListCache.value));
    }
    const warehouse = await readWarehouse();
    const supplierSync = { ok: false, partners: 0, imported: 0, changed: false, error: null };
    if (refresh) {
      try {
        const partners = await listPriceMasterPartners();
        const syncedSuppliers = syncWarehouseSuppliersFromPriceMaster(warehouse, partners);
        supplierSync.ok = true;
        supplierSync.partners = partners.length;
        supplierSync.imported = syncedSuppliers.imported;
        supplierSync.changed = syncedSuppliers.changed;
        if (syncedSuppliers.changed) {
          await writeWarehouse(warehouse);
          logger.info("imported suppliers from PriceMaster via suppliers api", { imported: syncedSuppliers.imported });
        }
      } catch (error) {
        supplierSync.error = error.message;
        logger.warn("supplier import from PriceMaster in /api/suppliers failed", { detail: error.message });
      }
    }
    const autoReactivated = applySupplierAutoReactivate(warehouse);
    if (autoReactivated.length) {
      await writeWarehouse(warehouse);
      logger.info("supplier auto-reactivated from suppliers api", { count: autoReactivated.length, suppliers: autoReactivated });
    }
    const impactCounts = supplierImpactCountMap(warehouse, warehouse.suppliers || []);
    const normalizedSuppliers = (warehouse.suppliers || []).map(normalizeManagedSupplier);
    const [ledgerMap, ratePayload] = await Promise.all([
      supplierLedgerSummaryMapForSuppliers(normalizedSuppliers),
      getUsdRate().catch(() => ({ rate: Number(process.env.DEFAULT_USD_RATE || 95) || 95 })),
    ]);
    const usdRate = Number(ratePayload?.rate || ratePayload || process.env.DEFAULT_USD_RATE || 95) || 95;
    const payload = {
      suppliers: normalizedSuppliers.map((supplier) => ({
        ...supplier,
        impactProductCount: impactCounts.get(supplier.id) || 0,
        ledger: ledgerMap.get(cleanText(supplier.id || supplier.partnerId || supplier.name)) || supplierLedgerSummaryFromEntries([]),
      })),
      supplierSync,
      usdRate,
    };
    suppliersListCache = { at: Date.now(), value: cloneAuditValue(payload) };
    response.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get("/api/suppliers/:id/profile", requireAdmin, async (request, response, next) => {
  try {
    const supplierId = cleanText(request.params.id);
    const warehouse = await readWarehouse();
    const suppliers = (warehouse.suppliers || []).map(normalizeManagedSupplier);
    const supplier = suppliers.find((item) => cleanText(item.id) === supplierId || cleanText(item.partnerId) === supplierId || normalizeSupplierName(item.name) === normalizeSupplierName(supplierId));
    if (!supplier) return response.status(404).json({ error: "Supplier not found.", code: "supplier_not_found" });
    const picking = await readSupplierPickingState();
    const rows = Object.values(picking.rows || {})
      .map(normalizeSupplierPickingRow)
      .filter((row) =>
        (supplier.partnerId && cleanText(row.partnerId) === cleanText(supplier.partnerId))
        || normalizeSupplierName(row.supplierName) === normalizeSupplierName(supplier.name)
      );
    const picked = rows.filter((row) => row.status === "picked");
    const missing = rows.filter((row) => row.status === "missing");
    const blocks = Object.values((await readSupplierCartState()).supplierBlocks || {})
      .filter((block) => cleanText(block.partnerId) === cleanText(supplier.partnerId));
    const averagePrice = rows.length ? rows.reduce((sum, row) => sum + Number(row.price || 0), 0) / rows.length : 0;
    const ledger = await listSupplierLedgerEntries({
      supplierName: supplier.name,
      partnerId: supplier.partnerId,
      status: "all",
      limit: 200,
      period: "all",
    });
    response.json({
      ok: true,
      supplier,
      stats: {
        picked: picked.length,
        missing: missing.length,
        totalPurchases: rows.length,
        successRate: rows.length ? Math.round((picked.length / rows.length) * 100) : null,
        averagePrice,
      },
      blocks,
      ledger: {
        source: ledger.source,
        total: ledger.total,
        summary: ledger.summary,
        entries: ledger.entries,
        error: ledger.error || "",
      },
      history: rows.slice(0, 200),
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/suppliers/:id/profile", requireAdmin, async (request, response, next) => {
  try {
    const supplierId = cleanText(request.params.id);
    const warehouse = await readWarehouse();
    const suppliers = (warehouse.suppliers || []).map(normalizeManagedSupplier);
    const index = suppliers.findIndex((item) => cleanText(item.id) === supplierId || cleanText(item.partnerId) === supplierId || normalizeSupplierName(item.name) === normalizeSupplierName(supplierId));
    if (index < 0) return response.status(404).json({ error: "Supplier not found.", code: "supplier_not_found" });
    suppliers[index] = normalizeManagedSupplier({
      ...suppliers[index],
      ...request.body,
      raw: {
        ...(suppliers[index].raw || {}),
        ...(request.body?.raw || {}),
      },
      updatedAt: new Date().toISOString(),
    });
    warehouse.suppliers = suppliers;
    await writeWarehouse(warehouse);
    response.json({ ok: true, supplier: suppliers[index] });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-ledger/summary", requireStaff, async (request, response, next) => {
  try {
    const supplierName = cleanText(request.query.supplierName || request.query.supplier || "");
    const partnerId = cleanText(request.query.partnerId || "");
    if (!supplierName && !partnerId) {
      return response.status(400).json({ error: "supplierName or partnerId is required.", code: "supplier_ledger_identity_required" });
    }
    const result = await listSupplierLedgerEntries({
      supplierName,
      partnerId,
      status: "active",
      limit: cleanLimit(request.query.limit, 100, 500),
      period: cleanText(request.query.period || "all").toLowerCase(),
    });
    response.json({ ok: true, supplierName, partnerId, ...result, entries: result.entries.slice(0, cleanLimit(request.query.limit, 20, 100)) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-ledger/entries", requireStaff, async (request, response, next) => {
  try {
    const supplierName = cleanText(request.query.supplierName || request.query.supplier || "");
    const partnerId = cleanText(request.query.partnerId || "");
    const result = await listSupplierLedgerEntries({
      supplierName,
      partnerId,
      status: cleanText(request.query.status || "active").toLowerCase(),
      limit: cleanLimit(request.query.limit, 200, 2000),
      period: cleanText(request.query.period || "all").toLowerCase(),
    });
    response.json({ ok: true, supplierName, partnerId, ...result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-ledger/payments", requireStaff, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(503).json({ error: "Supplier ledger requires PostgreSQL.", code: "supplier_ledger_postgres_required" });
    }
    const supplierName = cleanText(request.body?.supplierName || request.body?.supplier || "");
    const partnerId = cleanText(request.body?.partnerId || "");
    const amount = normalizeFinanceMoney(request.body?.amount, 0);
    if (!supplierName && !partnerId) return response.status(400).json({ error: "supplierName or partnerId is required.", code: "supplier_ledger_identity_required" });
    if (!(amount > 0)) return response.status(400).json({ error: "Payment amount must be greater than zero.", code: "supplier_ledger_amount_required" });
    const entry = normalizeSupplierLedgerEntry({
      sourceKey: `payment:${crypto.randomUUID()}`,
      entryType: "payment",
      supplierName,
      partnerId,
      amount: Math.abs(amount),
      currency: cleanText(request.body?.currency || "RUB").toUpperCase() || "RUB",
      note: cleanText(request.body?.note || ""),
      occurredAt: request.body?.paidAt || request.body?.occurredAt || new Date().toISOString(),
      createdBy: requestUsername(request),
      raw: {
        source: "manual_payment",
        supplierName,
        partnerId,
        amount,
        note: cleanText(request.body?.note || ""),
      },
    });
    const row = await getPrisma().supplierLedgerEntry.create({
      data: {
        id: entry.id,
        sourceKey: entry.sourceKey,
        entryType: entry.entryType,
        supplierName: entry.supplierName || null,
        partnerId: entry.partnerId || null,
        amount: entry.amount,
        currency: entry.currency,
        note: entry.note || null,
        status: "active",
        occurredAt: toDateOrNull(entry.occurredAt) || new Date(),
        createdBy: entry.createdBy || null,
        raw: entry.raw,
      },
    });
    const saved = supplierLedgerEntryFromPostgres(row);
    suppliersListCache = null;
    await appendAudit(request, "supplier_ledger.payment_create", {
      entityType: "supplier_ledger",
      entityId: saved.id,
      newValue: saved,
    }).catch((error) => logger.warn("supplier ledger payment audit failed", { detail: error?.message || String(error) }));
    const summary = await listSupplierLedgerEntries({ supplierName, partnerId, status: "active", limit: 100, period: "all" });
    response.status(201).json({ ok: true, entry: saved, summary: summary.summary });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-ledger/return-picking", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(503).json({ error: "Supplier ledger requires PostgreSQL.", code: "supplier_ledger_postgres_required" });
    }
    const pickingKey = cleanText(request.body?.pickingKey || "");
    const note = cleanText(request.body?.note || "");
    if (!pickingKey) return response.status(400).json({ error: "pickingKey is required.", code: "picking_key_required" });

    const debtEntry = await getPrisma().supplierLedgerEntry.findFirst({
      where: { pickingKey, entryType: "purchase_debt", status: "active" },
    });
    if (!debtEntry) return response.status(404).json({ error: "Запись долга для этой строки сборки не найдена.", code: "debt_entry_not_found" });

    const existingReturn = await getPrisma().supplierLedgerEntry.findFirst({
      where: { pickingKey, entryType: "supplier_return", status: "active" },
    });
    if (existingReturn) return response.status(409).json({ error: "Возврат для этой строки уже зафиксирован.", code: "return_already_exists", entry: supplierLedgerEntryFromPostgres(existingReturn) });

    const amount = Math.abs(Number(debtEntry.amount));
    const entry = normalizeSupplierLedgerEntry({
      sourceKey: `supplier_return:picking:${pickingKey}`,
      entryType: "supplier_return",
      supplierName: debtEntry.supplierName,
      partnerId: debtEntry.partnerId,
      amount,
      currency: debtEntry.currency || "RUB",
      pickingKey,
      financeOrderId: debtEntry.financeOrderId,
      orderId: debtEntry.orderId,
      postingNumber: debtEntry.postingNumber,
      offerId: debtEntry.offerId,
      productName: debtEntry.productName,
      quantity: debtEntry.quantity,
      note: note || "Возврат товара поставщику",
      occurredAt: new Date().toISOString(),
      createdBy: requestUsername(request),
      raw: { source: "return_picking", pickingKey, debtEntryId: debtEntry.id },
    });
    const row = await getPrisma().supplierLedgerEntry.create({
      data: {
        id: entry.id,
        sourceKey: entry.sourceKey,
        entryType: entry.entryType,
        supplierName: entry.supplierName || null,
        partnerId: entry.partnerId || null,
        amount: entry.amount,
        currency: entry.currency,
        pickingKey: entry.pickingKey || null,
        financeOrderId: entry.financeOrderId || null,
        orderId: entry.orderId || null,
        postingNumber: entry.postingNumber || null,
        offerId: entry.offerId || null,
        productName: entry.productName || null,
        quantity: entry.quantity,
        note: entry.note || null,
        status: "active",
        occurredAt: toDateOrNull(entry.occurredAt) || new Date(),
        createdBy: entry.createdBy || null,
        raw: entry.raw,
      },
    });
    const saved = supplierLedgerEntryFromPostgres(row);
    suppliersListCache = null;
    await appendAudit(request, "supplier_ledger.return_picking", {
      entityType: "supplier_ledger",
      entityId: saved.id,
      newValue: saved,
    }).catch((e) => logger.warn("supplier ledger return-picking audit failed", { detail: e?.message }));
    const summary = await listSupplierLedgerEntries({ supplierName: debtEntry.supplierName || "", partnerId: debtEntry.partnerId || "", status: "active", limit: 100, period: "all" });
    response.status(201).json({ ok: true, entry: saved, summary: summary.summary });
  } catch (error) {
    next(error);
  }
});

// Пересчёт долгов поставщика с неверной валютой (например, Инна — цены в ₽ но хранились как USD)
app.post("/api/supplier-ledger/fix-rub-amounts", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(503).json({ error: "Supplier ledger requires PostgreSQL.", code: "supplier_ledger_postgres_required" });
    }
    const supplierName = cleanText(request.body?.supplierName || "");
    const partnerId = cleanText(request.body?.partnerId || "");
    if (!supplierName && !partnerId) return response.status(400).json({ error: "supplierName or partnerId is required." });
    // Load all active purchase_debt entries for this supplier
    const where = {
      AND: [
        supplierLedgerIdentityWhere({ supplierName, partnerId }),
        { status: "active" },
        { entryType: "purchase_debt" },
      ],
    };
    const rows = await getPrisma().supplierLedgerEntry.findMany({ where });
    let fixed = 0;
    let skipped = 0;
    const details = [];
    for (const row of rows) {
      const raw = row.raw || {};
      const picking = raw.picking || {};
      const currency = String(picking.priceCurrency || picking.currency || "").toUpperCase();
      // Only fix entries that were NOT explicitly stored as RUB
      if (currency === "RUB" || currency === "RUR") { skipped++; continue; }
      const price = Number(picking.price || 0);
      const qty = Math.max(1, Math.round(Number(picking.quantity || 1)));
      if (!(price > 0)) { skipped++; continue; }
      const correctAmount = -normalizeFinanceMoney(price * qty, 0);
      const oldAmount = Number(row.amount);
      if (Math.abs(correctAmount - oldAmount) < 0.01) { skipped++; continue; }
      await getPrisma().supplierLedgerEntry.update({
        where: { id: row.id },
        data: {
          amount: correctAmount,
          raw: { ...raw, picking: { ...picking, priceCurrency: "RUB", _fixedFromCurrency: currency, _fixedAt: new Date().toISOString() } },
        },
      });
      details.push({ id: row.id, sourceKey: row.sourceKey, oldAmount, newAmount: correctAmount });
      fixed++;
    }
    suppliersListCache = null;
    await appendAudit(request, "supplier_ledger.fix_rub_amounts", {
      entityType: "supplier_ledger",
      entityId: supplierName || partnerId,
      newValue: { fixed, skipped, total: rows.length },
    }).catch(() => {});
    logger.info("supplier ledger fix-rub-amounts", { supplierName, partnerId, fixed, skipped, total: rows.length, by: request.session?.username });
    response.json({ ok: true, fixed, skipped, total: rows.length, details });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/supplier-ledger/reset-all", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(503).json({ error: "Supplier ledger requires PostgreSQL.", code: "supplier_ledger_postgres_required" });
    }
    const result = await getPrisma().supplierLedgerEntry.deleteMany({});
    suppliersListCache = null;
    logger.info("supplier ledger reset", { deleted: result.count, by: request.session?.username });
    await appendAudit(request, "supplier_ledger.reset_all", {
      entityType: "supplier_ledger",
      entityId: "all",
      newValue: { deleted: result.count },
    }).catch((error) => logger.warn("supplier ledger reset audit failed", { detail: error?.message || String(error) }));
    response.json({ ok: true, deleted: result.count });
  } catch (error) {
    next(error);
  }
});

// Comprehensive reset: clears ledger entries + picking history + cart drafts for all suppliers
app.delete("/api/supplier-ledger/reset-all-history", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(503).json({ error: "Supplier ledger requires PostgreSQL.", code: "supplier_ledger_postgres_required" });
    }
    const [ledger, picking, cart] = await Promise.all([
      getPrisma().supplierLedgerEntry.deleteMany({}),
      getPrisma().supplierPickingRow.deleteMany({}),
      getPrisma().supplierCartDraft.deleteMany({}),
    ]);
    suppliersListCache = null;
    logger.info("supplier full history reset", { ledger: ledger.count, picking: picking.count, cart: cart.count, by: request.session?.username });
    await appendAudit(request, "supplier_ledger.reset_all_history", {
      entityType: "supplier_ledger",
      entityId: "all",
      newValue: { ledger: ledger.count, picking: picking.count, cart: cart.count },
    }).catch((error) => logger.warn("supplier ledger reset history audit failed", { detail: error?.message || String(error) }));
    response.json({ ok: true, ledger: ledger.count, picking: picking.count, cart: cart.count });
  } catch (error) {
    next(error);
  }
});

// Корректировка: устанавливает баланс в targetBalance, создавая запись balance_correction
app.post("/api/supplier-ledger/adjust", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(503).json({ error: "Supplier ledger requires PostgreSQL.", code: "supplier_ledger_postgres_required" });
    }
    const supplierName = cleanText(request.body?.supplierName || request.body?.supplier || "");
    const partnerId = cleanText(request.body?.partnerId || "");
    const targetBalance = normalizeFinanceMoney(request.body?.targetBalance, null);
    const note = cleanText(request.body?.note || "");
    const currency = cleanText(request.body?.currency || "RUB").toUpperCase() || "RUB";
    if (!supplierName && !partnerId) return response.status(400).json({ error: "supplierName or partnerId is required.", code: "supplier_ledger_identity_required" });
    if (targetBalance === null || !Number.isFinite(targetBalance)) return response.status(400).json({ error: "targetBalance is required.", code: "target_balance_required" });
    const current = await listSupplierLedgerEntries({ supplierName, partnerId, status: "active", limit: 1000, period: "all" });
    const currentBalance = normalizeFinanceMoney(current.summary.balance, 0);
    const delta = targetBalance - currentBalance;
    if (Math.abs(delta) < 0.005) {
      return response.json({ ok: true, skipped: true, currentBalance, targetBalance, delta: 0, message: "Баланс уже совпадает, корректировка не нужна." });
    }
    const entry = normalizeSupplierLedgerEntry({
      sourceKey: `balance_correction:${crypto.randomUUID()}`,
      entryType: "balance_correction",
      supplierName,
      partnerId,
      amount: delta,
      currency,
      note: note || `Корректировка баланса: ${currentBalance} → ${targetBalance}`,
      occurredAt: new Date().toISOString(),
      createdBy: requestUsername(request),
      raw: { source: "balance_correction", supplierName, partnerId, currentBalance, targetBalance, delta, note },
    });
    const row = await getPrisma().supplierLedgerEntry.create({
      data: {
        id: entry.id,
        sourceKey: entry.sourceKey,
        entryType: entry.entryType,
        supplierName: entry.supplierName || null,
        partnerId: entry.partnerId || null,
        amount: entry.amount,
        currency: entry.currency,
        note: entry.note || null,
        status: "active",
        occurredAt: toDateOrNull(entry.occurredAt) || new Date(),
        createdBy: entry.createdBy || null,
        raw: entry.raw,
      },
    });
    const saved = supplierLedgerEntryFromPostgres(row);
    suppliersListCache = null;
    await appendAudit(request, "supplier_ledger.balance_adjust", {
      entityType: "supplier_ledger",
      entityId: saved.id,
      newValue: { ...saved, currentBalance, targetBalance, delta },
    }).catch((error) => logger.warn("supplier ledger adjust audit failed", { detail: error?.message || String(error) }));
    const summary = await listSupplierLedgerEntries({ supplierName, partnerId, status: "active", limit: 100, period: "all" });
    response.status(201).json({ ok: true, entry: saved, summary: summary.summary, currentBalance, targetBalance, delta });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-ledger/returns", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(503).json({ error: "Supplier ledger requires PostgreSQL.", code: "supplier_ledger_postgres_required" });
    }
    const supplierName = cleanText(request.body?.supplierName || request.body?.supplier || "");
    const partnerId = cleanText(request.body?.partnerId || "");
    const amount = normalizeFinanceMoney(request.body?.amount, 0);
    if (!supplierName && !partnerId) return response.status(400).json({ error: "supplierName or partnerId is required.", code: "supplier_ledger_identity_required" });
    if (!(amount > 0)) return response.status(400).json({ error: "Return amount must be greater than zero.", code: "supplier_ledger_amount_required" });
    const entry = normalizeSupplierLedgerEntry({
      sourceKey: `supplier_return:${crypto.randomUUID()}`,
      entryType: "supplier_return",
      supplierName,
      partnerId,
      amount: Math.abs(amount),
      currency: cleanText(request.body?.currency || "RUB").toUpperCase() || "RUB",
      note: cleanText(request.body?.note || ""),
      occurredAt: request.body?.occurredAt || new Date().toISOString(),
      createdBy: requestUsername(request),
      raw: { source: "manual_return", supplierName, partnerId, amount, note: cleanText(request.body?.note || "") },
    });
    const row = await getPrisma().supplierLedgerEntry.create({
      data: {
        id: entry.id,
        sourceKey: entry.sourceKey,
        entryType: entry.entryType,
        supplierName: entry.supplierName || null,
        partnerId: entry.partnerId || null,
        amount: entry.amount,
        currency: entry.currency,
        note: entry.note || null,
        status: "active",
        occurredAt: toDateOrNull(entry.occurredAt) || new Date(),
        createdBy: entry.createdBy || null,
        raw: entry.raw,
      },
    });
    const saved = supplierLedgerEntryFromPostgres(row);
    suppliersListCache = null;
    await appendAudit(request, "supplier_ledger.return_create", {
      entityType: "supplier_ledger",
      entityId: saved.id,
      newValue: saved,
    }).catch((error) => logger.warn("supplier ledger return audit failed", { detail: error?.message || String(error) }));
    const summary = await listSupplierLedgerEntries({ supplierName, partnerId, status: "active", limit: 100, period: "all" });
    response.status(201).json({ ok: true, entry: saved, summary: summary.summary });
  } catch (error) {
    next(error);
  }
});

