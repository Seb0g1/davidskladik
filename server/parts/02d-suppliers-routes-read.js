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
    const ledgerMap = await supplierLedgerSummaryMapForSuppliers(normalizedSuppliers);
    const payload = {
      suppliers: normalizedSuppliers.map((supplier) => ({
        ...supplier,
        impactProductCount: impactCounts.get(supplier.id) || 0,
        ledger: ledgerMap.get(cleanText(supplier.id || supplier.partnerId || supplier.name)) || supplierLedgerSummaryFromEntries([]),
      })),
      supplierSync,
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

