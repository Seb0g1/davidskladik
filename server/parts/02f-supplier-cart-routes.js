app.get("/api/supplier-cart/preview", requireAdmin, async (request, response, next) => {
  try {
    const preview = await buildSupplierCartPreview({
      marketplace: request.query.marketplace,
      from: request.query.from,
      to: request.query.to,
      limit: request.query.limit,
    });
    response.json(preview);
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/draft", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    const rows = state.draft?.rows || [];
    const ready = rows.filter((row) => row.ready && !row.alreadyCommitted).length;
    const alreadyCommitted = rows.filter((row) => row.alreadyCommitted).length;
    const skipped = rows.length - ready - alreadyCommitted;
    response.json({
      ok: true,
      draftId: state.draft?.id || "",
      generatedAt: state.draft?.generatedAt || null,
      generatedBy: state.draft?.generatedBy || "",
      rows,
      total: rows.length,
      ready,
      skipped,
      alreadyCommitted,
      warnings: state.draft?.summary?.warnings || [],
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/schedule", requireAdmin, async (_request, response, next) => {
  try {
    const settings = normalizeSupplierCartSettings((await readAppSettings()).supplierCart || {});
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/supplier-cart/schedule", requireAdmin, async (request, response, next) => {
  try {
    const appSettings = await readAppSettings();
    const settings = normalizeSupplierCartSettings({
      ...(appSettings.supplierCart || {}),
      ...(request.body || {}),
    });
    await writeAppSettings({
      ...appSettings,
      supplierCart: settings,
    });
    await scheduleSupplierCartAuto();
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/generate", requireAdmin, async (request, response, next) => {
  try {
    const preview = await generateSupplierCartDraft({
      marketplace: request.body?.marketplace || request.query.marketplace,
      from: request.body?.from || request.query.from,
      to: request.body?.to || request.query.to,
      limit: request.body?.limit || request.query.limit,
    }, request);
    response.json(preview);
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/commit", requireAdmin, async (request, response, next) => {
  try {
    const rows = Array.isArray(request.body?.rows) ? request.body.rows : [];
    const keys = Array.isArray(request.body?.keys) ? request.body.keys : [];
    const state = await readSupplierCartState();
    const sourceRows = rows.length
      ? rows
      : (state.draft?.rows?.length ? state.draft.rows : (await buildSupplierCartPreview(request.body || {})).rows);
    const selectedKeys = new Set(keys.map(cleanText).filter(Boolean));
    const selectedRows = selectedKeys.size ? sourceRows.filter((row) => selectedKeys.has(cleanText(row.key))) : sourceRows;
    const result = await insertSupplierCartRowsIntoPriceMaster(selectedRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      skipped: result.skipped,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      verifiedInPriceMaster: Boolean(result.verification?.ok),
      verifiedRows: Number(result.verification?.verifiedRows || 0),
      priceMasterDb: result.verification?.db || "",
      rows: result.inserted,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/pricemaster/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await getPriceMasterBasketStatus());
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/rollback-all", requireAdmin, async (request, response, next) => {
  try {
    const confirm = cleanText(request.body?.confirm);
    if (confirm !== "ROLLBACK_DAVIDSKLAD_SUPPLIER_CART") {
      return response.status(400).json({
        error: "Confirmation is required.",
        code: "supplier_cart_rollback_confirmation_required",
      });
    }
    const result = await rollbackSupplierCartAll(request, { dryRun: request.body?.dryRun !== false });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/manual-order", requireAdmin, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const quantity = Math.max(1, Math.round(Number(request.body?.quantity || 1) || 1));
    const marketplace = cleanText(request.body?.marketplace || "ozon").toLowerCase();
    const note = cleanText(request.body?.note || "").slice(0, 200);
    if (!offerId) return response.status(400).json({ error: "offerId is required.", code: "manual_order_no_offer_id" });
    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
    const product = findSupplierCartWarehouseProduct(warehouse, { offerId, marketplace, accountId: "" });
    if (!product) return response.status(404).json({ error: "Товар не найден на складе.", code: "product_not_found" });
    const state = await readSupplierCartState();
    const manualKey = `manual|manual|manual-${Date.now()}|${offerId}`;
    const manualLine = normalizeSupplierCartLine({
      key: manualKey,
      marketplace: "manual",
      offerId,
      quantity,
      productName: cleanText(product.productName || product.name || offerId),
      orderId: `manual-${Date.now()}`,
      accountId: "manual",
      accountName: "Ручной заказ",
    });
    const row = await resolveSupplierCartRow(warehouse, manualLine, state);
    if (!row.ready) {
      return response.status(400).json({
        error: "Не удалось подобрать поставщика.",
        skipReason: row.skipReason || "supplier_not_found",
        code: "manual_order_no_supplier",
      });
    }
    const rowWithNote = { ...row, quantity, manualNote: note };
    const result = await insertSupplierCartRowsIntoPriceMaster([rowWithNote], request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      row: result.inserted[0] || null,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/history", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    response.json({
      ok: true,
      updatedAt: state.updatedAt,
      totalProcessed: Object.keys(state.processed || {}).length,
      history: (state.history || []).slice().reverse(),
    });
  } catch (error) {
    next(error);
  }
});

