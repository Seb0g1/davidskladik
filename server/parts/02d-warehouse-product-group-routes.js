app.patch("/api/warehouse/products/auto-price/all", async (request, response, next) => {
  try {
    const enabled = Boolean(request.body.enabled);
    const warehouse = await readWarehouse();
    let changed = 0;
    const changedIds = [];
    for (const product of warehouse.products) {
      if (Boolean(product.autoPriceEnabled !== false) === enabled) continue;
      product.autoPriceEnabled = enabled;
      product.updatedAt = new Date().toISOString();
      product.userUpdatedAt = product.updatedAt;
      changed += 1;
      changedIds.push(product.id);
    }
    await writeWarehouse(warehouse);
    await appendAudit(request, "warehouse.auto_price.all_update", { productIds: changedIds, newValue: { enabled } });
    response.json({ ok: true, changed, products: [] });
    if (enabled) {
      queueAuthoritativePriceReprice({
        marketplace: "all",
        reason: "auto_all_enable",
        sourceEvent: "auto_all_enable",
        force: true,
        onlyChanged: false,
        refreshMarketplacePrices: true,
        livePriceMaster: true,
        verify: true,
        priority: QUEUE_PRIORITY.PRICE_BACKGROUND,
      }).catch((error) => logger.warn("all auto-price enable reprice queue failed", { detail: error?.message || String(error) }));
    }
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/group", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    if (Object.prototype.hasOwnProperty.call(request.body || {}, "stockOnlyManualPrices")) {
      if (!ids.size) return response.status(400).json({ error: "Select a product group for stock-only fallback prices." });
      const warehouse = await readWarehouse();
      const seedProducts = warehouse.products.filter((product) => ids.has(String(product.id)));
      const productsToChange = expandWarehouseProductsToGroups(warehouse.products, seedProducts);
      const prices = normalizeStockOnlyManualPrices(request.body.stockOnlyManualPrices);
      const oldValues = [];
      const now = new Date().toISOString();
      for (const product of productsToChange) {
        oldValues.push(cloneAuditValue({ id: product.id, stockOnlyManualPrices: product.stockOnlyManualPrices || null, updatedAt: product.updatedAt }));
        product.stockOnlyManualPrices = prices;
        product.updatedAt = now;
        product.userUpdatedAt = product.updatedAt;
      }
      if (productsToChange.length) {
        await writeWarehouseProductPatch(productsToChange, { reason: "warehouse_stock_only_manual_prices", writeLinks: false });
      }
      const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, productsToChange);
      await appendAudit(request, "warehouse.stock_only_manual_prices.save", { productIds: productsToChange.map((product) => product.id), oldValue: oldValues, newValue: prices });
      return response.json({ ok: true, changed: productsToChange.length, stockOnlyManualPrices: prices, products });
    }
    if (ids.size < 2) return response.status(400).json({ error: "Выберите минимум два товара для объединения." });
    const warehouse = await readWarehouse();
    const productsToChange = warehouse.products.filter((product) => ids.has(product.id));
    const conflicts = collectProductConflicts(productsToChange, productLocksFromRequest(request.body));
    if (conflicts.length) return conflictResponse(response, conflicts);
    const groupId = cleanText(request.body.groupId) || `manual-${crypto.randomUUID()}`;
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, manualGroupId: product.manualGroupId, updatedAt: product.updatedAt }));
      product.manualGroupId = groupId;
      product.updatedAt = new Date().toISOString();
      product.userUpdatedAt = product.updatedAt;
      changed += 1;
      changedIds.push(product.id);
    }
    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_group", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.group", { productIds: changedIds, oldValue: oldValues, newValue: { groupId } });
    response.json({ ok: true, groupId, changed, products });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/ungroup", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для разъединения." });
    const warehouse = await readWarehouse();
    const productsToChange = warehouse.products.filter((product) => ids.has(product.id));
    const conflicts = collectProductConflicts(productsToChange, productLocksFromRequest(request.body));
    if (conflicts.length) return conflictResponse(response, conflicts);
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, manualGroupId: product.manualGroupId, updatedAt: product.updatedAt }));
      product.manualGroupId = "";
      product.updatedAt = new Date().toISOString();
      product.userUpdatedAt = product.updatedAt;
      changed += 1;
      changedIds.push(product.id);
    }
    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_ungroup", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.ungroup", { productIds: changedIds, oldValue: oldValues, newValue: { groupId: "" } });
    response.json({ ok: true, changed, products });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body?.expectedUpdatedAt || request.query?.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    warehouse.products = warehouse.products.filter((product) => product.id !== request.params.id);
    await writeWarehouse(warehouse);
    await appendAudit(request, "warehouse.product.delete", { productId: product.id, offerId: product.offerId, oldValue: product });
    response.json({ ok: true, deletedId: request.params.id });
  } catch (error) {
    next(error);
  }
});



