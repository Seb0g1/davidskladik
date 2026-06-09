app.patch("/api/warehouse/products/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const expectedUpdatedAt = cleanText(request.body.expectedUpdatedAt || "");
    if (expectedUpdatedAt && cleanText(product.updatedAt || "") !== expectedUpdatedAt) {
      return response.status(409).json({
        error: "Конфликт обновления: карточка уже изменена другим пользователем.",
        code: "warehouse_product_conflict",
        currentUpdatedAt: product.updatedAt || null,
      });
    }
    const before = cloneAuditValue(product);

    if (request.body.markup !== undefined) {
      const markup = Number(request.body.markup);
      product.markup = Number.isFinite(markup) && markup > 0 ? markup : 0;
    }
    if (request.body.autoPriceEnabled !== undefined) product.autoPriceEnabled = Boolean(request.body.autoPriceEnabled);
    if (request.body.autoPriceMin !== undefined) {
      const value = Number(request.body.autoPriceMin);
      product.autoPriceMin = Number.isFinite(value) && value > 0 ? roundPrice(value) : null;
    }
    if (request.body.autoPriceMax !== undefined) {
      const value = Number(request.body.autoPriceMax);
      product.autoPriceMax = Number.isFinite(value) && value > 0 ? roundPrice(value) : null;
    }
    if (request.body.keyword !== undefined) product.keyword = cleanText(request.body.keyword);
    product.updatedAt = new Date().toISOString();

    await writeWarehouseProductPatch([product], { reason: "warehouse_product_update", writeLinks: false });
    const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]);
    await appendAudit(request, "warehouse.product.update", {
      productId: product.id,
      offerId: product.offerId,
      oldValue: before,
      newValue: product,
    });
    response.json({ ok: true, product: freshProduct || normalizeWarehouseProduct(product) });
    queueAuthoritativePriceReprice({
      productIds: [product.id],
      marketplace: "all",
      reason: "product_patch",
      sourceEvent: "product_patch",
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: 1,
    }).catch((error) => logger.warn("product patch reprice queue failed", { detail: error?.message || String(error), productId: product.id }));
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/markups/bulk", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    const optimisticLocks = new Map(
      (Array.isArray(request.body.optimisticLocks) ? request.body.optimisticLocks : [])
        .map((item) => [String(item?.id || ""), cleanText(item?.expectedUpdatedAt || "")]),
    );
    const markup = Number(request.body.markup);
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для изменения наценки." });
    if (!Number.isFinite(markup) || markup <= 0) return response.status(400).json({ error: "Укажите наценку больше нуля." });

    const warehouse = await readWarehouse();
    const conflicts = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      const expectedUpdatedAt = optimisticLocks.get(product.id);
      if (!expectedUpdatedAt) continue;
      if (cleanText(product.updatedAt || "") !== expectedUpdatedAt) {
        conflicts.push({
          id: product.id,
          offerId: product.offerId || "",
          expectedUpdatedAt,
          currentUpdatedAt: product.updatedAt || null,
        });
      }
    }
    if (conflicts.length) {
      return response.status(409).json({
        error: "Конфликт обновления: часть карточек уже изменена другим пользователем.",
        code: "warehouse_bulk_conflict",
        conflicts,
      });
    }
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, markup: product.markup, updatedAt: product.updatedAt }));
      product.markup = markup;
      product.updatedAt = new Date().toISOString();
      changed += 1;
      changedIds.push(product.id);
    }

    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_markup_bulk_update", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.markups.bulk_update", {
      productIds: changedIds,
      oldValue: oldValues,
      newValue: products.map((product) => ({ id: product.id, markup: product.markup, updatedAt: product.updatedAt })),
    });
    response.json({ ok: true, changed, products });
    queueAuthoritativePriceReprice({
      productIds: Array.from(ids),
      marketplace: "all",
      reason: "bulk_markup_patch",
      sourceEvent: "bulk_markup_patch",
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: 1,
    }).catch((error) => logger.warn("bulk markup reprice queue failed", { detail: error?.message || String(error), count: ids.size }));
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/auto-price/bulk", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для изменения AUTO-режима." });
    const enabled = Boolean(request.body.enabled);

    const warehouse = await readWarehouse();
    const productsToChange = warehouse.products.filter((product) => ids.has(product.id));
    const conflicts = collectProductConflicts(productsToChange, productLocksFromRequest(request.body));
    if (conflicts.length) return conflictResponse(response, conflicts);
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, autoPriceEnabled: product.autoPriceEnabled, updatedAt: product.updatedAt }));
      product.autoPriceEnabled = enabled;
      product.updatedAt = new Date().toISOString();
      changed += 1;
      changedIds.push(product.id);
    }

    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_auto_price_bulk_update", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.auto_price.bulk_update", {
      productIds: changedIds,
      oldValue: oldValues,
      newValue: products.map((product) => ({ id: product.id, autoPriceEnabled: product.autoPriceEnabled, updatedAt: product.updatedAt })),
    });
    response.json({ ok: true, changed, products });
    if (enabled) {
      queueAuthoritativePriceReprice({
        productIds: Array.from(ids),
        marketplace: "all",
        reason: "bulk_auto_enable",
        sourceEvent: "bulk_auto_enable",
        force: true,
        onlyChanged: false,
        refreshMarketplacePrices: true,
        livePriceMaster: true,
        verify: true,
        priority: 1,
      }).catch((error) => logger.warn("bulk auto-price enable reprice queue failed", { detail: error?.message || String(error), count: ids.size }));
    }
  } catch (error) {
    next(error);
  }
});

