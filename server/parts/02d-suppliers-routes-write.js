app.post("/api/suppliers", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = normalizeManagedSupplier(request.body);
    if (!supplier.name) return response.status(400).json({ error: "Укажите название поставщика." });

    const index = warehouse.suppliers.findIndex((item) => item.id === supplier.id);
    const before = index >= 0 ? cloneAuditValue(warehouse.suppliers[index]) : null;
    if (index >= 0) {
      warehouse.suppliers[index] = normalizeManagedSupplier({
        ...warehouse.suppliers[index],
        ...supplier,
        articles: warehouse.suppliers[index].articles,
        createdAt: warehouse.suppliers[index].createdAt,
      });
    } else {
      warehouse.suppliers.push(supplier);
    }
    const after = index >= 0 ? warehouse.suppliers[index] : supplier;

    await appendAudit(request, "supplier.save", {
      id: supplier.id,
      name: supplier.name,
      priceCurrency: supplier.priceCurrency,
      oldValue: before,
      newValue: after,
    });
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
    queueAuthoritativePriceReprice({
      marketplace: "all",
      reason: "supplier_save",
      sourceEvent: "supplier_save",
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: QUEUE_PRIORITY.PRICE_BACKGROUND,
    }).catch((error) => logger.warn("supplier save reprice queue failed", { detail: error?.message || String(error) }));
  } catch (error) {
    next(error);
  }
});

app.patch("/api/suppliers/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = warehouse.suppliers.find((item) => item.id === request.params.id);
    if (!supplier) return response.status(404).json({ error: "Поставщик не найден." });
    const before = cloneAuditValue(supplier);

    Object.assign(supplier, {
      name: request.body.name !== undefined ? cleanText(request.body.name) : supplier.name,
      stopped: request.body.stopped !== undefined ? Boolean(request.body.stopped) : supplier.stopped,
      note: request.body.note !== undefined ? cleanText(request.body.note) : supplier.note,
      stopReason: request.body.stopReason !== undefined ? cleanText(request.body.stopReason) : supplier.stopReason,
      trustFactor: request.body.trustFactor !== undefined || request.body.trust_factor !== undefined
        ? normalizeSupplierTrustFactor(request.body.trustFactor ?? request.body.trust_factor, supplier.trustFactor || 100)
        : normalizeSupplierTrustFactor(supplier.trustFactor, 100),
      orderCutoffTime: request.body.orderCutoffTime !== undefined || request.body.order_cutoff_time !== undefined
        ? normalizeSupplierOrderCutoff(request.body.orderCutoffTime || request.body.order_cutoff_time)
        : normalizeSupplierOrderCutoff(supplier.orderCutoffTime || ""),
      reseller: request.body.reseller !== undefined ? Boolean(request.body.reseller) : Boolean(supplier.reseller),
      priceCurrency: request.body.priceCurrency !== undefined
        ? normalizeManagedSupplier({ priceCurrency: request.body.priceCurrency }).priceCurrency
        : (supplier.priceCurrency || "USD"),
      pricingMode: request.body.pricingMode !== undefined || request.body.pricing_mode !== undefined
        ? normalizeSupplierPricingMode(request.body)
        : normalizeSupplierPricingMode(supplier),
      stockOnly: request.body.pricingMode !== undefined || request.body.pricing_mode !== undefined
        ? normalizeSupplierPricingMode(request.body) === "stock_only"
        : normalizeSupplierPricingMode(supplier) === "stock_only",
      inactiveComment: request.body.inactiveComment !== undefined ? cleanText(request.body.inactiveComment) : (supplier.inactiveComment || ""),
      inactiveUntil: request.body.inactiveUntil !== undefined ? (cleanText(request.body.inactiveUntil) || null) : (supplier.inactiveUntil || null),
      inactiveUntilUnknown: request.body.inactiveUntilUnknown !== undefined ? Boolean(request.body.inactiveUntilUnknown) : Boolean(supplier.inactiveUntilUnknown),
      updatedAt: new Date().toISOString(),
    });

    if (!supplier.stopped) {
      supplier.stopReason = "";
      supplier.inactiveComment = "";
      supplier.inactiveUntil = null;
      supplier.inactiveUntilUnknown = false;
    } else if (!supplier.inactiveUntil) {
      supplier.inactiveUntilUnknown = true;
    }

    const saved = await writeWarehouse(warehouse);
    const priceCurrencyChanged = before.priceCurrency !== supplier.priceCurrency;
    await appendAudit(request, "supplier.update", {
      id: supplier.id,
      stopped: supplier.stopped,
      name: supplier.name,
      priceCurrency: supplier.priceCurrency,
      oldValue: before,
      newValue: supplier,
    });
    response.json({ ok: true, warehouse: saved });
    const affectedProductIds = supplierImpactProductIds(warehouse, before, supplier);
    if (affectedProductIds.length) {
      queueMarketplaceJob("no-supplier-automation", { productIds: affectedProductIds, skipLinkedGrace: Boolean(supplier.stopped) }, { priority: QUEUE_PRIORITY.RECOVERY });
      if (!supplier.stopped) {
        queueLinkedProductActivation(affectedProductIds, "supplier_update", {
          username: requestUsername(request),
        }).catch((error) => logger.warn("supplier update activation failed", { detail: error?.message || String(error) }));
      }
      if (priceCurrencyChanged) {
        queueAuthoritativePriceReprice({
          productIds: affectedProductIds,
          marketplace: "all",
          reason: "supplier_currency_update",
          sourceEvent: "supplier_currency_update",
          force: true,
          onlyChanged: false,
          refreshMarketplacePrices: true,
          livePriceMaster: true,
          verify: true,
          priority: QUEUE_PRIORITY.PRICE_IMMEDIATE,
        }).catch((error) => logger.warn("supplier currency reprice queue failed", { detail: error?.message || String(error) }));
      }
    }
    if (!affectedProductIds.length) {
      queueAuthoritativePriceReprice({
        marketplace: "all",
        reason: "supplier_update",
        sourceEvent: "supplier_update",
        force: true,
        onlyChanged: false,
        refreshMarketplacePrices: true,
        livePriceMaster: true,
        verify: true,
        priority: QUEUE_PRIORITY.PRICE_BACKGROUND,
      }).catch((error) => logger.warn("supplier update reprice queue failed", { detail: error?.message || String(error) }));
    }
  } catch (error) {
    next(error);
  }
});

app.delete("/api/suppliers/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const before = warehouse.suppliers.find((supplier) => supplier.id === request.params.id) || null;
    const affectedProductIds = supplierImpactProductIds(warehouse, before);
    warehouse.suppliers = warehouse.suppliers.filter((supplier) => supplier.id !== request.params.id);
    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.delete", { id: request.params.id, oldValue: before });
    response.json({ ok: true, warehouse: saved });
    if (affectedProductIds.length) {
      queueMarketplaceJob("no-supplier-automation", { productIds: affectedProductIds }, { priority: QUEUE_PRIORITY.RECOVERY });
    }
  } catch (error) {
    next(error);
  }
});

app.post("/api/suppliers/:id/articles", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = warehouse.suppliers.find((item) => item.id === request.params.id);
    if (!supplier) return response.status(404).json({ error: "Поставщик не найден." });

    const article = normalizeSupplierArticle(request.body);
    if (!article.article) return response.status(400).json({ error: "Укажите артикул поставщика." });
    supplier.articles = Array.isArray(supplier.articles) ? supplier.articles : [];
    const index = supplier.articles.findIndex((item) => item.id === article.id);
    const before = index >= 0 ? cloneAuditValue(supplier.articles[index]) : null;
    if (index >= 0) supplier.articles[index] = article;
    else supplier.articles.push(article);

    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.article.save", { supplierId: supplier.id, article: article.article, oldValue: before, newValue: article });
    response.json({ ok: true, warehouse: saved });
    queueAuthoritativePriceReprice({
      marketplace: "all",
      reason: "supplier_article_save",
      sourceEvent: "supplier_article_save",
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: QUEUE_PRIORITY.PRICE_BACKGROUND,
    }).catch((error) => logger.warn("supplier article reprice queue failed", { detail: error?.message || String(error) }));
  } catch (error) {
    next(error);
  }
});

app.delete("/api/suppliers/:supplierId/articles/:articleId", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = warehouse.suppliers.find((item) => item.id === request.params.supplierId);
    if (!supplier) return response.status(404).json({ error: "Поставщик не найден." });
    const before = (supplier.articles || []).find((article) => article.id === request.params.articleId) || null;
    supplier.articles = (supplier.articles || []).filter((article) => article.id !== request.params.articleId);
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
    await appendAudit(request, "supplier.article.delete", { supplierId: supplier.id, articleId: request.params.articleId, oldValue: before });
    queueMarketplaceJob("no-supplier-automation", {}, { priority: QUEUE_PRIORITY.RECOVERY });
  } catch (error) {
    next(error);
  }
});


