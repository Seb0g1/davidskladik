app.post("/api/warehouse/products", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const input = normalizeWarehouseProduct(request.body);
    if (!input.offerId) return response.status(400).json({ error: "Укажите артикул товара маркетплейса." });
    if (!input.name) return response.status(400).json({ error: "Укажите название товара." });

    const index = warehouse.products.findIndex(
      (product) => product.id === input.id || (product.target === input.target && product.offerId === input.offerId),
    );
    const before = index >= 0 ? cloneAuditValue(warehouse.products[index]) : null;
    if (index >= 0) {
      const current = warehouse.products[index];
      warehouse.products[index] = normalizeWarehouseProduct({
        ...current,
        ...input,
        productId: input.productId || current.productId,
        sku: input.sku || current.sku,
        productUrl: input.productUrl || current.productUrl,
        source: current.source || input.source,
        ozon: hasObjectData(input.ozon) ? input.ozon : current.ozon,
        yandex: hasObjectData(input.yandex) ? input.yandex : current.yandex,
        exports: { ...(current.exports || {}), ...(input.exports || {}) },
        aiImages: input.aiImages?.length ? input.aiImages : current.aiImages,
        priceHistory: current.priceHistory || [],
        links: current.links,
        createdAt: current.createdAt,
      });
    } else {
      warehouse.products.push(input);
    }

    // Creating/saving a product is a user action — stamp userUpdatedAt so optimistic locking
    // can later distinguish a real concurrent user edit from background sweep churn.
    const userStampedProduct = index >= 0 ? warehouse.products[index] : input;
    userStampedProduct.userUpdatedAt = userStampedProduct.updatedAt || new Date().toISOString();

    await writeWarehouse(warehouse);
    const product = index >= 0 ? warehouse.products[index] : input;
    const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]);
    await appendAudit(request, index >= 0 ? "warehouse.product.save" : "warehouse.product.create", {
      productId: product.id,
      offerId: product.offerId,
      oldValue: before,
      newValue: product,
    });
    response.json({ ok: true, product: freshProduct || normalizeWarehouseProduct(product), warehouse });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    response.json({ product });
  } catch (error) {
    next(error);
  }
});
