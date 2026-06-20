// Snooze: temporarily hide a product from marketplace for N days, then auto-reactivate
// when the supplier has it back in their price list.
//
// POST /api/warehouse/products/:id/snooze  { days: 5 }
//   → sets raw.snooze, zeroes targetStock, sends stock=0 to marketplace
//
// DELETE /api/warehouse/products/:id/snooze
//   → clears snooze, queues supplier recovery

const SNOOZE_DEFAULT_DAYS = 5;
const SNOOZE_MAX_DAYS = 60;

app.post("/api/warehouse/products/:id/snooze", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });

    const days = Math.max(1, Math.min(SNOOZE_MAX_DAYS, Number(request.body.days || SNOOZE_DEFAULT_DAYS) || SNOOZE_DEFAULT_DAYS));
    const now = new Date().toISOString();
    const snoozedUntil = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();

    product.snooze = { snoozedAt: now, snoozedUntil, days };
    product.targetStock = 0;
    product.updatedAt = now;
    product.userUpdatedAt = now;

    await writeWarehouseProductPatch([product], { reason: "snooze", writeLinks: false });

    // Zero marketplace stock immediately (best-effort — stock sweep is excluded for snoozed products)
    const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]).catch(() => []);
    const target = freshProduct || product;
    if (target.offerId && target.target) {
      sendZeroStocksToMarketplace([target]).catch((error) => {
        logger.warn("snooze zero-stock send failed", { productId: product.id, detail: error?.message || String(error) });
      });
    }

    await appendAudit(request, "warehouse.product.snooze", { productId: product.id, offerId: product.offerId, days, snoozedUntil });
    response.json({ ok: true, product: normalizeWarehouseProduct(product) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:id/snooze", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });

    product.snooze = null;
    product.updatedAt = new Date().toISOString();
    product.userUpdatedAt = product.updatedAt;

    await writeWarehouseProductPatch([product], { reason: "snooze_cancel", writeLinks: false });
    await appendAudit(request, "warehouse.product.snooze_cancel", { productId: product.id, offerId: product.offerId });

    // Queue recovery so the product comes back to marketplace quickly
    queueMarketplaceJob("linked-supplier-recovery", { productIds: [product.id], source: "snooze_cancel" }, { priority: QUEUE_PRIORITY.RECOVERY })
      .catch((error) => logger.warn("snooze cancel recovery queue failed", { productId: product.id, detail: error?.message || String(error) }));

    response.json({ ok: true, product: normalizeWarehouseProduct(product) });
  } catch (error) {
    next(error);
  }
});
