// Snooze a specific supplier link for N days — excludes that link from supplier selection.
// When all links are snoozed, the product has no selectedSupplier and stock is zeroed immediately.
// After N days the snooze sweep checks if the supplier is back and queues recovery if so.
//
// POST /api/warehouse/products/:id/links/:linkId/snooze  { days: 5 }
// DELETE /api/warehouse/products/:id/links/:linkId/snooze

const SNOOZE_DEFAULT_DAYS = 5;
const SNOOZE_MAX_DAYS = 60;

app.post("/api/warehouse/products/:id/links/:linkId/snooze", async (request, response, next) => {
  try {
    const productId = cleanText(request.params.id);
    const linkId = cleanText(request.params.linkId);
    if (!productId || !linkId) return response.status(400).json({ error: "Не указан ID товара или привязки." });

    const [product] = await readWarehouseProductsFromPostgresByIds([productId]);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });

    const linkIndex = (product.links || []).findIndex((l) => l.id === linkId);
    if (linkIndex < 0) return response.status(404).json({ error: "Привязка не найдена." });

    const days = Math.max(1, Math.min(SNOOZE_MAX_DAYS, Number(request.body.days || SNOOZE_DEFAULT_DAYS) || SNOOZE_DEFAULT_DAYS));
    const now = new Date().toISOString();
    const snoozedUntil = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();

    const updatedLinks = product.links.map((link, i) =>
      i === linkIndex ? { ...link, snooze: { snoozedAt: now, snoozedUntil, days } } : link,
    );
    const updatedProduct = { ...product, links: updatedLinks, updatedAt: now, userUpdatedAt: now };

    await writeWarehouseProductPatch([updatedProduct], { reason: "snooze_link", writeLinks: true });

    const warehouse = await readWarehouse();
    const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [updatedProduct]).catch(() => [updatedProduct]);
    const target = freshProduct || updatedProduct;

    // If no supplier remains after snooze (all links snoozed), zero stock immediately
    if (!target.selectedSupplier && target.hasLinks) {
      const patched = { ...target, targetStock: 0 };
      await writeWarehouseProductPatch([patched], { reason: "snooze_link_zero", writeLinks: false });
      sendZeroStocksToMarketplace([patched]).catch((error) => {
        logger.warn("snooze link zero-stock send failed", { productId, linkId, detail: error?.message || String(error) });
      });
    }

    await appendAudit(request, "warehouse.link.snooze", { productId, linkId, days, snoozedUntil });
    response.json({ ok: true, product: normalizeWarehouseProduct(target) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:id/links/:linkId/snooze", async (request, response, next) => {
  try {
    const productId = cleanText(request.params.id);
    const linkId = cleanText(request.params.linkId);
    if (!productId || !linkId) return response.status(400).json({ error: "Не указан ID товара или привязки." });

    const [product] = await readWarehouseProductsFromPostgresByIds([productId]);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });

    const linkIndex = (product.links || []).findIndex((l) => l.id === linkId);
    if (linkIndex < 0) return response.status(404).json({ error: "Привязка не найдена." });

    const now = new Date().toISOString();
    const updatedLinks = product.links.map((link, i) => {
      if (i !== linkIndex) return link;
      const { snooze: _removed, ...rest } = link;
      return rest;
    });
    const updatedProduct = { ...product, links: updatedLinks, updatedAt: now, userUpdatedAt: now };

    await writeWarehouseProductPatch([updatedProduct], { reason: "snooze_link_cancel", writeLinks: true });
    await appendAudit(request, "warehouse.link.snooze_cancel", { productId, linkId });

    queueLinkedProductActivation([productId], "snooze_cancel", { username: requestUsername(request) })
      .catch((error) => logger.warn("snooze cancel recovery queue failed", { productId, detail: error?.message || String(error) }));

    // Optimistically clear stockZeroAt in the response so the UI immediately shows that recovery
    // is in progress, rather than keeping the "stock zeroed" state until the async job finishes.
    const displayProduct = updatedProduct.noSupplierAutomation?.stockZeroAt
      ? { ...updatedProduct, noSupplierAutomation: { ...(updatedProduct.noSupplierAutomation || {}), stockZeroAt: null } }
      : updatedProduct;
    response.json({ ok: true, product: normalizeWarehouseProduct(displayProduct) });
  } catch (error) {
    next(error);
  }
});
