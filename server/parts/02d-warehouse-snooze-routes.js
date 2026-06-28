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

    // If no supplier remains after snooze (all links snoozed), zero stock immediately.
    // Expand to group siblings so all marketplace variants (Ozon + Yandex) get zeroed —
    // the siblings share the same physical product and should not keep selling independently.
    // Also copy the snooze onto the sibling's matching link so the stock sweep (which runs
    // every 3 min) does not restore the sibling back to targetStock=5 while the snooze is active.
    if (!target.selectedSupplier && target.hasLinks) {
      const snoozeData = { snoozedAt: now, snoozedUntil, days };
      const patched = { ...target, targetStock: 0 };
      const groupProducts = await hydrateWarehouseProductsForIds([productId], { expandGroups: true });
      const siblingsToZero = groupProducts
        .filter((p) => String(p.id) !== String(productId))
        .map((p) => ({
          ...p,
          targetStock: 0,
          // Snooze ALL sibling links (not just matching by link.id) because sibling products
          // (Ozon/Yandex variants) have their own link IDs that differ from the snoozed product's
          // linkId. Without this, hasSnoozedLinks stays false on the sibling and the stock sweep
          // restores it to targetStock=5 within 3 minutes. Preserve user-initiated snoozes.
          links: (p.links || []).map((link) => {
            if (link.snooze && !link.snooze.groupSnoozedByLinkId) return link;
            return { ...link, snooze: { ...snoozeData, groupSnoozedByLinkId: linkId } };
          }),
        }));
      const allToZero = [patched, ...siblingsToZero];
      const toWrite = siblingsToZero.length ? allToZero : [patched];
      await writeWarehouseProductPatch(toWrite, { reason: "snooze_link_zero", writeLinks: true });
      sendZeroStocksToMarketplace(allToZero).catch((error) => {
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

    const defaultStock = Math.max(1, Number(process.env.LINKED_DEFAULT_TARGET_STOCK || 5) || 5);
    const now = new Date().toISOString();
    const updatedLinks = product.links.map((link, i) => {
      if (i !== linkIndex) return link;
      const { snooze: _removed, ...rest } = link;
      return rest;
    });
    // Reset targetStock to default so the stock sweep SQL picks this product up immediately
    // (COALESCE(target_stock,0) <= 0 OR marketplace_stock < target_stock) and the cooldown
    // bypass works correctly even if a pre-snooze cached entry exists.
    const updatedProduct = { ...product, links: updatedLinks, updatedAt: now, userUpdatedAt: now, targetStock: defaultStock };

    await writeWarehouseProductPatch([updatedProduct], { reason: "snooze_link_cancel", writeLinks: true });

    // Remove the snooze from sibling products so they regain selectedSupplier and are included
    // in the recovery triggered below. Handles both new snoozes (groupSnoozedByLinkId present
    // in Postgres after normalizer fix) and legacy snoozes (no groupSnoozedByLinkId, written
    // before normalizeWarehouseLink was fixed to preserve the marker).
    const siblingGroupProducts = await hydrateWarehouseProductsForIds([productId], { expandGroups: true });
    const allGroupIds = Array.from(new Set(siblingGroupProducts.map((p) => String(p.id))));
    const siblingsToUnsnooze = siblingGroupProducts
      .filter((p) => String(p.id) !== String(productId))
      .map((p) => {
        let removedSnooze = false;
        const newLinks = (p.links || []).map((link) => {
          if (!link.snooze?.snoozedUntil) return link;
          // Preserve snoozes from a different group (different groupSnoozedByLinkId).
          if (link.snooze.groupSnoozedByLinkId && link.snooze.groupSnoozedByLinkId !== linkId) return link;
          // Remove: matches this group's linkId, OR legacy snooze without a marker (written
          // before the normalizer preserved groupSnoozedByLinkId — treat as group-propagated).
          removedSnooze = true;
          const { snooze: _removed, ...rest } = link;
          return rest;
        });
        if (!removedSnooze) return null;
        return { ...p, updatedAt: now, targetStock: defaultStock, links: newLinks };
      })
      .filter(Boolean);
    if (siblingsToUnsnooze.length) {
      await writeWarehouseProductPatch(siblingsToUnsnooze, { reason: "snooze_link_cancel_siblings", writeLinks: true });
    }

    await appendAudit(request, "warehouse.link.snooze_cancel", { productId, linkId });

    // Optimistically clear stockZeroAt in the response so the UI immediately shows that recovery
    // is in progress, rather than keeping the "stock zeroed" state until the async job finishes.
    const displayProduct = updatedProduct.noSupplierAutomation?.stockZeroAt
      ? { ...updatedProduct, noSupplierAutomation: { ...(updatedProduct.noSupplierAutomation || {}), stockZeroAt: null } }
      : updatedProduct;
    response.json({ ok: true, product: normalizeWarehouseProduct(displayProduct) });

    // Fire-and-forget: send stock=defaultStock to ALL group members (Ozon + Yandex) immediately,
    // without waiting for the BullMQ worker. Uses snapshot PM (livePriceMaster: false) for speed
    // and reliability — live PM queries can time out and silently prevent stock restoration.
    // The BullMQ job below handles full recovery (DB state, price push, archived products).
    void buildFreshWarehouseProducts(allGroupIds, { livePriceMaster: false })
      .then(async (freshProducts) => {
        const toRestore = freshProducts
          .filter((p) => p?.selectedSupplier && !p.hasSnoozedLinks)
          .map((p) => ({
            ...p,
            targetStock: Math.max(defaultStock, Math.round(Number(p.targetStock || 0)) || defaultStock),
          }));
        if (!toRestore.length) return;
        await restoreStocksOnMarketplaces(toRestore);
      })
      .catch((err) => logger.warn("snooze_cancel direct stock restore failed", {
        productId,
        detail: err?.message || String(err),
      }));

    queueLinkedProductActivation([productId], "snooze_cancel", warehouseLinkActivationRequestMeta([productId], { username: requestUsername(request) }))
      .catch((error) => logger.warn("snooze cancel recovery queue failed", { productId, detail: error?.message || String(error) }));
  } catch (error) {
    next(error);
  }
});
