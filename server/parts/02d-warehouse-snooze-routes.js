// Snooze a specific supplier link for N days — excludes that link from supplier selection.
// When all links are snoozed, the product has no selectedSupplier and stock is zeroed immediately.
// After N days the snooze sweep checks if the supplier is back and queues recovery if so.
//
// POST /api/warehouse/products/:id/links/:linkId/snooze  { days: 5 }
// DELETE /api/warehouse/products/:id/links/:linkId/snooze

const SNOOZE_DEFAULT_DAYS = 5;
const SNOOZE_MAX_DAYS = 60;

async function applyWarehouseLinkSnooze(productId, linkId, days, { reason = "snooze_link" } = {}) {
  const [product] = await readWarehouseProductsFromPostgresByIds([productId]);
  if (!product) {
    const error = new Error("Товар склада не найден.");
    error.statusCode = 404;
    throw error;
  }
  const linkIndex = (product.links || []).findIndex((l) => l.id === linkId);
  if (linkIndex < 0) {
    const error = new Error("Привязка не найдена.");
    error.statusCode = 404;
    throw error;
  }

  const now = new Date().toISOString();
  const snoozedUntil = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();

  const updatedLinks = product.links.map((link, i) =>
    i === linkIndex ? { ...link, snooze: { snoozedAt: now, snoozedUntil, days } } : link,
  );
  const updatedProduct = { ...product, links: updatedLinks, updatedAt: now, userUpdatedAt: now };

  await writeWarehouseProductPatch([updatedProduct], { reason, writeLinks: true });

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
    await writeWarehouseProductPatch(toWrite, { reason: `${reason}_zero`, writeLinks: true });
    sendZeroStocksToMarketplace(allToZero).catch((error) => {
      logger.warn("snooze link zero-stock send failed", { productId, linkId, detail: error?.message || String(error) });
    });
  }

  return { product: target, snoozedUntil, days };
}

app.post("/api/warehouse/products/:id/links/:linkId/snooze", async (request, response, next) => {
  try {
    const productId = cleanText(request.params.id);
    const linkId = cleanText(request.params.linkId);
    if (!productId || !linkId) return response.status(400).json({ error: "Не указан ID товара или привязки." });

    const days = Math.max(1, Math.min(SNOOZE_MAX_DAYS, Number(request.body.days || SNOOZE_DEFAULT_DAYS) || SNOOZE_DEFAULT_DAYS));
    const { product, snoozedUntil } = await applyWarehouseLinkSnooze(productId, linkId, days, { reason: "snooze_link" });

    await appendAudit(request, "warehouse.link.snooze", { productId, linkId, days, snoozedUntil });
    response.json({ ok: true, product: normalizeWarehouseProduct(product) });
  } catch (error) {
    next(error);
  }
});

async function cancelWarehouseLinkSnooze(productId, linkId, request) {
    const [product] = await readWarehouseProductsFromPostgresByIds([productId]);
    if (!product) {
      const error = new Error("Товар склада не найден.");
      error.statusCode = 404;
      throw error;
    }

    const linkIndex = (product.links || []).findIndex((l) => l.id === linkId);
    if (linkIndex < 0) {
      const error = new Error("Привязка не найдена.");
      error.statusCode = 404;
      throw error;
    }

    const defaultStock = Math.max(1, Number(process.env.LINKED_DEFAULT_TARGET_STOCK || 5) || 5);
    const now = new Date().toISOString();
    const updatedLinks = product.links.map((link, i) => {
      if (i !== linkIndex) return link;
      const { snooze: _removed, ...rest } = link;
      return rest;
    });
    // Reset targetStock and clear automation state so the stock sweep and recovery job treat
    // this product as needing a fresh restore (not blocked by prior stockZeroAt/archivedAt state).
    const updatedProduct = {
      ...product,
      links: updatedLinks,
      updatedAt: now,
      userUpdatedAt: now,
      targetStock: defaultStock,
      noSupplierAutomation: product.noSupplierAutomation
        ? { ...product.noSupplierAutomation, stockZeroAt: null, archivedAt: null }
        : null,
    };

    await writeWarehouseProductPatch([updatedProduct], { reason: "snooze_link_cancel", writeLinks: true });

    // Remove the snooze from sibling products so they regain selectedSupplier and are included
    // in the recovery triggered below. Handles both new snoozes (groupSnoozedByLinkId present
    // in Postgres after normalizer fix) and legacy snoozes (no groupSnoozedByLinkId, written
    // before normalizeWarehouseLink was fixed to preserve the marker).
    const siblingGroupProducts = await hydrateWarehouseProductsForIds([productId], { expandGroups: true });
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
        return {
          ...p,
          updatedAt: now,
          targetStock: defaultStock,
          links: newLinks,
          noSupplierAutomation: p.noSupplierAutomation
            ? { ...p.noSupplierAutomation, stockZeroAt: null, archivedAt: null }
            : null,
        };
      })
      .filter(Boolean);
    if (siblingsToUnsnooze.length) {
      await writeWarehouseProductPatch(siblingsToUnsnooze, { reason: "snooze_link_cancel_siblings", writeLinks: true });
    }

    await appendAudit(request, "warehouse.link.snooze_cancel", { productId, linkId });

    // Direct stock restore — symmetric with the snooze POST which calls sendZeroStocksToMarketplace
    // without any PM lookup. We know these products had suppliers (they were snoozed), so we can
    // restore stock immediately using the already-known offerId/target/marketplace fields.
    // The BullMQ job below handles full recovery (unarchive, price push, DB state propagation).
    const directRestoreProducts = [
      { ...updatedProduct, targetStock: defaultStock },
      ...siblingsToUnsnooze.map((p) => ({ ...p, targetStock: defaultStock })),
    ];
    void restoreStocksOnMarketplaces(directRestoreProducts)
      .then((actions) => {
        const failed = actions.filter((a) => !a.ok);
        if (failed.length) {
          logger.warn("snooze_cancel direct stock restore partial failure", {
            productId,
            linkId,
            failed: failed.map((a) => ({ id: a.id, error: a.error })),
          });
        }
      })
      .catch((err) => logger.warn("snooze_cancel direct stock restore failed", {
        productId,
        detail: err?.message || String(err),
      }));

    queueLinkedProductActivation([productId], "snooze_cancel", warehouseLinkActivationRequestMeta([productId], { username: requestUsername(request) }))
      .catch((error) => logger.warn("snooze cancel recovery queue failed", { productId, detail: error?.message || String(error) }));

    return { product: updatedProduct };
}

app.delete("/api/warehouse/products/:id/links/:linkId/snooze", async (request, response, next) => {
  try {
    const productId = cleanText(request.params.id);
    const linkId = cleanText(request.params.linkId);
    if (!productId || !linkId) return response.status(400).json({ error: "Не указан ID товара или привязки." });

    const { product } = await cancelWarehouseLinkSnooze(productId, linkId, request);
    response.json({ ok: true, product: normalizeWarehouseProduct(product) });
  } catch (error) {
    next(error);
  }
});
