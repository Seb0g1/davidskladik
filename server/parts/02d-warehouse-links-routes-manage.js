app.post("/api/warehouse/products/links/delete", async (request, response, next) => {
  try {
    const refs = (Array.isArray(request.body?.refs) ? request.body.refs : [])
      .map((ref) => ({
        ...ref,
        productId: cleanText(ref.productId),
        linkId: cleanText(ref.linkId),
        linkTargetKey: cleanText(ref.linkTargetKey || ref.targetKey),
        expectedUpdatedAt: cleanText(ref.expectedUpdatedAt),
        expectedLinksSignature: cleanText(ref.expectedLinksSignature),
      }))
      .filter((ref) => ref.productId && (ref.linkId || ref.linkTargetKey || warehouseLinkHasMatchTarget(ref)));
    if (!refs.length) return response.status(400).json({ error: "Не выбраны привязки для удаления." });
    return await deleteWarehouseGroupLinkRefs(request, response, refs);
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:productId/links/:linkId", async (request, response, next) => {
  try {
    return await deleteWarehouseGroupLinkRefs(request, response, [{
      ...(request.body || {}),
      productId: request.params.productId,
      linkId: request.params.linkId,
      linkTargetKey: request.body?.linkTargetKey || request.body?.targetKey || request.query?.linkTargetKey || request.query?.targetKey,
      expectedUpdatedAt: request.body?.expectedUpdatedAt || request.query?.expectedUpdatedAt,
      expectedLinksSignature: request.body?.expectedLinksSignature || request.query?.expectedLinksSignature,
    }]);
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/links/sync-group", async (request, response, next) => {
  try {
    const productIds = (Array.isArray(request.body?.productIds) ? request.body.productIds : [])
      .map((id) => cleanText(id))
      .filter(Boolean);
    const groupKey = cleanText(request.body?.groupKey);
    const hydrateIds = groupKey ? [] : productIds;
    if (hydrateIds.length) await hydrateWarehouseProductsForIds(hydrateIds, { expandGroups: true });
    const initialWarehouse = await readWarehouse();
    const initialSeeds = groupKey
      ? warehouseProductsForGroupKey(initialWarehouse.products || [], groupKey)
      : (initialWarehouse.products || []).filter((product) => productIds.includes(String(product.id)));
    if (!initialSeeds.length) return response.status(404).json({ error: "Warehouse group not found." });
    const expandedProductIds = expandWarehouseProductsToGroups(initialWarehouse.products || [], initialSeeds)
      .map((product) => String(product.id));

    return await withWarehouseProductMutationLock(expandedProductIds, async () => {
      const warehouse = await readWarehouse();
      const seedProducts = groupKey
        ? warehouseProductsForGroupKey(warehouse.products || [], groupKey)
        : (warehouse.products || []).filter((product) => productIds.includes(String(product.id)));
      const targetProducts = expandWarehouseProductsToGroups(warehouse.products || [], seedProducts);
      if (!targetProducts.length) return response.status(404).json({ error: "Warehouse group not found." });

      const now = new Date().toISOString();
      const username = requestUsername(request);
      const syncResult = syncWarehouseProductGroupLinks(targetProducts, { now, username });
      const changedProducts = syncResult.changedProducts || [];
      const expandedIds = targetProducts.map((product) => product.id);
      if (changedProducts.length) {
        await writeWarehouseProductPatch(changedProducts, { reason: "warehouse_links_sync_group" });
      }
      const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, targetProducts);
      const activation = responseProducts.some((product) => (product.links || []).length)
        ? await queueLinkedProductActivation(expandedIds, changedProducts.length ? "link_sync_group" : "link_sync_group_unchanged", warehouseLinkActivationRequestMeta(expandedIds, { username: requestUsername(request) }))
        : { activationQueued: false, recoveryQueued: false, priceIntentId: null, affectedProductIds: expandedIds };
      response.json({
        ok: true,
        changed: changedProducts.length,
        products: responseProducts,
        persisted: changedProducts.length ? "written" : "unchanged",
        unchanged: !changedProducts.length,
        expandedProductIds: expandedIds,
        groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
        ...activation,
      });
      if (changedProducts.length) {
        appendAudit(request, "warehouse.links.sync_group", {
          productIds: changedProducts.map((product) => product.id),
          oldValue: syncResult.oldValues || [],
          newValue: responseProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
        }).catch((auditError) => logger.warn("link sync audit append failed", { detail: auditError?.message || String(auditError) }));
        void triggerLinkedProductStockSync(expandedIds, "link_sync_group").catch(() => {});
      }
    });
  } catch (error) {
    next(error);
  }
});


