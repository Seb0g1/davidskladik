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
    const refsByProduct = new Map();
    for (const ref of refs) {
      if (!refsByProduct.has(ref.productId)) refsByProduct.set(ref.productId, []);
      refsByProduct.get(ref.productId).push(ref);
    }

    return await withWarehouseProductMutationLock(Array.from(refsByProduct.keys()), async () => {
      const warehouse = await readWarehouse();
      const changedProducts = [];
      const changedIds = [];
      const oldValues = [];
      const deletedRefs = [];
      const alreadyDeletedRefs = [];
      const conflicts = [];

      for (const [productId, productRefs] of refsByProduct.entries()) {
        const product = warehouse.products.find((item) => String(item.id) === productId);
        if (!product) continue;
        const previousLinks = Array.isArray(product.links) ? product.links : [];
        const linkIds = new Set(productRefs.map((ref) => String(ref.linkId)));
        const removed = previousLinks.some((link) => linkIds.has(String(link.id)));
        if (!removed) {
          alreadyDeletedRefs.push(...productRefs);
          continue;
        }
        const lockRef = productRefs.find((ref) => ref.expectedUpdatedAt || ref.expectedLinksSignature) || productRefs[0] || {};
        const conflict = productConflict(product, {
          expectedUpdatedAt: lockRef.expectedUpdatedAt,
          expectedLinksSignature: lockRef.expectedLinksSignature,
        });
        if (conflict) {
          conflicts.push(conflict);
          continue;
        }
        oldValues.push(cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt }));
        product.links = compactWarehouseLinks(previousLinks.filter((link) => !linkIds.has(String(link.id))));
        product.updatedAt = new Date().toISOString();
        changedProducts.push(product);
        changedIds.push(product.id);
        deletedRefs.push(...productRefs);
      }

      if (conflicts.length) return conflictResponse(response, conflicts);
      if (!changedProducts.length) {
        return response.json({ ok: true, changed: 0, products: [], persisted: "already_deleted", alreadyDeleted: true, deletedRefs, alreadyDeletedRefs });
      }

      await writeWarehouseProductPatch(changedProducts, { reason: "warehouse_links_bulk_delete" });
      const idsWithRemainingLinks = changedProducts.filter((product) => (product.links || []).length).map((product) => product.id);
      const productsWithRemainingLinks = changedProducts.filter((product) => (product.links || []).length);
      const builtProducts = productsWithRemainingLinks.length ? await buildFreshWarehouseProductsFromKnownProducts(warehouse, productsWithRemainingLinks) : [];
      const builtById = new Map(builtProducts.map((product) => [String(product.id), product]));
      const responseProducts = changedProducts.map((product) => {
        const built = builtById.get(String(product.id));
        if (built) return built;
        return {
          ...normalizeWarehouseProduct(product),
          links: [],
          suppliers: [],
          selectedSupplier: null,
          selectedSupplierReason: "Нет сохранённых привязок.",
          ready: false,
          changed: false,
          hasLinks: false,
          status: "no_links",
        };
      });

      response.json({ ok: true, changed: changedProducts.length, products: responseProducts, persisted: "written", deletedRefs, alreadyDeletedRefs });
      appendAudit(request, "warehouse.links.bulk_delete", {
        productIds: changedIds,
        oldValue: oldValues,
        newValue: responseProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
      }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
      queueMarketplaceJob("no-supplier-automation", { productIds: changedIds }, { priority: 1 });
      if (idsWithRemainingLinks.length) await queueLinkedProductActivation(idsWithRemainingLinks, "link_delete", { username: requestUsername(request) });
    });
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
    return await withWarehouseProductMutationLock([request.params.productId], async () => {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.productId);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const before = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
    const previousLinks = Array.isArray(product.links) ? product.links : [];
    const removed = previousLinks.some((link) => String(link.id) === String(request.params.linkId));
    const conflict = productConflict(product, {
      expectedUpdatedAt: request.body?.expectedUpdatedAt || request.query?.expectedUpdatedAt,
      expectedLinksSignature: request.body?.expectedLinksSignature || request.query?.expectedLinksSignature,
    });
    if (conflict && !removed) return conflictResponse(response, [conflict]);
    if (!removed) {
      const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]);
      const responseProduct = freshProduct || normalizeWarehouseProduct(product);
      return response.json({ ok: true, product: responseProduct, links: responseProduct.links || [], persisted: "already_deleted", alreadyDeleted: true });
    }
    product.links = previousLinks.filter((link) => String(link.id) !== String(request.params.linkId));
    product.links = compactWarehouseLinks(product.links);
    product.updatedAt = new Date().toISOString();
    await writeWarehouseProductPatch([product], { reason: "warehouse_link_delete" });
    const [savedProduct] = product.links.length ? await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]) : [];
    const responseProduct = savedProduct || {
      ...normalizeWarehouseProduct(product),
      links: [],
      suppliers: [],
      selectedSupplier: null,
      selectedSupplierReason: "Нет сохранённых привязок.",
      ready: false,
      changed: false,
      hasLinks: false,
      status: "no_links",
    };
    response.json({ ok: true, product: responseProduct, links: responseProduct.links || [], persisted: "written" });
    appendAudit(request, "warehouse.link.delete", {
      productId: product.id,
      offerId: product.offerId,
      name: product.name,
      linkId: request.params.linkId,
      oldValue: before,
      newValue: { id: responseProduct.id, links: responseProduct.links || [], updatedAt: responseProduct.updatedAt },
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    queueMarketplaceJob("no-supplier-automation", { productIds: [request.params.productId] }, { priority: 1 });
    if ((responseProduct.links || []).length) await queueLinkedProductActivation([request.params.productId], "link_delete", { username: requestUsername(request) });
    });
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
      }
    });
  } catch (error) {
    next(error);
  }
});


