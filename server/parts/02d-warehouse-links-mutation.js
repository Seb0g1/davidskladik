async function buildWarehouseLinkMutationResponseProducts(warehouse, products = [], options = {}) {
  const targetProducts = Array.isArray(products) ? products.filter((product) => product?.id) : [];
  const productsWithLinks = targetProducts.filter((product) => (product.links || []).length);
  const builtProducts = productsWithLinks.length
    ? await buildFreshWarehouseProductsFromKnownProducts(warehouse, productsWithLinks, options)
    : [];
  const builtById = new Map(builtProducts.map((product) => [String(product.id), product]));
  return targetProducts.map((product) => {
    const built = builtById.get(String(product.id));
    if (built) return built;
    return {
      ...normalizeWarehouseProduct(product),
      links: [],
      suppliers: [],
      selectedSupplier: null,
      selectedSupplierReason: "No saved PriceMaster links.",
      ready: false,
      changed: false,
      hasLinks: false,
      everHadLinks: Boolean(product.everHadLinks),
      status: "no_links",
    };
  });
}

async function deleteWarehouseGroupLinkRefs(request, response, refsInput = []) {
  const refs = (Array.isArray(refsInput) ? refsInput : [])
    .map((ref) => {
      const normalized = {
        ...ref,
        productId: cleanText(ref.productId),
        linkId: cleanText(ref.linkId),
        linkTargetKey: cleanText(ref.linkTargetKey || ref.targetKey),
        expectedUpdatedAt: cleanText(ref.expectedUpdatedAt),
        expectedLinksSignature: cleanText(ref.expectedLinksSignature),
        article: cleanText(ref.article || ref.supplierArticle),
        supplierArticle: cleanText(ref.supplierArticle || ref.article),
        supplierName: cleanText(ref.supplierName),
        partnerId: cleanText(ref.partnerId),
        rowId: cleanText(ref.rowId),
        sourceRowId: cleanText(ref.sourceRowId || ref.rowId),
        exactName: cleanText(ref.exactName || ref.name),
        matchType: cleanText(ref.matchType),
        keyword: cleanText(ref.keyword),
        priceCurrency: cleanText(ref.priceCurrency || "USD"),
      };
      normalized.linkTargetKey = normalized.linkTargetKey || (warehouseLinkHasMatchTarget(normalized) ? warehouseLinkTargetKey(normalized) : "");
      return normalized;
    })
    .filter((ref) => ref.productId && (ref.linkId || ref.linkTargetKey));
  if (!refs.length) return response.status(400).json({ error: "No PriceMaster links selected for delete." });

  const requestedIds = new Set(refs.map((ref) => String(ref.productId)));
  const hydratedProducts = await hydrateWarehouseProductsForIds(Array.from(requestedIds), { expandGroups: true });
  let initialSeeds = hydratedProducts.filter((product) => requestedIds.has(String(product.id)));
  if (!initialSeeds.length) {
    initialSeeds = await readWarehouseProductsFromPostgresByIds(Array.from(requestedIds), { includeDisabledTargets: true });
    if (initialSeeds.length) mergeWarehouseProductsIntoMemory(initialSeeds);
  }
  const initialWarehouse = await readWarehouse();
  const expandedProductIds = (hydratedProducts.length
    ? hydratedProducts
    : expandWarehouseProductsToGroups(initialWarehouse.products || [], initialSeeds))
    .map((product) => String(product.id));
  if (!expandedProductIds.length) {
    return response.json({ ok: true, changed: 0, products: [], persisted: "already_deleted", alreadyDeleted: true, deletedRefs: [], alreadyDeletedRefs: refs });
  }

  return await withWarehouseProductMutationLock(expandedProductIds, async () => {
    const warehouse = await readWarehouse();
    let seedProducts = (warehouse.products || []).filter((product) => requestedIds.has(String(product.id)));
    if (!seedProducts.length && initialSeeds.length) seedProducts = initialSeeds;
    const targetProducts = expandWarehouseProductsToGroups(
      [...(warehouse.products || []), ...initialSeeds],
      seedProducts.length ? seedProducts : initialSeeds,
    );
    const targetIds = new Set(targetProducts.map((product) => String(product.id)));
    const deleteKeys = new Set();
    const deletedRefs = [];
    const alreadyDeletedRefs = [];
    const conflicts = [];
    const lookupProducts = new Map();
    for (const product of [...(warehouse.products || []), ...initialSeeds, ...targetProducts]) {
      if (product?.id) lookupProducts.set(String(product.id), product);
    }

    for (const ref of refs) {
      const product = lookupProducts.get(String(ref.productId));
      if (!product) {
        alreadyDeletedRefs.push(ref);
        continue;
      }
      const productLinks = Array.isArray(product.links) ? product.links : [];
      const link = productLinks.find((item) => ref.linkId && String(item.id) === String(ref.linkId))
        || productLinks.find((item) => ref.linkTargetKey && warehouseLinkTargetKey(item) === ref.linkTargetKey);
      if (!link) {
        const siblingHasLink = targetProducts.some((targetProduct) => (targetProduct.links || [])
          .some((item) => ref.linkTargetKey && warehouseLinkTargetKey(item) === ref.linkTargetKey));
        if (siblingHasLink && ref.linkTargetKey) {
          deleteKeys.add(ref.linkTargetKey);
          deletedRefs.push(ref);
        } else {
          alreadyDeletedRefs.push(ref);
        }
        continue;
      }
      const conflict = productConflict(product, {
        expectedUpdatedAt: ref.expectedUpdatedAt,
        expectedLinksSignature: ref.expectedLinksSignature,
      });
      if (conflict) {
        conflicts.push(conflict);
        continue;
      }
      // Removal below (and across group siblings) filters links by the SERVER-computed
      // warehouseLinkTargetKey, so deleteKeys must hold that exact key. The frontend's
      // ref.linkTargetKey is a DIFFERENT signature format (linkPrimarySignature) and never
      // equals it — using it here matched the link but then never filtered it out, so
      // "удалить поставщика" reported success while the link stayed. Always key off the
      // matched link's own server target key.
      deleteKeys.add(warehouseLinkTargetKey(link));
      if (ref.linkTargetKey) deleteKeys.add(ref.linkTargetKey);
      deletedRefs.push(ref);
    }

    if (conflicts.length) return conflictResponse(response, conflicts);
    const expandedIds = targetProducts.map((product) => product.id);
    if (!deleteKeys.size) {
      const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, targetProducts);
      return response.json({
        ok: true,
        changed: 0,
        products: responseProducts,
        persisted: "already_deleted",
        alreadyDeleted: true,
        deletedRefs,
        alreadyDeletedRefs,
        expandedProductIds: expandedIds,
        groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
      });
    }

    const changedProducts = [];
    const oldValues = [];
    const mutationSources = new Map();
    for (const product of [...(warehouse.products || []), ...targetProducts]) {
      if (product?.id) mutationSources.set(String(product.id), product);
    }
    for (const productId of targetIds) {
      let product = mutationSources.get(String(productId));
      if (!product && shouldUsePostgresStorage()) {
        [product] = await readWarehouseProductsFromPostgresByIds([productId], { includeDisabledTargets: true });
        if (product) mutationSources.set(String(productId), product);
      }
      if (!product) continue;
      const previousLinks = Array.isArray(product.links) ? product.links : [];
      const nextLinks = compactWarehouseLinks(previousLinks.filter((link) => !deleteKeys.has(warehouseLinkTargetKey(link))));
      if (warehouseProductLinkDetailsSignature({ ...product, links: nextLinks }) === warehouseProductLinkDetailsSignature(product)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt }));
      const updatedProduct = normalizeWarehouseProduct({
        ...product,
        links: nextLinks,
        everHadLinks: Boolean(product.everHadLinks || previousLinks.length > 0),
        updatedAt: new Date().toISOString(),
      });
      mutationSources.set(String(productId), updatedProduct);
      changedProducts.push(updatedProduct);
      const memoryIndex = (warehouse.products || []).findIndex((item) => String(item.id) === String(productId));
      if (memoryIndex >= 0) warehouse.products[memoryIndex] = updatedProduct;
    }

    const responseTargetProducts = targetProducts.map((product) => mutationSources.get(String(product.id)) || product);

    if (!changedProducts.length) {
      const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, responseTargetProducts);
      return response.json({
        ok: true,
        changed: 0,
        products: responseProducts,
        persisted: "already_deleted",
        alreadyDeleted: true,
        deletedRefs,
        alreadyDeletedRefs,
        expandedProductIds: expandedIds,
        groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
      });
    }

    await writeWarehouseProductPatch(changedProducts, { reason: "warehouse_links_group_delete" });
    const changedIds = changedProducts.map((product) => product.id);
    const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, responseTargetProducts);
    const idsWithRemainingLinks = responseProducts.filter((product) => (product.links || []).length).map((product) => product.id);
    const activation = idsWithRemainingLinks.length
      ? await queueLinkedProductActivation(idsWithRemainingLinks, "link_delete", { username: requestUsername(request) })
      : { activationQueued: false, recoveryQueued: false, priceIntentId: null, affectedProductIds: idsWithRemainingLinks };
    response.json({
      ok: true,
      changed: changedProducts.length,
      products: responseProducts,
      persisted: "written",
      deletedRefs,
      alreadyDeletedRefs,
      expandedProductIds: expandedIds,
      groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
      marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
      ...activation,
    });
    appendAudit(request, "warehouse.links.bulk_delete", {
      productIds: changedIds,
      oldValue: oldValues,
      newValue: responseProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    const automationIds = changedProducts
      .filter((product) => !(product.links || []).length && product.everHadLinks)
      .map((product) => product.id);
    if (automationIds.length) {
      queueMarketplaceJob("no-supplier-automation", { productIds: automationIds }, { priority: QUEUE_PRIORITY.RECOVERY });
    }
  });
}

