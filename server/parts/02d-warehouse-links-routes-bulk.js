app.post("/api/warehouse/products/links/bulk", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : [])
      .map((id) => String(id || "").trim())
      .filter(Boolean));
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для привязки." });

    const hydratedProducts = await hydrateWarehouseProductsForIds(Array.from(ids), { expandGroups: true });
    let initialSeeds = hydratedProducts.filter((product) => ids.has(String(product.id)));
    if (!initialSeeds.length) {
      const initialWarehouse = await readWarehouse();
      initialSeeds = (initialWarehouse.products || []).filter((product) => ids.has(String(product.id)));
    }
    if (!initialSeeds.length && shouldUsePostgresStorage()) {
      initialSeeds = await readWarehouseProductsFromPostgresByIds(Array.from(ids), { includeDisabledTargets: true });
      if (initialSeeds.length) mergeWarehouseProductsIntoMemory(initialSeeds);
    }
    if (!initialSeeds.length) {
      return response.status(404).json({
        error: "Warehouse product not found.",
        code: "warehouse_product_not_found",
        productIds: Array.from(ids),
      });
    }
    const initialWarehouse = await readWarehouse();
    const expandedProductIds = (hydratedProducts.length
      ? hydratedProducts
      : expandWarehouseProductsToGroups(initialWarehouse.products || [], initialSeeds))
      .map((product) => String(product.id));

    return await withWarehouseProductMutationLock(expandedProductIds, async () => {
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const warehouse = await readWarehouse();
    const linkSaveLookupOptions = { live: true, timeoutMs: 2500, cacheEmpty: false };
    const rawLinks = Array.isArray(request.body.links) && request.body.links.length ? request.body.links : [request.body];
    const submittedLinks = Array.from(new Map(rawLinks
      .map((link) => normalizeWarehouseLink(link))
      .filter(warehouseLinkHasMatchTarget)
      .map((link) => [warehouseLinkTargetKey(link), link])).values());
    const seedProducts = (warehouse.products || []).filter((product) => ids.has(String(product.id)));
    const targetProducts = expandWarehouseProductsToGroups(warehouse.products || [], seedProducts);
    const expandedIds = new Set(targetProducts.map((product) => String(product.id)));
    if (!targetProducts.length) return response.status(404).json({ error: "РўРѕРІР°СЂС‹ СЃРєР»Р°РґР° РЅРµ РЅР°Р№РґРµРЅС‹." });
    const productContext = targetProducts[0] || seedProducts[0] || {};
    const resolvedLinks = [];
    const resolveFailures = [];
    for (const [index, submittedLink] of submittedLinks.entries()) {
      try {
        const resolved = await resolvePriceMasterLinkForSave(submittedLink, usdRate, warehouse.suppliers, {
          ...linkSaveLookupOptions,
          productContext,
        });
        if (warehouseLinkHasMatchTarget(resolved)) resolvedLinks.push(resolved);
      } catch (error) {
        resolveFailures.push(priceMasterLinkValidationFailure(error, submittedLink, index));
      }
    }
    if (resolveFailures.length) {
      const ambiguous = resolveFailures.some((item) => item.code === "PM_LINK_AMBIGUOUS");
      return response.status(ambiguous ? 409 : 400).json({
        error: ambiguous
          ? "\u0423 \u0430\u0440\u0442\u0438\u043a\u0443\u043b\u0430 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0441\u0442\u0440\u043e\u043a PriceMaster, \u0432\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0442\u043e\u0447\u043d\u0443\u044e \u0441\u0442\u0440\u043e\u043a\u0443."
          : `PriceMaster validation failed for ${resolveFailures.length} link(s). Nothing was saved.`,
        code: ambiguous ? "PM_LINK_AMBIGUOUS" : "PM_LINK_BULK_VALIDATION_FAILED",
        failedLinks: resolveFailures,
        candidates: resolveFailures.flatMap((item) => Array.isArray(item.matches) ? item.matches : []).slice(0, 20),
      });
    }
    const baseLinks = Array.from(new Map(resolvedLinks
      .map((link) => [warehouseLinkTargetKey(link), link])).values());
    const baseLink = baseLinks[0] || normalizeWarehouseLink({});
    if (!baseLink.article && !baseLink.exactName && !baseLink.sourceRowId) {
      return response.status(400).json({ error: "\u0423\u043a\u0430\u0436\u0438\u0442\u0435 \u0430\u0440\u0442\u0438\u043a\u0443\u043b PriceMaster \u0438\u043b\u0438 \u0432\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0441\u0442\u0440\u043e\u043a\u0443 PriceMaster \u043f\u043e \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u044e." });
    }
    const now = new Date().toISOString();
    const username = requestUsername(request);
    const commonLinks = buildCommonWarehouseGroupLinks(targetProducts, baseLinks, { now, username });
    const commonSignature = commonLinks.map((link) => warehouseLinkTargetKey(link)).sort().join("||");
    const locks = productLocksFromRequest(request.body);
    const conflicts = collectProductConflictsExceptBackground(targetProducts, locks, { mergeOnly: true });
    const blockingConflicts = conflicts.filter((conflict) => {
      const product = targetProducts.find((item) => String(item.id) === String(conflict.id));
      return !canIgnoreStaleLinkSaveConflict(product, commonLinks, locks.get(String(conflict.id)));
    });
    if (blockingConflicts.length) {
      const alreadyApplied = targetProducts.length > 0 && targetProducts.every((product) => warehouseProductLinksSignature(product) === commonSignature);
      if (!alreadyApplied) return conflictResponse(response, blockingConflicts);
      const savedProducts = await buildFreshWarehouseProductsFromKnownProducts(warehouse, targetProducts, { usdRate });
      const activation = savedProducts.some((product) => (product.links || []).length)
        ? await queueLinkedProductActivation(targetProducts.map((product) => product.id), "link_bulk_add_or_update_unchanged", warehouseLinkActivationRequestMeta(targetProducts.map((product) => product.id), {
          username: requestUsername(request),
        }))
        : { activationQueued: false, recoveryQueued: false, priceIntentId: null, affectedProductIds: targetProducts.map((product) => product.id) };
      return withWarehouseMutation(async () => response.json({
        ok: true,
        changed: savedProducts.length || targetProducts.length,
        products: savedProducts,
        persisted: "already_written",
        alreadyWritten: true,
        expandedProductIds: targetProducts.map((product) => product.id),
        groupLinkSignature: warehouseGroupLinkSignature(savedProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(savedProducts),
        ...activation,
      }));
    }
    // baseLinks are already resolved AND existence-validated by resolvePriceMasterLinkForSave
    // above; a second live PriceMaster round-trip per link here only doubled the save latency
    // ("привязка долгая") without catching anything new.
    const failedLinks = [];
    if (failedLinks.length) {
      return response.status(400).json({
        error: `PriceMaster validation failed for ${failedLinks.length} link(s). Nothing was saved.`,
        code: "PM_LINK_BULK_VALIDATION_FAILED",
        failedLinks,
      });
    }
    const updatedIds = [];
    const oldValues = [];

    for (const product of warehouse.products) {
      if (!expandedIds.has(String(product.id))) continue;
      const beforeDetailsSignature = warehouseProductLinkDetailsSignature(product);
      const beforeValue = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
      product.links = commonLinks.map((link) => normalizeWarehouseLink({
        ...link,
        createdAt: link.createdAt || now,
        updatedAt: now,
        createdBy: link.createdBy || username,
        updatedBy: username,
      }));
      if (warehouseProductLinkDetailsSignature(product) === beforeDetailsSignature) continue;
      oldValues.push(beforeValue);
      product.autoPriceEnabled = true;
      if (commonLinks.length) product.everHadLinks = true;
      product.updatedAt = now;
      product.userUpdatedAt = now;
      updatedIds.push(product.id);
    }

    if (!targetProducts.length) return response.status(404).json({ error: "Товары склада не найдены." });
    const groupSignatureAfterMutation = warehouseGroupLinkSignature(targetProducts);
    if (!groupSignatureAfterMutation.ok) {
      return response.status(409).json({
        error: "PriceMaster links were not synchronized for the whole group. Nothing was queued for marketplace updates.",
        code: "warehouse_group_links_not_synced",
        groupLinkSignature: groupSignatureAfterMutation,
      });
    }

    if (!updatedIds.length) {
      const savedProducts = await buildFreshWarehouseProductsFromKnownProducts(warehouse, targetProducts, { usdRate });
      const activation = savedProducts.some((product) => (product.links || []).length)
        ? await queueLinkedProductActivation(targetProducts.map((product) => product.id), "link_bulk_add_or_update_unchanged", warehouseLinkActivationRequestMeta(targetProducts.map((product) => product.id), {
          username: requestUsername(request),
        }))
        : { activationQueued: false, recoveryQueued: false, priceIntentId: null, affectedProductIds: targetProducts.map((product) => product.id) };
      return withWarehouseMutation(async () => response.json({
        ok: true,
        changed: 0,
        products: savedProducts,
        persisted: "unchanged",
        unchanged: true,
        expandedProductIds: targetProducts.map((product) => product.id),
        groupLinkSignature: warehouseGroupLinkSignature(savedProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(savedProducts),
        ...activation,
      }));
    }

    // withWarehouseMutation invalidates AFTER the PG link write + activation settle. Earlier
    // invalidations run before the commit, so a concurrent catalog poll during the
    // (multi-second) save can repopulate warehouseFastPageCache with pre-link rows and serve
    // them stale for ~45s — that's why the page kept showing the product as "not linked"
    // right after saving (PLAN-HARDENING.md 1.4).
    await withWarehouseMutation(async () => {
    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => updatedIds.includes(product.id)),
      { reason: "warehouse_links_bulk_save" },
    );
    const updatedProducts = targetProducts;
    const savedProducts = await buildFreshWarehouseProductsFromKnownProducts(warehouse, updatedProducts, { usdRate });
    const expandedUpdatedIds = targetProducts.map((product) => product.id);
    const activation = await queueLinkedProductActivation(expandedUpdatedIds, "link_bulk_add_or_update", warehouseLinkActivationRequestMeta(expandedUpdatedIds, {
      username: requestUsername(request),
    }));
    response.json({
      ok: true,
      changed: savedProducts.length || updatedIds.length,
      products: savedProducts,
      persisted: "written",
      expandedProductIds: expandedUpdatedIds,
      groupLinkSignature: warehouseGroupLinkSignature(savedProducts),
      marketplacePriceBreakdown: marketplacePriceBreakdown(savedProducts),
      ...activation,
    });
    appendAudit(request, "warehouse.links.bulk_save", {
      productIds: updatedIds,
      links: baseLinks.map((link) => ({
        article: link.article,
        matchType: link.matchType,
        exactName: link.exactName,
        sourceRowId: link.sourceRowId,
        keyword: link.keyword,
        supplierName: link.supplierName,
        partnerId: link.partnerId,
        priceCurrency: link.priceCurrency,
      })),
      article: baseLink.article,
      matchType: baseLink.matchType,
      exactName: baseLink.exactName,
      sourceRowId: baseLink.sourceRowId,
      keyword: baseLink.keyword,
      supplierName: baseLink.supplierName,
      priceCurrency: baseLink.priceCurrency,
      oldValue: oldValues,
      newValue: savedProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    });
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/links", async (request, response, next) => {
  try {
    return await withWarehouseProductMutationLock([request.params.id], async () => {
    const product = await findWarehouseProductById(request.params.id);
    if (!product) {
      return response.status(404).json({
        error: "Warehouse product not found.",
        code: "warehouse_product_not_found",
        productId: request.params.id,
      });
    }
    await hydrateWarehouseProductsForIds([request.params.id], { expandGroups: true });
    const warehouse = await readWarehouse();
    const conflict = productConflict(product, request.body?.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
    const beforeDetailsSignature = warehouseProductLinkDetailsSignature(product);

    let link = normalizeWarehouseLink(request.body);
    if (!link.article && !link.exactName && !link.sourceRowId) {
      return response.status(400).json({ error: "Укажите артикул PriceMaster или выберите строку PriceMaster по названию." });
    }
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const linkSaveLookupOptions = { live: true, timeoutMs: 2500, cacheEmpty: false };
    const linkProductContext = normalizeWarehouseProduct(product);
    link = await resolvePriceMasterLinkForSave(link, usdRate, warehouse.suppliers, {
      ...linkSaveLookupOptions,
      productContext: linkProductContext,
    });
    await assertPriceMasterLinkExists(link, usdRate, warehouse.suppliers, {
      ...linkSaveLookupOptions,
      productContext: linkProductContext,
    });
    const now = new Date().toISOString();
    const username = requestUsername(request);
    product.links = Array.isArray(product.links) ? product.links : [];
    const index = product.links.findIndex((item) => item.id === link.id || warehouseLinksEqualForSave(item, link));
    if (index >= 0) {
      product.links[index] = mergeWarehouseLinkForSave(product.links[index], link, { now, username });
    }
    else product.links.push(normalizeWarehouseLink({
      ...link,
      createdAt: link.createdAt || now,
      updatedAt: now,
      createdBy: link.createdBy || username,
      updatedBy: username,
    }));
    product.links = compactWarehouseLinks(product.links);
    if (warehouseProductLinkDetailsSignature(product) === beforeDetailsSignature) {
      const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product], { usdRate });
      const normalized = freshProduct || normalizeWarehouseProduct(product);
      const activation = (normalized.links || []).length
        ? await queueLinkedProductActivation([product.id], "link_add_or_update_unchanged", warehouseLinkActivationRequestMeta([product.id], {
          username: requestUsername(request),
        }))
        : { activationQueued: false, recoveryQueued: false, priceIntentId: null, affectedProductIds: [product.id] };
      return response.json({ ok: true, product: normalized, links: normalized.links || [], persisted: "unchanged", unchanged: true, ...activation });
    }
    if (product.links.length > 0) {
      product.autoPriceEnabled = true;
      product.everHadLinks = true;
    }
    product.updatedAt = now;
    product.userUpdatedAt = now;
    // withWarehouseMutation invalidates AFTER the patch write + activation settle, so an
    // immediate GET of the warehouse page reflects the new link (PLAN-HARDENING.md 1.4) —
    // writeWarehouseProductPatch alone is not enough, since queueLinkedProductActivation
    // can still take long enough for a concurrent poll to repopulate the page cache with
    // pre-link rows.
    await withWarehouseMutation(async () => {
    await writeWarehouseProductPatch([product], { reason: "warehouse_link_save" });
    const [savedProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product], { usdRate });
    const activation = await queueLinkedProductActivation([product.id], "link_add_or_update", warehouseLinkActivationRequestMeta([product.id], {
      username: requestUsername(request),
    }));
    response.json({
      ok: true,
      product: savedProduct || normalizeWarehouseProduct(product),
      links: (savedProduct || product).links || [],
      persisted: "written",
      ...activation,
    });
    appendAudit(request, "warehouse.link.save", {
      productId: product.id,
      offerId: product.offerId,
      name: product.name,
      article: link.article,
      matchType: link.matchType,
      exactName: link.exactName,
      sourceRowId: link.sourceRowId,
      keyword: link.keyword,
      supplierName: link.supplierName,
      priceCurrency: link.priceCurrency,
      oldValue: before,
      newValue: { id: savedProduct?.id || product.id, links: (savedProduct || product).links || [], updatedAt: (savedProduct || product).updatedAt },
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    });
    });
  } catch (error) {
    next(error);
  }
});

