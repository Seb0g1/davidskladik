app.get("/api/supplier-cart/preview", requireAdmin, async (request, response, next) => {
  try {
    const preview = await buildSupplierCartPreview({
      marketplace: request.query.marketplace,
      from: request.query.from,
      to: request.query.to,
      limit: request.query.limit,
    });
    response.json(preview);
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/draft", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    const rows = state.draft?.rows || [];
    const ready = rows.filter((row) => row.ready && !row.alreadyCommitted).length;
    const alreadyCommitted = rows.filter((row) => row.alreadyCommitted).length;
    const skipped = rows.length - ready - alreadyCommitted;
    response.json({
      ok: true,
      draftId: state.draft?.id || "",
      generatedAt: state.draft?.generatedAt || null,
      generatedBy: state.draft?.generatedBy || "",
      rows,
      total: rows.length,
      ready,
      skipped,
      alreadyCommitted,
      warnings: state.draft?.summary?.warnings || [],
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/schedule", requireAdmin, async (_request, response, next) => {
  try {
    const settings = normalizeSupplierCartSettings((await readAppSettings()).supplierCart || {});
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/supplier-cart/schedule", requireAdmin, async (request, response, next) => {
  try {
    const appSettings = await readAppSettings();
    const settings = normalizeSupplierCartSettings({
      ...(appSettings.supplierCart || {}),
      ...(request.body || {}),
    });
    await writeAppSettings({
      ...appSettings,
      supplierCart: settings,
    });
    await scheduleSupplierCartAuto();
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/generate", requireAdmin, async (request, response, next) => {
  try {
    const preview = await generateSupplierCartDraft({
      marketplace: request.body?.marketplace || request.query.marketplace,
      from: request.body?.from || request.query.from,
      to: request.body?.to || request.query.to,
      limit: request.body?.limit || request.query.limit,
    }, request);
    response.json(preview);
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/commit", requireAdmin, async (request, response, next) => {
  try {
    const rows = Array.isArray(request.body?.rows) ? request.body.rows : [];
    const keys = Array.isArray(request.body?.keys) ? request.body.keys : [];
    const state = await readSupplierCartState();
    const sourceRows = rows.length
      ? rows
      : (state.draft?.rows?.length ? state.draft.rows : (await buildSupplierCartPreview(request.body || {})).rows);
    const selectedKeys = new Set(keys.map(cleanText).filter(Boolean));
    const selectedRows = selectedKeys.size ? sourceRows.filter((row) => selectedKeys.has(cleanText(row.key))) : sourceRows;
    const result = await insertSupplierCartRowsIntoPriceMaster(selectedRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      skipped: result.skipped,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      verifiedInPriceMaster: Boolean(result.verification?.ok),
      verifiedRows: Number(result.verification?.verifiedRows || 0),
      priceMasterDb: result.verification?.db || "",
      marketplaceConfirms: result.marketplaceConfirms || [],
      rows: result.inserted,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/pricemaster/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await getPriceMasterBasketStatus());
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/rollback-all", requireAdmin, async (request, response, next) => {
  try {
    const confirm = cleanText(request.body?.confirm);
    if (confirm !== "ROLLBACK_DAVIDSKLAD_SUPPLIER_CART") {
      return response.status(400).json({
        error: "Confirmation is required.",
        code: "supplier_cart_rollback_confirmation_required",
      });
    }
    const result = await rollbackSupplierCartAll(request, { dryRun: request.body?.dryRun !== false });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/manual-order", requireAdmin, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const quantity = Math.max(1, Math.round(Number(request.body?.quantity || 1) || 1));
    const marketplace = cleanText(request.body?.marketplace || "ozon").toLowerCase();
    const note = cleanText(request.body?.note || "").slice(0, 200);
    const forcedPartnerId = cleanText(request.body?.partnerId || "");
    const forcedRowId = cleanText(request.body?.rowId || "");
    if (!offerId) return response.status(400).json({ error: "offerId is required.", code: "manual_order_no_offer_id" });
    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
    const product = findSupplierCartWarehouseProduct(warehouse, { offerId, marketplace, accountId: "" });
    if (!product) return response.status(404).json({ error: "Товар не найден на складе.", code: "product_not_found" });
    const state = await readSupplierCartState();
    const manualKey = `manual|manual|manual-${Date.now()}|${offerId}`;
    const productName = cleanText(product.productName || product.name || offerId);
    const orderId = `manual-${Date.now()}`;

    let rowWithNote;
    if (forcedPartnerId) {
      const { options } = await listSupplierCartSupplierOptions(offerId);
      const chosen = pickSupplierCartOption(options, forcedPartnerId, forcedRowId);
      const rejection = supplierCartOptionRejection(chosen);
      if (rejection) return response.status(rejection.status).json({ error: rejection.error, code: rejection.code });
      rowWithNote = normalizeSupplierCartPreviewRow({
        key: manualKey,
        marketplace: "manual",
        offerId,
        quantity,
        productName,
        orderId,
        accountId: "manual",
        accountName: "Ручной заказ",
        supplierName: chosen.supplierName,
        partnerId: chosen.partnerId,
        offerRowId: chosen.rowId,
        price: chosen.price,
        originalPrice: chosen.originalPrice,
        priceCurrency: chosen.priceCurrency,
        trustFactor: chosen.trustFactor,
        orderCutoffTime: chosen.orderCutoffTime,
        reseller: chosen.reseller,
        stockOnlyFallback: chosen.stockOnly,
        available: true,
        ready: true,
        manualNote: note,
      });
    } else {
      const manualLine = normalizeSupplierCartLine({
        key: manualKey,
        marketplace: "manual",
        offerId,
        quantity,
        productName,
        orderId,
        accountId: "manual",
        accountName: "Ручной заказ",
      });
      const row = await resolveSupplierCartRow(warehouse, manualLine, state);
      if (!row.ready) {
        return response.status(400).json({
          error: "Не удалось подобрать поставщика.",
          skipReason: row.skipReason || "supplier_not_found",
          code: "manual_order_no_supplier",
        });
      }
      rowWithNote = { ...row, quantity, manualNote: note };
    }

    const result = await insertSupplierCartRowsIntoPriceMaster([rowWithNote], request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      row: result.inserted[0] || null,
    });
  } catch (error) {
    next(error);
  }
});

// Поиск товаров в снапшоте PriceMaster по названию/артикулу
app.get("/api/supplier-cart/pm-search", requireStaff, async (request, response, next) => {
  try {
    const q = cleanText(request.query.q || "").toLowerCase();
    const partnerId = cleanText(request.query.partnerId || "");
    const limit = cleanLimit(request.query.limit, 80, 200);
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ ok: false, error: "Database not available" });
    const baseWhere = { active: true, price: { not: null, gt: 0 } };
    if (partnerId) baseWhere.partnerId = partnerId;

    const tokenGroups = q ? pmQueryToTokenGroups(q) : null;
    const minMatchCount = tokenGroups ? pmMinMatchCount(tokenGroups) : 0;
    function buildWhere(groups) {
      if (groups && groups.length) {
        // Only required groups in SQL — optional (numbers, "ml") are so broad they flood
        // the LIMIT window and bury primary-keyword items. JS post-filter handles them.
        const sqlGroups = groups.filter((g) => !pmTokenGroupIsOptional(g));
        const activeGroups = sqlGroups.length ? sqlGroups : groups;
        const orTerms = activeGroups.flatMap((group) => group.flatMap((synonym) => [
          { nativeName: { contains: synonym, mode: "insensitive" } },
          { article: { contains: synonym, mode: "insensitive" } },
        ]));
        return { ...baseWhere, OR: orTerms };
      }
      if (q) {
        return { ...baseWhere, OR: [
          { nativeName: { contains: q, mode: "insensitive" } },
          { article: { contains: q, mode: "insensitive" } },
        ] };
      }
      return baseWhere;
    }

    const sel = { id: true, rowId: true, article: true, partnerId: true, partnerName: true, nativeName: true, price: true, currency: true, docDate: true };
    // Fetch a large candidate pool so older products (e.g. Dior items from past docs) are not
    // cut off before post-filter. OR-based SQL pre-filter casts a wide net; JS does precision.
    let items = await prisma.priceMasterSnapshotItem.findMany({ where: buildWhere(tokenGroups), orderBy: [{ docDate: "desc" }, { updatedAt: "desc" }], take: Math.min(limit * 15, 2000), select: sel });

    // Post-filter: apply quality bar (required keywords must match; numbers/units are optional).
    if (tokenGroups && tokenGroups.length >= 1) {
      items = items.filter((item) => {
        const hay = [cleanText(item.nativeName || ""), cleanText(item.article || "")].join(" ");
        return pmPassesSearchFilter(hay, tokenGroups);
      });
    }
    items = items.slice(0, limit);

    const usdRate = await getUsdRate();
    // Инна prices in PM snapshot are stored with currency="USD" (snapshot has no managed-supplier
    // awareness), but they are actually in RUB — detect by partner name so sorting is correct.
    const toRub = (price, currency, partnerName) => {
      const p = Number(price || 0);
      if (cleanText(currency || "USD").toUpperCase() === "RUB") return p;
      if (isInnaSupplierName(partnerName || "")) return p;
      return p * usdRate;
    };
    const isTesterName = (name) => {
      const n = cleanText(name || "").toLowerCase();
      return n.includes("отливант") || /\btest(?:er|ep|or|r)?\b/.test(n) || n.includes("тест");
    };
    // Relevance score for a name given the token groups:
    //   +2 per required group that matches
    //   +1 per optional group that matches (numbers/units)
    //   +1 bonus when a numeric token matches with BOTH-sides word boundary
    //      (exact volume: "5ml" scores higher than "1.5ml" or "15ml")
    const computeRelevance = (name, article) => {
      if (!tokenGroups || !tokenGroups.length) return 0;
      const hay = [name, article].join(" ").toLowerCase().replace(/ё/g, "е");
      let score = 0;
      for (const group of tokenGroups) {
        const isOptional = pmTokenGroupIsOptional(group);
        const matches = group.some((t) => pmTokenMatchesText(hay, t));
        if (!matches) continue;
        score += isOptional ? 1 : 2;
        // Extra +1 when a numeric token is a true standalone number (both boundaries)
        if (isOptional && group.some((t) => /^\d+$/.test(t))) {
          const exactMatch = group.some((t) => {
            if (!/^\d+$/.test(t)) return false;
            const esc = t.replace(/[-[\]/{}()*+?.\\^$|]/g, "\\$&");
            return new RegExp(`(?<!\\d)${esc}(?!\\d)`).test(hay);
          });
          if (exactMatch) score += 1;
        }
      }
      return score;
    };

    const mapped = items.map((item) => {
      const currency = cleanText(item.currency || "USD");
      const price = Number(item.price || 0);
      const partnerName = cleanText(item.partnerName || "");
      const name = cleanText(item.nativeName || "");
      const article = cleanText(item.article || "");
      return {
        id: item.id,
        rowId: cleanText(item.rowId || ""),
        article,
        partnerId: cleanText(item.partnerId || ""),
        supplierName: partnerName,
        name,
        price,
        currency: isInnaSupplierName(partnerName) ? "RUB" : currency,
        priceRub: toRub(price, currency, partnerName),
        isTester: isTesterName(item.nativeName || ""),
        docDate: item.docDate?.toISOString?.()?.slice(0, 10) || null,
        _relevance: computeRelevance(name, article),
      };
    });
    // Sort: testers/отливанты last; then by relevance desc; then price asc
    mapped.sort((a, b) => {
      if (a.isTester !== b.isTester) return a.isTester ? 1 : -1;
      if (b._relevance !== a._relevance) return b._relevance - a._relevance;
      return a.priceRub - b.priceRub;
    });
    mapped.forEach((item) => { delete item._relevance; });
    response.json({
      ok: true,
      total: mapped.length,
      items: mapped,
    });
  } catch (error) {
    next(error);
  }
});

// Ручной заказ из PM-поиска: выбранные строки снапшота → RequestDocs/RequestRows
app.post("/api/supplier-cart/pm-manual-commit", requireAdmin, async (request, response, next) => {
  try {
    const items = Array.isArray(request.body?.items) ? request.body.items : [];
    if (!items.length) return response.status(400).json({ ok: false, error: "No items provided." });
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ ok: false, error: "Database not available" });
    const ids = [...new Set(items.map((i) => cleanText(i.id)).filter(Boolean))];
    const snapshotRows = await prisma.priceMasterSnapshotItem.findMany({
      where: { id: { in: ids }, active: true },
    });
    const snapById = new Map(snapshotRows.map((r) => [r.id, r]));
    const cartRows = items
      .map((item) => {
        const snap = snapById.get(cleanText(item.id));
        if (!snap || !snap.rowId || !snap.partnerId) return null;
        const quantity = Math.max(1, Math.round(Number(item.quantity || 1) || 1));
        const note = cleanText(item.note || "").slice(0, 200);
        return normalizeSupplierCartPreviewRow({
          key: `manual|pm|${snap.partnerId}|${snap.article}|${Date.now()}`,
          marketplace: "manual",
          offerId: snap.article,
          productName: snap.nativeName || snap.article,
          quantity,
          partnerId: cleanText(snap.partnerId),
          supplierName: cleanText(snap.partnerName || ""),
          offerRowId: cleanText(snap.rowId),
          price: Number(snap.price || 0),
          priceCurrency: cleanText(snap.currency || "USD"),
          manualNote: note,
          ready: true,
          alreadyCommitted: false,
          trustFactor: 100,
          reseller: false,
          supplierScore: 100,
        });
      })
      .filter(Boolean);
    if (!cartRows.length) return response.status(400).json({ ok: false, error: "No valid rows found in PriceMaster snapshot." });
    const result = await insertSupplierCartRowsIntoPriceMaster(cartRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      skipped: result.skipped,
      docIds: result.docIds,
      pickingCreated: Array.isArray(result.pickingCreated) ? result.pickingCreated.length : (result.pickingCreated || 0),
      verifiedInPriceMaster: Boolean(result.verification?.ok),
      rows: result.inserted,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ready-to-ship", requireStaff, async (request, response, next) => {
  try {
    const lookbackDays = Math.min(60, Math.max(1, Number(request.query.days || 30) || 30));
    const limit = cleanLimit(request.query.limit, 500);
    const now = new Date();
    const from = new Date(now.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
    const to = now;
    const appSettings = await readAppSettings();
    const cartSettings = normalizeSupplierCartSettings(appSettings.supplierCart || {});

    const [ozonResult, yandexResult, wbResult] = await Promise.allSettled([
      fetchOzonSupplierCartLines({ from, to, limit: Math.ceil(limit * 0.6), statuses: cartSettings.includeOzonStatuses }),
      fetchYandexSupplierCartLines({ from, to, limit: Math.ceil(limit * 0.3), statuses: cartSettings.includeYandexStatuses, substatuses: cartSettings.includeYandexSubstatuses }),
      fetchWbSupplierCartLines({ limit: Math.ceil(limit * 0.3) }),
    ]);

    const lines = [
      ...(ozonResult.status === "fulfilled" ? ozonResult.value : []),
      ...(yandexResult.status === "fulfilled" ? yandexResult.value : []),
      ...(wbResult.status === "fulfilled" ? wbResult.value : []),
    ];
    const errors = [
      ozonResult.status === "rejected" ? `Ozon: ${ozonResult.reason?.message || "ошибка"}` : null,
      yandexResult.status === "rejected" ? `Yandex: ${yandexResult.reason?.message || "ошибка"}` : null,
      wbResult.status === "rejected" ? `WB: ${wbResult.reason?.message || "ошибка"}` : null,
    ].filter(Boolean);

    response.json({ ok: true, lines: lines.slice(0, limit), total: lines.length, errors });
  } catch (error) {
    next(error);
  }
});

// «Нету у поставщика» для заказа из маркетплейса: снузит привязку основного поставщика по offerId.
app.post("/api/ready-to-ship/missing", requireStaff, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const snoozeDays = Math.min(60, Math.max(1, Number(request.body?.snoozeDays || 7) || 7));
    const partnerIdHint = cleanText(request.body?.partnerId || "").toLowerCase();
    if (!offerId) return response.status(400).json({ ok: false, error: "offerId is required.", code: "missing_offer_id" });

    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
    const product = findSupplierCartWarehouseProduct(warehouse, { offerId });
    if (!product) return response.status(404).json({ ok: false, error: "Товар не найден на складе.", code: "product_not_found" });

    const links = product.links || [];
    const link = (partnerIdHint
      ? links.find((l) => cleanText(l.partnerId).toLowerCase() === partnerIdHint)
      : null)
      || links.find((l) => !l.snoozeUntil || new Date(l.snoozeUntil) <= new Date())
      || links[0];

    if (!link) return response.status(404).json({ ok: false, error: "Привязка поставщика не найдена.", code: "link_not_found" });

    const result = await applyWarehouseLinkSnooze(product.id, link.id, snoozeDays, { reason: "marketplace_order_missing" });
    await appendAudit(request, "supplier_cart.marketplace_order_missing", {
      entityType: "warehouse_product",
      entityId: product.id,
      newValue: { offerId, linkId: link.id, partnerId: link.partnerId, supplierName: link.supplierName, snoozeDays, snoozedUntil: result?.snoozedUntil },
    });
    response.json({
      ok: true,
      offerId,
      productId: product.id,
      linkId: link.id,
      supplierName: link.supplierName || link.partnerId || "",
      snoozedUntil: result?.snoozedUntil || null,
      snoozeDays,
    });
  } catch (error) {
    next(error);
  }
});

// Заказать у выбранного поставщика для заказа из маркетплейса (замена поставщика).
app.post("/api/ready-to-ship/order", requireStaff, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const partnerId = cleanText(request.body?.partnerId);
    const rowId = cleanText(request.body?.rowId);
    const quantity = Math.max(1, Math.round(Number(request.body?.quantity || 1) || 1));
    const marketplace = cleanText(request.body?.marketplace || "ozon").toLowerCase();
    const orderId = cleanText(request.body?.orderId || "");
    const accountId = cleanText(request.body?.accountId || "");
    const accountName = cleanText(request.body?.accountName || "");

    if (!offerId) return response.status(400).json({ ok: false, error: "offerId is required.", code: "missing_offer_id" });
    if (!partnerId || !rowId) return response.status(400).json({ ok: false, error: "partnerId and rowId are required.", code: "missing_supplier" });

    const { options } = await listSupplierCartSupplierOptions(offerId);
    const option = pickSupplierCartOption(options, partnerId, rowId);
    const rejection = supplierCartOptionRejection(option);
    if (rejection) return response.status(rejection.status).json({ ok: false, error: rejection.error, code: rejection.code });

    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
    const product = findSupplierCartWarehouseProduct(warehouse, { offerId, marketplace, accountId });
    if (!product) return response.status(404).json({ ok: false, error: "Товар не найден на складе.", code: "product_not_found" });

    const cartRow = normalizeSupplierCartPreviewRow({
      key: `ready-to-ship|${marketplace}|${orderId || Date.now()}|${offerId}`,
      marketplace,
      accountId: accountId || marketplace,
      accountName: accountName || marketplace,
      orderId: orderId || `mp-${Date.now()}`,
      offerId,
      productName: cleanText(product.productName || product.name || offerId),
      quantity,
      warehouseProductId: product.id,
      groupKey: warehouseProductPageGroupKey(product),
      groupOfferId: product.offerId,
      partnerId: option.partnerId,
      supplierName: option.supplierName,
      offerRowId: option.rowId,
      price: option.price,
      originalPrice: option.originalPrice,
      priceCurrency: option.priceCurrency,
      trustFactor: option.trustFactor,
      orderCutoffTime: option.orderCutoffTime,
      reseller: option.reseller,
      supplierScore: option.score || 0,
      available: true,
      ready: true,
    });

    const result = await insertSupplierCartRowsIntoPriceMaster([cartRow], request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      supplierName: option.supplierName,
      row: result.inserted[0] || null,
    });
  } catch (error) {
    next(error);
  }
});

// Пакетный заказ: несколько маркетплейсовых заказов → PM, авто-поставщик, группировка по поставщику.
app.post("/api/ready-to-ship/batch-order", requireAdmin, async (request, response, next) => {
  try {
    const lines = Array.isArray(request.body?.lines) ? request.body.lines : [];
    if (!lines.length) return response.status(400).json({ ok: false, error: "No lines provided.", code: "missing_lines" });

    const allOfferIds = [...new Set(lines.map((l) => cleanText(l.offerId)).filter(Boolean))];
    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), allOfferIds);
    const state = await readSupplierCartState();

    const cartRows = [];
    const failed = [];
    for (const line of lines) {
      const offerId = cleanText(line.offerId);
      const quantity = Math.max(1, Math.round(Number(line.quantity || 1) || 1));
      const marketplace = cleanText(line.marketplace || "ozon").toLowerCase();
      const orderId = cleanText(line.orderId || line.postingNumber || "");
      const accountId = cleanText(line.accountId || "");
      const accountName = cleanText(line.accountName || "");
      const lineKey = cleanText(line.key) || `ready-to-ship|${marketplace}|${orderId || Date.now()}|${offerId}`;
      if (!offerId) { failed.push({ key: lineKey, reason: "missing_offer_id" }); continue; }
      if (state.processed?.[lineKey]) { failed.push({ key: lineKey, offerId, reason: "already_committed" }); continue; }

      const product = findSupplierCartWarehouseProduct(warehouse, { offerId, marketplace, accountId });
      if (!product) { failed.push({ key: lineKey, offerId, reason: "product_not_found" }); continue; }

      const normalizedLine = normalizeSupplierCartLine({ marketplace, accountId, orderId, offerId, quantity });
      const row = await resolveSupplierCartRow(warehouse, normalizedLine, state);
      if (!row.ready) { failed.push({ key: lineKey, offerId, reason: row.skipReason || "no_supplier" }); continue; }

      cartRows.push(normalizeSupplierCartPreviewRow({
        ...row,
        key: lineKey,
        marketplace,
        accountId: accountId || marketplace,
        accountName: accountName || marketplace,
        orderId: orderId || `mp-${Date.now()}`,
        quantity,
        warehouseProductId: product.id,
        groupKey: warehouseProductPageGroupKey(product),
        groupOfferId: product.offerId,
        available: true,
        ready: true,
      }));
    }

    if (!cartRows.length) {
      return response.status(400).json({ ok: false, error: "No orders could be resolved.", failed });
    }

    const result = await insertSupplierCartRowsIntoPriceMaster(cartRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      failed: failed.length,
      failedDetails: failed,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/history", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    response.json({
      ok: true,
      updatedAt: state.updatedAt,
      totalProcessed: Object.keys(state.processed || {}).length,
      history: (state.history || []).slice().reverse(),
    });
  } catch (error) {
    next(error);
  }
});

