// Manual supplier replacement for the supplier cart (Автокорзина) and the
// picking list (Сборка):
//   - GET  /api/supplier-cart/alternatives           — live PriceMaster supplier
//     options for an offer (staff: pickers need it after "Не было").
//   - POST /api/supplier-cart/draft/:key/supplier    — override the auto-picked
//     supplier on a draft row before "Сформировать корзину" (admin).
//   - POST /api/supplier-picking-list/:key/replace-supplier — reorder an open or
//     missing picking row from another supplier right now: blocks the old
//     supplier for the SKU, removes the old PriceMaster request row (no double
//     order), inserts a new request doc and creates the retry picking row via
//     the standard replacementFor/replacementKey machinery (staff).

async function listSupplierCartSupplierOptions(offerIdInput = "", { now = new Date() } = {}) {
  const offerId = cleanText(offerIdInput);
  if (!offerId) return { options: [], skipReason: "offer_required" };
  const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
  const state = await readSupplierCartState();
  const product = findSupplierCartWarehouseProduct(warehouse, { offerId });
  if (!product) return { options: [], skipReason: "product_not_found" };
  const groupProducts = expandWarehouseProductsToGroups(warehouse.products || [], [product]);
  const groupLinks = buildCommonWarehouseGroupLinks(groupProducts, []);
  if (!groupLinks.length) return { options: [], skipReason: "no_pricemaster_link" };
  const usdRate = Number((await getUsdRate()).rate || process.env.DEFAULT_USD_RATE || 95);
  const matches = await getLivePriceMasterMatchesForLinks(groupLinks, warehouse.suppliers || [], usdRate);
  const blockedPartnerIds = activeSupplierBlocksForOffer(state, offerId, now);
  const seen = new Set();
  const options = [];
  for (const rows of matches.values()) {
    for (const row of rows || []) {
      if (!row) continue;
      const partnerId = cleanText(row.partnerId);
      const rowId = cleanText(row.rowId);
      if (!partnerId && !rowId) continue;
      const dedupeKey = `${partnerId.toLowerCase()}|${rowId}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      const stockOnly = supplierUsesStockOnlyPricing(null, row);
      const available = row.available !== false;
      const price = Number(row.price || 0) || 0;
      // Inactive PM row (OfferRows.Active=0) but NOT a hard-stopped managed supplier:
      // still include so the user can manually order from them (like pm-manual-commit).
      const inactivePm = !row.active && !row.stopped;
      options.push({
        partnerId,
        supplierName: cleanText(row.partnerName || row.supplierName),
        rowId,
        price,
        originalPrice: Number(row.originalPrice || 0) || 0,
        priceCurrency: cleanText(row.priceCurrency || "USD").toUpperCase(),
        available,
        trustFactor: normalizeSupplierTrustFactor(row.trustFactor, 100),
        orderCutoffTime: normalizeSupplierOrderCutoff(row.orderCutoffTime),
        reseller: Boolean(row.reseller),
        stockOnly,
        blocked: blockedPartnerIds.has(partnerId.toLowerCase()),
        cutoffPassed: supplierOrderCutoffPassed(row.orderCutoffTime, now),
        score: supplierCartOrderScore(row, usdRate, now),
        orderable: available && (stockOnly || price > 0),
        inactivePm,
      });
    }
  }
  const toUsd = (opt) => {
    const p = Number(opt.price || Number.POSITIVE_INFINITY);
    return opt.priceCurrency === "RUB" ? p / usdRate : p;
  };
  // Rank: 0=active+orderable, 1=active+blocked/cutoff, 2=inactive PM, 3=stopped/no_price
  const usableRank = (option) => {
    if (option.inactivePm) return 2;
    return option.orderable && !option.blocked && !option.cutoffPassed && !option.stockOnly ? 0 : (option.orderable && !option.blocked ? 1 : 3);
  };
  const priorityRank = (option) => {
    const name = cleanText(option.supplierName || "");
    if (/сорин/i.test(name)) return 0;
    if (/инна/i.test(name)) return 1;
    return 2;
  };
  options.sort((left, right) =>
    usableRank(left) - usableRank(right)
    || priorityRank(left) - priorityRank(right)
    || left.score - right.score
    || toUsd(left) - toUsd(right)
    || left.supplierName.localeCompare(right.supplierName, "ru", { sensitivity: "base" }),
  );
  return { options, blockedPartnerIds: [...blockedPartnerIds] };
}

function pickSupplierCartOption(options = [], partnerIdInput = "", rowIdInput = "") {
  const partnerId = cleanText(partnerIdInput).toLowerCase();
  const rowId = cleanText(rowIdInput);
  return options.find((option) =>
    cleanText(option.partnerId).toLowerCase() === partnerId
    && (!rowId || cleanText(option.rowId) === rowId)) || null;
}

function supplierCartOptionRejection(option) {
  if (!option) return { status: 404, error: "Поставщик с таким предложением не найден в PriceMaster.", code: "supplier_option_not_found" };
  if (option.blocked) return { status: 400, error: "Этот поставщик заблокирован для SKU после «Не было». Выберите другого.", code: "supplier_option_blocked" };
  // Inactive PM row (OfferRows.Active=0): allow manual order even though available=false.
  // This mirrors pm-manual-commit which lets the user order from inactive suppliers explicitly.
  if (!option.available && !option.inactivePm) return { status: 400, error: "У этого поставщика нет наличия по PriceMaster.", code: "supplier_option_unavailable" };
  if (!option.orderable && !option.inactivePm) return { status: 400, error: "У этого предложения нет закупочной цены.", code: "supplier_option_no_price" };
  return null;
}

app.get("/api/supplier-cart/alternatives", requireStaff, async (request, response, next) => {
  try {
    const offerId = cleanText(request.query.offerId || "");
    if (!offerId) return response.status(400).json({ error: "offerId is required.", code: "offer_required" });
    const result = await listSupplierCartSupplierOptions(offerId);
    response.json({ ok: true, offerId, ...result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/draft/:key/supplier", requireAdmin, async (request, response, next) => {
  try {
    const key = cleanText(request.params.key);
    const partnerId = cleanText(request.body?.partnerId);
    if (!partnerId) return response.status(400).json({ error: "partnerId is required.", code: "supplier_required" });
    const state = await readSupplierCartState();
    const rows = state.draft?.rows || [];
    const index = rows.findIndex((row) => cleanText(row.key) === key);
    if (index < 0) return response.status(404).json({ error: "Строка черновика не найдена. Сгенерируйте корзину заново.", code: "supplier_cart_draft_row_not_found" });
    const row = normalizeSupplierCartPreviewRow(rows[index]);
    if (row.alreadyCommitted) return response.status(409).json({ error: "Строка уже отправлена в PriceMaster.", code: "supplier_cart_already_committed" });
    const { options } = await listSupplierCartSupplierOptions(row.offerId);
    const chosen = pickSupplierCartOption(options, partnerId, request.body?.rowId);
    const rejection = supplierCartOptionRejection(chosen);
    if (rejection) return response.status(rejection.status).json({ error: rejection.error, code: rejection.code });
    const before = { supplierName: row.supplierName, partnerId: row.partnerId, offerRowId: row.offerRowId, price: row.price };
    state.draft.rows[index] = normalizeSupplierCartPreviewRow({
      ...row,
      supplierName: chosen.supplierName,
      partnerId: chosen.partnerId,
      offerRowId: chosen.rowId,
      price: chosen.price,
      originalPrice: chosen.originalPrice,
      priceCurrency: chosen.priceCurrency,
      trustFactor: chosen.trustFactor,
      orderCutoffTime: chosen.orderCutoffTime,
      reseller: chosen.reseller,
      supplierScore: chosen.score,
      available: true,
      ready: true,
      stockOnlyFallback: chosen.stockOnly,
      skipReason: "",
    });
    const draftRows = state.draft.rows;
    state.draft.summary = {
      ...(state.draft.summary || {}),
      total: draftRows.length,
      ready: draftRows.filter((item) => item.ready && !item.alreadyCommitted).length,
      alreadyCommitted: draftRows.filter((item) => item.alreadyCommitted).length,
      skipped: draftRows.filter((item) => !item.ready && !item.alreadyCommitted).length,
    };
    await writeSupplierCartState(state);
    await appendAudit(request, "supplier_cart.supplier_override", {
      entityType: "supplier_cart",
      entityId: key,
      oldValue: before,
      newValue: { supplierName: chosen.supplierName, partnerId: chosen.partnerId, offerRowId: chosen.rowId, price: chosen.price },
    });
    response.json({ ok: true, row: state.draft.rows[index] });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-picking-list/:key/replace-supplier", requireStaff, async (request, response, next) => {
  try {
    const key = cleanText(request.params.key);
    const partnerId = cleanText(request.body?.partnerId);
    if (!partnerId) return response.status(400).json({ error: "partnerId is required.", code: "supplier_required" });
    const state = await readSupplierPickingState();
    const current = state.rows[key] ? normalizeSupplierPickingRow(state.rows[key]) : null;
    if (!current) return response.status(404).json({ error: "Picking row not found.", code: "supplier_picking_not_found" });
    if (!["open", "missing", "picked"].includes(current.status)) {
      return response.status(409).json({ error: "Заменить поставщика можно только для строк «к сборке», «не было» или «собрано».", code: "supplier_picking_finalized" });
    }
    if (cleanText(current.partnerId).toLowerCase() === partnerId.toLowerCase()) {
      return response.status(400).json({ error: "Это текущий поставщик строки.", code: "supplier_option_same" });
    }
    const { options } = await listSupplierCartSupplierOptions(current.offerId);
    const chosen = pickSupplierCartOption(options, partnerId, request.body?.rowId);
    const rejection = supplierCartOptionRejection(chosen);
    if (rejection) return response.status(rejection.status).json({ error: rejection.error, code: rejection.code });

    const now = new Date();
    const username = requestUsername(request);

    // Find all sibling unit rows that share the same PM RequestRow (requestRowId).
    // When an order has quantity>1, createSupplierPickingRows splits it into :u0, :u1, …
    // — all pointing to the same PM row. We must replace them all at once so none are
    // left as orphans pointing to the deleted PM row.
    const siblingRows = [];
    if (current.requestRowId) {
      for (const [sibKey, sibData] of Object.entries(state.rows)) {
        if (sibKey === key) continue;
        const sib = normalizeSupplierPickingRow(sibData);
        if (
          cleanText(sib.requestRowId) === cleanText(current.requestRowId)
          && ["open", "picked"].includes(sib.status)
        ) {
          siblingRows.push(sib);
        }
      }
    }
    // Total units = current + all open siblings sharing the same PM row.
    const totalQuantity = [current, ...siblingRows].reduce((sum, r) => sum + Math.max(1, Number(r.quantity || 1)), 0);

    // 1) Mark current and all siblings as «не было»: block old supplier + free cart slot.
    const nextRetryAt = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString();
    const missingRow = normalizeSupplierPickingRow({
      ...current,
      status: "missing",
      missingBy: current.missingBy || username,
      missingAt: current.missingAt || now.toISOString(),
      missingReason: current.missingReason || cleanText(request.body?.reason || "supplier_replaced"),
      nextRetryAt: current.nextRetryAt || nextRetryAt,
    });
    state.rows[key] = missingRow;
    for (const sib of siblingRows) {
      state.rows[sib.key] = normalizeSupplierPickingRow({
        ...sib,
        status: "missing",
        missingBy: username,
        missingAt: now.toISOString(),
        missingReason: "supplier_replaced_sibling",
        nextRetryAt,
      });
    }
    await writeSupplierPickingState(state);
    const cartState = await readSupplierCartState();
    if (current.offerId && current.partnerId) {
      const blockKey = supplierBlockKey(current.offerId, current.partnerId);
      cartState.supplierBlocks[blockKey] = {
        offerId: current.offerId,
        partnerId: current.partnerId,
        supplierName: current.supplierName,
        reason: "supplier_replaced",
        blockedAt: now.toISOString(),
        blockedBy: username,
        expiresAt: missingRow.nextRetryAt,
        sourcePickingKey: current.key,
      };
    }
    const sourceCartKey = supplierCartSourceKeyForPickingRow(current);
    if (cartState.processed?.[sourceCartKey]) delete cartState.processed[sourceCartKey];
    if (cartState.processed?.[current.key]) delete cartState.processed[current.key];
    for (const sib of siblingRows) {
      const sibCartKey = supplierCartSourceKeyForPickingRow(sib);
      if (cartState.processed?.[sibCartKey]) delete cartState.processed[sibCartKey];
      if (cartState.processed?.[sib.key]) delete cartState.processed[sib.key];
    }
    await writeSupplierCartState(cartState);

    // 2) Убираем старую PM-строку (одна на все unit-ряды).
    let priceMasterCleanup = null;
    if (current.requestRowId) {
      priceMasterCleanup = await deleteSupplierCartPriceMasterRow(current).catch((error) => {
        logger.warn("replace supplier PM cleanup failed", { key, detail: error?.message || String(error) });
        return { error: error?.message || String(error) };
      });
    }

    // 3) Новая заявка новому поставщику — ОДНА строка с суммарным quantity всех unit-рядов.
    // key = sourceCartKey (base cart-draft key without :uN suffix) so that
    // createSupplierPickingRows expands it correctly into :u0/:u1/... retry rows
    // and insertSupplierCartRowsIntoPriceMaster finds the processed entry cleared.
    const newCartRow = normalizeSupplierCartPreviewRow({
      key: sourceCartKey,
      marketplace: current.marketplace,
      accountName: current.accountName,
      orderId: current.orderId,
      postingNumber: current.postingNumber,
      itemId: current.offerId,
      offerId: current.offerId,
      productName: current.productName,
      quantity: totalQuantity,
      warehouseProductId: current.warehouseProductId,
      supplierName: chosen.supplierName,
      partnerId: chosen.partnerId,
      offerRowId: chosen.rowId,
      price: chosen.price,
      originalPrice: chosen.originalPrice,
      priceCurrency: chosen.priceCurrency,
      saleAmount: current.saleAmount,
      payoutAmount: current.payoutAmount,
      soldAt: current.soldAt,
      trustFactor: chosen.trustFactor,
      orderCutoffTime: chosen.orderCutoffTime,
      reseller: chosen.reseller,
      supplierScore: chosen.score,
      available: true,
      ready: true,
      stockOnlyFallback: chosen.stockOnly,
    });
    const commit = await insertSupplierCartRowsIntoPriceMaster([newCartRow], request);
    // processed должен остаться под исходным ключом заказа, иначе следующий прогон
    // автокорзины перезакажет ту же позицию ещё раз.
    if (current.key !== sourceCartKey) {
      const afterState = await readSupplierCartState();
      const processedEntry = afterState.processed?.[current.key];
      if (processedEntry) {
        afterState.processed[sourceCartKey] = { ...processedEntry, key: sourceCartKey };
        delete afterState.processed[current.key];
        await writeSupplierCartState(afterState);
      }
    }
    const newPickingRow = commit.pickingCreated?.[0] || null;
    await appendAudit(request, "supplier_picking.supplier_replaced", {
      entityType: "supplier_picking",
      entityId: key,
      oldValue: { supplierName: current.supplierName, partnerId: current.partnerId, requestDocId: current.requestDocId, requestRowId: current.requestRowId },
      newValue: {
        supplierName: chosen.supplierName,
        partnerId: chosen.partnerId,
        newPickingKey: newPickingRow?.key || "",
        docIds: commit.docIds,
        priceMasterCleanup,
        siblingKeysReplaced: siblingRows.map((s) => s.key),
        totalQuantity,
      },
    });
    response.json({
      ok: true,
      oldKey: key,
      row: newPickingRow,
      supplierName: chosen.supplierName,
      inserted: commit.inserted.length,
      pmBlocked: (commit.pmBlocked || []).map((b) => ({ offerId: b.offerId, productName: b.productName, existingDocId: b.existingDocId, existingDocDate: b.existingDocDate })),
      docIds: commit.docIds,
      verifiedInPriceMaster: Boolean(commit.verification?.ok),
      priceMasterCleanup,
      siblingKeysReplaced: siblingRows.map((s) => s.key),
      totalQuantity,
    });
  } catch (error) {
    next(error);
  }
});
