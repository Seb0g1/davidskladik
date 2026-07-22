async function confirmOzonPostingPackaged(postingNumber, products = [], account = null) {
  if (!postingNumber || !products.length) return null;
  try {
    const result = await ozonRequest("/v1/posting/fbs/package", {
      posting_number: postingNumber,
      packages: [{ products }],
    }, account);
    logger.info("ozon posting packaged", { postingNumber, products: products.length });
    return { ok: true, postingNumber, result };
  } catch (error) {
    logger.warn("ozon posting package confirmation failed", {
      postingNumber,
      detail: error?.message || String(error),
    });
    return { ok: false, postingNumber, error: error?.message || String(error) };
  }
}

async function confirmYandexOrderReadyToShip(orderId, campaignId) {
  if (!orderId || !campaignId) return null;
  const shop = getYandexShopByTarget(campaignId) || getYandexShopByTarget("yandex");
  if (!shop) {
    logger.warn("yandex order ready-to-ship: shop not found", { orderId, campaignId });
    return { ok: false, orderId, reason: "shop_not_found" };
  }
  try {
    const result = await yandexRequest(shop, "PUT", `/v2/campaigns/${campaignId}/orders/${orderId}/status`, {
      order: { status: "PROCESSING", substatus: "READY_TO_SHIP" },
    });
    logger.info("yandex order ready-to-ship confirmed", { orderId, campaignId });
    return { ok: true, orderId, campaignId, result };
  } catch (error) {
    logger.warn("yandex order ready-to-ship failed", {
      orderId,
      campaignId,
      detail: error?.message || String(error),
    });
    return { ok: false, orderId, campaignId, error: error?.message || String(error) };
  }
}

async function confirmWbOrderShipped(orderId, account = null) {
  if (!orderId) return null;
  const wbAccount = account || getWbAccounts({ includeSyncDisabled: true })[0];
  if (!wbAccount) return { ok: false, orderId, reason: "no_wb_account" };
  try {
    const supply = await wbRequest(wbAccount, "marketplace", "POST", "/api/v3/supplies", {
      name: `DavidSklad-${new Date().toISOString().slice(0, 10)}-${orderId}`,
    });
    const supplyId = supply?.id;
    if (!supplyId) return { ok: false, orderId, reason: "no_supply_id", raw: supply };
    await wbRequest(wbAccount, "marketplace", "PATCH", `/api/v3/supplies/${supplyId}/orders/${orderId}`);
    await wbRequest(wbAccount, "marketplace", "PATCH", `/api/v3/supplies/${supplyId}/deliver`);
    logger.info("wb order shipped to supply", { orderId, supplyId });
    return { ok: true, orderId, supplyId };
  } catch (error) {
    logger.warn("wb order supply create failed", { orderId, detail: error?.message || String(error) });
    return { ok: false, orderId, error: error?.message || String(error) };
  }
}

async function confirmMarketplaceOrdersAfterInsert(inserted = []) {
  const results = [];

  // Ozon: сгруппировать не-экспресс строки по postingNumber и подтвердить упаковку
  const ozonRows = inserted.filter(
    (row) => row.marketplace === "ozon" && !row.isExpress && row.postingNumber && row.ozonProductId,
  );
  const byPosting = new Map();
  for (const row of ozonRows) {
    if (!byPosting.has(row.postingNumber)) byPosting.set(row.postingNumber, []);
    byPosting.get(row.postingNumber).push({
      product_id: Number(row.ozonProductId),
      quantity: Math.max(1, Math.round(Number(row.quantity || 1))),
    });
  }
  for (const [postingNumber, products] of byPosting.entries()) {
    results.push(await confirmOzonPostingPackaged(postingNumber, products));
  }

  // Yandex: сгруппировать не-экспресс строки по orderId и подтвердить READY_TO_SHIP
  const yandexOrders = new Map();
  for (const row of inserted) {
    if (row.marketplace !== "yandex" || row.isExpress || !row.orderId) continue;
    const campaignId = cleanText(row.campaignId || "");
    if (!yandexOrders.has(row.orderId)) yandexOrders.set(row.orderId, campaignId);
  }
  for (const [orderId, campaignId] of yandexOrders.entries()) {
    results.push(await confirmYandexOrderReadyToShip(orderId, campaignId));
  }

  return results;
}

async function insertSupplierCartRowsIntoPriceMaster(rows = [], request = null) {
  const readyRows = rows.map(normalizeSupplierCartPreviewRow).filter((row) => row.ready && !row.alreadyCommitted && row.offerRowId && row.partnerId);
  if (!readyRows.length) return { inserted: [], skipped: rows.length, docIds: [] };
  const state = await readSupplierCartState();
  const freshRows = readyRows.filter((row) => !state.processed?.[row.key]);
  if (!freshRows.length) return { inserted: [], skipped: readyRows.length, docIds: [] };

  // Items returned from ПВЗ: use physical stock instead of re-ordering from PM
  const pickingStateCheck = await readSupplierPickingState();
  const returnedByOfferId = new Map();
  for (const pRow of Object.values(pickingStateCheck.rows || {})) {
    const pr = normalizeSupplierPickingRow(pRow);
    if (pr.status === "returned" && pr.offerId) {
      const key = cleanText(pr.offerId).toLowerCase();
      if (!returnedByOfferId.has(key)) returnedByOfferId.set(key, pr);
    }
  }
  const pmRows = returnedByOfferId.size
    ? freshRows.filter((row) => !returnedByOfferId.has(cleanText(row.offerId).toLowerCase()))
    : freshRows;
  const returnCoveredRows = returnedByOfferId.size
    ? freshRows.filter((row) => returnedByOfferId.has(cleanText(row.offerId).toLowerCase()))
    : [];

  const byPartner = new Map();
  for (const row of pmRows) {
    const partnerId = cleanText(row.partnerId);
    if (!byPartner.has(partnerId)) byPartner.set(partnerId, []);
    byPartner.get(partnerId).push(row);
  }
  const connection = await pool.getConnection();
  const inserted = [];
  const docIds = [];
  let verification = null;
  let lockAcquired = false;
  try {
    const [lockRows] = await connection.query("SELECT GET_LOCK('davidsklad_supplier_cart', 10) AS locked");
    lockAcquired = Number(lockRows?.[0]?.locked || 0) === 1;
    if (!lockAcquired) {
      const error = new Error("PriceMaster cart is busy. Try again in a few seconds.");
      error.statusCode = 409;
      throw error;
    }
    await connection.beginTransaction();
    const [[docMax]] = await connection.query("SELECT COALESCE(MAX(DocID), 0) AS maxDocId FROM RequestDocs");
    const [[rowMax]] = await connection.query("SELECT COALESCE(MAX(RowID), 0) AS maxRowId FROM RequestRows");
    let nextDocId = Number(docMax?.maxDocId || 0) + 1;
    let nextRowId = Number(rowMax?.maxRowId || 0) + 1;
    for (const [partnerId, partnerRows] of byPartner.entries()) {
      const docId = nextDocId++;
      const isManualBatch = partnerRows.every((row) => row.marketplace === "manual");
      const comment = isManualBatch
        ? `ДавидСклад ручной заказ ${new Date().toLocaleString("ru-RU")}`
        : `ДавидСклад автокорзина ${new Date().toLocaleString("ru-RU")}`;
      await connection.query(
        "INSERT INTO RequestDocs (DocID, DocDate, PartnerID, Sended, Recieved, Comment, Registered) VALUES (?, NOW(), ?, 0, 0, ?, 1)",
        [docId, Number(partnerId), comment],
      );
      docIds.push(docId);
      for (const row of partnerRows) {
        const requestRowId = nextRowId++;
        const manualNote = cleanText(row.manualNote || "");
        const rowComment = row.marketplace === "manual"
          ? ["ДавидСклад", "ручной", row.offerId, manualNote].filter(Boolean).join(" · ").slice(0, 250)
          : [
              "ДавидСклад",
              row.marketplace,
              row.orderId || row.postingNumber,
              row.offerId,
            ].filter(Boolean).join(" · ").slice(0, 250);
        await connection.query(
          "INSERT INTO RequestRows (RowID, OfferRowID, RequestQuant, RequestPrice, RequestComment, DocID) VALUES (?, ?, ?, 0.00, ?, ?)",
          [requestRowId, Number(row.offerRowId), Math.max(1, Math.round(Number(row.quantity || 1))), rowComment, docId],
        );
        inserted.push({
          ...row,
          requestDocId: String(docId),
          requestRowId: String(requestRowId),
          committedAt: new Date().toISOString(),
        });
      }
    }
    await connection.commit();
    verification = await verifyPriceMasterInsertedRows(inserted.map((row) => row.requestRowId), docIds, connection);
    if (inserted.length && !verification.ok) {
      const error = new Error("PriceMaster insert was not visible after write.");
      error.statusCode = 502;
      error.code = "pricemaster_insert_not_visible";
      error.detail = verification;
      throw error;
    }
  } catch (error) {
    try { await connection.rollback(); } catch (_rollbackError) {}
    throw error;
  } finally {
    if (lockAcquired) {
      try { await connection.query("SELECT RELEASE_LOCK('davidsklad_supplier_cart')"); } catch (_releaseError) {}
    }
    connection.release();
  }

  const nextState = await readSupplierCartState();
  for (const row of inserted) {
    nextState.processed[row.key] = {
      key: row.key,
      marketplace: row.marketplace,
      orderId: row.orderId,
      postingNumber: row.postingNumber,
      offerId: row.offerId,
      quantity: row.quantity,
      warehouseProductId: row.warehouseProductId,
      supplierName: row.supplierName,
      partnerId: row.partnerId,
      offerRowId: row.offerRowId,
      trustFactor: row.trustFactor,
      orderCutoffTime: row.orderCutoffTime,
      reseller: row.reseller,
      supplierScore: row.supplierScore,
      saleAmount: row.saleAmount,
      payoutAmount: row.payoutAmount,
      soldAt: row.soldAt,
      stockOnlyFallback: row.stockOnlyFallback,
      requestDocId: row.requestDocId,
      requestRowId: row.requestRowId,
      committedAt: row.committedAt,
      committedBy: requestUsername(request),
    };
  }
  if (nextState.draft?.rows?.length) {
    const processedByKey = nextState.processed || {};
    nextState.draft.rows = nextState.draft.rows.map((row) => {
      const normalized = normalizeSupplierCartPreviewRow(row);
      const processed = processedByKey[normalized.key];
      return processed
        ? normalizeSupplierCartPreviewRow({
            ...normalized,
            alreadyCommitted: true,
            requestDocId: processed.requestDocId || normalized.requestDocId,
            requestRowId: processed.requestRowId || normalized.requestRowId,
          })
        : normalized;
    });
    const rows = nextState.draft.rows;
    nextState.draft.summary = {
      ...(nextState.draft.summary || {}),
      total: rows.length,
      ready: rows.filter((row) => row.ready && !row.alreadyCommitted).length,
      alreadyCommitted: rows.filter((row) => row.alreadyCommitted).length,
      skipped: rows.filter((row) => !row.ready && !row.alreadyCommitted).length,
    };
  }
  nextState.history = [
    ...(nextState.history || []),
    {
      at: new Date().toISOString(),
      user: requestUsername(request),
      inserted: inserted.length,
      docIds,
      verifiedInPriceMaster: Boolean(verification?.ok),
      verifiedRows: Number(verification?.verifiedRows || 0),
      priceMasterDb: verification?.db || "",
      rows: inserted.map((row) => ({
        key: row.key,
        marketplace: row.marketplace,
        orderId: row.orderId,
        offerId: row.offerId,
        quantity: row.quantity,
        supplierName: row.supplierName,
        partnerId: row.partnerId,
        trustFactor: row.trustFactor,
        orderCutoffTime: row.orderCutoffTime,
        reseller: row.reseller,
        requestDocId: row.requestDocId,
        requestRowId: row.requestRowId,
      })),
    },
  ].slice(-1000);
  await writeSupplierCartState(nextState);
  await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_cart.commit", {
    entityType: "supplier_cart",
    entityId: "pricemaster",
    newValue: { inserted: inserted.length, docIds, verifiedInPriceMaster: Boolean(verification?.ok), verifiedRows: Number(verification?.verifiedRows || 0), priceMasterDb: verification?.db || "", rows: inserted },
  });
  const pickingCreated = await createSupplierPickingRows(inserted, request);

  // Claim returned items from the return pool (товары вернулись из ПВЗ)
  let returnClaimed = [];
  if (returnCoveredRows.length) {
    const psReturns = await readSupplierPickingState();
    for (const row of returnCoveredRows) {
      const offerKey = cleanText(row.offerId).toLowerCase();
      const returnedRow = returnedByOfferId.get(offerKey);
      if (!returnedRow || psReturns.rows[row.key]) continue;
      const pickingRow = normalizeSupplierPickingRow({
        ...row,
        key: row.key,
        status: "picked",
        pickedBy: "auto:return",
        pickedAt: new Date().toISOString(),
        createdAt: new Date().toISOString(),
        createdBy: requestUsername(request),
        replacementFor: returnedRow.key,
      });
      psReturns.rows[pickingRow.key] = pickingRow;
      const currentRet = psReturns.rows[returnedRow.key];
      if (currentRet) {
        psReturns.rows[returnedRow.key] = normalizeSupplierPickingRow({
          ...currentRet,
          status: "return_used",
          replacementKey: row.key,
        });
      }
      returnClaimed.push(pickingRow);
    }
    if (returnClaimed.length) {
      await writeSupplierPickingState(psReturns);
      logger.info("picking rows claimed from ПВЗ return pool", { count: returnClaimed.length });
    }
  }

  const marketplaceConfirms = await confirmMarketplaceOrdersAfterInsert(inserted).catch((error) => {
    logger.warn("marketplace order confirmation after insert failed", { detail: error?.message || String(error) });
    return [];
  });
  return {
    inserted,
    skipped: readyRows.length - inserted.length - returnCoveredRows.length,
    returnClaimed,
    docIds,
    pickingCreated: [...(Array.isArray(pickingCreated) ? pickingCreated : []), ...returnClaimed],
    marketplaceConfirms,
    verification,
  };
}

// «Не было» на сборке: отправить прикреплённого поставщика в инактив (snooze привязки),
// тем же механизмом, что и «Отложить поставщика» в карточке товара.
async function snoozeSupplierLinkForPickingRow(row = {}, days = 7) {
  const normalized = normalizeSupplierPickingRow(row);
  const productId = cleanText(normalized.warehouseProductId);
  if (!productId) return { ok: false, reason: "no_warehouse_product" };
  const [product] = await readWarehouseProductsFromPostgresByIds([productId]);
  if (!product) return { ok: false, reason: "product_not_found" };
  const partnerId = cleanText(normalized.partnerId).toLowerCase();
  const supplierName = cleanText(normalized.supplierName).toLowerCase();
  const link = (product.links || []).find((item) => partnerId && cleanText(item.partnerId).toLowerCase() === partnerId)
    || (product.links || []).find((item) => supplierName && cleanText(item.supplierName).toLowerCase() === supplierName);
  if (!link) return { ok: false, reason: "link_not_found" };
  const normalizedDays = Math.max(1, Math.round(Number(days || 7) || 7));
  const result = await applyWarehouseLinkSnooze(productId, link.id, normalizedDays, { reason: "picking_missing_snooze" });
  return {
    ok: true,
    productId,
    linkId: link.id,
    supplierName: link.supplierName || normalized.supplierName,
    days: normalizedDays,
    snoozedUntil: result.snoozedUntil,
  };
}

function supplierCartSourceKeyForPickingRow(row = {}) {
  const normalized = normalizeSupplierPickingRow(row);
  return cleanText(normalized.replacementFor || normalized.key.replace(/\|retry:.+$/, ""));
}

async function deactivateSupplierBlockForPickingRow(row = {}, request = null) {
  const normalized = normalizeSupplierPickingRow(row);
  const blockKey = supplierBlockKey(normalized.offerId, normalized.partnerId);
  if (!blockKey || blockKey === "|") return null;
  const cartState = await readSupplierCartState();
  let removed = null;
  if (cartState.supplierBlocks?.[blockKey]) {
    removed = cartState.supplierBlocks[blockKey];
    delete cartState.supplierBlocks[blockKey];
    await writeSupplierCartState(cartState);
  }
  if (shouldUsePostgresStorage()) {
    try {
      await getPrisma().supplierBlock.updateMany({
        where: { blockKey, active: true },
        data: {
          active: false,
          raw: {
            ...(removed || {}),
            cancelledBy: requestUsername(request),
            cancelledAt: new Date().toISOString(),
          },
        },
      });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("supplier block deactivate failed", { detail: error?.message || String(error) });
    }
  }
  return removed;
}

async function restoreSupplierCartProcessedForPickingRow(row = {}, request = null) {
  const normalized = normalizeSupplierPickingRow(row);
  const sourceKey = supplierCartSourceKeyForPickingRow(normalized);
  if (!sourceKey) return null;
  const cartState = await readSupplierCartState();
  cartState.processed[sourceKey] = {
    key: sourceKey,
    marketplace: normalized.marketplace,
    orderId: normalized.orderId,
    postingNumber: normalized.postingNumber,
    offerId: normalized.offerId,
    quantity: normalized.quantity,
    warehouseProductId: normalized.warehouseProductId,
    supplierName: normalized.supplierName,
    partnerId: normalized.partnerId,
    offerRowId: normalized.offerRowId,
    trustFactor: normalized.trustFactor,
    orderCutoffTime: normalized.orderCutoffTime,
    reseller: normalized.reseller,
    supplierScore: normalized.supplierScore,
    saleAmount: normalized.saleAmount,
    payoutAmount: normalized.payoutAmount,
    soldAt: normalized.soldAt,
    stockOnlyFallback: normalized.stockOnlyFallback,
    requestDocId: normalized.requestDocId,
    requestRowId: normalized.requestRowId,
    committedAt: normalized.createdAt || new Date().toISOString(),
    committedBy: requestUsername(request),
  };
  await writeSupplierCartState(cartState);
  return cartState.processed[sourceKey];
}

async function removeSupplierCartProcessedForPickingRow(row = {}) {
  const normalized = normalizeSupplierPickingRow(row);
  const sourceKey = supplierCartSourceKeyForPickingRow(normalized);
  const cartState = await readSupplierCartState();
  if (sourceKey && cartState.processed?.[sourceKey]) delete cartState.processed[sourceKey];
  if (cartState.draft?.rows?.length) {
    cartState.draft.rows = cartState.draft.rows.map((draftRow) => {
      const normalizedDraft = normalizeSupplierCartPreviewRow(draftRow);
      if (normalizedDraft.key !== sourceKey) return normalizedDraft;
      return normalizeSupplierCartPreviewRow({
        ...normalizedDraft,
        alreadyCommitted: false,
        requestDocId: "",
        requestRowId: "",
      });
    });
    const rows = cartState.draft.rows;
    cartState.draft.summary = {
      ...(cartState.draft.summary || {}),
      total: rows.length,
      ready: rows.filter((item) => item.ready && !item.alreadyCommitted).length,
      alreadyCommitted: rows.filter((item) => item.alreadyCommitted).length,
      skipped: rows.filter((item) => !item.ready && !item.alreadyCommitted).length,
    };
  }
  await writeSupplierCartState(cartState);
  return sourceKey;
}

async function deleteSupplierPickingStateRow(key = "") {
  const pickingKey = cleanText(key);
  if (!pickingKey) return;
  if (shouldUsePostgresStorage()) {
    try {
      await getPrisma().supplierPickingRow.deleteMany({ where: { pickingKey } });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("supplier picking postgres delete failed", { detail: error?.message || String(error) });
    }
  }
}

async function deleteSupplierCartPriceMasterRow(row = {}) {
  const normalized = normalizeSupplierPickingRow(row);
  const requestRowId = Number(normalized.requestRowId || 0);
  const requestDocId = Number(normalized.requestDocId || 0);
  if (!requestRowId) return { deletedRows: 0, deletedDoc: false, requestRowId: normalized.requestRowId, requestDocId: normalized.requestDocId };
  const connection = await pool.getConnection();
  let lockAcquired = false;
  try {
    const [lockRows] = await connection.query("SELECT GET_LOCK('davidsklad_supplier_cart', 10) AS locked");
    lockAcquired = Number(lockRows?.[0]?.locked || 0) === 1;
    if (!lockAcquired) {
      const error = new Error("PriceMaster cart is busy. Try again in a few seconds.");
      error.statusCode = 409;
      throw error;
    }
    await connection.beginTransaction();
    const [deleteResult] = await connection.query("DELETE FROM RequestRows WHERE RowID = ?", [requestRowId]);
    let deletedDoc = false;
    if (requestDocId) {
      const [[remaining]] = await connection.query("SELECT COUNT(*) AS count FROM RequestRows WHERE DocID = ?", [requestDocId]);
      if (Number(remaining?.count || 0) === 0) {
        await connection.query("DELETE FROM RequestDocs WHERE DocID = ?", [requestDocId]);
        deletedDoc = true;
      }
    }
    await connection.commit();
    return { deletedRows: Number(deleteResult?.affectedRows || 0), deletedDoc, requestRowId: String(requestRowId), requestDocId: requestDocId ? String(requestDocId) : "" };
  } catch (error) {
    try { await connection.rollback(); } catch (_rollbackError) {}
    throw error;
  } finally {
    if (lockAcquired) {
      try { await connection.query("SELECT RELEASE_LOCK('davidsklad_supplier_cart')"); } catch (_releaseError) {}
    }
    connection.release();
  }
}
