async function confirmOzonPostingPackaged(postingNumber, products = [], account = null) {
  if (!postingNumber || !products.length) return null;
  try {
    const result = await ozonRequest("/v4/posting/fbs/ship", {
      posting_number: postingNumber,
      packages: [{ products }],
    }, account);
    logger.info("ozon posting shipped to awaiting_deliver", { postingNumber, products: products.length, accountId: account?.id });
    return { ok: true, postingNumber, result };
  } catch (error) {
    const ozonResponse = error?.ozon || null;
    const errMsg = error?.message || String(error);
    // Ozon returns errors when the order is already in a shipped/assembled state.
    // Treat "wrong state" as already-confirmed — the operator has nothing to do.
    const alreadyShipped = /wrong.?state|awaiting_deliver|not.*awaiting_packaging|нельзя|уже собран/i.test(errMsg)
      || (typeof ozonResponse === "object" && /wrong.?state|awaiting_deliver/i.test(String(ozonResponse?.message || "")));
    if (alreadyShipped) {
      logger.info("ozon posting already in assembled state — skipping re-ship", { postingNumber, accountId: account?.id, detail: errMsg });
      return { ok: true, postingNumber, alreadyConfirmed: true };
    }
    logger.warn("ozon posting package confirmation failed", {
      postingNumber,
      accountId: account?.id,
      products,
      detail: errMsg,
      ozonResponse,
    });
    return { ok: false, postingNumber, error: errMsg };
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

// Создать одну поставку WB для всех заказов аккаунта и добавить заказы в неё.
// НЕ вызывает /deliver — физическая отгрузка делается оператором вручную.
async function confirmWbOrdersWithSupply(rows = [], account = null) {
  const wbAccount = account || getWbAccounts({ includeSyncDisabled: true })[0];
  if (!wbAccount) return { ok: false, marketplace: "wb", reason: "no_wb_account" };
  const orderIds = [...new Set(rows.map((row) => cleanText(row.orderId)).filter(Boolean))];
  if (!orderIds.length) return { ok: true, marketplace: "wb", supplyId: null, orderIds: [] };
  try {
    const supply = await wbRequest(wbAccount, "marketplace", "POST", "/api/v3/supplies", {
      name: `ДавидСклад-${new Date().toISOString().slice(0, 10)}`,
    });
    const supplyId = supply?.id;
    if (!supplyId) return { ok: false, marketplace: "wb", reason: "no_supply_id", raw: supply };
    const addResults = [];
    for (const orderId of orderIds) {
      const numericId = Number(orderId);
      if (!numericId) { addResults.push({ orderId, ok: false, reason: "non_numeric_id" }); continue; }
      try {
        await wbRequest(wbAccount, "marketplace", "PATCH", `/api/v3/supplies/${supplyId}/orders/${numericId}`);
        addResults.push({ orderId, ok: true });
      } catch (error) {
        addResults.push({ orderId, ok: false, error: error?.message || String(error) });
      }
    }
    const added = addResults.filter((r) => r.ok).length;
    logger.info("wb supply created for cart orders", { supplyId, total: orderIds.length, added });
    return { ok: true, marketplace: "wb", supplyId, orderIds, addResults };
  } catch (error) {
    logger.warn("wb supply create for cart failed", { detail: error?.message || String(error) });
    return { ok: false, marketplace: "wb", error: error?.message || String(error) };
  }
}

async function confirmMarketplaceOrdersAfterInsert(inserted = []) {
  const results = [];

  // Ozon: сгруппировать не-экспресс строки по postingNumber, подтвердить упаковку с правильным кабинетом
  const byPosting = new Map();
  for (const row of inserted) {
    if (row.marketplace !== "ozon" || row.isExpress || !row.postingNumber || !row.ozonProductId) {
      if (row.marketplace === "ozon") {
        const skipReason = row.isExpress ? "express" : !row.postingNumber ? "no_posting_number" : "no_ozon_product_id";
        logger.warn("ozon posting confirm skipped", { postingNumber: row.postingNumber, ozonProductId: row.ozonProductId, isExpress: row.isExpress, offerId: row.offerId, reason: skipReason });
      }
      continue;
    }
    if (!byPosting.has(row.postingNumber)) byPosting.set(row.postingNumber, { products: [], accountId: cleanText(row.accountId || "") });
    byPosting.get(row.postingNumber).products.push({
      product_id: Number(row.ozonProductId),
      quantity: Math.max(1, Math.round(Number(row.quantity || 1))),
    });
  }
  for (const [postingNumber, { products, accountId }] of byPosting.entries()) {
    const account = getOzonAccountByTarget(accountId) || getOzonAccountByTarget("ozon");
    results.push(await confirmOzonPostingPackaged(postingNumber, products, account));
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

  // WB: сгруппировать заказы по кабинету, создать поставку и добавить в неё (без deliver)
  const wbByAccount = new Map();
  for (const row of inserted) {
    if (row.marketplace !== "wb" || !row.orderId) continue;
    const accountId = cleanText(row.accountId || "wb");
    if (!wbByAccount.has(accountId)) wbByAccount.set(accountId, []);
    wbByAccount.get(accountId).push(row);
  }
  for (const [accountId, wbRows] of wbByAccount.entries()) {
    const account = getWbAccountByTarget(accountId) || getWbAccounts({ includeSyncDisabled: true })[0];
    results.push(await confirmWbOrdersWithSupply(wbRows, account));
  }

  return results;
}

async function insertSupplierCartRowsIntoPriceMaster(rows = [], request = null, options = {}) {
  const allNormalized = rows.map(normalizeSupplierCartPreviewRow);
  const readyRows = allNormalized.filter((row) => row.ready && !row.alreadyCommitted && row.offerRowId && row.partnerId);
  const skippedNotReady = allNormalized
    .filter((row) => !row.ready || row.alreadyCommitted || !row.offerRowId || !row.partnerId)
    .map((row) => ({
      key: row.key,
      offerId: row.offerId,
      productName: row.productName,
      skipReason: row.skipReason || (row.alreadyCommitted ? "already_committed" : !row.ready ? "not_ready" : !row.offerRowId ? "no_offer_row_id" : "no_partner_id"),
    }));
  if (!readyRows.length) return { inserted: [], skipped: rows.length, skippedDetails: skippedNotReady, docIds: [] };
  const state = await readSupplierCartState();
  const freshRows = readyRows.filter((row) => !state.processed?.[row.key]);
  const staleSkipped = readyRows
    .filter((row) => state.processed?.[row.key])
    .map((row) => ({ key: row.key, offerId: row.offerId, productName: row.productName, skipReason: "already_in_state" }));
  const skippedDetails = [...skippedNotReady, ...staleSkipped];
  if (!freshRows.length) return { inserted: [], skipped: readyRows.length, skippedDetails, docIds: [] };

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

  // Live PM validation: before writing, confirm each selected OfferRow is still
  // active and has a price. Stock-only rows bypass this check — their PM row
  // may intentionally be Active=0 (price comes from our own warehouse stock).
  // If the pool is unavailable we fail open and proceed with snapshot data.
  let liveInactiveRowIds = new Set();
  // inactivePm rows were explicitly chosen by the user despite Active=0 — skip live validation,
  // same as stock-only rows. Otherwise the user-selected inactive supplier gets silently rejected.
  const rowsNeedingValidation = pmRows.filter((row) => !row.stockOnlyFallback && !row.inactivePm && row.offerRowId);
  if (rowsNeedingValidation.length && pool) {
    try {
      const rowIdsToCheck = [...new Set(rowsNeedingValidation.map((row) => Number(row.offerRowId)).filter((id) => id > 0))];
      if (rowIdsToCheck.length) {
        const [liveRows] = await pool.query(
          "SELECT RowID FROM OfferRows WHERE RowID IN (?) AND Active = 1 AND NativePrice > 0",
          [rowIdsToCheck],
        );
        const liveActiveIds = new Set((liveRows || []).map((r) => Number(r.RowID)));
        liveInactiveRowIds = new Set(rowIdsToCheck.filter((id) => !liveActiveIds.has(id)));
      }
    } catch (liveValidationError) {
      logger.warn("supplier_cart_live_validation_failed_open", { detail: liveValidationError?.message || String(liveValidationError) });
    }
  }

  // Split pmRows into validated (will be inserted) and live-inactive (will be skipped).
  const validatedPmRows = [];
  const liveInactiveSkipped = [];
  for (const row of pmRows) {
    if (!row.stockOnlyFallback && row.offerRowId && liveInactiveRowIds.has(Number(row.offerRowId))) {
      logger.warn("supplier_cart_live_validation_skip", {
        rowId: row.offerRowId,
        productName: row.productName,
        offerId: row.offerId,
        partnerId: row.partnerId,
        reason: "inactive",
      });
      liveInactiveSkipped.push({
        key: row.key,
        offerId: row.offerId,
        productName: row.productName,
        skipReason: "supplier_inactive_live",
      });
    } else {
      validatedPmRows.push(row);
    }
  }
  skippedDetails.push(...liveInactiveSkipped);

  const byPartner = new Map();
  for (const row of validatedPmRows) {
    const partnerId = cleanText(row.partnerId);
    if (!byPartner.has(partnerId)) byPartner.set(partnerId, []);
    byPartner.get(partnerId).push(row);
  }
  const connection = await pool.getConnection();
  const inserted = [];
  const pmBlocked = [];
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
      // Reuse an open doc for this supplier from today if one exists
      let docId;
      const [[existingDoc]] = await connection.query(
        "SELECT DocID FROM RequestDocs WHERE PartnerID=? AND Sended=0 AND Recieved=0 AND DATE(DocDate)=CURDATE() ORDER BY DocID DESC LIMIT 1",
        [Number(partnerId)],
      );
      if (existingDoc?.DocID) {
        docId = Number(existingDoc.DocID);
      } else {
        docId = nextDocId++;
        const comment = new Date().toLocaleDateString("ru-RU");
        await connection.query(
          "INSERT INTO RequestDocs (DocID, DocDate, PartnerID, Sended, Recieved, Comment, Registered) VALUES (?, NOW(), ?, 0, 0, ?, 1)",
          [docId, Number(partnerId), comment],
        );
      }
      docIds.push(docId);
      // Merge rows for the same product (offerId) from the same supplier into one PM line
      // with summed quantity. PM sometimes has multiple OfferRows for the same article, so
      // merging by offerRowId alone misses orders that resolved to different row IDs.
      // We use offerId as the canonical merge key and keep the first-resolved offerRowId.
      const mergedByOfferId = new Map();
      for (const row of partnerRows) {
        const key = cleanText(row.offerId).toLowerCase() || String(row.offerRowId);
        const qty = Math.max(1, Math.round(Number(row.quantity || 1)));
        if (mergedByOfferId.has(key)) {
          const entry = mergedByOfferId.get(key);
          entry.totalQuantity += qty;
          entry.sourceRows.push({ ...row, quantity: qty });
        } else {
          mergedByOfferId.set(key, { ...row, totalQuantity: qty, sourceRows: [{ ...row, quantity: qty }] });
        }
      }
      for (const entry of mergedByOfferId.values()) {
        // Dedup: check if this OfferRowID already has an undelivered (Recieved=0) RequestRows entry.
        const [[existingRow]] = await connection.query(
          `SELECT rr.RowID, rd.DocID, rd.Sended, rd.DocDate FROM RequestRows rr
           JOIN RequestDocs rd ON rd.DocID = rr.DocID
           WHERE rr.OfferRowID = ? AND rd.PartnerID = ? AND rd.Recieved = 0`,
          [Number(entry.offerRowId), Number(partnerId)],
        );
        if (existingRow?.RowID && Number(existingRow.Sended) === 0) {
          // Open order (not yet sent to supplier) — merge quantity into existing row instead of blocking.
          await connection.query(
            "UPDATE RequestRows SET RequestQuant = RequestQuant + ? WHERE RowID = ?",
            [entry.totalQuantity, Number(existingRow.RowID)],
          );
          logger.info("supplier_cart_insert_quantity_merged", { offerRowId: entry.offerRowId, partnerId, existingRowId: existingRow.RowID, existingDocId: existingRow.DocID, addedQty: entry.totalQuantity });
          const committedAt = new Date().toISOString();
          for (const sourceRow of entry.sourceRows) {
            inserted.push({ ...sourceRow, requestDocId: String(existingRow.DocID), requestRowId: String(existingRow.RowID), committedAt });
          }
          continue;
        }
        if (existingRow?.RowID) {
          // Sended=1: in-transit, but a NEW marketplace order has come in for the same product.
          // Insert a fresh PM row — the supplier will fulfil both requests separately.
          logger.info("supplier_cart_insert_new_despite_transit", { offerRowId: entry.offerRowId, partnerId, existingRowId: existingRow.RowID, existingDocId: existingRow.DocID });
        }
        const requestRowId = nextRowId++;
        const manualNote = cleanText(entry.manualNote || "");
        const rowComment = manualNote.slice(0, 250);
        await connection.query(
          "INSERT INTO RequestRows (RowID, OfferRowID, RequestQuant, RequestPrice, RequestComment, DocID) VALUES (?, ?, ?, 0.00, ?, ?)",
          [requestRowId, Number(entry.offerRowId), entry.totalQuantity, rowComment, docId],
        );
        // Include every source order as a separate inserted entry sharing the same PM row.
        // This ensures each order key is marked processed and gets its own picking row.
        const committedAt = new Date().toISOString();
        for (const sourceRow of entry.sourceRows) {
          inserted.push({
            ...sourceRow,
            requestDocId: String(docId),
            requestRowId: String(requestRowId),
            committedAt,
          });
        }
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
  // Sync pmBlocked items into state.processed so the draft shows them as alreadyCommitted.
  // This reconciles cases where the PM insert succeeded but the Postgres state write failed.
  const syncedAt = new Date().toISOString();
  for (const blocked of pmBlocked) {
    for (const sourceRow of blocked.sourceRows || []) {
      const normalized = normalizeSupplierCartPreviewRow(sourceRow);
      if (!normalized.key || nextState.processed?.[normalized.key]) continue;
      nextState.processed[normalized.key] = {
        key: normalized.key,
        marketplace: normalized.marketplace,
        orderId: normalized.orderId,
        postingNumber: normalized.postingNumber,
        offerId: blocked.offerId,
        quantity: normalized.quantity,
        warehouseProductId: normalized.warehouseProductId,
        supplierName: normalized.supplierName,
        partnerId: blocked.partnerId,
        offerRowId: blocked.offerRowId,
        requestDocId: String(blocked.existingDocId),
        requestRowId: String(blocked.existingRowId),
        committedAt: syncedAt,
        committedBy: "pm_sync",
      };
    }
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
  // Накапливаем дневной итог заказа (price * quantity по каждой вставленной строке)
  try {
    const todayKey = new Date().toISOString().slice(0, 10);
    const addTotal = inserted.reduce((sum, row) => sum + (Number(row.price) || 0) * Math.max(1, Math.round(Number(row.quantity || 1))), 0);
    const addItems = inserted.reduce((sum, row) => sum + Math.max(1, Math.round(Number(row.quantity || 1))), 0);
    if (addTotal > 0 || addItems > 0) await adjustDailyCartTotal(todayKey, addTotal, addItems);
  } catch (e) {
    logger.warn("daily_cart_total accumulate failed", { detail: e?.message || String(e) });
  }
  await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_cart.commit", {
    entityType: "supplier_cart",
    entityId: "pricemaster",
    newValue: { inserted: inserted.length, docIds, verifiedInPriceMaster: Boolean(verification?.ok), verifiedRows: Number(verification?.verifiedRows || 0), priceMasterDb: verification?.db || "", rows: inserted },
  });
  const pickingCreated = await createSupplierPickingRows(inserted, request, options);

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

  // Create picking rows for pmBlocked items (already in PM, state desync recovery).
  // These items were not inserted (PM dedup blocked them) but may be missing picking rows
  // if a previous crash interrupted state write after PM commit.
  const blockedSourceRows = pmBlocked.flatMap((b) =>
    (b.sourceRows || []).map((row) => ({
      ...normalizeSupplierCartPreviewRow(row),
      requestDocId: String(b.existingDocId || ""),
      requestRowId: String(b.existingRowId || ""),
    })),
  );
  const pickingBlockedCreated = blockedSourceRows.length
    ? await createSupplierPickingRows(blockedSourceRows, request, options)
    : [];

  // Run marketplace confirmations (Ozon ship, Yandex status, WB supply) in the background
  // so the HTTP response is not blocked by slow external API calls.
  // Also confirm orders for pmBlocked items that were auto-synced (previously committed to PM
  // but marketplace was never notified — state write failed at the time of the original commit).
  const syncedRows = pmBlocked.flatMap((b) => b.sourceRows || []);
  const toConfirm = [...inserted, ...syncedRows];
  // Run confirmations synchronously with a 6-second ceiling so the HTTP response includes
  // the Ozon/Yandex/WB results. On timeout we resolve to [] and let the background tail
  // finish on its own — the operator can use /reconfirm-marketplace as fallback.
  let marketplaceConfirms = [];
  const confirmPromise = confirmMarketplaceOrdersAfterInsert(toConfirm);
  const timeoutPromise = new Promise((resolve) => setTimeout(() => resolve(null), 6000));
  const confirmResult = await Promise.race([confirmPromise, timeoutPromise]);
  if (confirmResult !== null) {
    marketplaceConfirms = confirmResult;
  } else {
    logger.warn("marketplace confirmations timed out — running in background", { count: toConfirm.length });
    confirmPromise.catch((error) => {
      logger.warn("marketplace order confirmation after insert failed", { detail: error?.message || String(error) });
    });
  }
  return {
    inserted,
    skipped: readyRows.length - inserted.length - returnCoveredRows.length,
    skippedDetails,
    pmBlocked,
    returnClaimed,
    docIds,
    pickingCreated: [
      ...(Array.isArray(pickingCreated) ? pickingCreated : []),
      ...returnClaimed,
      ...pickingBlockedCreated,
    ],
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
  // Strip :uN unit suffix (and any trailing |retry:... on the same unit) then strip any bare |retry:...
  // so we always land on the original cart-draft key regardless of how many replacements have happened.
  const base = normalized.replacementFor || normalized.key;
  return cleanText(base.replace(/:u\d+(?:\|retry:[^|]*)?$/, "").replace(/\|retry:[^|]*$/, ""));
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
