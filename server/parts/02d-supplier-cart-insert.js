async function insertSupplierCartRowsIntoPriceMaster(rows = [], request = null) {
  const readyRows = rows.map(normalizeSupplierCartPreviewRow).filter((row) => row.ready && !row.alreadyCommitted && row.offerRowId && row.partnerId);
  if (!readyRows.length) return { inserted: [], skipped: rows.length, docIds: [] };
  const state = await readSupplierCartState();
  const freshRows = readyRows.filter((row) => !state.processed?.[row.key]);
  if (!freshRows.length) return { inserted: [], skipped: readyRows.length, docIds: [] };
  const byPartner = new Map();
  for (const row of freshRows) {
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
      const comment = `ДавидСклад автокорзина ${new Date().toLocaleString("ru-RU")}`;
      await connection.query(
        "INSERT INTO RequestDocs (DocID, DocDate, PartnerID, Sended, Recieved, Comment, Registered) VALUES (?, NOW(), ?, 0, 0, ?, 1)",
        [docId, Number(partnerId), comment],
      );
      docIds.push(docId);
      for (const row of partnerRows) {
        const requestRowId = nextRowId++;
        const rowComment = [
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
  return { inserted, skipped: readyRows.length - inserted.length, docIds, pickingCreated, verification };
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
