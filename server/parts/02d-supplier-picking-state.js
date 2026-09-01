function normalizeSupplierPickingRow(input = {}) {
  const key = cleanText(input.key || supplierCartItemKey(input));
  const status = ["picked", "missing", "reordered", "returned", "return_used"].includes(cleanText(input.status).toLowerCase())
    ? cleanText(input.status).toLowerCase()
    : "open";
  return {
    key,
    marketplace: cleanText(input.marketplace).toLowerCase(),
    accountName: cleanText(input.accountName || input.account_name),
    orderId: cleanText(input.orderId || input.order_id),
    postingNumber: cleanText(input.postingNumber || input.posting_number),
    offerId: cleanText(input.offerId || input.offer_id),
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity: Math.max(1, Math.round(Number(input.quantity || 1) || 1)),
    warehouseProductId: cleanText(input.warehouseProductId || input.productId || input.product_id),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
    offerRowId: cleanText(input.offerRowId || input.rowId || input.sourceRowId),
    price: Number(input.price || 0) || 0,
    priceCurrency: cleanText(input.priceCurrency || input.currency || "USD").toUpperCase(),
    saleAmount: input.saleAmount === undefined && input.sale_amount === undefined
      ? (computeMarketplaceSaleAmountRub(input) ?? null)
      : normalizeFinanceMoney(input.saleAmount ?? input.sale_amount, 0),
    payoutAmount: input.payoutAmount === undefined && input.payout_amount === undefined
      ? (computeMarketplaceSaleAmountRub(input) ?? null)
      : normalizeFinanceMoney(input.payoutAmount ?? input.payout_amount, 0),
    soldAt: input.soldAt || input.sold_at || input.orderedAt || null,
    stockOnlyFallback: Boolean(input.stockOnlyFallback),
    trustFactor: normalizeSupplierTrustFactor(input.trustFactor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(input.orderCutoffTime || input.order_cutoff_time),
    reseller: Boolean(input.reseller),
    supplierScore: Number(input.supplierScore || input.score || 0) || 0,
    isExpress: Boolean(input.isExpress),
    ozonProductId: Number(input.ozonProductId || 0) || 0,
    campaignId: cleanText(input.campaignId || input.campaign_id),
    requestDocId: cleanText(input.requestDocId || input.docId),
    requestRowId: cleanText(input.requestRowId || input.rowId),
    pickedQuantity: input.pickedQuantity != null ? Math.max(1, Math.round(Number(input.pickedQuantity) || 1)) : null,
    pricePaidRub: input.pricePaidRub != null ? (normalizeFinanceMoney(input.pricePaidRub, 0) || null) : null,
    status,
    createdAt: input.createdAt || new Date().toISOString(),
    createdBy: cleanText(input.createdBy || input.committedBy),
    pickedBy: cleanText(input.pickedBy),
    pickedAt: input.pickedAt || null,
    missingBy: cleanText(input.missingBy),
    missingAt: input.missingAt || null,
    missingReason: cleanText(input.missingReason || input.reason),
    missingSnoozeDays: Math.max(0, Math.round(Number(input.missingSnoozeDays || 0) || 0)),
    missingPermanent: Boolean(input.missingPermanent),
    missingSnoozeLinkId: cleanText(input.missingSnoozeLinkId),
    nextRetryAt: input.nextRetryAt || null,
    replacementFor: cleanText(input.replacementFor),
    replacementKey: cleanText(input.replacementKey),
    returnedBy: cleanText(input.returnedBy),
    returnedAt: input.returnedAt || null,
    wbSupplyId: cleanText(input.wbSupplyId || input.wb_supply_id),
    deferredUntil: input.deferredUntil || null,
  };
}

function compareSupplierPickingRows(left = {}, right = {}) {
  const supplierCompare = String(left.supplierName || "").localeCompare(String(right.supplierName || ""), "ru", { sensitivity: "base" });
  if (supplierCompare !== 0) return supplierCompare;
  const productLeft = String(left.productName || left.offerId || "");
  const productRight = String(right.productName || right.offerId || "");
  const productCompare = productLeft.localeCompare(productRight, "ru", { sensitivity: "base" });
  if (productCompare !== 0) return productCompare;
  return String(left.key || "").localeCompare(String(right.key || ""), "ru", { sensitivity: "base" });
}

function normalizeSupplierPickingState(input = {}) {
  const rows = input.rows && typeof input.rows === "object" && !Array.isArray(input.rows) ? input.rows : {};
  const invoices = Array.isArray(input.invoices) ? input.invoices : [];
  const normalizedRows = {};
  for (const [key, row] of Object.entries(rows)) {
    const normalized = normalizeSupplierPickingRow({ ...row, key: row?.key || key });
    if (normalized.key) normalizedRows[normalized.key] = normalized;
  }
  return {
    updatedAt: input.updatedAt || null,
    rows: normalizedRows,
    invoices: invoices.filter((item) => item && typeof item === "object").slice(-1000),
  };
}

function supplierPickingRowToPostgres(row = {}) {
  const normalized = normalizeSupplierPickingRow(row);
  return {
    pickingKey: normalized.key,
    marketplace: normalized.marketplace || null,
    accountName: normalized.accountName || null,
    orderId: normalized.orderId || null,
    postingNumber: normalized.postingNumber || null,
    offerId: normalized.offerId || null,
    productName: normalized.productName || null,
    quantity: normalized.quantity,
    supplierName: normalized.supplierName || null,
    partnerId: normalized.partnerId || null,
    offerRowId: normalized.offerRowId || null,
    price: normalized.price || null,
    priceCurrency: normalized.priceCurrency || null,
    trustFactor: normalized.trustFactor,
    orderCutoffTime: normalized.orderCutoffTime || null,
    reseller: Boolean(normalized.reseller),
    supplierScore: normalized.supplierScore || null,
    requestDocId: normalized.requestDocId || null,
    requestRowId: normalized.requestRowId || null,
    status: normalized.status,
    createdBy: normalized.createdBy || null,
    pickedBy: normalized.pickedBy || null,
    pickedAt: toDateOrNull(normalized.pickedAt),
    missingBy: normalized.missingBy || null,
    missingAt: toDateOrNull(normalized.missingAt),
    missingReason: normalized.missingReason || null,
    nextRetryAt: toDateOrNull(normalized.nextRetryAt),
    replacementFor: normalized.replacementFor || null,
    replacementKey: normalized.replacementKey || null,
    raw: normalized,
  };
}

function supplierPickingRowFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeSupplierPickingRow({
    ...raw,
    key: row.pickingKey || raw.key,
    marketplace: row.marketplace || raw.marketplace,
    accountName: row.accountName || raw.accountName,
    orderId: row.orderId || raw.orderId,
    postingNumber: row.postingNumber || raw.postingNumber,
    offerId: row.offerId || raw.offerId,
    productName: row.productName || raw.productName,
    quantity: row.quantity ?? raw.quantity,
    warehouseProductId: raw.warehouseProductId,
    supplierName: row.supplierName || raw.supplierName,
    partnerId: row.partnerId || raw.partnerId,
    offerRowId: row.offerRowId || raw.offerRowId,
    price: row.price === null || row.price === undefined ? raw.price : Number(row.price),
    priceCurrency: row.priceCurrency || raw.priceCurrency,
    saleAmount: raw.saleAmount,
    payoutAmount: raw.payoutAmount,
    soldAt: raw.soldAt,
    stockOnlyFallback: raw.stockOnlyFallback,
    trustFactor: row.trustFactor ?? raw.trustFactor,
    orderCutoffTime: row.orderCutoffTime || raw.orderCutoffTime,
    reseller: row.reseller,
    supplierScore: row.supplierScore === null || row.supplierScore === undefined ? raw.supplierScore : Number(row.supplierScore),
    isExpress: raw.isExpress,
    ozonProductId: raw.ozonProductId,
    campaignId: raw.campaignId,
    requestDocId: row.requestDocId || raw.requestDocId,
    requestRowId: row.requestRowId || raw.requestRowId,
    status: row.status || raw.status,
    createdAt: row.createdAt?.toISOString?.() || raw.createdAt,
    createdBy: row.createdBy || raw.createdBy,
    pickedBy: row.pickedBy || raw.pickedBy,
    pickedAt: row.pickedAt?.toISOString?.() || raw.pickedAt,
    missingBy: row.missingBy || raw.missingBy,
    missingAt: row.missingAt?.toISOString?.() || raw.missingAt,
    missingReason: row.missingReason || raw.missingReason,
    nextRetryAt: row.nextRetryAt?.toISOString?.() || raw.nextRetryAt,
    replacementFor: row.replacementFor || raw.replacementFor,
    replacementKey: row.replacementKey || raw.replacementKey,
  });
}

async function readSupplierPickingState() {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().supplierPickingRow.findMany({
        orderBy: [{ createdAt: "desc" }],
        take: 10000,
      });
      const normalizedRows = {};
      for (const row of rows.map(supplierPickingRowFromPostgres)) {
        if (row.key) normalizedRows[row.key] = row;
      }
      return normalizeSupplierPickingState({
        updatedAt: rows[0]?.updatedAt?.toISOString?.() || null,
        rows: normalizedRows,
      });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read supplier picking state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    return normalizeSupplierPickingState(JSON.parse(await fs.readFile(supplierPickingListPath, "utf8")));
  } catch (error) {
    if (error.code === "ENOENT") return normalizeSupplierPickingState();
    throw error;
  }
}

async function writeSupplierPickingState(state = {}) {
  const normalized = normalizeSupplierPickingState({
    ...state,
    updatedAt: new Date().toISOString(),
  });
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const rows = Object.values(normalized.rows || {}).map(supplierPickingRowToPostgres).filter((row) => row.pickingKey);
      for (const batch of chunkArray(rows, 250)) {
        await prisma.$transaction(batch.map((row) => prisma.supplierPickingRow.upsert({
          where: { pickingKey: row.pickingKey },
          create: row,
          update: {
            marketplace: row.marketplace,
            accountName: row.accountName,
            orderId: row.orderId,
            postingNumber: row.postingNumber,
            offerId: row.offerId,
            productName: row.productName,
            quantity: row.quantity,
            supplierName: row.supplierName,
            partnerId: row.partnerId,
            offerRowId: row.offerRowId,
            price: row.price,
            priceCurrency: row.priceCurrency,
            trustFactor: row.trustFactor,
            orderCutoffTime: row.orderCutoffTime,
            reseller: row.reseller,
            supplierScore: row.supplierScore,
            requestDocId: row.requestDocId,
            requestRowId: row.requestRowId,
            status: row.status,
            createdBy: row.createdBy,
            pickedBy: row.pickedBy,
            pickedAt: row.pickedAt,
            missingBy: row.missingBy,
            missingAt: row.missingAt,
            missingReason: row.missingReason,
            nextRetryAt: row.nextRetryAt,
            replacementFor: row.replacementFor,
            replacementKey: row.replacementKey,
            raw: row.raw,
          },
        })));
      }
      if (!jsonFallbackEnabled()) return normalized;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write supplier picking state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${supplierPickingListPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(normalized, null, 2), "utf8");
  await fs.rename(temporaryPath, supplierPickingListPath);
  return normalized;
}

async function createSupplierPickingRows(inserted = [], request = null, options = {}) {
  const rows = inserted.map(normalizeSupplierCartPreviewRow).filter((row) => row.key);
  if (!rows.length) return [];
  const state = await readSupplierPickingState();
  const created = [];
  const initialStatus = options.initialStatus || "open";
  for (const row of rows) {
    const totalQty = Math.max(1, Math.round(Number(row.quantity || 1) || 1));
    const units = totalQty > 1 ? totalQty : 1;
    for (let unitIdx = 0; unitIdx < units; unitIdx++) {
      const unitKey = units > 1 ? `${row.key}:u${unitIdx}` : row.key;
      const existing = state.rows[unitKey] ? normalizeSupplierPickingRow(state.rows[unitKey]) : null;
      if (existing && existing.status !== "missing") {
        // When a re-commit resolves to a different supplier for the same open picking row,
        // update the supplier info so the picking list reflects the latest PM commit.
        if (
          existing.status === "open"
          && row.ready
          && row.partnerId
          && cleanText(row.partnerId).toLowerCase() !== cleanText(existing.partnerId).toLowerCase()
        ) {
          const updatedRow = normalizeSupplierPickingRow({
            ...existing,
            supplierName: row.supplierName,
            partnerId: row.partnerId,
            offerRowId: row.offerRowId,
            price: row.price,
            priceCurrency: row.priceCurrency,
            trustFactor: row.trustFactor,
            orderCutoffTime: row.orderCutoffTime,
            reseller: row.reseller,
            supplierScore: row.supplierScore,
            requestDocId: row.requestDocId || existing.requestDocId,
            requestRowId: row.requestRowId || existing.requestRowId,
          });
          state.rows[unitKey] = updatedRow;
          created.push(updatedRow);
        }
        continue;
      }
      const pickingKey = existing?.status === "missing"
        ? `${unitKey}|retry:${row.requestRowId || Date.now()}`
        : unitKey;
      if (state.rows[pickingKey]) continue;
      const now = new Date().toISOString();
      const pickingRow = normalizeSupplierPickingRow({
        ...row,
        key: pickingKey,
        quantity: 1,
        saleAmount: undefined,
        payoutAmount: undefined,
        status: initialStatus,
        createdAt: row.committedAt || now,
        createdBy: requestUsername(request),
        replacementFor: existing?.status === "missing" ? existing.key : "",
        ...(initialStatus === "picked" ? { pickedAt: now, pickedBy: requestUsername(request) || "manual" } : {}),
      });
      state.rows[pickingRow.key] = pickingRow;
      if (existing?.status === "missing") {
        state.rows[existing.key] = normalizeSupplierPickingRow({
          ...existing,
          status: "reordered",
          replacementKey: pickingRow.key,
        });
      }
      created.push(pickingRow);
    }
  }
  if (created.length) await writeSupplierPickingState(state);
  for (const row of created) {
    await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_picking.created", {
      entityType: "supplier_picking",
      entityId: row.key,
      newValue: row,
    }).catch((error) => logger.warn("supplier picking audit failed", { detail: error?.message || String(error) }));
  }
  return created;
}
