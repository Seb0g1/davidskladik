function normalizeFinanceMoney(value, fallback = 0) {
  const n = Number(value ?? fallback);
  return Number.isFinite(n) ? Number(n.toFixed(2)) : Number(fallback || 0);
}

function financeOrderProfit(row = {}) {
  const payout = normalizeFinanceMoney(row.payoutAmount ?? row.payout_amount ?? row.saleAmount ?? row.sale_amount, 0);
  const purchase = normalizeFinanceMoney(row.purchaseCost ?? row.purchase_cost, 0);
  const fees = normalizeFinanceMoney(row.feesAmount ?? row.fees_amount, 0);
  const tax = normalizeFinanceMoney(row.taxAmount ?? row.tax_amount, 0);
  const penalties = normalizeFinanceMoney(row.penaltiesAmount ?? row.penalties_amount, 0);
  const refunds = normalizeFinanceMoney(row.refundsAmount ?? row.refunds_amount, 0);
  return normalizeFinanceMoney(payout - purchase - fees - tax - penalties - refunds, 0);
}

function financeOrderIdForPicking(row = {}) {
  const key = cleanText(row.key || supplierCartItemKey(row));
  if (!key) return "";
  return `picking:${crypto.createHash("sha1").update(key).digest("hex")}`;
}

function resolvePickingRowCurrency(row = {}) {
  const explicit = cleanText(row.priceCurrency || row.currency || "").toUpperCase();
  if (explicit === "RUB" || explicit === "RUR") return "RUB";
  if (explicit === "USD") {
    // Inna's PM prices are in RUB; old rows were stored with the default priceCurrency="USD"
    const supplierName = normalizeSupplierName(row.supplierName || row.partnerName || "");
    if (isInnaSupplierName(supplierName)) return "RUB";
  }
  return explicit || "USD";
}

async function financePurchaseCostRubFromPicking(row = {}) {
  // Picker may enter actual paid amount in RUB (overrides PM price calculation)
  const pricePaidRub = normalizeFinanceMoney(row.pricePaidRub, 0);
  if (pricePaidRub > 0) return pricePaidRub;
  const quantity = Math.max(1, Math.round(Number(row.quantity || 1) || 1));
  const price = normalizeFinanceMoney(row.price, 0);
  if (!(price > 0)) return null;
  // resolvePickingRowCurrency cross-checks with managed supplier maps so that
  // RUB suppliers (e.g. Inna) aren't inflated by the USD→RUB rate even when
  // old picking rows were stored with the default priceCurrency="USD".
  const currency = resolvePickingRowCurrency(row);
  if (currency === "RUB" || currency === "RUR") return normalizeFinanceMoney(price * quantity, 0);
  const ratePayload = await getUsdRate().catch(() => ({ rate: Number(process.env.DEFAULT_USD_RATE || 95) || 95 }));
  const usdRate = Number(ratePayload?.rate || ratePayload || process.env.DEFAULT_USD_RATE || 95) || 95;
  return normalizeFinanceMoney(price * usdRate * quantity, 0);
}

function normalizeFinanceExpense(input = {}) {
  const spentAt = toDateOrNull(input.spentAt || input.spent_at) || new Date();
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    type: cleanText(input.type || "manual_purchase") || "manual_purchase",
    supplierName: cleanText(input.supplierName || input.supplier_name),
    partnerId: cleanText(input.partnerId || input.partner_id),
    offerId: cleanText(input.offerId || input.offer_id),
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity: Math.max(1, Math.round(Number(input.quantity || 1) || 1)),
    amount: normalizeFinanceMoney(input.amount, 0),
    currency: cleanText(input.currency || "RUB").toUpperCase() || "RUB",
    note: cleanText(input.note),
    source: cleanText(input.source || "manual") || "manual",
    status: cleanText(input.status || "confirmed") || "confirmed",
    spentAt: spentAt.toISOString(),
    raw: input.raw && typeof input.raw === "object" ? input.raw : input,
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
  };
}

function normalizeFinanceOrder(input = {}) {
  const marketplaceText = cleanText(input.marketplace).toLowerCase();
  const marketplace = marketplaceText === "ozon" || marketplaceText === "yandex" || marketplaceText === "wb" || marketplaceText === "avito" ? marketplaceText : "";
  const saleAmount = input.saleAmount ?? input.sale_amount;
  const payoutAmount = input.payoutAmount ?? input.payout_amount;
  const purchaseCost = input.purchaseCost ?? input.purchase_cost;
  const feesAmount = input.feesAmount ?? input.fees_amount;
  const taxAmount = input.taxAmount ?? input.tax_amount;
  const penaltiesAmount = input.penaltiesAmount ?? input.penalties_amount;
  const refundsAmount = input.refundsAmount ?? input.refunds_amount;
  const row = {
    id: cleanText(input.id) || crypto.randomUUID(),
    marketplace,
    target: cleanText(input.target),
    orderId: cleanText(input.orderId || input.order_id) || cleanText(input.postingNumber || input.posting_number) || `manual-${crypto.randomUUID()}`,
    postingNumber: cleanText(input.postingNumber || input.posting_number),
    offerId: cleanText(input.offerId || input.offer_id),
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity: Math.max(1, Math.round(Number(input.quantity || 1) || 1)),
    saleAmount: saleAmount === undefined || saleAmount === null || saleAmount === "" ? null : normalizeFinanceMoney(saleAmount, 0),
    payoutAmount: payoutAmount === undefined || payoutAmount === null || payoutAmount === "" ? null : normalizeFinanceMoney(payoutAmount, 0),
    purchaseCost: purchaseCost === undefined || purchaseCost === null || purchaseCost === "" ? null : normalizeFinanceMoney(purchaseCost, 0),
    feesAmount: feesAmount === undefined || feesAmount === null || feesAmount === "" ? null : normalizeFinanceMoney(feesAmount, 0),
    taxAmount: taxAmount === undefined || taxAmount === null || taxAmount === "" ? null : normalizeFinanceMoney(taxAmount, 0),
    penaltiesAmount: penaltiesAmount === undefined || penaltiesAmount === null || penaltiesAmount === "" ? null : normalizeFinanceMoney(penaltiesAmount, 0),
    refundsAmount: refundsAmount === undefined || refundsAmount === null || refundsAmount === "" ? null : normalizeFinanceMoney(refundsAmount, 0),
    supplierName: cleanText(input.supplierName || input.supplier_name),
    partnerId: cleanText(input.partnerId || input.partner_id),
    source: cleanText(input.source || "manual") || "manual",
    status: cleanText(input.status || "open") || "open",
    soldAt: toDateOrNull(input.soldAt || input.sold_at)?.toISOString?.() || null,
    receivedAt: toDateOrNull(input.receivedAt || input.received_at)?.toISOString?.() || null,
    raw: input.raw && typeof input.raw === "object" ? input.raw : input,
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
  };
  row.profitAmount = normalizeFinanceMoney(input.profitAmount ?? input.profit_amount ?? financeOrderProfit(row), 0);
  return row;
}

function financeOrderFromPostgres(row = {}) {
  return normalizeFinanceOrder({
    id: row.id,
    marketplace: row.marketplace || "",
    target: row.target || "",
    orderId: row.orderId,
    postingNumber: row.postingNumber,
    offerId: row.offerId,
    productName: row.productName,
    quantity: row.quantity,
    saleAmount: row.saleAmount === null || row.saleAmount === undefined ? null : Number(row.saleAmount),
    payoutAmount: row.payoutAmount === null || row.payoutAmount === undefined ? null : Number(row.payoutAmount),
    purchaseCost: row.purchaseCost === null || row.purchaseCost === undefined ? null : Number(row.purchaseCost),
    feesAmount: row.feesAmount === null || row.feesAmount === undefined ? null : Number(row.feesAmount),
    taxAmount: row.taxAmount === null || row.taxAmount === undefined ? null : Number(row.taxAmount),
    penaltiesAmount: row.penaltiesAmount === null || row.penaltiesAmount === undefined ? null : Number(row.penaltiesAmount),
    refundsAmount: row.refundsAmount === null || row.refundsAmount === undefined ? null : Number(row.refundsAmount),
    profitAmount: row.profitAmount === null || row.profitAmount === undefined ? null : Number(row.profitAmount),
    supplierName: row.supplierName,
    partnerId: row.partnerId,
    source: row.source,
    status: row.status,
    soldAt: row.soldAt?.toISOString?.() || null,
    receivedAt: row.receivedAt?.toISOString?.() || null,
    raw: row.raw || {},
    createdAt: row.createdAt?.toISOString?.() || null,
    updatedAt: row.updatedAt?.toISOString?.() || null,
  });
}

function financeExpenseFromPostgres(row = {}) {
  return normalizeFinanceExpense({
    id: row.id,
    type: row.type,
    supplierName: row.supplierName,
    partnerId: row.partnerId,
    offerId: row.offerId,
    productName: row.productName,
    quantity: row.quantity,
    amount: Number(row.amount || 0),
    currency: row.currency,
    note: row.note,
    source: row.source,
    status: row.status,
    spentAt: row.spentAt?.toISOString?.() || null,
    raw: row.raw || {},
    createdAt: row.createdAt?.toISOString?.() || null,
    updatedAt: row.updatedAt?.toISOString?.() || null,
  });
}

function supplierLedgerSourceKeyForPicking(row = {}) {
  const key = cleanText(row.key || row.pickingKey || supplierCartItemKey(row));
  return `picking:${crypto.createHash("sha1").update(key || crypto.randomUUID()).digest("hex")}`;
}

function normalizeSupplierLedgerType(value = "") {
  const type = cleanText(value).toLowerCase();
  return ["purchase_debt", "payment", "adjustment", "supplier_return"].includes(type) ? type : "adjustment";
}

function normalizeSupplierLedgerEntry(input = {}) {
  const occurredAt = toDateOrNull(input.occurredAt || input.occurred_at || input.paidAt || input.paid_at) || new Date();
  const voidedAt = toDateOrNull(input.voidedAt || input.voided_at);
  const entryType = normalizeSupplierLedgerType(input.entryType || input.entry_type || input.type);
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    sourceKey: cleanText(input.sourceKey || input.source_key) || `${entryType}:${crypto.randomUUID()}`,
    entryType,
    supplierName: cleanText(input.supplierName || input.supplier_name),
    partnerId: cleanText(input.partnerId || input.partner_id),
    amount: normalizeFinanceMoney(input.amount, 0),
    currency: cleanText(input.currency || "RUB").toUpperCase() || "RUB",
    pickingKey: cleanText(input.pickingKey || input.picking_key),
    financeOrderId: cleanText(input.financeOrderId || input.finance_order_id),
    orderId: cleanText(input.orderId || input.order_id),
    postingNumber: cleanText(input.postingNumber || input.posting_number),
    offerId: cleanText(input.offerId || input.offer_id),
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity: Math.max(1, Math.round(Number(input.quantity || 1) || 1)),
    note: cleanText(input.note),
    status: cleanText(input.status || "active").toLowerCase() === "voided" ? "voided" : "active",
    occurredAt: occurredAt.toISOString(),
    voidedAt: voidedAt?.toISOString?.() || null,
    createdBy: cleanText(input.createdBy || input.created_by),
    raw: input.raw && typeof input.raw === "object" ? input.raw : input,
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
  };
}

function supplierLedgerEntryFromPostgres(row = {}) {
  return normalizeSupplierLedgerEntry({
    id: row.id,
    sourceKey: row.sourceKey,
    entryType: row.entryType,
    supplierName: row.supplierName,
    partnerId: row.partnerId,
    amount: Number(row.amount || 0),
    currency: row.currency,
    pickingKey: row.pickingKey,
    financeOrderId: row.financeOrderId,
    orderId: row.orderId,
    postingNumber: row.postingNumber,
    offerId: row.offerId,
    productName: row.productName,
    quantity: row.quantity,
    note: row.note,
    status: row.status,
    occurredAt: row.occurredAt?.toISOString?.() || null,
    voidedAt: row.voidedAt?.toISOString?.() || null,
    createdBy: row.createdBy,
    raw: row.raw || {},
    createdAt: row.createdAt?.toISOString?.() || null,
    updatedAt: row.updatedAt?.toISOString?.() || null,
  });
}

