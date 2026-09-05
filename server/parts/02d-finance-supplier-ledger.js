function supplierLedgerIdentityWhere({ supplierName = "", partnerId = "" } = {}) {
  const name = cleanText(supplierName);
  const partner = cleanText(partnerId);
  const OR = [];
  if (partner) OR.push({ partnerId: partner });
  if (name) OR.push({ supplierName: name });
  return OR.length ? { OR } : {};
}

// Detect entries where a RUB supplier's picking row was stored with priceCurrency="USD"
// (the old default). Returns the corrected amount so UI shows the right balance without
// requiring a DB migration to be run manually.
function correctEntryAmountForRubSupplier(entry) {
  if (entry.entryType !== "purchase_debt" || !(entry.amount < 0)) return entry.amount;
  const picking = entry.raw?.picking || {};
  const stored = String(picking.priceCurrency || picking.currency || "").toUpperCase();
  if (stored === "RUB" || stored === "RUR") return entry.amount;
  // stored as "USD" (or missing) — check if supplier is actually RUB-priced
  const supplierName = cleanText(entry.supplierName || picking.supplierName || "");
  const partnerId = cleanText(entry.partnerId || picking.partnerId || "");
  const resolvedCurrency = resolvePickingRowCurrency({ ...picking, supplierName, partnerId });
  if (resolvedCurrency !== "RUB") return entry.amount;
  const price = Number(picking.price || 0);
  const qty = Math.max(1, Math.round(Number(picking.quantity || 1)));
  return price > 0 ? -normalizeFinanceMoney(price * qty, 0) : entry.amount;
}

function supplierLedgerSummaryFromEntries(entries = []) {
  const active = entries.filter((entry) => entry.status !== "voided");
  // Use corrected amounts for RUB suppliers whose entries were stored with wrong USD default.
  const balance = active.reduce((sum, entry) => sum + normalizeFinanceMoney(correctEntryAmountForRubSupplier(entry), 0), 0);
  const debtTotal = active
    .filter((entry) => entry.amount < 0)
    .reduce((sum, entry) => sum + Math.abs(normalizeFinanceMoney(correctEntryAmountForRubSupplier(entry), 0)), 0);
  const paidTotal = active
    .filter((entry) => entry.amount > 0 && entry.entryType === "payment")
    .reduce((sum, entry) => sum + normalizeFinanceMoney(entry.amount, 0), 0);
  // creditTotal includes payments, balance corrections, and supplier returns — used for balanceUsd display
  const creditTotal = active
    .filter((entry) => entry.amount > 0)
    .reduce((sum, entry) => sum + normalizeFinanceMoney(entry.amount, 0), 0);
  const lastPayment = active
    .filter((entry) => entry.entryType === "payment")
    .sort((left, right) => String(right.occurredAt).localeCompare(String(left.occurredAt)))[0] || null;
  const lastDebt = active
    .filter((entry) => entry.amount < 0)
    .sort((left, right) => String(right.occurredAt).localeCompare(String(left.occurredAt)))[0] || null;
  // For USD-priced suppliers: compute native-currency debt total from raw picking data.
  // This lets the UI show debt in USD without RUB conversion.
  const debtTotalUsd = active
    .filter((entry) => entry.amount < 0 && entry.entryType === "purchase_debt")
    .reduce((sum, entry) => {
      const picking = entry.raw?.picking || {};
      const currency = String(picking.priceCurrency || picking.currency || "").toUpperCase();
      if (currency === "RUB" || currency === "RUR") return sum;
      const price = Number(picking.price || 0);
      const qty = Math.max(1, Math.round(Number(picking.quantity || 1)));
      return price > 0 ? sum + normalizeFinanceMoney(price * qty, 0) : sum;
    }, 0);
  return {
    balance: Math.round(balance),
    debtTotal: Math.round(debtTotal),
    paidTotal: Math.round(paidTotal),
    creditTotal: Math.round(creditTotal),
    debtTotalUsd: normalizeFinanceMoney(debtTotalUsd, 0),
    entries: active.length,
    lastPaymentAt: lastPayment?.occurredAt || null,
    lastDebtAt: lastDebt?.occurredAt || null,
  };
}

async function listSupplierLedgerEntries({ supplierName = "", partnerId = "", status = "active", limit = 200, period = "all" } = {}) {
  const normalizedLimit = Math.max(1, Math.min(2000, Number(limit || 200) || 200));
  if (!shouldUsePostgresStorage()) return { source: "disabled", total: 0, entries: [], summary: supplierLedgerSummaryFromEntries([]) };
  const statusText = cleanText(status).toLowerCase();
  const andFilters = [
    supplierLedgerIdentityWhere({ supplierName, partnerId }),
    statusText && statusText !== "all" ? { status: statusText === "voided" ? "voided" : "active" } : {},
    financePeriodWhere(period, "occurredAt"),
  ].filter((item) => Object.keys(item || {}).length);
  const where = andFilters.length ? { AND: andFilters } : {};
  try {
    const [total, rows] = await Promise.all([
      getPrisma().supplierLedgerEntry.count({ where }),
      getPrisma().supplierLedgerEntry.findMany({ where, orderBy: { occurredAt: "desc" }, take: normalizedLimit }),
    ]);
    const entries = rows.map(supplierLedgerEntryFromPostgres);
    const summaryRows = total > rows.length
      ? (await getPrisma().supplierLedgerEntry.findMany({ where, orderBy: { occurredAt: "desc" }, take: 10000 })).map(supplierLedgerEntryFromPostgres)
      : entries;
    return { source: "postgres", total, entries, summary: supplierLedgerSummaryFromEntries(summaryRows) };
  } catch (error) {
    logger.warn("supplier ledger postgres read failed", { detail: error?.message || String(error) });
    if (!jsonFallbackEnabled()) throw error;
    return { source: "postgres", total: 0, entries: [], summary: supplierLedgerSummaryFromEntries([]), error: error?.message || String(error) };
  }
}

async function supplierLedgerSummaryMapForSuppliers(suppliers = []) {
  const empty = new Map();
  if (!shouldUsePostgresStorage() || !Array.isArray(suppliers) || !suppliers.length) return empty;
  try {
    const rows = await getPrisma().supplierLedgerEntry.findMany({
      where: { status: "active" },
      orderBy: { occurredAt: "desc" },
      take: 20000,
    });
    const byKey = new Map();
    for (const entry of rows.map(supplierLedgerEntryFromPostgres)) {
      const keys = [
        entry.partnerId ? `partner:${cleanText(entry.partnerId).toLowerCase()}` : "",
        entry.supplierName ? `name:${normalizeSupplierName(entry.supplierName)}` : "",
      ].filter(Boolean);
      for (const key of keys) {
        if (!byKey.has(key)) byKey.set(key, []);
        byKey.get(key).push(entry);
      }
    }
    for (const supplier of suppliers) {
      const partnerKey = supplier.partnerId ? `partner:${cleanText(supplier.partnerId).toLowerCase()}` : "";
      const nameKey = supplier.name ? `name:${normalizeSupplierName(supplier.name)}` : "";
      // Merge both buckets so payments recorded without partnerId are included in the balance
      const seenIds = new Set();
      const entries = [];
      for (const entry of [...(partnerKey && byKey.get(partnerKey) || []), ...(nameKey && byKey.get(nameKey) || [])]) {
        if (!seenIds.has(entry.id)) { seenIds.add(entry.id); entries.push(entry); }
      }
      empty.set(cleanText(supplier.id || supplier.partnerId || supplier.name), supplierLedgerSummaryFromEntries(entries));
    }
  } catch (error) {
    logger.warn("supplier ledger summary map failed", { detail: error?.message || String(error) });
  }
  return empty;
}

async function upsertSupplierLedgerDebtFromPickingRow(row = {}, financeOrder = null, request = null) {
  if (!shouldUsePostgresStorage()) return null;
  const normalized = normalizeSupplierPickingRow(row);
  const purchaseCost = normalizeFinanceMoney(financeOrder?.purchaseCost ?? await financePurchaseCostRubFromPicking(normalized), 0);
  if (!(purchaseCost > 0) || !normalized.supplierName) return null;
  const entry = normalizeSupplierLedgerEntry({
    sourceKey: supplierLedgerSourceKeyForPicking(normalized),
    entryType: "purchase_debt",
    supplierName: normalized.supplierName,
    partnerId: normalized.partnerId,
    amount: -Math.abs(purchaseCost),
    currency: "RUB",
    pickingKey: normalized.key,
    financeOrderId: financeOrder?.id || financeOrderIdForPicking(normalized),
    orderId: normalized.orderId || normalized.postingNumber || normalized.key,
    postingNumber: normalized.postingNumber,
    offerId: normalized.offerId,
    productName: normalized.productName,
    quantity: normalized.quantity,
    note: "Долг создан при отметке Собрал",
    status: "active",
    occurredAt: normalized.pickedAt || new Date().toISOString(),
    createdBy: requestUsername(request || {}),
    raw: { picking: normalized, financeOrder },
  });
  try {
    const saved = await getPrisma().supplierLedgerEntry.upsert({
      where: { sourceKey: entry.sourceKey },
      create: {
        id: entry.id,
        sourceKey: entry.sourceKey,
        entryType: entry.entryType,
        supplierName: entry.supplierName || null,
        partnerId: entry.partnerId || null,
        amount: entry.amount,
        currency: entry.currency,
        pickingKey: entry.pickingKey || null,
        financeOrderId: entry.financeOrderId || null,
        orderId: entry.orderId || null,
        postingNumber: entry.postingNumber || null,
        offerId: entry.offerId || null,
        productName: entry.productName || null,
        quantity: entry.quantity,
        note: entry.note || null,
        status: "active",
        occurredAt: toDateOrNull(entry.occurredAt) || new Date(),
        voidedAt: null,
        createdBy: entry.createdBy || null,
        raw: entry.raw,
      },
      update: {
        supplierName: entry.supplierName || null,
        partnerId: entry.partnerId || null,
        amount: entry.amount,
        currency: entry.currency,
        pickingKey: entry.pickingKey || null,
        financeOrderId: entry.financeOrderId || null,
        orderId: entry.orderId || null,
        postingNumber: entry.postingNumber || null,
        offerId: entry.offerId || null,
        productName: entry.productName || null,
        quantity: entry.quantity,
        note: entry.note || null,
        status: "active",
        occurredAt: toDateOrNull(entry.occurredAt) || new Date(),
        voidedAt: null,
        raw: entry.raw,
      },
    });
    suppliersListCache = null;
    await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_ledger.debt_upsert", {
      entityType: "supplier_ledger",
      entityId: saved.id,
      newValue: supplierLedgerEntryFromPostgres(saved),
    }).catch((error) => logger.warn("supplier ledger audit failed", { detail: error?.message || String(error) }));
    return supplierLedgerEntryFromPostgres(saved);
  } catch (error) {
    logger.warn("supplier ledger debt upsert failed", { detail: error?.message || String(error) });
    if (!jsonFallbackEnabled()) throw error;
    return null;
  }
}

async function voidSupplierLedgerDebtForPickingRow(row = {}, request = null) {
  if (!shouldUsePostgresStorage()) return null;
  const sourceKey = supplierLedgerSourceKeyForPicking(row);
  try {
    const saved = await getPrisma().supplierLedgerEntry.update({
      where: { sourceKey },
      data: {
        status: "voided",
        voidedAt: new Date(),
        raw: {
          picking: normalizeSupplierPickingRow(row),
          voidedBy: requestUsername(request || {}),
          voidedAt: new Date().toISOString(),
        },
      },
    });
    suppliersListCache = null;
    await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_ledger.debt_void", {
      entityType: "supplier_ledger",
      entityId: saved.id,
      newValue: supplierLedgerEntryFromPostgres(saved),
    }).catch((error) => logger.warn("supplier ledger void audit failed", { detail: error?.message || String(error) }));
    return supplierLedgerEntryFromPostgres(saved);
  } catch (error) {
    if (error?.code === "P2025") return null;
    logger.warn("supplier ledger debt void failed", { detail: error?.message || String(error) });
    if (!jsonFallbackEnabled()) throw error;
    return null;
  }
}

// Startup migration: correct purchase_debt entries for RUB suppliers that were stored
// with priceCurrency="USD" (the old default), causing 90x inflated RUB amounts.
async function fixRubSupplierLedgerAmounts(prisma) {
  if (!prisma || !shouldUsePostgresStorage()) return { skipped: true };
  const rows = await prisma.supplierLedgerEntry.findMany({
    where: { status: "active", entryType: "purchase_debt", amount: { lt: 0 } },
  });
  let fixed = 0;
  for (const row of rows) {
    const raw = row.raw || {};
    const picking = raw.picking || {};
    const stored = String(picking.priceCurrency || picking.currency || "").toUpperCase();
    if (stored === "RUB" || stored === "RUR") continue;
    const supplierName = cleanText(row.supplierName || picking.supplierName || "");
    const partnerId = cleanText(row.partnerId || picking.partnerId || "");
    const resolvedCurrency = resolvePickingRowCurrency({ ...picking, supplierName, partnerId });
    if (resolvedCurrency !== "RUB") continue;
    const price = Number(picking.price || 0);
    const qty = Math.max(1, Math.round(Number(picking.quantity || 1)));
    if (!(price > 0)) continue;
    const correctAmount = -normalizeFinanceMoney(price * qty, 0);
    if (Math.abs(correctAmount - Number(row.amount)) < 0.01) continue;
    await prisma.supplierLedgerEntry.update({
      where: { id: row.id },
      data: {
        amount: correctAmount,
        raw: { ...raw, picking: { ...picking, priceCurrency: "RUB", _fixedAt: new Date().toISOString() } },
      },
    });
    fixed++;
  }
  if (fixed > 0) {
    suppliersListCache = null;
    logger.info("rub_supplier_ledger_fix complete", { fixed, total: rows.length });
  }
  return { fixed, total: rows.length };
}
