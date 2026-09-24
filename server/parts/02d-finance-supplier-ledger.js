function supplierLedgerIdentityWhere({ supplierName = "", partnerId = "" } = {}) {
  const name = cleanText(supplierName);
  const partner = cleanText(partnerId);
  const OR = [];
  if (partner) OR.push({ partnerId: partner });
  if (name) OR.push({ supplierName: { equals: name, mode: "insensitive" } });
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

function supplierLedgerRequestCurrency(value) {
  return String(value || "RUB").trim().toUpperCase() === "USD" ? "USD" : "RUB";
}

function supplierLedgerFallbackUsdRate() {
  return Number(process.env.DEFAULT_USD_RATE || 95) || 95;
}

async function supplierLedgerCurrentUsdRate() {
  const payload = await getUsdRate().catch(() => null);
  return Number(payload?.rate || payload || supplierLedgerFallbackUsdRate()) || supplierLedgerFallbackUsdRate();
}

// Rate that was in force when the entry was written (stored in raw.usdRate since 2026-09-25);
// older entries fall back to the current rate.
function supplierLedgerEntryUsdRate(entry, currentRate) {
  const raw = entry?.raw || {};
  const stored = Number(raw.usdRate || 0);
  return stored > 0 ? stored : currentRate;
}

// "Свести баланс" entries created before 2026-09-25 carry raw.targetBalance in RUB and were
// computed against a balance that summed RUB and USD amounts together, so their stored delta
// is meaningless. Their intent was "the balance is targetBalance now" — replay them that way.
// Before 2026-08-28 the UI sent USD targets unconverted, so those stay plain deltas.
const LEGACY_SUPPLIER_CHECKPOINT_SINCE = "2026-08-28";
function isLegacySupplierBalanceCheckpoint(entry) {
  const raw = entry?.raw || {};
  return entry.entryType === "balance_correction"
    && raw.mode !== "delta"
    && String(entry.occurredAt || "") >= LEGACY_SUPPLIER_CHECKPOINT_SINCE
    && raw.targetBalance !== undefined && raw.targetBalance !== null && raw.targetBalance !== ""
    && Number.isFinite(Number(raw.targetBalance));
}

// Native value of a purchase debt in USD, when the picking row was priced in USD and the
// picker did not override it with the actual RUB amount paid.
function supplierLedgerDebtUsdFromPicking(entry) {
  if (entry.entryType !== "purchase_debt") return null;
  if (String(entry.currency || "RUB").toUpperCase() === "USD") return null;
  const picking = entry.raw?.picking || {};
  if (Number(picking.pricePaidRub || 0) > 0) return null;
  const currency = String(picking.priceCurrency || picking.currency || "").toUpperCase();
  if (currency === "RUB" || currency === "RUR") return null;
  if (correctEntryAmountForRubSupplier(entry) !== entry.amount) return null;
  const price = Number(picking.price || 0);
  const qty = Math.max(1, Math.round(Number(picking.quantity || 1)));
  return price > 0 ? -price * qty : null;
}

function supplierLedgerEntryKey(entry) {
  const partner = cleanText(entry.partnerId);
  if (partner) return `partner:${partner.toLowerCase()}`;
  return `name:${normalizeSupplierName(entry.supplierName)}`;
}

// Single source of truth for supplier balances. Every entry is valued both in RUB and in USD
// at the rate of its own moment, so the suppliers list, the drawer, the picking page and the
// finance page show the same numbers and USD balances do not drift when today's rate changes.
// perSupplier: replay legacy checkpoints per supplier (summaries spanning many suppliers).
function supplierLedgerSummaryFromEntries(entries = [], { usdRate, perSupplier = false } = {}) {
  const currentRate = Number(usdRate) > 0 ? Number(usdRate) : supplierLedgerFallbackUsdRate();
  const active = entries
    .filter((entry) => entry.status !== "voided")
    .slice()
    .sort((a, b) => String(a.occurredAt || "").localeCompare(String(b.occurredAt || ""))
      || String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
  // A full return of a picked row cancels exactly the debt it created, in both currencies.
  const debtByPickingKey = new Map();
  for (const entry of active) {
    if (entry.entryType !== "purchase_debt" || !entry.pickingKey) continue;
    const isUsd = String(entry.currency || "RUB").toUpperCase() === "USD";
    const usd = isUsd
      ? entry.amount
      : supplierLedgerDebtUsdFromPicking(entry) ?? correctEntryAmountForRubSupplier(entry) / supplierLedgerEntryUsdRate(entry, currentRate);
    debtByPickingKey.set(entry.pickingKey, { amount: Math.abs(entry.amount), currency: String(entry.currency || "RUB").toUpperCase(), usd });
  }
  const running = new Map();
  const totals = {
    debtRub: 0, debtUsd: 0,
    paidRub: 0, paidUsd: 0, paidOnlyUsd: 0, paidOnlyRub: 0,
    returnsRub: 0, returnsUsd: 0,
    correctionsRub: 0, correctionsUsd: 0,
    creditRub: 0, creditOnlyUsd: 0, creditOnlyRub: 0,
  };
  for (const entry of active) {
    const key = perSupplier ? supplierLedgerEntryKey(entry) : "all";
    const run = running.get(key) || { rub: 0, usd: 0 };
    const rate = supplierLedgerEntryUsdRate(entry, currentRate);
    const isUsd = String(entry.currency || "RUB").toUpperCase() === "USD";
    const amount = correctEntryAmountForRubSupplier(entry);
    let rub;
    let usd;
    if (isLegacySupplierBalanceCheckpoint(entry)) {
      const targetRub = Number(entry.raw.targetBalance);
      rub = targetRub - run.rub;
      usd = targetRub / rate - run.usd;
    } else if (isUsd) {
      usd = amount;
      rub = amount * rate;
    } else {
      rub = amount;
      const debtUsd = supplierLedgerDebtUsdFromPicking(entry);
      if (debtUsd !== null) usd = debtUsd;
      else {
        const debt = entry.entryType === "supplier_return" && entry.pickingKey ? debtByPickingKey.get(entry.pickingKey) : null;
        const fullReturn = debt && debt.currency === "RUB" && Math.abs(Math.abs(amount) - debt.amount) < 0.01;
        usd = fullReturn ? -debt.usd : amount / rate;
      }
    }
    run.rub += rub;
    run.usd += usd;
    running.set(key, run);
    if (entry.entryType === "purchase_debt") {
      totals.debtRub -= rub;
      totals.debtUsd -= usd;
    } else if (entry.entryType === "payment") {
      totals.paidRub += rub;
      totals.paidUsd += usd;
      if (entry.amount > 0) {
        if (isUsd) totals.paidOnlyUsd += entry.amount;
        else totals.paidOnlyRub += entry.amount;
      }
    } else if (entry.entryType === "supplier_return") {
      totals.returnsRub += rub;
      totals.returnsUsd += usd;
    } else {
      totals.correctionsRub += rub;
      totals.correctionsUsd += usd;
    }
    if (entry.amount > 0) {
      totals.creditRub += rub;
      if (isUsd) totals.creditOnlyUsd += entry.amount;
      else totals.creditOnlyRub += entry.amount;
    }
  }
  let balanceRub = 0;
  let balanceUsd = 0;
  for (const run of running.values()) {
    balanceRub += run.rub;
    balanceUsd += run.usd;
  }
  const lastOf = (list) => (list.length ? list[list.length - 1] : null);
  const lastPayment = lastOf(active.filter((entry) => entry.entryType === "payment"));
  const lastDebt = lastOf(active.filter((entry) => entry.amount < 0));
  const money = (value) => normalizeFinanceMoney(Math.abs(value) < 0.005 ? 0 : value, 0);
  return {
    // balance === balanceRub (RUB equivalent); balanceUsd is the USD equivalent. The UI shows
    // the one matching the supplier currency and never recomputes it client-side.
    balance: money(balanceRub),
    balanceRub: money(balanceRub),
    balanceUsd: money(balanceUsd),
    usdRate: currentRate,
    debtTotal: Math.round(totals.debtRub),
    debtTotalRub: money(totals.debtRub),
    debtTotalUsd: money(totals.debtUsd),
    paidTotal: Math.round(totals.paidRub),
    paidTotalRubEquiv: money(totals.paidRub),
    paidTotalUsdEquiv: money(totals.paidUsd),
    // Raw sums of payments by the currency they were entered in (kept for compatibility).
    paidTotalUsd: money(totals.paidOnlyUsd),
    paidTotalRubOnly: Math.round(totals.paidOnlyRub),
    returnsTotalRub: money(totals.returnsRub),
    returnsTotalUsd: money(totals.returnsUsd),
    correctionsTotalRub: money(totals.correctionsRub),
    correctionsTotalUsd: money(totals.correctionsUsd),
    creditTotal: Math.round(totals.creditRub),
    creditTotalUsd: money(totals.creditOnlyUsd),
    creditTotalRub: Math.round(totals.creditOnlyRub),
    debtStoredInRub: active.some((e) => e.entryType === "purchase_debt" && String(e.currency || "RUB").toUpperCase() !== "USD"),
    entries: active.length,
    lastPaymentAt: lastPayment?.occurredAt || null,
    lastDebtAt: lastDebt?.occurredAt || null,
  };
}

async function listSupplierLedgerEntries({ supplierName = "", partnerId = "", status = "active", limit = 200, period = "all" } = {}) {
  const normalizedLimit = Math.max(1, Math.min(2000, Number(limit || 200) || 200));
  if (!shouldUsePostgresStorage()) return { source: "disabled", total: 0, entries: [], summary: supplierLedgerSummaryFromEntries([]) };
  const usdRate = await supplierLedgerCurrentUsdRate();
  // Without a supplier filter the summary spans every supplier: replay checkpoints per supplier.
  const perSupplier = !cleanText(supplierName) && !cleanText(partnerId);
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
    return { source: "postgres", total, entries, summary: supplierLedgerSummaryFromEntries(summaryRows, { usdRate, perSupplier }) };
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
    const usdRate = await supplierLedgerCurrentUsdRate();
    const LEDGER_SUMMARY_LIMIT = 50000;
    const [totalCount, rows] = await Promise.all([
      getPrisma().supplierLedgerEntry.count({ where: { status: "active" } }),
      getPrisma().supplierLedgerEntry.findMany({
        where: { status: "active" },
        orderBy: { occurredAt: "desc" },
        take: LEDGER_SUMMARY_LIMIT,
      }),
    ]);
    if (totalCount > LEDGER_SUMMARY_LIMIT) {
      logger.warn("supplier ledger summary truncated — balance may be understated", {
        total: totalCount,
        fetched: rows.length,
        limit: LEDGER_SUMMARY_LIMIT,
      });
    }
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
      empty.set(cleanText(supplier.id || supplier.partnerId || supplier.name), supplierLedgerSummaryFromEntries(entries, { usdRate }));
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
  const usdRate = await supplierLedgerCurrentUsdRate();
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
    raw: { picking: normalized, financeOrder, usdRate },
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
