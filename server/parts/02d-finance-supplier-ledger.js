function supplierLedgerIdentityWhere({ supplierName = "", partnerId = "" } = {}) {
  const name = cleanText(supplierName);
  const partner = cleanText(partnerId);
  const OR = [];
  if (partner) OR.push({ partnerId: partner });
  if (name) OR.push({ supplierName: { equals: name, mode: "insensitive" } });
  return OR.length ? { OR } : {};
}

function supplierLedgerRequestCurrency(value) {
  return String(value || "RUB").trim().toUpperCase() === "USD" ? "USD" : "RUB";
}

function supplierLedgerFallbackUsdRate() {
  return Number(process.env.DEFAULT_USD_RATE || 95) || 95;
}

// Only used to fill the RUB column of finance rows for new debts; balances never use a rate.
async function supplierLedgerCurrentUsdRate() {
  const payload = await getUsdRate().catch(() => null);
  return Number(payload?.rate || payload || supplierLedgerFallbackUsdRate()) || supplierLedgerFallbackUsdRate();
}

// Supplier currency comes from the managed supplier card (Инна and a few cosmetics shops are
// RUB, everyone else USD). Cached briefly: the list is small and rarely changes.
let supplierLedgerCurrencyIndexCache = null;
async function supplierLedgerCurrencyIndex() {
  if (supplierLedgerCurrencyIndexCache && Date.now() - supplierLedgerCurrencyIndexCache.at < 60_000) return supplierLedgerCurrencyIndexCache.index;
  const index = { byPartner: new Map(), byName: new Map() };
  try {
    const rows = await getPrisma().managedSupplier.findMany({ select: { name: true, partnerId: true, defaultCurrency: true } });
    for (const row of rows) {
      const currency = String(row.defaultCurrency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
      if (cleanText(row.partnerId)) index.byPartner.set(cleanText(row.partnerId).toLowerCase(), currency);
      if (cleanText(row.name)) index.byName.set(normalizeSupplierName(row.name), currency);
    }
  } catch (error) {
    logger.warn("supplier currency index failed", { detail: error?.message || String(error) });
  }
  supplierLedgerCurrencyIndexCache = { at: Date.now(), index };
  return index;
}

function supplierLedgerCurrencyFromIndex(index, { supplierName = "", partnerId = "" } = {}) {
  const partner = cleanText(partnerId).toLowerCase();
  if (partner && index?.byPartner?.has(partner)) return index.byPartner.get(partner);
  const name = normalizeSupplierName(supplierName);
  if (name && index?.byName?.has(name)) return index.byName.get(name);
  if (isInnaSupplierName(name)) return "RUB";
  return "";
}

async function supplierLedgerCurrencyFor(identity = {}) {
  return supplierLedgerCurrencyFromIndex(await supplierLedgerCurrencyIndex(), identity) || "USD";
}

// Without a supplier card, the currency the picked rows were priced in decides.
function supplierLedgerInferCurrency(entries = []) {
  let rub = 0;
  let usd = 0;
  for (const entry of entries) {
    if (entry.entryType !== "purchase_debt") continue;
    const picking = entry.raw?.picking || {};
    const currency = resolvePickingRowCurrency({ ...picking, supplierName: entry.supplierName || picking.supplierName, partnerId: entry.partnerId || picking.partnerId });
    if (currency === "RUB") rub += 1;
    else usd += 1;
  }
  return rub > usd ? "RUB" : "USD";
}

// Old "Свести баланс" entries (no raw.mode="delta", on or after 2026-08-28) meant "the balance is
// targetBalance now"; their stored delta was computed against broken balances. Replay them as such.
const LEGACY_SUPPLIER_CHECKPOINT_SINCE = "2026-08-28";
function isLegacySupplierBalanceCheckpoint(entry) {
  const raw = entry?.raw || {};
  return entry.entryType === "balance_correction"
    && raw.mode !== "delta"
    && String(entry.occurredAt || "") >= LEGACY_SUPPLIER_CHECKPOINT_SINCE
    && raw.targetBalance !== undefined && raw.targetBalance !== null && raw.targetBalance !== ""
    && Number.isFinite(Number(raw.targetBalance));
}

// A picked row owes its unit price × the quantity actually picked, in the supplier's currency.
// The unit price is what the picker entered at «Собрал» (pricePaid), else the PM price.
function supplierLedgerDebtNative(entry, currency) {
  const picking = entry.raw?.picking || {};
  const qty = Math.max(1, Math.round(Number(picking.pickedQuantity || picking.quantity || entry.quantity || 1)));
  const paid = Number(picking.pricePaid || 0);
  if (paid > 0) return -normalizeFinanceMoney(paid * qty, 0);
  // Before pricePaid existed a RUB supplier's actual price was saved as pricePaidRub.
  const paidRub = Number(picking.pricePaidRub || 0);
  if (currency === "RUB" && paidRub > 0) return -normalizeFinanceMoney(paidRub * qty, 0);
  const price = Number(picking.price || 0);
  if (price > 0) {
    const pickingCurrency = resolvePickingRowCurrency({
      ...picking,
      supplierName: entry.supplierName || picking.supplierName,
      partnerId: entry.partnerId || picking.partnerId,
    });
    if (pickingCurrency === currency) return -normalizeFinanceMoney(price * qty, 0);
  }
  return null;
}

// Value of one entry in the supplier's own currency. Entries written in the other currency are
// legacy leftovers (the stored rate of that moment is the only honest way to read them).
function supplierLedgerEntryNative(entry, currency, debtNativeByKey) {
  const entryCurrency = String(entry.currency || "RUB").toUpperCase() === "USD" ? "USD" : "RUB";
  if (entry.entryType === "purchase_debt") {
    const debt = supplierLedgerDebtNative(entry, currency);
    if (debt !== null) return { value: debt, foreign: false };
  }
  if (entry.entryType === "supplier_return" && entry.pickingKey && debtNativeByKey.has(entry.pickingKey)) {
    // A return of the whole picked row cancels exactly the debt that row created.
    const debt = debtNativeByKey.get(entry.pickingKey);
    if (debt.entryCurrency === entryCurrency && Math.abs(Math.abs(Number(entry.amount)) - debt.stored) < 0.01) {
      return { value: Math.abs(debt.native), foreign: false };
    }
  }
  const amount = Number(entry.amount || 0);
  if (entryCurrency === currency) return { value: amount, foreign: false };
  const rate = Number(entry.raw?.usdRate || entry.raw?._migratedRate || 0) || supplierLedgerFallbackUsdRate();
  return { value: currency === "USD" ? amount / rate : amount * rate, foreign: true };
}

function supplierLedgerEntryKey(entry) {
  const partner = cleanText(entry.partnerId);
  if (partner) return `partner:${partner.toLowerCase()}`;
  return `name:${normalizeSupplierName(entry.supplierName)}`;
}

// Single source of truth for one supplier's balance, in that supplier's own currency:
// picked rows add debt at their price, payments / returns / corrections reduce it by exactly
// the amount entered. No exchange rates. The suppliers list, the drawer and the picking page
// only display these numbers.
function supplierLedgerSummaryFromEntries(entries = [], { currency = "" } = {}) {
  const active = entries
    .filter((entry) => entry.status !== "voided")
    .slice()
    .sort((a, b) => String(a.occurredAt || "").localeCompare(String(b.occurredAt || ""))
      || String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
  const cur = currency === "RUB" || currency === "USD" ? currency : supplierLedgerInferCurrency(active);
  const debtNativeByKey = new Map();
  for (const entry of active) {
    if (entry.entryType !== "purchase_debt" || !entry.pickingKey) continue;
    const native = supplierLedgerEntryNative(entry, cur, debtNativeByKey).value;
    debtNativeByKey.set(entry.pickingKey, {
      native,
      stored: Math.abs(Number(entry.amount || 0)),
      entryCurrency: String(entry.currency || "RUB").toUpperCase() === "USD" ? "USD" : "RUB",
    });
  }
  let balance = 0;
  let debt = 0;
  let paid = 0;
  let returns = 0;
  let corrections = 0;
  let foreignEntries = 0;
  for (const entry of active) {
    let value;
    if (isLegacySupplierBalanceCheckpoint(entry)) {
      value = Number(entry.raw.targetBalance) - balance;
    } else {
      const native = supplierLedgerEntryNative(entry, cur, debtNativeByKey);
      value = native.value;
      if (native.foreign) foreignEntries += 1;
    }
    balance += value;
    if (entry.entryType === "purchase_debt") debt -= value;
    else if (entry.entryType === "payment") paid += value;
    else if (entry.entryType === "supplier_return") returns += value;
    else corrections += value;
  }
  const lastOf = (list) => (list.length ? list[list.length - 1] : null);
  const lastPayment = lastOf(active.filter((entry) => entry.entryType === "payment"));
  const lastDebt = lastOf(active.filter((entry) => Number(entry.amount) < 0));
  const money = (value) => normalizeFinanceMoney(Math.abs(value) < 0.005 ? 0 : value, 0);
  const isUsd = cur === "USD";
  // Only the supplier's own currency is filled; the other-currency fields stay 0 so nothing
  // can show a converted number by mistake.
  const inCur = (value, want) => (cur === want ? money(value) : 0);
  return {
    currency: cur,
    balance: money(balance),
    debtTotal: money(debt),
    paidTotal: money(paid),
    returnsTotal: money(returns),
    correctionsTotal: money(corrections),
    balanceUsd: inCur(balance, "USD"),
    balanceRub: inCur(balance, "RUB"),
    debtTotalUsd: inCur(debt, "USD"),
    debtTotalRub: inCur(debt, "RUB"),
    paidTotalUsdEquiv: inCur(paid, "USD"),
    paidTotalRubEquiv: inCur(paid, "RUB"),
    paidTotalUsd: inCur(paid, "USD"),
    paidTotalRubOnly: inCur(paid, "RUB"),
    returnsTotalUsd: inCur(returns, "USD"),
    returnsTotalRub: inCur(returns, "RUB"),
    correctionsTotalUsd: inCur(corrections, "USD"),
    correctionsTotalRub: inCur(corrections, "RUB"),
    debtStoredInRub: !isUsd,
    foreignEntries,
    entries: active.length,
    lastPaymentAt: lastPayment?.occurredAt || null,
    lastDebtAt: lastDebt?.occurredAt || null,
  };
}

// Summary over many suppliers (finance page): each supplier is summed in its own currency,
// USD and RUB totals are kept apart.
function supplierLedgerSummaryAcrossSuppliers(entries = [], currencyIndex = null) {
  const groups = new Map();
  for (const entry of entries) {
    const key = supplierLedgerEntryKey(entry);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  }
  const total = {
    currency: "mixed", balance: 0, balanceUsd: 0, balanceRub: 0, debtTotalUsd: 0, debtTotalRub: 0,
    paidTotalUsdEquiv: 0, paidTotalRubEquiv: 0, paidTotalUsd: 0, paidTotalRubOnly: 0,
    returnsTotalUsd: 0, returnsTotalRub: 0, correctionsTotalUsd: 0, correctionsTotalRub: 0,
    foreignEntries: 0, entries: 0, lastPaymentAt: null, lastDebtAt: null,
    // What we owe: only suppliers in debt count, an advance at one does not offset another.
    owedUsd: 0, owedRub: 0,
  };
  const sumKeys = Object.keys(total).filter((key) => typeof total[key] === "number" && !["balance", "owedUsd", "owedRub"].includes(key));
  for (const list of groups.values()) {
    const first = list[0] || {};
    const currency = supplierLedgerCurrencyFromIndex(currencyIndex, { supplierName: first.supplierName, partnerId: first.partnerId });
    const summary = supplierLedgerSummaryFromEntries(list, { currency });
    for (const key of sumKeys) total[key] = normalizeFinanceMoney(total[key] + Number(summary[key] || 0), 0);
    if (summary.balance < 0) {
      if (summary.currency === "RUB") total.owedRub = normalizeFinanceMoney(total.owedRub - summary.balance, 0);
      else total.owedUsd = normalizeFinanceMoney(total.owedUsd - summary.balance, 0);
    }
    if (summary.lastPaymentAt && String(summary.lastPaymentAt) > String(total.lastPaymentAt || "")) total.lastPaymentAt = summary.lastPaymentAt;
    if (summary.lastDebtAt && String(summary.lastDebtAt) > String(total.lastDebtAt || "")) total.lastDebtAt = summary.lastDebtAt;
  }
  total.balance = total.balanceRub;
  total.debtTotal = total.debtTotalRub;
  total.paidTotal = total.paidTotalRubEquiv;
  return total;
}

async function listSupplierLedgerEntries({ supplierName = "", partnerId = "", status = "active", limit = 200, period = "all" } = {}) {
  const normalizedLimit = Math.max(1, Math.min(2000, Number(limit || 200) || 200));
  if (!shouldUsePostgresStorage()) return { source: "disabled", total: 0, entries: [], summary: supplierLedgerSummaryFromEntries([]) };
  // Without a supplier filter the summary spans every supplier, each in its own currency.
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
    const currencyIndex = await supplierLedgerCurrencyIndex();
    const summary = perSupplier
      ? supplierLedgerSummaryAcrossSuppliers(summaryRows, currencyIndex)
      : supplierLedgerSummaryFromEntries(summaryRows, { currency: supplierLedgerCurrencyFromIndex(currencyIndex, { supplierName, partnerId }) });
    return { source: "postgres", total, entries, summary };
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
    const currencyIndex = await supplierLedgerCurrencyIndex();
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
      empty.set(cleanText(supplier.id || supplier.partnerId || supplier.name), supplierLedgerSummaryFromEntries(entries, {
        currency: supplierLedgerCurrencyFromIndex(currencyIndex, { supplierName: supplier.name, partnerId: supplier.partnerId }),
      }));
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
  const sourceKey = supplierLedgerSourceKeyForPicking(normalized);
  // Re-marking a row rewrites its debt; the rate of the original purchase must survive that,
  // or the debt's USD value (and the balance) shifts to today's rate.
  const existing = await getPrisma().supplierLedgerEntry.findUnique({ where: { sourceKey } }).catch(() => null);
  const usdRate = Number(existing?.raw?.usdRate || 0) || await supplierLedgerCurrentUsdRate();
  const entry = normalizeSupplierLedgerEntry({
    sourceKey,
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
    const existing = await getPrisma().supplierLedgerEntry.findUnique({ where: { sourceKey } }).catch(() => null);
    const existingRaw = existing?.raw && typeof existing.raw === "object" ? existing.raw : {};
    const saved = await getPrisma().supplierLedgerEntry.update({
      where: { sourceKey },
      data: {
        status: "voided",
        voidedAt: new Date(),
        raw: {
          picking: normalizeSupplierPickingRow(row),
          ...(Number(existingRaw.usdRate || 0) > 0 ? { usdRate: Number(existingRaw.usdRate) } : {}),
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
