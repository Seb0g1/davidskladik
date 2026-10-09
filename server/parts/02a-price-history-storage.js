async function appendPriceHistoryRows(rows = []) {
  if (!shouldUsePostgresStorage()) return 0;
  const normalizedRows = (Array.isArray(rows) ? rows : [])
    .map((row) => {
      // Store price breakdown in response so we can diagnose "where did this price come from"
      // without a schema migration. Fields: pmPriceUsd, usdRate, markup, supplierName, supplierArticle, reason.
      const breakdown = {};
      if (row.pmPriceUsd != null) breakdown.pmPriceUsd = Number(row.pmPriceUsd);
      if (row.usdRate != null) breakdown.usdRate = Number(row.usdRate);
      if (row.markup != null) breakdown.markup = Number(row.markup);
      if (row.supplierName) breakdown.supplierName = cleanText(row.supplierName);
      if (row.supplierArticle) breakdown.supplierArticle = cleanText(row.supplierArticle);
      if (row.reason) breakdown.reason = cleanText(row.reason);
      const existingResponse = cloneAuditValue(row.response || row.result || null);
      const mergedResponse = Object.keys(breakdown).length
        ? { ...(existingResponse && typeof existingResponse === "object" ? existingResponse : {}), ...breakdown }
        : existingResponse;
      return {
        productId: cleanText(row.productId || row.id) || null,
        marketplace: normalizeMarketplaceEnum(row.marketplace || "ozon"),
        target: cleanText(row.target || row.marketplace) || null,
        offerId: cleanText(row.offerId || row.offer_id),
        oldPrice: row.oldPrice === undefined || row.oldPrice === null ? null : (roundPrice(row.oldPrice) || 0),
        newPrice: roundPrice(row.newPrice ?? row.price ?? 0) || 0,
        status: normalizeQueueStatusEnum(row.status || (row.error ? "failed" : "success")),
        response: mergedResponse,
        error: cleanText(row.error || ""),
        createdAt: toDateOrNull(row.createdAt || row.at) || new Date(),
      };
    })
    .filter((row) => row.offerId && row.newPrice > 0);
  if (!normalizedRows.length) return 0;
  try {
    const windowMs = priceHistoryDedupeWindowMs();
    const seenRows = new Set();
    const candidateRows = [];
    for (const row of normalizedRows) {
      const createdAt = row.createdAt || new Date();
      const rowKey = [
        row.productId || "",
        row.marketplace,
        row.target || "",
        row.offerId,
        row.oldPrice ?? "",
        row.newPrice,
        row.status,
        row.error || "",
        Math.floor(createdAt.getTime() / windowMs),
      ].join("|");
      if (seenRows.has(rowKey)) continue;
      seenRows.add(rowKey);
      candidateRows.push({ row, createdAt });
    }
    if (!candidateRows.length) return 0;

    // Single batch query: find any existing rows within dedup window for all candidates.
    const windowStart = new Date(Math.min(...candidateRows.map(({ createdAt }) => createdAt.getTime())) - windowMs);
    const windowEnd = new Date(Math.max(...candidateRows.map(({ createdAt }) => createdAt.getTime())) + windowMs);
    const offerIds = [...new Set(candidateRows.map(({ row }) => row.offerId))];
    const existingRows = await getPrisma().priceHistory.findMany({
      where: {
        offerId: { in: offerIds },
        createdAt: { gte: windowStart, lte: windowEnd },
      },
      select: { productId: true, marketplace: true, target: true, offerId: true, oldPrice: true, newPrice: true, status: true, error: true, createdAt: true },
    });
    const existingKeys = new Set(
      existingRows.map((r) => {
        const t = r.createdAt ? r.createdAt.getTime() : 0;
        return [r.productId || "", r.marketplace, r.target || "", r.offerId, r.oldPrice ?? "", r.newPrice, r.status, r.error || "", Math.floor(t / windowMs)].join("|");
      }),
    );
    const dedupedRows = candidateRows
      .filter(({ row, createdAt }) => {
        const key = [row.productId || "", row.marketplace, row.target || "", row.offerId, row.oldPrice ?? "", row.newPrice, row.status, row.error || "", Math.floor(createdAt.getTime() / windowMs)].join("|");
        return !existingKeys.has(key);
      })
      .map(({ row }) => row);
    if (!dedupedRows.length) return 0;
    const result = await getPrisma().priceHistory.createMany({
      data: dedupedRows,
      skipDuplicates: true,
    });
    return result.count || 0;
  } catch (error) {
    logger.warn("postgres price history append failed", { detail: error?.message || String(error), rows: normalizedRows.length });
    return 0;
  }
}

function priceHistoryRowFromPostgres(row = {}) {
  const resp = row.response && typeof row.response === "object" ? row.response : null;
  return {
    id: row.id || null,
    productId: row.productId || null,
    marketplace: row.marketplace || "ozon",
    target: row.target || null,
    offerId: row.offerId || null,
    oldPrice: row.oldPrice ?? null,
    newPrice: row.newPrice ?? null,
    status: row.status || "pending",
    response: row.response || null,
    error: row.error || "",
    // Price breakdown (stored in response field to avoid schema migration)
    pmPriceUsd: resp?.pmPriceUsd ?? null,
    usdRate: resp?.usdRate ?? null,
    markup: resp?.markup ?? null,
    supplierName: resp?.supplierName ?? null,
    supplierArticle: resp?.supplierArticle ?? null,
    reason: resp?.reason ?? null,
    at: row.createdAt ? row.createdAt.toISOString() : null,
    createdAt: row.createdAt ? row.createdAt.toISOString() : null,
  };
}

async function readPriceHistory({ productId, offerId, marketplace, status, dateFrom, dateTo, limit = 100, offset = 0 } = {}) {
  const productIds = splitList(productId);
  const offerIds = splitList(offerId);
  const statuses = splitList(status)
    .map((item) => item.toLowerCase() === "error" ? "failed" : item.toLowerCase())
    .filter((item) => item !== "all");
  const marketplaceFilter = cleanText(marketplace).toLowerCase();
  const from = toDateOrNull(dateFrom);
  const to = toDateOrNull(dateTo);
  const safeLimit = Math.max(1, Math.min(500, Number(limit || 100) || 100));
  const safeOffset = Math.max(0, Number(offset || 0) || 0);

  if (shouldUsePostgresStorage()) {
    try {
      const where = {};
      if (productIds.length) where.productId = { in: productIds };
      if (offerIds.length) where.offerId = { in: offerIds };
      if (marketplaceFilter && marketplaceFilter !== "all") where.marketplace = normalizeMarketplaceEnum(marketplaceFilter);
      if (statuses.length) where.status = { in: statuses.map((item) => normalizeQueueStatusEnum(item)) };
      if (from || to) {
        where.createdAt = {};
        if (from) where.createdAt.gte = from;
        if (to) where.createdAt.lte = to;
      }
      const [total, rows] = await Promise.all([
        getPrisma().priceHistory.count({ where }),
        getPrisma().priceHistory.findMany({
          where,
          orderBy: { createdAt: "desc" },
          skip: safeOffset,
          take: safeLimit,
        }),
      ]);
      return {
        source: "postgres",
        total,
        limit: safeLimit,
        offset: safeOffset,
        items: rows.map(priceHistoryRowFromPostgres),
      };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read price history postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }

  const warehouse = await readWarehouse();
  const rows = [];
  for (const product of warehouse.products || []) {
    if (productIds.length && !productIds.includes(String(product.id))) continue;
    if (offerIds.length && !offerIds.includes(String(product.offerId))) continue;
    if (marketplaceFilter && marketplaceFilter !== "all" && cleanText(product.marketplace) !== marketplaceFilter) continue;
    for (const entry of product.priceHistory || []) {
      const at = toDateOrNull(entry.at || entry.createdAt);
      const normalizedStatus = normalizeQueueStatusEnum(entry.status === "error" ? "failed" : entry.status);
      if (statuses.length && !statuses.includes(normalizedStatus)) continue;
      if (from && (!at || at < from)) continue;
      if (to && (!at || at > to)) continue;
      rows.push({
        productId: product.id,
        marketplace: product.marketplace,
        target: entry.target || product.target || product.marketplace,
        offerId: entry.offerId || product.offerId,
        oldPrice: entry.oldPrice ?? null,
        newPrice: entry.newPrice ?? null,
        status: normalizedStatus,
        response: null,
        error: entry.error || "",
        supplierName: entry.supplierName || "",
        supplierArticle: entry.supplierArticle || "",
        reason: entry.reason || "",
        at: at ? at.toISOString() : null,
        createdAt: at ? at.toISOString() : null,
      });
    }
  }
  rows.sort((a, b) => new Date(b.at || 0) - new Date(a.at || 0));
  return {
    source: "json",
    total: rows.length,
    limit: safeLimit,
    offset: safeOffset,
    items: rows.slice(safeOffset, safeOffset + safeLimit),
  };
}

// ─── Price guard ────────────────────────────────────────────────────────────
// 2026-10-02/03: a supplier placeholder (Сорин «1000 $») and rouble prices read as dollars («Ирина (марка)»
// 3900, «Сафронова (марка)» 4205) became 230 403 ₽ … 1 244 177 ₽ on Ozon and Market. A price that looks
// like that is never sent on its own: it waits on «Проверка цен» until a person approves it.
const PRICE_GUARD_SUSPECT_USD = 900;       // a dollar row this high for a bottle up to 200 ml
// 2026-10-09 (the owner's rule): a rise goes out by itself; a big drop only when the supplier's price list is fresh
// — a stale or unread row must not sell the product below what it costs now
const PRICE_GUARD_MAX_DROP = Math.min(0.9, Math.max(0.05, Number(process.env.PRICE_GUARD_MAX_DROP || 0.3) || 0.3));
const PRICE_GUARD_FRESH_HOURS = Math.max(6, Number(process.env.PRICE_GUARD_FRESH_HOURS || 36) || 36);

/** The supplier row's price list date (PriceMaster DocDate, Moscow time) and whether it is fresh enough to drop by. */
function priceGuardSupplierFreshness(supplier = {}) {
  const raw = cleanText(supplier.docDate || supplier.DocDate);
  const at = raw ? Date.parse(/[zZ]|[+-]\d\d:?\d\d$/.test(raw) ? raw : `${raw.replace(" ", "T")}+03:00`) : NaN;
  const unread = cleanText(supplier.priceSource) === "timeout";
  const fresh = Number.isFinite(at) && Date.now() - at <= PRICE_GUARD_FRESH_HOURS * 3600_000 && !unread;
  return { fresh, at: Number.isFinite(at) ? new Date(at) : null, unread };
}

/** Why a supplier row's price can't be trusted (rubles typed as dollars, a placeholder), or "". */
function priceGuardRowProblem(row = {}, productName = "") {
  const currency = cleanText(row.priceCurrency || row.currency || "USD").toUpperCase();
  if (currency === "RUB" || currency === "RUR") return "";
  const price = Number(row.originalPrice ?? row.price ?? 0);
  if (!(price >= PRICE_GUARD_SUSPECT_USD)) return "";
  const volumes = priceMasterBottleVolumes(`${cleanText(row.name)} ${cleanText(productName)}`);
  const volume = volumes.length ? Math.max(...volumes) : 0;
  if (volume && volume > 200) return "";
  return `цена поставщика ${price} $ — похоже на рубли или заглушку`;
}

/** Verdict for one price send: null = fine, else { reason }. An approved price passes. */
function priceGuardVerdict(product = {}, approvedPrice = 0) {
  const next = Math.round(Number(product.nextPrice || 0));
  const current = Math.round(Number(product.currentPrice || 0));
  if (!(next > 0)) return null;
  if (approvedPrice > 0 && Math.abs(next - approvedPrice) <= Math.max(50, approvedPrice * 0.02)) return null;
  const supplier = product.selectedSupplier || {};
  // a broken supplier row (roubles read as dollars, a «1000 $» placeholder) is held whichever way the price goes
  const rowProblem = priceGuardRowProblem(supplier, product.name);
  if (rowProblem) return { reason: rowProblem };
  // a rise is sent as it is
  if (current > 0 && next < current * (1 - PRICE_GUARD_MAX_DROP)) {
    const { fresh, at, unread } = priceGuardSupplierFreshness(supplier);
    if (!fresh) {
      const when = at ? `от ${at.toLocaleString("ru-RU", { timeZone: "Europe/Moscow", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" })}` : "без даты";
      return { reason: `цена упадёт на ${Math.round((1 - next / current) * 100)}%: ${current} → ${next} ₽, ${unread ? "прайс поставщика не прочитан" : `прайс поставщика ${when} — не свежий`}` };
    }
  }
  return null;
}

let priceGuardTableReady = false;
async function ensurePriceGuardTable() {
  if (priceGuardTableReady) return getPrisma();
  const prisma = getPrisma();
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS price_guard_holds (
      product_id TEXT PRIMARY KEY,
      marketplace TEXT,
      target TEXT,
      offer_id TEXT,
      name TEXT,
      current_price INTEGER,
      next_price INTEGER,
      supplier TEXT,
      supplier_price NUMERIC,
      supplier_currency TEXT,
      reason TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      approved_price INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  priceGuardTableReady = true;
  return prisma;
}

/** product id → approved price for the products in this send. */
async function readPriceGuardApprovals(productIds = []) {
  if (!productIds.length || !getPrisma() || !shouldUsePostgresStorage()) return new Map();
  const prisma = await ensurePriceGuardTable();
  const rows = await prisma.$queryRawUnsafe(
    `SELECT product_id, approved_price FROM price_guard_holds WHERE product_id = ANY($1::text[]) AND status = 'approved'`,
    productIds.map(String),
  );
  return new Map(rows.map((r) => [String(r.product_id), Number(r.approved_price || 0)]));
}

async function recordPriceGuardHolds(holds = []) {
  if (!holds.length || !getPrisma() || !shouldUsePostgresStorage()) return;
  const prisma = await ensurePriceGuardTable();
  for (const h of holds) {
    const s = h.product.selectedSupplier || {};
    await prisma.$executeRawUnsafe(
      `INSERT INTO price_guard_holds (product_id, marketplace, target, offer_id, name, current_price, next_price, supplier, supplier_price, supplier_currency, reason, status, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', now())
       ON CONFLICT (product_id) DO UPDATE SET current_price = EXCLUDED.current_price, next_price = EXCLUDED.next_price,
         supplier = EXCLUDED.supplier, supplier_price = EXCLUDED.supplier_price, supplier_currency = EXCLUDED.supplier_currency,
         reason = EXCLUDED.reason, name = EXCLUDED.name,
         -- a rejected or approved verdict stays for the same price; a different price asks again
         status = CASE WHEN price_guard_holds.status IN ('rejected', 'approved') AND abs(coalesce(price_guard_holds.next_price, 0) - EXCLUDED.next_price) <= greatest(50, EXCLUDED.next_price * 0.02)
                       THEN price_guard_holds.status ELSE 'pending' END,
         updated_at = now()`,
      String(h.product.id), cleanText(h.product.marketplace), cleanText(h.product.target), cleanText(h.product.offerId),
      cleanText(h.product.name).slice(0, 300), Math.round(Number(h.product.currentPrice || 0)) || null, Math.round(Number(h.product.nextPrice || 0)),
      cleanText(s.partnerName || s.supplierName).slice(0, 120), Number(s.originalPrice ?? s.price ?? 0) || null,
      cleanText(s.priceCurrency || s.currency).slice(0, 8), h.reason,
    ).catch((error) => logger.warn("price guard hold write failed", { detail: error?.message }));
  }
}

/** A price that passed (supplier fixed, price normal again) closes its open hold. */
async function clearPriceGuardHolds(productIds = []) {
  if (!productIds.length || !getPrisma() || !shouldUsePostgresStorage()) return;
  const prisma = await ensurePriceGuardTable();
  await prisma.$executeRawUnsafe(
    `UPDATE price_guard_holds SET status = 'cleared', updated_at = now() WHERE product_id = ANY($1::text[]) AND status IN ('pending', 'rejected')`,
    productIds.map(String),
  ).catch(() => null);
}
