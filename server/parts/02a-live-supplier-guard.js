// «Нет активного поставщика — нет продажи»: последний рубеж перед отправкой остатков на маркетплейсы.
//
// Положительный остаток уходит в Ozon / Маркет, только если у товара есть активная строка в ПОСЛЕДНЕМ прайсе
// поставщика живого PriceMaster (MySQL, не снимок): по строке (RowID), артикулу + поставщику или точному
// названию + поставщику — у выбранного поставщика или у любой привязки. Нет такой строки → вместо остатка
// уходит 0 (лог stock_guard_blocked). Сомнительные (нет ключей для проверки) перепроверяются полным расчётом
// склада по живому PriceMaster. PriceMaster недоступен → положительные остатки не отправляются вовсе
// (fail closed), но и ничего не обнуляется. Товары без привязок продаются только с пометкой «продаётся
// вручную» (noSupplierAutomation.manualSellableAt, 48 ч).
//
// Индекс живых строк — один запрос на ~195k строк, кэш LIVE_SUPPLIER_GUARD_TTL_SECONDS (деф. 120).
// LIVE_SUPPLIER_GUARD=false выключает защиту.

const liveSupplierGuardEnabled = process.env.LIVE_SUPPLIER_GUARD !== "false";
const liveSupplierGuardTtlMs = Math.max(30, Number(process.env.LIVE_SUPPLIER_GUARD_TTL_SECONDS || 120) || 120) * 1000;
const LIVE_GUARD_MANUAL_TTL_MS = 48 * 3_600_000;
let liveSupplierIndexCache = null;
let liveSupplierIndexLoading = null;

function liveGuardKey(value) {
  return String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
}

/** Pure: index of active rows of the latest documents. rows: [{ rowId, article, name, partnerId, partnerName }] */
function buildLiveSupplierIndex(rows = []) {
  const index = { rowIds: new Set(), keys: new Set(), size: 0, at: Date.now() };
  for (const r of rows) {
    if (r.rowId !== undefined && r.rowId !== null && String(r.rowId) !== "") index.rowIds.add(String(r.rowId));
    const partners = [liveGuardKey(r.partnerId), liveGuardKey(r.partnerName)].filter(Boolean);
    const article = liveGuardKey(r.article);
    const name = liveGuardKey(r.name);
    for (const p of partners) {
      if (article) index.keys.add(`a|${article}|${p}`);
      if (name) index.keys.add(`n|${name}|${p}`);
    }
    index.size += 1;
  }
  return index;
}

/** Pure: lookup keys of a product's supplier candidates (selected supplier, its resolved row, every link). */
function liveSupplierCandidates(product = {}) {
  const list = [];
  const push = (c) => { if (c && typeof c === "object") list.push(c); };
  push(product.selectedSupplier);
  if (product.selectedSupplier?.resolvedPriceMasterRow) push(product.selectedSupplier.resolvedPriceMasterRow);
  for (const link of Array.isArray(product.links) ? product.links : []) {
    push(link);
    if (link?.resolvedPriceMasterRow) push(link.resolvedPriceMasterRow);
  }
  return list;
}

/**
 * Pure: true — an active row of the latest docs backs the product; false — candidates exist, none is live;
 * null — nothing to check by (no row / article / name keys).
 */
function productHasLiveSupplierRow(product = {}, index) {
  let checkable = false;
  for (const c of liveSupplierCandidates(product)) {
    const rowId = String(c.rowId ?? c.sourceRowId ?? c.RowID ?? "").trim();
    const article = liveGuardKey(c.article ?? c.supplierArticle ?? c.NativeID);
    const name = liveGuardKey(c.exactName ?? c.name ?? c.nativeName);
    const partners = [liveGuardKey(c.partnerId ?? c.PartnerID), liveGuardKey(c.supplierName ?? c.partnerName)].filter(Boolean);
    if (rowId) {
      checkable = true;
      if (index.rowIds.has(rowId)) return true;
    }
    for (const p of partners) {
      if (article) { checkable = true; if (index.keys.has(`a|${article}|${p}`)) return true; }
      if (name) { checkable = true; if (index.keys.has(`n|${name}|${p}`)) return true; }
    }
  }
  return checkable ? false : null;
}

function liveGuardManualSellable(product = {}, nowMs = Date.now()) {
  const at = product.noSupplierAutomation?.manualSellableAt || product.raw?.noSupplierAutomation?.manualSellableAt;
  return Boolean(at && nowMs - new Date(at).getTime() < LIVE_GUARD_MANUAL_TTL_MS);
}

async function loadLiveSupplierIndex() {
  const startedAt = Date.now();
  const cte = await pmLatestDocsCteSql();
  const [rows] = await pool.query(
    `${cte}
     SELECT r.RowID AS rowId, r.NativeID AS article, r.NativeName AS name, d.PartnerID AS partnerId, p.PartnerName AS partnerName
     FROM pm_latest_docs ld
     JOIN OfferDocs d ON d.DocID = ld.DocID
     JOIN OfferRows r ON r.DocID = d.DocID
     LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
     WHERE r.Ignored = 0 AND r.Active != 0`,
  );
  if (!rows.length) throw new Error("PriceMaster вернул 0 активных строк — не доверяем");
  const index = buildLiveSupplierIndex(rows);
  logger.info("live supplier index loaded", { rows: rows.length, ms: Date.now() - startedAt });
  return index;
}

async function getLiveSupplierIndex() {
  if (liveSupplierIndexCache && Date.now() - liveSupplierIndexCache.at < liveSupplierGuardTtlMs) return liveSupplierIndexCache;
  if (!liveSupplierIndexLoading) {
    liveSupplierIndexLoading = loadLiveSupplierIndex()
      .then((index) => { liveSupplierIndexCache = index; return index; })
      .finally(() => { liveSupplierIndexLoading = null; });
  }
  return liveSupplierIndexLoading;
}

/** Products may come without links (Ozon API items, slim rows): load links / automation flags from Postgres. */
async function liveGuardHydrate(items) {
  const missing = items.filter((p) => !Array.isArray(p.links));
  if (!missing.length || !getPrisma()) return items;
  const ids = missing.map((p) => p.id).filter(Boolean);
  const offerIds = missing.filter((p) => !p.id).map((p) => cleanText(p.offerId || p.offer_id)).filter(Boolean);
  const rows = await getPrisma().warehouseProduct.findMany({
    where: { OR: [ids.length ? { id: { in: ids } } : null, offerIds.length ? { offerId: { in: offerIds } } : null].filter(Boolean) },
    select: { id: true, marketplace: true, target: true, offerId: true, raw: true, links: { select: { supplierArticle: true, supplierName: true, partnerId: true, sourceRowId: true, exactName: true } } },
  });
  const byId = new Map(rows.map((r) => [r.id, r]));
  const byOffer = new Map();
  for (const r of rows) {
    const key = cleanText(r.offerId).toLowerCase();
    if (!byOffer.has(key)) byOffer.set(key, []);
    byOffer.get(key).push(r);
  }
  return items.map((p) => {
    if (Array.isArray(p.links)) return p;
    const found = (p.id && byId.get(p.id)) || null;
    const sameOffer = found ? [found] : byOffer.get(cleanText(p.offerId || p.offer_id).toLowerCase()) || [];
    const links = sameOffer.flatMap((r) => r.links.map((l) => ({ ...l, article: l.supplierArticle })));
    return { ...p, id: p.id || sameOffer[0]?.id, links, noSupplierAutomation: p.noSupplierAutomation || found?.raw?.noSupplierAutomation || sameOffer[0]?.raw?.noSupplierAutomation };
  });
}

/**
 * Splits the positive-stock items of a send. getStock(item) → the stock about to be sent.
 * Returns { allowed: Set(item), blocked: Set(item), skipped: Set(item) }: blocked → send 0 instead,
 * skipped (PriceMaster unreachable) → don't send a positive stock now. Items with stock ≤ 0 are always allowed.
 */
async function guardPositiveStockItems(items = [], { getStock = (item) => Number(item.stock || 0), context = "" } = {}) {
  const allowed = new Set();
  const blocked = new Set();
  const skipped = new Set();
  const positive = [];
  for (const item of items) {
    if (Number(getStock(item)) > 0) positive.push(item);
    else allowed.add(item);
  }
  if (!liveSupplierGuardEnabled || !positive.length) {
    for (const item of positive) allowed.add(item);
    return { allowed, blocked, skipped };
  }
  let index;
  try {
    index = await getLiveSupplierIndex();
  } catch (error) {
    logger.warn("stock_guard_pm_unavailable", { context, items: positive.length, detail: error?.message || String(error) });
    for (const item of positive) skipped.add(item);
    return { allowed, blocked, skipped };
  }
  const hydrated = await liveGuardHydrate(positive).catch(() => positive);
  const nowMs = Date.now();
  const doubtful = [];
  hydrated.forEach((product, i) => {
    const item = positive[i];
    const hasLinks = Array.isArray(product.links) && product.links.length > 0;
    if (!hasLinks) {
      if (liveGuardManualSellable(product, nowMs)) allowed.add(item);
      else blocked.add(item);
      return;
    }
    const live = productHasLiveSupplierRow(product, index);
    if (live === true) allowed.add(item);
    else doubtful.push({ item, product });
  });
  // Second opinion: the full warehouse calculation over live PriceMaster (keyword links, renamed articles)
  if (doubtful.length) {
    const ids = doubtful.map((d) => d.product.id).filter(Boolean);
    const fresh = ids.length
      ? await buildFreshWarehouseProducts(ids, { livePriceMaster: true, batchPriceMaster: true }).catch(() => [])
      : [];
    const freshById = new Map((fresh || []).map((p) => [p.id, p]));
    for (const { item, product } of doubtful) {
      const f = freshById.get(product.id);
      const ok = f && f.selectedSupplier && productHasLiveSupplierRow({ selectedSupplier: f.selectedSupplier, links: [] }, index) !== false;
      if (ok) allowed.add(item);
      else blocked.add(item);
    }
  }
  if (blocked.size) {
    logger.warn("stock_guard_blocked", {
      context,
      blocked: blocked.size,
      sample: [...blocked].slice(0, 10).map((p) => `${p.marketplace || ""}:${p.target || ""}:${p.offerId || p.offer_id}`),
    });
  }
  return { allowed, blocked, skipped };
}
