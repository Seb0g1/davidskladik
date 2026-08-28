// Direct MySQL per-article / per-rowId queries with a 5-minute TTL cache.
// Supplements (does NOT replace) the in-memory snapshot for individual lookups
// where freshness matters (e.g. article-moved detection, stale-price guard).
//
// Usage:
//   const rows = await getLivePMRowsForArticle("178");
//   const row  = await getLivePMRowById(2331708);

const livePMArticleCache = new Map(); // article → { rows, expiresAt }
const livePMRowIdCache   = new Map(); // rowId  → { row,  expiresAt }
const LIVE_PM_CACHE_TTL  = 5 * 60 * 1000;

function livePMBuildSelect() {
  return `
    SELECT
      r.RowID       AS rowId,
      r.NativeID    AS article,
      r.NativeName  AS name,
      r.NativePrice AS price,
      r.Active      AS active,
      r.Ignored     AS ignored,
      d.DocDate     AS docDate,
      d.PartnerID   AS partnerId,
      p.PartnerName AS partnerName
    FROM OfferRows r
    JOIN OfferDocs d ON d.DocID = r.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
  `;
}

function livePMMapRow(row) {
  return {
    rowId:       String(row.rowId   || ""),
    article:     String(row.article || ""),
    name:        String(row.name    || ""),
    price:       Number(row.price   || 0),
    active:      Boolean(row.active),
    ignored:     Boolean(row.ignored),
    docDate:     row.docDate  || null,
    partnerId:   String(row.partnerId  || ""),
    partnerName: String(row.partnerName || ""),
  };
}

async function getLivePMRowsForArticle(article) {
  if (!article) return [];
  const key = String(article).trim();
  const cached = livePMArticleCache.get(key);
  if (cached && Date.now() < cached.expiresAt) return cached.rows;

  try {
    const [raw] = await pool.query(
      `${livePMBuildSelect()}
       WHERE BINARY TRIM(r.NativeID) = BINARY ?
         AND r.Ignored = 0
       ORDER BY d.DocDate DESC, r.RowID DESC
       LIMIT 50`,
      [key],
    );
    const rows = raw.map(livePMMapRow);
    livePMArticleCache.set(key, { rows, expiresAt: Date.now() + LIVE_PM_CACHE_TTL });
    return rows;
  } catch (error) {
    logger.warn("getLivePMRowsForArticle failed", { article: key, detail: error?.message });
    return [];
  }
}

async function getLivePMRowById(rowId) {
  if (!rowId) return null;
  const key = String(rowId).trim();
  const cached = livePMRowIdCache.get(key);
  if (cached && Date.now() < cached.expiresAt) return cached.row;

  try {
    const [raw] = await pool.query(
      `${livePMBuildSelect()}
       WHERE r.RowID = ?
       LIMIT 1`,
      [Number(key)],
    );
    const row = raw.length ? livePMMapRow(raw[0]) : null;
    livePMRowIdCache.set(key, { row, expiresAt: Date.now() + LIVE_PM_CACHE_TTL });
    return row;
  } catch (error) {
    logger.warn("getLivePMRowById failed", { rowId: key, detail: error?.message });
    return null;
  }
}

// Evict cache entries older than TTL (called opportunistically, not on a timer).
function evictLivePMCache() {
  const now = Date.now();
  for (const [k, v] of livePMArticleCache) if (now >= v.expiresAt) livePMArticleCache.delete(k);
  for (const [k, v] of livePMRowIdCache)   if (now >= v.expiresAt) livePMRowIdCache.delete(k);
}
