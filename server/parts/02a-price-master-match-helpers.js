function firstImageUrl(value) {
  if (Array.isArray(value)) return cleanText(value[0]);
  const text = cleanText(value);
  if (!text) return "";
  return text.split(/\r?\n|,/).map(cleanText).find(Boolean) || "";
}

function localPublicFilePathFromUrl(value, request) {
  const raw = cleanText(value);
  if (!raw) return "";
  let pathname = raw;
  if (/^https?:\/\//i.test(raw)) {
    try {
      const parsed = new URL(raw);
      const base = new URL(uploadBaseUrl(request));
      if (parsed.origin !== base.origin) return "";
      pathname = parsed.pathname;
    } catch (_error) {
      return "";
    }
  }
  const decodedPathname = decodeURIComponent(pathname.split("?")[0] || "");
  if (!decodedPathname.startsWith("/uploads/")) return "";
  const normalized = path.normalize(decodedPathname.replace(/^\/+/, ""));
  const fullPath = path.join(publicDir, normalized);
  const relative = path.relative(publicDir, fullPath);
  if (relative.startsWith("..") || path.isAbsolute(relative)) return "";
  return fullPath;
}

function normalizeSearchText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/[^a-zа-я0-9]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function includesKeyword(value, keyword) {
  const search = normalizeSearchText(keyword);
  if (!search) return true;
  const source = normalizeSearchText(value);
  return search.split(" ").every((token) => source.includes(token));
}

function exactPriceMasterNameMatches(value, expected) {
  const left = normalizeSearchText(value);
  const right = normalizeSearchText(expected);
  return Boolean(left && right && left === right);
}

function priceMasterNameTokens(value) {
  return normalizeSearchText(value)
    .split(" ")
    .map((token) => token.trim())
    .filter((token) => token.length >= 2 && !["edp", "edt", "parfum", "perfume", "extrait", "de", "ml"].includes(token));
}

function extractPriceMasterVolumes(value) {
  const text = String(value || "").toLowerCase();
  const volumes = new Set();
  for (const match of text.matchAll(/(\d+(?:[.,]\d+)?)\s*(?:ml|мл)/gi)) {
    const number = Number(String(match[1] || "").replace(",", "."));
    if (Number.isFinite(number) && number > 0) volumes.add(String(number).replace(/\.0$/, ""));
  }
  for (const match of text.matchAll(/(?:^|[^0-9])(\d{1,3})(?:\s|$)/g)) {
    const number = Number(match[1]);
    if (Number.isFinite(number) && number > 0 && number <= 500) volumes.add(String(number));
  }
  return Array.from(volumes);
}

function priceMasterTesterFlag(value) {
  const text = normalizeSearchText(value);
  return /\b(test|tester|пробник|тестер)\b/i.test(text);
}

function priceMasterArticleCandidateScore(row = {}, productContext = {}) {
  const productName = cleanText(productContext.name || productContext.title || productContext.offerId || productContext.sku);
  const rowName = cleanText(row.name || row.NativeName || row.nativeName);
  const productTokens = priceMasterNameTokens(productName);
  const rowTokens = new Set(priceMasterNameTokens(rowName));
  const shared = productTokens.filter((token) => rowTokens.has(token));
  let score = shared.length * 12;
  if (productTokens.length && shared.length === productTokens.length) score += 20;
  if (productName && rowName && exactPriceMasterNameMatches(productName, rowName)) score += 80;

  const productVolumes = extractPriceMasterVolumes(productName);
  const rowVolumes = extractPriceMasterVolumes(rowName);
  const sharedVolumes = productVolumes.filter((volume) => rowVolumes.includes(volume));
  const volumeMismatch = productVolumes.length > 0 && rowVolumes.length > 0 && !sharedVolumes.length;
  if (sharedVolumes.length) score += 45;
  if (volumeMismatch) score -= 80;

  const productTester = priceMasterTesterFlag(productName);
  const rowTester = priceMasterTesterFlag(rowName);
  if (productTester === rowTester) score += 8;
  else score -= 35;
  if (String(row.active ?? row.Active ?? true) === "false" || row.active === false || row.Active === false) score -= 30;

  const reason = [
    shared.length ? `tokens:${shared.join(",")}` : "tokens:none",
    sharedVolumes.length ? `volume:${sharedVolumes.join(",")}` : (volumeMismatch ? `volume_mismatch:${productVolumes.join("/")}->${rowVolumes.join("/")}` : "volume:unknown"),
    productTester === rowTester ? "tester:match" : "tester:mismatch",
  ];
  return {
    score,
    reason: reason.join("; "),
    sharedTokens: shared,
    productVolumes,
    rowVolumes,
    volumeMismatch,
  };
}

function publicPriceMasterCandidate(row = {}, productContext = {}) {
  const scored = priceMasterArticleCandidateScore(row, productContext);
  const partnerName = cleanText(row.partnerName || row.PartnerName);
  return {
    rowId: cleanText(row.rowId || row.RowID || row.id),
    article: cleanText(row.article || row.NativeID || row.nativeId),
    name: cleanText(row.name || row.NativeName || row.nativeName),
    partnerName,
    supplierName: partnerName,
    partnerId: cleanText(row.partnerId || row.PartnerID),
    price: Number(row.price ?? row.NativePrice ?? 0) || 0,
    docDate: cleanText(row.docDate || row.DocDate),
    active: row.active !== false && row.Active !== false,
    matchScore: scored.score,
    reason: scored.reason,
  };
}

function priceMasterSnapshotRowFields(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const currency = cleanText(
    row.currency || row.priceCurrency || row.sourceCurrency
    || raw.currency || raw.priceCurrency || raw.Currency,
  ).toUpperCase();
  return {
    rowId: cleanText(row.rowId || row.RowID || raw.rowId || raw.RowID || row.id || row.ID),
    article: cleanText(row.article || row.NativeID || raw.article || raw.NativeID || raw.nativeId || raw.offerId),
    name: cleanText(row.name || row.NativeName || row.nativeName || raw.name || raw.NativeName || raw.nativeName),
    partnerId: cleanText(row.partnerId || row.PartnerID || raw.partnerId || raw.PartnerID),
    partnerName: cleanText(row.partnerName || row.PartnerName || raw.partnerName || raw.PartnerName),
    docDate: row.docDate instanceof Date ? row.docDate.toISOString() : cleanText(row.docDate || row.DocDate || raw.docDate || raw.DocDate),
    price: row.price ?? row.NativePrice ?? raw.price ?? raw.NativePrice,
    currency: currency === "RUB" || currency === "RUR" ? "RUB" : (currency === "USD" ? "USD" : ""),
    active: row.active !== false && row.Active !== false && row.Active !== 0,
    ignored: Boolean(row.ignored || row.Ignored || raw.ignored || raw.Ignored),
  };
}

function priceMasterRowExplicitCurrency(row = {}) {
  const fields = row.currency ? row : priceMasterSnapshotRowFields(row);
  const mode = cleanText(fields.currency || fields.priceCurrency || fields.sourceCurrency).toUpperCase();
  if (mode === "RUB" || mode === "RUR") return "RUB";
  if (mode === "USD") return "USD";
  return "";
}

function isIvannaSupplierName(partnerName = "") {
  const normalized = normalizeSupplierName(partnerName);
  if (!normalized) return false;
  return normalized === "иванна"
    || normalized === "ivanna"
    || normalized.includes("иванна")
    || normalized.includes("ivanna");
}

function isInnaSupplierName(partnerName = "") {
  const normalized = normalizeSupplierName(partnerName);
  if (!normalized) return false;
  if (isIvannaSupplierName(normalized)) return false;
  return normalized === "инна"
    || normalized === "inna"
    || normalized.includes("инна")
    || normalized.includes("inna");
}

function isSorinSupplierName(partnerName = "") {
  const normalized = normalizeSupplierName(partnerName);
  if (!normalized) return false;
  return normalized === "сорин"
    || normalized === "sorin"
    || normalized.includes("сорин")
    || normalized.includes("sorin");
}

function managedSupplierPriceCurrency(supplier = null, row = {}, link = {}) {
  const explicit = cleanText(supplier?.priceCurrency || supplier?.defaultCurrency).toUpperCase();
  if (explicit === "RUB" || explicit === "RUR") return "RUB";
  // Check name before falling back to explicit "USD" — Инна's supplier record was created
  // with the form default "USD" which overrode name-based detection and caused 850 RUB to
  // be treated as $850 USD in markup rule lookup.
  const partnerName = normalizeSupplierName(
    supplier?.partnerName
    || supplier?.supplierName
    || supplier?.name
    || row.partnerName
    || row.supplierName
    || link.supplierName
    || "",
  );
  if (isInnaSupplierName(partnerName)) return "RUB";
  if (explicit === "USD") return "USD";
  return "USD";
}

function supplierUsesRubPriceMasterPricing(supplier = null, row = {}) {
  return managedSupplierPriceCurrency(supplier, row) === "RUB";
}

let stockOnlySupplierNamesCache = { at: 0, names: null };
function stockOnlySupplierNameSet() {
  const now = Date.now();
  if (stockOnlySupplierNamesCache.names && now - stockOnlySupplierNamesCache.at < 60_000) {
    return stockOnlySupplierNamesCache.names;
  }
  const builtins = ["наш склад", "nash sklad", "our warehouse", "own stock"];
  const fromEnv = String(process.env.PRICEMASTER_STOCK_ONLY_SUPPLIER_NAMES || "")
    .split(/[,;|]/)
    .map((value) => normalizeSupplierName(value))
    .filter(Boolean);
  const names = new Set([...builtins, ...fromEnv]);
  stockOnlySupplierNamesCache = { at: now, names };
  return names;
}

function supplierUsesStockOnlyPricing(supplier = null, row = {}) {
  if (normalizeSupplierPricingMode(supplier || {}) === "stock_only") return true;
  const partnerName = normalizeSupplierName(supplier?.name || row.partnerName || row.supplierName || "");
  if (!partnerName) return false;
  if (stockOnlySupplierNameSet().has(partnerName)) return true;
  return partnerName.includes("наш склад")
    || partnerName.includes("nash sklad")
    || partnerName.includes("our warehouse");
}

function warehouseProductUsesStockOnlyPricing(product = {}) {
  if (!product || typeof product !== "object") return false;
  if (product.stockOnlyFallbackActive === true) return true;
  if (product.priceSource === "stock_only_manual") return true;
  if (supplierUsesStockOnlyPricing(null, product.selectedSupplier || {})) return true;
  return false;
}

function inferPriceMasterRowCurrency(row = {}, supplier = null) {
  return managedSupplierPriceCurrency(supplier, row);
}

function pickSafeArticlePriceMasterRow(matches = [], productContext = {}) {
  const rows = (Array.isArray(matches) ? matches : []).filter(Boolean);
  if (rows.length <= 1) {
    return {
      row: rows[0] || null,
      resolvedBy: rows.length ? "article_unique" : "",
      candidates: rows.map((row) => publicPriceMasterCandidate(row, productContext)),
    };
  }
  const scored = rows
    .map((row) => ({ row, ...priceMasterArticleCandidateScore(row, productContext) }))
    .sort((a, b) =>
      b.score - a.score
      || (Number(b.row.active === false ? 0 : 1) - Number(a.row.active === false ? 0 : 1))
      || String(b.row.docDate || "").localeCompare(String(a.row.docDate || ""))
      || Number(b.row.rowId || 0) - Number(a.row.rowId || 0));
  const [best, second] = scored;
  const candidates = scored.map((item) => ({
    ...publicPriceMasterCandidate(item.row, productContext),
    matchScore: item.score,
    reason: item.reason,
  }));
  const confident = best && best.score >= 35 && (!second || best.score - second.score >= 20);
  if (!confident) return { row: null, resolvedBy: "", ambiguous: true, candidates };
  return { row: best.row, resolvedBy: "product_name_score", candidates };
}

// Common Russian legal-form prefixes (ООО, ИП, АО …) stored with a trailing space so the
// strip works on the result of normalizeSupplierName() which is already lowercased.
const PM_SUPPLIER_LEGAL_PREFIXES = ["ооо ", "ип ", "ао ", "зао ", "пао ", "гк ", "тд ", "оао ", "нпо "];

function stripSupplierLegalFormPrefix(normalized = "") {
  for (const prefix of PM_SUPPLIER_LEGAL_PREFIXES) {
    if (normalized.startsWith(prefix) && normalized.length > prefix.length) {
      return normalized.slice(prefix.length).trim();
    }
  }
  return normalized;
}

// Fuzzy supplier name comparison: considers "Авангард" equal to "ООО Авангард" but NOT to
// "Авангард Ростов" (different company). partnerId is a stronger anchor — this is only a
// last-resort fallback for links whose supplierName was saved before PM added/removed a legal
// form prefix.
function supplierNamesMatch(partnerName = "", linkSupplierName = "") {
  const a = normalizeSupplierName(partnerName);
  const b = normalizeSupplierName(linkSupplierName);
  if (a === b) return true;
  return stripSupplierLegalFormPrefix(a) === stripSupplierLegalFormPrefix(b);
}

function priceMasterRowMatchesLink(row = {}, link = {}) {
  const fields = priceMasterSnapshotRowFields(row);
  const supplierOk = !link.supplierName || supplierNamesMatch(fields.partnerName, link.supplierName);
  const partnerOk = link.matchType === "article"
    ? true
    : (!link.partnerId || String(fields.partnerId || "") === String(link.partnerId));
  const keywordOk = includesKeyword(fields.name, link.keyword);
  if (!supplierOk || !partnerOk || !keywordOk) return false;
  if (link.matchType === "selected_row") {
    if (link.sourceRowId && String(fields.rowId || "") === String(link.sourceRowId)) return true;
    if (link.exactName && exactPriceMasterNameMatches(fields.name, link.exactName)) return true;
    // Staleness fallback: when a supplier re-uploads an offer, PriceMaster deactivates the
    // pinned row (sourceRowId) and creates a NEW active row with a different RowID and
    // slightly different NativeName for the SAME supplier+article. Without this, the product
    // loses its supplier ("not_available") and goes to stock 0 with no price/markup even
    // though the supplier is in stock. supplierOk + partnerOk above already scope this to the
    // same supplier (name) AND same partnerId, so an article fall-through cannot leak to a
    // different supplier — at worst it follows the same supplier's current row for the article.
    const pinnedArticle = cleanText(link.article).toLowerCase();
    if (pinnedArticle && cleanText(fields.article).toLowerCase() === pinnedArticle) return true;
    return false;
  }
  if (link.matchType === "exact_name") {
    return exactPriceMasterNameMatches(fields.name, link.exactName || link.article);
  }
  const article = cleanText(link.article).toLowerCase();
  return Boolean(article && cleanText(fields.article).toLowerCase() === article);
}

function hasObjectData(value) {
  if (!value || typeof value !== "object") return false;
  return Object.values(value).some((item) => {
    if (Array.isArray(item)) return item.length > 0;
    if (item && typeof item === "object") return hasObjectData(item);
    return item !== undefined && item !== null && item !== "";
  });
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value || {}).filter(([, item]) => {
      if (Array.isArray(item)) return item.length > 0;
      if (item && typeof item === "object") return hasObjectData(item);
      return item !== undefined && item !== null && item !== "";
    }),
  );
}

function hasDraftInput(input = {}) {
  return Boolean(input && typeof input === "object" && Object.values(input).some((value) => {
    if (Array.isArray(value)) return value.length > 0;
    if (value && typeof value === "object") return hasObjectData(value);
    return value !== undefined && value !== null && value !== "";
  }));
}

// When a selected_row link has a pinned sourceRowId, the staleness-fallback article
// search (priceMasterRowMatchesLink check #3) returns OTHER rows from the same supplier
// that share the article — e.g. Далик has both ALHAMBRA and LATTAFA QUEEN under LQOA100.
// All those rows end up in rawSuppliers and pickWarehouseSupplier picks the cheapest,
// ignoring the user's explicit pin.
// This function keeps only the exact rowId match when it is present; falls back to
// exactName / resolvedPriceMasterRow.name matches; and only uses article matches as a
// staleness fallback (re-upload with new RowID) when the name tokens confirm it is the
// same product. Returning unrelated article matches risks ordering the wrong product when
// a supplier reuses an article number for a completely different item.
function filterSelectedRowMatchesToBestPin(link, matches) {
  if (!Array.isArray(matches) || matches.length <= 1) return matches;
  if (link.matchType !== "selected_row" || !link.sourceRowId) return matches;

  const byRowId = matches.filter((m) => String(m.rowId || "") === String(link.sourceRowId));
  // Return the pinned row only when it's still active AND has a price. If the supplier
  // re-uploaded with a new RowID (old row kept active but price set to 0), fall through
  // to the staleness fallback so the new row with a real price is found instead.
  if (byRowId.some((m) => m.active !== false && (m.price || 0) > 0)) return byRowId;

  // Use explicit exactName first; fall back to the name recorded when the row was pinned.
  // resolvedPriceMasterRow.name is stored by the link UI when a user selects a specific row.
  const fallbackName = link.exactName
    || cleanText((link.resolvedPriceMasterRow && link.resolvedPriceMasterRow.name) || "");
  if (fallbackName) {
    const byName = matches.filter((m) => exactPriceMasterNameMatches(
      cleanText(m.name || m.nativeName || ""), fallbackName,
    ));
    if (byName.length) return byName;
  }

  // Staleness fallback of last resort: the pinned row is gone AND no exact-name match found.
  // Only follow article-matching rows that share at least one meaningful name token with the
  // pinned product name — this confirms the new row is the same product re-uploaded under a
  // different RowID, not a different product that the supplier assigned the same article to.
  // Without this guard, selectSupplierCartSupplierFromMatches would pick the highest rowId
  // among unrelated rows sharing the article, potentially ordering the wrong product.
  if (fallbackName) {
    const anchorTokens = new Set(priceMasterNameTokens(fallbackName));
    if (anchorTokens.size > 0) {
      const bySimilarName = matches.filter((m) => {
        const rowTokens = priceMasterNameTokens(cleanText(m.name || m.nativeName || ""));
        return rowTokens.some((token) => anchorTokens.has(token));
      });
      // Use the token-similar rows only when at least one is active with a price.
      // Otherwise fall back to the pinned row (inactive) so the cart shows not_available
      // instead of ordering a different product from this supplier.
      if (bySimilarName.some((m) => m.active !== false && (m.price || 0) > 0)) return bySimilarName;
    }
  }

  // The pinned row is gone, no name match, no name-similar active row. Return the pinned
  // row (inactive/zero-price) so upstream shows supplier_not_available rather than routing
  // the order to an unrelated product at this supplier.
  return byRowId.length ? byRowId : matches.slice(0, 1);
}

// When a supplier has multiple PM rows for the same article (e.g. Далик encodes different
// perfumes under the same numeric NativeID), use the marketplace order product name to
// pick the correct row by token overlap. Falls through without filtering when the name
// gives no useful signal or there's nothing to disambiguate.
function disambiguateSupplierCartMatchesByOrderName(matches, productName) {
  if (!productName || !matches || !matches.size) return matches;
  const orderTokens = priceMasterNameTokens(productName);
  if (!orderTokens.length) return matches;
  const orderSet = new Set(orderTokens);

  const result = new Map();
  for (const [linkId, rows] of matches) {
    if (!Array.isArray(rows) || rows.length <= 1) { result.set(linkId, rows); continue; }

    // Only disambiguate when rows have distinct PM names (same name = no signal to use).
    const uniqueNames = new Set(rows.map((r) => normalizeSearchText(cleanText(r.name || ""))).filter(Boolean));
    if (uniqueNames.size <= 1) { result.set(linkId, rows); continue; }

    const scored = rows.map((r) => {
      const pmTokens = new Set(priceMasterNameTokens(r.name || ""));
      const intersection = orderTokens.filter((t) => pmTokens.has(t)).length;
      const union = new Set([...orderSet, ...pmTokens]).size;
      return { row: r, score: union > 0 ? intersection / union : 0 };
    });

    const maxScore = Math.max(...scored.map((s) => s.score));
    if (maxScore > 0) {
      const best = scored.filter((s) => s.score >= maxScore * 0.8).map((s) => s.row);
      if (best.length > 0 && best.length < rows.length) { result.set(linkId, best); continue; }
    }
    result.set(linkId, rows);
  }
  return result;
}

