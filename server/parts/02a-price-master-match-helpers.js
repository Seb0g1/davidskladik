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

function managedSupplierPriceCurrency(supplier = null, row = {}, link = {}) {
  const explicit = cleanText(supplier?.priceCurrency || supplier?.defaultCurrency).toUpperCase();
  if (explicit === "RUB" || explicit === "RUR") return "RUB";
  if (explicit === "USD") return "USD";
  const partnerName = normalizeSupplierName(
    supplier?.name
    || supplier?.partnerName
    || supplier?.supplierName
    || row.partnerName
    || row.supplierName
    || link.supplierName
    || "",
  );
  return isInnaSupplierName(partnerName) ? "RUB" : "USD";
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

function priceMasterRowMatchesLink(row = {}, link = {}) {
  const fields = priceMasterSnapshotRowFields(row);
  const supplierOk =
    !link.supplierName ||
    normalizeSupplierName(fields.partnerName) === normalizeSupplierName(link.supplierName);
  const partnerOk = link.matchType === "article"
    ? true
    : (!link.partnerId || String(fields.partnerId || "") === String(link.partnerId));
  const keywordOk = includesKeyword(fields.name, link.keyword);
  if (!supplierOk || !partnerOk || !keywordOk) return false;
  if (link.matchType === "selected_row") {
    if (link.sourceRowId && String(fields.rowId || "") === String(link.sourceRowId)) return true;
    if (link.exactName) return exactPriceMasterNameMatches(fields.name, link.exactName);
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

