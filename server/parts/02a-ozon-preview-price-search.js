async function buildOzonProductPreview({ limit = 200, search = "" } = {}) {
  const [rules, existingOfferIds, candidates] = await Promise.all([
    readOzonProductRules(),
    getOzonOfferIdSet(5000),
    getPriceMasterProductCandidates({ limit, search }),
  ]);

  const rows = candidates.map((candidate) => {
    const existing = existingOfferIds.has(candidate.offerId);
    const built = buildOzonProductPayload(candidate, rules);
    return {
      ...candidate,
      ozonExists: existing,
      nextPrice: Number(built.item.price || 0),
      missing: built.missing,
      warnings: built.warnings,
      ready: !existing && built.ready,
    };
  });

  return {
    createdAt: new Date().toISOString(),
    total: rows.length,
    existing: rows.filter((row) => row.ozonExists).length,
    ready: rows.filter((row) => row.ready).length,
    blocked: rows.filter((row) => !row.ready).length,
    rows,
  };
}

function likeSearch(value) {
  return `%${String(value || "").trim()}%`;
}

function mapPriceMasterSearchResponseRow(row = {}, usdRate = 95, supplierMaps = managedSupplierMaps()) {
  const priceCurrency = resolvePriceMasterRowCurrency(row, {}, supplierMaps, usdRate);
  const normalizedPrice = normalizePriceMasterPrice(row.price ?? row.NativePrice ?? 0, usdRate, priceCurrency);
  const article = cleanText(row.article || row.NativeID || row.offerId || row.nativeId || "");
  const name = cleanText(row.name || row.NativeName || row.nativeName || "");
  const partnerName = cleanText(row.partnerName || row.PartnerName || row.supplierName || "");
  const rowId = cleanText(row.rowId || row.RowID || row.id || "");
  const price = normalizedPrice.price || Number(row.price || row.NativePrice || 0) || 0;
  const active = row.active !== false && row.Active !== false && row.Active !== 0;
  return {
    id: rowId || `${article}:${partnerName}:${name}`,
    rowId,
    article,
    supplierName: partnerName,
    partnerId: cleanText(row.partnerId || row.PartnerID || ""),
    keyword: name,
    name,
    price,
    originalPrice: normalizedPrice.originalPrice,
    currency: normalizedPrice.priceCurrency || priceCurrency,
    priceCurrency: normalizedPrice.priceCurrency || priceCurrency,
    available: row.available !== false && active && price > 0,
    active,
    updatedAt: row.docDate || row.DocDate || row.updatedAt || null,
  };
}


