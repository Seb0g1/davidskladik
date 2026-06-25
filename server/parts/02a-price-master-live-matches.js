async function getPriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article || link.exactName || link.sourceRowId);
  if (!normalizedLinks.length) return new Map();

  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const snapshotIndexes = await getPriceMasterSnapshotIndexes();

  const map = new Map();
  for (const link of normalizedLinks) {
    let candidateRows = [];
    if (link.matchType === "article") {
      candidateRows = snapshotIndexes.byArticle.get(link.article) || [];
    } else if (link.matchType === "selected_row" && link.article) {
      // Self-heal: a pinned row gets deactivated when the supplier re-uploads its offer
      // (new RowID, same partner+article). Select by article so the current active row is a
      // candidate; priceMasterRowMatchesLink + partnerId/supplier scoping keep it correct.
      candidateRows = snapshotIndexes.byArticle.get(link.article)
        || snapshotIndexes.byRowId.get(cleanText(link.sourceRowId))
        || [];
    } else if (link.matchType === "selected_row" && link.sourceRowId) {
      candidateRows = snapshotIndexes.byRowId.get(cleanText(link.sourceRowId)) || [];
    } else {
      const exactName = cleanText(link.exactName || link.article).toLowerCase();
      candidateRows = exactName ? (snapshotIndexes.byName.get(exactName) || []) : snapshotIndexes.rows;
    }
    const matches = candidateRows
      .filter((row) => priceMasterRowMatchesLink(row, link))
      .map((row) => {
        const fields = priceMasterSnapshotRowFields(row);
        const stoppedSupplier = stoppedMap.get(normalizeSupplierName(fields.partnerName));
        const pricingMeta = priceMasterSupplierPricingMeta(fields, supplierMaps);
        const priceCurrency = resolvePriceMasterRowCurrency(fields, link, supplierMaps, usdRate);
        const normalizedPrice = normalizePriceMasterPrice(fields.price, usdRate, priceCurrency);
        const price = stoppedSupplier ? 0 : normalizedPrice.price;
        const active = stoppedSupplier ? false : Boolean(fields.active);
        return {
          ...link,
          rowId: fields.rowId,
          article: fields.article,
          name: fields.name,
          partnerId: fields.partnerId,
          partnerName: fields.partnerName,
          price,
          priceCurrency,
          originalPrice: normalizedPrice.originalPrice,
          sourceCurrency: normalizedPrice.sourceCurrency,
          convertedFromRub: normalizedPrice.convertedFromRub,
          priceSource: "snapshot",
          active,
          stopped: Boolean(stoppedSupplier),
          stopReason: stoppedSupplier?.note || null,
          pricingMode: pricingMeta.pricingMode,
          stockOnly: pricingMeta.stockOnly,
          priceEligible: pricingMeta.priceEligible,
          stockEligible: pricingMeta.stockEligible,
          trustFactor: pricingMeta.trustFactor,
          orderCutoffTime: pricingMeta.orderCutoffTime,
          reseller: pricingMeta.reseller,
          available: active && (pricingMeta.stockOnly || price > 0),
          docDate: fields.docDate,
        };
      });
    map.set(link.id, matches);
  }

  return map;
}

async function findPriceMasterRowsForLink(linkInput, usdRate, managedSuppliers = []) {
  const link = normalizeWarehouseLink(linkInput);
  if (!link.article && !link.exactName && !link.sourceRowId) return [];
  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const conditions = ["r.Ignored = 0"];
  if (offerDocsActiveColumn) conditions.push(`d.${offerDocsActiveColumn} != 0`);
  const params = [];
  if (link.matchType === "selected_row" && link.article) {
    // Self-heal pinned links: match the supplier's current rows for this article (scoped to
    // partner below) instead of the pinned RowID, which the supplier's re-upload deactivated.
    conditions.push("BINARY TRIM(r.NativeID) = BINARY ?");
    params.push(link.article);
  } else if (link.matchType === "selected_row" && link.sourceRowId) {
    conditions.push("r.RowID = ?");
    params.push(Number(link.sourceRowId));
  } else if (link.matchType === "selected_row" || link.matchType === "exact_name") {
    conditions.push("r.NativeName IS NOT NULL AND TRIM(r.NativeName) <> ''");
    if (link.exactName || link.article) {
      conditions.push("LOWER(TRIM(r.NativeName)) = LOWER(TRIM(?))");
      params.push(link.exactName || link.article);
    }
  } else {
    conditions.push("BINARY TRIM(r.NativeID) = BINARY ?");
    params.push(link.article);
  }
  if (link.partnerId && link.matchType !== "article") {
    conditions.push("d.PartnerID = ?");
    params.push(Number(link.partnerId));
  }
  const [rows] = await pool.query(
    `
    SELECT
      r.NativeID AS article,
      r.NativeName AS name,
      r.NativePrice AS price,
      r.Active AS active,
      r.Ignored AS ignored,
      r.RowID AS rowId,
      d.DocDate AS docDate,
      d.PartnerID AS partnerId,
      p.PartnerName AS partnerName
    FROM OfferRows r
    JOIN OfferDocs d ON d.DocID = r.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE ${conditions.join(" AND ")}
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT 200
    `,
    params,
  );
  return rows
    .filter((row) => priceMasterRowMatchesLink(row, link))
    .map((row) => {
      const priceCurrency = resolvePriceMasterRowCurrency(row, link, supplierMaps, usdRate);
      const pricingMeta = priceMasterSupplierPricingMeta(row, supplierMaps);
      const priceData = normalizePriceMasterPrice(row.price, usdRate, priceCurrency);
      return {
        ...row,
        priceCurrency,
        ...priceData,
        pricingMode: pricingMeta.pricingMode,
        stockOnly: pricingMeta.stockOnly,
        priceEligible: pricingMeta.priceEligible,
        stockEligible: pricingMeta.stockEligible,
        trustFactor: pricingMeta.trustFactor,
        orderCutoffTime: pricingMeta.orderCutoffTime,
        reseller: pricingMeta.reseller,
        active: Boolean(row.active),
        ignored: Boolean(row.ignored),
        available: Boolean(row.active) && (pricingMeta.stockOnly || Number(priceData.price || 0) > 0),
      };
    });
}

function priceMasterSnapshotLinkRow(row = {}, link = {}, usdRate, supplierMaps = managedSupplierMaps()) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const rowCurrency = cleanText(row.currency || raw.currency || raw.priceCurrency).toUpperCase();
  const base = {
    article: cleanText(row.article || raw.article || raw.NativeID || raw.offerId || ""),
    name: cleanText(row.nativeName || raw.name || raw.NativeName || ""),
    price: row.price ?? raw.price ?? raw.NativePrice ?? 0,
    active: row.active !== false,
    ignored: Boolean(raw.ignored || raw.Ignored),
    rowId: cleanText(row.rowId || raw.rowId || raw.RowID || row.id),
    docDate: row.docDate instanceof Date ? row.docDate.toISOString() : cleanText(row.docDate || raw.docDate || raw.DocDate || ""),
    partnerId: cleanText(row.partnerId || raw.partnerId || raw.PartnerID || ""),
    partnerName: cleanText(row.partnerName || raw.partnerName || raw.PartnerName || raw.name || ""),
    currency: rowCurrency === "RUB" || rowCurrency === "RUR" ? "RUB" : (rowCurrency === "USD" ? "USD" : ""),
  };
  const priceCurrency = resolvePriceMasterRowCurrency(base, link, supplierMaps, usdRate);
  const pricingMeta = priceMasterSupplierPricingMeta(base, supplierMaps);
  const priceData = normalizePriceMasterPrice(base.price, usdRate, priceCurrency);
  return {
    ...base,
    priceCurrency,
    ...priceData,
    priceSource: "postgres_snapshot",
    pricingMode: pricingMeta.pricingMode,
    stockOnly: pricingMeta.stockOnly,
    priceEligible: pricingMeta.priceEligible,
    stockEligible: pricingMeta.stockEligible,
    trustFactor: pricingMeta.trustFactor,
    orderCutoffTime: pricingMeta.orderCutoffTime,
    reseller: pricingMeta.reseller,
    available: Boolean(base.active) && (pricingMeta.stockOnly || Number(priceData.price || 0) > 0),
  };
}

async function findPriceMasterSnapshotRowsForLink(linkInput, usdRate, managedSuppliers = []) {
  if (!shouldUsePostgresStorage()) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  const link = normalizeWarehouseLink(linkInput);
  if (!warehouseLinkHasMatchTarget(link)) return [];

  const and = [];
  if (link.matchType === "selected_row" && link.article) {
    // Self-heal pinned links (see findPriceMasterRowsForLink): match by article + partner so
    // a re-uploaded supplier's current active row replaces the deactivated pinned row.
    and.push({ article: cleanText(link.article) });
  } else if (link.matchType === "selected_row" && link.sourceRowId) {
    and.push({ rowId: cleanText(link.sourceRowId) });
  } else if (link.matchType === "selected_row" || link.matchType === "exact_name") {
    const exactName = cleanText(link.exactName || link.article);
    if (exactName) and.push({ nativeName: { equals: exactName, mode: "insensitive" } });
  } else {
    and.push({ article: cleanText(link.article) });
  }
  if (link.partnerId && link.matchType !== "article") and.push({ partnerId: cleanText(link.partnerId) });
  if (!and.length) return [];

  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const rows = await prisma.priceMasterSnapshotItem.findMany({
    where: { AND: and },
    orderBy: [{ docDate: "desc" }, { updatedAt: "desc" }],
    take: 200,
  });
  return rows
    .map((row) => priceMasterSnapshotLinkRow(row, link, usdRate, supplierMaps))
    .filter((row) => priceMasterRowMatchesLink(row, link));
}

async function getLivePriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article || link.exactName || link.sourceRowId);
  if (!normalizedLinks.length) return new Map();
  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const map = new Map();
  const mapRow = (link) => (row) => {
    const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
    const price = stoppedSupplier ? 0 : row.price;
    const active = stoppedSupplier ? false : Boolean(row.active);
    return {
      ...link,
      rowId: row.rowId,
      article: row.article,
      name: row.name,
      partnerId: row.partnerId,
      partnerName: row.partnerName,
      price,
      priceCurrency: row.priceCurrency,
      originalPrice: row.originalPrice,
      sourceCurrency: row.sourceCurrency,
      convertedFromRub: row.convertedFromRub,
      priceSource: "live",
      active,
      stopped: Boolean(stoppedSupplier),
      stopReason: stoppedSupplier?.note || null,
      pricingMode: row.pricingMode,
      stockOnly: row.stockOnly,
      priceEligible: row.priceEligible,
      stockEligible: row.stockEligible,
      trustFactor: row.trustFactor,
      orderCutoffTime: row.orderCutoffTime,
      reseller: row.reseller,
      available: active && (row.stockOnly || price > 0),
      docDate: row.docDate,
    };
  };
  // Parallel lookups with a small pool: the serial per-link version cost 60-90s per
  // 1000 products (one local MySQL query per link, awaited one by one).
  const lookupConcurrency = Math.max(1, Math.min(32, Number(process.env.PM_LIVE_LOOKUP_CONCURRENCY || 12) || 12));
  for (let offset = 0; offset < normalizedLinks.length; offset += lookupConcurrency) {
    const slice = normalizedLinks.slice(offset, offset + lookupConcurrency);
    const sliceRows = await Promise.all(slice.map(async (link) => ({
      link,
      rows: await findPriceMasterRowsForLink(link, usdRate, managedSuppliers),
    })));
    for (const { link, rows } of sliceRows) {
      map.set(link.id, rows.map(mapRow(link)));
    }
  }
  return map;
}


