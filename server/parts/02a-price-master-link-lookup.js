function priceMasterLinkLookupCacheKey(linkInput, usdRate) {
  const link = normalizeWarehouseLink(linkInput);
  return [
    String(link.matchType || "article"),
    cleanText(link.article).toLowerCase(),
    cleanText(link.exactName).toLowerCase(),
    cleanText(link.sourceRowId),
    cleanText(link.partnerId),
    normalizeSupplierName(link.supplierName),
    cleanText(link.keyword).toLowerCase(),
    cleanText(link.priceCurrency || "USD").toUpperCase(),
    Number(usdRate || 0).toFixed(4),
  ].join("|");
}

function setPriceMasterLinkLookupCache(key, rows) {
  if (!key) return;
  if (priceMasterLinkLookupCache.size >= priceMasterLinkLookupCacheMax) {
    const oldest = priceMasterLinkLookupCache.keys().next().value;
    if (oldest) priceMasterLinkLookupCache.delete(oldest);
  }
  priceMasterLinkLookupCache.set(key, { at: Date.now(), rows: Array.isArray(rows) ? rows : [] });
}

function getPriceMasterSearchCache(key) {
  const cached = priceMasterSearchCache.get(key);
  if (!cached || Date.now() - cached.at >= priceMasterSearchCacheTtlMs) return null;
  return Array.isArray(cached.value) ? cloneAuditValue(cached.value) : cached.value;
}

function setPriceMasterSearchCache(key, value) {
  if (!key) return;
  if (priceMasterSearchCache.size >= priceMasterSearchCacheMax) {
    const oldest = priceMasterSearchCache.keys().next().value;
    if (oldest) priceMasterSearchCache.delete(oldest);
  }
  priceMasterSearchCache.set(key, { at: Date.now(), value: cloneAuditValue(value) });
}

async function findPriceMasterRowsForLinkFast(linkInput, usdRate, managedSuppliers = [], options = {}) {
  const link = normalizeWarehouseLink(linkInput);
  if (!warehouseLinkHasMatchTarget(link)) return [];
  const key = priceMasterLinkLookupCacheKey(link, usdRate);
  const cached = priceMasterLinkLookupCache.get(key);
  if (cached && Date.now() - cached.at < priceMasterLinkLookupCacheTtlMs) {
    if (cached.rows.length || options.live === false) return cached.rows;
  }

  const snapshotMap = await getPriceMasterMatchesForLinks([link], managedSuppliers, usdRate).catch((error) => {
    logger.warn("PriceMaster snapshot link lookup skipped", { detail: error?.message || String(error) });
    return new Map();
  });
  const snapshotRows = snapshotMap.get(link.id) || [];
  if (snapshotRows.length) {
    setPriceMasterLinkLookupCache(key, snapshotRows);
    return snapshotRows;
  }

  const postgresRows = await findPriceMasterSnapshotRowsForLink(link, usdRate, managedSuppliers).catch((error) => {
    logger.warn("PriceMaster postgres snapshot link lookup skipped", { detail: error?.message || String(error) });
    return null;
  });
  if (postgresRows?.length || options.live === false) {
    const rows = postgresRows || [];
    setPriceMasterLinkLookupCache(key, rows);
    return rows;
  }

  const timeoutMs = Math.max(250, Number(options.timeoutMs || process.env.LINK_SAVE_PM_TIMEOUT_MS || 1200));
  try {
    const liveRows = await Promise.race([
      findPriceMasterRowsForLink(link, usdRate, managedSuppliers),
      promiseTimeout(timeoutMs, "link_save_pm_timeout"),
    ]);
    setPriceMasterLinkLookupCache(key, liveRows);
    return liveRows;
  } catch (error) {
    logger.warn("PriceMaster link lookup skipped", {
      article: link.article,
      matchType: link.matchType,
      detail: error?.message || String(error),
    });
    if (options.cacheEmpty !== false) setPriceMasterLinkLookupCache(key, []);
    return [];
  }
}

async function getBatchPriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate, { timeoutMs } = {}) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article || link.exactName || link.sourceRowId);
  if (!normalizedLinks.length) return new Map();
  const specialLinks = normalizedLinks.filter((link) => link.matchType !== "article");
  const articleLinks = normalizedLinks.filter((link) => link.matchType === "article" && link.article);
  const map = new Map();
  if (specialLinks.length) {
    for (const link of specialLinks) {
      map.set(link.id, await findPriceMasterRowsForLinkFast(link, usdRate, managedSuppliers, {
        timeoutMs,
        cacheEmpty: false,
      }));
    }
  }
  if (!articleLinks.length) return map;
  const articles = Array.from(new Set(articleLinks.map((link) => link.article))).slice(0, 500);
  const placeholders = articles.map(() => "?").join(",");
  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const [rows] = await pool.query({
    sql: `
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
    WHERE BINARY TRIM(r.NativeID) IN (${placeholders}) AND r.Ignored = 0
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT 5000
    `,
    values: articles,
    timeout: Math.max(250, Number(timeoutMs || process.env.WAREHOUSE_PAGE_PM_TIMEOUT_MS || 1500)),
  });
  const rowsByArticle = new Map();
  for (const row of rows || []) {
    const article = cleanText(row.article);
    if (!rowsByArticle.has(article)) rowsByArticle.set(article, []);
    rowsByArticle.get(article).push(row);
  }
  for (const link of articleLinks) {
    const matches = (rowsByArticle.get(link.article) || [])
      .filter((row) => priceMasterRowMatchesLink(row, link))
      .map((row) => {
        const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
        const pricingMeta = priceMasterSupplierPricingMeta(row, supplierMaps);
        const priceCurrency = resolvePriceMasterRowCurrency(row, link, supplierMaps, usdRate);
        const normalizedPrice = normalizePriceMasterPrice(row.price, usdRate, priceCurrency);
        const price = stoppedSupplier ? 0 : normalizedPrice.price;
        const active = stoppedSupplier ? false : Boolean(row.active);
        return {
          ...link,
          rowId: row.rowId,
          article: row.article,
          name: row.name,
          partnerId: row.partnerId,
          partnerName: row.partnerName,
          price,
          priceCurrency,
          originalPrice: normalizedPrice.originalPrice,
          sourceCurrency: normalizedPrice.sourceCurrency,
          convertedFromRub: normalizedPrice.convertedFromRub,
          priceSource: "live",
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
          docDate: row.docDate,
        };
      });
    map.set(link.id, matches);
  }
  return map;
}

async function assertPriceMasterLinkExists(linkInput, usdRate, managedSuppliers = [], options = {}) {
  const link = normalizeWarehouseLink(linkInput);
  const matches = await findPriceMasterRowsForLinkFast(link, usdRate, managedSuppliers, options);
  if (matches.length) return matches;
  if (link.matchType === "selected_row" && link.sourceRowId) {
    return [{
      rowId: link.sourceRowId,
      article: link.article,
      name: link.exactName,
      partnerId: link.partnerId,
      partnerName: link.supplierName,
      active: true,
      priceSource: "explicit_row_selection",
    }];
  }
  const articleRows = await findPriceMasterRowsForLinkFast({ ...link, supplierName: "", partnerId: "", keyword: "" }, usdRate, managedSuppliers, options);
  const label = link.matchType === "article"
    ? `артикул "${link.article}" должен совпадать с PriceMaster точно`
    : `выбранная строка "${link.exactName || link.article || link.sourceRowId}" должна существовать в PriceMaster`;
  const detailParts = [label];
  if (!articleRows.length) {
    detailParts.push(link.matchType === "article" ? "в PriceMaster нет строки с таким точным артикулом" : "строка PriceMaster не найдена");
  } else {
    if (link.supplierName) detailParts.push(`поставщик должен быть "${link.supplierName}"`);
    if (link.keyword) detailParts.push(`название должно содержать ключ "${link.keyword}"`);
  }
  const error = new Error(`Привязка не сохранена: ${detailParts.join(", ")}.`);
  error.statusCode = 400;
  error.code = "PM_LINK_NOT_FOUND";
  error.matches = articleRows.slice(0, 10).map((row) => ({
    article: row.article,
    name: row.name,
    partnerName: row.partnerName,
    price: row.price,
    active: row.active,
  }));
  throw error;
}

function priceMasterLinkValidationFailure(error, linkInput = {}, index = 0) {
  const link = normalizeWarehouseLink(linkInput);
  return {
    index,
    article: link.article,
    exactName: link.exactName,
    sourceRowId: link.sourceRowId,
    supplierName: link.supplierName,
    partnerId: link.partnerId,
    priceCurrency: link.priceCurrency,
    code: error?.code || "PM_LINK_NOT_FOUND",
    detail: error?.message || String(error),
    matches: Array.isArray(error?.matches) ? error.matches : [],
  };
}

async function resolvePriceMasterLinkForSave(linkInput, usdRate, managedSuppliers = [], options = {}) {
  const link = normalizeWarehouseLink(linkInput);
  if (!warehouseLinkHasMatchTarget(link)) return link;
  const productContext = options.productContext || options.product || {};
  const matches = await findPriceMasterRowsForLinkFast(link, usdRate, managedSuppliers, options);
  if (!matches.length && link.matchType === "selected_row" && link.sourceRowId) {
    return normalizeWarehouseLink({
      ...link,
      resolvedBy: link.resolvedBy || "selected_row_explicit",
    });
  }
  if (matches.length) {
    const selection = link.matchType === "article"
      ? pickSafeArticlePriceMasterRow(matches, productContext)
      : { row: matches[0], resolvedBy: link.matchType === "selected_row" ? "selected_row" : "exact_name", candidates: matches.map((row) => publicPriceMasterCandidate(row, productContext)) };
    if (selection.ambiguous) {
      const error = new Error("У артикула найдено несколько строк PriceMaster, выберите точную строку.");
      error.statusCode = 409;
      error.code = "PM_LINK_AMBIGUOUS";
      error.candidates = selection.candidates.slice(0, 20);
      error.matches = error.candidates;
      throw error;
    }
    const best = selection.row || matches[0];
    return normalizeWarehouseLink({
      ...link,
      matchType: link.matchType === "article" && selection.resolvedBy === "product_name_score" ? "selected_row" : link.matchType,
      exactName: link.exactName || (link.matchType !== "article" || selection.resolvedBy === "product_name_score" ? best.name : ""),
      sourceRowId: link.sourceRowId || (link.matchType === "selected_row" || selection.resolvedBy === "product_name_score" ? best.rowId : ""),
      supplierName: link.supplierName || best.partnerName,
      partnerId: link.partnerId || best.partnerId,
      resolvedBy: selection.resolvedBy,
      resolvedPriceMasterRow: publicPriceMasterCandidate(best, productContext),
    });
  }

  if (link.matchType === "article" && link.article) {
    const nameLink = normalizeWarehouseLink({
      ...link,
      article: "",
      matchType: "exact_name",
      exactName: link.exactName || link.article,
    });
    const nameMatches = await findPriceMasterRowsForLinkFast(nameLink, usdRate, managedSuppliers, options);
    if (nameMatches.length) {
      const best = nameMatches[0];
      return normalizeWarehouseLink({
        ...nameLink,
        supplierName: nameLink.supplierName || best.partnerName,
        partnerId: nameLink.partnerId || best.partnerId,
      });
    }
  }

  return link;
}
