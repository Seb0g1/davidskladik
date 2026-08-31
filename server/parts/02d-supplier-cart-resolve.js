function supplierPickingInvoiceRows(state = {}, period = "30d") {
  const now = new Date();
  const normalizedPeriod = cleanText(period || "30d").toLowerCase();
  const from = normalizedPeriod === "all"
    ? null
    : new Date(now.getTime() - Math.max(1, Number.parseInt(normalizedPeriod, 10) || 30) * 24 * 60 * 60 * 1000);
  return Object.values(state.rows || {})
    .map(normalizeSupplierPickingRow)
    .filter((row) => row.status === "picked")
    .filter((row) => {
      if (!from) return true;
      const pickedAt = toDateOrNull(row.pickedAt || row.createdAt);
      return pickedAt && pickedAt.getTime() >= from.getTime();
    })
    .sort((left, right) => String(right.pickedAt || right.createdAt).localeCompare(String(left.pickedAt || left.createdAt)));
}

function supplierCartRange(input = {}, settings = defaultAppSettings().supplierCart) {
  const now = new Date();
  const to = toDateOrNull(input.to) || now;
  const from = toDateOrNull(input.from) || new Date(to.getTime() - Math.max(1, Number(settings.lookbackHours || 48)) * 60 * 60 * 1000);
  return { from, to };
}

function supplierCartItemKey(line = {}) {
  return [
    cleanText(line.marketplace).toLowerCase(),
    cleanText(line.accountId || line.campaignId || line.target).toLowerCase(),
    cleanText(line.orderId || line.postingNumber || line.externalOrderId).toLowerCase(),
    cleanText(line.itemId || line.offerId || line.sku).toLowerCase(),
  ].join("|");
}

function normalizeSupplierCartLine(input = {}) {
  const marketplace = cleanText(input.marketplace).toLowerCase();
  const offerId = cleanText(input.offerId || input.offer_id || input.sku);
  const quantity = Math.max(1, Math.round(Number(input.quantity || input.count || 1) || 1));
  const line = {
    key: cleanText(input.key),
    marketplace,
    accountId: cleanText(input.accountId || input.account_id || input.target || input.campaignId),
    accountName: cleanText(input.accountName || input.account_name),
    campaignId: cleanText(input.campaignId || input.campaign_id),
    orderId: cleanText(input.orderId || input.order_id || input.postingNumber || input.posting_number),
    postingNumber: cleanText(input.postingNumber || input.posting_number),
    externalOrderId: cleanText(input.externalOrderId || input.external_order_id),
    itemId: cleanText(input.itemId || input.item_id || input.id || offerId),
    offerId,
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity,
    orderedAt: input.orderedAt || input.createdAt || input.in_process_at || null,
    status: cleanText(input.status),
    isExpress: Boolean(input.isExpress),
    ozonProductId: Number(input.ozonProductId || 0) || 0,
    raw: input.raw && typeof input.raw === "object" ? input.raw : undefined,
  };
  line.key = line.key || supplierCartItemKey(line);
  return line;
}

function normalizeSupplierCartPreviewRow(input = {}) {
  const line = normalizeSupplierCartLine(input);
  const saleAmount = computeMarketplaceSaleAmountRub(input);
  const payoutAmount = input.payoutAmount ?? input.payout_amount ?? saleAmount;
  return {
    ...line,
    warehouseProductId: cleanText(input.warehouseProductId || input.productId),
    groupKey: cleanText(input.groupKey),
    groupOfferId: cleanText(input.groupOfferId || input.group_offer_id || line.offerId),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
    offerRowId: cleanText(input.offerRowId || input.rowId || input.sourceRowId),
    price: Number(input.price || 0) || 0,
    originalPrice: Number(input.originalPrice || 0) || 0,
    priceCurrency: cleanText(input.priceCurrency || input.currency || "USD").toUpperCase(),
    saleAmount: saleAmount === null ? null : saleAmount,
    payoutAmount: payoutAmount === null || payoutAmount === undefined || payoutAmount === "" ? null : normalizeFinanceMoney(payoutAmount, 0),
    soldAt: input.soldAt || input.sold_at || line.orderedAt || null,
    trustFactor: normalizeSupplierTrustFactor(input.trustFactor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(input.orderCutoffTime || input.order_cutoff_time),
    reseller: Boolean(input.reseller),
    supplierScore: Number(input.supplierScore || input.score || 0) || 0,
    available: input.available !== false,
    ready: Boolean(input.ready),
    alreadyCommitted: Boolean(input.alreadyCommitted),
    stockOnlyFallback: Boolean(input.stockOnlyFallback),
    skipReason: cleanText(input.skipReason || input.reason),
    requestDocId: cleanText(input.requestDocId || input.docId),
    requestRowId: cleanText(input.requestRowId || input.rowId),
    manualNote: cleanText(input.manualNote || ""),
  };
}

function selectSupplierCartSupplierFromMatches(matches = new Map(), blockedPartnerIds = new Set(), usdRate = 95, now = new Date()) {
  const isSorinSupplier = (row) => /сорин/i.test(cleanText(row.partnerName || row.supplierName || ""));
  const isInnaSupplier = (row) => /инна/i.test(cleanText(row.partnerName || row.supplierName || ""));

  // Priority overrides: Сорин → Инна. Each is picked first if available and has a price.
  // Requires available=true so out-of-stock items fall through to other suppliers.
  for (const isPriority of [isSorinSupplier, isInnaSupplier]) {
    for (const [linkId, rows] of matches.entries()) {
      for (const row of rows || []) {
        if (!isPriority(row)) continue;
        if (!row?.available) continue;
        if (Number(row.price || 0) <= 0) continue;
        const partnerId = cleanText(row.partnerId).toLowerCase();
        if (blockedPartnerIds.has(partnerId)) continue;
        return {
          selected: { ...row, linkId },
          stockOnlyFallback: false,
          blockedAvailable: 0,
          cutoffPassedAvailable: 0,
        };
      }
    }
  }

  const rawCandidates = [];
  let blockedAvailable = 0;
  let cutoffPassedAvailable = 0;
  let stockOnlyBlocked = 0;

  for (const [linkId, rows] of matches.entries()) {
    for (const row of rows || []) {
      if (!row?.available || !row?.active) continue;
      if (supplierUsesStockOnlyPricing(null, row)) continue;
      if (Number(row.price || 0) <= 0) continue;
      const partnerId = cleanText(row.partnerId).toLowerCase();
      if (blockedPartnerIds.has(partnerId)) {
        blockedAvailable += 1;
        continue;
      }
      if (supplierOrderCutoffPassed(row.orderCutoffTime, now)) {
        cutoffPassedAvailable += 1;
        continue;
      }
      rawCandidates.push({ ...row, linkId });
    }
  }

  // Deduplicate per (partnerId, linkId): when a supplier has multiple rows for the same
  // article code (e.g. different products labelled with the same NativeID), keep only the
  // most-recently uploaded one (highest rowId). This prevents the cheapest-but-wrong row
  // from winning over the correct current row for that product.
  const bestByPartnerLink = new Map();
  for (const c of rawCandidates) {
    const key = `${cleanText(c.partnerId).toLowerCase()}|${String(c.linkId || "")}`;
    const existing = bestByPartnerLink.get(key);
    if (!existing || Number(c.rowId || 0) > Number(existing.rowId || 0)) {
      bestByPartnerLink.set(key, c);
    }
  }
  const candidates = [...bestByPartnerLink.values()];

  const toUsd = (row) => {
    const p = Number(row.price || Number.POSITIVE_INFINITY);
    return cleanText(row.priceCurrency || row.currency || "USD").toUpperCase() === "RUB" ? p / usdRate : p;
  };
  candidates.sort((left, right) =>
    (isSorinSupplier(left) ? 0 : 1) - (isSorinSupplier(right) ? 0 : 1)
    || supplierCartOrderScore(left, usdRate, now) - supplierCartOrderScore(right, usdRate, now)
    || toUsd(left) - toUsd(right)
    || normalizeSupplierTrustFactor(right.trustFactor, 100) - normalizeSupplierTrustFactor(left.trustFactor, 100)
    || String(left.partnerName || "").localeCompare(String(right.partnerName || ""), "ru", { sensitivity: "base" }),
  );

  if (candidates[0]) {
    return {
      selected: candidates[0],
      stockOnlyFallback: false,
      blockedAvailable,
      cutoffPassedAvailable,
    };
  }

  const stockOnlyRows = [];
  for (const [linkId, rows] of matches.entries()) {
    for (const row of rows || []) {
      if (!supplierUsesStockOnlyPricing(null, row) || !row?.available) continue;
      const partnerId = cleanText(row.partnerId).toLowerCase();
      if (blockedPartnerIds.has(partnerId)) {
        stockOnlyBlocked += 1;
        continue;
      }
      stockOnlyRows.push({ ...row, linkId });
    }
  }

  const stockOnly = pickWarehouseStockOnlySupplier(stockOnlyRows);
  if (stockOnly) {
    let skipReason = "stock_only_fallback";
    if (blockedAvailable || stockOnlyBlocked) skipReason = "stock_only_fallback_after_supplier_blocked";
    else if (cutoffPassedAvailable) skipReason = "stock_only_fallback_after_supplier_cutoff";
    return {
      selected: stockOnly,
      stockOnlyFallback: true,
      blockedAvailable,
      cutoffPassedAvailable,
      skipReason,
    };
  }

  return {
    selected: null,
    stockOnlyFallback: false,
    blockedAvailable,
    cutoffPassedAvailable,
    skipReason: blockedAvailable
      ? "supplier_blocked_no_alternative"
      : (cutoffPassedAvailable ? "supplier_cutoff_passed_no_alternative" : "supplier_not_available"),
  };
}

function normalizeOzonSupplierCartPostings(data = {}, account = {}) {
  const postings = Array.isArray(data?.result?.postings)
    ? data.result.postings
    : (Array.isArray(data?.postings) ? data.postings : []);
  const lines = [];
  for (const posting of postings) {
    const products = Array.isArray(posting.products) ? posting.products : [];
    const isExpress = Boolean(posting.is_express || posting.delivery_method?.type === "Express");
    for (const product of products) {
      const line = normalizeSupplierCartLine({
        marketplace: "ozon",
        accountId: account.id || account.clientId || "ozon",
        accountName: account.name || "Ozon",
        orderId: posting.order_id || posting.orderId || posting.posting_number,
        postingNumber: posting.posting_number,
        externalOrderId: posting.posting_number,
        itemId: product.sku || product.offer_id || product.offerId,
        offerId: product.offer_id || product.offerId,
        productName: product.name,
        quantity: product.quantity,
        orderedAt: posting.in_process_at || posting.created_at,
        status: posting.status,
        isExpress,
        ozonProductId: Number(product.sku || product.product_id) || 0,
        raw: { postingNumber: posting.posting_number, product, isExpress },
      });
      if (line.offerId) lines.push(line);
    }
  }
  // Sum quantities for duplicate keys (same posting + same SKU)
  const byKey = new Map();
  for (const line of lines) {
    if (byKey.has(line.key)) {
      byKey.get(line.key).quantity += line.quantity;
    } else {
      byKey.set(line.key, { ...line });
    }
  }
  return Array.from(byKey.values());
}

function normalizeYandexSupplierCartOrders(data = {}, shop = {}) {
  const orders = Array.isArray(data?.orders)
    ? data.orders
    : (Array.isArray(data?.result?.orders) ? data.result.orders : []);
  const lines = [];
  for (const order of orders) {
    // Skip orders already confirmed ready-to-ship or further along — the Yandex API
    // substatus filter is best-effort and sometimes returns READY_TO_SHIP orders anyway.
    const orderSubstatus = cleanText(order.substatus || order.subStatus || "").toUpperCase();
    if (orderSubstatus === "READY_TO_SHIP" || orderSubstatus === "SHIPPED" || orderSubstatus === "DELIVERY") continue;
    const items = Array.isArray(order.items) ? order.items : [];
    const isExpress = cleanText(order.delivery?.type || order.deliveryType || "").toUpperCase() === "EXPRESS";
    const orderCampaignId = cleanText(order.campaignId || shop.campaignId || "");
    for (const item of items) {
      const itemStatus = cleanText(item.itemStatus || item.status).toUpperCase();
      if (itemStatus === "REJECTED" || itemStatus === "RETURNED") continue;
      const line = normalizeSupplierCartLine({
        marketplace: "yandex",
        accountId: shop.id || shop.campaignId || "yandex",
        accountName: shop.name || "Yandex Market",
        campaignId: orderCampaignId,
        orderId: order.id || order.orderId,
        externalOrderId: order.externalOrderId,
        itemId: item.id || item.offerId,
        offerId: item.offerId,
        productName: item.offerName || item.name,
        quantity: item.count,
        orderedAt: order.creationDate || order.creationDateTime || order.updateDate,
        status: order.status,
        isExpress,
        raw: { orderId: order.id, item, isExpress, campaignId: orderCampaignId },
      });
      if (line.offerId) lines.push(line);
    }
  }
  // Sum quantities for duplicate keys (same order + same SKU)
  const byKey = new Map();
  for (const line of lines) {
    if (byKey.has(line.key)) {
      byKey.get(line.key).quantity += line.quantity;
    } else {
      byKey.set(line.key, { ...line });
    }
  }
  return Array.from(byKey.values());
}

async function fetchOzonSupplierCartLines({ from, to, limit, statuses } = {}) {
  const accounts = getOzonAccounts();
  const lines = [];
  // Never fetch awaiting_deliver — those orders are already packed and queued for courier
  // pickup. Including them causes the cart to re-order already-assembled goods.
  const statusList = (Array.isArray(statuses) && statuses.length ? statuses : ["awaiting_packaging"])
    .map((item) => cleanText(item).toLowerCase())
    .filter((s) => Boolean(s) && s !== "awaiting_deliver");
  for (const account of accounts) {
    for (const status of statusList) {
      let offset = 0;
      while (lines.length < limit) {
        const pageLimit = Math.min(1000, Math.max(1, limit - lines.length));
        // Extend the Ozon API "to" by 48 h so overdue postings whose in_process_at
        // gets reset by Ozon to "today" or "tomorrow" are still captured.
        const ozonTo = new Date(to.getTime() + 48 * 60 * 60 * 1000);
        const data = await ozonRequest("/v3/posting/fbs/list", {
          dir: "ASC",
          filter: {
            since: from.toISOString(),
            to: ozonTo.toISOString(),
            status,
          },
          limit: pageLimit,
          offset,
          with: { analytics_data: false, financial_data: false },
        }, account);
        const pageLines = normalizeOzonSupplierCartPostings(data, account);
        lines.push(...pageLines);
        const postings = Array.isArray(data?.result?.postings) ? data.result.postings : [];
        if (postings.length < pageLimit) break;
        offset += postings.length;
      }
    }
  }
  return lines.slice(0, limit);
}

async function fetchYandexSupplierCartLines({ from, to, limit, statuses, substatuses } = {}) {
  const shops = uniqueYandexShopsByBusiness();
  const lines = [];
  const statusList = (Array.isArray(statuses) && statuses.length ? statuses : ["PROCESSING"])
    .map((item) => cleanText(item).toUpperCase())
    .filter(Boolean);
  const substatusList = (Array.isArray(substatuses) && substatuses.length ? substatuses : ["STARTED"])
    .map((item) => cleanText(item).toUpperCase())
    .filter(Boolean);
  for (const shop of shops) {
    let pageToken = "";
    while (lines.length < limit) {
      const campaignIds = parseYandexCampaignIds(shop.campaignId).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0);
      const query = new URLSearchParams({ limit: String(Math.min(50, Math.max(1, limit - lines.length))) });
      if (pageToken) query.set("pageToken", pageToken);
      const data = await yandexRequest(shop, "POST", `/v1/businesses/${shop.businessId}/orders?${query.toString()}`, {
        ...(campaignIds.length ? { campaignIds } : {}),
        statuses: statusList,
        substatuses: substatusList,
        dates: {
          fromDate: from.toISOString().slice(0, 10),
          toDate: to.toISOString().slice(0, 10),
        },
        fake: false,
        sourcePlatforms: ["MARKET"],
      });
      lines.push(...normalizeYandexSupplierCartOrders(data, shop));
      pageToken = cleanText(data?.paging?.nextPageToken || data?.result?.paging?.nextPageToken || data?.nextPageToken);
      if (!pageToken) break;
    }
  }
  return lines.slice(0, limit);
}

function normalizeWbSupplierCartOrders(data = {}, account = {}) {
  const orders = Array.isArray(data?.orders) ? data.orders : [];
  const lines = [];
  for (const order of orders) {
    const offerId = cleanText(order.article || "");
    if (!offerId) continue;
    const line = normalizeSupplierCartLine({
      marketplace: "wb",
      accountId: account.id || "wb",
      accountName: account.name || "Wildberries",
      orderId: String(order.id || ""),
      itemId: String(order.id || ""),
      offerId,
      productName: offerId,
      quantity: 1,
      orderedAt: cleanText(order.createdAt || ""),
      status: "new",
      raw: { orderId: order.id, article: order.article, nmId: order.nmId },
    });
    if (line.offerId) lines.push(line);
  }
  // WB orders each have a unique order ID, so no dedup needed — but group by article+status for safety
  return lines;
}

async function fetchWbSupplierCartLines({ limit } = {}) {
  const accounts = getWbAccounts();
  const lines = [];
  for (const account of accounts) {
    if (lines.length >= limit) break;
    const data = await wbRequest(account, "marketplace", "GET", "/api/v3/orders/new");
    lines.push(...normalizeWbSupplierCartOrders(data, account));
  }
  return lines.slice(0, limit);
}

// The api process keeps the warehouse in memory only as a postgres stub
// (postgresOnly) after the OOM hardening. Unrelated routes partially hydrate
// that stub via mergeWarehouseProductsIntoMemory, so a non-empty products list
// does NOT mean the warehouse is fully loaded — treating it that way made the
// cart resolver report product_not_found for every offer missing from the
// partial cache. On a postgresOnly warehouse we always load the offers being
// resolved from Postgres (with links) and merge them over the cached products.
async function hydrateSupplierCartWarehouse(warehouse = {}, offerIds = []) {
  if (!warehouse?.postgresOnly) return warehouse;
  if (!shouldUsePostgresStorage()) return warehouse;
  const unique = [...new Set(offerIds.map((offerId) => cleanText(offerId)).filter(Boolean))];
  if (!unique.length) return warehouse;
  const loaded = [];
  for (const batch of chunkArray(unique, 200)) {
    const rows = await getPrisma().warehouseProduct.findMany({
      where: { offerId: { in: batch, mode: "insensitive" } },
      include: { links: true },
    }).catch((error) => {
      logger.warn("supplier cart warehouse hydrate failed", { detail: error?.message || String(error) });
      return [];
    });
    loaded.push(...rows.map(productFromPostgres));
  }
  if (!loaded.length) return warehouse;
  // Привязка может жить на соседе по группе с другим артикулом (ozon/yandex-пара,
  // ручная группа) — без соседей buildCommonWarehouseGroupLinks её не увидит.
  const withSiblings = await readWarehouseGroupSiblingsFromPostgres(loaded).catch((error) => {
    logger.warn("supplier cart group sibling hydrate failed", { detail: error?.message || String(error) });
    return loaded;
  });
  const byId = new Map((warehouse.products || []).map((product) => [String(product.id), product]));
  for (const product of withSiblings) byId.set(String(product.id), product);
  return { ...warehouse, products: Array.from(byId.values()) };
}

function findSupplierCartWarehouseProduct(warehouse = {}, line = {}) {
  const offer = cleanText(line.offerId).toLowerCase();
  if (!offer) return null;
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const candidates = products.filter((product) => cleanText(product.offerId).toLowerCase() === offer);
  if (!candidates.length) return null;
  const marketplace = cleanText(line.marketplace).toLowerCase();
  const target = cleanText(line.accountId || line.campaignId).toLowerCase();
  // Exact target match first — prevents primary "ozon" account products from being selected
  // when the same offerId exists on both Ozon accounts (matchesOzonTarget returns true for
  // target="ozon" regardless of which account is being looked up).
  return (
    (target && candidates.find((product) => {
      if (normalizeWarehouseProduct(product).marketplace !== marketplace) return false;
      return cleanText(product.target).toLowerCase() === target;
    })) ||
    candidates.find((product) => {
      const normalized = normalizeWarehouseProduct(product);
      if (normalized.marketplace !== marketplace) return false;
      if (!target) return true;
      if (marketplace === "ozon") return matchesOzonTarget(product.target, target);
      return matchesYandexTarget(product.target, target);
    }) ||
    candidates.find((product) => normalizeWarehouseProduct(product).marketplace === marketplace) ||
    candidates[0]
  );
}

async function resolveSupplierCartRow(warehouse = {}, line = {}, state = {}, { preloadedMatches = null, preloadedUsdRate = null, pickedKeys = null } = {}) {
  const normalizedLine = normalizeSupplierCartLine(line);
  const processed = state.processed?.[normalizedLine.key];
  const alreadyPicked = pickedKeys instanceof Set && pickedKeys.has(normalizedLine.key);
  const product = findSupplierCartWarehouseProduct(warehouse, normalizedLine);
  if (!product) {
    return normalizeSupplierCartPreviewRow({
      ...normalizedLine,
      ready: false,
      skipReason: "product_not_found",
      saleAmount: computeMarketplaceSaleAmountRub(normalizedLine),
      soldAt: normalizedLine.orderedAt,
      alreadyCommitted: Boolean(processed) || alreadyPicked,
      requestDocId: processed?.requestDocId,
      requestRowId: processed?.requestRowId,
    });
  }
  const groupProducts = expandWarehouseProductsToGroups(warehouse.products || [], [product]);
  const groupLinks = buildCommonWarehouseGroupLinks(groupProducts, []);
  if (!groupLinks.length) {
    return normalizeSupplierCartPreviewRow({
      ...normalizedLine,
      warehouseProductId: product.id,
      groupKey: warehouseProductPageGroupKey(product),
      groupOfferId: product.offerId,
      ready: false,
      skipReason: "no_pricemaster_link",
      saleAmount: computeMarketplaceSaleAmountRub(normalizedLine),
      soldAt: normalizedLine.orderedAt,
      alreadyCommitted: Boolean(processed) || alreadyPicked,
      requestDocId: processed?.requestDocId,
      requestRowId: processed?.requestRowId,
    });
  }
  const usdRate = preloadedUsdRate ?? await getUsdRate();
  // Use a pre-fetched batch PM map when available (avoids N+1 MySQL queries per order row).
  // Fall back to per-link live fetch only when no preloaded map was supplied.
  let matches;
  if (preloadedMatches) {
    matches = new Map();
    for (const link of groupLinks) {
      const linkMatches = preloadedMatches.get(link.id);
      if (linkMatches !== undefined) matches.set(link.id, linkMatches);
    }
    if (!matches.size) {
      matches = await getLivePriceMasterMatchesForLinks(groupLinks, warehouse.suppliers || [], usdRate);
    }
  } else {
    matches = await getLivePriceMasterMatchesForLinks(groupLinks, warehouse.suppliers || [], usdRate);
  }
  // Name-based disambiguation: when a supplier has multiple PM rows for the same article
  // (e.g. Далик stores different products under the same numeric code), narrow down to
  // the row whose PM name best matches the ordered product name. Safe no-op when there
  // is only one row or names are identical.
  // Prefer marketplace order name; fall back to warehouse product name when the order
  // name is absent (e.g. WB orders) so Dalik multi-product articles still disambiguate.
  const disambigName = normalizedLine.productName || normalizeWarehouseProduct(product).name;
  if (disambigName) {
    matches = disambiguateSupplierCartMatchesByOrderName(matches, disambigName);
  }
  const blockedPartnerIds = activeSupplierBlocksForOffer(state, normalizedLine.offerId);
  const {
    selected,
    stockOnlyFallback,
    skipReason: selectionSkipReason,
  } = selectSupplierCartSupplierFromMatches(matches, blockedPartnerIds, usdRate);
  if (!selected) {
    return normalizeSupplierCartPreviewRow({
      ...normalizedLine,
      warehouseProductId: product.id,
      groupKey: warehouseProductPageGroupKey(product),
      groupOfferId: product.offerId,
      ready: false,
      skipReason: selectionSkipReason,
      saleAmount: computeMarketplaceSaleAmountRub(normalizedLine),
      soldAt: normalizedLine.orderedAt,
      alreadyCommitted: Boolean(processed) || alreadyPicked,
      requestDocId: processed?.requestDocId,
      requestRowId: processed?.requestRowId,
    });
  }
  return normalizeSupplierCartPreviewRow({
    ...normalizedLine,
    warehouseProductId: product.id,
    groupKey: warehouseProductPageGroupKey(product),
    groupOfferId: product.offerId,
    supplierName: selected.partnerName,
    partnerId: selected.partnerId,
    offerRowId: selected.rowId,
    price: selected.price,
    originalPrice: selected.originalPrice,
    priceCurrency: selected.priceCurrency,
    saleAmount: computeMarketplaceSaleAmountRub(normalizedLine),
    soldAt: normalizedLine.orderedAt,
    trustFactor: selected.trustFactor,
    orderCutoffTime: selected.orderCutoffTime,
    reseller: selected.reseller,
    supplierScore: supplierCartOrderScore(selected, usdRate),
    available: true,
    ready: true,
    stockOnlyFallback,
    skipReason: stockOnlyFallback ? selectionSkipReason : "",
    alreadyCommitted: Boolean(processed) || alreadyPicked,
    requestDocId: processed?.requestDocId,
    requestRowId: processed?.requestRowId,
  });
}
