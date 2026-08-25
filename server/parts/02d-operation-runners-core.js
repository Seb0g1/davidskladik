async function runSalesAutomationOperation(payload = {}) {
  const result = await queueAuthoritativePriceReprice({
    productIds: Array.isArray(payload.productIds) ? payload.productIds : undefined,
    marketplace: payload.marketplace || "all",
    force: payload.force === true,
    onlyChanged: payload.onlyChanged !== false,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
    verify: payload.verify !== false,
    limit: cleanLimit(payload.limit, 1000, 50000),
    reason: payload.reason || "sales_automation_operation",
    sourceEvent: "sales_automation_operation",
  });
  return {
    ok: result.ok !== false,
    ...result,
    summary: `Sales automation queued ${result.queued || 0} SKU in ${result.queuedBatches || 0} batches.`,
  };
}

async function runProblemProductsRepairOperation(payload = {}, request = null, options = {}) {
  const productIds = Array.isArray(payload.productIds) ? payload.productIds.map(cleanText).filter(Boolean).slice(0, 100) : [];
  const results = [];
  let processed = 0;
  for (const id of productIds) {
    processed += 1;
    await options.onProgress?.({
      progress: 5 + (processed / Math.max(1, productIds.length)) * 90,
      summary: `Repairing ${processed} of ${productIds.length} problem products.`,
    });
    try {
      results.push(await repairWarehouseProductGroup(id, request));
    } catch (error) {
      results.push({ ok: false, productId: id, error: error?.message || String(error) });
    }
  }
  return {
    ok: results.every((item) => item.ok !== false),
    repaired: results.filter((item) => item.ok !== false).length,
    failed: results.filter((item) => item.ok === false).length,
    results,
    summary: `Problem products repaired ${results.filter((item) => item.ok !== false).length}; failed ${results.filter((item) => item.ok === false).length}.`,
  };
}

async function runBrandIndexRebuildOperation(payload = {}) {
  if (!shouldUsePostgresStorage()) {
    return { ok: false, error: "postgres_required", summary: "Brand index requires PostgreSQL storage." };
  }
  const limit = cleanLimit(payload.limit, 100000, 200000);
  const result = await rebuildWarehouseBrandIndexPostgres(getPrisma(), { limit });
  return {
    ok: result.ok !== false,
    ...result,
    source: "postgres",
    summary: `Brand index rebuilt: indexed ${result.indexed || result.created || 0}; scanned ${result.scanned || 0}.`,
  };
}

async function runYandexPricePushOperation(payload = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const force = payload?.force === true;
  const onlyChanged = payload?.onlyChanged !== false;
  const result = await sendWarehousePrices({
    marketplace: "yandex",
    limit,
    force,
    onlyChanged,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
  });
  return {
    ok: result.ok,
    marketplace: "yandex",
    limit,
    force,
    onlyChanged,
    processed: result.selected || limit,
    sent: result.sent || 0,
    failed: result.failed || 0,
    skipped: Array.isArray(result.skipped) ? result.skipped.length : Number(result.skipped || 0) || 0,
    yandexSent: result.yandexSent || 0,
    yandexFailed: result.yandexFailed || 0,
    yandexSkipped: result.yandexSkipped || 0,
    errors: Array.isArray(result.failedItems) ? result.failedItems : [],
    ...result,
    summary: `Yandex price push sent ${result.yandexSent || result.sent || 0}; failed ${result.yandexFailed || result.failed || 0}; skipped ${result.yandexSkipped || (Array.isArray(result.skipped) ? result.skipped.length : 0)}.`,
  };
}

async function runRestoreYandexMarkupsOperation(payload = {}) {
  if (!shouldUsePostgresStorage()) {
    return { ok: false, error: "postgres_required", summary: "Требуется PostgreSQL." };
  }
  const prisma = getPrisma();
  if (!prisma) return { ok: false, error: "no_db", summary: "Нет подключения к БД." };

  const dryRun = payload.dryRun !== false;
  const minMarkup = Number(payload.minMarkup || 1.0) || 1.0;
  const maxMarkup = Number(payload.maxMarkup || 6.0) || 6.0;

  // Use lean SQL queries to avoid OOM from loading full Prisma objects with links.
  // Step 1: get product IDs + raw JSON for markup/yandex fields only
  const productRows = await prisma.$queryRawUnsafe(`
    SELECT id, offer_id AS "offerId", raw
    FROM warehouse_products
    WHERE marketplace = 'yandex' AND archived = false
  `);
  if (!productRows.length) {
    return { ok: true, dryRun, updated: 0, skipped: 0, total: 0, summary: "Нет Яндекс-товаров." };
  }

  // Step 2: get articles from links table for these products
  const productIds = productRows.map((r) => String(r.id));
  const linkRows = await prisma.$queryRawUnsafe(`
    SELECT product_id AS "productId", supplier_article AS article, supplier_name AS "supplierName"
    FROM product_links
    WHERE product_id = ANY($1) AND supplier_article IS NOT NULL AND supplier_article != ''
  `, productIds);
  // Map productId → [articles]; also track which articles are from Инна (RUB-native)
  const articlesByProduct = new Map();
  const innaArticles = new Set(); // articles linked to Инна — prices in PM are in RUB, not USD
  for (const link of linkRows) {
    const pid = String(link.productId);
    if (!articlesByProduct.has(pid)) articlesByProduct.set(pid, []);
    articlesByProduct.get(pid).push(cleanText(link.article));
    if (isInnaSupplierName(link.supplierName || "")) innaArticles.add(cleanText(link.article));
  }
  // Also check raw.links fallback (for Yandex products linked via raw JSON only)
  for (const row of productRows) {
    const pid = String(row.id);
    if (articlesByProduct.has(pid)) continue;
    const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
    const rawLinks = Array.isArray(raw.links) ? raw.links : [];
    const rawArticles = rawLinks.map((l) => cleanText(l.article || l.supplierArticle || "")).filter(Boolean);
    if (rawArticles.length) articlesByProduct.set(pid, rawArticles);
  }

  const yandexProducts = productRows
    .filter((r) => (articlesByProduct.get(String(r.id)) || []).length > 0)
    .map((r) => ({ id: String(r.id), offerId: cleanText(r.offerId), raw: r.raw || {} }));

  if (!yandexProducts.length) {
    return { ok: true, dryRun, updated: 0, skipped: 0, total: productRows.length, summary: "Нет Яндекс-товаров с привязкой PM." };
  }

  // Step 3: last successful Yandex price per product
  const productIdList = yandexProducts.map((p) => p.id);
  const historyRows = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT ON (product_id) product_id, new_price
    FROM price_history
    WHERE product_id = ANY($1)
      AND marketplace = 'yandex'
      AND status = 'success'
    ORDER BY product_id, created_at DESC
  `, productIdList);
  const lastPriceById = new Map(historyRows.map((r) => [String(r.product_id), Number(r.new_price)]));

  // Step 4: cheapest non-ignored PM price per article from live MySQL
  // (pool is the PM MySQL connection; prices are in USD by default)
  const allArticles = Array.from(new Set(
    Array.from(articlesByProduct.values()).flat().filter(Boolean),
  ));
  const pmPriceByArticle = new Map();
  if (allArticles.length) {
    const queryTimeout = Math.max(8000, Number(process.env.WAREHOUSE_PAGE_PM_TIMEOUT_MS || 15000) || 15000);
    for (const batch of chunkArray(allArticles, 500)) {
      const placeholders = batch.map(() => "?").join(",");
      try {
        const [pmRows] = await pool.query({
          sql: `SELECT BINARY TRIM(r.NativeID) AS article, MIN(r.NativePrice) AS price
                FROM OfferRows r
                WHERE BINARY TRIM(r.NativeID) IN (${placeholders}) AND r.Ignored = 0
                GROUP BY BINARY TRIM(r.NativeID)`,
          values: batch,
          timeout: queryTimeout,
        });
        for (const row of pmRows) {
          const key = String(row.article || "").trim();
          if (!pmPriceByArticle.has(key) && Number(row.price) > 0) {
            pmPriceByArticle.set(key, Number(row.price));
          }
        }
      } catch (pmError) {
        logger.warn("restore_yandex_markups PM batch failed", { detail: pmError?.message || String(pmError) });
      }
    }
  }

  // Step 5: current USD rate (getUsdRate returns { rate, source, ... })
  const usdRateObj = await getUsdRate().catch(() => null);
  const usdRate = Number(usdRateObj?.rate || process.env.DEFAULT_USD_RATE || 95) || 95;

  const updates = [];
  const skipped = [];

  for (const product of yandexProducts) {
    const lastPrice = lastPriceById.get(product.id);
    if (!lastPrice || lastPrice <= 0) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "no_price_history" });
      continue;
    }

    const articles = articlesByProduct.get(product.id) || [];
    let bestPmRub = null;
    for (const article of articles) {
      const pmPrice = pmPriceByArticle.get(article);
      if (!pmPrice || pmPrice <= 0) continue;
      // Инна prices in PM are in RUB — use as-is. All other suppliers price in USD.
      const rubEquiv = innaArticles.has(article) ? pmPrice : pmPrice * usdRate;
      if (bestPmRub === null || rubEquiv < bestPmRub) bestPmRub = rubEquiv;
    }

    if (!bestPmRub || bestPmRub <= 0) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "no_pm_price", lastPrice });
      continue;
    }

    const markup = Math.round((lastPrice / bestPmRub) * 10000) / 10000;
    if (!Number.isFinite(markup) || markup < minMarkup || markup > maxMarkup) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "markup_out_of_range", markup: Math.round(markup * 100) / 100, lastPrice, pmRub: Math.round(bestPmRub) });
      continue;
    }

    updates.push({ id: product.id, offerId: product.offerId, raw: product.raw, markup, lastPrice, pmRub: Math.round(bestPmRub) });
  }

  if (!dryRun && updates.length) {
    // Single-query bulk patch via CTE to minimise round-trips and event-loop blocking.
    // Build: WITH u(id, patch) AS (VALUES ...) UPDATE warehouse_products SET raw=raw||patch::jsonb
    const chunkSize = 500;
    for (let i = 0; i < updates.length; i += chunkSize) {
      const chunk = updates.slice(i, i + chunkSize);
      // Build a VALUES list: ('id1','{"markup":...}'), ...
      const valuesPlaceholders = chunk.map((_, j) => `($${j * 2 + 1}, $${j * 2 + 2}::jsonb)`).join(", ");
      const valuesArgs = chunk.flatMap(({ id, markup, raw: rawField }) => {
        const yandex = {
          ...(rawField.yandex && typeof rawField.yandex === "object" ? rawField.yandex : {}),
          extra: {
            ...(rawField.yandex?.extra && typeof rawField.yandex?.extra === "object" ? rawField.yandex.extra : {}),
            manualMarkup: true,
          },
        };
        return [id, JSON.stringify({ markup, yandex })];
      });
      await prisma.$executeRawUnsafe(
        `UPDATE warehouse_products AS wp
         SET raw = wp.raw || u.patch, updated_at = NOW()
         FROM (VALUES ${valuesPlaceholders}) AS u(pid, patch)
         WHERE wp.id = u.pid`,
        ...valuesArgs,
      );
    }
    // Invalidate in-memory cache so next warehouse read picks up new markups
    invalidateWarehouseViewCache();
    // Queue Yandex repricing in batches to avoid huge BullMQ payload
    const repriceBatch = 500;
    for (let i = 0; i < updates.length; i += repriceBatch) {
      const batchIds = updates.slice(i, i + repriceBatch).map((u) => u.id);
      await queueAuthoritativePriceReprice({
        productIds: batchIds,
        marketplace: "yandex",
        reason: "restore_yandex_markups",
        sourceEvent: "restore_yandex_markups",
        force: true,
        onlyChanged: false,
      }).catch((error) => logger.warn("restore_yandex_markups reprice queue failed", { detail: error?.message || String(error) }));
    }
  }

  const sampleUpdates = updates.slice(0, 10).map(({ offerId, markup, lastPrice, pmRub }) => ({ offerId, markup, lastPrice, pmRub }));

  // Per-tier statistics (PM USD price buckets)
  const tierBoundaries = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 80, 90, 100, 120, 150, 200, 300, 500];
  const tierStats = tierBoundaries.map((minUsd, i) => {
    const maxUsd = tierBoundaries[i + 1] ?? Infinity;
    const inTier = updates.filter((u) => {
      const pmUsd = u.pmRub / usdRate;
      return pmUsd >= minUsd && pmUsd < maxUsd;
    });
    if (!inTier.length) return null;
    const markups = inTier.map((u) => u.markup).sort((a, b) => a - b);
    const avg = markups.reduce((s, m) => s + m, 0) / markups.length;
    const median = markups[Math.floor(markups.length / 2)];
    return {
      minUsd,
      maxUsd: Number.isFinite(maxUsd) ? maxUsd : null,
      count: inTier.length,
      avg: Math.round(avg * 1000) / 1000,
      median: Math.round(median * 1000) / 1000,
    };
  }).filter(Boolean);

  return {
    ok: true,
    dryRun,
    total: productRows.length,
    withLinks: yandexProducts.length,
    updated: updates.length,
    skipped: skipped.length,
    usdRate,
    sampleUpdates,
    sampleSkipped: skipped.slice(0, 5),
    tierStats,
    summary: `${dryRun ? "[DRY RUN] " : ""}Яндекс наценки: обновлено ${updates.length}, пропущено ${skipped.length} из ${yandexProducts.length}.`,
  };
}

async function runLinkedSupplierRecoveryOperation(payload = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const warehouse = await readWarehouse();
  const marketplaceFilter = cleanText(payload?.marketplace || "all").toLowerCase();
  const productIdSet = Array.isArray(payload?.productIds) && payload.productIds.length
    ? new Set(payload.productIds.map((id) => cleanText(id)).filter(Boolean))
    : null;
  const offerIdSet = Array.isArray(payload?.offerIds) && payload.offerIds.length
    ? new Set(payload.offerIds.map((id) => cleanText(id).toLowerCase()).filter(Boolean))
    : null;
  const candidateLimit = productIdSet || offerIdSet ? Math.max(limit, (warehouse.products || []).length) : limit;
  const candidates = linkedRecoveryCandidateProducts(warehouse.products || [], candidateLimit)
    .filter((product) => {
      if (marketplaceFilter !== "all" && cleanText(product.marketplace).toLowerCase() !== marketplaceFilter) return false;
      if (productIdSet && !productIdSet.has(String(product.id))) return false;
      if (offerIdSet && !offerIdSet.has(cleanText(product.offerId).toLowerCase())) return false;
      return true;
    })
    .slice(0, limit);

  if (!candidates.length) {
    return {
      ok: true,
      scanned: Math.min(limit, (warehouse.products || []).length),
      candidates: 0,
      recovered: 0,
      restoredStocks: 0,
      unarchived: 0,
      unarchivePending: 0,
      queuedByDailyLimit: 0,
      queueSize: (await readOzonUnarchiveQueue().catch(() => ({ items: [] }))).items.length || 0,
      errors: [],
      summary: "Нет привязанных карточек, которым нужно восстановление.",
    };
  }

  const rebuilt = [];
  for (const chunk of chunkArray(candidates, 200)) {
    const products = await buildFreshWarehouseProductsFromKnownProducts(
      warehouse,
      chunk,
      {
        refreshPrices: false,
        persistMutations: false,
        livePriceMaster: false,
        batchPriceMaster: false,
      },
    );
    rebuilt.push(...products);
  }

  const ready = rebuilt.filter((product) => product.hasLinks && product.selectedSupplier);
  const forceRecovery = payload.force !== false;
  const needsRecovery = forceRecovery
    ? ready
    : ready.filter((product) => (
        marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true })
        || Boolean(product.noSupplierAutomation?.stockZeroAt)
        || Boolean(product.noSupplierAutomation?.archivedAt)
      ));
  const notReady = candidates.length - ready.length;
  const alreadySellable = Math.max(0, ready.length - needsRecovery.length);
  const result = await runSupplierRecoveryAutomation(
    { products: needsRecovery },
    { productIds: needsRecovery.map((product) => product.id), source: "targeted", force: true },
  );
  const sellableRecovered = Number(result.sellableRecovered || 0);
  const unarchiveFailed = Number(result.unarchiveFailed || 0);
  const unarchivePending = Number(result.unarchivePending || 0);
  const queuedByDailyLimit = Number(result.queuedByDailyLimit || 0);
  const stockFailed = Number(result.stockFailed || 0);
  return {
    ok: result.errors?.length ? false : true,
    partial: Boolean(result.errors?.length && (result.recovered || result.restoredStocks || result.unarchived)),
    scanned: Math.min(limit, (warehouse.products || []).length),
    candidates: candidates.length,
    ready: ready.length,
    notReady,
    alreadySellable,
    needsRecovery: needsRecovery.length,
    recovered: result.recovered || 0,
    sellableRecovered,
    restoredStocks: result.restoredStocks || 0,
    unarchived: result.unarchived || 0,
    unarchivePending,
    queuedByDailyLimit,
    nextRetryAt: result.nextRetryAt || null,
    queueSize: result.queueSize || 0,
    queuedSamples: result.queuedSamples || [],
    unarchiveFailed,
    stockFailed,
    errors: result.errors || [],
    productStatuses: result.productStatuses || [],
    summary: `Проверено ${candidates.length}; с доступным поставщиком ${ready.length}; уже продавались ${alreadySellable}; нужно восстановить ${needsRecovery.length}; полностью восстановлено ${sellableRecovered}; без поставщика ${notReady}; ошибки разархива ${unarchiveFailed}; ошибки остатков ${stockFailed}.`,
  };
}

