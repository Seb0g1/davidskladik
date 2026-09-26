// Express-warehouse sync (Ozon «Наш склад» warehouse, Yandex «EXPRESS · Наш склад» campaign).
//
// Only products that are physically at hand go to the express warehouses: a product with a
// link to supplier «Сорин» or «Наш склад» whose row is ACTIVE in PriceMaster gets exactly
// SORIN_EXPRESS_STOCK (default 2) units; every other product there gets 0:
//   - Ozon  express warehouse SORIN_EXPRESS_OZON_WAREHOUSE_ID  (default 1020005000398404)
//   - Yandex express campaign  SORIN_EXPRESS_YANDEX_CAMPAIGN_ID (default 149026853)
// syncSorinExpressStocks runs after every stock sweep: it sets 2 / 0 for linked products and,
// at most every EXPRESS_ENFORCE_INTERVAL_MINUTES, reads what the express warehouses actually
// hold and zeroes whatever is not eligible (products whose Сорин link was removed, stock put
// there by hand or by an old sync).

const sorinExpressOzonWarehouseIdEnv = cleanText(
  process.env.SORIN_EXPRESS_OZON_WAREHOUSE_ID || "1020005000398404",
);
// Если задан — используем только этот Ozon-аккаунт для экспресс-склада (остальные игнорируем).
// Нужно когда экспресс-склад принадлежит только одному кабинету.
const sorinExpressOzonAccountId = cleanText(process.env.SORIN_EXPRESS_OZON_ACCOUNT_ID || "");
// Default: 149026853 = «EXPRESS · Наш склад».
const sorinExpressYandexCampaignIdEnv = cleanText(
  process.env.SORIN_EXPRESS_YANDEX_CAMPAIGN_ID || "149026853",
);
// Опциональный отдельный API-ключ для Яндекс Экспресс кампании.
// Если не задан — используем ключ основного магазина из YANDEX_SHOPS_JSON (тот же бизнес).
const sorinExpressYandexApiKey = cleanText(process.env.SORIN_EXPRESS_YANDEX_API_KEY || "");
const sorinExpressStockEnv = Math.max(1, Number(process.env.SORIN_EXPRESS_STOCK || 2) || 2);
const sorinExpressSyncEnabled = process.env.SORIN_EXPRESS_SYNC_ENABLED !== "false";
const expressEnforceIntervalMs = Math.max(
  5 * 60_000,
  Number(process.env.EXPRESS_ENFORCE_INTERVAL_MINUTES || 15) * 60_000 || 15 * 60_000,
);
let expressEnforceLastAt = 0;
let expressEnforceRunning = false;
// Per-campaign FORBIDDEN circuit breaker: after a 403 on Yandex Express, back off for 1 hour
// before retrying — avoids spamming failed API calls per sync cycle.
const sorinYandexForbiddenAt = new Map(); // campaignId -> ms
const SORIN_YANDEX_FORBIDDEN_BACKOFF_MS = 60 * 60 * 1000; // 1 hour

// Suppliers whose goods may be sold from the express warehouses.
function expressSupplierKey(name = "") {
  const normalized = normalizeSupplierName(name);
  if (isSorinSupplierName(name) || normalized.includes("сорин") || normalized.includes("sorin")) return "sorin";
  if (stripSupplierLegalFormPrefix(normalized) === "наш склад") return "our_stock";
  return "";
}

function productHasSorinLink(product = {}) {
  if (!product) return false;
  const sel = product.selectedSupplier?.supplierName || product.selectedSupplier?.partnerName || "";
  if (isSorinSupplierName(sel)) return true;
  const links = Array.isArray(product.links) ? product.links : [];
  return links.some((link) => isSorinSupplierName(link.supplierName || link.partnerName || ""));
}

function expressSyncConfig(runtimeSettings = null) {
  const db = runtimeSettings?.sorinExpress;
  let yandexCampaignId = cleanText(String(db?.yandexCampaignId || sorinExpressYandexCampaignIdEnv || "149026853"));
  // The express campaign must never be a regular shop: the settings once held the main
  // campaign 128820967, so this sync overwrote the main Yandex stock (2 for Sorin goods, 0 for
  // the rest of them) while the real express campaign was never updated.
  const regularCampaigns = new Set(getYandexShops({ includeSyncDisabled: true }).map((shop) => String(shop.campaignId)));
  if (regularCampaigns.has(yandexCampaignId)) {
    const fallback = regularCampaigns.has(sorinExpressYandexCampaignIdEnv) ? "" : sorinExpressYandexCampaignIdEnv;
    logger.warn("express_campaign_is_regular_shop", { configured: yandexCampaignId, using: fallback || null });
    yandexCampaignId = fallback;
  }
  let ozonWarehouseId = cleanText(String(db?.ozonWarehouseId || sorinExpressOzonWarehouseIdEnv));
  // Same guard for Ozon: only the configured express warehouse, never a regular FBS one.
  if (ozonWarehouseId !== sorinExpressOzonWarehouseIdEnv && sorinExpressOzonWarehouseIdEnv) {
    logger.warn("express_warehouse_differs_from_env", { configured: ozonWarehouseId, using: sorinExpressOzonWarehouseIdEnv });
    ozonWarehouseId = sorinExpressOzonWarehouseIdEnv;
  }
  return {
    enabled: db ? db.enabled !== false : sorinExpressSyncEnabled,
    stock: Math.max(1, Number(db?.stock ?? sorinExpressStockEnv) || sorinExpressStockEnv),
    yandexCampaignId,
    ozonWarehouseId,
  };
}

function expressOzonAccounts() {
  return sorinExpressOzonAccountId
    ? getOzonAccounts().filter((a) => a.id === sorinExpressOzonAccountId)
    : getOzonAccounts();
}

// The Yandex express campaign lives in the same business as the main shop; it is not in
// YANDEX_SHOPS_JSON, so the request uses the main shop's business with the express key.
function expressYandexShop(campaignId) {
  const allShops = getYandexShops({ includeSyncDisabled: true });
  const matchedShop = allShops.find((s) => String(s.campaignId) === String(campaignId));
  if (!matchedShop && !sorinExpressYandexApiKey) return null;
  const baseShop = matchedShop || allShops[0];
  if (!baseShop) return null;
  return {
    ...baseShop,
    id: `yandex-express-${campaignId}`,
    name: "Яндекс Экспресс",
    campaignId,
    apiKey: sorinExpressYandexApiKey || baseShop.apiKey,
  };
}

// Товары с привязкой к Сорину или «Нашему складу»: по строке на привязку.
async function loadExpressLinkedProducts() {
  const prisma = getPrisma();
  if (!prisma) return [];
  const rows = await prisma.$queryRawUnsafe(`
    SELECT p.id, p.marketplace, p.target, p.offer_id AS "offerId",
           l.supplier_article AS "supplierArticle", l.supplier_name AS "supplierName"
    FROM warehouse_products p
    JOIN product_links l ON l.product_id = p.id
    WHERE p.archived = false
      AND (
        l.supplier_name ILIKE '%сорин%'
        OR l.supplier_name ILIKE '%sorin%'
        OR l.supplier_name ILIKE '%наш склад%'
      )
  `);
  return (Array.isArray(rows) ? rows : [])
    .map((row) => ({ ...row, supplierKey: expressSupplierKey(row.supplierName) }))
    .filter((row) => row.supplierKey);
}

// Kept for callers that only need the Sorin-linked rows.
async function loadSorinLinkedProducts() {
  return (await loadExpressLinkedProducts()).filter((row) => row.supplierKey === "sorin");
}

// Какие артикулы сейчас активны у Сорина / «Нашего склада» в PriceMaster: Set "key|article".
// null — PM недоступен (тогда ничего не зануляем, чтобы не было ложного нуля).
async function fetchActiveExpressArticlesFromPm(articles) {
  if (!articles.length) return new Set();
  try {
    await discoverOfferDocsActiveColumn();
    const activeDocFilter = offerDocsActiveColumn
      ? ` AND d.${offerDocsActiveColumn}${offerDocsActiveFilterSuffix}`
      : "";
    const active = new Set();
    for (const batch of chunkArray(articles, 500)) {
      const placeholders = batch.map(() => "?").join(", ");
      const [rows] = await pool.query(
        `SELECT DISTINCT p.PartnerName AS partnerName, TRIM(r.NativeID) AS article
         FROM OfferRows r
         JOIN OfferDocs d ON d.DocID = r.DocID
         JOIN Partners p ON p.PartnerID = d.PartnerID
         WHERE BINARY TRIM(r.NativeID) IN (${placeholders})
           AND r.Ignored = 0
           AND r.Active != 0
           AND (p.PartnerName LIKE '%Сорин%' OR p.PartnerName LIKE '%Sorin%' OR p.PartnerName LIKE '%Наш склад%')
           ${activeDocFilter}`,
        batch,
      );
      for (const row of rows) {
        const key = expressSupplierKey(row.partnerName);
        const article = cleanText(String(row.article || ""));
        if (key && article) active.add(`${key}|${article}`);
      }
    }
    return active;
  } catch (error) {
    logger.warn("sorin_express_pm_check_failed", { detail: error?.message || String(error) });
    return null;
  }
}

// Offers that must hold express stock, per marketplace, plus the linked ones that must not.
async function resolveExpressEligibility() {
  const rawRows = await loadExpressLinkedProducts();
  const productMap = new Map();
  for (const row of rawRows) {
    const key = `${row.marketplace}:${String(row.target || "")}:${row.offerId}`;
    if (!productMap.has(key)) {
      productMap.set(key, {
        id: row.id,
        marketplace: String(row.marketplace).toLowerCase(),
        target: row.target,
        offerId: String(row.offerId),
        linkKeys: new Set(),
        hasBareLink: false,
      });
    }
    const article = cleanText(String(row.supplierArticle || ""));
    if (article) productMap.get(key).linkKeys.add(`${row.supplierKey}|${article}`);
    else productMap.get(key).hasBareLink = true;
  }
  const products = [...productMap.values()];
  const allArticles = [...new Set(products.flatMap((p) => [...p.linkKeys].map((k) => k.split("|").slice(1).join("|"))))];
  const activeKeys = await fetchActiveExpressArticlesFromPm(allArticles);
  const pmAvailable = activeKeys !== null;
  const isActive = (p) => !pmAvailable || [...p.linkKeys].some((k) => activeKeys.has(k)) || (p.hasBareLink && !p.linkKeys.size);
  return {
    pmAvailable,
    products,
    active: products.filter(isActive),
    inactive: pmAvailable ? products.filter((p) => !isActive(p)) : [],
  };
}

async function sendOzonExpressStocks(account, warehouseId, rows, results, counterPrefix = "ozon") {
  for (const chunk of chunkArray(rows, 100)) {
    try {
      const response = await ozonRequest("/v2/products/stocks", {
        stocks: chunk.map((r) => ({ offer_id: r.offerId, warehouse_id: Number(warehouseId), stock: r.stock })),
      }, account);
      // Ozon answers 200 and reports refused items one by one (e.g. PRODUCT_IS_NOT_CREATED).
      const refused = (response?.result || []).filter((item) => item.updated === false || (item.errors || []).length);
      const refusedIds = new Set(refused.map((item) => cleanText(item.offer_id)));
      const accepted = chunk.filter((r) => !refusedIds.has(r.offerId));
      const zeroed = accepted.filter((s) => s.stock === 0).length;
      results[`${counterPrefix}Sent`] += accepted.length - zeroed;
      results[`${counterPrefix}Zeroed`] += zeroed;
      results[`${counterPrefix}Failed`] += refused.length;
      if (refused.length) {
        logger.warn("sorin_express_ozon_stock_refused", {
          account: account.id,
          items: refused.slice(0, 10).map((item) => `${item.offer_id}:${(item.errors || []).map((e) => e.code).join(",")}`),
          count: refused.length,
        });
      }
    } catch (error) {
      results[`${counterPrefix}Failed`] += chunk.length;
      logger.warn("sorin_express_ozon_stock_failed", {
        account: account.id,
        items: chunk.length,
        detail: error?.message || String(error),
      });
    }
  }
}

async function sendYandexExpressStocks(campaignId, rows, results) {
  const shop = expressYandexShop(campaignId);
  if (!shop) {
    logger.warn("sorin_express_sync: Yandex Express campaign not in YANDEX_SHOPS_JSON and no SORIN_EXPRESS_YANDEX_API_KEY — skipping Yandex.", { campaign: campaignId });
    results.yandexFailed += rows.length;
    return;
  }
  const forbiddenAt = sorinYandexForbiddenAt.get(campaignId) || 0;
  if (forbiddenAt && Date.now() - forbiddenAt < SORIN_YANDEX_FORBIDDEN_BACKOFF_MS) {
    results.yandexFailed += rows.length;
    return;
  }
  let forbidden = false;
  for (const chunk of chunkArray(rows, 100)) {
    if (forbidden) { results.yandexFailed += chunk.length; continue; }
    try {
      await sendYandexStockChunk(shop, chunk);
      const zeroed = chunk.filter((s) => s.stock === 0).length;
      results.yandexSent += chunk.length - zeroed;
      results.yandexZeroed += zeroed;
      sorinYandexForbiddenAt.delete(campaignId);
    } catch (error) {
      results.yandexFailed += chunk.length;
      const isForbidden = /403|forbidden/i.test(error?.message || "") || error?.statusCode === 403 || error?.status === 403;
      if (isForbidden) {
        forbidden = true;
        sorinYandexForbiddenAt.set(campaignId, Date.now());
        logger.warn("sorin_express_yandex_forbidden_circuit_break", { campaign: campaignId, detail: error?.message || String(error) });
      } else {
        logger.warn("sorin_express_yandex_stock_failed", { campaign: campaignId, items: chunk.length, detail: error?.message || String(error) });
      }
    }
  }
}

async function syncSorinExpressStocks() {
  if (!shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  const config = expressSyncConfig(await readAppSettings().catch(() => null));
  if (!config.enabled) return { status: "disabled" };

  const eligibility = await resolveExpressEligibility().catch((error) => {
    logger.warn("sorin_express_sync: load failed", { detail: error?.message || String(error) });
    return null;
  });
  if (!eligibility) return { status: "error" };
  const { active, inactive, products, pmAvailable } = eligibility;
  const results = { ozonSent: 0, ozonFailed: 0, ozonZeroed: 0, yandexSent: 0, yandexFailed: 0, yandexZeroed: 0 };

  if (config.ozonWarehouseId) {
    for (const account of expressOzonAccounts()) {
      const forAccount = (list) => list.filter((r) => r.marketplace === "ozon" && matchesOzonTarget(String(r.target || "ozon"), account.id));
      const rows = [
        ...forAccount(active).map((r) => ({ offerId: r.offerId, stock: config.stock })),
        ...forAccount(inactive).map((r) => ({ offerId: r.offerId, stock: 0 })),
      ];
      if (rows.length) await sendOzonExpressStocks(account, config.ozonWarehouseId, rows, results);
    }
  }

  if (config.yandexCampaignId) {
    const rows = [
      ...active.filter((r) => r.marketplace === "yandex").map((r) => ({ offerId: r.offerId, stock: config.stock })),
      ...inactive.filter((r) => r.marketplace === "yandex").map((r) => ({ offerId: r.offerId, stock: 0 })),
    ];
    if (rows.length) await sendYandexExpressStocks(config.yandexCampaignId, rows, results);
  }

  logger.info("sorin_express_sync_complete", {
    expressProducts: products.length,
    active: active.length,
    inactive: inactive.length,
    pmAvailable,
    expressStock: config.stock,
    ...results,
  });

  if (pmAvailable && !expressEnforceRunning && Date.now() - expressEnforceLastAt >= expressEnforceIntervalMs) {
    expressEnforceLastAt = Date.now();
    enforceExpressWarehouseStocks({ eligibility, config }).catch((error) =>
      logger.warn("express_enforce_failed", { detail: error?.message || String(error) }));
  }
  return { status: "ok", expressProducts: products.length, active: active.length, inactive: inactive.length, ...results };
}

// Offers currently holding stock on the Yandex express campaign.
async function listYandexExpressStockedOffers(campaignId) {
  const shop = expressYandexShop(campaignId);
  if (!shop) return null;
  const stocked = new Map(); // offerId -> count
  let pageToken = "";
  for (let page = 0; page < 500; page += 1) {
    const query = `limit=200${pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : ""}`;
    const data = await yandexRequest(shop, "POST", `/v2/campaigns/${campaignId}/offers/stocks?${query}`, {});
    for (const warehouse of data?.result?.warehouses || []) {
      for (const offer of warehouse.offers || []) {
        const fit = (offer.stocks || []).filter((s) => s.type === "FIT").reduce((sum, s) => sum + Number(s.count || 0), 0);
        if (fit > 0) stocked.set(cleanText(offer.offerId), fit);
      }
    }
    pageToken = data?.result?.paging?.nextPageToken || "";
    if (!pageToken) break;
  }
  return stocked;
}

// Offers of an Ozon account that hold stock on the express warehouse
// (/v1/product/info/warehouse/stocks lists one FBS warehouse, 1000 per page).
async function listOzonExpressStockedOffers(account, warehouseId) {
  const stocked = new Set();
  let cursor = "";
  for (let page = 0; page < 500; page += 1) {
    const data = await ozonRequest("/v1/product/info/warehouse/stocks", {
      warehouse_id: Number(warehouseId),
      limit: 1000,
      ...(cursor ? { cursor } : {}),
    }, account);
    const stocks = data?.stocks || data?.result?.stocks || [];
    for (const row of stocks) {
      if (Number(row.present || 0) > 0 && row.offer_id) stocked.add(cleanText(row.offer_id));
    }
    cursor = data?.cursor || "";
    if (!data?.has_next || !cursor || !stocks.length) break;
  }
  return stocked;
}

// Zero every offer on the express warehouses that is not eligible (not an active Сорин /
// «Наш склад» product). Reads the real express stock, so it also catches products whose
// link was removed or that were stocked there by hand.
async function enforceExpressWarehouseStocks({ dryRun = false, eligibility = null, config = null } = {}) {
  if (expressEnforceRunning) return { status: "already_running" };
  expressEnforceRunning = true;
  try {
    const cfg = config || expressSyncConfig(await readAppSettings().catch(() => null));
    const elig = eligibility || await resolveExpressEligibility();
    if (!elig.pmAvailable) return { status: "pm_unavailable" };
    const eligible = (marketplace) => new Set(elig.active.filter((p) => p.marketplace === marketplace).map((p) => p.offerId));
    const results = { dryRun, ozonStocked: 0, ozonToZero: 0, ozonSent: 0, ozonZeroed: 0, ozonFailed: 0, yandexStocked: 0, yandexToZero: 0, yandexSent: 0, yandexZeroed: 0, yandexFailed: 0 };
    const samples = { ozon: [], yandex: [] };

    if (cfg.ozonWarehouseId) {
      const ozonEligible = eligible("ozon");
      for (const account of expressOzonAccounts()) {
        const stocked = await listOzonExpressStockedOffers(account, cfg.ozonWarehouseId);
        results.ozonStocked += stocked.size;
        const toZero = [...stocked].filter((offerId) => !ozonEligible.has(offerId));
        results.ozonToZero += toZero.length;
        samples.ozon.push(...toZero.slice(0, 10));
        if (!dryRun && toZero.length) {
          await sendOzonExpressStocks(account, cfg.ozonWarehouseId, toZero.map((offerId) => ({ offerId, stock: 0 })), results);
        }
      }
    }

    if (cfg.yandexCampaignId) {
      const stocked = await listYandexExpressStockedOffers(cfg.yandexCampaignId);
      if (stocked) {
        const yandexEligible = eligible("yandex");
        results.yandexStocked = stocked.size;
        const toZero = [...stocked.keys()].filter((offerId) => !yandexEligible.has(offerId));
        results.yandexToZero = toZero.length;
        samples.yandex.push(...toZero.slice(0, 10));
        if (!dryRun && toZero.length) {
          await sendYandexExpressStocks(cfg.yandexCampaignId, toZero.map((offerId) => ({ offerId, stock: 0 })), results);
        }
      }
    }
    logger.info("express_enforce_complete", { ...results, samples });
    return { status: "ok", ...results, samples };
  } finally {
    expressEnforceRunning = false;
  }
}

// Manual endpoint (/api/ozon/zero-express-stock): same enforcement, on demand.
async function zeroAllNonSorinExpressStock({ dryRun = false } = {}) {
  if (!shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  return enforceExpressWarehouseStocks({ dryRun });
}
