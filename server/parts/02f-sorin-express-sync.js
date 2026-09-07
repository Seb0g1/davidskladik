// Sorin express-warehouse sync.
// Products that have an ACTIVE link to supplier Сорин in PriceMaster are stocked at exactly
// SORIN_EXPRESS_STOCK (default 2) units regardless of price (Sorin is always priority) on:
//   - Ozon  express warehouse SORIN_EXPRESS_OZON_WAREHOUSE_ID  (default 1020005000398404)
//   - Yandex express campaign  SORIN_EXPRESS_YANDEX_CAMPAIGN_ID (default 216697459)
// Products whose Sorin rows are inactive/stopped in PM receive stock=0 to prevent
// express orders when Sorin cannot fulfil the product.
// The sync runs every stock sweep cycle (~3 min).

const sorinExpressOzonWarehouseId = cleanText(
  process.env.SORIN_EXPRESS_OZON_WAREHOUSE_ID || "1020005000398404",
);
// Если задан — используем только этот Ozon-аккаунт для экспресс-склада (остальные игнорируем).
// Нужно когда экспресс-склад принадлежит только одному кабинету.
const sorinExpressOzonAccountId = cleanText(process.env.SORIN_EXPRESS_OZON_ACCOUNT_ID || "");
const sorinExpressYandexCampaignId = cleanText(
  process.env.SORIN_EXPRESS_YANDEX_CAMPAIGN_ID || "216697459",
);
// Опциональный отдельный API-ключ для Яндекс Экспресс кампании.
// Если не задан — используем ключ первого Яндекс-магазина.
const sorinExpressYandexApiKey = cleanText(process.env.SORIN_EXPRESS_YANDEX_API_KEY || "");
const sorinExpressStock = Math.max(1, Number(process.env.SORIN_EXPRESS_STOCK || 2) || 2);
const sorinExpressSyncEnabled = process.env.SORIN_EXPRESS_SYNC_ENABLED !== "false";

function productHasSorinLink(product = {}) {
  if (!product) return false;
  const sel = product.selectedSupplier?.supplierName || product.selectedSupplier?.partnerName || "";
  if (isSorinSupplierName(sel)) return true;
  const links = Array.isArray(product.links) ? product.links : [];
  return links.some((link) => isSorinSupplierName(link.supplierName || link.partnerName || ""));
}

// Загружает товары с Сорин-привязками, возвращая все supplier_article для каждого.
// Одному товару может соответствовать несколько артикулов (несколько ссылок на PM).
async function loadSorinLinkedProducts() {
  const prisma = getPrisma();
  if (!prisma) return [];
  const rows = await prisma.$queryRawUnsafe(`
    SELECT p.id, p.marketplace, p.target, p.offer_id AS "offerId",
           l.supplier_article AS "supplierArticle"
    FROM warehouse_products p
    JOIN product_links l ON l.product_id = p.id
    WHERE p.archived = false
      AND (
        l.supplier_name ILIKE '%сорин%'
        OR l.supplier_name ILIKE '%sorin%'
      )
  `);
  return Array.isArray(rows) ? rows : [];
}

// Проверяет в PM MySQL какие из переданных артикулов активны у Сорина.
// Возвращает Set активных артикулов, или null если PM недоступен (→ не зануляем, fallback).
async function fetchActiveSorinArticlesFromPm(articles) {
  if (!articles.length) return new Set();
  try {
    await discoverOfferDocsActiveColumn();
    const activeDocFilter = offerDocsActiveColumn
      ? ` AND d.${offerDocsActiveColumn}${offerDocsActiveFilterSuffix}`
      : "";
    const placeholders = articles.map(() => "?").join(", ");
    const [rows] = await pool.query(
      `SELECT DISTINCT BINARY TRIM(r.NativeID) AS article
       FROM OfferRows r
       JOIN OfferDocs d ON d.DocID = r.DocID
       JOIN Partners p ON p.PartnerID = d.PartnerID
       WHERE BINARY TRIM(r.NativeID) IN (${placeholders})
         AND r.Active = 1
         AND (p.PartnerName LIKE '%Сорин%' OR p.PartnerName LIKE '%Sorin%')
         ${activeDocFilter}`,
      articles,
    );
    return new Set(rows.map((r) => cleanText(String(r.article || ""))).filter(Boolean));
  } catch (error) {
    logger.warn("sorin_express_pm_check_failed", { detail: error?.message || String(error) });
    return null; // PM недоступен — не зануляем продукты, чтобы не было ложного нуля
  }
}

async function syncSorinExpressStocks() {
  if (!sorinExpressSyncEnabled) return { status: "disabled" };
  if (!shouldUsePostgresStorage()) return { status: "postgres_disabled" };

  const rawRows = await loadSorinLinkedProducts().catch((error) => {
    logger.warn("sorin_express_sync: load failed", { detail: error?.message || String(error) });
    return [];
  });
  if (!rawRows.length) return { status: "ok", sorinProducts: 0 };

  // Группируем по (marketplace:target:offerId) — собираем все артикулы для каждого товара.
  const productMap = new Map();
  for (const row of rawRows) {
    const key = `${row.marketplace}:${String(row.target || "")}:${row.offerId}`;
    if (!productMap.has(key)) {
      productMap.set(key, {
        id: row.id,
        marketplace: String(row.marketplace).toLowerCase(),
        target: row.target,
        offerId: String(row.offerId),
        articles: new Set(),
      });
    }
    const article = cleanText(String(row.supplierArticle || ""));
    if (article) productMap.get(key).articles.add(article);
  }

  const products = [...productMap.values()];

  // Проверяем в PM какие артикулы активны у Сорина.
  const allArticles = [...new Set(products.flatMap((p) => [...p.articles]))].filter(Boolean);
  const activePmArticles = await fetchActiveSorinArticlesFromPm(allArticles);
  const pmAvailable = activePmArticles !== null;

  // Активные: хотя бы один артикул активен в PM (или PM недоступен → все активны).
  // Неактивные: все артикулы мёртвые → шлём stock=0 чтобы Ozon/Яндекс не принимали заказы.
  const activeProducts = pmAvailable
    ? products.filter((p) => p.articles.size === 0 || [...p.articles].some((a) => activePmArticles.has(a)))
    : products;
  const inactiveProducts = pmAvailable
    ? products.filter((p) => p.articles.size > 0 && ![...p.articles].some((a) => activePmArticles.has(a)))
    : [];

  const ozonActive = activeProducts.filter((p) => p.marketplace === "ozon");
  const ozonInactive = inactiveProducts.filter((p) => p.marketplace === "ozon");
  const yandexActive = activeProducts.filter((p) => p.marketplace === "yandex");
  const yandexInactive = inactiveProducts.filter((p) => p.marketplace === "yandex");

  const results = { ozonSent: 0, ozonFailed: 0, ozonZeroed: 0, yandexSent: 0, yandexFailed: 0, yandexZeroed: 0 };

  // ── Ozon ──────────────────────────────────────────────────────────────────
  if (sorinExpressOzonWarehouseId && (ozonActive.length || ozonInactive.length)) {
    const ozonAccounts = sorinExpressOzonAccountId
      ? getOzonAccounts().filter((a) => a.id === sorinExpressOzonAccountId)
      : getOzonAccounts();
    for (const account of ozonAccounts) {
      const activeForAccount = ozonActive.filter((r) =>
        matchesOzonTarget(String(r.target || "ozon"), account.id),
      );
      const inactiveForAccount = ozonInactive.filter((r) =>
        matchesOzonTarget(String(r.target || "ozon"), account.id),
      );

      const stocksToSend = [
        ...activeForAccount.map((r) => ({
          offer_id: r.offerId,
          warehouse_id: Number(sorinExpressOzonWarehouseId),
          stock: sorinExpressStock,
        })),
        ...inactiveForAccount.map((r) => ({
          offer_id: r.offerId,
          warehouse_id: Number(sorinExpressOzonWarehouseId),
          stock: 0,
        })),
      ];

      for (const chunk of chunkArray(stocksToSend, 100)) {
        try {
          await ozonRequest("/v2/products/stocks", { stocks: chunk }, account);
          const zeroed = chunk.filter((s) => s.stock === 0).length;
          results.ozonSent += chunk.length - zeroed;
          results.ozonZeroed += zeroed;
        } catch (error) {
          results.ozonFailed += chunk.length;
          logger.warn("sorin_express_ozon_stock_failed", {
            account: account.id,
            items: chunk.length,
            detail: error?.message || String(error),
          });
        }
      }

      if (inactiveForAccount.length) {
        logger.info("sorin_express_sync: zeroed inactive products on Ozon", {
          account: account.id,
          count: inactiveForAccount.length,
          offerIds: inactiveForAccount.slice(0, 10).map((r) => r.offerId),
        });
      }
    }
  }

  // ── Yandex ────────────────────────────────────────────────────────────────
  if (sorinExpressYandexCampaignId && (yandexActive.length || yandexInactive.length)) {
    const baseShop = getYandexShops({ includeSyncDisabled: true })[0];
    if (baseShop) {
      const expressShop = {
        ...baseShop,
        id: `yandex-express-${sorinExpressYandexCampaignId}`,
        name: "Яндекс Экспресс",
        campaignId: sorinExpressYandexCampaignId,
        // Если задан отдельный ключ для Экспресс — используем его.
        apiKey: sorinExpressYandexApiKey || baseShop.apiKey,
      };

      const stockRows = [
        ...yandexActive.map((r) => ({ offerId: r.offerId, stock: sorinExpressStock })),
        ...yandexInactive.map((r) => ({ offerId: r.offerId, stock: 0 })),
      ];

      for (const chunk of chunkArray(stockRows, 100)) {
        try {
          await sendYandexStockChunk(expressShop, chunk);
          const zeroed = chunk.filter((s) => s.stock === 0).length;
          results.yandexSent += chunk.length - zeroed;
          results.yandexZeroed += zeroed;
        } catch (error) {
          results.yandexFailed += chunk.length;
          logger.warn("sorin_express_yandex_stock_failed", {
            campaign: sorinExpressYandexCampaignId,
            items: chunk.length,
            detail: error?.message || String(error),
          });
        }
      }

      if (yandexInactive.length) {
        logger.info("sorin_express_sync: zeroed inactive products on Yandex express", {
          count: yandexInactive.length,
          offerIds: yandexInactive.slice(0, 10).map((r) => r.offerId),
        });
      }
    } else {
      logger.warn("sorin_express_sync: no Yandex shop configured, skipping Yandex express stock");
    }
  }

  logger.info("sorin_express_sync_complete", {
    sorinProducts: products.length,
    active: activeProducts.length,
    inactive: inactiveProducts.length,
    pmAvailable,
    expressStock: sorinExpressStock,
    ...results,
  });
  return { status: "ok", sorinProducts: products.length, active: activeProducts.length, inactive: inactiveProducts.length, ...results };
}
