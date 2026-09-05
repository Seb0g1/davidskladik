// Sorin express-warehouse sync.
// Products that have ANY link to supplier Сорин are stocked at exactly
// SORIN_EXPRESS_STOCK (default 2) units on:
//   - Ozon  express warehouse SORIN_EXPRESS_OZON_WAREHOUSE_ID  (default 1020005000398404)
//   - Yandex express campaign  SORIN_EXPRESS_YANDEX_CAMPAIGN_ID (default 216697459)
// The sync runs every stock sweep cycle (~3 min) and pushes unconditionally so
// that depletion by a sale is restored on the next tick.

const sorinExpressOzonWarehouseId = cleanText(
  process.env.SORIN_EXPRESS_OZON_WAREHOUSE_ID || "1020005000398404",
);
const sorinExpressYandexCampaignId = cleanText(
  process.env.SORIN_EXPRESS_YANDEX_CAMPAIGN_ID || "216697459",
);
const sorinExpressStock = Math.max(1, Number(process.env.SORIN_EXPRESS_STOCK || 2) || 2);
const sorinExpressSyncEnabled = process.env.SORIN_EXPRESS_SYNC_ENABLED !== "false";

function productHasSorinLink(product = {}) {
  if (!product) return false;
  const sel = product.selectedSupplier?.supplierName || product.selectedSupplier?.partnerName || "";
  if (isSorinSupplierName(sel)) return true;
  const links = Array.isArray(product.links) ? product.links : [];
  return links.some((link) => isSorinSupplierName(link.supplierName || link.partnerName || ""));
}

async function loadSorinLinkedProducts() {
  const prisma = getPrisma();
  if (!prisma) return [];
  // Find product IDs that have at least one Сорин link
  const rows = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT p.id, p.marketplace, p.target, p.offer_id AS "offerId"
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

async function syncSorinExpressStocks() {
  if (!sorinExpressSyncEnabled) return { status: "disabled" };
  if (!shouldUsePostgresStorage()) return { status: "postgres_disabled" };

  const rows = await loadSorinLinkedProducts().catch((error) => {
    logger.warn("sorin_express_sync: load failed", { detail: error?.message || String(error) });
    return [];
  });
  if (!rows.length) return { status: "ok", sorinProducts: 0 };

  const ozonRows = rows.filter((r) => String(r.marketplace).toLowerCase() === "ozon");
  const yandexRows = rows.filter((r) => String(r.marketplace).toLowerCase() === "yandex");

  const results = { ozonSent: 0, ozonFailed: 0, yandexSent: 0, yandexFailed: 0 };

  // ── Ozon ──────────────────────────────────────────────────────────────────
  if (sorinExpressOzonWarehouseId && ozonRows.length) {
    for (const account of getOzonAccounts()) {
      // Match products whose target belongs to this account.
      const accountRows = ozonRows.filter((row) =>
        matchesOzonTarget(String(row.target || "ozon"), account.id),
      );
      if (!accountRows.length) continue;
      const stocks = accountRows.map((row) => ({
        offer_id: String(row.offerId),
        warehouse_id: Number(sorinExpressOzonWarehouseId),
        stock: sorinExpressStock,
      }));
      for (const chunk of chunkArray(stocks, 100)) {
        try {
          await ozonRequest("/v2/products/stocks", { stocks: chunk }, account);
          results.ozonSent += chunk.length;
        } catch (error) {
          results.ozonFailed += chunk.length;
          logger.warn("sorin_express_ozon_stock_failed", {
            account: account.id,
            items: chunk.length,
            detail: error?.message || String(error),
          });
        }
      }
    }
  }

  // ── Yandex ────────────────────────────────────────────────────────────────
  if (sorinExpressYandexCampaignId && yandexRows.length) {
    // Borrow credentials from any active Yandex shop (they share the same business).
    const baseShop = getYandexShops({ includeSyncDisabled: true })[0];
    if (baseShop) {
      const expressShop = {
        ...baseShop,
        id: `yandex-express-${sorinExpressYandexCampaignId}`,
        name: "Яндекс Экспресс",
        campaignId: sorinExpressYandexCampaignId,
      };
      const stockRows = yandexRows.map((row) => ({
        offerId: String(row.offerId),
        stock: sorinExpressStock,
      }));
      for (const chunk of chunkArray(stockRows, 100)) {
        try {
          await sendYandexStockChunk(expressShop, chunk);
          results.yandexSent += chunk.length;
        } catch (error) {
          results.yandexFailed += chunk.length;
          logger.warn("sorin_express_yandex_stock_failed", {
            campaign: sorinExpressYandexCampaignId,
            items: chunk.length,
            detail: error?.message || String(error),
          });
        }
      }
    } else {
      logger.warn("sorin_express_sync: no Yandex shop configured, skipping Yandex express stock");
    }
  }

  logger.info("sorin_express_sync_complete", {
    sorinProducts: rows.length,
    ozonRows: ozonRows.length,
    yandexRows: yandexRows.length,
    expressStock: sorinExpressStock,
    ...results,
  });
  return { status: "ok", sorinProducts: rows.length, ...results };
}
