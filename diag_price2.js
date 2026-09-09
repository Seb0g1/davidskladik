// Finds products where a cheaper supplier exists but pinned selected_row wins.
// We compare: pm_snapshot price of the pinned selected_row vs cheapest available
// price from OTHER links on the same product.
// Using pm_snapshot_items for batch queries (same DB the reconciler uses).

require("dotenv").config({ path: "/var/www/davidsklad/davidskladik/.env" });
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
const p = new PrismaClient({ log: [] });

async function main() {
  // Get USD rate from app settings
  const settings = await p.$queryRawUnsafe(
    "SELECT raw FROM app_settings LIMIT 1"
  ).catch(() => []);
  const appRaw = settings[0]?.raw || {};
  const usdRate = Number(appRaw.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95);
  process.stdout.write("USD rate: " + usdRate + "\n\n");

  // Get markup rules to approximate final price
  const markupRules = Array.isArray(appRaw.markupRules) ? appRaw.markupRules : [];
  const defaultMarkupOzon = Number(appRaw.defaultMarkups?.ozon || 1.7);
  const defaultMarkupYandex = Number(appRaw.defaultMarkups?.yandex || 1.6);

  function getMarkup(marketplace, usdPrice) {
    const fallback = marketplace === "ozon" ? defaultMarkupOzon : defaultMarkupYandex;
    if (!markupRules.length || !(usdPrice > 0)) return fallback;
    const sorted = [...markupRules].sort((a, b) => b.minUsd - a.minUsd);
    const matched = sorted.find(r => usdPrice >= Number(r.minUsd || 0));
    return Number(matched?.coefficient || fallback);
  }

  function finalPrice(usdPrice, marketplace) {
    const markup = getMarkup(marketplace, usdPrice);
    return Math.round(usdPrice * usdRate * markup);
  }

  // Query: for each product with multiple links, get the pinned selected_row price
  // and the best price from other links, joined to pm_snapshot_items.
  // We want: pinned_price > alternative_price + 50 RUB (final price)
  //
  // For selected_row links: price = pm_snapshot_items.price WHERE row_id = source_row_id
  // For article links: price = MIN(pm_snapshot_items.price) WHERE article = supplier_article AND partner_id = pl.partner_id AND active = true

  const stale = await p.$queryRawUnsafe(`
    WITH link_prices AS (
      SELECT
        pl.product_id,
        pl.id AS link_id,
        pl.raw->>'matchType' AS match_type,
        pl.partner_id,
        COALESCE(pl.raw->>'article', pl.supplier_article) AS article,
        COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') AS pinned_row_id,
        pl.raw->>'supplierName' AS supplier_name,
        wp.marketplace,
        wp.raw->>'offerId' AS offer_id,
        wp.raw->>'name' AS product_name,
        -- For selected_row: get price of the exact pinned row
        pm_pin.price::float AS pinned_price,
        pm_pin.active AS pinned_active,
        pm_pin.native_name AS pinned_name,
        -- For this link: get the cheapest active row for same article+partner
        pm_best.price::float AS best_price,
        pm_best.row_id AS best_row_id,
        pm_best.native_name AS best_name
      FROM product_links pl
      JOIN warehouse_products wp ON wp.id = pl.product_id
      LEFT JOIN pm_snapshot_items pm_pin
        ON pm_pin.row_id = COALESCE(pl.source_row_id, pl.raw->>'sourceRowId')
        AND pl.raw->>'matchType' = 'selected_row'
      LEFT JOIN LATERAL (
        SELECT pm2.price, pm2.row_id, pm2.native_name
        FROM pm_snapshot_items pm2
        WHERE pm2.article = COALESCE(NULLIF(pl.raw->>'article', ''), pl.supplier_article)
          AND pm2.partner_id = pl.partner_id
          AND pm2.active = true
          AND pm2.price > 0
        ORDER BY pm2.price ASC
        LIMIT 1
      ) pm_best ON true
      WHERE wp.archived = false
    ),
    product_pinned AS (
      -- Products that have at least one active pinned selected_row
      SELECT DISTINCT product_id
      FROM link_prices
      WHERE match_type = 'selected_row'
        AND pinned_active = true
        AND pinned_price > 0
    ),
    product_with_pinned AS (
      SELECT lp.*
      FROM link_prices lp
      JOIN product_pinned pp ON pp.product_id = lp.product_id
    )
    -- For each product: compare cheapest pinned price vs cheapest alternative price
    SELECT
      product_id,
      offer_id,
      marketplace,
      LEFT(product_name, 60) AS product_name,
      MIN(CASE WHEN match_type = 'selected_row' AND pinned_active = true AND pinned_price > 0
              THEN pinned_price END) AS cheapest_pinned_usd,
      MIN(CASE WHEN match_type != 'selected_row' AND best_price > 0
              THEN best_price
              WHEN match_type = 'selected_row' AND best_price > 0 AND best_price < pinned_price
              THEN best_price END) AS cheapest_alt_usd,
      (SELECT pinned_name FROM product_with_pinned lp2
       WHERE lp2.product_id = product_with_pinned.product_id
         AND match_type = 'selected_row' AND pinned_active = true AND pinned_price > 0
       ORDER BY pinned_price ASC LIMIT 1) AS pinned_supplier_name,
      (SELECT supplier_name FROM product_with_pinned lp3
       WHERE lp3.product_id = product_with_pinned.product_id
         AND match_type != 'selected_row' AND best_price > 0
       ORDER BY best_price ASC LIMIT 1) AS alt_supplier_name
    FROM product_with_pinned
    GROUP BY product_id, offer_id, marketplace, product_name
    HAVING
      MIN(CASE WHEN match_type = 'selected_row' AND pinned_active = true AND pinned_price > 0
              THEN pinned_price END) IS NOT NULL
      AND MIN(CASE WHEN match_type != 'selected_row' AND best_price > 0
              THEN best_price
              WHEN match_type = 'selected_row' AND best_price > 0 AND best_price < pinned_price
              THEN best_price END) IS NOT NULL
    ORDER BY
      (MIN(CASE WHEN match_type = 'selected_row' AND pinned_active = true AND pinned_price > 0 THEN pinned_price END)
       - MIN(CASE WHEN match_type != 'selected_row' AND best_price > 0 THEN best_price
             WHEN match_type = 'selected_row' AND best_price > 0 AND best_price < pinned_price THEN best_price END)) DESC
    LIMIT 200
  `);

  process.stdout.write("Candidates with multiple links (selected_row + other): " + stale.length + "\n\n");

  // Apply final price comparison with markup
  const threshold = 50; // RUB
  const affected = [];
  for (const row of stale) {
    const pinnedFinal = finalPrice(Number(row.cheapest_pinned_usd), row.marketplace);
    const altFinal = finalPrice(Number(row.cheapest_alt_usd), row.marketplace);
    const diff = pinnedFinal - altFinal;
    if (diff > threshold) {
      affected.push({ ...row, pinnedFinal, altFinal, diff });
    }
  }

  process.stdout.write("Affected (cheaper alt suppressed by pinned restriction): " + affected.length + "\n\n");
  affected.slice(0, 30).forEach(r => {
    process.stdout.write(
      r.marketplace + "/" + r.offer_id + "\n" +
      "  Selected (pinned): " + r.pinnedFinal + " RUB  [" + (r.pinned_supplier_name || "?").slice(0, 50) + "]\n" +
      "  Cheaper alt:       " + r.altFinal + " RUB  [" + (r.alt_supplier_name || "?").slice(0, 50) + "]\n" +
      "  Diff: -" + r.diff + " RUB\n"
    );
  });
}

main().catch(e => process.stdout.write("ERR: " + e.message + "\n")).finally(() => p.$disconnect().catch(() => {}));
