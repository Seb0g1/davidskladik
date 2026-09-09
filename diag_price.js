require("dotenv").config({ path: "/var/www/davidsklad/davidskladik/.env" });
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
const p = new PrismaClient({ log: [] });

async function main() {
  // Find products with multiple links where at least one is selected_row
  const multiLink = await p.$queryRawUnsafe(`
    SELECT
      wp.id,
      wp.raw->>'offerId' AS offer_id,
      wp.marketplace,
      COUNT(pl.id) AS link_count,
      array_agg(pl.raw->>'matchType') AS match_types,
      array_agg(pl.partner_id) AS partner_ids
    FROM warehouse_products wp
    JOIN product_links pl ON pl.product_id = wp.id
    WHERE wp.archived = false
    GROUP BY wp.id
    HAVING COUNT(pl.id) > 1
      AND 'selected_row' = ANY(array_agg(pl.raw->>'matchType'))
    LIMIT 5
  `);
  process.stdout.write("Products with multiple links (incl. selected_row): " + multiLink.length + "\n");
  multiLink.forEach(r => process.stdout.write(
    "  " + r.marketplace + "/" + r.offer_id + " links=" + r.link_count + " types=" + JSON.stringify(r.match_types) + "\n"
  ));

  // Count total
  const total = await p.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM (
      SELECT wp.id
      FROM warehouse_products wp
      JOIN product_links pl ON pl.product_id = wp.id
      WHERE wp.archived = false
      GROUP BY wp.id
      HAVING COUNT(pl.id) > 1
        AND 'selected_row' = ANY(array_agg(pl.raw->>'matchType'))
    ) sub
  `);
  process.stdout.write("Total multi-link+selected_row products: " + total[0].cnt + "\n");

  // Also check: products with selectedSupplier stored in raw
  const withSel = await p.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products
    WHERE archived = false
      AND raw->'selectedSupplier' IS NOT NULL
      AND raw->'selectedSupplier' != 'null'::jsonb
  `);
  process.stdout.write("Products with stored selectedSupplier: " + withSel[0].cnt + "\n");
}

main().catch(e => process.stdout.write("ERR: " + e.message + "\n")).finally(() => p.$disconnect().catch(() => {}));
