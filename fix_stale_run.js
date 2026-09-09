require("dotenv").config({ path: "/var/www/davidsklad/davidskladik/.env" });
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
const prisma = new PrismaClient({ log: [] });

const DRY_RUN = process.argv[2] !== "apply";

async function main() {
  const sql = `
    SELECT
      pl.id                           AS link_id,
      pl.product_id,
      pl.supplier_article,
      pl.partner_id,
      COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') AS pinned_row_id,
      pm_old.price::float             AS old_price,
      pm_old.active                   AS old_active,
      LEFT(pm_old.native_name, 60)    AS old_name,
      pm_new.row_id                   AS new_row_id,
      pm_new.price::float             AS new_price,
      LEFT(pm_new.native_name, 60)    AS new_name,
      wp.raw->>'offerId'              AS offer_id,
      wp.marketplace
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    LEFT JOIN pm_snapshot_items pm_old
      ON pm_old.row_id = COALESCE(pl.source_row_id, pl.raw->>'sourceRowId')
     AND pm_old.partner_id = pl.partner_id
    JOIN LATERAL (
      SELECT pm2.row_id, pm2.price, pm2.native_name
      FROM pm_snapshot_items pm2
      WHERE pm2.article = COALESCE(NULLIF(pl.raw->>'article', ''), pl.supplier_article)
        AND pm2.partner_id = pl.partner_id
        AND pm2.active = true
        AND pm2.price > 0
      ORDER BY pm2.doc_date DESC, pm2.row_id DESC
      LIMIT 1
    ) pm_new ON true
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') IS NOT NULL
      AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') <> ''
      AND pm_new.row_id <> COALESCE(pl.source_row_id, pl.raw->>'sourceRowId')
      AND (
        pm_old.row_id IS NULL
        OR pm_old.active = false
        OR pm_old.price IS NULL
        OR pm_old.price = 0
      )
  `;

  const stale = await prisma.$queryRawUnsafe(sql);
  process.stdout.write("Stale links found: " + stale.length + "\n");

  if (!stale.length) {
    process.stdout.write("Nothing to fix.\n");
    return;
  }

  // Show first 20
  stale.slice(0, 20).forEach(r => {
    process.stdout.write(
      "[" + r.marketplace + "] offerId=" + r.offer_id +
      " art=" + r.supplier_article + " partner=" + r.partner_id + "\n" +
      "  OLD row=" + r.pinned_row_id + " price=" + r.old_price + " active=" + r.old_active +
      " name=" + (r.old_name || "NULL") + "\n" +
      "  NEW row=" + r.new_row_id + " price=" + r.new_price +
      " name=" + (r.new_name || "") + "\n"
    );
  });
  if (stale.length > 20) process.stdout.write("... and " + (stale.length - 20) + " more\n");

  if (DRY_RUN) {
    process.stdout.write("\nDRY RUN — pass 'apply' argument to actually fix.\n");
    return;
  }

  // Apply fixes
  let fixed = 0;
  for (const row of stale) {
    await prisma.$executeRawUnsafe(
      `UPDATE product_links
       SET raw = jsonb_set(jsonb_set(raw, '{sourceRowId}', $1::jsonb), '{resolvedBy}', '"stale_row_id_fix"'),
           source_row_id = $3,
           updated_at = now()
       WHERE id = $2`,
      JSON.stringify(row.new_row_id),
      row.link_id,
      row.new_row_id
    );
    fixed++;
  }

  const productIds = [...new Set(stale.map(r => r.product_id))];
  process.stdout.write("\nFixed: " + fixed + " links across " + productIds.length + " products\n");
  process.stdout.write("Product IDs: " + productIds.join(", ") + "\n");
}

main()
  .catch(e => process.stdout.write("ERR: " + e.message + "\n"))
  .finally(() => prisma.$disconnect().catch(() => {}));
