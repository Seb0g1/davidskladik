require("dotenv").config({ path: "/var/www/davidsklad/davidskladik/.env" });
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
const prisma = new PrismaClient({ log: [] });

async function main() {
  const staleLinks = await prisma.$queryRawUnsafe(`
    SELECT
      pl.id                           AS link_id,
      pl.product_id,
      pl.supplier_article,
      pl.partner_id,
      COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') AS pinned_row_id,
      pm_new.row_id                  AS new_row_id,
      wp.id                          AS product_id_str,
      wp.marketplace
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    LEFT JOIN pm_snapshot_items pm_old
      ON pm_old.row_id = COALESCE(pl.source_row_id, pl.raw->>'sourceRowId')
      AND pm_old.partner_id::text = pl.partner_id::text
    JOIN LATERAL (
      SELECT pm2.row_id, pm2.price, pm2.doc_date
      FROM pm_snapshot_items pm2
      WHERE pm2.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
        AND pm2.partner_id::text = pl.partner_id::text
        AND pm2.active = true
        AND pm2.price IS NOT NULL AND pm2.price > 0
      ORDER BY pm2.doc_date DESC, pm2.row_id DESC
      LIMIT 1
    ) pm_new ON true
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') IS NOT NULL
      AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') != ''
      AND pm_new.row_id != COALESCE(pl.source_row_id, pl.raw->>'sourceRowId')
      AND (
        pm_old.row_id IS NULL
        OR pm_old.active = false
        OR pm_old.price IS NULL
        OR pm_old.price = 0
      )
    LIMIT 5000
  `);

  process.stdout.write("Remaining stale links: " + staleLinks.length + "\n");
  if (!staleLinks.length) {
    process.stdout.write("All fixed already — running linked reconciler via PM2 signal would refresh stocks.\n");
    process.stdout.write("Product links were already updated. Stocks will refresh on next reconciler cycle.\n");
    return;
  }

  // If there are still stale links (shouldn't happen), fix them
  let fixed = 0;
  for (const row of staleLinks) {
    await prisma.$executeRawUnsafe(
      `UPDATE product_links
       SET raw = jsonb_set(jsonb_set(raw, '{sourceRowId}', $1::jsonb), '{resolvedBy}', '"bulk_stale_recovery"'),
           source_row_id = $3,
           updated_at = now()
       WHERE id = $2`,
      JSON.stringify(row.new_row_id),
      row.link_id,
      row.new_row_id
    );
    fixed++;
  }
  process.stdout.write("Fixed remaining: " + fixed + "\n");
}

main()
  .catch(e => process.stdout.write("ERR: " + e.message + "\n"))
  .finally(() => prisma.$disconnect().catch(() => {}));
