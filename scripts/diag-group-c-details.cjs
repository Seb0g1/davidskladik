#!/usr/bin/env node
"use strict";
// Shows group C article links with no active PM match — grouped by supplier to see root cause.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Article links with no active PM match, grouped by supplier
  const bySupplier = await prisma.$queryRawUnsafe(`
    SELECT
      pl.supplier_name,
      pl.partner_id::text AS partner_id,
      COUNT(DISTINCT pl.id) AS link_count,
      COUNT(DISTINCT wp.offer_id) AS product_count,
      MAX(pm_active.active_rows) AS max_active_rows_for_partner,
      ARRAY_AGG(DISTINCT COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)) AS articles
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id::text = pl.product_id
    LEFT JOIN pm_snapshot_items pm_match
      ON pm_match.partner_id::text = pl.partner_id::text
      AND pm_match.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
      AND pm_match.active = true
    LEFT JOIN LATERAL (
      SELECT COUNT(*) AS active_rows FROM pm_snapshot_items
      WHERE partner_id::text = pl.partner_id::text AND active = true
    ) pm_active ON true
    WHERE pl.raw->>'matchType' = 'article'
      AND pl.partner_id IS NOT NULL
    GROUP BY pl.supplier_name, pl.partner_id
    HAVING COUNT(DISTINCT pm_match.row_id) = 0
    ORDER BY product_count DESC, link_count DESC
  `);

  console.log(`\n=== Group C: article links with no active PM match (by supplier) ===\n`);
  for (const r of bySupplier) {
    const arts = (r.articles || []).filter(Boolean).slice(0, 5).join(", ");
    const moreArts = (r.articles || []).filter(Boolean).length > 5 ? ` (+${(r.articles || []).filter(Boolean).length - 5} more)` : "";
    console.log(`${r.supplier_name} (partner=${r.partner_id}): ${r.product_count} products, ${r.link_count} links`);
    console.log(`  partner total active PM rows: ${r.max_active_rows_for_partner}`);
    console.log(`  articles in links: ${arts}${moreArts}`);
  }

  // Also check: are any of these articles present in PM but under different partner?
  console.log(`\n=== Cross-check: articles found in PM under different partner? ===`);
  const crossCheck = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT
      pl.supplier_name, pl.partner_id::text AS pl_partner,
      COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article) AS article,
      pm.partner_id::text AS pm_partner, pm.partner_name, pm.price::float AS price
    FROM product_links pl
    JOIN pm_snapshot_items pm
      ON pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
      AND pm.active = true
      AND pm.partner_id::text <> pl.partner_id::text
    LEFT JOIN pm_snapshot_items pm_self
      ON pm_self.partner_id::text = pl.partner_id::text
      AND pm_self.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
      AND pm_self.active = true
    WHERE pl.raw->>'matchType' = 'article'
      AND pl.partner_id IS NOT NULL
      AND pm_self.row_id IS NULL
    ORDER BY pl.supplier_name, article
    LIMIT 30
  `);
  if (!crossCheck.length) {
    console.log("  None found — articles are truly absent from PM.");
  } else {
    for (const r of crossCheck) {
      console.log(`  ${r.supplier_name}(partner=${r.pl_partner}) art="${r.article}" → found in PM under ${r.pm_partner}/${r.pm_partner_name} @ $${r.price}`);
    }
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
