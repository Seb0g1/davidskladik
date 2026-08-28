#!/usr/bin/env node
"use strict";
// For all products where pinned (selected_row) supplier is $30+ more expensive
// than a cheaper article-linked supplier: make the cheaper supplier also selected_row
// so it enters the pinned pool and wins on price.
// Same fix as 20185 / Зураб vs Родина.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

const MIN_USD = 3;
const MAX_USD = 3000;
const minDiffArg = process.argv.find((a) => a.startsWith("--min-diff="));
const MIN_DIFF_USD = minDiffArg ? parseFloat(minDiffArg.split("=")[1]) : 30;
const DRY_RUN = process.argv.includes("--dry-run");

async function main() {
  const prisma = getPrisma();
  if (DRY_RUN) console.log("*** DRY RUN — no changes will be made ***\n");

  // 1. Active pinned (selected_row) links with pm price
  const pinnedLinks = await prisma.$queryRawUnsafe(`
    SELECT pl.id AS link_id, pl.product_id, pl.supplier_name,
           pl.raw->>'sourceRowId' AS source_row_id,
           pm.price::float AS usd
    FROM product_links pl
    INNER JOIN pm_snapshot_items pm ON pm.row_id = (pl.raw->>'sourceRowId')
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND (pl.raw->>'sourceRowId') IS NOT NULL
      AND pm.active = true
      AND pm.price::float BETWEEN ${MIN_USD} AND ${MAX_USD}
  `);

  // 2. Best (cheapest) article-linked row per (product_id, partner_id), with pm row_id
  const articleBest = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT ON (pl.product_id, pl.partner_id)
      pl.id AS link_id, pl.product_id, pl.supplier_name,
      pl.partner_id::text AS partner_id,
      pm.row_id AS pm_row_id, pm.price::float AS usd
    FROM product_links pl
    INNER JOIN pm_snapshot_items pm
      ON pm.partner_id::text = pl.partner_id::text
      AND pm.article = COALESCE(NULLIF(pl.raw->>'article', ''), pl.supplier_article)
    WHERE pl.raw->>'matchType' = 'article'
      AND pl.partner_id IS NOT NULL
      AND pm.active = true
      AND pm.price::float BETWEEN ${MIN_USD} AND ${MAX_USD}
    ORDER BY pl.product_id, pl.partner_id, pm.price ASC
  `);

  // 3. Group by product_id
  const pinnedByProduct = new Map();
  for (const r of pinnedLinks) {
    const pid = String(r.product_id);
    if (!pinnedByProduct.has(pid)) pinnedByProduct.set(pid, []);
    pinnedByProduct.get(pid).push(r);
  }
  const articleByProduct = new Map();
  for (const r of articleBest) {
    const pid = String(r.product_id);
    if (!articleByProduct.has(pid)) articleByProduct.set(pid, []);
    articleByProduct.get(pid).push(r);
  }

  // 4. Find products where cheapestPinned - cheapestArticle >= MIN_DIFF_USD
  // Collect which article links to promote to selected_row
  const toFix = []; // { productId, articleLinkId, pmRowId, articleSupplier, articleUsd, pinnedSupplier, pinnedUsd }
  for (const [productId, pinned] of pinnedByProduct) {
    const articles = articleByProduct.get(productId);
    if (!articles) continue;
    const cheapestPinned = pinned.slice().sort((a, b) => a.usd - b.usd)[0];
    const cheaperArticles = articles.filter((a) => cheapestPinned.usd - a.usd >= MIN_DIFF_USD);
    if (!cheaperArticles.length) continue;
    // Only promote the single cheapest article supplier per product
    const best = cheaperArticles.sort((a, b) => a.usd - b.usd)[0];
    toFix.push({
      productId,
      articleLinkId: String(best.link_id),
      pmRowId: String(best.pm_row_id),
      articleSupplier: best.supplier_name,
      articleUsd: best.usd,
      pinnedSupplier: cheapestPinned.supplier_name,
      pinnedUsd: cheapestPinned.usd,
    });
  }

  if (!toFix.length) {
    console.log("No cases found meeting criteria. ✓");
    await prisma.$disconnect();
    return;
  }

  // Load product info for display
  const ids = [...new Set(toFix.map((f) => f.productId))];
  const placeholders = ids.map((id) => `'${id.replace(/[^a-zA-Z0-9_-]/g, "")}'`).join(",");
  const wpRows = await prisma.$queryRawUnsafe(`
    SELECT id::text AS id, offer_id, marketplace, current_price
    FROM warehouse_products WHERE id::text IN (${placeholders})
  `);
  const wpMap = new Map(wpRows.map((r) => [String(r.id), r]));

  // Group by offer_id to show deduplicated summary
  const byOffer = new Map();
  for (const f of toFix) {
    const wp = wpMap.get(f.productId);
    const key = `${wp?.offer_id}|${f.pinnedSupplier}|${f.articleSupplier}`;
    if (!byOffer.has(key)) byOffer.set(key, { ...f, wp, marketplaces: [] });
    byOffer.get(key).marketplaces.push(wp?.marketplace || "?");
  }

  console.log(`Will fix ${byOffer.size} unique products (${toFix.length} product_links rows):\n`);
  let n = 0;
  for (const [, e] of byOffer) {
    const diff = (e.pinnedUsd - e.articleUsd).toFixed(1);
    console.log(`[${++n}] art=${e.wp?.offer_id} [${e.marketplaces.join("+")}]`);
    console.log(`  PINNED:  ${e.pinnedSupplier} ${e.pinnedUsd} USD`);
    console.log(`  PROMOTE: ${e.articleSupplier} ${e.articleUsd} USD (−$${diff}) → rowId=${e.pmRowId}`);
  }
  console.log();

  if (DRY_RUN) {
    console.log("DRY RUN complete — run without --dry-run to apply changes.");
    await prisma.$disconnect();
    return;
  }

  // 5. Apply fixes
  let updatedLinks = 0;
  let updatedProducts = 0;

  for (const { productId, articleLinkId, pmRowId, articleSupplier } of toFix) {
    // 5a. Update product_links: matchType → selected_row, add sourceRowId
    const currentLink = await prisma.$queryRaw`
      SELECT raw->>'matchType' AS match_type, raw->>'sourceRowId' AS source_row_id
      FROM product_links WHERE id = ${articleLinkId}
    `;
    const alreadyFixed = currentLink[0]?.match_type === "selected_row"
      && currentLink[0]?.source_row_id === pmRowId;
    if (!alreadyFixed) {
      await prisma.$queryRaw`
        UPDATE product_links
        SET raw = raw || ${`{"matchType":"selected_row","sourceRowId":"${pmRowId}"}`}::jsonb,
            updated_at = NOW()
        WHERE id = ${articleLinkId}
      `;
      updatedLinks++;
    }

    // 5b. Update warehouse_products.raw.links[] for this product
    const wpRows2 = await prisma.$queryRaw`
      SELECT id, raw->'links' AS links_json FROM warehouse_products WHERE id = ${productId}
    `;
    if (!wpRows2.length) continue;
    const links = wpRows2[0].links_json;
    if (!Array.isArray(links)) continue;

    let changed = false;
    const newLinks = links.map((l) => {
      if (l.id !== articleLinkId) return l;
      if (l.matchType === "selected_row" && l.sourceRowId === pmRowId) return l;
      changed = true;
      return { ...l, matchType: "selected_row", sourceRowId: pmRowId };
    });

    if (changed) {
      const newLinksJson = JSON.stringify(newLinks);
      await prisma.$queryRaw`
        UPDATE warehouse_products
        SET raw = jsonb_set(raw, '{links}', ${newLinksJson}::jsonb),
            updated_at = NOW()
        WHERE id = ${productId}
      `;
      updatedProducts++;
    }
  }

  console.log(`\nDone.`);
  console.log(`  product_links updated: ${updatedLinks}`);
  console.log(`  warehouse_products updated: ${updatedProducts}`);
  console.log(`\nCheaper suppliers are now selected_row → will win over more expensive pinned suppliers.`);

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
