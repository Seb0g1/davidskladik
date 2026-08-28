#!/usr/bin/env node
"use strict";
// Finds products where a selected_row (pinned) supplier is active and more expensive
// than a cheaper article-linked supplier for the SAME product.
// Uses article matching (not just partner_id) to avoid false positives.
// Price sanity filter: 3 USD < price < 3000 USD.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

const MIN_USD = 3;
const MAX_USD = 3000;
const MIN_DIFF_USD = 5; // ignore trivial differences

async function main() {
  const prisma = getPrisma();

  const appRow = await prisma.appSetting.findUnique({ where: { key: "app" } });
  const app = appRow?.value || {};
  const rate = Number(app.fixedUsdRate || 85);
  const rules = app.markupRules || [];

  function resolveMarkup(usd, marketplace) {
    const scoped = rules.filter((r) => !r.marketplace || r.marketplace === "all" || r.marketplace === marketplace);
    const sorted = [...scoped].sort((a, b) => b.minUsd - a.minUsd);
    const matched = sorted.find((r) => usd >= Number(r.minUsd || 0));
    return Number(matched?.coefficient || app.defaultMarkups?.[marketplace] || 1.6);
  }

  console.log(`rate=${rate}, markupRules=${rules.length}, priceFilter=${MIN_USD}–${MAX_USD} USD\n`);

  // 1. Active pinned (selected_row) links with their pm price, sanity-filtered
  const pinnedLinks = await prisma.$queryRawUnsafe(`
    SELECT pl.product_id, pl.supplier_name, pl.raw->>'sourceRowId' AS source_row_id,
           pm.price::float AS usd
    FROM product_links pl
    INNER JOIN pm_snapshot_items pm ON pm.row_id = (pl.raw->>'sourceRowId')
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND (pl.raw->>'sourceRowId') IS NOT NULL
      AND pm.active = true
      AND pm.price::float BETWEEN ${MIN_USD} AND ${MAX_USD}
  `);

  // 2. Active article-linked suppliers, matched by partner_id AND article (NativeID),
  //    sanity-filtered. Picks the cheapest row per (product_id, supplier_name).
  const articleLinks = await prisma.$queryRawUnsafe(`
    SELECT pl.product_id, pl.supplier_name, pl.partner_id::text AS partner_id,
           MIN(pm.price::float) AS min_usd
    FROM product_links pl
    INNER JOIN pm_snapshot_items pm
      ON pm.partner_id::text = pl.partner_id::text
      AND pm.article = COALESCE(NULLIF(pl.raw->>'article', ''), pl.supplier_article)
    WHERE pl.raw->>'matchType' = 'article'
      AND pl.partner_id IS NOT NULL
      AND pm.active = true
      AND pm.price::float BETWEEN ${MIN_USD} AND ${MAX_USD}
    GROUP BY pl.product_id, pl.supplier_name, pl.partner_id
  `);

  // 3. Group by product_id
  const byProduct = new Map();
  for (const row of pinnedLinks) {
    const pid = String(row.product_id);
    if (!byProduct.has(pid)) byProduct.set(pid, { pinned: [], article: [] });
    byProduct.get(pid).pinned.push(row);
  }
  for (const row of articleLinks) {
    const pid = String(row.product_id);
    if (!byProduct.has(pid)) byProduct.set(pid, { pinned: [], article: [] });
    byProduct.get(pid).article.push(row);
  }

  // 4. Find products where cheapest pinned > cheapest article (by at least MIN_DIFF_USD)
  const problems = [];
  for (const [productId, { pinned, article }] of byProduct) {
    if (!pinned.length || !article.length) continue;
    const cheapestPinned = pinned.slice().sort((a, b) => a.usd - b.usd)[0];
    const cheaperArticle = article.filter((a) => cheapestPinned.usd - a.min_usd >= MIN_DIFF_USD);
    if (!cheaperArticle.length) continue;
    cheaperArticle.sort((a, b) => a.min_usd - b.min_usd);
    problems.push({ productId, cheapestPinned, cheaperArticle });
  }

  if (!problems.length) {
    console.log("No significant cases found. ✓");
    await prisma.$disconnect();
    return;
  }

  // 5. Load product info
  const ids = [...new Set(problems.map((p) => p.productId))];
  const placeholders = ids.map((id) => `'${id.replace(/[^a-zA-Z0-9_-]/g, "")}'`).join(",");
  const wpRows = await prisma.$queryRawUnsafe(`
    SELECT id::text AS id, offer_id, marketplace, current_price
    FROM warehouse_products WHERE id::text IN (${placeholders})
  `);
  const wpMap = new Map(wpRows.map((r) => [String(r.id), r]));

  // Sort by USD difference descending
  problems.sort((a, b) =>
    (b.cheapestPinned.usd - b.cheaperArticle[0].min_usd) -
    (a.cheapestPinned.usd - a.cheaperArticle[0].min_usd)
  );

  // Deduplicate by offer_id (yandex+ozon show as 2 rows — pick one for display)
  const seenOffers = new Map();
  for (const p of problems) {
    const wp = wpMap.get(p.productId);
    if (!wp) continue;
    const key = `${wp.offer_id}:${p.cheapestPinned.supplier_name}`;
    if (!seenOffers.has(key)) seenOffers.set(key, []);
    seenOffers.get(key).push({ ...p, wp });
  }

  console.log(`Found ${seenOffers.size} unique products (offer+pinned-supplier) with overprice:\n`);
  let n = 0;
  for (const [, entries] of seenOffers) {
    const { wp, cheapestPinned, cheaperArticle } = entries[0];
    const markets = entries.map((e) => wpMap.get(e.productId)?.marketplace).filter(Boolean).join("+");
    const m = resolveMarkup(cheapestPinned.usd, wp.marketplace);
    const mA = resolveMarkup(cheaperArticle[0].min_usd, wp.marketplace);
    const pinnedRub = Math.round(cheapestPinned.usd * rate * m);
    const articleRub = Math.round(cheaperArticle[0].min_usd * rate * mA);
    const diffUsd = (cheapestPinned.usd - cheaperArticle[0].min_usd).toFixed(1);
    const diffRub = pinnedRub - articleRub;

    console.log(`[${++n}] art=${wp.offer_id} [${markets}] currentPrice=${wp.current_price}₽`);
    console.log(`  PINNED:  ${cheapestPinned.supplier_name} ${cheapestPinned.usd} USD → ${pinnedRub}₽`);
    console.log(`  CHEAPER: ${cheaperArticle[0].supplier_name} ${cheaperArticle[0].min_usd} USD → ${articleRub}₽`);
    console.log(`  surplus: +${diffUsd} USD / +${diffRub}₽`);
    if (cheaperArticle.length > 1) {
      const others = cheaperArticle.slice(1, 4).map((a) => `${a.supplier_name} ${a.min_usd}$`).join(", ");
      console.log(`  also cheaper: ${others}${cheaperArticle.length > 4 ? ` (+${cheaperArticle.length - 4} more)` : ""}`);
    }
  }

  console.log(`\nTotal: ${seenOffers.size} unique products affected`);
  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
