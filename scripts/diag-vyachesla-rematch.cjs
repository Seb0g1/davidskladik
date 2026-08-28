#!/usr/bin/env node
"use strict";
// Investigates re-matching Вячесла Колесниченко (partner=100) products.
// His 62 active PM rows have new articles but the warehouse links still use old ones.
// Strategy: match by normalized native_name from pm_snapshot_items vs warehouse product name.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

function normalize(s) {
  return (s || "").toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^a-zа-яё0-9 ]/gi, " ")
    .trim();
}

// Extract key tokens: brand name + volume (e.g. "Lalique L'Insoumis 100ml")
function tokens(s) {
  return new Set(normalize(s).split(" ").filter((t) => t.length >= 3));
}

function tokenOverlap(a, b) {
  const ta = tokens(a);
  const tb = tokens(b);
  let common = 0;
  for (const t of ta) if (tb.has(t)) common++;
  return ta.size + tb.size > 0 ? (2 * common) / (ta.size + tb.size) : 0; // Dice coefficient
}

async function main() {
  const prisma = getPrisma();

  // 1. All active PM rows for Вячесла
  const pmRows = await prisma.$queryRawUnsafe(`
    SELECT row_id, article, native_name, price::float AS price, raw
    FROM pm_snapshot_items
    WHERE partner_id::text = '100' AND active = true
    ORDER BY price ASC
  `);
  console.log(`Вячесла (partner 100) active PM rows: ${pmRows.length}`);
  for (const r of pmRows.slice(0, 5)) {
    console.log(`  rowId=${r.row_id} art=${r.article} name="${r.native_name}" price=$${r.price}`);
  }
  if (pmRows.length > 5) console.log(`  ... (${pmRows.length - 5} more)`);

  // 2. Products linked to Вячесла with NO active PM match (group C)
  const badLinks = await prisma.$queryRawUnsafe(`
    SELECT pl.id AS link_id, pl.product_id, wp.offer_id, wp.marketplace,
           wp.raw->>'name' AS wp_name,
           COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article) AS article
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id::text = pl.product_id
    LEFT JOIN pm_snapshot_items pm
      ON pm.partner_id::text = '100'
      AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
      AND pm.active = true
    WHERE pl.partner_id::text = '100'
      AND pl.raw->>'matchType' = 'article'
      AND pm.row_id IS NULL
    ORDER BY wp.offer_id, wp.marketplace
  `);
  console.log(`\nUnmatched links for Вячесла: ${badLinks.length}\n`);

  // 3. Try name-based matching
  const matches = [];
  const noMatch = [];

  for (const link of badLinks) {
    const wpName = link.wp_name || link.offer_id || "";
    let best = null;
    let bestScore = 0;
    for (const pm of pmRows) {
      const score = tokenOverlap(wpName, pm.native_name || "");
      if (score > bestScore) { bestScore = score; best = pm; }
    }
    if (best && bestScore >= 0.5) {
      matches.push({ link, pm: best, score: bestScore });
    } else {
      noMatch.push({ link, best, bestScore });
    }
  }

  console.log(`=== Name-match results ===`);
  console.log(`  Matched (score ≥ 0.5): ${matches.length}`);
  console.log(`  No match:              ${noMatch.length}\n`);

  // Show matched pairs
  const shownPairs = new Set();
  console.log(`Sample matches:`);
  for (const { link, pm, score } of matches.slice(0, 20)) {
    const key = `${link.offer_id}|${pm.row_id}`;
    if (shownPairs.has(key)) continue;
    shownPairs.add(key);
    console.log(`  [${score.toFixed(2)}] art=${link.offer_id} [${link.marketplace}]`);
    console.log(`    wp_name:  "${(link.wp_name || "").slice(0, 70)}"`);
    console.log(`    pm_name:  "${(pm.native_name || "").slice(0, 70)}" $${pm.price} rowId=${pm.row_id}`);
  }

  console.log(`\nSample no-matches (first 10):`);
  for (const { link, best, bestScore } of noMatch.slice(0, 10)) {
    console.log(`  art=${link.offer_id} [${link.marketplace}] bestScore=${bestScore.toFixed(2)}`);
    console.log(`    wp_name: "${(link.wp_name || "").slice(0, 70)}"`);
    if (best) console.log(`    closest: "${(best.native_name || "").slice(0, 70)}" $${best.price}`);
  }

  // Check unique offer_ids that matched vs not
  const matchedOffers = new Set(matches.map((m) => m.link.offer_id));
  const noMatchOffers = new Set(noMatch.map((m) => m.link.offer_id));
  console.log(`\nUnique offer_ids matched: ${matchedOffers.size}`);
  console.log(`Unique offer_ids not matched: ${noMatchOffers.size}`);

  // Show all PM rows that have NO match candidate (may indicate truly new products)
  const usedPmRows = new Set(matches.map((m) => m.pm.row_id));
  const unusedPmRows = pmRows.filter((r) => !usedPmRows.has(r.row_id));
  console.log(`\nPM rows not matched to any product (${unusedPmRows.length}):`);
  for (const r of unusedPmRows) {
    console.log(`  rowId=${r.row_id} art=${r.article} "${r.native_name}" $${r.price}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
