#!/usr/bin/env node
"use strict";
// Fixes selected_row links whose sourceRowId no longer exists in pm_snapshot_items.
// Strategy per link:
//   1. If raw->>'article' is a real NativeID (not equal to sourceRowId):
//      → change matchType to 'article' so it finds the current row dynamically
//   2. If article is empty or equals sourceRowId (no NativeID was stored):
//      → look for the supplier's current active row by partner_id in pm_snapshot_items
//        and set both article + sourceRowId to the best match
//   3. If no current row found at all → skip (log it)

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

const DRY_RUN = process.argv.includes("--dry-run");

async function main() {
  const prisma = getPrisma();
  if (DRY_RUN) console.log("*** DRY RUN ***\n");

  // 1. All selected_row links with MISSING sourceRowId
  const staleLinks = await prisma.$queryRawUnsafe(`
    SELECT pl.id AS link_id, pl.product_id,
           pl.supplier_name, pl.partner_id::text AS partner_id,
           pl.raw->>'sourceRowId' AS source_row_id,
           pl.raw->>'article'     AS raw_article,
           pl.supplier_article    AS supplier_article
    FROM product_links pl
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND (pl.raw->>'sourceRowId') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM pm_snapshot_items pm WHERE pm.row_id = (pl.raw->>'sourceRowId')
      )
  `);

  console.log(`Found ${staleLinks.length} stale selected_row links\n`);

  // 2. For each partner_id in stale links, preload their current active PM rows
  const partnerIds = [...new Set(staleLinks.map((l) => l.partner_id).filter(Boolean))];
  const currentRows = partnerIds.length ? await prisma.$queryRawUnsafe(`
    SELECT row_id, article, partner_id::text AS partner_id, partner_name, price::float AS price, active
    FROM pm_snapshot_items
    WHERE partner_id::text = ANY(ARRAY[${partnerIds.map((p) => `'${p}'`).join(",")}]::text[])
      AND active = true AND price::float > 0
    ORDER BY partner_id, price ASC
  `) : [];

  const rowsByPartner = new Map();
  for (const r of currentRows) {
    const pid = String(r.partner_id);
    if (!rowsByPartner.has(pid)) rowsByPartner.set(pid, []);
    rowsByPartner.get(pid).push(r);
  }

  // 3. Classify each link and plan fix
  const fixes = []; // { linkId, productId, supplierName, action, newArticle, newSourceRowId, newMatchType }
  const skipped = [];

  for (const link of staleLinks) {
    const rawArticle = String(link.raw_article || "").trim();
    const sourceRowId = String(link.source_row_id || "");
    const suppArt = String(link.supplier_article || "").trim();
    // Is it a real NativeID? (not equal to sourceRowId, not empty, not a pure number that looks like a rowId)
    const isRealArticle = rawArticle && rawArticle !== sourceRowId && !/^\d{7,}$/.test(rawArticle);

    if (isRealArticle) {
      // Strategy 1: article already known — just switch matchType
      fixes.push({
        linkId: link.link_id,
        productId: link.product_id,
        supplierName: link.supplier_name,
        action: "article_from_raw",
        newMatchType: "article",
        newArticle: rawArticle,
        newSourceRowId: null, // keep existing but matchType changes
      });
    } else {
      // Strategy 2: look for current active rows for this partner
      const partnerRows = rowsByPartner.get(link.partner_id) || [];
      // Try to match by supplier_article first
      const articleToTry = suppArt || rawArticle;
      let best = partnerRows.find((r) => r.article && articleToTry && r.article === articleToTry);
      if (!best && partnerRows.length === 1) best = partnerRows[0]; // only 1 row — must be it
      if (!best) {
        skipped.push({ ...link, reason: `partner ${link.partner_id} has ${partnerRows.length} current rows, can't auto-pick` });
        continue;
      }
      fixes.push({
        linkId: link.link_id,
        productId: link.product_id,
        supplierName: link.supplier_name,
        action: "new_row_found",
        newMatchType: "selected_row",
        newArticle: best.article || rawArticle,
        newSourceRowId: String(best.row_id),
        bestRowPrice: best.price,
      });
    }
  }

  // 4. Show plan
  const byAction = { article_from_raw: [], new_row_found: [] };
  for (const f of fixes) byAction[f.action].push(f);

  console.log(`Plan:`);
  console.log(`  article_from_raw (switchType→article): ${byAction.article_from_raw.length}`);
  console.log(`  new_row_found (update sourceRowId):    ${byAction.new_row_found.length}`);
  console.log(`  skipped (can't auto-fix):              ${skipped.length}`);

  if (skipped.length) {
    console.log(`\nSkipped (manual review needed):`);
    for (const s of skipped.slice(0, 20)) {
      console.log(`  art=${s.product_id.slice(0, 30)} ${s.supplier_name}: ${s.reason}`);
    }
  }

  // Deduplicated supplier+action view
  const seen = new Set();
  console.log(`\nSample fixes (article_from_raw):`);
  for (const f of byAction.article_from_raw.slice(0, 8)) {
    const k = `${f.supplierName}|${f.newArticle}`;
    if (seen.has(k)) continue; seen.add(k);
    console.log(`  ${f.supplierName}: matchType→article article="${f.newArticle}"`);
  }
  console.log(`\nSample fixes (new_row_found):`);
  for (const f of byAction.new_row_found.slice(0, 8)) {
    console.log(`  ${f.supplierName}: sourceRowId=${f.newSourceRowId} price=${f.bestRowPrice}$`);
  }

  if (DRY_RUN) { console.log("\nDRY RUN — no changes"); await prisma.$disconnect(); return; }

  // 5. Apply fixes
  let updatedLinks = 0;
  let updatedProducts = 0;

  for (const fix of fixes) {
    // 5a. Update product_links.raw
    if (fix.action === "article_from_raw") {
      await prisma.$queryRaw`
        UPDATE product_links
        SET raw = raw || ${{ matchType: "article" }}::jsonb,
            updated_at = NOW()
        WHERE id = ${fix.linkId}
      `;
    } else {
      const patch = { matchType: "selected_row", sourceRowId: fix.newSourceRowId };
      if (fix.newArticle) patch.article = fix.newArticle;
      await prisma.$queryRaw`
        UPDATE product_links
        SET raw = raw || ${patch}::jsonb,
            updated_at = NOW()
        WHERE id = ${fix.linkId}
      `;
    }
    updatedLinks++;

    // 5b. Update warehouse_products.raw.links[]
    const wpRows = await prisma.$queryRaw`
      SELECT id, raw->'links' AS links_json FROM warehouse_products WHERE id = ${fix.productId}
    `;
    if (!wpRows.length) continue;
    const links = wpRows[0].links_json;
    if (!Array.isArray(links)) continue;

    let changed = false;
    const newLinks = links.map((l) => {
      if (l.id !== fix.linkId) return l;
      changed = true;
      if (fix.action === "article_from_raw") {
        return { ...l, matchType: "article" };
      } else {
        return { ...l, matchType: "selected_row", sourceRowId: fix.newSourceRowId, ...(fix.newArticle ? { article: fix.newArticle } : {}) };
      }
    });
    if (changed) {
      const json = JSON.stringify(newLinks);
      await prisma.$queryRaw`
        UPDATE warehouse_products
        SET raw = jsonb_set(raw, '{links}', ${json}::jsonb), updated_at = NOW()
        WHERE id = ${fix.productId}
      `;
      updatedProducts++;
    }
  }

  console.log(`\nDone. product_links updated=${updatedLinks}, warehouse_products updated=${updatedProducts}`);
  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
