#!/usr/bin/env node
"use strict";
// Запускать: cd /var/www/davidsklad/davidskladik && node scripts/_tmp_ozon_tnved_diag2.cjs
const path = require("path");
process.chdir(path.join(__dirname, ".."));

async function main() {
  const server = require(path.join(__dirname, "..", "server.js"));
  await new Promise(r => setTimeout(r, 4000));
  const { ozonRequest, getOzonAccountByTarget } = server;
  const account = getOzonAccountByTarget("ozon");
  if (!account) { console.log("NO OZON ACCOUNT"); process.exit(1); }

  // Fetch small sample
  const listData = await ozonRequest("/v3/product/list", { filter: {}, limit: 100, last_id: "" }, account);
  const ids = ((listData.result && listData.result.items) || []).slice(0, 50).map(x => x.product_id).filter(Boolean);
  const infoData = await ozonRequest("/v3/product/info/list", { product_id: ids }, account);
  const items = infoData.items || (infoData.result && infoData.result.items) || [];

  const sample = items.slice(0, 5).map(p => ({
    offerId: p.offer_id,
    descCatId: p.description_category_id,
    typeId: p.type_id,
  }));
  console.log("SAMPLE:", JSON.stringify(sample, null, 2));

  const zeroType = items.filter(p => !p.type_id || Number(p.type_id) === 0).length;
  const hasType  = items.filter(p => p.type_id && Number(p.type_id) > 0).length;
  console.log(`typeId=0: ${zeroType} / valid typeId: ${hasType} / total: ${items.length}`);

  const valid = items.find(p => p.type_id && Number(p.type_id) > 0 && p.description_category_id);
  if (valid) {
    console.log(`\nAttr lookup: descCatId=${valid.description_category_id} typeId=${valid.type_id}`);
    try {
      const attrData = await ozonRequest("/v1/description-category/attribute", {
        description_category_id: Number(valid.description_category_id),
        description_type_id: Number(valid.type_id),
        language: "DEFAULT",
      }, account);
      const attrs = attrData.result || [];
      const tnved = attrs.filter(a => {
        const n = (a.name || "").toLowerCase().replace(/[\s.,]+/g, "");
        return n.includes("tnved") || n.includes("тнвэд") || n.includes("кодтн") || n.includes("тарифн");
      });
      console.log(`Total attrs: ${attrs.length}  ТН ВЭД matches:`, JSON.stringify(tnved.map(a => ({ id: a.id, name: a.name }))));
      if (!tnved.length) {
        console.log("All attr names:", attrs.map(a => a.name).join(" | "));
      }
    } catch (e) { console.log("Attr error:", e.message); }
  } else {
    console.log("No product with valid typeId found in sample");
  }
  process.exit(0);
}
main().catch(e => { console.error(e.message); process.exit(1); });
