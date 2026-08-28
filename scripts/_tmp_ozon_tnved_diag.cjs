#!/usr/bin/env node
"use strict";
const path = require("path");
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const scriptBody = `
process.chdir("/var/www/davidsklad/davidskladik");
async function main() {
  const server = require("./server.js");
  await new Promise(r => setTimeout(r, 4000));
  const { ozonRequest, getOzonAccountByTarget } = server;
  const account = getOzonAccountByTarget("ozon");
  if (!account) { console.log("NO OZON ACCOUNT"); process.exit(1); }

  const listData = await ozonRequest("/v3/product/list", { filter: {}, limit: 100, last_id: "" }, account);
  const ids = (listData.result && listData.result.items || []).slice(0, 50).map(function(x){ return x.product_id; }).filter(Boolean);
  const infoData = await ozonRequest("/v3/product/info/list", { product_id: ids }, account);
  const items = infoData.items || (infoData.result && infoData.result.items) || [];
  const sample = items.slice(0, 5).map(function(p){ return { offerId: p.offer_id, descCatId: p.description_category_id, typeId: p.type_id }; });
  console.log("SAMPLE:", JSON.stringify(sample));

  const zeroType = items.filter(function(p){ return !p.type_id || Number(p.type_id) === 0; }).length;
  const hasType = items.filter(function(p){ return p.type_id && Number(p.type_id) > 0; }).length;
  console.log("typeId=0:", zeroType, "valid:", hasType, "total:", items.length);

  const valid = items.find(function(p){ return p.type_id && Number(p.type_id) > 0 && p.description_category_id; });
  if (valid) {
    console.log("Testing attr: descCatId=" + valid.description_category_id + " typeId=" + valid.type_id);
    try {
      const attrData = await ozonRequest("/v1/description-category/attribute", {
        description_category_id: Number(valid.description_category_id),
        description_type_id: Number(valid.type_id),
        language: "DEFAULT"
      }, account);
      const attrs = attrData.result || [];
      const tnved = attrs.filter(function(a){
        const n = (a.name || "").toLowerCase().replace(/[\\s.,]+/g, "");
        return n.includes("tnved") || n.includes("тнвэд") || n.includes("кодтн") || n.includes("тарифн");
      });
      console.log("Attrs total:", attrs.length, "ТН ВЭД found:", JSON.stringify(tnved.map(function(a){ return {id:a.id, name:a.name}; })));
      if (!tnved.length) console.log("All names:", attrs.slice(0,20).map(function(a){ return a.name; }).join(" | "));
    } catch(e) { console.log("Attr error:", e.message); }
  }
  process.exit(0);
}
main().catch(function(e){ console.error(e.message); process.exit(1); });
`;

const conn = new Client();
conn.on("ready", () => {
  const remote = "/tmp/_ozon_diag2.js";
  // Write script then execute
  conn.exec(`cat > ${remote} <<'ENDHEREDOC'\n${scriptBody}\nENDHEREDOC\nnode ${remote} 2>&1`, (err, stream) => {
    if (err) { console.error(err.message); conn.end(); return; }
    let out = "";
    stream.on("data", d => out += d);
    stream.stderr.on("data", d => out += d);
    stream.on("close", () => { conn.end(); console.log(out); });
  });
}).connect({ host: "81.17.154.153", port: 22, username: "root", password });
conn.on("error", e => { console.error("SSH:", e.message); process.exit(1); });
