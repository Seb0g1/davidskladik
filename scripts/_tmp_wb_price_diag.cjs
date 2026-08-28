#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_wb_price_diag_run.js";

const diagScript = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();
async function main() {
  const row = await p.appSetting.findUnique({ where: { key: "app" } });
  const settings = row?.value || {};

  const usdRate = settings.fixedUsdRate ? Number(settings.fixedUsdRate) : null;
  const effectiveRate = usdRate || Number(process.env.DEFAULT_USD_RATE || 95);
  console.log("=== USD rate:", usdRate ? ("fixed=" + usdRate) : ("env=" + effectiveRate));
  console.log("=== defaultMarkups:", JSON.stringify(settings.defaultMarkups || {}));

  const rules = Array.isArray(settings.markupRules) ? settings.markupRules : [];
  console.log("\\n=== Все правила наценки (" + rules.length + " шт.) ===");
  rules.forEach(r => console.log("  marketplace=" + (r.marketplace||'?') + " minUsd=" + r.minUsd + " coefficient=" + r.coefficient));

  const wbRules = rules.filter(r => !r.marketplace || r.marketplace === 'all' || r.marketplace === 'wb');
  console.log("\\n=== Применимые к WB (" + wbRules.length + " шт.) ===");
  wbRules.forEach(r => console.log("  marketplace=" + (r.marketplace||'?') + " minUsd=" + r.minUsd + " coefficient=" + r.coefficient));

  // Расчёт для разных usd-цен поставщика
  const defaults = settings.defaultMarkups || {};
  const fallbackWb = Number(defaults.wb || process.env.DEFAULT_WB_MARKUP || 1.6);
  const sorted = [...wbRules].sort((a, b) => b.minUsd - a.minUsd);

  for (const usd of [5, 10, 20, 30, 50, 100]) {
    const matched = sorted.find(r => usd >= Number(r.minUsd || 0));
    const coeff = Number(matched?.coefficient || fallbackWb);
    const purchaseRub = Math.round(usd * effectiveRate);
    const priceRub = Math.round(purchaseRub * coeff);
    const maxRub = 20000;
    const sellable = priceRub <= maxRub;
    console.log(\`  $\${usd} → купить \${purchaseRub}₽ × \${coeff} = \${priceRub}₽\${sellable ? '' : ' ← ВЫШЕ 20000, не продаётся'}\`);
  }

  await p.$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const ws = sftp.createWriteStream(remoteScript);
    ws.on("close", () => {
      conn.exec("cd " + remoteRoot + " && node " + remoteScript + " 2>&1", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", d => process.stdout.write(d));
        stream.stderr.on("data", d => process.stderr.write(d));
        stream.on("close", () => { conn.exec("rm -f " + remoteScript, () => conn.end()); });
      });
    });
    ws.end(diagScript);
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
