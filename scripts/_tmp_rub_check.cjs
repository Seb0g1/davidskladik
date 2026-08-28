#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_rub_check_run.js";

const diagScript = [
  'require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });',
  'const { PrismaClient } = require("@prisma/client");',
  'const p = new PrismaClient();',
  'async function main() {',
  '  // Price history for Inna products today - old vs new prices',
  '  const hist = await p.$queryRawUnsafe(`',
  '    SELECT ph.offer_id, ph.old_price, ph.new_price, ph.status, ph.created_at',
  '    FROM price_history ph',
  '    JOIN product_links pl ON pl.product_id = ph.product_id AND pl.price_currency = \'RUB\'',
  '      AND (lower(pl.supplier_name) LIKE \'%инна%\' OR lower(pl.supplier_name) LIKE \'%inna%\')',
  '    WHERE ph.created_at > now() - interval \'3 hours\'',
  '    ORDER BY ph.created_at DESC',
  '    LIMIT 10',
  '  `).catch(e => { console.error("hist err:", e.message); return []; });',
  '  console.log("=== Price changes for Inna products (last 3h) ===");',
  '  hist.forEach(r => {',
  '    const old = Number(r.old_price||0);',
  '    const nw = Number(r.new_price||0);',
  '    const pct = old ? Math.round((nw-old)/old*100) : "?";',
  '    console.log(" ", r.offer_id, "| old:", old, "-> new:", nw, "| delta:", pct+"%", "| status:", r.status, "| at:", r.created_at);',
  '  });',
  '  console.log("  count:", hist.length);',
  '',
  '  // Overall: avg old vs new price for Inna products changed today',
  '  const avgHist = await p.$queryRawUnsafe(`',
  '    SELECT',
  '      COUNT(*)::int AS n,',
  '      AVG(ph.old_price) AS avg_old,',
  '      AVG(ph.new_price) AS avg_new',
  '    FROM price_history ph',
  '    JOIN product_links pl ON pl.product_id = ph.product_id AND pl.price_currency = \'RUB\'',
  '      AND (lower(pl.supplier_name) LIKE \'%инна%\' OR lower(pl.supplier_name) LIKE \'%inna%\')',
  '    WHERE ph.created_at > now() - interval \'3 hours\'',
  '      AND ph.old_price IS NOT NULL AND ph.old_price > 0',
  '  `).catch(()=>[{}]);',
  '  const a = avgHist[0] || {};',
  '  console.log("\\nTotal price changes:", a.n);',
  '  console.log("Avg old price:", Math.round(Number(a.avg_old||0)));',
  '  console.log("Avg new price:", Math.round(Number(a.avg_new||0)));',
  '  const pctChange = a.avg_old > 0 ? Math.round((a.avg_new - a.avg_old) / a.avg_old * 100) : 0;',
  '  console.log("Avg delta:", pctChange + "%");',
  '',
  '  await p.$disconnect();',
  '}',
  'main().catch(e=>{ console.error("diag error:", e.message); process.exit(1); });',
].join("\n");

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
