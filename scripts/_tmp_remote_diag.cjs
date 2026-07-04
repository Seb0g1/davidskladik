#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_diag_run.js";

const diagScript = [
  'const { execSync } = require("child_process");',
  // Last 60 error/warn lines from PM2
  'try {',
  '  const logs = execSync("pm2 logs --nostream --lines 200 2>&1").toString();',
  '  const lines = logs.split("\\n").filter(l => {',
  '    try {',
  '      if (!l.includes("level")) return false;',
  '      const p = JSON.parse(l.replace(/^[^{]*/,""));',
  '      if (p.level !== "error" && p.level !== "warn") return false;',
  '      // Ozon reviews PermissionDenied: backoff is active, startup burst is non-actionable',
  '      if (p.msg && p.msg.includes("ozon reviews") && String(p.detail||"").includes("PermissionDenied")) return false;',
  '      return true;',
  '    } catch { return false; }',
  '  });',
  '  console.log("=== Server errors/warnings (last 20) ===");',
  '  lines.slice(-20).forEach(l => {',
  '    try {',
  '      const p = JSON.parse(l.replace(/^[^{]*/,""));',
  '      const extra = p.path ? p.path+(p.elapsedMs?" "+p.elapsedMs+"ms":"") : (p.detail ? String(p.detail).slice(0,100) : "");',
  '      console.log("["+p.level+"]", p.msg, extra);',
  '    } catch { console.log(l.slice(0,120)); }',
  '  });',
  '} catch(e) { console.log("pm2 log error:", e.message.slice(0,100)); }',
  '',
  // Health summary
  'require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });',
  'const { PrismaClient } = require("@prisma/client");',
  'const p = new PrismaClient();',
  'async function main() {',
  '  // Sweep heartbeats',
  '  const sweeps = await p.$queryRawUnsafe("SELECT name, status, interval_ms::text AS interval_ms, last_run_at FROM sweep_heartbeats ORDER BY name").catch(()=>[]);',
  '  console.log("\\n=== Sweep heartbeats ===");',
  '  sweeps.forEach(s => { const ageM = s.last_run_at ? Math.round((Date.now()-new Date(s.last_run_at).getTime())/60000) : -1; const intMs = Number(s.interval_ms||0); const stale = intMs > 0 && ageM*60000 > intMs*2.5; console.log(" ", s.name, "| status:", s.status, "| ageMin:", ageM, "| stale:", stale); });',
  '',
  '  // Price queue size',
  '  const priceQ = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS n FROM sales_automation_sku_states WHERE price_status = \'pending\' AND last_calculated_at > now() - interval \'30 minutes\'").then(r=>Number(r?.[0]?.n||0)).catch(()=>-1);',
  '  const priceQAll = await p.salesAutomationSkuState.count({ where: { priceStatus: "pending" } }).catch(()=>-1);',
  '  console.log("\\nPrice queue pending (last 30min):", priceQ, "/ total pending:", priceQAll);',
  '',
  '  // Stale price linked count',
  '  const stale = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS n FROM warehouse_products p WHERE p.archived=false AND p.target_price IS NOT NULL AND p.target_price>0 AND (p.current_price IS NULL OR p.current_price<>p.target_price) AND p.updated_at < now() - interval \'1 hour\' AND (p.raw->>\'priceVerifiedAt\' IS NULL OR (p.raw->>\'priceVerifiedAt\')::timestamptz < now() - interval \'6 hours\') AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id=p.id)").catch(()=>[{n:-1}]);',
  '  console.log("Stale price linked:", stale[0]?.n);',
  '',
  '  // Ozon unarchive queue stats',
  '  const queueStats = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS total, COUNT(*) FILTER(WHERE next_retry_at <= now())::int AS due FROM ozon_unarchive_queue WHERE status!=\'success\'").catch(()=>[{total:-1,due:-1}]);',
  '  console.log("Ozon unarchive queue: total=", queueStats[0]?.total, "due=", queueStats[0]?.due);',
  '',
  '  // Products with errors in lastStockSend or lastArchiveSend (last 24h)',
  '  const stockErrors = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS n FROM warehouse_products WHERE raw->\'lastStockSend\'->>\'status\'=\'error\' AND archived=false AND (raw->\'lastStockSend\'->>\'at\')::timestamptz > now()-interval\'24 hours\'").catch(()=>[{n:-1}]);',
  '  console.log("Recent stock send errors (24h):", stockErrors[0]?.n);',
  '',
  '  // salesAutomationSkuState status breakdown',
  '  const skuStatusBreakdown = await p.$queryRawUnsafe("SELECT price_status, COUNT(*)::int AS n FROM sales_automation_sku_states GROUP BY price_status ORDER BY n DESC LIMIT 10").catch(()=>[]);',
  '  console.log("\\n=== SKU price_status breakdown ===");',
  '  skuStatusBreakdown.forEach(r => console.log(" ", r.price_status, ":", r.n));',
  '',
  '  // Pending older than 1h (stuck jobs)',
  '  const oldPending = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS n FROM sales_automation_sku_states WHERE price_status=\'pending\' AND last_calculated_at < now()-interval\'1 hour\'").then(r=>Number(r?.[0]?.n||0)).catch(()=>-1);',
  '  console.log("Pending > 1h old (stuck):", oldPending);',
  '',
  '  // BullMQ failed jobs',
  '  const bq = await p.$queryRawUnsafe("SELECT queue_name, COUNT(*)::int AS n FROM bullmq_jobs WHERE status=\'failed\' GROUP BY queue_name ORDER BY n DESC LIMIT 5").catch(()=>[]);',
  '  if (bq.length) { console.log("\\n=== BullMQ failed by queue ==="); bq.forEach(r=>console.log(" ", r.queue_name, ":", r.n)); }',
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
