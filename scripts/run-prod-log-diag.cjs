#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve()));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 60000,
    });
  });
  try {
    await exec(conn, [
      "echo '=== TODAY ERRORS ==='",
      "grep '2026-06-07' /root/.pm2/logs/davidsklad-error-0.log | tail -n 80",
      "echo '=== DB COUNTS ==='",
      `cd /var/www/davidsklad/davidskladik && node -e "
        require('dotenv').config();
        const {PrismaClient}=require('@prisma/client');
        const p=new PrismaClient();
        (async()=>{
          const [products, links, orphanStates]=await Promise.all([
            p.warehouseProduct.count(),
            p.productLink.count(),
            p.\\\$queryRaw\\\`SELECT COUNT(*)::int AS cnt FROM sales_automation_sku_states s WHERE NOT EXISTS (SELECT 1 FROM warehouse_products w WHERE w.id = s.product_id)\\\`,
          ]);
          console.log(JSON.stringify({products, links, orphanSalesAutomationStates: orphanStates[0]?.cnt}, null, 2));
          await p.\\\$disconnect();
        })().catch(e=>{console.error(e);process.exit(1);});
      "`,
      "echo '=== CURL CATALOG ==='",
      "curl -sS -m 45 -o /tmp/page.json -w 'http:%{http_code} time:%{time_total}\\n' 'http://127.0.0.1:3000/api/warehouse/products/page?page=1&pageSize=40'",
      "node -e \"const j=require('/tmp/page.json'); console.log(JSON.stringify({items:j.items?.length,total:j.total,partial:j.partial,sourceError:j.sourceError,stale:j.stale},null,2));\"",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
