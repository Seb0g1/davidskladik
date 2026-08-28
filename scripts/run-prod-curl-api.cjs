#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
function exec(conn, command, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("exec timeout")), timeoutMs);
    conn.exec(command, (err, stream) => {
      if (err) { clearTimeout(timer); return reject(err); }
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => { clearTimeout(timer); resolve(out); });
    });
  });
}
async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
  });
  try {
    const auth = Buffer.from("david:CGJ-Ge-90").toString("base64");
    const h = `-H "Authorization: Basic ${auth}"`;

    console.log("\n=== K18001 warehouse product (raw) ===\n");
    await exec(conn, `curl -s ${h} "http://localhost:3000/api/warehouse/products?search=K18001&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(p.get('offerId',''),p.get('marketplace',''),p.get('currentPrice',''),p.get('targetPrice',''),p.get('nextPrice',''),str(p.get('selectedSupplier',{}) or {})[:200]) for p in (d.get('data') or d.get('products') or [])]" 2>&1 || curl -s ${h} "http://localhost:3000/api/warehouse/products?search=K18001&limit=5" 2>&1 | head -c 2000`, 25000);

    console.log("\n=== /api/warehouse/prices/breakdown fields ===\n");
    await exec(conn, `curl -s ${h} "http://localhost:3000/api/warehouse/prices/breakdown?search=K18001&limit=5" 2>&1 | head -c 3000`, 25000);
  } finally { conn.end(); }
}
main().catch((e) => { console.error(e.message); process.exit(1); });
