"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function sshExec(cmd, timeout = 60000) {
  return new Promise((resolve, reject) => {
    const client = new Client();
    let output = "";
    const timer = setTimeout(() => { client.end(); reject(new Error("SSH timeout")); }, timeout);
    client.on("ready", () => {
      client.exec(cmd, (err, stream) => {
        if (err) { clearTimeout(timer); client.end(); reject(err); return; }
        stream.on("data", (d) => { output += d; });
        stream.stderr.on("data", (d) => { output += d; });
        stream.on("close", () => { clearTimeout(timer); client.end(); resolve(output); });
      });
    }).on("error", (e) => { clearTimeout(timer); reject(e); })
      .connect({ host: "81.17.154.153", port: 22, username: "root", password });
  });
}

function sshPut(content, remotePath) {
  return new Promise((resolve, reject) => {
    const client = new Client();
    client.on("ready", () => {
      client.sftp((err, sftp) => {
        if (err) { client.end(); reject(err); return; }
        const stream = sftp.createWriteStream(remotePath);
        stream.on("close", () => { client.end(); resolve(); });
        stream.on("error", (e) => { client.end(); reject(e); });
        stream.write(content);
        stream.end();
      });
    }).on("error", reject)
      .connect({ host: "81.17.154.153", port: 22, username: "root", password });
  });
}

const remoteScript = `
"use strict";
const https = require("https");
const fs = require("fs");

const accountsPath = "/var/www/davidsklad/davidskladik/data/marketplace-accounts.json";
const accounts = JSON.parse(fs.readFileSync(accountsPath, "utf8")).accounts || [];
const second = accounts.find(a => a.id === "ozon-3d10ec43");

function ozonRequest(path, body, account) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const options = {
      hostname: "api-seller.ozon.ru",
      path,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data),
        "Client-Id": account.clientId,
        "Api-Key": account.apiKey,
      },
    };
    const req = https.request(options, (res) => {
      let body = "";
      res.on("data", d => body += d);
      res.on("end", () => {
        try { resolve(JSON.parse(body)); } catch(e) { resolve({ raw: body }); }
      });
    });
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

async function main() {
  // Get a few out_of_stock products from DB to test
  const { PrismaClient } = require("@prisma/client");
  const prisma = new PrismaClient();
  const outOfStock = await prisma.$queryRaw\`
    SELECT offer_id AS "offerId", raw->'marketplaceState' AS ms
    FROM warehouse_products
    WHERE target = 'ozon-3d10ec43'
      AND status = 'out_of_stock'
      AND archived = false
    LIMIT 5
  \`;
  await prisma.$disconnect();

  const offerIds = outOfStock.map(r => r.offerId);
  console.log("Testing offerIds:", offerIds);

  // Check stocks for these products
  const stockResult = await ozonRequest("/v1/product/info/stocks", { filter: { offer_id: offerIds, visibility: "ALL" }, limit: 100 }, second);
  console.log("\\n=== FBS/FBO stocks (/v1/product/info/stocks) ===");
  const items = stockResult?.result?.items || [];
  for (const item of items) {
    console.log(JSON.stringify({
      offer_id: item.offer_id,
      fbs: item.stocks?.filter(s => s.type === "fbs") || [],
      fbo: item.stocks?.filter(s => s.type === "fbo") || [],
    }));
  }

  // Also check if sending stock without warehouse_id returns an error or success
  if (offerIds[0]) {
    console.log("\\n=== Attempting stock send WITHOUT warehouse_id for", offerIds[0], "===");
    const sendResult = await ozonRequest("/v2/products/stocks", {
      stocks: [{ offer_id: offerIds[0], stock: 3 }]
    }, second);
    console.log(JSON.stringify(sendResult));

    // Also try with a specific FBO warehouse ID from analytics
    // (this will fail gracefully)
    console.log("\\n=== Analytics warehouse listing ===");
    const analyticsResult = await ozonRequest("/v2/analytics/stock_on_warehouses", { warehouse_type: "FBS", limit: 5, offset: 0 }, second);
    console.log(JSON.stringify(analyticsResult?.result?.rows?.slice(0,3) || analyticsResult));
  }
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

async function main() {
  await sshPut(remoteScript, "/tmp/ozon2-fbs.cjs");
  await sshExec("cp /tmp/ozon2-fbs.cjs /var/www/davidsklad/davidskladik/ozon2-fbs-tmp.cjs");
  const result = await sshExec("cd /var/www/davidsklad/davidskladik && node ozon2-fbs-tmp.cjs; rm -f ozon2-fbs-tmp.cjs /tmp/ozon2-fbs.cjs", 60000);
  console.log(result);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
