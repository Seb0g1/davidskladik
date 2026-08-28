"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function sshExec(cmd, timeout = 120000) {
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
// Call Ozon API /v1/warehouse/list for the second account
const https = require("https");

// Read accounts file
const fs = require("fs");
const accountsPath = "/var/www/davidsklad/davidskladik/data/marketplace-accounts.json";
const accounts = JSON.parse(fs.readFileSync(accountsPath, "utf8")).accounts || [];
const second = accounts.find(a => a.id === "ozon-3d10ec43");
if (!second) { console.log("Account ozon-3d10ec43 not found"); process.exit(1); }

console.log("Found account:", second.id, "clientId:", second.clientId?.slice(0,4) + "...");

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
  // Get warehouses
  const warehouseResult = await ozonRequest("/v1/warehouse/list", {}, second);
  console.log("\\n=== Warehouses ===");
  const warehouses = warehouseResult.result || warehouseResult.warehouses || warehouseResult.items || [];
  console.log(JSON.stringify(warehouses.slice(0, 20), null, 2));

  // Also check a sample product stock to see what warehouse IDs are in the stock response
  const listResult = await ozonRequest("/v3/product/list", { filter: { visibility: "ALL" }, limit: 5, last_id: "" }, second);
  const items = listResult?.result?.items || [];
  if (items.length) {
    const offerIds = items.map(i => i.offer_id);
    const stockResult = await ozonRequest("/v2/product/list", { filter: { offer_id: offerIds, visibility: "ALL" }, limit: 5 }, second);
    // Use FBS stocks endpoint
    const fbsStockResult = await ozonRequest("/v2/analytics/stock_on_warehouses", { warehouse_type: "ALL", limit: 5, offset: 0 }, second);
    console.log("\\n=== Sample FBS stock (v2/analytics/stock_on_warehouses) ===");
    console.log(JSON.stringify(fbsStockResult?.result?.rows?.slice(0, 3), null, 2));
  }
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

async function main() {
  console.log("Uploading...");
  await sshPut(remoteScript, "/tmp/ozon2-wh.cjs");
  await sshExec("cp /tmp/ozon2-wh.cjs /var/www/davidsklad/davidskladik/ozon2-wh-tmp.cjs");
  console.log("Running...");
  const result = await sshExec("cd /var/www/davidsklad/davidskladik && node ozon2-wh-tmp.cjs; rm -f ozon2-wh-tmp.cjs /tmp/ozon2-wh.cjs", 60000);
  console.log(result);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
