"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const BRAND = process.argv[2] || "goldwell";

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
const https = require("https");
const fs = require("fs");
const { PrismaClient } = require("@prisma/client");

const BRAND = ${JSON.stringify(BRAND)};

const accountsPath = "/var/www/davidsklad/davidskladik/data/marketplace-accounts.json";
const accounts = JSON.parse(fs.readFileSync(accountsPath, "utf8")).accounts || [];
const second = accounts.find(a => a.id === "ozon-3d10ec43");
if (!second) { console.log("Account ozon-3d10ec43 not found"); process.exit(1); }

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

async function getAllOzonProducts(account) {
  const all = [];
  let lastId = "";
  while (true) {
    const res = await ozonRequest("/v3/product/list", {
      filter: { visibility: "ALL" },
      limit: 1000,
      last_id: lastId,
    }, account);
    const items = res?.result?.items || [];
    all.push(...items);
    const total = res?.result?.total || 0;
    if (!items.length || all.length >= total) break;
    lastId = res?.result?.last_id || "";
    if (!lastId) break;
  }
  return all;
}

async function main() {
  const prisma = new PrismaClient();

  // 1. Products in DB from second account matching brand
  const dbProducts = await prisma.$queryRaw\`
    SELECT id, offer_id AS "offerId", name, target, status, archived,
      brand,
      (SELECT COUNT(*) FROM product_links WHERE product_id = wp.id)::int AS link_count
    FROM warehouse_products wp
    WHERE target = 'ozon-3d10ec43'
      AND archived = false
      AND (
        brand ILIKE ${"'%" + BRAND + "%'"}
        OR name ILIKE ${"'%" + BRAND + "%'"}
      )
    ORDER BY name
  \`;
  console.log("\\n=== В БД (target=ozon-3d10ec43) с брендом " + BRAND + " ===");
  console.log("Найдено:", dbProducts.length);
  for (const p of dbProducts) {
    console.log("  DB:", p.offerId, "|", p.name?.slice(0, 50), "| links:", p.link_count, "| status:", p.status);
  }

  // 2. Products on second Ozon account — get all, then filter by name/brand
  console.log("\\n=== Загружаем все товары со второго Ozon аккаунта... ===");
  const allOzon = await getAllOzonProducts(second);
  console.log("Всего товаров на втором аккаунте Ozon:", allOzon.length);

  // Get detailed info for brand filtering - use product info list
  // First get all offer IDs
  const allOfferIds = allOzon.map(i => i.offer_id).filter(Boolean);

  // Fetch info in chunks of 100 to find brand-matching ones
  const brandProducts = [];
  const chunkSize = 100;
  for (let i = 0; i < allOfferIds.length; i += chunkSize) {
    const chunk = allOfferIds.slice(i, i + chunkSize);
    const infoRes = await ozonRequest("/v3/product/info/list", {
      offer_id: chunk,
    }, second);
    const items = infoRes?.items || infoRes?.result?.items || [];
    for (const item of items) {
      const name = item.name || item.offer_id || "";
      if (name.toLowerCase().includes(BRAND.toLowerCase())) {
        brandProducts.push({
          offerId: item.offer_id,
          name: name.slice(0, 60),
          productId: item.product_id,
          visibility: item.visibility,
          archived: item.is_archived,
        });
      }
    }
  }

  console.log("\\n=== На втором Ozon аккаунте товары с брендом " + BRAND + " ===");
  console.log("Найдено:", brandProducts.length);
  for (const p of brandProducts) {
    console.log("  OZON:", p.offerId, "|", p.name, "| productId:", p.productId, "| vis:", p.visibility, "| archived:", p.archived);
  }

  // 3. Find products on Ozon that are NOT in DB
  const dbOfferIds = new Set(dbProducts.map(p => p.offerId));
  const missingFromDb = brandProducts.filter(p => !dbOfferIds.has(p.offerId));
  console.log("\\n=== Есть на Ozon, но НЕТ в БД ===");
  console.log("Количество:", missingFromDb.length);
  for (const p of missingFromDb) {
    console.log("  MISSING:", p.offerId, "|", p.name, "| archived:", p.archived);
  }

  // 4. Find products in DB that are NOT on Ozon
  const ozonOfferIds = new Set(brandProducts.map(p => p.offerId));
  const missingFromOzon = dbProducts.filter(p => !ozonOfferIds.has(p.offerId));
  console.log("\\n=== Есть в БД, но НЕТ на Ozon ===");
  console.log("Количество:", missingFromOzon.length);
  for (const p of missingFromOzon) {
    console.log("  DB_ONLY:", p.offerId, "|", p.name?.slice(0, 50));
  }

  await prisma.$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

async function main() {
  console.log(`Checking brand "${BRAND}" on second Ozon account vs DB...`);
  await sshPut(remoteScript, "/tmp/ozon2-brand.cjs");
  await sshExec("cp /tmp/ozon2-brand.cjs /var/www/davidsklad/davidskladik/ozon2-brand-tmp.cjs");
  const result = await sshExec("cd /var/www/davidsklad/davidskladik && node ozon2-brand-tmp.cjs; rm -f ozon2-brand-tmp.cjs /tmp/ozon2-brand.cjs", 90000);
  console.log(result);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
