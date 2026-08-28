#!/usr/bin/env node
"use strict";
// Добор фото карточек Яндекса: находит через offer-cards карточки с ошибками
// изображений («Нет изображения», «Изображение не по правилам») и прогоняет по
// ним repairWeakYandexCardsFromOzonPostgres (фото от Ozon-двойника, при
// необходимости — свежие с Ozon API, с пушем карточки в Яндекс).
// Без --confirmed — только список и dry-run ремонта.
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const confirmed = process.argv.includes("--confirmed");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_yandex_photo_fix.js";

const runScript = [
  'const path = require("node:path");',
  'require("dotenv").config({ path: path.resolve(__dirname, "../.env") });',
  'const confirmed = ' + JSON.stringify(confirmed) + ';',
  'async function main() {',
  '  const { PrismaClient } = require("@prisma/client");',
  '  const server = require(path.resolve(__dirname, "../server.js"));',
  '  const { getYandexOfferCardsContentStatus, repairWeakYandexCardsFromOzonPostgres, yandexStockShops } = server;',
  '  const seenBusiness = new Set();',
  '  const shops = yandexStockShops().filter((shop) => {',
  '    if (!shop.apiKey || !shop.businessId || seenBusiness.has(String(shop.businessId))) return false;',
  '    seenBusiness.add(String(shop.businessId));',
  '    return true;',
  '  });',
  '  if (!shops.length) throw new Error("no yandex shops configured");',
  '  const prisma = new PrismaClient();',
  '  try {',
  '    const rows = await prisma.warehouseProduct.findMany({',
  '      where: { marketplace: "yandex" },',
  '      select: { offerId: true },',
  '    });',
  '    const offerIds = [...new Set(rows.map((row) => String(row.offerId || "").trim()).filter(Boolean))];',
  '    console.log("yandex offers in PG:", offerIds.length);',
  '    const offenders = new Map();',
  '    for (const shop of shops) {',
  '      const cards = await getYandexOfferCardsContentStatus(shop, offerIds);',
  '      for (const card of cards) {',
  '        const imageErrors = (card.errors || []).filter((error) =>',
  '          /изображен/i.test(String(error.message || error.comment || error.type || "")));',
  '        if (imageErrors.length) {',
  '          offenders.set(card.offerId, imageErrors.map((error) => error.message || error.comment || error.type));',
  '        }',
  '      }',
  '    }',
  '    console.log("cards with image ERRORS:", offenders.size);',
  '    for (const [offerId, errors] of offenders) console.log(" -", offerId, "|", errors.join("; "));',
  '    if (!offenders.size) return;',
  '    const result = await repairWeakYandexCardsFromOzonPostgres(prisma, {',
  '      dryRun: !confirmed,',
  '      pushToYandex: confirmed,',
  '      offerIds: [...offenders.keys()],',
  '      batchSize: 50,',
  '    });',
  '    console.log("repair " + (confirmed ? "(CONFIRMED)" : "(dry run)") + ":");',
  '    console.log(JSON.stringify(result, null, 2));',
  '  } finally {',
  '    await prisma.$disconnect();',
  '  }',
  '}',
  'main().then(() => process.exit(0)).catch((e) => { console.error("PHOTO_FIX_FAILED:", e.stack || e.message); process.exit(1); });',
].join("\n");

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const ws = sftp.createWriteStream(remoteScript);
    ws.on("close", () => {
      conn.exec("cd " + remoteRoot + " && node " + remoteScript + " 2>&1", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", (d) => process.stdout.write(d));
        stream.stderr.on("data", (d) => process.stderr.write(d));
        stream.on("close", () => { conn.exec("rm -f " + remoteScript, () => conn.end()); });
      });
    });
    ws.end(runScript);
  });
}).connect({ host: "81.17.154.153", port: 22, username: "root", password, readyTimeout: 60000 });
