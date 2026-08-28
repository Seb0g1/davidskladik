#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const script = `
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
process.chdir("/var/www/davidsklad/davidskladik");
const p = new PrismaClient();

p.warehouseProduct.findMany({
  where: { currentPrice: { gt: 0 }, archived: false, marketplace: { in: ["ozon","yandex"] } },
  select: { offerId: true, brand: true, name: true, currentPrice: true, marketplace: true },
  take: 20,
  orderBy: { name: "asc" },
}).then(rows => {
  console.log("First 20 products by name:");
  rows.forEach(r => console.log(r.marketplace.padEnd(7), JSON.stringify(r.offerId).padEnd(20), r.currentPrice, "|", (r.name||"").slice(0,40)));

  // Count unique offerIds
  return p.warehouseProduct.findMany({
    where: { currentPrice: { gt: 0 }, archived: false, marketplace: { in: ["ozon","yandex"] } },
    select: { offerId: true },
    take: 1000,
  }).then(all => {
    const uniq = new Set(all.map(r => r.offerId));
    console.log("\\nUnique offerIds in first 1000:", uniq.size, "/ 1000");
  });
}).finally(() => p.$disconnect());
`;

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const w = sftp.createWriteStream("/tmp/_shopdiag.js");
    w.on("close", () => {
      conn.exec("node /tmp/_shopdiag.js", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", d => process.stdout.write(d));
        stream.stderr.on("data", d => process.stderr.write(d));
        stream.on("close", () => conn.end());
      });
    });
    w.end(script);
  });
}).on("error", e => console.error(e)).connect({
  host: "81.17.154.153", username: "root", password: process.env.DEPLOY_PASSWORD, readyTimeout: 60000
});
