#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const script = `
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
process.chdir("/var/www/davidsklad/davidskladik");
const p = new PrismaClient();
const markup = 2.2;
const usdRate = 95;

(async () => {
  const where = { archived: false, marketplace: { in: ["ozon","yandex"] }, NOT: { status: "deleted" }, currentPrice: { gt: 0 } };
  const rows = await p.warehouseProduct.findMany({
    where,
    include: { links: { take: 1 } },
    orderBy: { name: "asc" },
    take: 12,
    skip: 0,
  });
  console.log("Fetched:", rows.length);

  const articles = rows.flatMap(r => (r.links||[]).map(l => (l.supplierArticle||"").toString().trim())).filter(Boolean);
  console.log("Articles:", articles.slice(0,5).join(", "));

  const snaps = articles.length ? await p.priceMasterSnapshotItem.findMany({
    where: { article: { in: articles }, active: true },
    select: { article: true, price: true },
  }) : [];
  console.log("PM snaps:", snaps.length);

  const pmMap = new Map();
  for (const s of snaps) {
    const cur = pmMap.get(s.article);
    const price = Number(s.price||0);
    if (!cur || price < Number(cur.price)) pmMap.set(s.article, s);
  }

  const seen = new Set();
  let count = 0;
  for (const r of rows) {
    const key = (r.offerId||"").trim().toLowerCase();
    if (!key || seen.has(key)) { console.log("  SKIP dedup:", r.offerId); continue; }
    seen.add(key);
    count++;

    const link = r.links[0];
    const snap = link ? pmMap.get((link.supplierArticle||"").toString().trim()) : null;
    const priceUsd = snap ? Number(snap.price||0) : 0;
    const cpNum = Number(r.currentPrice||0);
    const priceRub = priceUsd > 0 ? Math.round(priceUsd * usdRate * markup) : cpNum > 0 ? Math.round(cpNum * markup / 100) : 0;
    const name = (r.name||"").trim();
    const pass = priceRub > 0 && name.length > 1;
    console.log(pass ? "PASS" : "FAIL", r.marketplace, r.offerId, "cp="+r.currentPrice, "cpNum="+cpNum, "priceRub="+priceRub, "name.len="+name.length);
    if (count >= 6) break;
  }
})().finally(() => p.$disconnect());
`;

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const w = sftp.createWriteStream("/tmp/_spd2.js");
    w.on("close", () => {
      conn.exec("node /tmp/_spd2.js 2>&1", (err2, stream) => {
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
