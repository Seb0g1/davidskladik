#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const script = `
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
process.chdir("/var/www/davidsklad/davidskladik");
const p = new PrismaClient();
Promise.all([
  p.warehouseProduct.count({ where: { currentPrice: { gt: 0 }, archived: false } }),
  p.warehouseProduct.count({ where: { archived: false } }),
  p.warehouseProduct.findMany({
    where: { currentPrice: { gt: 0 }, archived: false },
    select: { offerId: true, brand: true, name: true, currentPrice: true, targetStock: true },
    take: 5,
    orderBy: { currentPrice: "desc" }
  }),
]).then(([priced, total, samples]) => {
  console.log("Total products:", total);
  console.log("With currentPrice > 0:", priced);
  console.log("Samples (highest price):");
  samples.forEach(s => console.log("  " + (s.brand||"") + " | " + (s.name||"").slice(0,40) + " | currentPrice: " + s.currentPrice));
}).finally(() => p.$disconnect());
`;

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const w = sftp.createWriteStream("/tmp/_chkp.js");
    w.on("close", () => {
      conn.exec("node /tmp/_chkp.js", (err2, stream) => {
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
