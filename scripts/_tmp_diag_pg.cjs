#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_diag_pg_run.js";

const diagScript = `
require("dotenv").config({ path: require("path").resolve(__dirname, "../.env") });
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();
async function main() {
  const trgm = await p.$queryRawUnsafe("SELECT extname FROM pg_extension WHERE extname='pg_trgm'").catch(()=>[]);
  console.log("pg_trgm installed:", trgm.length > 0);
  const dubl = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS n FROM warehouse_products WHERE name ILIKE '%дубль%'").catch(()=>[{n:-1}]);
  console.log("Products with 'дубль':", dubl[0]?.n);
  const rowSize = await p.$queryRawUnsafe("SELECT pg_size_pretty(pg_relation_size('warehouse_products')) AS s, COUNT(*)::int AS n FROM warehouse_products").catch(()=>[{}]);
  console.log("Table size:", rowSize[0]?.s, "rows:", rowSize[0]?.n);
  const explain = await p.$queryRawUnsafe("EXPLAIN (FORMAT TEXT) SELECT COUNT(*) FROM warehouse_products WHERE (marketplace='ozon' OR marketplace='yandex') AND NOT (name ILIKE '%дубль%') AND NOT EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = warehouse_products.id)").catch(()=>[{"QUERY PLAN":"err"}]);
  console.log("Count unlinked plan:");
  explain.forEach(r => console.log(" ", Object.values(r)[0]));
  const explainLinked = await p.$queryRawUnsafe("EXPLAIN (FORMAT TEXT) SELECT COUNT(*) FROM warehouse_products WHERE (marketplace='ozon' OR marketplace='yandex') AND NOT (name ILIKE '%дубль%') AND EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = warehouse_products.id)").catch(()=>[{"QUERY PLAN":"err"}]);
  console.log("Count linked plan:");
  explainLinked.forEach(r => console.log(" ", Object.values(r)[0]));
  await p.$disconnect();
}
main().catch(e=>{ console.error("err:", e.message); process.exit(1); });
`;

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
