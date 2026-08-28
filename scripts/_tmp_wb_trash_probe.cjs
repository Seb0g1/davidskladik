#!/usr/bin/env node
"use strict";

// Read-only: список корзины карточек WB с прода (диагностика дубликатов
// vendorCode при upload). Ничего не восстанавливает и не меняет.
// Использование: DEPLOY_PASSWORD=... node scripts/_tmp_wb_trash_probe.cjs

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const probeSource = `
const fs = require("fs");
const accounts = JSON.parse(fs.readFileSync("/var/www/davidsklad/davidskladik/data/marketplace-accounts.json", "utf8"));
const wb = (accounts.accounts || []).find((a) => a.marketplace === "wb" && a.apiKey);
if (!wb) { console.log("no wb account"); process.exit(0); }
const dupes = new Set(["00005192", "51231322", "pc01860", "5259979", "301469(63)"]);
(async () => {
  const cards = [];
  let cursor = { limit: 100 };
  for (let page = 0; page < 30; page += 1) {
    const r = await fetch("https://content-api.wildberries.ru/content/v2/get/cards/trash?locale=ru", {
      method: "POST",
      headers: { Authorization: wb.apiKey, "Content-Type": "application/json" },
      body: JSON.stringify({ settings: { cursor, filter: { withPhoto: -1 }, sort: { ascending: false } } }),
    });
    if (r.status !== 200) { console.log("trash list status", r.status, (await r.text()).slice(0, 200)); break; }
    const data = await r.json();
    const batch = Array.isArray(data.cards) ? data.cards : [];
    cards.push(...batch);
    if (batch.length < cursor.limit) break;
    cursor = { limit: 100, trashedAt: data.cursor && data.cursor.trashedAt, nmID: data.cursor && data.cursor.nmID };
    if (!cursor.trashedAt || !cursor.nmID) break;
    await new Promise((res) => setTimeout(res, 1500));
  }
  console.log("карточек в корзине:", cards.length);
  for (const card of cards) {
    const code = String(card.vendorCode || "").toLowerCase();
    const mark = dupes.has(code) ? "  <== ДУБЛИКАТ ИЗ ОШИБКИ APPLY" : "";
    console.log("  nmID=" + card.nmID, "vc=" + card.vendorCode, "trashedAt=" + (card.trashedAt || "?"), String(card.title || "").slice(0, 55) + mark);
  }
})();
`;

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => resolve());
    });
  });
}

function sftpPutBuffer(conn, buffer, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      const write = sftp.createWriteStream(remotePath);
      write.on("close", resolve);
      write.on("error", reject);
      write.end(buffer);
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
    await sftpPutBuffer(conn, Buffer.from(probeSource, "utf8"), "/tmp/wb-trash-probe.cjs");
    await exec(conn, "node /tmp/wb-trash-probe.cjs; rm -f /tmp/wb-trash-probe.cjs");
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
