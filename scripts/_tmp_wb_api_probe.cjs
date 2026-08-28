#!/usr/bin/env node
"use strict";

// Read-only: замер WB feedbacks/chat API с прода боевым токеном кабинета
// (диагностика 504 на /api/questions и /api/chats?marketplace=wb).
// Токен не печатается. Использование: DEPLOY_PASSWORD=... node scripts/_tmp_wb_api_probe.cjs

const fs = require("node:fs");
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
console.log("accounts by marketplace:", JSON.stringify((accounts.accounts || []).map((a) => a.marketplace + (a.hidden ? ":hidden" : "") + (a.syncEnabled === false ? ":syncOff" : ""))));
console.log("wb account:", wb.id, "token len", wb.apiKey.length);
async function probe(name, url) {
  const t0 = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 45000);
  try {
    const r = await fetch(url, { headers: { Authorization: wb.apiKey }, signal: controller.signal });
    const text = await r.text();
    console.log(name + ":", r.status, (Date.now() - t0) + "ms", "retry=" + (r.headers.get("x-ratelimit-retry") || "-"), text.slice(0, 200).replace(/\\n/g, " "));
  } catch (e) {
    console.log(name + ": FAIL", (Date.now() - t0) + "ms", e.name, e.message);
  } finally { clearTimeout(timer); }
}
(async () => {
  await probe("questions", "https://feedbacks-api.wildberries.ru/api/v1/questions?isAnswered=false&take=30&skip=0&order=dateDesc");
  await probe("questions-count", "https://feedbacks-api.wildberries.ru/api/v1/questions/count-unanswered");
  await probe("chats-list", "https://buyer-chat-api.wildberries.ru/api/v1/seller/chats");
  await probe("events", "https://buyer-chat-api.wildberries.ru/api/v1/seller/events");
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
    await sftpPutBuffer(conn, Buffer.from(probeSource, "utf8"), "/tmp/wb-api-probe.cjs");
    await exec(conn, "node /tmp/wb-api-probe.cjs; rm -f /tmp/wb-api-probe.cjs");
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
