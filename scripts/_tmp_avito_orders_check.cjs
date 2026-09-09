#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");

const password = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;

if (!privateKey && !password) {
  console.error("Either DEPLOY_PASSWORD or SSH key at ~/.ssh/davidsklad_deploy is required");
  process.exit(1);
}

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => resolve(out));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      ...(privateKey ? { privateKey } : { password }),
      readyTimeout: 60000,
    });
  });

  console.log("\n=== Avito Orders API — extended endpoint search ===\n");

  const script = `
const fs = require('fs');
const ACCOUNTS_FILE = '/var/www/davidsklad/davidskladik/data/marketplace-accounts.json';
const BASE = 'https://api.avito.ru';

async function tryEndpoint(tok, url, method) {
  method = method || 'GET';
  const r = await fetch(url, { method, headers: { Authorization: 'Bearer ' + tok }, signal: AbortSignal.timeout(10000) });
  const t = await r.text();
  let preview = t.slice(0, 150);
  try { const j = JSON.parse(t); preview = JSON.stringify(j).slice(0, 150); } catch(e) {}
  console.log(' ', r.status, url.replace(BASE, ''), '->', preview);
}

async function run() {
  const d = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
  const account = (d.accounts || []).find(a => a.marketplace === 'avito' && !a.hidden);

  const tr = await fetch(BASE + '/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'client_credentials', client_id: account.clientId, client_secret: account.apiKey }).toString(),
    signal: AbortSignal.timeout(15000),
  });
  const td = await tr.json();
  const tok = td.access_token;
  const uid = 386426392;
  const since = new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10);

  console.log('--- без userId в пути ---');
  await tryEndpoint(tok, BASE + '/core/v1/orders?updatedAtFrom=' + since);
  await tryEndpoint(tok, BASE + '/v1/orders?updatedAtFrom=' + since);
  await tryEndpoint(tok, BASE + '/delivery/v1/orders?updatedAtFrom=' + since);

  console.log('--- sales/items ---');
  await tryEndpoint(tok, BASE + '/core/v1/accounts/' + uid + '/sales?limit=10');
  await tryEndpoint(tok, BASE + '/core/v1/accounts/' + uid + '/items/stats?');
  await tryEndpoint(tok, BASE + '/core/v1/accounts/' + uid + '/purchases?limit=10');

  console.log('--- доставка ---');
  await tryEndpoint(tok, BASE + '/delivery/v2/orders?updatedAtFrom=' + since + '&limit=10');
  await tryEndpoint(tok, BASE + '/shipping/v1/accounts/' + uid + '/orders?limit=10');

  console.log('--- чаты с контекстом покупки ---');
  const chatR = await fetch(BASE + '/messenger/v2/accounts/' + uid + '/chats?category=u2i&limit=20', {
    headers: { Authorization: 'Bearer ' + tok }, signal: AbortSignal.timeout(10000)
  });
  const chatD = await chatR.json();
  const chats = chatD.chats || [];
  console.log('Total chats:', chats.length);
  const purchaseChats = chats.filter(c => c.context && c.context.type === 'item_purchase' || (c.context && c.context.type === 'u2i'));
  console.log('Purchase context chats:', purchaseChats.length);
  chats.slice(0, 5).forEach(c => {
    console.log(' chat', c.id, 'type:', c.context && c.context.type, 'title:', c.context && c.context.value && c.context.value.title);
  });
}
run().catch(e => console.error('FATAL:', e.message));
`;

  const b64 = Buffer.from(script).toString("base64");
  await exec(conn, `echo ${b64} | base64 -d | node 2>&1`);

  conn.end();
}

main().catch((e) => { console.error("Script error:", e.message); process.exit(1); });
