#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve()));
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
    await exec(conn, [
      "sleep 3",
      "curl -sS -m 60 'http://127.0.0.1:3000/api/warehouse/products/page?page=1&pageSize=5' | head -c 2000",
      "echo",
      "curl -sS -m 60 -o /tmp/wh-page.json -w '%{http_code}' 'http://127.0.0.1:3000/api/warehouse/products/page?page=1&pageSize=40'",
      "echo",
      "node -e \"const j=require('/tmp/wh-page.json'); console.log(JSON.stringify({items:j.items?.length,total:j.total,partial:j.partial,source:j.source,first:j.items?.[0]?.id},null,2));\"",
      "pm2 logs davidsklad --lines 15 --nostream",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
