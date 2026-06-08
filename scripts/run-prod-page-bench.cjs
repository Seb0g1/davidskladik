#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve(out)));
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
      "curl -s -o /dev/null -w 'grouped_default:%{time_total}s\\n' 'http://127.0.0.1/api/warehouse/products/page?page=1&pageSize=40&grouped=true'",
      "curl -s -o /dev/null -w 'search_vilhelm:%{time_total}s\\n' 'http://127.0.0.1/api/warehouse/products/page?page=1&pageSize=40&q=VILHELM&grouped=true'",
      "cd /var/www/davidsklad/davidskladik && node -e \"const http=require('http');const u='/api/warehouse/products/page?page=1&pageSize=40&q=VILHELM&grouped=true';http.get('http://127.0.0.1'+u,r=>{let s='';r.on('data',d=>s+=d);r.on('end',()=>{const j=JSON.parse(s);console.log(JSON.stringify({items:j.items?.length,total:j.total,partial:j.partial,sourceError:j.sourceError},null,2));});}).on('error',e=>console.error(e));\"",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
