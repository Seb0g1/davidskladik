#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";

const conn = new Client();
conn.on("ready", () => {
  // pm2 startOrRestart перечитывает ecosystem.config.cjs с диска
  conn.exec(
    `cd ${remoteRoot} && pm2 startOrRestart ecosystem.config.cjs --only davidsklad-worker && sleep 4 && pm2 jlist 2>&1 | node -e "const d=require('fs').readFileSync('/dev/stdin','utf8'); const j=JSON.parse(d); const w=j.find(p=>p.name==='davidsklad-worker'); if(w){const e=w.pm2_env; const env=e.NODE_OPTIONS||e.env?.NODE_OPTIONS||'?'; const ar=e.AUTHORITATIVE_REPRICE_BATCH_SIZE||e.env?.AUTHORITATIVE_REPRICE_BATCH_SIZE||'(not set)'; console.log('NODE_OPTIONS:', env); console.log('AUTHORITATIVE_REPRICE_BATCH_SIZE:', ar); console.log('uptime:', Math.round((Date.now()-e.pm_uptime)/1000)+'s'); console.log('restarts:', e.restart_time); console.log('status:', e.status); } else { console.log('worker not found'); }"`,
    (err2, stream) => {
      if (err2) { console.error(err2); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
