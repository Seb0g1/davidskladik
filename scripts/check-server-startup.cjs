#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const conn = new Client();
conn.on("ready", () => {
  conn.exec(
    `echo '=== pm2 startup ===' && \
     pm2 startup 2>&1 | tail -5 && \
     echo '' && echo '=== pm2 save status ===' && \
     ls -la /root/.pm2/dump.pm2 2>/dev/null || echo 'no dump file' && \
     echo '' && echo '=== systemd pm2 service ===' && \
     systemctl is-enabled pm2-root 2>/dev/null || systemctl is-enabled pm2 2>/dev/null || echo 'not found' && \
     echo '' && echo '=== pm2 list ===' && \
     pm2 list --no-color`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
