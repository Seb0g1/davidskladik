#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const appUser = process.env.APP_USER || "david";
const appPassword = process.env.APP_PASSWORD;

const conn = new Client();
conn.on("ready", () => {
  // Login + trigger feed refresh + trigger upload
  conn.exec(
    `curl -s -c /tmp/af.txt -b /tmp/af.txt \
      -X POST http://localhost:3000/api/login \
      -H 'Content-Type: application/json' \
      -d '{"username":"${appUser}","password":"${appPassword}"}' > /dev/null && \
    echo '=== Triggering feed refresh ===' && \
    curl -s -c /tmp/af.txt -b /tmp/af.txt \
      -X POST http://localhost:3000/api/avito/feed/refresh \
      -H 'Content-Type: application/json' \
      -d '{}' && \
    echo '' && echo '=== Waiting 5s ===' && sleep 5 && \
    echo '=== Feed stats ===' && \
    curl -s -c /tmp/af.txt -b /tmp/af.txt http://localhost:3000/api/avito/sync && \
    rm -f /tmp/af.txt`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
