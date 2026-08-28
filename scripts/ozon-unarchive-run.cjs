#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const appUser = process.env.APP_USER || "david";
const appPassword = process.env.APP_PASSWORD;

const conn = new Client();
conn.on("ready", () => {
  conn.exec(
    `curl -s -c /tmp/oz.txt -b /tmp/oz.txt \
      -X POST http://localhost:3000/api/login \
      -H 'Content-Type: application/json' \
      -d '{"username":"${appUser}","password":"${appPassword}"}' > /dev/null && \
    echo '=== Queue status ===' && \
    curl -s -c /tmp/oz.txt -b /tmp/oz.txt http://localhost:3000/api/ozon/unarchive-queue && \
    echo '' && echo '=== Rebuilding queue ===' && \
    curl -s -c /tmp/oz.txt -b /tmp/oz.txt \
      -X POST http://localhost:3000/api/ozon/unarchive-queue/rebuild \
      -H 'Content-Type: application/json' \
      -d '{}' && \
    echo '' && echo '=== Processing 100 items ===' && \
    curl -s -c /tmp/oz.txt -b /tmp/oz.txt \
      -X POST http://localhost:3000/api/ozon/unarchive-queue/process \
      -H 'Content-Type: application/json' \
      -d '{"limit":100,"force":true}' && \
    rm -f /tmp/oz.txt`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
