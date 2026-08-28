#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const conn = new Client();
conn.on("ready", () => {
  // Логинимся и запускаем автозагрузку
  conn.exec(
    `curl -s -c /tmp/avito_cookie.txt -b /tmp/avito_cookie.txt \
      -X POST http://localhost:3000/api/login \
      -H 'Content-Type: application/json' \
      -d '{"username":"${process.env.APP_USER || "admin"}","password":"${process.env.APP_PASSWORD}"}' && \
    echo '' && \
    curl -s -c /tmp/avito_cookie.txt -b /tmp/avito_cookie.txt \
      -X POST http://localhost:3000/api/avito/upload \
      -H 'Content-Type: application/json' \
      -d '{}' && \
    rm -f /tmp/avito_cookie.txt`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
