#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const appPassword = process.env.APP_PASSWORD;
const appUser = process.env.APP_USER || "admin";

const conn = new Client();
conn.on("ready", () => {
  conn.exec(
    `curl -s -c /tmp/ac2.txt -b /tmp/ac2.txt \
      -X POST http://localhost:3000/api/login \
      -H 'Content-Type: application/json' \
      -d '{"username":"${appUser}","password":"${appPassword}"}' > /dev/null && \
    curl -s -c /tmp/ac2.txt -b /tmp/ac2.txt http://localhost:3000/api/avito/uploads?perPage=3 && \
    rm -f /tmp/ac2.txt`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
