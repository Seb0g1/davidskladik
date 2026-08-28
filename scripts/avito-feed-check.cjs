#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const remoteRoot = "/var/www/davidsklad/davidskladik";

const conn = new Client();
conn.on("ready", () => {
  conn.exec(
    `cd ${remoteRoot} && node --env-file .env -e "
const fs = require('fs');
// Проверяем import rules (maxItems) и текущий фид
const rules = JSON.parse(fs.readFileSync('data/avito-import-rules.json','utf8') || '{}');
const feed = JSON.parse(fs.readFileSync('data/avito-listings.json','utf8') || '{}');
const items = feed.items || [];
const oos = items.filter(i => i.outOfStock).length;
const inFeed = items.filter(i => !i.outOfStock).length;
console.log(JSON.stringify({
  maxItems: rules.maxItems,
  feedTotal: items.length,
  inFeed,
  outOfStock: oos,
  feedToken: (feed.feedToken||'').slice(0,8)+'...',
  updatedAt: feed.updatedAt,
}));
"`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
