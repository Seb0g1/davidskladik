#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const remoteRoot = "/var/www/davidsklad/davidskladik";
const appUser = process.env.APP_USER || "david";
const appPassword = process.env.APP_PASSWORD;

const conn = new Client();
conn.on("ready", () => {
  // Получаем feedToken и считаем <Ad> в XML
  conn.exec(
    `cd ${remoteRoot} && node --env-file .env -e "
const fs = require('fs');
const feed = JSON.parse(fs.readFileSync('data/avito-listings.json','utf8'));
const token = feed.feedToken;
const items = feed.items || [];
const inFeed = items.filter(i => !i.outOfStock && i.enabled !== false);
console.log('feedToken:', token.slice(0,16)+'...');
console.log('inFeed items (json):', inFeed.length);
" && \
curl -s "http://localhost:3000/api/avito/feed/${process.env.AVITO_FEED_CHECK_TOKEN || 'TOKEN'}" 2>/dev/null | grep -o '<Ad>' | wc -l || true`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
