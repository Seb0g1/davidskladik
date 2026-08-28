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
const f = JSON.parse(fs.readFileSync('data/avito-listings.json','utf8'));
const items = f.items || [];
const active = items.filter(i => !i.outOfStock && (i.imageUrls||[]).length > 0);
const noImages = items.filter(i => !i.outOfStock && !(i.imageUrls||[]).length);
const oos = items.filter(i => i.outOfStock);
// oos sample: first 3 enabled items
const sample = oos.filter(i=>i.enabled!==false).slice(0,5).map(i=>({
  adId:i.adId,
  title:(i.title||'').slice(0,50),
  targetStock:i.stockQuantity,
  lastSynced:i.lastSyncedAt,
  imgCount:(i.imageUrls||[]).length,
}));
console.log(JSON.stringify({
  total: items.length,
  inFeed: active.length,
  outOfStock: oos.length,
  noImagesFeed: noImages.length,
  sample,
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
