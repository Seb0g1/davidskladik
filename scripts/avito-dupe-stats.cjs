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
const feed = JSON.parse(fs.readFileSync('data/avito-listings.json','utf8'));
const items = feed.items || [];
const active = items.filter(i => !i.outOfStock && i.enabled !== false && i.title);

// Title dedup (same as buildAvitoFeedXml)
const seenSrcIds = new Set();
const seenTitles = new Set();
let unique = 0, dupSrc = 0, dupTitle = 0, noImages = 0, withImages = 0;
for (const item of active) {
  const srcId = (item.sourceProductId || '').trim();
  const titleKey = (item.title || '').toLowerCase().replace(/ё/g,'е').trim();
  if (srcId && seenSrcIds.has(srcId)) { dupSrc++; continue; }
  if (titleKey && seenTitles.has(titleKey)) { dupTitle++; continue; }
  if (srcId) seenSrcIds.add(srcId);
  if (titleKey) seenTitles.add(titleKey);
  unique++;
  if ((item.imageUrls||[]).length) withImages++;
  else noImages++;
}
console.log(JSON.stringify({
  activeInJson: active.length,
  uniqueAfterDedup: unique,
  withImages,
  noImages,
  dupBySourceId: dupSrc,
  dupByTitle: dupTitle,
  // Sample duplicate titles (first 5)
  sampleDupTitles: (() => {
    const seen2 = new Set(); const dups = [];
    for (const item of active) {
      const k = (item.title||'').toLowerCase().replace(/ё/g,'е').trim();
      if (seen2.has(k) && dups.length < 5) dups.push(item.title);
      seen2.add(k);
    }
    return dups;
  })(),
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
