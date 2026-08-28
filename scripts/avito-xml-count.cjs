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
const token = feed.feedToken;
const items = feed.items || [];
const inFeed = items.filter(i => !i.outOfStock && i.enabled !== false);
console.log(JSON.stringify({ token: token.slice(0,12), inFeed: inFeed.length, total: items.length }));
// Проверяем XML через localhost
const http = require('http');
const req = http.request({
  hostname: 'localhost', port: 3000,
  path: '/public/avito-feed/' + token + '.xml', method: 'GET'
}, res => {
  let body = '';
  res.on('data', chunk => body += chunk);
  res.on('end', () => {
    const count = (body.match(/<Ad>/g) || []).length;
    const size = body.length;
    console.log(JSON.stringify({ xmlAdCount: count, xmlSizeBytes: size, status: res.statusCode }));
  });
});
req.on('error', e => console.error('HTTP error:', e.message));
req.end();
"`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => { setTimeout(() => conn.end(), 3000); });
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
