#!/usr/bin/env node
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) throw err;
    const local = path.join(root, "scripts/prod-post-deploy-check.cjs");
    const remote = `${remoteRoot}/scripts/prod-post-deploy-check.cjs`;
    fs.createReadStream(local).pipe(sftp.createWriteStream(remote)).on("close", () => {
      conn.exec(`cd ${remoteRoot} && node scripts/prod-post-deploy-check.cjs`, (e, stream) => {
        stream.on("data", (d) => process.stdout.write(d));
        stream.stderr.on("data", (d) => process.stderr.write(d));
        stream.on("close", (code) => { conn.end(); process.exit(code || 0); });
      });
    });
  });
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 90000 });
