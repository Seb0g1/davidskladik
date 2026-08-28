#!/usr/bin/env node
"use strict";
/**
 * Деплоит обновлённый ecosystem.config.cjs и перезапускает worker с новым heap limit.
 * Изменения: --max-old-space-size 5120→3072, AUTHORITATIVE_REPRICE_BATCH_SIZE=100.
 */
const { Client } = require("ssh2");
const fs = require("fs");
const path = require("path");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const remoteRoot = "/var/www/davidsklad/davidskladik";
const localEcosystem = path.join(__dirname, "..", "ecosystem.config.cjs");

const conn = new Client();
conn.on("ready", () => {
  console.log("SSH connected — uploading ecosystem.config.cjs...");
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const content = fs.readFileSync(localEcosystem);
    const ws = sftp.createWriteStream(remoteRoot + "/ecosystem.config.cjs");
    ws.on("close", () => {
      console.log("ecosystem.config.cjs uploaded — restarting worker...");
      // Перезапускаем только worker (не трогаем api), чтобы применить новый NODE_OPTIONS
      conn.exec(
        `cd ${remoteRoot} && pm2 restart davidsklad-worker --update-env && sleep 3 && pm2 jlist 2>&1`,
        (err2, stream) => {
          if (err2) { console.error(err2); conn.end(); return; }
          stream.on("data", d => process.stdout.write(d));
          stream.stderr.on("data", d => process.stderr.write(d));
          stream.on("close", (code) => {
            console.log("\n--- exit code:", code);
            conn.end();
          });
        }
      );
    });
    ws.end(content);
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
