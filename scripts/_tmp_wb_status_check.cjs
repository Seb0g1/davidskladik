#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const conn = new Client();
conn.on("ready", () => {
  conn.exec(
    `cat /var/www/davidsklad/davidskladik/data/wb-sync-status.json 2>/dev/null || echo 'FILE_NOT_FOUND'`,
    (err, stream) => {
      if (err) { console.error(err.message); conn.end(); return; }
      let out = "";
      stream.on("data", (d) => out += d);
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => {
        conn.end();
        if (out.trim() === "FILE_NOT_FOUND") {
          console.log("Файл wb-sync-status.json не найден на сервере");
          return;
        }
        try {
          const data = JSON.parse(out);
          console.log("=== WB Sync Status ===");
          console.log("Running:", data.running);
          console.log("Enabled:", data.enabled);
          console.log("Next run:", data.nextRunAt);
          if (data.lastResult) {
            console.log("\n=== Last Result ===");
            console.log("At:", data.lastResult.at);
            console.log("Status:", data.lastResult.status);
            console.log("Cards:", data.lastResult.cards);
            console.log("Prices sent:", data.lastResult.pricesSent);
            console.log("Prices error:", data.lastResult.pricesError || "none");
            console.log("Stocks sent:", data.lastResult.stocksSent);
            console.log("Skipped manual:", data.lastResult.skippedManual);
            console.log("Duration:", data.lastResult.durationMs ? `${Math.round(data.lastResult.durationMs / 1000)}s` : "?");
          } else {
            console.log("No last result");
          }
        } catch (e) {
          console.log("Raw:", out);
        }
      });
    }
  );
}).connect({
  host: "81.17.154.153",
  port: 22,
  username: "root",
  password,
});
conn.on("error", (e) => { console.error("SSH error:", e.message); process.exit(1); });
