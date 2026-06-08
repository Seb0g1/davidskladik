#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const https = require("node:https");

require("dotenv").config();

const root = path.resolve(__dirname, "..");
const alertPath = path.join(root, "data", "last-prod-alert.json");
const debounceMs = Math.max(60_000, Number(process.env.PROD_ALERT_DEBOUNCE_MS || 900000) || 900000);
const failures = process.argv.slice(2).filter(Boolean);

function readState() {
  try {
    return JSON.parse(fs.readFileSync(alertPath, "utf8"));
  } catch {
    return { lastAlertAt: 0, consecutiveFailures: 0 };
  }
}

function writeState(state) {
  fs.mkdirSync(path.dirname(alertPath), { recursive: true });
  fs.writeFileSync(alertPath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function sendTelegram(text) {
  const token = String(process.env.TELEGRAM_BOT_TOKEN || "").trim();
  const chatId = String(process.env.TELEGRAM_CHAT_ID || "").trim();
  if (!token || !chatId) return Promise.resolve(false);
  const payload = JSON.stringify({ chat_id: chatId, text });
  return new Promise((resolve) => {
    const req = https.request({
      hostname: "api.telegram.org",
      path: `/bot${token}/sendMessage`,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(payload),
      },
    }, (res) => {
      res.on("data", () => {});
      res.on("end", () => resolve(res.statusCode >= 200 && res.statusCode < 300));
    });
    req.on("error", () => resolve(false));
    req.write(payload);
    req.end();
  });
}

async function main() {
  const now = Date.now();
  const state = readState();
  const next = {
    lastAlertAt: state.lastAlertAt || 0,
    consecutiveFailures: (state.consecutiveFailures || 0) + 1,
    lastFailureAt: new Date(now).toISOString(),
    failures: failures.length ? failures : ["prod health check failed"],
  };
  writeState(next);

  const message = [
    "davidsklad prod alert",
    `time: ${next.lastFailureAt}`,
    `consecutiveFailures: ${next.consecutiveFailures}`,
    ...next.failures.slice(0, 8),
  ].join("\n");

  process.stderr.write(`${message}\n`);

  if (next.consecutiveFailures >= 2 && now - next.lastAlertAt >= debounceMs) {
    const sent = await sendTelegram(message);
    if (sent) {
      next.lastAlertAt = now;
      writeState(next);
    }
  }
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
});
