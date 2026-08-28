#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";
const pollCmd = "ps aux | grep -E 'node scripts/(delete-yandex|repair-linked|audit-)' | grep -v grep || true";

function execCapture(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; });
      stream.stderr.on("data", (d) => { out += d; });
      stream.on("close", (code) => resolve({ code, out: out.trim() }));
    });
  });
}

function connect() {
  return new Promise((resolve, reject) => {
    const conn = new Client();
    conn.on("ready", () => resolve(conn)).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 60000,
    });
  });
}

async function pollOnce(label) {
  const conn = await connect();
  try {
    const { out } = await execCapture(conn, pollCmd);
    console.log(`\n=== ${label} ===`);
    console.log(out || "(no matching processes)");
    return out;
  } finally {
    conn.end();
  }
}

async function runRemote(label, command) {
  const conn = await connect();
  try {
    console.log(`\n=== ${label} ===`);
    const { code, out } = await execCapture(conn, command);
    if (out) console.log(out);
    if (code) throw new Error(`exit ${code}`);
  } finally {
    conn.end();
  }
}

async function watchUntilIdle(maxMinutes = 45, intervalSec = 150) {
  const deadline = Date.now() + maxMinutes * 60 * 1000;
  let n = 1;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, intervalSec * 1000));
    n += 1;
    const out = await pollOnce(`POLL ${n} ${new Date().toISOString().slice(11, 19)}`);
    if (!out) {
      console.log("\n=== IDLE ===");
      return true;
    }
  }
  await pollOnce(`TIMEOUT after ${maxMinutes}m`);
  return false;
}

async function main() {
  const mode = process.argv[2] || "poll";
  if (mode === "poll") {
    await pollOnce(process.argv[3] || "POLL");
    return;
  }
  if (mode === "watch") {
    const idle = await watchUntilIdle();
    if (!idle) {
      console.log("\n=== STILL RUNNING after 45m — do NOT launch more scripts ===");
      process.exit(2);
    }
    await runRemote("DRY-RUN delete-yandex-small-volume", `cd ${remoteRoot} && node scripts/delete-yandex-small-volume.cjs --dry-run`);
    await runRemote("prod-post-deploy-check", `cd ${remoteRoot} && node scripts/prod-post-deploy-check.cjs`);
    return;
  }
  if (mode === "dry-run") {
    await runRemote("DRY-RUN delete-yandex-small-volume", `cd ${remoteRoot} && node scripts/delete-yandex-small-volume.cjs --dry-run`);
    return;
  }
  if (mode === "post-check") {
    await runRemote("prod-post-deploy-check", `cd ${remoteRoot} && node scripts/prod-post-deploy-check.cjs`);
    return;
  }
  console.error("Usage: poll | watch | dry-run | post-check");
  process.exit(1);
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
