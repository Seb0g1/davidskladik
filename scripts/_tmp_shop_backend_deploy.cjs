#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const REMOTE = "/var/www/davidsklad/davidskladik";

function exec(conn, cmd, opts = {}) {
  return new Promise((resolve, reject) => {
    let out = "";
    conn.exec(cmd, opts, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error("exit " + code)) : resolve(out)));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) =>
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153", username: "root", password,
      readyTimeout: 60000, keepaliveInterval: 10000,
    })
  );

  try {
    console.log("=== Pulling latest code ===");
    // Force-sync to remote branch, preserving .env and node_modules
    await exec(conn, [
      `cd ${REMOTE}`,
      `git fetch origin`,
      `git checkout -f codex/restore-4dfc0cb`,
      `git clean -fd --exclude='.env' --exclude='node_modules' --exclude='*.log'`,
      `git reset --hard origin/codex/restore-4dfc0cb`,
    ].join(" && "));

    console.log("\n=== Running DB migration ===");
    await exec(conn, `cd ${REMOTE} && npx prisma migrate deploy`);

    console.log("\n=== Generating Prisma client ===");
    await exec(conn, `cd ${REMOTE} && npx prisma generate`);

    console.log("\n=== Reloading API server ===");
    await exec(conn, `pm2 reload davidsklad-api --update-env`);

    console.log("\n=== Reloading worker ===");
    await exec(conn, `pm2 reload davidsklad-worker --update-env`);

    console.log("\n=== Status ===");
    await exec(conn, "pm2 list");

    console.log("\n✓ Backend deployed!");
  } finally {
    conn.end();
  }
}

main().catch(e => { console.error("FAIL:", e.message); process.exit(1); });
