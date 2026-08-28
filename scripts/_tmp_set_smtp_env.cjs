#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const ENV_FILE = "/var/www/davidsklad/davidskladik/.env";

const smtpVars = {
  SHOP_SMTP_HOST: "smtp.timeweb.ru",
  SHOP_SMTP_PORT: "465",
  SHOP_SMTP_USER: "noreply@magicvibes.ru",
  SHOP_SMTP_PASS: "PTsr7oI>^pUYg\\",
};

function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    let out = "";
    conn.exec(cmd, (err, stream) => {
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
      readyTimeout: 30000,
    })
  );

  try {
    // Remove existing SHOP_SMTP_ lines, then append fresh ones
    await exec(conn, `sed -i '/^SHOP_SMTP_/d' ${ENV_FILE}`);
    for (const [key, val] of Object.entries(smtpVars)) {
      // Use printf to avoid shell interpretation of special chars
      const escaped = val.replace(/'/g, "'\\''");
      await exec(conn, `printf '%s\\n' '${key}=${escaped}' >> ${ENV_FILE}`);
    }
    console.log("✓ SMTP vars written, reloading api...");
    await exec(conn, "pm2 reload davidsklad-api --update-env");
    console.log("✓ API reloaded");
    // Verify
    await exec(conn, `grep SHOP_SMTP ${ENV_FILE}`);
  } finally {
    conn.end();
  }
}

main().catch(e => { console.error("FAIL:", e.message); process.exit(1); });
