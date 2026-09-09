#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { Client } = require("ssh2");

require("dotenv").config();

const sshPassword = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;
if (!privateKey && !sshPassword) { console.error("No SSH key or DEPLOY_PASSWORD"); process.exit(1); }

const pmHost = process.env.PM_DB_HOST || "127.0.0.1";
const pmPort = Number(process.env.PM_DB_PORT || 3306);
const pmUser = process.env.PM_DB_USER || "root";
const pmPass = process.env.PM_DB_PASSWORD || "";
const pmDb   = process.env.PM_DB_NAME || "pricemaster7";

function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      let out = ""; let errOut = "";
      stream.on("data", (d) => { out += d; });
      stream.stderr.on("data", (d) => { errOut += d; });
      stream.on("close", () => resolve({ out, errOut }));
    });
  });
}

function sftpWriteFile(sftp, remotePath, content) {
  return new Promise((resolve, reject) => {
    const stream = sftp.createWriteStream(remotePath);
    stream.on("close", resolve);
    stream.on("error", reject);
    stream.end(content);
  });
}

function openSftp(conn) {
  return new Promise((resolve, reject) => conn.sftp((err, sftp) => err ? reject(err) : resolve(sftp)));
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    const cfg = { host: "81.17.154.153", username: "root", readyTimeout: 30000 };
    if (privateKey) cfg.privateKey = privateKey;
    else cfg.password = sshPassword;
    conn.on("ready", resolve).on("error", reject).connect(cfg);
  });

  const script = `
const m = require('/var/www/davidsklad/davidskladik/node_modules/mysql2/promise');
(async () => {
  const c = await m.createConnection({
    host: ${JSON.stringify(pmHost)},
    port: ${pmPort},
    user: ${JSON.stringify(pmUser)},
    password: ${JSON.stringify(pmPass)},
    database: ${JSON.stringify(pmDb)},
    connectTimeout: 5000,
  });
  const [r1] = await c.query('DESCRIBE OfferRows');
  console.log('=OfferRows=');
  r1.forEach(r => console.log(r.Field + '\\t' + r.Type));
  const [r2] = await c.query('DESCRIBE OfferDocs');
  console.log('=OfferDocs=');
  r2.forEach(r => console.log(r.Field + '\\t' + r.Type));
  const [r3] = await c.query('SELECT * FROM OfferRows WHERE Active=1 AND NativePrice>0 LIMIT 1');
  if (r3[0]) {
    console.log('=SampleRow keys=');
    console.log(Object.keys(r3[0]).join(', '));
  }
  await c.end();
})().catch(e => { console.error('ERR:', e.message); process.exit(1); });
`;

  try {
    const sftp = await openSftp(conn);
    await sftpWriteFile(sftp, "/tmp/pm_inspect.js", script);
    const result = await exec(conn, "node /tmp/pm_inspect.js && rm -f /tmp/pm_inspect.js");
    console.log(result.out || "(empty)");
    if (result.errOut.trim()) console.log("STDERR:", result.errOut.slice(0, 300));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
