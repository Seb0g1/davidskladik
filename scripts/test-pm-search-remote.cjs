"use strict";
const { Client } = require("ssh2");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "../.env") });

const keyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const localScript = path.join(__dirname, "_pm_search_test.cjs");
const remoteScript = `${remoteRoot}/_pm_search_test.cjs`;

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve()));
    });
  });
}

function openSftp(conn) {
  return new Promise((res, rej) => { conn.sftp((e, s) => e ? rej(e) : res(s)); });
}

function sftpPut(sftp, local, remote) {
  return new Promise((res, rej) => {
    const r = fs.createReadStream(local);
    const w = sftp.createWriteStream(remote);
    w.on("close", res);
    w.on("error", rej);
    r.pipe(w);
  });
}

async function main() {
  const conn = new Client();
  await new Promise((res, rej) => {
    conn.on("ready", res).on("error", rej).connect({
      host: "81.17.154.153", username: "root",
      privateKey: fs.readFileSync(keyPath), readyTimeout: 15000,
    });
  });
  try {
    const sftp = await openSftp(conn);
    await sftpPut(sftp, localScript, remoteScript);
    await exec(conn, `cd ${remoteRoot} && node _pm_search_test.cjs; rm -f _pm_search_test.cjs`);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
