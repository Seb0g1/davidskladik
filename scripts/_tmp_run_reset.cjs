"use strict";
require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;

const remoteRoot = "/var/www/davidsklad/davidskladik";
const tmpRemotePath = `${remoteRoot}/_reset_tmp.cjs`;

const resetScript = `"use strict";
require("dotenv").config();
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();
Promise.all([
  prisma.appSetting.deleteMany({ where: { key: { startsWith: "picker_balance:" } } }),
  prisma.appSetting.deleteMany({ where: { key: { startsWith: "daily_cart_total:" } } }),
  prisma.supplierLedgerEntry.deleteMany({}),
]).then(function(results) {
  console.log("picker_balance records deleted: " + results[0].count);
  console.log("daily_cart_total records deleted: " + results[1].count);
  console.log("supplier_ledger_entries deleted: " + results[2].count);
  return prisma.disconnect ? prisma.disconnect() : prisma["$" + "disconnect"]();
}).then(function() {
  console.log("Done.");
}).catch(function(e) {
  console.error("Error: " + e.message);
  process.exit(1);
});
`;

function execRemote(conn, command) {
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
  return new Promise((resolve, reject) => conn.sftp((err, sftp) => err ? reject(err) : resolve(sftp)));
}

function sftpWrite(sftp, content, remotePath) {
  return new Promise((resolve, reject) => {
    const write = sftp.createWriteStream(remotePath);
    write.on("close", resolve);
    write.on("error", reject);
    write.end(content);
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve);
    conn.on("error", reject);
    conn.connect({
      host: "81.17.154.153",
      port: 22,
      username: "root",
      ...(privateKey ? { privateKey } : { password }),
    });
  });
  console.log("SSH connected. Uploading reset script...");
  const sftp = await openSftp(conn);
  await sftpWrite(sftp, resetScript, tmpRemotePath);
  console.log("Running reset script on server...\n");
  await execRemote(conn, `cd ${remoteRoot} && node ${tmpRemotePath}`);
  await execRemote(conn, `rm -f ${tmpRemotePath}`);
  console.log("\nReset complete.");
  conn.end();
}

main().catch((error) => {
  console.error("Error:", error.message);
  process.exit(1);
});
