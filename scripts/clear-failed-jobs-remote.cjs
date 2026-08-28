"use strict";
const { Client } = require("ssh2");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "../.env") });

const password = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;

if (!password && !privateKey) {
  console.error("Need DEPLOY_PASSWORD or SSH key at ~/.ssh/davidsklad_deploy");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";

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

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    const cfg = { host: "81.17.154.153", username: "root", readyTimeout: 30000 };
    if (privateKey) cfg.privateKey = privateKey;
    else cfg.password = password;
    conn.on("ready", resolve).on("error", reject).connect(cfg);
  });

  try {
    const script = `node -e "
const { Queue } = require('bullmq');
const Redis = require('ioredis');
async function run() {
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const connection = new Redis(redisUrl, { maxRetriesPerRequest: null });
  const queue = new Queue('marketplace-tasks', { connection });
  const counts = await queue.getJobCounts('failed');
  console.log('Failed jobs before clean:', JSON.stringify(counts));
  const cleaned = await queue.clean(0, 10000, 'failed');
  console.log('Cleaned:', cleaned.length, 'jobs');
  const counts2 = await queue.getJobCounts('failed');
  console.log('Failed jobs after clean:', JSON.stringify(counts2));
  await queue.close();
  await connection.quit();
  process.exit(0);
}
run().catch(e => { console.error(e); process.exit(1); });
" 2>&1`;

    await exec(conn, `cd ${remoteRoot} && ${script}`);
  } finally {
    conn.end();
  }
}

main().catch((err) => { console.error(err); process.exit(1); });
