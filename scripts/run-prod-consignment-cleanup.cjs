"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function sshExec(cmd, timeout = 60000) {
  return new Promise((resolve, reject) => {
    const client = new Client();
    let output = "";
    const timer = setTimeout(() => { client.end(); reject(new Error("SSH timeout")); }, timeout);
    client.on("ready", () => {
      client.exec(cmd, (err, stream) => {
        if (err) { clearTimeout(timer); client.end(); reject(err); return; }
        stream.on("data", (d) => { output += d; });
        stream.stderr.on("data", (d) => { output += d; });
        stream.on("close", () => { clearTimeout(timer); client.end(); resolve(output); });
      });
    }).on("error", (e) => { clearTimeout(timer); reject(e); })
      .connect({ host: "81.17.154.153", port: 22, username: "root", password });
  });
}

async function main() {
  const loginOut = await sshExec(
    `curl -s -c /tmp/ds-cln.txt -X POST http://localhost:3000/api/login -H 'Content-Type: application/json' -d '{"username":"david","password":"CGJ-Ge-90"}'`
  );
  console.log("Login:", loginOut.slice(0, 100));

  const cleanupOut = await sshExec(
    `curl -s -b /tmp/ds-cln.txt -X POST http://localhost:3000/api/consignment/pm-sync/cleanup -H 'Content-Type: application/json' -d '{}'`
  );
  console.log("Cleanup result:", cleanupOut);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
