"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function sshExec(cmd, timeout = 120000) {
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
    `curl -s -c /tmp/ds-crst.txt -X POST http://localhost:3000/api/login -H 'Content-Type: application/json' -d '{"username":"david","password":"CGJ-Ge-90"}'`
  );
  console.log("Login:", loginOut.slice(0, 60));

  // Step 1: Reset (delete old PM sales + restore quantities)
  console.log("\n[1/2] Resetting PM sales...");
  const resetRaw = await sshExec(
    `curl -s -b /tmp/ds-crst.txt -X POST http://localhost:3000/api/consignment/pm-sync/reset -H 'Content-Type: application/json' -d '{}'`
  );
  let reset;
  try { reset = JSON.parse(resetRaw); } catch { console.log("Raw:", resetRaw.slice(0,300)); return; }
  console.log("Reset result:", JSON.stringify(reset));

  // Step 2: Fresh sync from PM
  console.log("\n[2/2] Running fresh PM sync...");
  const syncRaw = await sshExec(
    `curl -s -b /tmp/ds-crst.txt -X POST http://localhost:3000/api/consignment/pm-sync -H 'Content-Type: application/json' -d '{}'`,
    120000
  );
  let sync;
  try { sync = JSON.parse(syncRaw); } catch { console.log("Raw:", syncRaw.slice(0,300)); return; }
  console.log("Sync result:", JSON.stringify(sync));
}

main().catch(e => { console.error(e.message); process.exit(1); });
