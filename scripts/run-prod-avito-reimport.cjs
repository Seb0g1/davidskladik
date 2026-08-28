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
    `curl -s -c /tmp/ds-ri.txt -X POST http://localhost:3000/api/login -H 'Content-Type: application/json' -d '{"username":"david","password":"CGJ-Ge-90"}'`
  );
  console.log("Login:", loginOut.slice(0, 60));

  console.log("\nRunning import/apply...");
  const applyRaw = await sshExec(
    `curl -s -b /tmp/ds-ri.txt -X POST http://localhost:3000/api/avito/import/apply -H 'Content-Type: application/json' -d '{}'`
  );
  let apply;
  try { apply = JSON.parse(applyRaw); } catch { console.log("Raw:", applyRaw.slice(0,300)); return; }
  console.log("Apply:", JSON.stringify({ matched: apply.matchedCount, skipped: apply.skippedCount, created: apply.created, updated: apply.updated, total: apply.totalListings, byReason: apply.skippedByReason }));

  console.log("\nTriggering Avito upload...");
  const uploadRaw = await sshExec(
    `curl -s -b /tmp/ds-ri.txt -X POST http://localhost:3000/api/avito/upload -H 'Content-Type: application/json' -d '{}'`
  );
  console.log("Upload:", uploadRaw.slice(0, 200));
}

main().catch(e => { console.error(e.message); process.exit(1); });
