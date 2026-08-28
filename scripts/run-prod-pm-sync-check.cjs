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
  // Login
  const loginOut = await sshExec(
    `curl -s -c /tmp/ds-pmcheck.txt -X POST http://localhost:3000/api/login -H 'Content-Type: application/json' -d '{"username":"david","password":"CGJ-Ge-90"}'`
  );
  console.log("Login:", loginOut.slice(0, 80));

  // Count items before
  const beforeOut = await sshExec(
    `curl -s -b /tmp/ds-pmcheck.txt http://localhost:3000/api/consignment/items`
  );
  let beforeCount = "?";
  try { beforeCount = JSON.parse(beforeOut).items?.length ?? JSON.parse(beforeOut).total ?? "?"; } catch {}
  console.log("Items before sync:", beforeCount);

  // Run PM sync
  console.log("\nRunning PM sync...");
  const syncOut = await sshExec(
    `curl -s -b /tmp/ds-pmcheck.txt -X POST http://localhost:3000/api/consignment/pm-sync -H 'Content-Type: application/json' -d '{}'`,
    120000
  );
  let syncResult;
  try { syncResult = JSON.parse(syncOut); } catch { console.log("Raw:", syncOut.slice(0, 300)); return; }
  console.log("Sync result:", JSON.stringify(syncResult, null, 2));

  // Count items after
  const afterOut = await sshExec(
    `curl -s -b /tmp/ds-pmcheck.txt http://localhost:3000/api/consignment/items`
  );
  let afterCount = "?";
  try { afterCount = JSON.parse(afterOut).items?.length ?? JSON.parse(afterOut).total ?? "?"; } catch {}
  console.log("\nItems after sync:", afterCount);

  if (beforeCount !== "?" && afterCount !== "?") {
    const diff = Number(afterCount) - Number(beforeCount);
    console.log(`Diff: ${diff > 0 ? "+" : ""}${diff} ${diff === 0 ? "✓ нет лишних записей" : "⚠ изменилось!"}`);
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
