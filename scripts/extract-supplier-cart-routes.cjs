"use strict";

const { execSync } = require("child_process");
const path = require("path");

const root = path.join(__dirname, "..");
const tmpPath = path.join(root, "scripts", ".tmp-server-f816cfc.js");
execSync(`git show f816cfc:server.js > "${tmpPath}"`, { cwd: root, shell: true, maxBuffer: 50 * 1024 * 1024 });
const content = require("fs").readFileSync(tmpPath, "utf8");
const lines = content.split(/\r?\n/);

let start = -1;
let end = -1;
for (let i = 0; i < lines.length; i += 1) {
  if (start < 0 && lines[i].includes('app.get("/api/supplier-cart/preview"')) start = i;
  if (start >= 0 && end < 0 && lines[i].includes('app.get("/api/supplier-picking-list"')) end = i;
}

if (start < 0 || end < 0) {
  console.error("Could not find supplier cart route block", { start, end });
  process.exit(1);
}

const block = lines.slice(start, end).join("\n");
const outPath = path.join(root, "server", "parts", "02f-supplier-cart-routes.js");
require("fs").writeFileSync(outPath, block + "\n", "utf8");
console.log(`Wrote ${end - start} lines to ${outPath}`);
