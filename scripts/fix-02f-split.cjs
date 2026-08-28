"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const tailPath = path.join(root, "routes", "supplier-operations-tail.js");
const tail = fs.readFileSync(tailPath, "utf8").split(/\r?\n/);

const supplierOpsPath = path.join(root, "server", "parts", "02f-routes-supplier-ops.js");
const supplierOps = ['"use strict";', "", ...tail.slice(333, 1274)].join("\n");
fs.writeFileSync(supplierOpsPath, supplierOps);

const syncRoutes = tail.slice(1275, 1352).join("\n");
const bgPath = path.join(root, "server", "parts", "02f-background-jobs.js");
let bg = fs.readFileSync(bgPath, "utf8");
const marker = "app.use((error, request, response, _next) => {";
if (!bg.includes(marker)) {
  throw new Error("error middleware marker not found in 02f-background-jobs.js");
}
if (!bg.includes('app.post("/api/sync"')) {
  bg = bg.replace(marker, `${syncRoutes}\n\n${marker}`);
  fs.writeFileSync(bgPath, bg);
}

fs.unlinkSync(tailPath);
fs.unlinkSync(path.join(root, "server", "parts", "02f-register-supplier-operations-tail.js"));

const sourcePath = path.join(root, "server", "source.js");
let source = fs.readFileSync(sourcePath, "utf8");
source = source.replace('  "02f-register-supplier-operations-tail.js",\n', '  "02f-routes-supplier-ops.js",\n');
fs.writeFileSync(sourcePath, source);

console.log("fix-02f-split complete", {
  supplierOpsLines: 1274 - 333,
  syncRoutesLines: 1352 - 1275,
});
