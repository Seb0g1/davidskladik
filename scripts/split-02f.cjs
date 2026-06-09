"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.join(__dirname, "..");
const PARTS = path.join(ROOT, "server", "parts");
const ROUTES = path.join(ROOT, "routes");
const SOURCE = path.join(ROOT, "server", "source.js");
const BOOTSTRAP = path.join(ROOT, "server", "parts", "01-bootstrap.js");

const INPUT = path.join(PARTS, "02f-routes-tail.js");

function readLines(filePath) {
  return fs.readFileSync(filePath, "utf8").split(/\r?\n/);
}

function sliceLines(lines, start, end) {
  return lines.slice(start - 1, end).join("\n");
}

function extractDeps(routeSource) {
  const builtins = new Set([
    "true", "false", "null", "undefined", "async", "await", "function", "const", "let", "var",
    "return", "if", "else", "try", "catch", "next", "new", "Number", "String", "Boolean", "Array",
    "Object", "Math", "Date", "Set", "Map", "Promise", "Error", "JSON", "process", "request",
    "response", "app", "entry", "details", "error", "ok", "id", "type", "status", "limit", "rows",
    "results", "item", "items", "row", "product", "products", "shop", "shops", "target", "targets",
    "data", "body", "query", "params", "path", "method", "code", "message", "detail", "logger",
    "dryRun", "confirmed", "summary", "warnings", "blocked", "sent", "failed", "deleted", "planned",
    "preview", "warehouse", "actions", "offer", "offers", "result", "stage", "name", "key", "value",
    "from", "to", "total", "history", "audit", "generatedAt", "protectedBrands", "deleteLimit",
    "sendLimit", "alreadyRunning", "running", "started", "nextRunAt", "everyHours", "uploadError",
    "matches", "ozon", "yandex", "exports", "updatedAt", "before", "after", "chunk", "account", "rules",
    "candidates", "selected", "eligibleRows", "selectedRows", "cardResults", "priceStage", "stockStage",
    "stageWarnings", "exportedProducts", "priceResultByTargetOffer", "yandexProducts", "baseExportState",
    "exportState", "priceResult", "sentOfferIds", "yandexExistingOfferIds", "initialRows",
    "candidateOfferIds", "productsById", "selectedProducts", "failedRows", "sentCount", "skippedExisting",
    "skippedBlocked", "skippedMissing", "limitedToDelete", "toDelete", "deleteSummary", "get", "post",
    "patch", "delete", "put", "filter", "map", "find", "slice", "push", "length", "includes", "has",
    "set", "for", "of", "in", "typeof", "instanceof", "isFinite", "isArray", "max", "min", "round",
    "trim", "toLowerCase", "toISOString", "randomUUID", "now", "Date", "response", "request", "next",
    "app", "deps", "module", "exports", "registerSupplierOperationsTailRoutes", "publicYandexCleanupAuditEntry",
    "publicYandexImportAuditEntry",
  ]);
  const identRe = /\b([A-Za-z_$][A-Za-z0-9_$]*)\b/g;
  const counts = {};
  let m;
  while ((m = identRe.exec(routeSource))) {
    const id = m[1];
    if (!builtins.has(id)) counts[id] = (counts[id] || 0) + 1;
  }
  return Object.keys(counts).sort((a, b) => a.localeCompare(b));
}

function buildRoutesModule(routeSource, helpersSource, deps) {
  const depsLines = deps.map((name) => `    ${name},`).join("\n");
  return `"use strict";

function registerSupplierOperationsTailRoutes(app, deps) {
  const {
${depsLines}
  } = deps;

${helpersSource}

${routeSource}
}

module.exports = { registerSupplierOperationsTailRoutes };
`;
}

function buildRegisterPart(deps) {
  const depsLines = deps.map((name) => `  ${name},`).join("\n");
  return `registerSupplierOperationsTailRoutes(app, {
${depsLines}
  runSync,
  runDailyRefresh,
  getDailySyncStatus,
  startManualWarehouseSync,
  getManualWarehouseSyncStatus,
  runMarketplaceMaintenanceCycle,
  dailySyncPromise,
  marketplaceMaintenancePromise,
  marketplaceMaintenanceNextRunAt,
  marketplaceMaintenanceHours,
});
`;
}

function updateBootstrap() {
  let content = fs.readFileSync(BOOTSTRAP, "utf8");
  const marker = 'const { registerLegacyCatalogRoutes } = require("../routes/legacy-catalog");';
  const insert = `const { registerSupplierOperationsTailRoutes } = require("../routes/supplier-operations-tail");\n${marker}`;
  if (!content.includes("supplier-operations-tail")) {
    content = content.replace(marker, insert);
    fs.writeFileSync(BOOTSTRAP, content, "utf8");
  }
}

function updateSource() {
  const content = fs.readFileSync(SOURCE, "utf8");
  const next = content
    .replace(
      /"02f-routes-tail\.js",\s*\n/,
      `"02f-background-jobs.js",\n  "02f-register-supplier-operations-tail.js",\n`
    );
  if (next === content) {
    throw new Error("Could not update server/source.js — 02f-routes-tail.js entry not found");
  }
  fs.writeFileSync(SOURCE, next, "utf8");
}

function main() {
  const lines = readLines(INPUT);
  const route1 = sliceLines(lines, 2, 597);
  const helpers = sliceLines(lines, 599, 635);
  const route2 = sliceLines(lines, 637, 942);
  const background = [
    sliceLines(lines, 944, 3051),
    sliceLines(lines, 3131, 3207),
  ].join("\n\n");
  const route3 = sliceLines(lines, 3053, 3129);
  const routeSource = [route1, route2, route3].join("\n\n");

  const deps = extractDeps(routeSource + "\n" + helpers);
  const extraDeps = [
    "requireAdmin",
    "requireStaff",
    "yandexCleanupDeleteLimit",
    "yandexImportSendLimit",
    "runSync",
    "runDailyRefresh",
    "getDailySyncStatus",
    "startManualWarehouseSync",
    "getManualWarehouseSyncStatus",
    "runMarketplaceMaintenanceCycle",
    "dailySyncPromise",
    "marketplaceMaintenancePromise",
    "marketplaceMaintenanceNextRunAt",
    "marketplaceMaintenanceHours",
  ];
  for (const name of extraDeps) {
    if (!deps.includes(name)) deps.push(name);
  }
  deps.sort((a, b) => a.localeCompare(b));

  const routesPath = path.join(ROUTES, "supplier-operations-tail.js");
  fs.writeFileSync(routesPath, buildRoutesModule(routeSource, helpers, deps), "utf8");

  const bgPath = path.join(PARTS, "02f-background-jobs.js");
  fs.writeFileSync(bgPath, background + "\n", "utf8");

  const registerPath = path.join(PARTS, "02f-register-supplier-operations-tail.js");
  fs.writeFileSync(registerPath, buildRegisterPart(deps), "utf8");

  updateBootstrap();
  updateSource();
  fs.unlinkSync(INPUT);

  console.log("split-02f: created", path.relative(ROOT, routesPath));
  console.log("split-02f: created", path.relative(ROOT, bgPath));
  console.log("split-02f: created", path.relative(ROOT, registerPath));
  console.log("split-02f: removed", path.relative(ROOT, INPUT));
  console.log("split-02f: deps count", deps.length);
}

main();
