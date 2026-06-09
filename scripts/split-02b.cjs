"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const srcPath = path.join(root, "server/parts/02b-routes-catalog.js");
const lines = fs.readFileSync(srcPath, "utf8").split(/\r?\n/);

const helperRanges = [[75, 186], [424, 525], [544, 570]];
const routeRanges = [[1, 74], [188, 224], [226, 422], [527, 542], [572, lines.length]];

function extract(ranges) {
  const out = [];
  for (const [a, b] of ranges) {
    for (let i = a; i <= b; i += 1) out.push(lines[i - 1]);
  }
  return out.join("\n");
}

const helpersBody = extract(helperRanges);
const routesBody = extract(routeRanges);

const manualDeps = [
  "collectHealthDetails",
  "readSnapshot",
  "readHistory",
  "pool",
  "cleanLimit",
  "cleanText",
  "likeSearch",
  "requireAdmin",
  "shouldUsePostgresStorage",
  "getPrisma",
  "auditRowToEntry",
  "jsonFallbackEnabled",
  "toDateOrNull",
  "logger",
  "getPriceMasterSearchCache",
  "setPriceMasterSearchCache",
  "readAppSettings",
  "readAudit",
  "readAuditFiltered",
  "normalizePriceMasterPrice",
  "normalizeSupplierName",
  "searchPriceMasterSnapshotPartners",
  "searchPriceMasterSnapshotOffers",
  "listBrandFallbackCandidates",
  "getOzonAccountByTarget",
  "listPriceMasterPartners",
  "getOzonCategoryList",
  "ozonRequest",
  "buildOzonAttributesTemplate",
  "buildOzonPricePreview",
  "buildOzonPricePayload",
  "sendOzonPricePayloadChunks",
  "buildOzonProductPreview",
  "buildOzonManualProductItem",
  "auditEntryProductIds",
  "publicLinkAuditEntry",
];

const depsDestructure = manualDeps.map((d) => `    ${d},`).join("\n");

const legacyCatalog = `"use strict";

function registerLegacyCatalogRoutes(app, deps) {
  const {
${depsDestructure}
  } = deps;

${routesBody}
}

module.exports = { registerLegacyCatalogRoutes };
`;

const registerPart = `const { registerLegacyCatalogRoutes } = require("./routes/legacy-catalog");

registerLegacyCatalogRoutes(app, {
  collectHealthDetails,
  readSnapshot,
  readHistory,
  pool,
  cleanLimit,
  cleanText,
  likeSearch,
  requireAdmin,
  shouldUsePostgresStorage,
  getPrisma,
  auditRowToEntry,
  jsonFallbackEnabled,
  toDateOrNull,
  logger,
  getPriceMasterSearchCache,
  setPriceMasterSearchCache,
  readAppSettings,
  readAudit,
  readAuditFiltered,
  normalizePriceMasterPrice,
  normalizeSupplierName,
  searchPriceMasterSnapshotPartners,
  searchPriceMasterSnapshotOffers,
  listBrandFallbackCandidates,
  getOzonAccountByTarget,
  listPriceMasterPartners,
  getOzonCategoryList,
  ozonRequest,
  buildOzonAttributesTemplate,
  buildOzonPricePreview,
  buildOzonPricePayload,
  sendOzonPricePayloadChunks,
  buildOzonProductPreview,
  buildOzonManualProductItem,
  auditEntryProductIds,
  publicLinkAuditEntry,
});
`;

fs.writeFileSync(path.join(root, "server/parts/02b-shared-helpers.js"), `${helpersBody}\n`, "utf8");
fs.writeFileSync(path.join(root, "routes/legacy-catalog.js"), legacyCatalog, "utf8");
fs.writeFileSync(path.join(root, "server/parts/02b-register-legacy-catalog.js"), `${registerPart}\n`, "utf8");
fs.unlinkSync(srcPath);

const sourcePath = path.join(root, "server/source.js");
let source = fs.readFileSync(sourcePath, "utf8");
source = source.replace(
  '  "02b-routes-catalog.js",',
  '  "02b-shared-helpers.js",\n  "02b-register-legacy-catalog.js",'
);
fs.writeFileSync(sourcePath, source, "utf8");

const bootstrapPath = path.join(root, "server/parts/01-bootstrap.js");
let bootstrap = fs.readFileSync(bootstrapPath, "utf8");
if (!bootstrap.includes("registerLegacyCatalogRoutes")) {
  bootstrap = bootstrap.replace(
    'const { registerUsersRoutes } = require("./routes/users");\n',
    'const { registerUsersRoutes } = require("./routes/users");\nconst { registerLegacyCatalogRoutes } = require("./routes/legacy-catalog");\n'
  );
  fs.writeFileSync(bootstrapPath, bootstrap, "utf8");
}

console.log("split-02b: done");
