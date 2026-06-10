const { after, test } = require("node:test");
const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const fs = require("node:fs/promises");
const path = require("node:path");
const { promisify } = require("node:util");
const request = require("supertest");
const { readServerSource } = require("../server/source");

const execFileAsync = promisify(execFile);

process.env.APP_PASSWORD = process.env.APP_PASSWORD || "smoke-test-password";
process.env.APP_SESSION_SECRET = process.env.APP_SESSION_SECRET || "smoke-test-session-secret-min-32-chars!";
process.env.APP_USER = process.env.APP_USER || "admin";
process.env.AUTO_ARCHIVE_ON_NO_LINKS = "true";
process.env.PUBLIC_BASE_URL = "http://localhost";
process.env.DISABLE_BACKGROUND_JOBS = "true";
process.env.BULLMQ_ENABLED = "false";
process.env.DB_MODE = "json";
process.env.DATABASE_URL = "";
process.env.JSON_FALLBACK_ENABLED = "true";

const appUsersPath = path.join(__dirname, "..", "data", "app-users.json");
const appDeletedUsersPath = path.join(__dirname, "..", "data", "app-users-deleted.json");
const appSettingsPath = path.join(__dirname, "..", "data", "app-settings.json");
const marketplaceAccountsPath = path.join(__dirname, "..", "data", "marketplace-accounts.json");
const warehousePath = path.join(__dirname, "..", "data", "warehouse.json");
const personalWarehousePath = path.join(__dirname, "..", "data", "personal-warehouse.json");
const operationJobsPath = path.join(__dirname, "..", "data", "operation-jobs.json");
const aiImageJobsPath = path.join(__dirname, "..", "data", "ai-image-jobs.json");
const auditLogPath = path.join(__dirname, "..", "data", "audit-log.jsonl");
const supplierPickingListPath = path.join(__dirname, "..", "data", "supplier-picking-list.json");
const financeStatePath = path.join(__dirname, "..", "data", "finance-state.json");

async function backupFile(filePath) {
  try {
    return await fs.readFile(filePath, "utf8");
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

async function restoreFile(filePath, content) {
  if (content === null) {
    await fs.unlink(filePath).catch((error) => {
      if (error.code !== "ENOENT") throw error;
    });
    return;
  }
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, content, "utf8");
}

const {
  app,
  resolveMarkupCoefficient,
  resolveAvailabilityPolicy,
  normalizeManagedSupplier,
  normalizePriceMasterSnapshotItemForPostgres,
  resolvePriceMasterRowCurrency,
  normalizePriceMasterPrice,
  calculateRubPrice,
  warehouseSupplierPurchaseRubPrice,
  managedSupplierMaps,
  supplierImpactProductIds,
  priceMasterChangeImpactProductIds,
  changedWarehouseProductIdsByAutomationFingerprint,
  backgroundAutomationProductIds,
  pickNoSupplierAutomationCandidates,
  pickSupplierRecoveryCandidates,
  summarizeSupplierRecoveryProducts,
  runNoSupplierMarketplaceAutomation,
  runSupplierRecoveryAutomation,
  pickWarehouseSupplier,
  pickWarehouseStockOnlySupplier,
  priceMasterSupplierPricingMeta,
  supplierUsesRubPriceMasterPricing,
  supplierUsesStockOnlyPricing,
  warehouseBrandMatches,
  normalizeWarehouseProduct,
  mergeProducts,
  applyOzonInfoToWarehouseProduct,
  productFromPostgres,
  marketplaceStateCodeFromPostgresRow,
  warehousePageProductMatches,
  warehousePagePostgresWhere,
  warehousePagePostgresPrimaryIdentityWhere,
  sortWarehouseProductsForSearch,
  preferWarehousePrimaryIdentityMatches,
  buildWarehouseSkuDiagnostics,
  addWarehousePageGroupSiblings,
  expandWarehouseProductsToGroups,
  buildWarehousePageProductGroups,
  linkedRecoveryCandidateProducts,
  summarizeWarehouseCounterStats,
  pickOzonDetailOfferIds,
  ozonProductNeedsDetailRefresh,
  isWeakOzonWarehouseProduct,
  pickWeakOzonProductIds,
  buildOzonStockPayloadItems,
  marketplaceHasPositiveStock,
  marketplaceOfferAutomationKey,
  shouldSendTargetStockForProduct,
  pickTargetStockSendProducts,
  priceAffectingSettingsChanged,
  warehouseLinkIdentityKey,
  pickSafeArticlePriceMasterRow,
  priceMasterRowMatchesLink,
  snapshotRowMatchesPriceMasterSearch,
  searchPriceMasterSnapshotJsonRows,
  priceMasterArticleCandidateScore,
  productLinkPostgresIdentityKey,
  dedupeProductLinkRows,
  warehouseProductLinkDetailsSignature,
  mergeWarehouseLinkForSave,
  warehouseProductLinksSignature,
  warehouseGroupLinkSignature,
  buildCommonWarehouseGroupLinks,
  syncWarehouseProductGroupLinks,
  marketplacePriceBreakdown,
  productConflict,
  canIgnoreStaleLinkSaveConflict,
  warehouseLinkHasMatchTarget,
  pickOzonCabinetListedPrice,
  shouldSkipWarehousePriceSend,
  isDuplicatePriceHistoryEntry,
  buildOzonPricePayload,
  isOzonResourceExhaustedError,
  isOzonPerItemPriceLimitError,
  isOzonOldPriceLessError,
  isExpectedMarketplaceArchiveBlock,
  extractOzonPriceResponseFailures,
  buildPriceRetryItem,
  YANDEX_MIN_VOLUME_ML,
  extractOzonYandexImportVolumesMl,
  collectYandexVolumeSearchText,
  assessYandexSmallVolume,
  isYandexSmallVolumeBlocked,
  ozonProductShouldMaterializeYandexSibling,
  ozonYandexImportBlockReasons,
  buildOzonYandexImportCandidate,
  summarizeOzonYandexImportPreview,
  productContentQuality,
  applyAiContentDraftToProduct,
  buildYandexOfferMapping,
  sendApprovedYandexProductContent,
  shouldPreferCompatibleOpenAiChatRequest,
  isCodexSaleAiProvider,
  effectiveOpenAiImageModel,
  openAiChatCompletionAttempts,
  isOpenAiBillingLimitError,
  isOpenAiRequestFormatError,
  getLocalYandexExportedOfferIdSet,
  buildYandexWarehouseProductFromOzonExport,
  countWarehouseProductGroups,
  warehouseGroupCountUsesFullCatalogScan,
  warehouseGroupCountUsesSqlFastPath,
  warehousePostgresWhereRequiresUnlinkedOnly,
  isWeakYandexWarehouseCard,
  patchYandexWarehouseProductFromOzonDonor,
  materializeYandexExportedProductsForWarehouse,
  marketplaceProductMarkupOverride,
  applyYandexPriceSendToWarehouse,
  buildYandexPriceUpdateFromOzonProduct,
  pickOzonProductStockForYandex,
  buildYandexStockUpdatePayload,
  buildYandexStockRestoreProducts,
  getYandexShopByTarget,
  productLooksArchived,
  pickArchivedStockRestoreCandidates,
  parseYandexCampaignIds,
  yandexStockShops,
  summarizeApiErrorPayload,
  apiPayloadHasErrors,
  sendYandexStocksForExportedOzonProducts,
  parseProtectedBrandList,
  buildYandexCleanupCandidate,
  summarizeYandexCleanupPreview,
  priceRetryQueueKey,
  findActiveDelayedPriceRetry,
  appendPriceHistoryRows,
  readPriceHistory,
  readPriceRetryQueue,
  writeWarehouse,
  writePriceRetryQueue,
  priceRetryQueuePath,
  unarchiveProductsOnMarketplaces,
  verifyOzonUnarchiveActions,
  processOzonUnarchiveQueue,
  readOzonUnarchiveQueue,
  writeOzonUnarchiveQueue,
  ozonUnarchiveQueuePath,
  ozonUnarchiveDateKey,
  createSupplierPickingRows,
  normalizeSupplierPickingRow,
  compareSupplierPickingRows,
  normalizeSupplierTrustFactor,
  normalizeSupplierOrderCutoff,
  supplierOrderCutoffPassed,
  supplierCartOrderScore,
  selectSupplierCartSupplierFromMatches,
  shutdownForTests,
} = require("../server.js");
const postgres = require("../lib/postgres.js");
const seedPostgres = require("../scripts/seed-postgres-from-json.cjs");

after(async () => {
  await shutdownForTests?.();
  await postgres.closePrisma?.();
});

test("GET /health", async () => {
  const res = await request(app).get("/health").expect(200);
  assert.equal(res.body.ok, true);
  assert.ok(res.body.service);
  assert.equal(typeof res.body.version, "string");
  assert.ok(res.body.version.length > 0);
  assert.ok(res.body.components);
  assert.equal(res.body.components.storage.mode, "json");
  assert.equal(res.body.components.redis.queueMode, "inline");
});

test("modern index is primary and legacy index keeps cache-busted assets", async () => {
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: process.env.APP_USER, password: process.env.APP_PASSWORD })
    .expect(200);
  const res = await agent.get("/").expect(200);
  assert.match(res.text, /\/app-modern\/assets\/index-[^"]+\.js/);
  assert.match(res.text, /\/app-modern\/assets\/index-[^"]+\.css/);
  assert.match(res.headers["cache-control"], /no-cache/);

  const legacyRes = await agent.get("/legacy").expect(200);
  assert.match(legacyRes.text, /\/app\.js\?v=[^"]+/);
  assert.match(legacyRes.text, /\/styles\.css\?v=[^"]+/);
  assert.match(legacyRes.headers["cache-control"], /no-cache/);
});

test("legacy page aliases keep old admin screens available", async () => {
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: process.env.APP_USER, password: process.env.APP_PASSWORD })
    .expect(200);
  const res = await agent.get("/legacy/settings").expect(200);
  assert.match(res.text, /settingsForm/);
  const operations = await agent.get("/legacy/operations").expect(200);
  assert.match(operations.text, /operationsList/);
});

test("modern PriceMaster link UI keeps drafts scoped and removes manual supplier search", async () => {
  const source = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "routes", "WarehousePage.tsx"), "utf8");
  assert.match(source, /draftScopeKey/);
  assert.match(source, /setDrafts\(\[\]\)/);
  assert.match(source, /<LinksPanel key=\{products\.map\(\(item\) => item\.id\)\.sort\(\)\.join\("\|"\)\}/);
  assert.match(source, /expectedLinksSignature: productLinksSignature\(item\)/);
  assert.doesNotMatch(source, /supplier=\$\{encodeURIComponent/);
  assert.doesNotMatch(source, /setSupplierFilter/);
  assert.match(source, /limit=100/);
  assert.match(source, /addAllSearchRows/);
  assert.match(source, /selectedLinkIds/);
  assert.match(source, /className="pm-result-title"/);
});

test("modern UI uses role-gating, logo branding, and group-level PM counts", async () => {
  const appSource = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "App.tsx"), "utf8");
  const warehouseSource = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "routes", "WarehousePage.tsx"), "utf8");
  const typesSource = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "types.ts"), "utf8");
  const stylesSource = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "styles.css"), "utf8");
  assert.match(appSource, /\/api\/session/);
  assert.match(appSource, /visibleNavItems/);
  assert.match(appSource, /headerRoutes/);
  assert.match(appSource, /"warehouse", "picking-list", "suppliers", "supplier-cart", "settings"/);
  assert.match(appSource, /\/app\/suppliers/);
  assert.match(appSource, /brand-logo/);
  assert.match(appSource, /WarehousePage isAdmin=\{isAdmin\}/);
  assert.match(warehouseSource, /WarehousePage\(\{ isAdmin = true \}/);
  assert.match(warehouseSource, /const groupLinkCount = uniqueLinks\(products\)\.length/);
  assert.match(warehouseSource, /isAdmin \? <GroupActions/);
  assert.match(warehouseSource, /isAdmin \? <QuickActions/);
  assert.match(warehouseSource, /isAdmin \? <AiImagesPanel/);
  assert.match(warehouseSource, /isAdmin \? <section className="detail-section">/);
  assert.match(warehouseSource, /\/api\/warehouse\/brands/);
  assert.match(warehouseSource, /datalist id="warehouse-brand-list"/);
  assert.match(warehouseSource, /\/ai-assistant/);
  assert.match(warehouseSource, /studioPhotoPresets/);
  assert.match(typesSource, /WarehouseBrandsSchema/);
  assert.match(typesSource, /AiAssistantResponseSchema/);
  assert.match(typesSource, /supplierAlternatives/);
  assert.match(stylesSource, /\.brand-logo/);
  assert.match(stylesSource, /\.brand-filter-wrap/);
  assert.match(stylesSource, /\.ai-assistant-card/);
  assert.match(stylesSource, /overflow-x: hidden/);
});

test("modern copy name action keeps only latin letters and digits", async () => {
  const commonSource = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "lib", "common.ts"), "utf8");
  const warehouseSource = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "routes", "WarehousePage.tsx"), "utf8");
  assert.match(commonSource, /copyableLatinProductName/);
  assert.match(commonSource, /\[\^A-Za-z0-9\]\+/);
  assert.match(warehouseSource, /copyableLatinProductName\(product\.name\) \|\| product\.offerId \|\| product\.sku/);

  const sanitize = (value) => String(value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  assert.equal(sanitize("12 Parfumeurs Le Charmeur Парфюмерная вода 100 мл"), "12 Parfumeurs Le Charmeur 100");
});

test("Codex Sale AI config supports env key aliases and image presets", async () => {
  const serverSource = readServerSource();
  const settingsSource = await fs.readFile(path.join(__dirname, "..", "frontend", "src", "routes", "SettingsPage.tsx"), "utf8");
  assert.match(serverSource, /CODEX_LB_API_KEY/);
  assert.match(serverSource, /CODEX_SALE_API_KEY/);
  assert.match(serverSource, /https:\/\/codex\.sale\/v1/);
  assert.match(serverSource, /gpt-image-2/);
  assert.match(serverSource, /aiImageStudioPresets/);
  assert.match(serverSource, /\/api\/warehouse\/products\/:id\/ai-assistant/);
  assert.match(settingsSource, /codexSaleAiPreset/);
  assert.match(settingsSource, /Codex Sale preset/);
  assert.match(settingsSource, /ToolsSettingsPanel/);
  assert.match(settingsSource, /\/app\/recovery-queue/);
});

test("warehouse page product groups merge marketplace variants by offer and manual group", () => {
  const products = [
    normalizeWarehouseProduct({ id: "ozon-1", marketplace: "ozon", offerId: "41059", name: "Ozon row", links: [{ id: "l1", article: "pm-1", supplierName: "A" }] }),
    normalizeWarehouseProduct({ id: "yandex-1", marketplace: "yandex", offerId: "41059", name: "Yandex row", links: [{ id: "l2", article: "pm-1", supplierName: "A" }] }),
    normalizeWarehouseProduct({ id: "ozon-2", marketplace: "ozon", offerId: "DIFFERENT", name: "Different" }),
    normalizeWarehouseProduct({ id: "manual-a", marketplace: "ozon", offerId: "A-1", manualGroupId: "manual-demo", name: "Manual A" }),
    normalizeWarehouseProduct({ id: "manual-b", marketplace: "yandex", offerId: "B-1", manualGroupId: "manual-demo", name: "Manual B" }),
  ];

  const groups = buildWarehousePageProductGroups(products);
  const group41059 = groups.find((group) => group.offerId === "41059");
  assert.ok(group41059);
  assert.equal(group41059.products.length, 2);
  assert.deepEqual(group41059.marketplaces, ["Ozon", "Yandex"]);
  assert.equal(group41059.links.length, 1);

  assert.equal(groups.filter((group) => group.offerId === "DIFFERENT").length, 1);
  const manualGroup = groups.find((group) => group.manualGroupId === "manual-demo");
  assert.ok(manualGroup);
  assert.equal(manualGroup.products.length, 2);
  assert.deepEqual(manualGroup.products.map((product) => product.offerId).sort(), ["A-1", "B-1"]);
});

test("old index path can still be opened directly", async () => {
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: process.env.APP_USER, password: process.env.APP_PASSWORD })
    .expect(200);
  const res = await agent.get("/legacy/index.html").expect(200);
  assert.match(res.text, /\/app\.js\?v=[^"]+/);
  assert.match(res.text, /\/styles\.css\?v=[^"]+/);
  assert.match(res.headers["cache-control"], /no-cache/);
});

test("GET /health deep exposes operational component details", async () => {
  const previousHost = process.env.PM_DB_HOST;
  const previousName = process.env.PM_DB_NAME;
  try {
    delete process.env.PM_DB_HOST;
    delete process.env.PM_DB_NAME;
    const res = await request(app).get("/health?deep=1").expect(200);
    assert.equal(typeof res.body.ok, "boolean");
    assert.equal(res.body.components.storage.mode, "json");
    assert.equal(res.body.components.postgres.enabled, false);
    assert.equal(res.body.components.pricemaster.configured, false);
    assert.equal(res.body.components.redis.queueMode, "inline");
    assert.equal(typeof res.body.components.ozon.accounts, "number");
  } finally {
    if (previousHost === undefined) delete process.env.PM_DB_HOST;
    else process.env.PM_DB_HOST = previousHost;
    if (previousName === undefined) delete process.env.PM_DB_NAME;
    else process.env.PM_DB_NAME = previousName;
  }
});

test("Ozon to Yandex import blocks forbidden names and small volumes", () => {
  assert.equal(YANDEX_MIN_VOLUME_ML, 20);
  assert.deepEqual(extractOzonYandexImportVolumesMl("Ex Nihilo Blue 7,5 мл"), [7.5]);
  assert.deepEqual(extractOzonYandexImportVolumesMl("Парфюмерная вода 20 ml"), [20]);
  assert.deepEqual(extractOzonYandexImportVolumesMl("Sample 1.8мл"), [1.8]);
  assert.deepEqual(extractOzonYandexImportVolumesMl("Travel 2ml"), [2]);
  assert.deepEqual(extractOzonYandexImportVolumesMl("Набор 3x7ml"), [7]);
  assert.deepEqual(extractOzonYandexImportVolumesMl("2,5мл"), [2.5]);
  assert.deepEqual(extractOzonYandexImportVolumesMl("10мл"), [10]);

  assert.equal(assessYandexSmallVolume("Creed 7 ml").blocked, true);
  assert.equal(assessYandexSmallVolume("Creed 20 ml").blocked, false);
  assert.equal(isYandexSmallVolumeBlocked({
    name: "Hidden volume",
    ozon: { attributes: [{ name: "Объем", value: "15 мл" }] },
  }), true);
  assert.equal(ozonProductShouldMaterializeYandexSibling({
    offerId: "SMALL-1",
    name: "Sample 10 мл",
    exports: { yandex: { status: "sent" } },
  }), false);
  assert.equal(ozonProductShouldMaterializeYandexSibling({
    offerId: "OK-1",
    name: "Sample 50 мл",
    exports: { yandex: { status: "sent" } },
  }), true);

  assert.ok(ozonYandexImportBlockReasons({ name: "Ex Nihilo 15 мл", ozon: { vendor: "Ex Nihilo" } }).some((reason) => reason.includes("20 мл")));
  assert.ok(ozonYandexImportBlockReasons({ name: "Отливант Creed Aventus 50 мл", ozon: { vendor: "Creed" } }).some((reason) => reason.includes("Отливант")));
  assert.ok(ozonYandexImportBlockReasons({ name: "Creed Aventus без коробки 100 мл", ozon: { vendor: "Creed" } }).some((reason) => reason.includes("без коробки")));
  assert.deepEqual(ozonYandexImportBlockReasons({ name: "Creed Aventus 20 мл", ozon: { vendor: "Creed" } }), []);
});

test("warehouse grouped counter and yandex media repair helpers", () => {
  assert.equal(warehouseGroupCountUsesFullCatalogScan({ q: "", linked: "all", marketplace: "all", state: "all" }), true);
  assert.equal(warehouseGroupCountUsesFullCatalogScan({ q: "VILHELM", linked: "all", marketplace: "all", state: "all" }), false);
  assert.equal(warehouseGroupCountUsesFullCatalogScan({ q: "", linked: "ready", marketplace: "all", state: "all" }), false);
  assert.equal(warehouseGroupCountUsesSqlFastPath({ q: "", linked: "unlinked", marketplace: "all", state: "all" }), true);
  assert.equal(warehouseGroupCountUsesSqlFastPath({ q: "", linked: "linked", marketplace: "all", state: "all" }), true);
  assert.equal(warehouseGroupCountUsesSqlFastPath({ q: "", linked: "ready", marketplace: "all", state: "all" }), false);
  assert.equal(warehouseGroupCountUsesSqlFastPath({ q: "SKU", linked: "unlinked", marketplace: "all", state: "all" }), false);
  assert.equal(
    warehousePostgresWhereRequiresUnlinkedOnly(warehousePagePostgresWhere({ linked: "unlinked" })),
    true,
  );
  assert.equal(
    warehousePostgresWhereRequiresUnlinkedOnly(warehousePagePostgresWhere({ linked: "linked" })),
    false,
  );
  assert.equal(countWarehouseProductGroups([
    { id: "ozon-1", marketplace: "ozon", offerId: "SKU-1", raw: { manualGroupId: "auto-pair-ozon-1" } },
    { id: "yandex-1", marketplace: "yandex", offerId: "SKU-1", raw: { manualGroupId: "auto-pair-ozon-1", yandex: { extra: { sourceProductId: "ozon-1" } } } },
    { id: "ozon-2", marketplace: "ozon", offerId: "SKU-2" },
  ]), 2);
  assert.equal(countWarehouseProductGroups([
    { id: "ozon-1", marketplace: "ozon", offerId: "SKU-1", raw: { manualGroupId: "auto-pair-ozon-1" } },
    { id: "yandex-1", marketplace: "yandex", offerId: "SKU-1" },
    { id: "ozon-2", marketplace: "ozon", offerId: "SKU-2" },
  ]), 2);

  const { buildWarehouseDetailProductsFromPageWarehouse } = require("../server.js");
  assert.equal(typeof buildWarehouseDetailProductsFromPageWarehouse, "function");

  const { applyGroupLinkInheritanceForPage, propagateGroupSupplierContextForPage } = require("../server.js");
  const inherited = applyGroupLinkInheritanceForPage([
    { id: "ozon-1", marketplace: "ozon", offerId: "SKU-1", links: [{ id: "l1", article: "PM-1", supplierName: "Supplier A" }] },
    { id: "yandex-1", marketplace: "yandex", offerId: "SKU-1", links: [] },
  ]);
  assert.equal(inherited.find((item) => item.id === "yandex-1")?.links?.length, 1);

  const propagated = propagateGroupSupplierContextForPage([
    {
      id: "ozon-1",
      marketplace: "ozon",
      offerId: "SKU-2",
      links: [{ id: "l1", article: "PM-2", supplierName: "Supplier B" }],
      selectedSupplier: { article: "PM-2", supplierName: "Supplier B", price: 1200 },
      targetPrice: 2500,
      nextPrice: 2500,
    },
    {
      id: "yandex-1",
      marketplace: "yandex",
      offerId: "SKU-2",
      links: [{ id: "l1", article: "PM-2", supplierName: "Supplier B" }],
      selectedSupplier: null,
      targetPrice: 0,
    },
  ]);
  const yandexSibling = propagated.find((item) => item.id === "yandex-1");
  assert.equal(yandexSibling?.selectedSupplier?.article, "PM-2");
  assert.equal(yandexSibling?.targetPrice, 2500);

  const propagatedFromLinks = propagateGroupSupplierContextForPage([
    {
      id: "ozon-link",
      marketplace: "ozon",
      offerId: "SKU-3",
      links: [{ id: "l3", article: "PM-3", supplierName: "Supplier C", price: 900 }],
      selectedSupplier: null,
      nextPrice: 1800,
    },
    {
      id: "yandex-link",
      marketplace: "yandex",
      offerId: "SKU-3",
      links: [{ id: "l3", article: "PM-3", supplierName: "Supplier C", price: 900 }],
      selectedSupplier: null,
    },
  ]);
  const yandexFromLinks = propagatedFromLinks.find((item) => item.id === "yandex-link");
  assert.equal(yandexFromLinks?.selectedSupplier?.supplierName, "Supplier C");
  assert.equal(yandexFromLinks?.nextPrice, 1800);

  const propagatedStock = propagateGroupSupplierContextForPage([
    {
      id: "ozon-stock",
      marketplace: "ozon",
      offerId: "SKU-4",
      links: [{ id: "l4", article: "PM-4", supplierName: "Supplier D" }],
      selectedSupplier: { article: "PM-4", supplierName: "Supplier D", price: 500 },
      targetStock: 3,
    },
    {
      id: "yandex-stock",
      marketplace: "yandex",
      offerId: "SKU-4",
      links: [{ id: "l4b", article: "PM-4", supplierName: "Supplier D" }],
      selectedSupplier: null,
      targetStock: 0,
    },
  ]);
  assert.equal(propagatedStock.find((item) => item.id === "yandex-stock")?.targetStock, 3);

  const weakYandex = normalizeWarehouseProduct({
    id: "yandex-weak",
    marketplace: "yandex",
    offerId: "SKU-1",
    yandex: { vendor: "", pictures: [] },
  });
  assert.equal(isWeakYandexWarehouseCard(weakYandex), true);

  const patched = patchYandexWarehouseProductFromOzonDonor(weakYandex, normalizeWarehouseProduct({
    id: "ozon-1",
    marketplace: "ozon",
    offerId: "SKU-1",
    imageUrl: "https://example.test/ozon.jpg",
    ozon: { vendor: "Creed", images: ["https://example.test/ozon.jpg"] },
  }));
  assert.equal(patched.yandex.vendor, "Creed");
  assert.equal(patched.imageUrl, "https://example.test/ozon.jpg");
  assert.ok(Number(patched.yandex.extra?.weightDimensions?.weight) > 0);
  const repairedOffer = buildYandexOfferMapping(patched).offer;
  assert.ok(repairedOffer.weightDimensions?.length > 0);
  assert.ok(repairedOffer.pictures?.length > 0);

  const cleanup = buildYandexCleanupCandidate({
    offer: { offerId: "SMALL-1", name: "Sample 10 мл", vendor: "Brand" },
  }, { id: "yandex", businessId: "biz-1" }, ["Brand"]);
  assert.equal(cleanup.smallVolume, true);
  assert.equal(cleanup.action, "delete");
});

test("warehouse group detail light enrich skips unlinked PriceMaster batch", async () => {
  const { buildWarehouseDetailProductsFromPageWarehouse } = require("../server.js");
  const detail = await buildWarehouseDetailProductsFromPageWarehouse({
    products: [
      { id: "ozon-unlinked", marketplace: "ozon", offerId: "SKU-U1", links: [] },
      { id: "yandex-unlinked", marketplace: "yandex", offerId: "SKU-U1", links: [] },
    ],
    suppliers: [],
  }, { refreshPrices: false, usdRate: 95 });
  assert.equal(detail.length, 2);
  assert.equal(detail[0].offerId, "SKU-U1");
  assert.deepEqual(detail[0].links, []);
});

test("Ozon to Yandex import blocks unsafe non-perfume and low quality cards", () => {
  assert.ok(ozonYandexImportBlockReasons({ name: "помада", ozon: { vendor: "Magic" } }).some((reason) => reason.includes("Категория")));
  assert.ok(ozonYandexImportBlockReasons({ name: "свеча ароматическая", ozon: { vendor: "Magic" } }).some((reason) => reason.includes("Категория")));
  assert.ok(ozonYandexImportBlockReasons({ name: "Краска", ozon: { vendor: "Magic" } }).some((reason) => reason.includes("Категория")));
  assert.ok(ozonYandexImportBlockReasons({ name: "щзхщц", ozon: { vendor: "Magic" } }).some((reason) => reason.includes("Подозрительное")));
  assert.ok(ozonYandexImportBlockReasons({ name: "-0-", ozon: { vendor: "Magic" } }).some((reason) => reason.includes("Подозрительное")));
  assert.deepEqual(ozonYandexImportBlockReasons({ name: "Giorgio Armani Si Passione Eclat Парфюмерная вода 90 мл" }), []);
  assert.ok(ozonYandexImportBlockReasons({ name: "Creed Aventus" }).some((reason) => reason.includes("нет объема")));
  assert.ok(ozonYandexImportBlockReasons({ name: "парфюмерная вода для мужчин" }).some((reason) => reason.includes("общее название")));
  assert.ok(ozonYandexImportBlockReasons({ name: "Сильной фиксации 15 350 мл", ozon: { vendor: "Magic" } }).some((reason) => reason.includes("Подозрительный объем")));
  assert.deepEqual(ozonYandexImportBlockReasons({ name: "Creed Aventus 100 мл", ozon: { vendor: "Creed" }, marketplaceState: { code: "archived", archived: true } }), []);
});

test("Ozon to Yandex import candidate exposes eligibility summary", () => {
  const ready = buildOzonYandexImportCandidate(normalizeWarehouseProduct({
    id: "ozon-ready",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-100",
    productId: "100",
    name: "Creed Aventus 100 мл",
    imageUrl: "https://example.test/image.jpg",
    marketplacePrice: 4500,
    marketplaceState: { code: "active", visibility: "VISIBLE" },
    ozon: {
      name: "Creed Aventus 100 мл",
      vendor: "Creed",
      description: "Описание",
      categoryId: 123,
      images: ["https://example.test/image.jpg"],
      price: 4500,
    },
  }));
  const blocked = buildOzonYandexImportCandidate(normalizeWarehouseProduct({
    id: "ozon-blocked",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-7",
    productId: "7",
    name: "Creed Aventus 7.5 ml",
    imageUrl: "https://example.test/image.jpg",
    marketplacePrice: 1500,
    marketplaceState: { code: "active", visibility: "VISIBLE" },
    ozon: {
      name: "Creed Aventus 7.5 ml",
      vendor: "Creed",
      description: "Описание",
      categoryId: 123,
      images: ["https://example.test/image.jpg"],
      price: 1500,
    },
  }));

  assert.equal(ready.eligible, true);
  assert.equal(blocked.eligible, false);
  assert.equal(summarizeOzonYandexImportPreview([ready, blocked]).eligible, 1);
  assert.equal(summarizeOzonYandexImportPreview([ready, blocked]).blocked, 1);
});

test("AI content draft improves Yandex readiness without touching stock or price", () => {
  const source = normalizeWarehouseProduct({
    id: "ai-content-1",
    marketplace: "ozon",
    target: "ozon",
    offerId: "AI-SKU-1",
    name: "Giorgio Armani Si Passione Парфюмерная вода 90 мл",
    currentPrice: 5000,
    targetStock: 3,
    ozon: {
      offerId: "AI-SKU-1",
      name: "Giorgio Armani Si Passione Парфюмерная вода 90 мл",
      vendor: "Giorgio Armani",
      description: "Парфюмерная вода",
      marketCategoryId: 12345,
      primaryImage: "https://example.test/image.jpg",
    },
  });
  assert.equal(productContentQuality(source, "yandex").reasons.includes("short_description"), true);
  const enhanced = applyAiContentDraftToProduct(source, {
    name: "Giorgio Armani Si Passione парфюмерная вода женская 90 мл",
    vendor: "Giorgio Armani",
    description: "Женская парфюмерная вода Giorgio Armani Si Passione с выразительным цветочно-фруктовым характером. Подходит для ежедневного образа и вечернего выхода, раскрывается ярко и аккуратно, сохраняя узнаваемый стиль бренда.",
    bulletPoints: ["90 мл", "женский аромат"],
  }, "yandex");
  const quality = productContentQuality(enhanced, "yandex");
  assert.equal(quality.ready, true);
  assert.equal(buildYandexOfferMapping(enhanced).ready, true);
  assert.equal(enhanced.currentPrice, 5000);
  assert.equal(enhanced.targetStock, 3);
});

test("Ozon to Yandex import blocks offers that already exist in Yandex", () => {
  const existing = buildOzonYandexImportCandidate(normalizeWarehouseProduct({
    id: "ozon-existing",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-EXISTS",
    productId: "101",
    name: "Giorgio Armani Si Passione Eclat Парфюмерная вода 90 мл",
    imageUrl: "https://example.test/image.jpg",
    marketplacePrice: 6500,
    marketplaceState: { code: "active", visibility: "VISIBLE" },
    ozon: {
      name: "Giorgio Armani Si Passione Eclat Парфюмерная вода 90 мл",
      vendor: "Giorgio Armani",
      description: "Описание",
      categoryId: 123,
      images: ["https://example.test/image.jpg"],
      price: 6500,
    },
  }), { yandexExistingOfferIds: new Set(["oz-exists"]) });

  assert.equal(existing.existingInYandex, true);
  assert.equal(existing.eligible, false);
  assert.equal(summarizeOzonYandexImportPreview([existing]).existingInYandex, 1);
});

test("Ozon to Yandex import treats locally exported Yandex products as existing", () => {
  const set = getLocalYandexExportedOfferIdSet([
    { offerId: "SKU-1", exports: { yandex: { status: "sent" } } },
    { offerId: "SKU-2", exports: { yandexShop: { status: "sent", targetName: "Yandex Market" } } },
    { offerId: "SKU-3", exports: { yandex: { status: "failed" } } },
    { offerId: "SKU-4", marketplace: "yandex" },
    { offerId: "", exports: { yandex: { status: "sent" } } },
  ]);

  assert.equal(set.has("sku-1"), true);
  assert.equal(set.has("sku-2"), true);
  assert.equal(set.has("sku-3"), false);
  assert.equal(set.has("sku-4"), true);
  assert.equal(set.size, 3);
});

test("Ozon to Yandex import creates a Yandex warehouse variant after export", () => {
  const ozonProduct = normalizeWarehouseProduct({
    id: "ozon-source",
    marketplace: "ozon",
    target: "ozon",
    offerId: "SKU-YA-1",
    productId: "12345",
    name: "Creed Aventus 100 ml",
    imageUrl: "https://example.test/creed.jpg",
    marketplacePrice: 12000,
    targetPrice: 11990,
    marketplaceState: { code: "active", stock: 4 },
    ozon: {
      vendor: "Creed",
      description: "Perfume",
      images: ["https://example.test/creed-2.jpg"],
    },
    links: [{ article: "PM-1", partnerId: "10", supplierName: "Supplier" }],
  });
  const yandexProduct = buildYandexWarehouseProductFromOzonExport(
    ozonProduct,
    { id: "yandex-main", name: "Yandex Main" },
    { status: "sent", sentAt: "2026-05-14T10:00:00.000Z", price: 11990, stock: 4 },
  );
  const merged = mergeProducts([ozonProduct], [yandexProduct]);

  assert.equal(yandexProduct.marketplace, "yandex");
  assert.equal(yandexProduct.target, "yandex-main");
  assert.equal(yandexProduct.offerId, "SKU-YA-1");
  assert.equal(yandexProduct.marketplacePrice, 11990);
  assert.equal(yandexProduct.currentPrice, 11990);
  assert.equal(yandexProduct.targetPrice, 11990);
  assert.equal(yandexProduct.marketplaceState.stock, 4);
  assert.equal(yandexProduct.lastYandexPriceSend.status, "success");
  assert.equal(yandexProduct.lastYandexPriceSend.requestedPrice, 11990);
  assert.equal(yandexProduct.yandex.vendor, "Creed");
  assert.equal(yandexProduct.links.length, 1);
  assert.deepEqual(new Set(merged.map((item) => item.marketplace)), new Set(["ozon", "yandex"]));
});

test("mergeProducts keeps the live Yandex price when the imported row is missing it", () => {
  const currentYandex = normalizeWarehouseProduct({
    id: "yandex-live",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-LIVE",
    name: "Live Yandex product",
    currentPrice: 14500,
    targetPrice: 14500,
    marketplacePrice: 14500,
    yandex: {
      offerId: "SKU-YA-LIVE",
      name: "Live Yandex product",
      price: 14500,
      extra: { exportedFrom: "ozon" },
    },
    lastYandexPriceSend: {
      status: "success",
      at: "2026-05-14T10:00:00.000Z",
      requestedPrice: 14500,
      cabinetPriceAtSend: 14100,
    },
  });
  const importedYandex = normalizeWarehouseProduct({
    id: "yandex-live-import",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-LIVE",
    name: "Live Yandex product",
    yandex: {
      offerId: "SKU-YA-LIVE",
      name: "Live Yandex product",
      extra: { exportedFrom: "ozon" },
    },
  });

  const merged = mergeProducts([currentYandex], [importedYandex]);
  assert.equal(merged.length, 1);
  assert.equal(merged[0].marketplace, "yandex");
  assert.equal(merged[0].currentPrice, 14500);
  assert.equal(merged[0].targetPrice, 14500);
  assert.equal(merged[0].marketplacePrice, 14500);
  assert.equal(merged[0].lastYandexPriceSend.status, "success");
});

test("mergeProducts keeps the live Yandex price when an Ozon-derived clone brings stale values", () => {
  const currentYandex = normalizeWarehouseProduct({
    id: "yandex-live-locked",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-LOCKED",
    name: "Locked Yandex product",
    currentPrice: 18100,
    targetPrice: 18100,
    marketplacePrice: 18100,
    yandex: {
      offerId: "SKU-YA-LOCKED",
      name: "Locked Yandex product",
      price: 18100,
      extra: { exportedFrom: "ozon" },
    },
    lastYandexPriceSend: {
      status: "success",
      at: "2026-05-14T11:00:00.000Z",
      requestedPrice: 18100,
      cabinetPriceAtSend: 17500,
    },
  });
  const importedClone = normalizeWarehouseProduct({
    id: "yandex-live-locked-import",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-LOCKED",
    name: "Locked Yandex product",
    currentPrice: 14200,
    targetPrice: 14200,
    marketplacePrice: 14200,
    targetStock: 7,
    yandex: {
      offerId: "SKU-YA-LOCKED",
      name: "Locked Yandex product",
      price: 14200,
      extra: { exportedFrom: "ozon" },
    },
  });

  const merged = mergeProducts([currentYandex], [importedClone]);
  assert.equal(merged.length, 1);
  assert.equal(merged[0].currentPrice, 18100);
  assert.equal(merged[0].targetPrice, 18100);
  assert.equal(merged[0].marketplacePrice, 18100);
  assert.equal(merged[0].yandex.price, 18100);
  assert.equal(merged[0].targetStock, 7);
  assert.equal(merged[0].lastYandexPriceSend.status, "success");
});

test("mergeProducts keeps manual Yandex markup on live rows", () => {
  const currentYandex = normalizeWarehouseProduct({
    id: "yandex-markup-live",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-MARKUP-LIVE",
    name: "Markup Yandex product",
    markup: 1.85,
    markupSource: "manual",
    yandex: {
      offerId: "SKU-YA-MARKUP-LIVE",
      name: "Markup Yandex product",
      price: 12900,
      extra: { exportedFrom: "ozon", manualMarkup: true },
    },
    lastYandexPriceSend: {
      status: "success",
      at: "2026-05-14T12:00:00.000Z",
      requestedPrice: 12900,
      cabinetPriceAtSend: 12500,
    },
  });
  const importedClone = normalizeWarehouseProduct({
    id: "yandex-markup-live-import",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-MARKUP-LIVE",
    name: "Markup Yandex product",
    markup: 0,
    yandex: {
      offerId: "SKU-YA-MARKUP-LIVE",
      name: "Markup Yandex product",
      extra: { exportedFrom: "ozon" },
    },
  });

  const merged = mergeProducts([currentYandex], [importedClone]);
  assert.equal(merged.length, 1);
  assert.equal(merged[0].markup, 1.85);
  assert.equal(merged[0].lastYandexPriceSend.status, "success");
});

test("warehouse write materializes Yandex rows and keeps the real shop target", async () => {
  const previousWarehouse = await backupFile(warehousePath);
  const previousAccounts = await backupFile(marketplaceAccountsPath);
  const sourceWarehouse = {
    createdAt: "2026-05-14T10:00:00.000Z",
    updatedAt: "2026-05-14T10:00:00.000Z",
    products: [
      normalizeWarehouseProduct({
        id: "ozon-source",
        marketplace: "ozon",
        target: "ozon",
        offerId: "SKU-YA-2",
        productId: "54321",
        name: "Creed Aventus 100 ml",
        imageUrl: "https://example.test/creed.jpg",
        marketplacePrice: 12000,
        marketplaceState: { code: "active", stock: 4 },
        exports: {
          yandex: { status: "sent", sentAt: "2026-05-14T10:00:00.000Z" },
        },
      }),
    ],
    suppliers: [],
  };

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({
      accounts: [
        {
          id: "yandex-main",
          marketplace: "yandex",
          name: "Yandex Main",
          apiKey: "token",
          businessId: "171782339",
          campaignId: "128820967",
          syncEnabled: true,
        },
      ],
    }, null, 2));
    const warehouse = await writeWarehouse(sourceWarehouse, { writePostgres: false });
    const yandexProduct = warehouse.products.find((product) => product.marketplace === "yandex" && product.offerId === "SKU-YA-2");

    assert.ok(yandexProduct, "expected a materialized Yandex product");
    assert.equal(yandexProduct.target, "yandex-main");
    assert.equal(yandexProduct.yandex.extra.shopId, "yandex-main");
    assert.equal(yandexProduct.yandex.extra.businessId, "171782339");
    assert.equal(yandexProduct.yandex.extra.campaignId, "128820967");
  } finally {
    await restoreFile(warehousePath, previousWarehouse);
    await restoreFile(marketplaceAccountsPath, previousAccounts);
  }
});

test("Ozon to Yandex stock sync uses Ozon stock from state and warehouses", () => {
  assert.equal(pickOzonProductStockForYandex({ marketplaceState: { stock: 7, warehouses: [{ stock: 1 }] } }), 7);
  assert.equal(pickOzonProductStockForYandex({ marketplaceState: { warehouses: [{ stock: 2 }, { present: 3 }] } }), 5);
  assert.equal(pickOzonProductStockForYandex({ marketplaceState: { stock: 0 } }), 0);
  assert.equal(pickOzonProductStockForYandex({ marketplaceState: { code: "archived", stock: 9, warehouses: [{ stock: 9 }] } }), 0);
  assert.equal(pickOzonProductStockForYandex({ marketplaceState: { visibility: "ARCHIVED", stock: 4 } }), 0);
});

test("Yandex stock update payload uses campaign stock format", () => {
  assert.deepEqual(buildYandexStockUpdatePayload([
    { offerId: "SKU-1", stock: 3 },
    { offer_id: "SKU-2", stock: 0 },
    { offerId: "  ", stock: 10 },
  ], "2026-05-14T10:00:00.000Z"), {
    skus: [
      { sku: "SKU-1", items: [{ type: "FIT", count: 3, updatedAt: "2026-05-14T10:00:00.000Z" }] },
      { sku: "SKU-2", items: [{ type: "FIT", count: 0, updatedAt: "2026-05-14T10:00:00.000Z" }] },
    ],
  });
});

test("Yandex stock restore candidates ignore zero stock rows and dedupe by business", () => {
  const restoreProducts = buildYandexStockRestoreProducts([
    { offerId: "SKU-1", stock: 4 },
    { offer_id: "SKU-2", targetStock: 0 },
    { offerId: "SKU-3", stock: 2 },
  ], [
    { id: "yandex-main", apiKey: "token-1", businessId: "171782339", campaignId: "128820967" },
    { id: "yandex-second", apiKey: "token-2", businessId: "171782339", campaignId: "149026853" },
    { id: "yandex-third", apiKey: "token-3", businessId: "222222222", campaignId: "149079105" },
  ]);

  assert.equal(restoreProducts.length, 4);
  assert.deepEqual(restoreProducts.map((item) => item.target), [
    "yandex-main",
    "yandex-main",
    "yandex-third",
    "yandex-third",
  ]);
  assert.deepEqual(restoreProducts.map((item) => item.offerId), [
    "SKU-1",
    "SKU-3",
    "SKU-1",
    "SKU-3",
  ]);
  assert.ok(restoreProducts.every((item) => String(item.id || "").startsWith("yandex-")));
});

test("archived stock restore candidates include unlinked archived products", () => {
  const products = [
    normalizeWarehouseProduct({
      id: "ozon-archived",
      target: "ozon",
      marketplace: "ozon",
      offerId: "OZ-1",
      productId: "123",
      marketplaceState: { code: "archived" },
      links: [],
    }),
    normalizeWarehouseProduct({
      id: "yandex-archived",
      target: "yandex-06c2112c",
      marketplace: "yandex",
      offerId: "YA-1",
      marketplaceState: { archived: true },
      links: [],
    }),
    normalizeWarehouseProduct({
      id: "ozon-active",
      target: "ozon",
      marketplace: "ozon",
      offerId: "OZ-2",
      productId: "124",
      marketplaceState: { code: "active" },
      links: [],
    }),
  ];
  assert.equal(productLooksArchived(products[0]), true);
  const picked = pickArchivedStockRestoreCandidates(products, { limit: 10 });
  assert.deepEqual(picked.map((item) => item.id), ["ozon-archived", "yandex-archived"]);
});

test("manual sellable archived restore prevents no-link auto archive", () => {
  const product = normalizeWarehouseProduct({
    id: "manual-sellable",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-3",
    productId: "125",
    marketplaceState: { code: "active", stock: 3 },
    noSupplierAutomation: { manualSellableAt: "2026-01-01T00:00:00.000Z" },
    links: [],
  });
  const result = pickNoSupplierAutomationCandidates([product], { includeNoLinks: true });
  assert.equal(result.toArchive.length, 0);
  assert.equal(result.toZeroStock.length, 0);
});

test("Yandex stock shops use only the configured stock campaign from comma-separated ids", () => {
  assert.deepEqual(parseYandexCampaignIds("128820967,149026853; 149079105 149079105"), [
    "128820967",
    "149026853",
    "149079105",
  ]);
  const shops = yandexStockShops([{
    id: "yandex-main",
    name: "Yandex",
    apiKey: "token",
    businessId: "171782339",
    campaignId: "128820967,149026853",
  }]);
  assert.equal(shops.length, 1);
  assert.equal(shops[0].campaignId, "128820967");
  const expressOnly = yandexStockShops([{
    id: "yandex-express",
    name: "Yandex Express",
    apiKey: "token",
    businessId: "171782339",
    campaignId: "149026853",
  }]);
  assert.equal(expressOnly.length, 0);
});

test("Yandex API error summary includes nested response details", () => {
  assert.equal(summarizeApiErrorPayload({
    code: "BAD_REQUEST",
    errors: [
      { message: "offerMappings[0].offer.name is invalid" },
      { code: "INVALID_CATEGORY" },
    ],
  }, "fallback"), "BAD_REQUEST; offerMappings[0].offer.name is invalid; INVALID_CATEGORY");
});

test("Yandex API error summary unwraps operation-level stock errors", () => {
  const payload = {
    status: "ERROR",
    result: {
      status: "ERROR",
      errors: [
        { code: "VALIDATION_ERROR", message: "operation failed with errors" },
        { sku: "SKU-1", message: "Offer is archived" },
      ],
    },
  };

  assert.equal(apiPayloadHasErrors(payload), true);
  assert.equal(
    summarizeApiErrorPayload(payload, "fallback"),
    "operation failed with errors; VALIDATION_ERROR; Offer is archived; SKU-1",
  );
});

test("Yandex exported stock stage fails loudly without campaign id", async () => {
  const result = await sendYandexStocksForExportedOzonProducts([
    normalizeWarehouseProduct({
      id: "p1",
      offerId: "SKU-1",
      marketplaceState: { code: "active", stock: 4 },
    }),
  ], {
    stockShops: [{ id: "shop-1", name: "Shop 1", apiKey: "token", businessId: 123 }],
    existingOfferIds: new Set(["sku-1"]),
  });

  assert.equal(result.ok, false);
  assert.equal(result.sent, 0);
  assert.equal(result.failed, 1);
  assert.equal(result.results[0].stage, "stock");
  assert.match(result.results[0].error, /campaignId/);
});

test("Ozon to Yandex price update uses exported offer price", () => {
  const item = buildYandexPriceUpdateFromOzonProduct(normalizeWarehouseProduct({
    id: "ozon-yandex-price",
    marketplace: "ozon",
    offerId: "SKU-PRICE-1",
    name: "Giorgio Armani Si Passione Eclat Парфюмерная вода 90 мл",
    ozon: {
      name: "Giorgio Armani Si Passione Eclat Парфюмерная вода 90 мл",
      price: 4510.4,
      vendor: "Giorgio Armani",
      marketCategoryId: 123,
      images: ["https://example.test/image.jpg"],
      description: "Описание",
    },
  }));
  assert.equal(item.offerId, "SKU-PRICE-1");
  assert.deepEqual(item.price, { value: 4510, currencyId: "RUR" });
});

test("Ozon to Yandex price update does not reuse Ozon price when Yandex calculation is required", () => {
  const item = buildYandexPriceUpdateFromOzonProduct(
    normalizeWarehouseProduct({
      id: "ozon-yandex-no-fallback",
      marketplace: "ozon",
      offerId: "SKU-PRICE-NO-FALLBACK",
      name: "Source product",
      ozon: {
        name: "Source product",
        price: 4510.4,
        vendor: "Source Brand",
      },
    }),
    { allowSourceFallback: false },
  );

  assert.equal(item, null);
});

test("Yandex price update prefers the Yandex variant price when it exists", () => {
  const item = buildYandexPriceUpdateFromOzonProduct(
    normalizeWarehouseProduct({
      id: "ozon-yandex-price-source",
      marketplace: "ozon",
      offerId: "SKU-PRICE-2",
      name: "Source product",
      ozon: {
        name: "Source product",
        price: 4510.4,
        vendor: "Source Brand",
        marketCategoryId: 123,
        images: ["https://example.test/source.jpg"],
        description: "Source description",
      },
    }),
    {
      yandexProduct: normalizeWarehouseProduct({
        id: "yandex-price-row",
        marketplace: "yandex",
        target: "yandex-01",
        offerId: "SKU-PRICE-2",
        name: "Yandex product",
        currentPrice: 8600,
        targetPrice: 7125,
        targetStock: 3,
        yandex: {
          name: "Yandex product",
          vendor: "Yandex Brand",
          marketCategoryId: 123,
          pictures: ["https://example.test/yandex.jpg"],
          description: "Yandex description",
          price: 7125,
        },
      }),
    },
  );
  assert.equal(item.offerId, "SKU-PRICE-2");
  assert.deepEqual(item.price, { value: 7125, currencyId: "RUR" });
});

test("successful Yandex price send updates the local warehouse variant price", () => {
  const warehouse = {
    products: [
      normalizeWarehouseProduct({
        id: "ya-local-price",
        marketplace: "yandex",
        target: "yandex-shop",
        offerId: "SKU-YA-LOCAL",
        name: "Yandex local row",
        marketplacePrice: 5000,
        currentPrice: 5000,
        yandex: { offerId: "SKU-YA-LOCAL", price: 5000 },
      }),
    ],
  };
  const changed = applyYandexPriceSendToWarehouse(
    warehouse,
    { id: "yandex-shop", name: "Yandex Shop" },
    { offerId: "SKU-YA-LOCAL", price: { value: 7125, currencyId: "RUR" } },
    "2026-05-15T06:40:00.000Z",
  );

  assert.equal(changed, true);
  assert.equal(warehouse.products[0].marketplacePrice, 7125);
  assert.equal(warehouse.products[0].currentPrice, 7125);
  assert.equal(warehouse.products[0].targetPrice, 7125);
  assert.equal(warehouse.products[0].yandex.price, 7125);
  assert.equal(warehouse.products[0].lastYandexPriceSend.status, "success");
  assert.equal(warehouse.products[0].lastYandexPriceSend.requestedPrice, 7125);
});

test("Yandex cleanup protects brands found in name, description, and characteristics", () => {
  const brands = parseProtectedBrandList("Giorgio Armani\nCreed; Ex Nihilo");
  assert.deepEqual(brands, ["Giorgio Armani", "Creed", "Ex Nihilo"]);

  const protectedByName = buildYandexCleanupCandidate({
    offer: { offerId: "ya-1", name: "Giorgio Armani Si Passione 90 мл" },
  }, { id: "shop", name: "Shop" }, brands);
  const protectedByDescription = buildYandexCleanupCandidate({
    offer: { offerId: "ya-2", name: "Парфюмерная вода 90 мл", description: "Аромат Creed Aventus" },
  }, { id: "shop", name: "Shop" }, brands);
  const protectedByCharacteristics = buildYandexCleanupCandidate({
    offer: { offerId: "ya-3", name: "Парфюмерная вода 90 мл", params: [{ name: "Бренд", value: "Ex Nihilo" }] },
  }, { id: "shop", name: "Shop" }, brands);
  const protectedBrandSmallVolume = buildYandexCleanupCandidate({
    offer: { offerId: "ya-small", name: "Creed Aventus 15 мл", description: "Оригинальный Creed" },
  }, { id: "shop", name: "Shop" }, brands);
  const unprotected = buildYandexCleanupCandidate({
    offer: { offerId: "ya-4", name: "Unknown Brand 90 мл" },
  }, { id: "shop", name: "Shop" }, brands);
  const alreadyArchived = buildYandexCleanupCandidate({
    offer: { offerId: "ya-5", name: "Unknown Brand 100 мл", archived: true },
  }, { id: "shop", name: "Shop" }, brands);

  assert.equal(protectedByName.action, "keep");
  assert.equal(protectedByDescription.action, "keep");
  assert.equal(protectedByCharacteristics.action, "keep");
  assert.equal(protectedBrandSmallVolume.action, "delete");
  assert.equal(protectedBrandSmallVolume.smallVolume, true);
  assert.equal(protectedBrandSmallVolume.protected, false);
  assert.equal(unprotected.action, "delete");
  assert.equal(alreadyArchived.action, "delete");
  assert.deepEqual(summarizeYandexCleanupPreview([
    protectedByName,
    protectedByDescription,
    protectedByCharacteristics,
    protectedBrandSmallVolume,
    unprotected,
    alreadyArchived,
  ]), { total: 6, protected: 3, toDelete: 3, toArchive: 3, alreadyArchived: 1 });
});

test("ops diagnostics command emits machine-readable report", async () => {
  const scriptPath = path.join(__dirname, "..", "scripts", "ops-diagnose.cjs");
  const { stdout } = await execFileAsync(process.execPath, [scriptPath, "--json", "--weak-limit=2", "--log-lines=0"], {
    cwd: path.join(__dirname, ".."),
    env: {
      ...process.env,
      DB_MODE: "json",
      DATABASE_URL: "",
      JSON_FALLBACK_ENABLED: "true",
      BULLMQ_ENABLED: "false",
      DISABLE_BACKGROUND_JOBS: "true",
    },
    timeout: 30_000,
  });
  const report = JSON.parse(stdout);
  assert.equal(typeof report.ok, "boolean");
  assert.equal(typeof report.generatedAt, "string");
  assert.equal(typeof report.warehouse.products, "number");
  assert.equal(typeof report.links.total, "number");
  assert.equal(typeof report.ozon.weakCards.total, "number");
  assert.ok(report.priceRetryQueue.byStatus);
  assert.ok(Array.isArray(report.recommendations));
});

test("detects Ozon per-item rate limit errors", () => {
  const error = new Error("price-batch-set for seller api: rpc error: code = ResourceExhausted desc = error limiting: acquire limit per item: items limit: limit exceeded");
  assert.equal(isOzonResourceExhaustedError(error), true);
  assert.equal(isOzonPerItemPriceLimitError(error), true);
});

test("Ozon FBO archive refusal is treated as an expected marketplace block", () => {
  assert.equal(isExpectedMarketplaceArchiveBlock("item has fbo stock"), true);
  assert.equal(isExpectedMarketplaceArchiveBlock("Yandex Market API error 500"), false);
});

test("Ozon price response item errors are queued as delayed retry items", () => {
  const payload = { offer_id: "OZ-1", price: "195586", currency_code: "RUB" };
  const failures = extractOzonPriceResponseFailures({
    result: [
      {
        offer_id: "OZ-1",
        updated: false,
        errors: [{ message: "price-batch-set for seller api: rpc error: code = ResourceExhausted desc = error limiting: acquire limit per item: items limit: limit exceeded" }],
      },
    ],
  }, [payload]);
  assert.equal(failures.length, 1);
  assert.equal(failures[0].payload.offer_id, "OZ-1");
  const retry = buildPriceRetryItem({
    id: "p1",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-1",
    price: 195586,
  }, failures[0].error, new Date("2026-05-13T00:00:00.000Z"));
  assert.equal(retry.status, "delayed");
  assert.equal(retry.retryReason, "ozon_per_item_price_limit");
  assert.ok(new Date(retry.nextRetryAt).getTime() >= new Date("2026-05-13T01:00:00.000Z").getTime());
});

test("active delayed Ozon price retry blocks duplicate auto send", () => {
  const delayed = {
    productId: "p1",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-1",
    status: "delayed",
    retryReason: "ozon_per_item_price_limit",
    nextRetryAt: "2026-05-13T01:05:00.000Z",
  };
  assert.equal(priceRetryQueueKey(delayed), "p1:ozon");
  const found = findActiveDelayedPriceRetry([delayed], {
    id: "p1",
    productId: "p1",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-1",
  }, new Date("2026-05-13T00:10:00.000Z"));
  assert.equal(found, delayed);
  const expired = findActiveDelayedPriceRetry([delayed], {
    id: "p1",
    target: "ozon",
    marketplace: "ozon",
  }, new Date("2026-05-13T02:10:00.000Z"));
  assert.equal(expired, null);
});

test("non-limit delayed Ozon price retry does not block auto send", () => {
  const delayed = {
    productId: "p1",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-1",
    status: "delayed",
    retryReason: "send_failed",
    error: "old price is less than price",
    nextRetryAt: "2026-05-13T01:05:00.000Z",
  };
  const found = findActiveDelayedPriceRetry([delayed], {
    id: "p1",
    productId: "p1",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-1",
  }, new Date("2026-05-13T00:10:00.000Z"));
  assert.equal(found, null);
});

test("Ozon old price errors are healed with a higher old_price retry", () => {
  const error = new Error("old price is less than price");
  assert.equal(isOzonOldPriceLessError(error), true);
  const retry = buildPriceRetryItem({
    id: "p1",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-1",
    price: 4500,
    oldPrice: 4000,
  }, error, new Date("2026-05-13T00:00:00.000Z"));
  assert.equal(retry.status, "pending");
  assert.equal(retry.retryReason, "ozon_old_price_adjusted");
  assert.equal(retry.forceOldPrice, true);
  assert.equal(retry.oldPrice, 5400);
  assert.equal(buildOzonPricePayload(retry).old_price, "5400");
});

test("selected auto price jobs still skip unchanged prices", () => {
  const unchanged = shouldSkipWarehousePriceSend({
    currentPrice: 1574,
    nextPrice: 1574,
    minDiffRub: 0,
    minDiffPct: 0,
  });
  assert.equal(unchanged.skip, true);
  assert.equal(unchanged.reason, "unchanged");

  const forced = shouldSkipWarehousePriceSend({
    currentPrice: 1574,
    nextPrice: 1574,
    force: true,
  });
  assert.equal(forced.skip, false);
});

test("price send skip helper respects ruble and percent thresholds", () => {
  assert.deepEqual(
    shouldSkipWarehousePriceSend({ currentPrice: 1000, nextPrice: 1010, minDiffRub: 20 }),
    { skip: true, reason: "min_diff_rub", diffRub: 10, minDiffRub: 20 },
  );
  const pct = shouldSkipWarehousePriceSend({ currentPrice: 1000, nextPrice: 1003, minDiffPct: 0.5 });
  assert.equal(pct.skip, true);
  assert.equal(pct.reason, "min_diff_pct");
  assert.equal(Math.round(pct.diffPct * 10) / 10, 0.3);
  assert.equal(shouldSkipWarehousePriceSend({ currentPrice: 0, nextPrice: 1574 }).skip, false);
});

test("price history duplicate detection suppresses identical recent sends", () => {
  const now = new Date("2026-05-14T07:06:00.000Z");
  const previous = {
    productId: "p1",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-1",
    oldPrice: 1574,
    newPrice: 1574,
    status: "success",
    error: "",
    at: "2026-05-14T07:05:00.000Z",
  };
  assert.equal(isDuplicatePriceHistoryEntry(previous, {
    productId: "p1",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-1",
    oldPrice: 1574,
    newPrice: 1574,
    status: "success",
    error: "",
  }, { now, windowMs: 15 * 60 * 1000 }), true);
  assert.equal(isDuplicatePriceHistoryEntry(previous, {
    ...previous,
    newPrice: 1600,
  }, { now, windowMs: 15 * 60 * 1000 }), false);
  assert.equal(isDuplicatePriceHistoryEntry(previous, previous, {
    now: new Date("2026-05-14T08:00:00.000Z"),
    windowMs: 15 * 60 * 1000,
  }), false);
});

test("price history append is a no-op without PostgreSQL", async () => {
  const count = await appendPriceHistoryRows([
    {
      productId: "p1",
      marketplace: "ozon",
      target: "ozon",
      offerId: "OZ-1",
      oldPrice: 170000,
      newPrice: 195586,
      status: "delayed",
      error: "limit",
      at: "2026-05-13T00:00:00.000Z",
    },
  ]);
  assert.equal(count, 0);
});

test("price history API is available with JSON fallback", async () => {
  const history = await readPriceHistory({ limit: 5 });
  assert.equal(Array.isArray(history.items), true);
  assert.equal(history.source, "json");

  const agent = request.agent(app);
  const login = await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);
  const cookie = (login.headers["set-cookie"] || []).map((item) => item.split(";")[0]).join("; ");
  const res = await agent
    .get("/api/warehouse/prices/history?limit=5")
    .set("Cookie", cookie)
    .expect(200);
  assert.equal(res.body.ok, true);
  assert.equal(Array.isArray(res.body.items), true);
});

test("postgres warehouse product falls back to raw links only when relation links are not loaded", () => {
  const product = productFromPostgres({
    id: "pg-link-fallback",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-RAW-LINK",
    name: "Raw link product",
    raw: {
      links: [
        {
          id: "raw-link-1",
          article: "PM-123",
          supplierName: "Supplier A",
          partnerId: "77",
          priceCurrency: "RUB",
        },
      ],
    },
    createdAt: new Date("2026-05-13T00:00:00.000Z"),
    updatedAt: new Date("2026-05-13T00:00:00.000Z"),
  });
  assert.equal(product.links.length, 1);
  assert.equal(product.links[0].article, "PM-123");
  assert.equal(product.links[0].supplierName, "Supplier A");
  assert.equal(product.links[0].priceCurrency, "RUB");
});

test("postgres warehouse product keeps empty relation links empty", () => {
  const product = productFromPostgres({
    id: "pg-link-empty-relation",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-DELETED-LINK",
    name: "Deleted link product",
    raw: {
      links: [{ id: "stale-raw-link", article: "PM-OLD", supplierName: "Old Supplier" }],
    },
    links: [],
    createdAt: new Date("2026-05-13T00:00:00.000Z"),
    updatedAt: new Date("2026-05-13T00:00:00.000Z"),
  });
  assert.equal(product.links.length, 0);
});

test("postgres Yandex warehouse product falls back to raw links when relation links are empty", () => {
  const product = productFromPostgres({
    id: "pg-yandex-link-fallback",
    marketplace: "yandex",
    target: "yandex-01",
    offerId: "YA-RAW-LINK",
    name: "Yandex raw link product",
    currentPrice: 1280,
    targetPrice: 1340,
    targetStock: 3,
    raw: {
      links: [
        {
          id: "raw-yandex-link-1",
          article: "PM-YA-123",
          supplierName: "Supplier Y",
          partnerId: "77",
          priceCurrency: "RUB",
        },
      ],
    },
    links: [],
    createdAt: new Date("2026-05-13T00:00:00.000Z"),
    updatedAt: new Date("2026-05-13T00:00:00.000Z"),
  });
  assert.equal(product.links.length, 1);
  assert.equal(product.links[0].article, "PM-YA-123");
  assert.equal(product.links[0].supplierName, "Supplier Y");
  assert.equal(product.links[0].priceCurrency, "RUB");
  assert.equal(product.currentPrice, 1280);
  assert.equal(product.targetPrice, 1340);
  assert.equal(product.targetStock, 3);
});

test("postgres warehouse product exposes link audit metadata", () => {
  const product = productFromPostgres({
    id: "pg-link-meta",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-LINK-META",
    name: "Link meta product",
    raw: {},
    links: [
      {
        id: "link-meta-1",
        supplierArticle: "PM-META",
        supplierName: "Supplier Meta",
        partnerId: "88",
        priceCurrency: "USD",
        keyword: null,
        raw: { createdBy: "anna", updatedBy: "david" },
        createdAt: new Date("2026-05-13T01:00:00.000Z"),
        updatedAt: new Date("2026-05-13T02:00:00.000Z"),
      },
    ],
    createdAt: new Date("2026-05-13T00:00:00.000Z"),
    updatedAt: new Date("2026-05-13T03:00:00.000Z"),
  });
  assert.equal(product.links[0].createdBy, "anna");
  assert.equal(product.links[0].updatedBy, "david");
  assert.equal(product.links[0].updatedAt, "2026-05-13T02:00:00.000Z");
});

test("postgres warehouse product prefers rich raw Ozon details over weak row fields", () => {
  const product = productFromPostgres({
    id: "pg-rich-raw",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-RICH-RAW",
    name: "Товар Ozon",
    images: { imageUrl: "https://cdn.example.com/row.jpg" },
    raw: {
      name: "Calvin Klein CK IN2U Туалетная вода для мужчин 50 мл",
      imageUrl: "https://cdn.example.com/raw.jpg",
      ozon: {
        name: "Calvin Klein CK IN2U",
        images: ["https://cdn.example.com/ozon.jpg"],
      },
    },
    links: [],
    createdAt: new Date("2026-05-13T00:00:00.000Z"),
    updatedAt: new Date("2026-05-13T00:00:00.000Z"),
  });
  assert.equal(product.name, "Calvin Klein CK IN2U Туалетная вода для мужчин 50 мл");
  assert.equal(product.imageUrl, "https://cdn.example.com/raw.jpg");
});

test("postgres warehouse product uses stored image column when raw has no image", () => {
  const product = productFromPostgres({
    id: "pg-image-column",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-IMAGE-COLUMN",
    name: "Stored image product",
    images: { imageUrl: "https://cdn.example.com/row.jpg" },
    raw: {},
    links: [],
    createdAt: new Date("2026-05-13T00:00:00.000Z"),
    updatedAt: new Date("2026-05-13T00:00:00.000Z"),
  });
  assert.equal(product.imageUrl, "https://cdn.example.com/row.jpg");
});

test("postgres Ozon state helpers prefer marketplaceState over internal target stock/status", () => {
  assert.equal(
    marketplaceStateCodeFromPostgresRow({
      status: "no_supplier",
      targetStock: 0,
      marketplaceState: { code: "active", stock: 3 },
    }),
    "active",
  );
  assert.equal(
    marketplaceStateCodeFromPostgresRow({
      status: "ok",
      targetStock: 0,
      marketplaceState: { code: "out_of_stock", stock: 0 },
    }),
    "out_of_stock",
  );
});

test("marketplace sync merge preserves links when Ozon target changes to account id", () => {
  const merged = mergeProducts(
    [
      {
        id: "local-linked-product",
        marketplace: "ozon",
        target: "ozon",
        offerId: "OZ-LINKED",
        productId: "12345",
        name: "Linked product",
        links: [
          {
            id: "link-1",
            article: "PM-LINKED",
            supplierName: "Supplier A",
          },
        ],
      },
    ],
    [
      {
        id: "imported-product",
        marketplace: "ozon",
        target: "account-1",
        offerId: "OZ-LINKED",
        productId: "12345",
        name: "Linked product from Ozon",
        marketplaceState: { code: "active", stock: 3 },
      },
    ],
  );

  assert.equal(merged.length, 1);
  assert.equal(merged[0].id, "local-linked-product");
  assert.equal(merged[0].target, "account-1");
  assert.equal(merged[0].links.length, 1);
  assert.equal(merged[0].links[0].article, "PM-LINKED");
  assert.equal(merged[0].marketplaceState.code, "active");
});

test("marketplace sync merge keeps known Ozon state and price on partial import", () => {
  const merged = mergeProducts(
    [
      {
        id: "local-active-product",
        marketplace: "ozon",
        target: "ozon",
        offerId: "OZ-PARTIAL",
        productId: "777",
        name: "Amouage Reflection Man Eau De Parfum 100ml",
        imageUrl: "https://example.test/amouage.jpg",
        productUrl: "https://www.ozon.ru/product/777/",
        sku: "123456",
        ozon: {
          offerId: "OZ-PARTIAL",
          name: "Amouage Reflection Man Eau De Parfum 100ml",
          vendor: "Amouage",
          primaryImage: "https://example.test/amouage.jpg",
          images: ["https://example.test/amouage.jpg"],
          barcode: "4600000000001",
        },
        marketplacePrice: 12345,
        marketplaceMinPrice: 10000,
        marketplaceState: { code: "active", label: "Активен Ozon", stock: 3, present: 3 },
        links: [{ id: "link-1", article: "PM-PARTIAL", supplierName: "Supplier A" }],
      },
    ],
    [
      {
        id: "imported-partial-product",
        marketplace: "ozon",
        target: "account-1",
        offerId: "OZ-PARTIAL",
        productId: "777",
        name: "Товар Ozon",
        marketplacePrice: null,
        marketplaceMinPrice: null,
        marketplaceState: { code: "out_of_stock", label: "Нет в наличии Ozon", stock: 0, partial: true },
      },
    ],
  );

  assert.equal(merged.length, 1);
  assert.equal(merged[0].id, "local-active-product");
  assert.equal(merged[0].target, "account-1");
  assert.equal(merged[0].marketplaceState.code, "active");
  assert.equal(merged[0].marketplaceState.stock, 3);
  assert.equal(merged[0].marketplacePrice, 12345);
  assert.equal(merged[0].marketplaceMinPrice, 10000);
  assert.equal(merged[0].name, "Amouage Reflection Man Eau De Parfum 100ml");
  assert.equal(merged[0].imageUrl, "https://example.test/amouage.jpg");
  assert.equal(merged[0].productUrl, "https://www.ozon.ru/product/777/");
  assert.equal(merged[0].sku, "123456");
  assert.equal(merged[0].ozon.name, "Amouage Reflection Man Eau De Parfum 100ml");
  assert.equal(merged[0].ozon.vendor, "Amouage");
  assert.equal(merged[0].ozon.primaryImage, "https://example.test/amouage.jpg");
  assert.deepEqual(merged[0].ozon.images, ["https://example.test/amouage.jpg"]);
  assert.equal(merged[0].links.length, 1);
});

test("Ozon enrichment keeps existing state when stock and status are missing", () => {
  const product = normalizeWarehouseProduct({
    id: "enrich-active-product",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-ENRICH",
    productId: "555",
    name: "Existing product",
    marketplacePrice: 1000,
    marketplaceState: { code: "active", label: "Активен Ozon", stock: 3 },
  });
  const enriched = applyOzonInfoToWarehouseProduct(
    product,
    { name: "Better Ozon name", primary_image: "https://example.test/image.jpg" },
    { id: "ozon", name: "Ozon" },
    {},
    {},
  );

  assert.equal(enriched.marketplaceState.code, "active");
  assert.equal(enriched.marketplaceState.stock, 3);
  assert.equal(enriched.marketplacePrice, 1000);
  assert.equal(enriched.name, "Better Ozon name");
  assert.equal(enriched.ozon.name, "Better Ozon name");
  assert.equal(enriched.imageUrl, "https://example.test/image.jpg");
});

test("Ozon sync refreshes details only for new or incomplete products", () => {
  const existingComplete = normalizeWarehouseProduct({
    target: "ozon",
    marketplace: "ozon",
    offerId: "complete",
    productId: "1",
    name: "Complete perfume",
    imageUrl: "https://example.test/image.jpg",
    marketplacePrice: 1234,
    marketplaceState: { code: "active" },
  });
  const existingWeak = normalizeWarehouseProduct({
    target: "ozon",
    marketplace: "ozon",
    offerId: "weak",
    productId: "2",
    name: "weak",
    marketplaceState: { code: "unknown", partial: true },
  });
  const existingByOffer = new Map([
    ["complete", existingComplete],
    ["weak", existingWeak],
  ]);
  const list = [
    { offer_id: "COMPLETE" },
    { offer_id: "WEAK" },
    { offer_id: "new" },
  ];

  assert.equal(ozonProductNeedsDetailRefresh(existingComplete), false);
  assert.equal(ozonProductNeedsDetailRefresh(existingWeak), true);
  assert.equal(ozonProductNeedsDetailRefresh(normalizeWarehouseProduct({
    target: "ozon",
    marketplace: "ozon",
    offerId: "generic",
    productId: "3",
    name: "Товар Ozon",
    marketplacePrice: 1000,
    marketplaceState: { code: "active" },
  })), true);
  assert.equal(ozonProductNeedsDetailRefresh(normalizeWarehouseProduct({
    target: "ozon",
    marketplace: "ozon",
    offerId: "real-generic",
    productId: "4",
    name: "Товар Ozon",
    imageUrl: "https://cdn.example.com/generic.jpg",
    marketplacePrice: 1000,
    marketplaceState: { code: "active" },
  })), true);
  assert.deepEqual(pickOzonDetailOfferIds(list, existingByOffer, 10), ["WEAK", "new"]);
});

test("weak Ozon repair picker selects only incomplete warehouse cards", () => {
  const complete = normalizeWarehouseProduct({
    id: "complete",
    target: "ozon",
    marketplace: "ozon",
    offerId: "complete-offer",
    productId: "1",
    name: "Complete perfume",
    imageUrl: "https://example.test/image.jpg",
    marketplacePrice: 1234,
    marketplaceState: { code: "active" },
  });
  const weakName = normalizeWarehouseProduct({
    id: "weak-name",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-WEAK",
    productId: "2",
    name: "Товар Ozon",
    imageUrl: "https://example.test/image.jpg",
    marketplacePrice: 999,
    marketplaceState: { code: "active" },
  });
  const weakImage = normalizeWarehouseProduct({
    id: "weak-image",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZ-NO-IMAGE",
    productId: "3",
    name: "Real Ozon product",
    marketplacePrice: 999,
    marketplaceState: { code: "active" },
  });
  const yandex = normalizeWarehouseProduct({
    id: "yandex-weak",
    target: "yandex",
    marketplace: "yandex",
    offerId: "YA-1",
    name: "Товар Ozon",
  });

  assert.equal(isWeakOzonWarehouseProduct(complete), false);
  assert.equal(isWeakOzonWarehouseProduct(weakName), true);
  assert.equal(isWeakOzonWarehouseProduct(weakImage), true);
  assert.deepEqual(pickWeakOzonProductIds([complete, weakName, yandex, weakImage], 10), ["weak-name", "weak-image"]);
  assert.deepEqual(pickWeakOzonProductIds([weakName, weakImage], 1), ["weak-name"]);
});

test("price retry queue recovers from an empty file", async () => {
  const backup = await backupFile(priceRetryQueuePath);
  try {
    await fs.mkdir(path.dirname(priceRetryQueuePath), { recursive: true });
    await fs.writeFile(priceRetryQueuePath, "", "utf8");
    const queue = await readPriceRetryQueue();
    assert.deepEqual(queue.items, []);
    await writePriceRetryQueue({ items: [{ id: "p1", target: "ozon" }] });
    const restored = await readPriceRetryQueue();
    assert.equal(restored.items.length, 1);
  } finally {
    await restoreFile(priceRetryQueuePath, backup);
  }
});

test("retry queue API can delete selected items only", async () => {
  const backup = await backupFile(priceRetryQueuePath);
  try {
    await writePriceRetryQueue({
      items: [
        { id: "p1", target: "ozon", marketplace: "ozon", offerId: "OZ-1", price: 1000 },
        { id: "p2", target: "ozon", marketplace: "ozon", offerId: "OZ-2", price: 2000 },
      ],
    });
    const agent = request.agent(app);
    await agent
      .post("/api/login")
      .send({ username: "admin", password: process.env.APP_PASSWORD })
      .expect(200);
    await agent
      .delete("/api/warehouse/prices/retry-queue")
      .send({ queueKeys: ["p1:ozon"] })
      .expect(200);
    const queue = await readPriceRetryQueue();
    assert.equal(queue.items.length, 1);
    assert.equal(queue.items[0].id, "p2");
  } finally {
    await restoreFile(priceRetryQueuePath, backup);
  }
});

test("PostgreSQL layer stays disabled without DATABASE_URL", () => {
  const previousUrl = process.env.DATABASE_URL;
  const previousMode = process.env.DB_MODE;
  const previousFallback = process.env.JSON_FALLBACK_ENABLED;
  try {
    delete process.env.DATABASE_URL;
    process.env.DB_MODE = "postgres";
    delete process.env.JSON_FALLBACK_ENABLED;
    assert.equal(postgres.hasDatabaseUrl(), false);
    assert.equal(postgres.postgresModeEnabled(), false);
    assert.equal(postgres.jsonFallbackEnabled(), true);
  } finally {
    if (previousUrl === undefined) delete process.env.DATABASE_URL;
    else process.env.DATABASE_URL = previousUrl;
    if (previousMode === undefined) delete process.env.DB_MODE;
    else process.env.DB_MODE = previousMode;
    if (previousFallback === undefined) delete process.env.JSON_FALLBACK_ENABLED;
    else process.env.JSON_FALLBACK_ENABLED = previousFallback;
  }
});

test("JSON seed normalizers prepare products, links, and retry items for PostgreSQL", () => {
  const product = seedPostgres.normalizeProductForPostgres({
    id: "p1",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-1",
    name: "Demo",
    brand: "Brand",
    currentPrice: 170000,
    nextPrice: 195586,
    targetStock: 3,
    updatedAt: "2026-05-13T00:00:00.000Z",
  });
  assert.equal(product.id, "p1");
  assert.equal(product.marketplace, "ozon");
  assert.equal(product.offerId, "OZ-1");
  assert.equal(product.targetPrice, 195586);

  const link = seedPostgres.normalizeLinkForPostgres({ id: "p1" }, {
    article: "81319",
    supplierName: "Сорин",
    partnerId: "88",
    priceCurrency: "RUB",
  });
  assert.equal(link.productId, "p1");
  assert.equal(link.supplierArticle, "81319");
  assert.equal(link.priceCurrency, "RUB");

  const retry = seedPostgres.normalizeRetryItemForPostgres({
    id: "p1",
    marketplace: "ozon",
    target: "ozon",
    offerId: "OZ-1",
    price: 195586,
    nextRetryAt: "2026-05-13T01:05:00.000Z",
  });
  assert.equal(retry.status, "delayed");
  assert.equal(retry.queueKey, "p1:ozon");
});

test("warehouse page includes marketplace siblings for visible offer groups", () => {
  const ozon = normalizeWarehouseProduct({ id: "ozon-a", marketplace: "ozon", target: "ozon", offerId: "SKU-1", name: "Ozon row" });
  const unrelated = normalizeWarehouseProduct({ id: "ozon-b", marketplace: "ozon", target: "ozon", offerId: "SKU-2", name: "Other row" });
  const yandex = normalizeWarehouseProduct({ id: "yandex-a", marketplace: "yandex", target: "yandex-shop", offerId: "SKU-1", name: "Yandex row" });
  const page = addWarehousePageGroupSiblings([ozon, unrelated, yandex], [ozon, unrelated]);

  assert.deepEqual(page.map((product) => product.id), ["ozon-a", "ozon-b", "yandex-a"]);
});

test("linked recovery includes Yandex sibling without direct links", () => {
  const candidates = linkedRecoveryCandidateProducts([
    {
      id: "ozon-linked",
      marketplace: "ozon",
      target: "ozon",
      offerId: "CC-AASH5001",
      links: [{ article: "41059", supplierName: "Supplier" }],
      marketplaceState: { code: "active" },
    },
    {
      id: "yandex-archived-sibling",
      marketplace: "yandex",
      target: "yandex-06c2112c",
      offerId: "CC-AASH5001",
      links: [],
      marketplaceState: { code: "archived", archived: true },
    },
  ]);
  const yandex = candidates.find((product) => product.id === "yandex-archived-sibling");
  assert.ok(yandex);
  assert.equal(yandex.links.length, 1);
  assert.equal(yandex.links[0].article, "41059");
});

test("warehouse summary counters use linked product stats, not page-sized snapshots", () => {
  const stats = summarizeWarehouseCounterStats({
    totalProducts: 5,
    linkedProducts: [{ id: "p1" }, { id: "p2" }, { id: "p3" }],
    builtLinkedProducts: [
      { id: "p1", ready: true, changed: true },
      { id: "p2", ready: true, changed: false },
      { id: "p3", ready: false, changed: false },
    ],
  });

  assert.equal(stats.linkedProducts, 3);
  assert.equal(stats.ready, 2);
  assert.equal(stats.changed, 1);
  assert.equal(stats.withoutSupplier, 2);
  assert.equal(stats.linkedNotReady, 1);
});

test("warehouse page link filters support ready, changed, and linked Ozon archive", () => {
  const ready = {
    ...normalizeWarehouseProduct({
      id: "ready",
      target: "ozon",
      marketplace: "ozon",
      offerId: "READY",
      ready: true,
      changed: false,
      links: [{ id: "l1", article: "A1", supplierName: "Supplier" }],
      marketplaceState: { code: "active" },
    }),
    ready: true,
    changed: false,
  };
  const changed = {
    ...normalizeWarehouseProduct({
      id: "changed",
      target: "ozon",
      marketplace: "ozon",
      offerId: "CHANGED",
      ready: true,
      changed: true,
      links: [{ id: "l2", article: "A2", supplierName: "Supplier" }],
      marketplaceState: { code: "active" },
    }),
    ready: true,
    changed: true,
  };
  const archived = {
    ...normalizeWarehouseProduct({
      id: "archived",
      target: "ozon",
      marketplace: "ozon",
      offerId: "ARCH",
      ready: false,
      changed: false,
      links: [{ id: "l3", article: "A3", supplierName: "Supplier" }],
      marketplaceState: { code: "archived" },
    }),
    ready: false,
    changed: false,
  };
  const unlinkedArchived = normalizeWarehouseProduct({
    id: "unlinked-archived",
    target: "ozon",
    marketplace: "ozon",
    offerId: "NO-LINK-ARCH",
    marketplaceState: { code: "archived" },
  });

  assert.equal(warehousePageProductMatches(ready, { linked: "ready" }), true);
  assert.equal(warehousePageProductMatches(archived, { linked: "ready" }), false);
  assert.equal(warehousePageProductMatches(changed, { linked: "changed" }), true);
  assert.equal(warehousePageProductMatches(ready, { linked: "changed" }), false);
  assert.equal(warehousePageProductMatches(archived, { linked: "linked_archived" }), true);
  assert.equal(warehousePageProductMatches(unlinkedArchived, { linked: "linked_archived" }), false);
});

test("warehouse page search matches supplier link fields", () => {
  const product = normalizeWarehouseProduct({
    id: "linked-search-product",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OZON-SEARCH",
    name: "Marketplace name",
    links: [{
      id: "linked-search-link",
      article: "PM-LINK-777",
      supplierName: "Special Supplier",
      partnerId: "991",
      keyword: "amber",
    }],
  });

  assert.equal(warehousePageProductMatches(product, { q: "pm-link-777" }), true);
  assert.equal(warehousePageProductMatches(product, { q: "special supplier" }), true);
  assert.equal(warehousePageProductMatches(product, { q: "amber" }), true);
  assert.equal(warehousePageProductMatches(product, { q: "991" }), false);
  assert.equal(warehousePageProductMatches(product, { q: "missing-query" }), false);
});

test("warehouse page article search does not treat supplier partner id as product article", () => {
  const where = warehousePagePostgresWhere({ q: "991" });
  const serialized = JSON.stringify(where);
  assert.equal(serialized.includes("supplierArticle"), true);
  assert.equal(serialized.includes("partnerId"), false);
});

test("warehouse page strict postgres search checks product identity before supplier links", () => {
  const primaryWhere = warehousePagePostgresPrimaryIdentityWhere({ q: "41059", linked: "linked" });
  const serialized = JSON.stringify(primaryWhere);
  assert.equal(serialized.includes("supplierArticle"), false);
  assert.equal(serialized.includes("\"offerId\""), true);
  assert.equal(serialized.includes("\"productId\""), true);
  assert.equal(serialized.includes("\"links\":{\"some\":{}"), true);
});

test("warehouse page article search ranks product offer ids before supplier links", () => {
  const supplierMatch = normalizeWarehouseProduct({
    id: "supplier-match",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OTHER",
    links: [{ id: "supplier-match-link", article: "NF-00004538", supplierName: "Supplier" }],
  });
  const offerMatch = normalizeWarehouseProduct({
    id: "offer-match",
    target: "ozon",
    marketplace: "ozon",
    offerId: "NF-00004538",
    links: [{ id: "offer-match-link", article: "DIFFERENT", supplierName: "Supplier" }],
  });
  const sorted = sortWarehouseProductsForSearch([supplierMatch, offerMatch], { q: "NF-00004538" });
  assert.equal(sorted[0].id, "offer-match");
});

test("POST /api/login неверный пароль", async () => {
  await request(app)
    .post("/api/login")
    .send({ username: "admin", password: "wrong" })
    .expect(401);
});

test("POST /api/login успех", async () => {
  const res = await request(app)
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);
  assert.equal(res.body.ok, true);
  assert.ok(res.headers["set-cookie"]);
});

test("marketplace accounts can be saved from UI storage without editing env", async () => {
  const backup = await backupFile(marketplaceAccountsPath);
  const agent = request.agent(app);
  const accountName = `Smoke Yandex ${Date.now()}`;

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({ updatedAt: new Date().toISOString(), accounts: [] }, null, 2));
    await agent
      .post("/api/login")
      .send({ username: "admin", password: process.env.APP_PASSWORD })
      .expect(200);

    const created = await agent
      .post("/api/marketplace-accounts")
      .send({
        marketplace: "yandex",
        name: accountName,
        businessId: "123456",
        campaignId: "654321",
        apiKey: "test-yandex-api-key",
        syncEnabled: "true",
      })
      .expect(200);

    const account = created.body.accounts.find((item) => item.name === accountName);
    assert.ok(account);
    assert.equal(account.marketplace, "yandex");
    assert.equal(account.configured, true);
    assert.equal(account.businessId, "123456");
    assert.notEqual(account.apiKey, "test-yandex-api-key");

    const stored = JSON.parse(await fs.readFile(marketplaceAccountsPath, "utf8"));
    assert.ok(stored.accounts.some((item) => item.name === accountName && item.apiKey === "test-yandex-api-key"));

    await agent
      .post("/api/marketplace-accounts/not-found/test")
      .expect(404);
  } finally {
    await restoreFile(marketplaceAccountsPath, backup);
  }
});

test("POST /api/login supports APP_USERS_JSON roles", async () => {
  const previousUsers = process.env.APP_USERS_JSON;
  process.env.APP_USERS_JSON = JSON.stringify([
    { username: "manager", password: "manager-pass", role: "manager" },
  ]);
  try {
    const res = await request(app)
      .post("/api/login")
      .send({ username: "manager", password: "manager-pass" })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.username, "manager");
    assert.equal(res.body.role, "manager");
  } finally {
    if (previousUsers === undefined) delete process.env.APP_USERS_JSON;
    else process.env.APP_USERS_JSON = previousUsers;
  }
});

test("admin can add employees and managers cannot open admin areas", async () => {
  const backup = await backupFile(appUsersPath);
  const admin = request.agent(app);
  const manager = request.agent(app);
  const username = `manager-${Date.now()}`;
  const password = "manager-pass";

  try {
    await restoreFile(appUsersPath, JSON.stringify({ users: [] }, null, 2));
    await admin
      .post("/api/login")
      .send({ username: "admin", password: process.env.APP_PASSWORD })
      .expect(200);

    const created = await admin
      .post("/api/users")
      .send({ username, password, role: "manager" })
      .expect(200);
    assert.ok(created.body.users.some((user) => user.username === username && user.role === "manager"));
    const storedUsers = JSON.parse(await fs.readFile(appUsersPath, "utf8")).users || [];
    const storedUser = storedUsers.find((user) => user.username === username);
    assert.match(storedUser.password, /^scrypt\$/);
    assert.notEqual(storedUser.password, password);

    const login = await manager
      .post("/api/login")
      .send({ username, password })
      .expect(200);
    assert.equal(login.body.role, "manager");

    await manager.get("/api/settings").expect(403);
    await manager.get("/api/history").expect(403);
    await manager.get("/api/pricemaster/search?q=no-such-pricemaster-row&limit=1").expect(200);
    await manager.get("/settings.html").expect(302).expect("Location", "/");

    const disabled = await admin
      .put(`/api/users/${encodeURIComponent(username)}`)
      .send({ active: false })
      .expect(200);
    assert.ok(disabled.body.users.some((user) => user.username === username && user.disabled === true));

    await request(app)
      .post("/api/login")
      .send({ username, password })
      .expect(401);

    const enabled = await admin
      .put(`/api/users/${encodeURIComponent(username)}`)
      .send({ active: true })
      .expect(200);
    assert.ok(enabled.body.users.some((user) => user.username === username && user.disabled === false));

    const promoted = await admin
      .put(`/api/users/${encodeURIComponent(username)}`)
      .send({ role: "admin" })
      .expect(200);
    assert.ok(promoted.body.users.some((user) => user.username === username && user.role === "admin"));

    await admin.delete(`/api/users/${encodeURIComponent(username)}`).expect(200);
  } finally {
    await restoreFile(appUsersPath, backup);
  }
});

test("admin can read employee PriceMaster link statistics and managers cannot", async () => {
  const usersBackup = await backupFile(appUsersPath);
  const deletedUsersBackup = await backupFile(appDeletedUsersPath);
  const warehouseBackup = await backupFile(personalWarehousePath);
  const auditBackup = await backupFile(auditLogPath);
  const admin = request.agent(app);
  const manager = request.agent(app);
  const now = new Date().toISOString();
  const oldDate = new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString();

  try {
    await restoreFile(appUsersPath, JSON.stringify({
      users: [
        { username: "manager-stats", password: "manager-pass", role: "manager" },
        { username: "delete-stats", password: "delete-pass", role: "manager" },
      ],
    }, null, 2));
    await restoreFile(appDeletedUsersPath, null);
    const statsWarehouse = {
      products: [
        {
          id: "stats-product-1",
          marketplace: "ozon",
          offerId: "STATS-1",
          name: "Stats product",
          links: [
            { id: "stats-link-1", article: "PM-1", supplierName: "Supplier", createdBy: "admin", updatedBy: "manager-stats", createdAt: now, updatedAt: now },
          ],
        },
      ],
      suppliers: [],
    };
    await writeWarehouse(statsWarehouse);
    await restoreFile(auditLogPath, [
      JSON.stringify({
        at: now,
        user: "admin",
        role: "admin",
        action: "warehouse.links.bulk_save",
        productId: ["stats-product-1", "stats-product-2"],
        details: {
          productIds: ["stats-product-1", "stats-product-2"],
          offerIds: ["STATS-1", "STATS-2"],
          links: [{ article: "PM-1", supplierName: "Supplier" }],
        },
      }),
      JSON.stringify({
        at: now,
        user: "manager-stats",
        role: "manager",
        action: "warehouse.link.delete",
        productId: "stats-product-1",
        details: { productId: "stats-product-1", offerId: "STATS-1", linkId: "stats-link-1" },
      }),
      JSON.stringify({
        at: oldDate,
        user: "admin",
        role: "admin",
        action: "warehouse.links.bulk_save",
        productId: ["old-product"],
        details: { productIds: ["old-product"], links: [{ article: "OLD" }] },
      }),
    ].join("\n") + "\n");

    await admin
      .post("/api/login")
      .send({ username: "admin", password: process.env.APP_PASSWORD })
      .expect(200);
    await manager
      .post("/api/login")
      .send({ username: "manager-stats", password: "manager-pass" })
      .expect(200);

    await manager.get("/api/users/stats?period=30d").expect(403);
    const res = await admin.get("/api/users/stats?period=30d").expect(200);
    assert.equal(res.body.period, "30d");
    assert.equal(res.body.periodDays, 30);
    assert.ok(res.body.summary);
    assert.equal(res.body.summary.actionsTotal, 2);
    const adminStats = res.body.users.find((user) => user.username === "admin");
    const managerStats = res.body.users.find((user) => user.username === "manager-stats");
    assert.ok(adminStats);
    assert.ok(managerStats);
    assert.equal(adminStats.currentLinksCreated, 1);
    assert.equal(adminStats.linksAdded, 2);
    assert.equal(adminStats.affectedProducts, 2);
    assert.equal(adminStats.affectedOfferIds, 2);
    assert.equal(managerStats.currentLinksUpdated, 1);
    assert.equal(managerStats.linksDeleted, 1);

    const all = await admin.get("/api/users/stats?period=all").expect(200);
    assert.equal(all.body.period, "all");
    const allAdminStats = all.body.users.find((user) => user.username === "admin");
    assert.ok(allAdminStats.linksAdded >= 3);

    await manager.post("/api/users/stats/export").send({ period: "30d" }).expect(403);
    const pdf = await admin
      .post("/api/users/stats/export")
      .send({ period: "30d", usernames: ["admin", "manager-stats"] })
      .expect(200)
      .expect("Content-Type", /application\/pdf/);
    assert.ok(Buffer.isBuffer(pdf.body));
    assert.equal(pdf.body.subarray(0, 4).toString("utf8"), "%PDF");
    assert.ok(pdf.body.length > 5000);
    const usersRouteSource = await fs.readFile(path.join(__dirname, "..", "routes", "users.js"), "utf8");
    assert.match(usersRouteSource, /logo1\.png/);

    await admin.delete("/api/users/delete-stats?hard=true").expect(200);
    const usersAfterHardDelete = await admin.get("/api/users").expect(200);
    assert.equal(usersAfterHardDelete.body.users.some((user) => user.username === "delete-stats"), false);
  } finally {
    await restoreFile(appUsersPath, usersBackup);
    await restoreFile(appDeletedUsersPath, deletedUsersBackup);
    if (warehouseBackup) {
      await writeWarehouse(JSON.parse(warehouseBackup));
    } else {
      await writeWarehouse({ products: [], suppliers: [] });
    }
    await restoreFile(personalWarehousePath, warehouseBackup);
    await restoreFile(auditLogPath, auditBackup);
  }
});

test("admin can read manual warehouse sync status without starting long request", async () => {
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);
  const res = await agent.get("/api/warehouse/sync/status").expect(200);
  assert.ok(["idle", "running", "ok", "failed"].includes(res.body.status));
  assert.equal(typeof res.body.running, "boolean");
});

test("admin can read operation jobs and invalid operation types are rejected", async () => {
  const backup = await backupFile(operationJobsPath);
  const agent = request.agent(app);
  try {
    await restoreFile(operationJobsPath, JSON.stringify({
      updatedAt: new Date().toISOString(),
      jobs: [{ id: "job-smoke", type: "health-deep", title: "Deep health check", status: "completed", progress: 100, createdAt: new Date().toISOString() }],
    }, null, 2));
    await agent
      .post("/api/login")
      .send({ username: "admin", password: process.env.APP_PASSWORD })
      .expect(200);

    const list = await agent.get("/api/operations").expect(200);
    assert.ok(list.body.jobs.some((job) => job.id === "job-smoke"));

    await agent
      .post("/api/operations")
      .send({ type: "unknown-operation", payload: {} })
      .expect(400);
  } finally {
    await restoreFile(operationJobsPath, backup);
  }
});

test("PUT /api/settings saves markup settings", async () => {
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  const before = await agent.get("/api/settings").expect(200);
  const previous = before.body.settings;
  const nextRate = Number(previous.fixedUsdRate || 95) === 95 ? 96 : 95;
  const nextOzonMarkup = Number(previous.defaultMarkups?.ozon || 1.7) === 1.91 ? 1.92 : 1.91;
  try {
    const res = await agent
      .put("/api/settings")
      .send({
        fixedUsdRate: nextRate,
        defaultMarkups: { ozon: nextOzonMarkup, yandex: 1.62 },
        markupRules: [{ marketplace: "all", minUsd: 0, coefficient: nextOzonMarkup }],
        availabilityRules: [{ marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 }],
      })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.settings.fixedUsdRate, nextRate);
    assert.equal(res.body.settings.defaultMarkups.ozon, nextOzonMarkup);
    assert.equal(res.body.settings.markupRules[0].coefficient, nextOzonMarkup);
    assert.equal(res.body.settings.availabilityRules[0].targetStock, 10);
    assert.equal(res.body.priceAffectingChanged, true);
    assert.equal(res.body.priceRepriceQueued, true);
    assert.equal(res.body.priceRepriceReason, "settings_price_update");
  } finally {
    if (previous) {
      await agent.put("/api/settings").send(previous);
    }
  }
});

test("POST /api/settings/pricing/adjust-percent updates marketplace coefficients", async () => {
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  const before = await agent.get("/api/settings").expect(200);
  const previous = before.body.settings;
  try {
    await agent
      .put("/api/settings")
      .send({
        ...previous,
        defaultMarkups: { ozon: 2, yandex: 3 },
        markupRules: [
          { marketplace: "all", minUsd: 0, coefficient: 4 },
          { marketplace: "ozon", minUsd: 10, coefficient: 5 },
          { marketplace: "yandex", minUsd: 10, coefficient: 6 },
        ],
      })
      .expect(200);

    const res = await agent
      .post("/api/settings/pricing/adjust-percent")
      .send({ marketplace: "ozon", direction: "decrease", percent: 2 })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.priceRepriceQueued, true);
    assert.equal(res.body.settings.defaultMarkups.ozon, 1.96);
    assert.equal(res.body.settings.defaultMarkups.yandex, 3);
    const rules = res.body.settings.markupRules;
    assert.ok(rules.some((rule) => rule.marketplace === "ozon" && rule.minUsd === 0 && rule.coefficient === 3.92));
    assert.ok(rules.some((rule) => rule.marketplace === "yandex" && rule.minUsd === 0 && rule.coefficient === 4));
    assert.ok(rules.some((rule) => rule.marketplace === "ozon" && rule.minUsd === 10 && rule.coefficient === 4.9));
    assert.ok(rules.some((rule) => rule.marketplace === "yandex" && rule.minUsd === 10 && rule.coefficient === 6));
    assert.equal(res.body.priceRepriceReason, "settings_price_adjust_percent");
  } finally {
    if (previous) await agent.put("/api/settings").send(previous);
  }
});

test("manager cannot apply pricing percent adjustment", async () => {
  const previousUsers = process.env.APP_USERS_JSON;
  process.env.APP_USERS_JSON = JSON.stringify([
    { username: "pricing-manager", password: "manager-pass", role: "manager" },
  ]);
  try {
    const manager = request.agent(app);
    await manager
      .post("/api/login")
      .send({ username: "pricing-manager", password: "manager-pass" })
      .expect(200);
    await manager
      .post("/api/settings/pricing/adjust-percent")
      .send({ marketplace: "all", direction: "decrease", percent: 2 })
      .expect(403);
  } finally {
    if (previousUsers === undefined) delete process.env.APP_USERS_JSON;
    else process.env.APP_USERS_JSON = previousUsers;
  }
});

test("price settings changes trigger repricing decisions", () => {
  const base = {
    fixedUsdRate: 95,
    defaultMarkups: { ozon: 1.7, yandex: 1.6 },
    markupRules: [{ marketplace: "all", minUsd: 0, coefficient: 1.7 }],
    availabilityRules: [{ marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 }],
    ai: { textModel: "gpt-5.4-mini", imageModel: "gpt-image-2" },
  };

  assert.equal(priceAffectingSettingsChanged(base, {
    ...base,
    ai: { textModel: "gpt-5.5", imageModel: "gpt-image-2" },
  }), false);
  assert.equal(priceAffectingSettingsChanged(base, { ...base, fixedUsdRate: 101 }), true);
  assert.equal(priceAffectingSettingsChanged(base, {
    ...base,
    defaultMarkups: { ozon: 1.8, yandex: 1.6 },
  }), true);
  assert.equal(priceAffectingSettingsChanged(base, {
    ...base,
    availabilityRules: [{ marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 5 }],
  }), true);
});

test("PUT /api/settings saves AI provider settings without exposing API key", async () => {
  const backup = await backupFile(appSettingsPath);
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  const before = await agent.get("/api/settings").expect(200);
  const previous = before.body.settings;
  try {
    const res = await agent
      .put("/api/settings")
      .send({
        ...previous,
        ai: {
          enabled: true,
          providerId: "codexsale",
          baseUrl: "https://codex.sale/v1/images/generations",
          apiKey: "sk-test-secret-1234",
          textModel: "gpt-5.4-mini",
          imageModel: "gpt-image-2",
          imageSize: "1024x1024",
          imageQuality: "auto",
          imageFormat: "png",
        },
      })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.settings.ai.providerId, "codexsale");
    assert.equal(res.body.settings.ai.baseUrl, "https://codex.sale/v1");
    assert.equal(res.body.settings.ai.apiKeySet, true);
    assert.equal(res.body.settings.ai.apiKey, "__masked__");
    assert.equal(res.body.settings.ai.apiKeyMasked.endsWith("1234"), true);

    const second = await agent
      .put("/api/settings")
      .send({
        ...res.body.settings,
        ai: { ...res.body.settings.ai, textModel: "gpt-5.4" },
      })
      .expect(200);
    assert.equal(second.body.settings.ai.apiKeySet, true);
    assert.equal(second.body.settings.ai.textModel, "gpt-5.4");
    assert.equal(second.body.priceAffectingChanged, false);
    assert.equal(second.body.priceRepriceQueued, false);
  } finally {
    await restoreFile(appSettingsPath, backup);
  }
});

test("AI settings test error does not mention Price Master", async () => {
  const backup = await backupFile(appSettingsPath);
  const previousKey = process.env.OPENAI_API_KEY;
  const previousCodexLbKey = process.env.CODEX_LB_API_KEY;
  const previousCodexSaleKey = process.env.CODEX_SALE_API_KEY;
  delete process.env.OPENAI_API_KEY;
  delete process.env.CODEX_LB_API_KEY;
  delete process.env.CODEX_SALE_API_KEY;
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const res = await agent
      .post("/api/settings/ai/test")
      .send({
        ai: {
          enabled: true,
          providerId: "codexsale",
          baseUrl: "https://codex.sale/v1/images/generations",
          apiKeySet: false,
          apiKey: "",
          textModel: "gpt-5.4-mini",
        },
      })
      .expect(400);

    assert.match(res.body.error, /AI/);
    assert.doesNotMatch(`${res.body.error} ${res.body.detail}`, /Price Master/i);
  } finally {
    if (previousKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
    if (previousCodexLbKey === undefined) delete process.env.CODEX_LB_API_KEY;
    else process.env.CODEX_LB_API_KEY = previousCodexLbKey;
    if (previousCodexSaleKey === undefined) delete process.env.CODEX_SALE_API_KEY;
    else process.env.CODEX_SALE_API_KEY = previousCodexSaleKey;
    await restoreFile(appSettingsPath, backup);
  }
});

test("OpenAI request format errors are retryable for compatible providers", () => {
  assert.equal(isOpenAiRequestFormatError({
    status: 400,
    code: "invalid_request_error",
    error: { message: "Апстрим отклонил формат запроса. Проверь модель и параметры запроса." },
  }), true);
  assert.equal(isOpenAiRequestFormatError({
    status: 401,
    code: "invalid_api_key",
    error: { message: "invalid key" },
  }), false);
});

test("AI image requests never reuse text model names", () => {
  const codexSale = { providerId: "codexsale", baseUrl: "https://codex.sale/v1" };
  assert.equal(effectiveOpenAiImageModel("gpt-5.4", codexSale), "gpt-image-2");
  assert.equal(effectiveOpenAiImageModel("gpt-5.4-mini", codexSale), "gpt-image-2");
  assert.equal(effectiveOpenAiImageModel("gpt-image-1.5", codexSale), "gpt-image-2");
  assert.equal(effectiveOpenAiImageModel("gpt-image-2", codexSale), "gpt-image-2");
});

test("OpenAI billing limit errors are detected for fail-fast AI jobs", () => {
  assert.equal(isOpenAiBillingLimitError({ code: "openai_billing_limit" }), true);
  assert.equal(isOpenAiBillingLimitError({ message: "insufficient_quota" }), true);
  assert.equal(isOpenAiBillingLimitError({ message: "temporary upstream timeout" }), false);
});

test("Codex Sale AI requests prefer compatible chat parameters first", () => {
  assert.equal(shouldPreferCompatibleOpenAiChatRequest({
    providerId: "codexsale",
    baseUrl: "https://codex.sale/v1",
  }), true);
  assert.equal(isCodexSaleAiProvider({
    providerId: "codexsale",
    baseUrl: "https://codex.sale/v1",
  }), true);
  const attempts = openAiChatCompletionAttempts({
    model: "gpt-5.4-mini",
    messages: [{ role: "user", content: "{}" }],
    temperature: 0.2,
    response_format: { type: "json_object" },
  }, { preferCompatible: true });
  assert.equal(attempts[0].response_format, undefined);
  assert.equal(attempts[0].temperature, undefined);
  assert.equal(attempts[1].response_format.type, "json_object");
});

test("resolveMarkupCoefficient applies threshold >= 10 USD", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace: "ozon",
    supplierUsdPrice: 12,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [{ minUsd: 10, coefficient: 3 }, { minUsd: 20, coefficient: 2.8 }],
    },
  });
  assert.equal(value, 3);
});

test("resolveMarkupCoefficient applies threshold >= 20 USD", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace: "ozon",
    supplierUsdPrice: 25,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [{ minUsd: 10, coefficient: 3 }, { minUsd: 20, coefficient: 2.8 }],
    },
  });
  assert.equal(value, 2.8);
});

test("resolveMarkupCoefficient converts RUB supplier price before USD thresholds", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace: "ozon",
    supplierUsdPrice: 900,
    supplierPriceCurrency: "RUB",
    usdRate: 100,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [{ minUsd: 900, coefficient: 1.4 }],
    },
  });
  assert.equal(value, 1.7);
});

test("resolveMarkupCoefficient uses Yandex defaults and Yandex-scoped rules", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace: "yandex",
    supplierUsdPrice: 14,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [
        { marketplace: "ozon", minUsd: 10, coefficient: 3 },
        { marketplace: "yandex", minUsd: 10, coefficient: 2.4 },
      ],
    },
  });
  assert.equal(value, 2.4);
});

test("resolveMarkupCoefficient keeps marketplace scoped rules separate", () => {
  const appSettings = {
    defaultMarkups: { ozon: 1.7, yandex: 1.6 },
    markupRules: [
      { marketplace: "ozon", minUsd: 10, coefficient: 3.1 },
      { marketplace: "yandex", minUsd: 10, coefficient: 2.2 },
    ],
  };
  assert.equal(resolveMarkupCoefficient({ productMarkup: 0, marketplace: "ozon", supplierUsdPrice: 20, appSettings }), 3.1);
  assert.equal(resolveMarkupCoefficient({ productMarkup: 0, marketplace: "yandex", supplierUsdPrice: 20, appSettings }), 2.2);
});

test("common warehouse group links synchronize supplier-aware signatures", () => {
  const products = [
    { id: "ozon-1", marketplace: "ozon", links: [{ id: "a", article: "A1", supplierName: "Alpha", partnerId: "1", priceCurrency: "USD" }] },
    { id: "yandex-1", marketplace: "yandex", links: [{ id: "b", article: "A1", supplierName: "Beta", partnerId: "2", priceCurrency: "USD" }] },
  ];
  const commonLinks = buildCommonWarehouseGroupLinks(products, [
    { id: "c", matchType: "selected_row", sourceRowId: "2066033", exactName: "Tester 30 ml", supplierName: "Gamma", partnerId: "3", priceCurrency: "USD" },
  ], { now: "2026-05-19T00:00:00.000Z", username: "tester" });
  for (const product of products) product.links = commonLinks;
  const signature = warehouseGroupLinkSignature(products);
  assert.equal(signature.ok, true);
  assert.equal(signature.products[0].linkCount, 3);
  assert.equal(signature.products[1].signature, signature.products[0].signature);
  assert.ok(warehouseProductLinksSignature(products[0]).includes("partner:1"));
  assert.ok(warehouseProductLinksSignature(products[0]).includes("row:2066033"));
});

test("warehouse group expansion includes marketplace siblings by offerId and manual group", () => {
  const products = [
    { id: "ozon-1", offerId: "41059", marketplace: "ozon" },
    { id: "yandex-1", offerId: "41059", marketplace: "yandex" },
    { id: "other", offerId: "999", marketplace: "ozon" },
    { id: "manual-a", offerId: "A", manualGroupId: "manual-1", marketplace: "ozon" },
    { id: "manual-b", offerId: "B", manualGroupId: "manual-1", marketplace: "yandex" },
  ];
  assert.deepEqual(
    expandWarehouseProductsToGroups(products, [products[0]]).map((product) => product.id).sort(),
    ["ozon-1", "yandex-1"],
  );
  assert.deepEqual(
    expandWarehouseProductsToGroups(products, [products[3]]).map((product) => product.id).sort(),
    ["manual-a", "manual-b"],
  );
});

test("syncWarehouseProductGroupLinks spreads union links to every marketplace sibling", () => {
  const products = [
    {
      id: "ozon-1",
      offerId: "14547634",
      marketplace: "ozon",
      links: [{ id: "ozon-link", article: "11333", supplierName: "Zurab", partnerId: "10", priceCurrency: "USD" }],
    },
    {
      id: "yandex-1",
      offerId: "14547634",
      marketplace: "yandex",
      links: [{ id: "yandex-link", matchType: "selected_row", sourceRowId: "2066033", exactName: "Tester 30 ml", supplierName: "Svetlana", partnerId: "96", priceCurrency: "USD" }],
    },
  ];
  const result = syncWarehouseProductGroupLinks(products, { now: "2026-05-19T12:00:00.000Z", username: "tester" });
  assert.equal(result.changedProducts.length, 2);
  assert.equal(products[0].links.length, 2);
  assert.equal(products[1].links.length, 2);
  assert.equal(warehouseGroupLinkSignature(products).ok, true);
  assert.equal(warehouseProductLinksSignature(products[0]), warehouseProductLinksSignature(products[1]));
});

test("marketplace price breakdown returns separate coefficients for shared PriceMaster links", () => {
  const rows = marketplacePriceBreakdown([
    {
      id: "ozon-1",
      offerId: "41059",
      marketplace: "ozon",
      markupCoefficient: 1.7,
      usdRate: 100,
      currentPrice: 1500,
      targetPrice: 1700,
      selectedSupplier: { supplierName: "Alpha", article: "A1", price: 10, priceCurrency: "USD", calculatedPrice: 1700 },
    },
    {
      id: "yandex-1",
      offerId: "41059",
      marketplace: "yandex",
      markupCoefficient: 1.6,
      usdRate: 100,
      currentPrice: 1500,
      targetPrice: 1600,
      selectedSupplier: { supplierName: "Alpha", article: "A1", price: 10, priceCurrency: "USD", calculatedPrice: 1600 },
    },
  ]);
  assert.equal(rows[0].marketplace, "ozon");
  assert.equal(rows[0].targetPrice, 1700);
  assert.equal(rows[1].marketplace, "yandex");
  assert.equal(rows[1].markupCoefficient, 1.6);
  assert.notEqual(rows[0].targetPrice, rows[1].targetPrice);
});

test("Ozon current cabinet price prefers seller price visible in cabinet", () => {
  const value = pickOzonCabinetListedPrice({
    currentPrice: 29315,
    marketingSellerPrice: 23500,
    marketingPrice: 28993,
    retailPrice: 29315,
  });
  assert.equal(value, 23500);
});

test("Ozon price payload disables auto price controls by default", () => {
  const previous = process.env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS;
  delete process.env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS;
  try {
    assert.deepEqual(buildOzonPricePayload({ offerId: "56989", price: 29315 }), {
      offer_id: "56989",
      price: "29315",
      currency_code: "RUB",
      old_price: "35178",
      auto_action_enabled: "DISABLED",
      price_strategy_enabled: "DISABLED",
    });
  } finally {
    if (previous === undefined) delete process.env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS;
    else process.env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS = previous;
  }
});

test("resolveMarkupCoefficient uses product markup override", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 2.2,
    marketplace: "ozon",
    supplierUsdPrice: 25,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [{ minUsd: 10, coefficient: 3 }, { minUsd: 20, coefficient: 2.8 }],
    },
  });
  assert.equal(value, 2.2);
});

test("normalizeWarehouseProduct clears copied Yandex markup unless it is manual", () => {
  const cloned = normalizeWarehouseProduct({
    id: "yandex-clone",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-MARKUP",
    markup: 2.75,
    yandex: {
      offerId: "SKU-YA-MARKUP",
      price: 12345,
      extra: { exportedFrom: "ozon" },
    },
  });
  const manual = normalizeWarehouseProduct({
    id: "yandex-manual",
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-MARKUP-MANUAL",
    markup: 2.75,
    markupSource: "manual",
    yandex: {
      offerId: "SKU-YA-MARKUP-MANUAL",
      price: 12345,
      extra: { manualMarkup: true },
    },
  });

  assert.equal(cloned.markup, 0);
  assert.equal(manual.markup, 2.75);
});

test("Yandex pricing ignores copied Ozon markup unless the override is manual", () => {
  const staleYandex = {
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-STALE-MARKUP",
    markup: 2.75,
    yandex: { extra: { exportedFrom: "ozon" } },
  };
  const manualYandex = {
    marketplace: "yandex",
    target: "yandex-main",
    offerId: "SKU-YA-MANUAL-MARKUP",
    markup: 2.15,
    markupSource: "manual",
    yandex: { extra: { manualMarkup: true } },
  };
  const ozon = {
    marketplace: "ozon",
    target: "ozon",
    offerId: "SKU-OZON-MARKUP",
    markup: 2.75,
  };

  assert.equal(marketplaceProductMarkupOverride(staleYandex), 0);
  assert.equal(marketplaceProductMarkupOverride(manualYandex), 2.15);
  assert.equal(marketplaceProductMarkupOverride(ozon), 2.75);
});

test("normalizeWarehouseProduct treats yandex shop targets as Yandex even without account metadata", () => {
  const product = normalizeWarehouseProduct({
    id: "target-yandex-unknown",
    target: "yandex-06c2112c",
    offerId: "SKU-YA-TARGET",
    name: "Yandex target product",
  });

  assert.equal(product.marketplace, "yandex");
  assert.equal(product.target, "yandex-06c2112c");
  assert.equal(product.targetName, "Yandex Market");
});

test("resolveAvailabilityPolicy lowers markup and raises stock for many suppliers", () => {
  const policy = resolveAvailabilityPolicy({
    marketplace: "ozon",
    availableSupplierCount: 5,
    baseMarkup: 1.7,
    appSettings: {
      availabilityRules: [
        { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
        { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 },
      ],
    },
  });
  assert.equal(policy.markupCoefficient, 1.65);
  assert.equal(policy.targetStock, 10);
});

test("resolveAvailabilityPolicy keeps base markup and small stock for few suppliers", () => {
  const policy = resolveAvailabilityPolicy({
    marketplace: "ozon",
    availableSupplierCount: 1,
    baseMarkup: 1.7,
    appSettings: {
      availabilityRules: [
        { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
        { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 },
      ],
    },
  });
  assert.equal(policy.markupCoefficient, 1.7);
  assert.equal(policy.targetStock, 3);
});

test("resolveAvailabilityPolicy falls back to stock 3 when rules are empty", () => {
  const policy = resolveAvailabilityPolicy({
    marketplace: "ozon",
    availableSupplierCount: 2,
    baseMarkup: 1.7,
    appSettings: { availabilityRules: [] },
  });
  assert.equal(policy.targetStock, 3);
});

test("normalizePriceMasterPrice keeps ruble values native without USD conversion", () => {
  const value = normalizePriceMasterPrice(9500, 95, "RUB");
  assert.equal(value.sourceCurrency, "RUB");
  assert.equal(value.convertedFromRub, true);
  assert.equal(value.price, 9500);
  assert.equal(value.originalPrice, 9500);
});

test("normalizePriceMasterPrice keeps dollar-like values in USD", () => {
  const value = normalizePriceMasterPrice(9500, 95);
  assert.equal(value.sourceCurrency, "USD");
  assert.equal(value.convertedFromRub, false);
  assert.equal(value.price, 9500);
});

test("normalizeManagedSupplier defaults PriceMaster currency to USD", () => {
  const supplier = normalizeManagedSupplier({ name: "Supplier" });
  assert.equal(supplier.priceCurrency, "USD");
});

test("normalizeManagedSupplier supports stock-only pricing mode", () => {
  const supplier = normalizeManagedSupplier({ name: "Own stock", pricingMode: "stock_only" });
  assert.equal(supplier.pricingMode, "stock_only");
  assert.equal(supplier.stockOnly, true);
});

test("normalizePriceMasterSnapshotItemForPostgres prepares rows for PostgreSQL", () => {
  const updatedAt = new Date("2026-05-13T03:00:00.000Z");
  const row = normalizePriceMasterSnapshotItemForPostgres({
    rowId: 123,
    article: " PM-123 ",
    partnerId: 88,
    partnerName: "Supplier",
    name: "Native Name",
    price: 42.5,
    currency: "RUR",
    docDate: "2026-05-12T10:00:00.000Z",
    active: 1,
  }, updatedAt);

  assert.equal(row.article, "PM-123");
  assert.equal(row.partnerId, "88");
  assert.equal(row.partnerName, "Supplier");
  assert.equal(row.nativeName, "Native Name");
  assert.equal(row.price, "42.5");
  assert.equal(row.currency, "RUB");
  assert.equal(row.docDate.toISOString(), "2026-05-12T10:00:00.000Z");
  assert.equal(row.updatedAt, updatedAt);
  assert.ok(row.id);
});

test("normalizePriceMasterSnapshotItemForPostgres keeps rows without supplier article", () => {
  const row = normalizePriceMasterSnapshotItemForPostgres({
    rowId: 991,
    article: "",
    partnerId: 32277,
    partnerName: "Иванна",
    name: "EX NIHILO BLUE TALISMAN 7.5ml Extrait De Parfum в коробке",
    price: 36,
  }, new Date("2026-05-13T03:00:00.000Z"));

  assert.equal(row.article, "__no_article__:991");
  assert.equal(row.rowId, "991");
  assert.equal(row.nativeName, "EX NIHILO BLUE TALISMAN 7.5ml Extrait De Parfum в коробке");
  assert.equal(row.partnerId, "32277");
});

test("resolvePriceMasterRowCurrency uses managed supplier priceCurrency setting", () => {
  const rubMaps = {
    byPartnerId: new Map([["88", { name: "Supplier", priceCurrency: "RUB" }]]),
    byName: new Map(),
  };
  assert.equal(resolvePriceMasterRowCurrency(
    { partnerId: "88", partnerName: "Supplier", currency: "USD", price: 128 },
    { priceCurrency: "USD" },
    rubMaps,
  ), "RUB");
  const usdMaps = {
    byPartnerId: new Map([["88", { name: "Supplier", priceCurrency: "USD" }]]),
    byName: new Map(),
  };
  assert.equal(resolvePriceMasterRowCurrency(
    { partnerId: "88", partnerName: "Supplier", currency: "RUB", price: 5000 },
    { priceCurrency: "RUB" },
    usdMaps,
  ), "USD");
  assert.equal(resolvePriceMasterRowCurrency(
    { partnerName: "Инна", price: 53 },
    {},
    managedSupplierMaps(),
  ), "RUB");
});

test("supplierUsesRubPriceMasterPricing matches INNA only, not IVANNA", () => {
  assert.equal(supplierUsesRubPriceMasterPricing(null, { partnerName: "Инна" }), true);
  assert.equal(supplierUsesRubPriceMasterPricing(null, { partnerName: "Inna" }), true);
  assert.equal(supplierUsesRubPriceMasterPricing(null, { partnerName: "Иванна" }), false);
  assert.equal(supplierUsesRubPriceMasterPricing(null, { partnerName: "Ivanna" }), false);
  assert.equal(supplierUsesRubPriceMasterPricing({ name: "Avangard", priceCurrency: "RUB" }), true);
  assert.equal(supplierUsesRubPriceMasterPricing({ name: "Avangard", priceCurrency: "USD" }), false);
});

test("resolvePriceMasterRowCurrency treats Иванна as USD with rate conversion", () => {
  const currency = resolvePriceMasterRowCurrency(
    { partnerName: "Иванна", price: 128 },
    { priceCurrency: "USD" },
    managedSupplierMaps(),
    95,
  );
  assert.equal(currency, "USD");
  const normalized = normalizePriceMasterPrice(128, 95, currency);
  assert.equal(normalized.price, 128);
  assert.equal(calculateRubPrice(normalized.price, 95, 1.7, normalized), 20672);
  assert.equal(warehouseSupplierPurchaseRubPrice({ ...normalized, partnerName: "Иванна" }, 95), 12160);
});

test("Инна retail price uses markup only without USD rate conversion", () => {
  const row = {
    partnerName: "Инна",
    price: 53,
    originalPrice: 53,
    sourceCurrency: "RUB",
    priceCurrency: "RUB",
    convertedFromRub: true,
  };
  assert.equal(calculateRubPrice(row.price, 95, 11.9, row), 631);
  assert.equal(warehouseSupplierPurchaseRubPrice(row, 95), 53);
});

test("resolvePriceMasterRowCurrency keeps dollar suppliers unchanged", () => {
  const currency = resolvePriceMasterRowCurrency(
    { partnerName: "Avangard", price: 128 },
    {},
    managedSupplierMaps(),
    95,
  );
  assert.equal(currency, "USD");
  assert.equal(normalizePriceMasterPrice(128, 95, currency).price, 128);
});

test("pickWarehouseSupplier chooses the cheapest available calculated price", () => {
  const picked = pickWarehouseSupplier([
    { partnerName: "Expensive", available: true, price: 20, calculatedPrice: 3400, docDate: "2026-01-01" },
    { partnerName: "Cheap", available: true, price: 10, calculatedPrice: 1700, docDate: "2026-01-02" },
    { partnerName: "Missing", available: false, price: 1, calculatedPrice: 100, docDate: "2026-01-03" },
  ]);
  assert.equal(picked.partnerName, "Cheap");
});

test("pickWarehouseSupplier prefers cheapest supplier purchase price", () => {
  const picked = pickWarehouseSupplier([
    { partnerName: "Cheapest purchase", available: true, price: 10, purchaseRubPrice: 950, calculatedPrice: 1700, effectiveFinalPrice: 4000, docDate: "2026-01-02" },
    { partnerName: "Lower retail markup", available: true, price: 12, purchaseRubPrice: 1140, calculatedPrice: 2040, effectiveFinalPrice: 2500, docDate: "2026-01-01" },
  ]);
  assert.equal(picked.partnerName, "Cheapest purchase");
});

test("Наш Склад is always stock-only and never used for price", () => {
  assert.equal(supplierUsesStockOnlyPricing(null, { partnerName: "Наш Склад" }), true);
  assert.equal(supplierUsesStockOnlyPricing(null, { partnerName: "Наш склад (остатки)" }), true);
  assert.equal(supplierUsesStockOnlyPricing(null, { partnerName: "Поставщик Наш склад" }), true);
  const meta = priceMasterSupplierPricingMeta({ partnerName: "Наш Склад", price: 5000, active: true });
  assert.equal(meta.stockOnly, true);
  assert.equal(meta.priceEligible, false);
  const picked = pickWarehouseSupplier([
    { partnerName: "Наш Склад", available: true, price: 5000, calculatedPrice: 8500, docDate: "2026-01-01", stockOnly: true, priceEligible: false },
    { partnerName: "Авангард", available: true, price: 90, calculatedPrice: 12636, docDate: "2026-01-02", priceEligible: true },
  ]);
  assert.equal(picked.partnerName, "Авангард");
  const pickedUnsafeFlags = pickWarehouseSupplier([
    { partnerName: "Наш склад", available: true, price: 5000, calculatedPrice: 8500, docDate: "2026-01-01", priceEligible: true, stockOnly: false },
    { partnerName: "Авангард", available: true, price: 90, calculatedPrice: 12636, docDate: "2026-01-02", priceEligible: true },
  ]);
  assert.equal(pickedUnsafeFlags.partnerName, "Авангард");
});

test("pickWarehouseSupplier ignores stock-only suppliers for price", () => {
  const picked = pickWarehouseSupplier([
    { partnerName: "Own stock", available: true, price: 1, calculatedPrice: 78, docDate: "2026-01-03", stockOnly: true, priceEligible: false },
    { partnerName: "Real supplier", available: true, price: 90, calculatedPrice: 12636, docDate: "2026-01-01", priceEligible: true },
  ]);
  assert.equal(picked.partnerName, "Real supplier");
  const fallback = pickWarehouseStockOnlySupplier([
    { partnerName: "Own stock", available: true, price: 1, calculatedPrice: 78, docDate: "2026-01-03", stockOnly: true, priceEligible: false },
  ]);
  assert.equal(fallback.partnerName, "Own stock");
});

test("warehouseProductUsesStockOnlyPricing blocks all price push paths", () => {
  const { warehouseProductUsesStockOnlyPricing } = require("../server.js");
  assert.equal(warehouseProductUsesStockOnlyPricing({
    stockOnlyFallbackActive: true,
    selectedSupplier: { partnerName: "Авангард", priceEligible: true },
    nextPrice: 9000,
  }), true);
  assert.equal(warehouseProductUsesStockOnlyPricing({
    stockOnlyFallbackActive: false,
    selectedSupplier: { partnerName: "Наш Склад", price: 1 },
    nextPrice: 5000,
  }), true);
  assert.equal(warehouseProductUsesStockOnlyPricing({
    selectedSupplier: { partnerName: "Авангард", priceEligible: true },
    nextPrice: 2500,
  }), false);
});

test("propagateGroupSupplierContextForPage does not copy stock-only donor prices", () => {
  const { propagateGroupSupplierContextForPage } = require("../server.js");
  const propagated = propagateGroupSupplierContextForPage([
    {
      id: "ozon-stock-only",
      marketplace: "ozon",
      offerId: "SKU-NS",
      links: [{ id: "l5", article: "PM-5", supplierName: "Наш Склад" }],
      selectedSupplier: { partnerName: "Наш Склад", price: 1 },
      stockOnlyFallbackActive: true,
      nextPrice: 7800,
      targetPrice: 7800,
      targetStock: 2,
    },
    {
      id: "yandex-stock-only",
      marketplace: "yandex",
      offerId: "SKU-NS",
      links: [{ id: "l5b", article: "PM-5", supplierName: "Наш Склад" }],
      selectedSupplier: null,
      nextPrice: 0,
      targetPrice: 0,
      targetStock: 0,
    },
  ]);
  const sibling = propagated.find((item) => item.id === "yandex-stock-only");
  assert.equal(sibling?.selectedSupplier, null);
  assert.equal(sibling?.nextPrice, 0);
  assert.equal(sibling?.targetPrice, 0);
  assert.equal(sibling?.targetStock, 2);
});

test("stockOnlyManualPriceForProduct prefers marketplace-scoped manual prices", () => {
  const { stockOnlyManualPriceForProduct, normalizeStockOnlyManualPrices } = require("../server.js");
  assert.deepEqual(normalizeStockOnlyManualPrices({ default: 100, ozon: 200, yandex: 300 }), {
    default: 100,
    ozon: 200,
    yandex: 300,
  });
  assert.equal(stockOnlyManualPriceForProduct({
    marketplace: "ozon",
    stockOnlyManualPrices: { default: 100, ozon: 250, yandex: 300 },
  }), 250);
  assert.equal(stockOnlyManualPriceForProduct({
    marketplace: "yandex",
    stockOnlyManualPrices: { default: 100, ozon: 250, yandex: 310 },
  }), 310);
  assert.equal(stockOnlyManualPriceForProduct({
    marketplace: "wildberries",
    stockOnlyManualPrices: { default: 150, ozon: 250 },
  }), 150);
  assert.equal(stockOnlyManualPriceForProduct({
    marketplace: "ozon",
    stockOnlyManualPrices: { default: 400 },
  }), 400);
});

test("applyWarehouseNextPriceLimits enforces Ozon min price after auto bounds", () => {
  const { applyWarehouseNextPriceLimits } = require("../server.js");
  assert.equal(applyWarehouseNextPriceLimits(900, { autoPriceMin: 1000, autoPriceMax: 5000, ozonMinPrice: 1200 }), 1200);
  assert.equal(applyWarehouseNextPriceLimits(1500, { autoPriceMin: 1000, autoPriceMax: 5000, ozonMinPrice: 1200 }), 1500);
  assert.equal(applyWarehouseNextPriceLimits(800, { autoPriceMin: 0, autoPriceMax: 0, ozonMinPrice: 900 }), 900);
  assert.equal(applyWarehouseNextPriceLimits(0, { ozonMinPrice: 900 }), 0);
  assert.equal(applyWarehouseNextPriceLimits(6000, { autoPriceMin: 1000, autoPriceMax: 5000, ozonMinPrice: 1200 }), 5000);
});

test("supplier cart scoring respects trust, reseller flag and Moscow cutoff", () => {
  assert.equal(normalizeSupplierTrustFactor(120), 100);
  assert.equal(normalizeSupplierTrustFactor(-5), 0);
  assert.equal(normalizeSupplierOrderCutoff("9:30"), "09:30");
  assert.equal(supplierOrderCutoffPassed("13:00", new Date("2026-05-25T11:30:00.000Z")), true);
  assert.equal(supplierOrderCutoffPassed("15:00", new Date("2026-05-25T11:30:00.000Z")), false);

  const trusted = supplierCartOrderScore({ price: 100, trustFactor: 100, orderCutoffTime: "15:00" }, new Date("2026-05-25T08:00:00.000Z"));
  const reseller = supplierCartOrderScore({ price: 100, trustFactor: 100, reseller: true, orderCutoffTime: "15:00" }, new Date("2026-05-25T08:00:00.000Z"));
  const late = supplierCartOrderScore({ price: 100, trustFactor: 100, orderCutoffTime: "13:00" }, new Date("2026-05-25T11:30:00.000Z"));
  assert.ok(trusted < reseller);
  assert.ok(late > reseller);
});

test("selectSupplierCartSupplierFromMatches prefers regular suppliers over stock-only", () => {
  const now = new Date("2026-05-25T08:00:00.000Z");
  const matches = new Map([
    ["link-1", [
      { partnerId: "stock", partnerName: "Наш склад", available: true, active: true, price: 1, docDate: "2026-01-03", stockOnly: true, priceEligible: false },
      { partnerId: "real", partnerName: "Авангард", available: true, active: true, price: 90, docDate: "2026-01-02", priceEligible: true, trustFactor: 100, orderCutoffTime: "15:00" },
    ]],
  ]);
  const result = selectSupplierCartSupplierFromMatches(matches, new Set(), now);
  assert.equal(result.selected?.partnerName, "Авангард");
  assert.equal(result.stockOnlyFallback, false);
});

test("selectSupplierCartSupplierFromMatches falls back to stock-only when no regular supplier", () => {
  const now = new Date("2026-05-25T08:00:00.000Z");
  const matches = new Map([
    ["link-1", [
      { partnerId: "stock", partnerName: "Наш склад", available: true, active: true, price: 0, docDate: "2026-01-03", stockOnly: true, priceEligible: false },
    ]],
  ]);
  const result = selectSupplierCartSupplierFromMatches(matches, new Set(), now);
  assert.equal(result.selected?.partnerName, "Наш склад");
  assert.equal(result.stockOnlyFallback, true);
  assert.equal(result.skipReason, "stock_only_fallback");
});

test("selectSupplierCartSupplierFromMatches uses stock-only after supplier block", () => {
  const now = new Date("2026-05-25T08:00:00.000Z");
  const matches = new Map([
    ["link-1", [
      { partnerId: "real", partnerName: "Авангард", available: true, active: true, price: 90, docDate: "2026-01-02", priceEligible: true, trustFactor: 100, orderCutoffTime: "15:00" },
      { partnerId: "stock", partnerName: "Наш склад", available: true, active: true, price: 0, docDate: "2026-01-03", stockOnly: true, priceEligible: false },
    ]],
  ]);
  const result = selectSupplierCartSupplierFromMatches(matches, new Set(["real"]), now);
  assert.equal(result.selected?.partnerName, "Наш склад");
  assert.equal(result.stockOnlyFallback, true);
  assert.equal(result.skipReason, "stock_only_fallback_after_supplier_blocked");
  assert.equal(result.blockedAvailable, 1);
});

test("warehouse brand filter falls back to marketplace product data", () => {
  const product = {
    name: "Нишевый аромат без бренда в корне",
    ozon: {
      name: "AMOUAGE Guidance 100 ml",
      attributes: [],
    },
  };

  assert.equal(warehouseBrandMatches(product, "Amouage"), true);
});

test("warehouse brand filter finds brand in raw Ozon attributes", () => {
  const product = {
    name: "Товар без названия бренда",
    ozon: {
      attributes: [
        {
          id: 85,
          name: "Бренд",
          values: [{ value: "Amouage" }],
        },
      ],
    },
  };

  assert.equal(warehouseBrandMatches(product, "Amouage"), true);
});

test("warehouse brand filter scans non-standard marketplace fields", () => {
  const product = {
    name: "Товар без бренда в названии",
    ozon: {
      rawPayload: {
        brand_name_from_api: "Amouage",
      },
    },
  };

  assert.equal(warehouseBrandMatches(product, "Amouage"), true);
});

test("warehouse brand filter finds brand in nested raw marketplace attributes", () => {
  const product = {
    name: "Niche perfume 100 ml",
    ozon: {
      rawPayload: {
        result: {
          attributes: [
            { attribute_name: "Brand", values: [{ value: "Marc-Antoine Barrois" }] },
          ],
        },
      },
    },
  };

  assert.equal(warehouseBrandMatches(product, "marc antoine"), true);
});

test("warehouse brand filter does not match arbitrary raw text", () => {
  const product = {
    name: "Shampoo for hair 250 ml",
    ozon: {
      rawPayload: {
        description: "Compatible search phrase Amouage from an old audit note",
      },
    },
  };

  assert.equal(warehouseBrandMatches(product, "Amouage"), false);
});

test("normalizeWarehouseProduct preserves AI image draft review state", () => {
  const product = normalizeWarehouseProduct({
    target: "ozon",
    offerId: "ai-draft-1",
    name: "AI Test Product",
    aiImages: [
      {
        id: "draft-1",
        status: "approved",
        sourceImageUrl: "http://localhost/uploads/images/source.png",
        resultUrl: "http://localhost/uploads/ai-images/result.png",
        prompt: "Generate for {productName}",
        reviewedAt: "2026-01-01T00:00:00.000Z",
      },
    ],
  });

  assert.equal(product.aiImages.length, 1);
  assert.equal(product.aiImages[0].status, "approved");
  assert.equal(product.aiImages[0].resultUrl, "http://localhost/uploads/ai-images/result.png");
});

test("AI quality candidates load cached low-quality cards without Yandex sync", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-yandex-quality-${Date.now()}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "yandex-qa",
        marketplace: "yandex",
        offerId: "LOW-QUALITY-40",
        name: "Low Quality Yandex Product",
        yandex: {
          extra: {
            cardQuality: {
              contentRating: 35,
              averageContentRating: 56,
              recommendations: ["Add description", { text: "Add photos" }],
            },
          },
        },
      })
      .expect(200);

    const res = await agent
      .get("/api/warehouse/yandex-quality-candidates?cached=1&threshold=40&limit=1000&resultLimit=20")
      .expect(200);

    assert.equal(res.body.cached, true);
    assert.ok(res.body.qualityLoaded >= 1);
    assert.ok(res.body.products.some((row) => row.product?.id === smokeId));
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
});

test("AI image generation requires OpenAI key before creating draft", async () => {
  const settingsBackup = await backupFile(appSettingsPath);
  const agent = request.agent(app);
  const smokeId = `smoke-ai-${Date.now()}`;
  let product;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  const saved = await agent
    .post("/api/warehouse/products")
    .send({
      id: smokeId,
      target: "ozon",
      offerId: smokeId,
      name: "Smoke AI Product",
      ozon: {
        offerId: smokeId,
        name: "Smoke AI Product",
        primaryImage: "http://localhost/uploads/images/source.png",
      },
    })
    .expect(200);

  product = saved.body.warehouse.products.find((item) => item.id === smokeId);
  assert.ok(product);

  const previousKey = process.env.OPENAI_API_KEY;
  const previousCodexLbKey = process.env.CODEX_LB_API_KEY;
  const previousCodexSaleKey = process.env.CODEX_SALE_API_KEY;
  const previousRelayUrl = process.env.OPENAI_RELAY_URL;
  const previousRelaySecret = process.env.OPENAI_RELAY_SECRET;
  delete process.env.OPENAI_API_KEY;
  delete process.env.CODEX_LB_API_KEY;
  delete process.env.CODEX_SALE_API_KEY;
  delete process.env.OPENAI_RELAY_URL;
  delete process.env.OPENAI_RELAY_SECRET;
  try {
    const currentSettings = await agent.get("/api/settings").expect(200);
    await agent
      .put("/api/settings")
      .send({
        ...currentSettings.body.settings,
        ai: { ...(currentSettings.body.settings.ai || {}), apiKeySet: false, apiKey: "" },
      })
      .expect(200);

    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/generate`)
      .send({ sourceImageUrl: "http://localhost/uploads/images/source.png" })
      .expect(400);

    assert.equal(res.body.code, "openai_not_configured");
  } finally {
    if (previousKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
    if (previousCodexLbKey === undefined) delete process.env.CODEX_LB_API_KEY;
    else process.env.CODEX_LB_API_KEY = previousCodexLbKey;
    if (previousCodexSaleKey === undefined) delete process.env.CODEX_SALE_API_KEY;
    else process.env.CODEX_SALE_API_KEY = previousCodexSaleKey;
    if (previousRelayUrl === undefined) delete process.env.OPENAI_RELAY_URL;
    else process.env.OPENAI_RELAY_URL = previousRelayUrl;
    if (previousRelaySecret === undefined) delete process.env.OPENAI_RELAY_SECRET;
    else process.env.OPENAI_RELAY_SECRET = previousRelaySecret;
    if (product?.id) await agent.delete(`/api/warehouse/products/${encodeURIComponent(product.id)}`).expect(200);
    await restoreFile(appSettingsPath, settingsBackup);
  }
});

test("Codex Sale AI image generation uses edit endpoint with source image", async () => {
  const settingsBackup = await backupFile(appSettingsPath);
  const originalFetch = global.fetch;
  const agent = request.agent(app);
  const smokeId = `smoke-codex-sale-image-${Date.now()}`;
  const generatedPngBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=";
  const calls = [];
  let generatedUploadPath = null;

  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const currentSettings = await agent.get("/api/settings").expect(200);
    await agent
      .put("/api/settings")
      .send({
        ...currentSettings.body.settings,
        ai: {
          ...(currentSettings.body.settings.ai || {}),
          enabled: true,
          providerId: "codexsale",
          baseUrl: "https://codex.sale/v1/images/generations",
          apiKey: "sk-test-codex-sale-image",
          textModel: "gpt-5.4-mini",
          imageModel: "gpt-image-2",
          imageSize: "1024x1024",
          imageQuality: "auto",
          imageFormat: "png",
        },
      })
      .expect(200);

    await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        marketplace: "ozon",
        offerId: smokeId,
        name: "Smoke Codex Sale Image Product",
        ozon: {
          offerId: smokeId,
          name: "Smoke Codex Sale Image Product",
          primaryImage: "https://example.invalid/source.png",
        },
        aiImages: [
          {
            id: "existing-draft",
            status: "pending",
            sourceImageUrl: "https://example.invalid/source.png",
            resultUrl: "http://localhost/uploads/ai-images/existing.png",
          },
        ],
      })
      .expect(200);

    global.fetch = async (url, options = {}) => {
      if (String(url) === "https://example.invalid/source.png") {
        calls.push({ url: String(url), sourceFetch: true });
        return new Response(Buffer.from(generatedPngBase64, "base64"), {
          status: 200,
          headers: { "content-type": "image/png" },
        });
      }
      const form = options.body;
      calls.push({
        url: String(url),
        model: typeof form?.get === "function" ? form.get("model") : null,
        prompt: typeof form?.get === "function" ? form.get("prompt") : null,
        image: typeof form?.get === "function" ? form.get("image") : null,
      });
      if (String(url) !== "https://codex.sale/v1/images/edits") {
        throw new Error(`unexpected fetch ${url}`);
      }
      return new Response(JSON.stringify({ data: [{ b64_json: generatedPngBase64 }] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };

    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/generate`)
      .send({
        sourceImageUrl: "https://example.invalid/source.png",
        prompt: "Create clean marketplace product image",
        count: 1,
        sync: true,
        expectedUpdatedAt: "2020-01-01T00:00:00.000Z",
      })
      .expect(200);

    assert.equal(res.body.drafts.length, 1);
    assert.equal(res.body.product.aiImages.length, 2);
    assert.equal(res.body.product.aiImages[0].id, "existing-draft");
    assert.equal(calls.length, 2);
    assert.equal(calls[0].sourceFetch, true);
    assert.equal(calls[1].model, "gpt-image-2");
    assert.ok(calls[1].image);
    assert.match(String(calls[1].prompt || ""), /Smoke Codex Sale Image Product/);
    assert.equal(res.body.draft.sourceImageUrl, "https://example.invalid/source.png");
    assert.match(res.body.draft.resultUrl, /\/uploads\/ai-images\//);
    const generatedPathname = new URL(res.body.draft.resultUrl).pathname;
    generatedUploadPath = path.join(__dirname, "..", "public", ...generatedPathname.split("/").filter(Boolean));
  } finally {
    global.fetch = originalFetch;
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).catch(() => {});
    if (generatedUploadPath) await fs.unlink(generatedUploadPath).catch(() => {});
    await restoreFile(appSettingsPath, settingsBackup);
  }
});

test("AI image generation starts background job and saves drafts progressively", async () => {
  const settingsBackup = await backupFile(appSettingsPath);
  const jobsBackup = await backupFile(aiImageJobsPath);
  const originalFetch = global.fetch;
  const agent = request.agent(app);
  const smokeId = `smoke-ai-job-${Date.now()}`;
  const generatedPngBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=";
  const generatedUploadPaths = [];
  let editCalls = 0;
  let activeEdits = 0;
  let maxConcurrentEdits = 0;

  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const currentSettings = await agent.get("/api/settings").expect(200);
    await agent
      .put("/api/settings")
      .send({
        ...currentSettings.body.settings,
        ai: {
          ...(currentSettings.body.settings.ai || {}),
          enabled: true,
          providerId: "codexsale",
          baseUrl: "https://codex.sale/v1",
          apiKey: "sk-test-codex-sale-image",
          textModel: "gpt-5.4-mini",
          imageModel: "gpt-image-2",
          imageSize: "1024x1024",
          imageQuality: "auto",
          imageFormat: "png",
        },
      })
      .expect(200);

    await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        marketplace: "ozon",
        offerId: smokeId,
        name: "Smoke AI Job Product",
        ozon: {
          offerId: smokeId,
          name: "Smoke AI Job Product",
          primaryImage: "https://example.invalid/job-source.png",
        },
      })
      .expect(200);

    global.fetch = async (url, options = {}) => {
      if (String(url) === "https://example.invalid/job-source.png") {
        return new Response(Buffer.from(generatedPngBase64, "base64"), {
          status: 200,
          headers: { "content-type": "image/png" },
        });
      }
      if (String(url) !== "https://codex.sale/v1/images/edits") {
        throw new Error(`unexpected fetch ${url}`);
      }
      assert.equal(typeof options.body?.get, "function");
      editCalls += 1;
      activeEdits += 1;
      maxConcurrentEdits = Math.max(maxConcurrentEdits, activeEdits);
      await new Promise((resolve) => setTimeout(resolve, 25));
      activeEdits -= 1;
      return new Response(JSON.stringify({ data: [{ b64_json: generatedPngBase64 }] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };

    const start = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/generate`)
      .send({
        sourceImageUrl: "https://example.invalid/job-source.png",
        prompt: "Create two clean marketplace product images",
        count: 2,
      })
      .expect(202);

    assert.ok(start.body.jobId);
    assert.equal(start.body.status, "queued");

    const duplicate = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/generate`)
      .send({
        sourceImageUrl: "https://example.invalid/job-source.png",
        prompt: "Create duplicate product images",
        count: 2,
      })
      .expect(202);
    assert.equal(duplicate.body.jobId, start.body.jobId);

    let jobPayload = null;
    for (let attempt = 0; attempt < 80; attempt += 1) {
      jobPayload = await agent
        .get(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/jobs/${encodeURIComponent(start.body.jobId)}`)
        .expect(200);
      if (["completed", "failed", "partial"].includes(jobPayload.body.job.status)) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    assert.equal(jobPayload.body.job.status, "completed");
    assert.equal(jobPayload.body.job.draftIds.length, 2);
    assert.equal(editCalls, 2);
    assert.equal(maxConcurrentEdits, 1);
    assert.equal(jobPayload.body.product.aiImages.length, 2);
    for (const draft of jobPayload.body.product.aiImages) {
      const generatedPathname = new URL(draft.resultUrl).pathname;
      generatedUploadPaths.push(path.join(__dirname, "..", "public", ...generatedPathname.split("/").filter(Boolean)));
    }
  } finally {
    global.fetch = originalFetch;
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).catch(() => {});
    for (const filePath of generatedUploadPaths) await fs.unlink(filePath).catch(() => {});
    await restoreFile(appSettingsPath, settingsBackup);
    await restoreFile(aiImageJobsPath, jobsBackup);
  }
});

test("warehouse product patch rejects stale expectedUpdatedAt", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-lock-${Date.now()}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const created = await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        offerId: smokeId,
        name: "Smoke Lock Product",
      })
      .expect(200);

    const currentUpdatedAt = created.body.product.updatedAt;
    assert.ok(currentUpdatedAt);

    await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(smokeId)}`)
      .send({ markup: 1.77, expectedUpdatedAt: "2026-01-01T00:00:00.000Z" })
      .expect(409);

    const ok = await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(smokeId)}`)
      .send({ markup: 1.77, expectedUpdatedAt: currentUpdatedAt })
      .expect(200);

    assert.equal(ok.body.product.markup, 1.77);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
});

test("warehouse link writes reject stale expectedUpdatedAt before validation", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-link-lock-${Date.now()}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const created = await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        offerId: smokeId,
        name: "Smoke Link Lock Product",
      })
      .expect(200);
    const staleUpdatedAt = created.body.product.updatedAt;
    await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(smokeId)}`)
      .send({ markup: 1.81, expectedUpdatedAt: staleUpdatedAt })
      .expect(200);

    const add = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/links`)
      .send({ article: "PM-DOES-NOT-MATTER", expectedUpdatedAt: staleUpdatedAt })
      .expect(409);
    assert.equal(add.body.code, "warehouse_product_conflict");
    assert.equal(add.body.conflicts[0].id, smokeId);
    assert.equal(add.body.conflicts[0].freshProduct.id, smokeId);

    const alreadyRemoved = await agent
      .delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}/links/no-link?expectedUpdatedAt=${encodeURIComponent(staleUpdatedAt)}`)
      .expect(200);
    assert.equal(alreadyRemoved.body.alreadyDeleted, true);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
});

test("single warehouse link delete ignores stale background updatedAt when link signature is unchanged", async () => {
  const agent = request.agent(app);
  const suffix = Date.now();
  const firstId = `smoke-single-link-a-${suffix}`;
  const secondId = `smoke-single-link-b-${suffix}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const first = await agent
      .post("/api/warehouse/products")
      .send({
        id: firstId,
        target: "ozon",
        marketplace: "ozon",
        offerId: "SINGLE-LINK-1",
        name: "Single link Ozon",
        links: [{ id: "link-a", article: "PM-SINGLE-1", supplierName: "Supplier A" }],
      })
      .expect(200);
    await agent
      .post("/api/warehouse/products")
      .send({
        id: secondId,
        target: "yandex-01",
        marketplace: "yandex",
        offerId: "SINGLE-LINK-1",
        name: "Single link Yandex",
        links: [{ id: "link-b", article: "PM-SINGLE-1", supplierName: "Supplier A" }],
      })
      .expect(200);

    const expectedUpdatedAt = first.body.product.updatedAt;
    const expectedLinksSignature = warehouseProductLinksSignature(first.body.product);
    await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(firstId)}`)
      .send({ markup: 1.91, expectedUpdatedAt })
      .expect(200);

    const removed = await agent
      .delete(`/api/warehouse/products/${encodeURIComponent(firstId)}/links/link-a`)
      .send({ expectedUpdatedAt, expectedLinksSignature })
      .expect(200);

    assert.equal(removed.body.changed, 2);
    assert.equal(removed.body.products.length, 2);
    assert.equal(removed.body.products.every((product) => product.links.length === 0), true);
    assert.equal(removed.body.groupLinkSignature.ok, true);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(firstId)}`).catch(() => {});
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(secondId)}`).catch(() => {});
  }
});

test("bulk warehouse link delete removes grouped marketplace refs together", async () => {
  const agent = request.agent(app);
  const suffix = Date.now();
  const firstId = `smoke-bulk-link-a-${suffix}`;
  const secondId = `smoke-bulk-link-b-${suffix}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const first = await agent
      .post("/api/warehouse/products")
      .send({
        id: firstId,
        target: "ozon",
        marketplace: "ozon",
        offerId: "BULK-LINK-1",
        name: "Bulk link Ozon",
        links: [{ id: "link-a", article: "PM-BULK-1", supplierName: "Supplier A" }],
      })
      .expect(200);
    const second = await agent
      .post("/api/warehouse/products")
      .send({
        id: secondId,
        target: "yandex-01",
        marketplace: "yandex",
        offerId: "BULK-LINK-1",
        name: "Bulk link Yandex",
        links: [{ id: "link-b", article: "PM-BULK-1", supplierName: "Supplier A" }],
      })
      .expect(200);

    const refs = [
      {
        productId: firstId,
        linkId: "link-a",
        expectedUpdatedAt: first.body.product.updatedAt,
        expectedLinksSignature: warehouseProductLinksSignature(first.body.product),
      },
    ];
    const removed = await agent
      .post("/api/warehouse/products/links/delete")
      .send({ refs })
      .expect(200);
    assert.equal(removed.body.changed, 2);
    assert.equal(removed.body.products.length, 2);
    assert.equal(removed.body.products.every((product) => product.links.length === 0), true);
    assert.equal(removed.body.groupLinkSignature.ok, true);
    assert.deepEqual(removed.body.expandedProductIds.sort(), [firstId, secondId].sort());

    const repeated = await agent
      .post("/api/warehouse/products/links/delete")
      .send({ refs: refs.map((ref) => ({ productId: ref.productId, linkId: ref.linkId })) })
      .expect(200);
    assert.equal(repeated.body.alreadyDeleted, true);
    assert.equal(repeated.body.changed, 0);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(firstId)}`).catch(() => {});
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(secondId)}`).catch(() => {});
  }
});

test("Ozon unarchive does not enforce a local 100 item daily limit", async () => {
  const accountsBackup = await backupFile(marketplaceAccountsPath);
  const queueBackup = await backupFile(ozonUnarchiveQueuePath);
  const originalFetch = global.fetch;
  const calls = [];
  global.fetch = async (url, options = {}) => {
    calls.push({ url: String(url), body: JSON.parse(options.body || "{}") });
    return new Response(JSON.stringify({ result: true }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  const unarchiveCalls = () => calls.filter((call) => String(call.url).includes("/v1/product/unarchive"));

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({
      updatedAt: new Date().toISOString(),
      accounts: [{
        id: "ozon-test",
        marketplace: "ozon",
        name: "Ozon Test",
        clientId: "client",
        apiKey: "key",
        syncEnabled: true,
      }],
    }, null, 2));
    await writeOzonUnarchiveQueue({ items: [], daily: {} });
    const products = Array.from({ length: 101 }, (_, index) => ({
      id: `ozon-queue-${index + 1}`,
      marketplace: "ozon",
      target: "ozon-test",
      productId: String(index + 1),
      offerId: `OZQ-${index + 1}`,
    }));

    const firstRun = await unarchiveProductsOnMarketplaces(products);
    assert.equal(unarchiveCalls().length, 2);
    assert.equal(unarchiveCalls()[0].body.product_id.length, 100);
    assert.deepEqual(unarchiveCalls()[1].body.product_id, [101]);
    assert.equal(firstRun.filter((item) => item.ok && !item.pending).length, 101);
    assert.equal(firstRun.filter((item) => item.queuedByDailyLimit).length, 0);

    let queue = await readOzonUnarchiveQueue();
    assert.equal(queue.items.length, 0);

    await writeOzonUnarchiveQueue({
      items: [],
      daily: {
        [ozonUnarchiveDateKey()]: {
          "ozon-test": 100,
        },
      },
    });
    const secondRun = await unarchiveProductsOnMarketplaces([products[100]]);
    assert.equal(unarchiveCalls().length, 3);
    assert.deepEqual(unarchiveCalls()[2].body.product_id, [101]);
    assert.equal(secondRun[0].ok, true);
    assert.equal(secondRun[0].pending, undefined);
    queue = await readOzonUnarchiveQueue();
    assert.equal(queue.items.length, 0);
  } finally {
    global.fetch = originalFetch;
    await restoreFile(marketplaceAccountsPath, accountsBackup);
    await restoreFile(ozonUnarchiveQueuePath, queueBackup);
  }
});

test("Ozon unarchive queues only after Ozon returns a limit error", async () => {
  const accountsBackup = await backupFile(marketplaceAccountsPath);
  const queueBackup = await backupFile(ozonUnarchiveQueuePath);
  const originalFetch = global.fetch;
  const originalAttempts = process.env.OZON_REQUEST_MAX_ATTEMPTS;
  process.env.OZON_REQUEST_MAX_ATTEMPTS = "1";
  global.fetch = async () => new Response(JSON.stringify({ message: "daily limit exceeded" }), {
    status: 429,
    headers: { "content-type": "application/json" },
  });

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({
      updatedAt: new Date().toISOString(),
      accounts: [{
        id: "ozon-test",
        marketplace: "ozon",
        name: "Ozon Test",
        clientId: "client",
        apiKey: "key",
        syncEnabled: true,
      }],
    }, null, 2));
    await writeOzonUnarchiveQueue({ items: [], daily: {} });

    const [result] = await unarchiveProductsOnMarketplaces([{
      id: "ozon-api-limit-1",
      marketplace: "ozon",
      target: "ozon-test",
      productId: "7001",
      offerId: "OZ-LIMIT-1",
    }]);

    assert.equal(result.ok, true);
    assert.equal(result.pending, true);
    assert.equal(result.queuedByDailyLimit, true);
    assert.equal(result.warning, "ozon_unarchive_daily_limit_queued");
    const queue = await readOzonUnarchiveQueue();
    assert.equal(queue.items.length, 1);
    assert.equal(queue.items[0].id, "ozon-api-limit-1");
  } finally {
    global.fetch = originalFetch;
    if (originalAttempts === undefined) delete process.env.OZON_REQUEST_MAX_ATTEMPTS;
    else process.env.OZON_REQUEST_MAX_ATTEMPTS = originalAttempts;
    await restoreFile(marketplaceAccountsPath, accountsBackup);
    await restoreFile(ozonUnarchiveQueuePath, queueBackup);
  }
});

test("Ozon unarchive resolves numeric product_id by offerId before API call", async () => {
  const accountsBackup = await backupFile(marketplaceAccountsPath);
  const queueBackup = await backupFile(ozonUnarchiveQueuePath);
  const originalFetch = global.fetch;
  const calls = [];
  global.fetch = async (url, options = {}) => {
    const body = JSON.parse(options.body || "{}");
    calls.push({ url: String(url), body });
    if (String(url).includes("/v3/product/info/list")) {
      return new Response(JSON.stringify({
        items: [{
          product_id: 777001,
          offer_id: "OZON-RESOLVE-1",
          visibility: "ARCHIVED",
        }],
      }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    return new Response(JSON.stringify({ result: true }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({
      updatedAt: new Date().toISOString(),
      accounts: [{
        id: "ozon-test",
        marketplace: "ozon",
        name: "Ozon Test",
        clientId: "client",
        apiKey: "key",
        syncEnabled: true,
      }],
    }, null, 2));
    await writeOzonUnarchiveQueue({ items: [], daily: {} });

    const actions = await unarchiveProductsOnMarketplaces([{
      id: "warehouse-uuid-1",
      marketplace: "ozon",
      target: "ozon-test",
      productId: "warehouse-uuid-1",
      offerId: "OZON-RESOLVE-1",
    }]);

    const unarchiveCall = calls.find((call) => String(call.url).includes("/v1/product/unarchive"));
    assert.ok(unarchiveCall);
    assert.deepEqual(unarchiveCall.body.product_id, [777001]);
    assert.equal(actions[0].ok, true);
    assert.equal(actions[0].ozonProductId, "777001");
  } finally {
    global.fetch = originalFetch;
    await restoreFile(marketplaceAccountsPath, accountsBackup);
    await restoreFile(ozonUnarchiveQueuePath, queueBackup);
  }
});

test("Ozon unarchive records explicit missing product_id instead of silently skipping", async () => {
  const accountsBackup = await backupFile(marketplaceAccountsPath);
  const queueBackup = await backupFile(ozonUnarchiveQueuePath);
  const originalFetch = global.fetch;
  const calls = [];
  global.fetch = async (url, options = {}) => {
    calls.push({ url: String(url), body: JSON.parse(options.body || "{}") });
    return new Response(JSON.stringify({ items: [] }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({
      updatedAt: new Date().toISOString(),
      accounts: [{
        id: "ozon-test",
        marketplace: "ozon",
        name: "Ozon Test",
        clientId: "client",
        apiKey: "key",
        syncEnabled: true,
      }],
    }, null, 2));
    await writeOzonUnarchiveQueue({ items: [], daily: {} });

    const actions = await unarchiveProductsOnMarketplaces([{
      id: "warehouse-missing-product-id",
      marketplace: "ozon",
      target: "ozon-test",
      productId: "warehouse-missing-product-id",
      offerId: "OZON-MISSING-ID",
    }]);

    assert.equal(calls.some((call) => String(call.url).includes("/v1/product/unarchive")), false);
    assert.equal(actions[0].ok, false);
    assert.equal(actions[0].pending, true);
    assert.equal(actions[0].error, "ozon_product_id_missing");

    const queue = await readOzonUnarchiveQueue();
    assert.equal(queue.items.length, 1);
    assert.equal(queue.items[0].id, "warehouse-missing-product-id");
    assert.equal(queue.items[0].warning, "ozon_product_id_missing");
    assert.equal(queue.items[0].error, "ozon_product_id_missing");
    assert.equal(queue.items[0].attempts, 1);
    assert.ok(queue.items[0].lastAttemptAt);
  } finally {
    global.fetch = originalFetch;
    await restoreFile(marketplaceAccountsPath, accountsBackup);
    await restoreFile(ozonUnarchiveQueuePath, queueBackup);
  }
});

test("Ozon unarchive verification requeues still archived products", async () => {
  const accountsBackup = await backupFile(marketplaceAccountsPath);
  const queueBackup = await backupFile(ozonUnarchiveQueuePath);
  const originalFetch = global.fetch;
  global.fetch = async (_url, options = {}) => {
    const body = JSON.parse(options.body || "{}");
    const productId = Array.isArray(body.product_id) ? body.product_id[0] : 1001;
    const offerId = Array.isArray(body.offer_id) ? body.offer_id[0] : "OZON-STILL-ARCHIVED";
    return new Response(JSON.stringify({
      items: [{
        product_id: productId,
        offer_id: offerId,
        visibility: "ARCHIVED",
        status: { state: "ARCHIVED" },
      }],
    }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({
      updatedAt: new Date().toISOString(),
      accounts: [{
        id: "ozon-test",
        marketplace: "ozon",
        name: "Ozon Test",
        clientId: "client",
        apiKey: "key",
        syncEnabled: true,
      }],
    }, null, 2));
    await writeOzonUnarchiveQueue({ items: [], daily: {} });
    const product = {
      id: "ozon-still-archived-product",
      marketplace: "ozon",
      target: "ozon-test",
      productId: "1001",
      offerId: "OZON-STILL-ARCHIVED",
    };

    const actions = await verifyOzonUnarchiveActions([product], [{
      id: product.id,
      type: "unarchive",
      target: product.target,
      offerId: product.offerId,
      ok: true,
    }], { attempts: 1, delayMs: 0 });

    assert.equal(actions[0].ok, true);
    assert.equal(actions[0].pending, true);
    assert.equal(actions[0].verified, false);
    assert.equal(actions[0].warning, "still_archived_after_unarchive");
    assert.ok(actions[0].nextRetryAt);

    const queue = await readOzonUnarchiveQueue();
    assert.equal(queue.items.length, 1);
    assert.equal(queue.items[0].id, product.id);
    assert.equal(queue.items[0].warning, "ozon_unarchive_verify_pending");
    const agent = request.agent(app);
    await agent
      .post("/api/login")
      .send({ username: process.env.APP_USER, password: process.env.APP_PASSWORD })
      .expect(200);
    const publicQueue = await agent
      .get("/api/ozon/unarchive-queue")
      .expect(200);
    assert.equal(publicQueue.body.verificationPending, 1);
    assert.equal(publicQueue.body.warningCounts.ozon_unarchive_verify_pending, 1);
  } finally {
    global.fetch = originalFetch;
    await restoreFile(marketplaceAccountsPath, accountsBackup);
    await restoreFile(ozonUnarchiveQueuePath, queueBackup);
  }
});

test("Ozon unarchive queue processor skips future rows", async () => {
  const queueBackup = await backupFile(ozonUnarchiveQueuePath);
  try {
    await writeOzonUnarchiveQueue({
      items: [{
        id: "future-row",
        productId: "1001",
        offerId: "FUTURE-1",
        target: "ozon-test",
        nextRetryAt: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
      }],
      daily: {},
    });
    const futureResult = await processOzonUnarchiveQueue({ source: "smoke_future", limit: 10 });
    assert.equal(futureResult.selected, 0);
    assert.equal((await readOzonUnarchiveQueue()).items.length, 1);
  } finally {
    await restoreFile(ozonUnarchiveQueuePath, queueBackup);
  }
});

test("Ozon unarchive queue processor sends due rows without manual retry", async () => {
  const accountsBackup = await backupFile(marketplaceAccountsPath);
  const queueBackup = await backupFile(ozonUnarchiveQueuePath);
  const warehouseBackup = await backupFile(warehousePath);
  const originalFetch = global.fetch;
  const calls = [];
  global.fetch = async (url, options = {}) => {
    const body = JSON.parse(options.body || "{}");
    calls.push({ url: String(url), body });
    if (String(url).includes("/v3/product/info/list")) {
      return new Response(JSON.stringify({
        items: [{
          product_id: 909001,
          offer_id: "QUEUE-DUE-1",
          visibility: "VISIBLE",
          status: { state: "ACTIVE" },
        }],
      }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    return new Response(JSON.stringify({ result: true }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({
      updatedAt: new Date().toISOString(),
      accounts: [{
        id: "ozon-test",
        marketplace: "ozon",
        name: "Ozon Test",
        clientId: "client",
        apiKey: "key",
        syncEnabled: true,
      }],
    }, null, 2));
    await writeWarehouse({
      products: [{
        id: "queue-due-product",
        marketplace: "ozon",
        target: "ozon-test",
        offerId: "QUEUE-DUE-1",
        productId: "909001",
        name: "Queue due product",
        marketplaceState: { code: "archived", archived: true, stock: 0 },
        links: [],
      }],
      suppliers: [],
      updatedAt: new Date().toISOString(),
    });
    await writeOzonUnarchiveQueue({
      items: [{
        id: "queue-due-product",
        warehouseProductId: "queue-due-product",
        productId: "909001",
        ozonProductId: "909001",
        offerId: "QUEUE-DUE-1",
        target: "ozon-test",
        nextRetryAt: new Date(Date.now() - 60 * 1000).toISOString(),
      }],
      daily: {},
    });

    const result = await processOzonUnarchiveQueue({ source: "smoke_due", limit: 10 });
    assert.equal(result.selected, 1);
    assert.equal(calls.some((call) => String(call.url).includes("/v1/product/unarchive")), true);
    assert.equal(calls.some((call) => String(call.url).includes("/v2/products/stocks")), true);
    assert.equal((await readOzonUnarchiveQueue()).items.length, 0);
  } finally {
    global.fetch = originalFetch;
    await restoreFile(marketplaceAccountsPath, accountsBackup);
    await restoreFile(ozonUnarchiveQueuePath, queueBackup);
    await restoreFile(warehousePath, warehouseBackup);
  }
});

test("sync-group repairs divergent PriceMaster links across marketplace siblings", async () => {
  const agent = request.agent(app);
  const suffix = Date.now();
  const firstId = `smoke-sync-link-a-${suffix}`;
  const secondId = `smoke-sync-link-b-${suffix}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    await agent
      .post("/api/warehouse/products")
      .send({
        id: firstId,
        target: "ozon",
        marketplace: "ozon",
        offerId: "SYNC-LINK-1",
        name: "Sync link Ozon",
        links: [{ id: "link-a", article: "PM-SYNC-1", supplierName: "Supplier A", partnerId: "1" }],
      })
      .expect(200);
    await agent
      .post("/api/warehouse/products")
      .send({
        id: secondId,
        target: "yandex-01",
        marketplace: "yandex",
        offerId: "SYNC-LINK-1",
        name: "Sync link Yandex",
        links: [{ id: "link-b", matchType: "selected_row", sourceRowId: "2066033", exactName: "Tester 30 ml", supplierName: "Supplier B", partnerId: "2" }],
      })
      .expect(200);

    const synced = await agent
      .post("/api/warehouse/products/links/sync-group")
      .send({ productIds: [firstId] })
      .expect(200);
    assert.equal(synced.body.products.length, 2);
    assert.equal(synced.body.products.every((product) => product.links.length === 2), true);
    assert.equal(synced.body.groupLinkSignature.ok, true);
    assert.deepEqual(synced.body.expandedProductIds.sort(), [firstId, secondId].sort());
    assert.equal(typeof synced.body.activationQueued, "boolean");
    assert.equal(typeof synced.body.recoveryQueued, "boolean");
    assert.deepEqual(synced.body.affectedProductIds.sort(), [firstId, secondId].sort());

    const repeated = await agent
      .post("/api/warehouse/products/links/sync-group")
      .send({ productIds: [firstId] })
      .expect(200);
    assert.equal(repeated.body.unchanged, true);
    assert.equal(repeated.body.disabled, true);
    assert.deepEqual(repeated.body.affectedProductIds.sort(), [firstId, secondId].sort());
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(firstId)}`).catch(() => {});
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(secondId)}`).catch(() => {});
  }
});

test("two different warehouse products can be updated with independent locks", async () => {
  const agent = request.agent(app);
  const firstId = `smoke-lock-a-${Date.now()}`;
  const secondId = `smoke-lock-b-${Date.now()}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const first = await agent
      .post("/api/warehouse/products")
      .send({ id: firstId, target: "ozon", offerId: firstId, name: "Smoke Lock A" })
      .expect(200);
    const second = await agent
      .post("/api/warehouse/products")
      .send({ id: secondId, target: "ozon", offerId: secondId, name: "Smoke Lock B" })
      .expect(200);

    await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(firstId)}`)
      .send({ markup: 1.91, expectedUpdatedAt: first.body.product.updatedAt })
      .expect(200);

    const secondUpdate = await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(secondId)}`)
      .send({ markup: 1.92, expectedUpdatedAt: second.body.product.updatedAt })
      .expect(200);
    assert.equal(secondUpdate.body.product.markup, 1.92);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(firstId)}`).expect(200);
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(secondId)}`).expect(200);
  }
});

test("AI image draft approval updates local Ozon image fields only", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-ai-approve-${Date.now()}`;
  const draftId = "draft-approved-smoke";
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        offerId: smokeId,
        name: "Smoke AI Approve Product",
        ozon: {
          offerId: smokeId,
          name: "Smoke AI Approve Product",
          primaryImage: "http://localhost/uploads/images/original.png",
          images: ["http://localhost/uploads/images/original.png"],
        },
        aiImages: [
          {
            id: draftId,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated.png",
            prompt: "Generate marketplace image",
          },
        ],
      })
      .expect(200);

    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/${encodeURIComponent(draftId)}/approve`)
      .send({})
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.product.ozon.primaryImage, "http://localhost/uploads/ai-images/generated.png");
    assert.equal(res.body.product.ozon.images[0], "http://localhost/uploads/ai-images/generated.png");
    assert.equal(res.body.product.aiImages[0].status, "approved");
    assert.equal(res.body.result, undefined);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
});

test("AI image batch approval keeps selected image first and saves batch gallery", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-ai-batch-${Date.now()}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        offerId: smokeId,
        name: "Smoke AI Batch Product",
        ozon: {
          offerId: smokeId,
          name: "Smoke AI Batch Product",
          primaryImage: "http://localhost/uploads/images/original.png",
          images: ["http://localhost/uploads/images/original.png"],
        },
        aiImages: [
          {
            id: "draft-batch-1",
            batchId: "batch-smoke",
            variantIndex: 1,
            variantTotal: 3,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated-1.png",
            prompt: "Main slide",
          },
          {
            id: "draft-batch-2",
            batchId: "batch-smoke",
            variantIndex: 2,
            variantTotal: 3,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated-2.png",
            prompt: "Benefits slide",
          },
          {
            id: "draft-batch-3",
            batchId: "batch-smoke",
            variantIndex: 3,
            variantTotal: 3,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated-3.png",
            prompt: "Notes slide",
          },
        ],
      })
      .expect(200);

    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/draft-batch-2/approve`)
      .send({})
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.product.ozon.primaryImage, "http://localhost/uploads/ai-images/generated-2.png");
    assert.deepEqual(res.body.product.ozon.images.slice(0, 3), [
      "http://localhost/uploads/ai-images/generated-2.png",
      "http://localhost/uploads/ai-images/generated-1.png",
      "http://localhost/uploads/ai-images/generated-3.png",
    ]);
    assert.equal(res.body.product.aiImages.every((item) => item.status === "approved"), true);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
});

test("automation ignores products without links", () => {
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([
    { id: "nolinks", hasLinks: false, selectedSupplier: null, noSupplierAutomation: {} },
  ]);
  assert.equal(toZeroStock.length, 0);
  assert.equal(toArchive.length, 0);
});

test("targeted automation keeps unlinked products sellable by default", () => {
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([
    {
      id: "nolinks-targeted",
      hasLinks: false,
      everHadLinks: false,
      selectedSupplier: null,
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 3 },
    },
  ], { includeNoLinks: true });
  assert.equal(toZeroStock.length, 0);
  assert.equal(toArchive.length, 0);
});

test("automation zeros formerly linked product after all links removed", () => {
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([
    {
      id: "was-linked-now-bare",
      hasLinks: false,
      everHadLinks: true,
      selectedSupplier: null,
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 3 },
      updatedAt: "2026-01-01T00:00:00.000Z",
    },
  ], { includeNoLinks: true, now: "2026-05-18T12:00:00.000Z" });
  assert.equal(toZeroStock.length, 1);
  assert.equal(toZeroStock[0].id, "was-linked-now-bare");
  assert.equal(toArchive.length, 0);
});

test("automation never zeros product that was never linked", () => {
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([
    {
      id: "never-linked",
      hasLinks: false,
      everHadLinks: false,
      selectedSupplier: null,
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 3 },
    },
  ], { includeNoLinks: true });
  assert.equal(toZeroStock.length, 0);
  assert.equal(toArchive.length, 0);
});

test("targeted no-supplier automation reports only supplied product scope when no action is needed", async () => {
  const result = await runNoSupplierMarketplaceAutomation({
    products: [{
      id: "targeted-no-action",
      hasLinks: false,
      selectedSupplier: null,
      noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z", archivedAt: "2026-01-01T00:00:00.000Z" },
      marketplaceState: { code: "archived", stock: 0 },
    }],
  }, { productIds: ["targeted-no-action"], includeNoLinks: true, source: "targeted" });
  assert.equal(result.zeroStockSent, 0);
  assert.equal(result.archived, 0);
  assert.equal(result.productStatuses.length, 1);
  assert.equal(result.productStatuses[0].id, "targeted-no-action");
});

test("supplier updates target only impacted warehouse products", () => {
  const warehouse = {
    products: [
      { id: "p1", links: [{ supplierName: "Иванна", partnerId: "101" }] },
      { id: "p2", links: [{ supplierName: "Сорин", partnerId: "202" }] },
      { id: "p3", links: [{ supplierName: "Иванна" }] },
      { id: "p4", links: [] },
    ],
  };

  assert.deepEqual(supplierImpactProductIds(warehouse, { name: "Иванна", partnerId: "101" }), ["p1", "p3"]);
  assert.deepEqual(supplierImpactProductIds(warehouse, { name: "old", partnerId: "202" }, { name: "Сорин" }), ["p2"]);
  assert.deepEqual(supplierImpactProductIds(warehouse, { name: "missing", partnerId: "999" }), []);
});

test("PriceMaster delta price push targets only linked changed rows", () => {
  const warehouse = {
    products: [
      { id: "article-match", links: [{ article: "A-1", supplierName: "Supplier", partnerId: "101" }] },
      { id: "name-match", links: [{ matchType: "exact_name", exactName: "No Article Perfume 100 ml", partnerId: "202" }] },
      { id: "same-supplier-different-article", links: [{ article: "A-2", supplierName: "Supplier", partnerId: "101" }] },
      { id: "selected-row-match", links: [{ matchType: "selected_row", sourceRowId: "77", partnerId: "303" }] },
    ],
  };
  const changes = [
    {
      type: "price_changed",
      current: { article: "A-1", name: "Alpha", partnerId: "101", partnerName: "Supplier", rowId: 10, price: 20 },
      previous: { article: "A-1", name: "Alpha", partnerId: "101", partnerName: "Supplier", rowId: 10, price: 18 },
    },
    {
      type: "returned",
      current: { article: "", name: "No Article Perfume 100 ml", partnerId: "202", partnerName: "Other", rowId: 66, active: true },
    },
    {
      type: "price_changed",
      current: { article: "Z-9", name: "Selected", partnerId: "303", partnerName: "Third", rowId: 77, price: 25 },
      previous: { article: "Z-9", name: "Selected", partnerId: "303", partnerName: "Third", rowId: 77, price: 21 },
    },
  ];

  assert.deepEqual(
    priceMasterChangeImpactProductIds(warehouse, changes).productIds,
    ["article-match", "name-match", "selected-row-match"],
  );
});

test("PriceMaster delta price push expands impacted rows to marketplace siblings", () => {
  const warehouse = {
    products: [
      { id: "ozon-row", marketplace: "ozon", offerId: "SKU-1", links: [{ article: "A-1", supplierName: "Supplier", partnerId: "101" }] },
      { id: "yandex-row", marketplace: "yandex", offerId: "SKU-1", links: [] },
      { id: "other-row", marketplace: "yandex", offerId: "SKU-2", links: [] },
    ],
  };
  const result = priceMasterChangeImpactProductIds(warehouse, [{
    type: "price_changed",
    current: { article: "A-1", name: "Alpha", partnerId: "101", partnerName: "Supplier", rowId: 10, price: 20 },
  }]);

  assert.deepEqual(result.productIds, ["ozon-row", "yandex-row"]);
  assert.equal(result.directProducts, 1);
  assert.equal(result.groupExpandedProducts, 2);
});

test("PriceMaster delta price push refuses oversized change sets", () => {
  const result = priceMasterChangeImpactProductIds(
    { products: [{ id: "p1", links: [{ article: "A-1" }] }] },
    [
      { type: "price_changed", current: { article: "A-1", partnerId: "1" } },
      { type: "price_changed", current: { article: "A-2", partnerId: "1" } },
    ],
    { maxChanges: 1 },
  );
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "too_many_pricemaster_changes");
  assert.deepEqual(result.productIds, []);
});

test("PriceMaster delta can fall back to full linked price reconcile", () => {
  const result = priceMasterChangeImpactProductIds(
    {
      products: [
        { id: "ozon-row", marketplace: "ozon", offerId: "SKU-1", links: [{ article: "A-1" }] },
        { id: "yandex-row", marketplace: "yandex", offerId: "SKU-1", links: [] },
        { id: "unlinked", marketplace: "ozon", offerId: "SKU-2", links: [] },
      ],
    },
    [
      { type: "price_changed", current: { article: "A-1", partnerId: "1" } },
      { type: "price_changed", current: { article: "A-2", partnerId: "1" } },
    ],
    { maxChanges: 1, fullReconcileOnTooMany: true },
  );
  assert.equal(result.fallbackFullReconcile, true);
  assert.equal(result.reason, "too_many_pricemaster_changes_full_reconcile");
  assert.deepEqual(result.productIds, ["ozon-row", "yandex-row"]);
});

test("marketplace sync change fingerprint ignores timestamp-only churn", () => {
  const before = [{
    id: "p1",
    target: "ozon",
    marketplace: "ozon",
    offerId: "A-1",
    marketplacePrice: 100,
    targetStock: 3,
    marketplaceState: { code: "active", active: true },
    updatedAt: "2026-01-01T00:00:00.000Z",
  }];
  const afterTimestampOnly = [{ ...before[0], updatedAt: "2026-01-02T00:00:00.000Z" }];
  const afterStockChanged = [{ ...afterTimestampOnly[0], targetStock: 0 }];

  assert.deepEqual(changedWarehouseProductIdsByAutomationFingerprint(before, afterTimestampOnly), []);
  assert.deepEqual(changedWarehouseProductIdsByAutomationFingerprint(before, afterStockChanged), ["p1"]);
});

test("background automation scope combines marketplace changes and PriceMaster delta", () => {
  const warehouse = {
    marketplaceSyncChangedProductIds: ["marketplace-change"],
    products: [
      { id: "marketplace-change", links: [] },
      { id: "pm-change", links: [{ article: "A-1" }] },
      { id: "unrelated", links: [{ article: "B-2" }] },
    ],
  };
  const result = backgroundAutomationProductIds({}, warehouse);
  assert.deepEqual(result.productIds, ["marketplace-change"]);

  const withPriceMaster = backgroundAutomationProductIds(
    { changedRows: [{ type: "price_changed", current: { article: "A-1", partnerId: "1" } }] },
    warehouse,
  );
  assert.deepEqual(withPriceMaster.productIds, ["marketplace-change", "pm-change"]);
});

test("automation zeros linked product when supplier disappeared", () => {
  const { toZeroStock } = pickNoSupplierAutomationCandidates([
    {
      id: "linked-no-supplier",
      hasLinks: true,
      selectedSupplier: null,
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 3 },
      updatedAt: "2026-01-01T00:00:00.000Z",
    },
  ], { now: "2026-05-18T12:00:00.000Z" });
  assert.equal(toZeroStock.length, 1);
  assert.equal(toZeroStock[0].id, "linked-no-supplier");
});

test("automation does not zero manually restored linked product", () => {
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([
    {
      id: "linked-manual-sellable",
      hasLinks: true,
      selectedSupplier: null,
      noSupplierAutomation: { manualSellableAt: "2026-05-18T12:00:00.000Z" },
      marketplaceState: { code: "active", stock: 3 },
    },
  ]);
  assert.equal(toZeroStock.length, 0);
  assert.equal(toArchive.length, 0);
});

test("automation does not archive linked product without supplier", () => {
  const { toArchive } = pickNoSupplierAutomationCandidates([
    { id: "candidate", hasLinks: true, selectedSupplier: null, noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z" }, marketplaceState: { code: "inactive" } },
    { id: "not-ready", hasLinks: true, selectedSupplier: null, noSupplierAutomation: {}, marketplaceState: { code: "inactive" } },
  ]);
  assert.equal(toArchive.length, 0);
});

test("automation re-zeros linked product when marketplace stock returned without supplier", () => {
  const product = {
    id: "stock-returned",
    hasLinks: true,
    selectedSupplier: null,
    noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z" },
    marketplaceState: { stock: 2 },
    updatedAt: "2026-01-01T00:00:00.000Z",
  };
  assert.equal(marketplaceHasPositiveStock(product), true);
  const { toZeroStock } = pickNoSupplierAutomationCandidates([product], { includeNoLinks: true, now: "2026-05-18T12:00:00.000Z" });
  assert.equal(toZeroStock.length, 1);
  assert.equal(toZeroStock[0].id, "stock-returned");
});

test("automation protects unlinked duplicate offer when sibling is linked", () => {
  const linked = {
    id: "linked-sku",
    marketplace: "yandex",
    target: "yandex-real",
    offerId: "DUP-SKU-1",
    hasLinks: true,
    selectedSupplier: { price: 10, available: true },
    noSupplierAutomation: {},
    marketplaceState: { code: "active", stock: 3 },
  };
  const duplicateWithoutLinks = {
    id: "unlinked-duplicate-sku",
    marketplace: "yandex",
    target: "yandex-real",
    offerId: "DUP-SKU-1",
    hasLinks: false,
    everHadLinks: false,
    selectedSupplier: null,
    noSupplierAutomation: {},
    marketplaceState: { code: "active", stock: 3 },
  };

  assert.equal(marketplaceOfferAutomationKey(linked), "yandex:yandex-real:dup-sku-1");
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([linked, duplicateWithoutLinks], { includeNoLinks: true });
  assert.deepEqual(toZeroStock.map((product) => product.id), []);
  assert.deepEqual(toArchive.map((product) => product.id), []);
});

test("stock push never sends zero for linked product while supplier is unavailable", () => {
  assert.equal(shouldSendTargetStockForProduct({
    id: "linked-without-current-supplier",
    hasLinks: true,
    ready: false,
    selectedSupplier: null,
    targetStock: null,
    marketplaceState: { code: "active", stock: 3 },
  }), false);
  assert.equal(shouldSendTargetStockForProduct({
    id: "linked-ready",
    hasLinks: true,
    ready: true,
    selectedSupplier: { price: 10, available: true },
    targetStock: 3,
    marketplaceState: { code: "active", stock: 0 },
  }), true);
});

test("target stock push only selects ready linked products with positive changed stock", () => {
  const selected = pickTargetStockSendProducts([
    {
      id: "ready-linked",
      marketplace: "yandex",
      target: "yandex-main",
      offerId: "SKU-READY",
      hasLinks: true,
      ready: true,
      selectedSupplier: { price: 10, available: true },
      targetStock: 3,
      marketplaceState: { code: "active", stock: 0 },
    },
    {
      id: "missing-supplier",
      marketplace: "yandex",
      target: "yandex-main",
      offerId: "SKU-NO-SUPPLIER",
      hasLinks: true,
      ready: false,
      selectedSupplier: null,
      targetStock: 0,
      marketplaceState: { code: "active", stock: 3 },
    },
    {
      id: "unchanged-stock",
      marketplace: "yandex",
      target: "yandex-main",
      offerId: "SKU-SAME",
      hasLinks: true,
      ready: true,
      selectedSupplier: { price: 10, available: true },
      targetStock: 3,
      marketplaceState: { code: "active", stock: 3 },
    },
    {
      id: "unlinked-positive",
      marketplace: "yandex",
      target: "yandex-main",
      offerId: "SKU-UNLINKED",
      hasLinks: false,
      ready: true,
      selectedSupplier: { price: 10, available: true },
      targetStock: 3,
      marketplaceState: { code: "active", stock: 0 },
    },
  ]);

  assert.deepEqual(selected.map((product) => product.id), ["ready-linked"]);
  assert.equal(selected[0].targetStock, 3);
});

test("Ozon stock payload targets configured warehouses and zeros all of them", async () => {
  const previous = process.env.OZON_STOCK_WAREHOUSE_IDS;
  process.env.OZON_STOCK_WAREHOUSE_IDS = "111,222";
  try {
    const targetPayload = await buildOzonStockPayloadItems(
      [{ offerId: "sku-1", targetStock: 10 }],
      { id: "ozon" },
      (item) => item.targetStock,
    );
    assert.deepEqual(targetPayload, [{ offer_id: "sku-1", warehouse_id: 111, stock: 10 }]);

    const zeroPayload = await buildOzonStockPayloadItems(
      [{ offerId: "sku-1" }],
      { id: "ozon" },
      () => 0,
      { allWarehouses: true },
    );
    assert.deepEqual(zeroPayload, [
      { offer_id: "sku-1", warehouse_id: 111, stock: 0 },
      { offer_id: "sku-1", warehouse_id: 222, stock: 0 },
    ]);
  } finally {
    if (previous === undefined) delete process.env.OZON_STOCK_WAREHOUSE_IDS;
    else process.env.OZON_STOCK_WAREHOUSE_IDS = previous;
  }
});

test("Ozon stock payload reuses stored warehouses without obsolete list call", async () => {
  const previous = process.env.OZON_STOCK_WAREHOUSE_IDS;
  delete process.env.OZON_STOCK_WAREHOUSE_IDS;
  try {
    const payload = await buildOzonStockPayloadItems(
      [{
        offerId: "sku-stored",
        marketplaceState: {
          warehouses: [
            { warehouseId: "333", warehouseName: "Gingir", stock: 4 },
            { warehouse_id: "444", name: "Backup", present: 1 },
          ],
        },
      }],
      { id: "ozon" },
      () => 0,
      { allWarehouses: true },
    );
    assert.deepEqual(payload, [
      { offer_id: "sku-stored", warehouse_id: 333, stock: 0 },
      { offer_id: "sku-stored", warehouse_id: 444, stock: 0 },
    ]);
  } finally {
    if (previous === undefined) delete process.env.OZON_STOCK_WAREHOUSE_IDS;
    else process.env.OZON_STOCK_WAREHOUSE_IDS = previous;
  }
});

test("warehouse link identity ignores client draft id duplicates", () => {
  const a = warehouseLinkIdentityKey({ id: "draft-1", article: "A-1", partnerId: "88", supplierName: " Supplier ", keyword: "Blue", priceCurrency: "rub" });
  const b = warehouseLinkIdentityKey({ id: "draft-2", article: "A-1", partnerId: "88", supplierName: "supplier", keyword: "blue", priceCurrency: "RUB" });
  assert.equal(a, b);
});

test("postgres product link identity treats null fields as real duplicate keys", () => {
  const a = productLinkPostgresIdentityKey({
    productId: "p-1",
    supplierArticle: " Art-1 ",
    partnerId: null,
    supplierName: null,
    keyword: null,
    priceCurrency: "usd",
  });
  const b = productLinkPostgresIdentityKey({
    productId: "p-1",
    supplierArticle: "art-1",
    partnerId: "",
    supplierName: "",
    keyword: "",
    priceCurrency: "USD",
  });
  assert.equal(a, b);
});

test("postgres product link rows are deduped before createMany", () => {
  const rows = dedupeProductLinkRows([
    {
      id: "old",
      productId: "p-2",
      supplierArticle: "A-2",
      partnerId: null,
      supplierName: null,
      keyword: null,
      priceCurrency: "USD",
      updatedAt: new Date("2026-01-01T00:00:00.000Z"),
    },
    {
      id: "new",
      productId: "p-2",
      supplierArticle: "a-2",
      partnerId: "",
      supplierName: "",
      keyword: "",
      priceCurrency: "usd",
      updatedAt: new Date("2026-01-02T00:00:00.000Z"),
    },
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].id, "new");
});

test("warehouse link identity keeps selected PriceMaster rows distinct from article-only links", () => {
  const a = warehouseLinkIdentityKey({
    id: "draft-1",
    matchType: "selected_row",
    article: "PM-77",
    sourceRowId: "row-a",
    supplierName: "Supplier A",
  });
  const b = warehouseLinkIdentityKey({
    id: "draft-2",
    matchType: "article",
    article: "pm-77",
    sourceRowId: "row-b",
    supplierName: " supplier a ",
  });
  assert.notEqual(a, b);
  assert.match(a, /^selected_row\|row:row-a\|/);
  assert.match(b, /^article\|article:pm-77\|/);
});

test("PriceMaster snapshot search matches legacy RowID and tokenized query", () => {
  const rows = [
    {
      RowID: "2163035",
      NativeID: "GTT81",
      NativeName: "Gritti Beyond the Wall Extrait De Parfum 100ml",
      PartnerName: "Давидгор",
      NativePrice: 124,
      Active: 1,
    },
    {
      rowId: "999",
      article: "OTHER-1",
      name: "Other product",
      partnerName: "Other supplier",
      price: 10,
      active: true,
    },
  ];
  assert.equal(snapshotRowMatchesPriceMasterSearch(rows[0], { q: "GTT81" }), true);
  assert.equal(snapshotRowMatchesPriceMasterSearch(rows[0], { q: "gritti beyond" }), true);
  assert.equal(snapshotRowMatchesPriceMasterSearch(rows[0], { q: "OTHER-1" }), false);
  const found = searchPriceMasterSnapshotJsonRows(rows, { q: "GTT81", limit: 10, usdRate: 95 });
  assert.equal(found.length, 1);
  assert.equal(found[0].rowId, "2163035");
  assert.equal(found[0].article, "GTT81");
});

test("priceMasterRowMatchesLink accepts snapshot rows with legacy RowID and NativeID fields", () => {
  const link = {
    matchType: "selected_row",
    sourceRowId: "2163035",
    partnerId: "116",
    supplierName: "Давидгор",
    keyword: "",
  };
  const legacyRow = {
    RowID: "2163035",
    NativeID: "GTT81",
    NativeName: "Gritti Beyond the Wall Extrait De Parfum 100ml",
    PartnerID: "116",
    PartnerName: "Давидгор",
    Active: 1,
  };
  assert.equal(priceMasterRowMatchesLink(legacyRow, link), true);
  assert.equal(priceMasterRowMatchesLink({ rowId: "999" }, link), false);
});

test("duplicate PriceMaster article resolver prefers matching product name and volume", () => {
  const rows = [
    {
      article: "GTT81",
      name: "Gritti Beyond the Wall Extrait De Parfum 100ml",
      price: 124,
      rowId: "2163035",
      partnerId: "116",
      partnerName: "Давидгор",
      active: true,
    },
    {
      article: "GTT81",
      name: "Gritti Tangerina edp 2ml",
      price: 3,
      rowId: "2265774",
      partnerId: "116",
      partnerName: "Давидгор",
      active: true,
    },
  ];

  const beyond = pickSafeArticlePriceMasterRow(rows, { name: "Gritti Beyond the Wall 100" });
  assert.equal(beyond.row.rowId, "2163035");
  assert.equal(beyond.resolvedBy, "product_name_score");

  const tangerina = pickSafeArticlePriceMasterRow(rows, { name: "Gritti Tangerina 2ml" });
  assert.equal(tangerina.row.rowId, "2265774");
  assert.equal(tangerina.resolvedBy, "product_name_score");
});

test("duplicate PriceMaster article resolver refuses ambiguous low-confidence matches", () => {
  const rows = [
    { article: "DUP-1", name: "Brand Alpha 100ml", price: 100, rowId: "1", partnerName: "Supplier" },
    { article: "DUP-1", name: "Brand Beta 100ml", price: 90, rowId: "2", partnerName: "Supplier" },
  ];
  const result = pickSafeArticlePriceMasterRow(rows, { name: "Brand 100" });
  assert.equal(result.ambiguous, true);
  assert.equal(result.row, null);
  assert.equal(result.candidates.length, 2);
  assert.ok(priceMasterArticleCandidateScore(rows[0], { name: "Brand Alpha 100ml" }).score > 0);
});

test("warehouse product normalization collapses duplicate supplier links by target", () => {
  const product = normalizeWarehouseProduct({
    id: "dup-link-product",
    offerId: "DUP-1",
    links: [
      {
        id: "link-original",
        article: " PM-1 ",
        supplierName: "Supplier A",
        priceCurrency: "usd",
        createdAt: "2026-01-01T00:00:00.000Z",
      },
      {
        id: "link-repeat",
        article: "pm-1",
        sourceRowId: "price-master-row-2",
        partnerId: "101",
        supplierName: " supplier a ",
        priceCurrency: "USD",
        updatedBy: "manager",
      },
    ],
  });

  assert.equal(product.links.length, 1);
  assert.equal(product.links[0].id, "link-original");
  assert.equal(product.links[0].article, "PM-1");
  assert.equal(product.links[0].supplierName, "Supplier A");
  assert.equal(product.links[0].updatedBy, "manager");
});

test("warehouse product normalization merges supplier-enriched duplicate links", () => {
  const product = normalizeWarehouseProduct({
    id: "enriched-dup-link-product",
    offerId: "DUP-ENRICHED-1",
    links: [
      {
        id: "link-original",
        article: "PM-2",
        createdAt: "2026-01-01T00:00:00.000Z",
      },
      {
        id: "link-enriched",
        article: "pm-2",
        supplierName: "Supplier B",
        partnerId: "202",
        priceCurrency: "USD",
        updatedBy: "manager",
      },
    ],
  });

  assert.equal(product.links.length, 1);
  assert.equal(product.links[0].id, "link-original");
  assert.equal(product.links[0].article, "PM-2");
  assert.equal(product.links[0].supplierName, "Supplier B");
  assert.equal(product.links[0].partnerId, "202");
});

test("warehouse link save merge enriches an existing matching link", () => {
  const product = normalizeWarehouseProduct({
    id: "save-merge-link-product",
    offerId: "SAVE-MERGE-1",
    links: [{ id: "link-original", article: "PM-22", createdAt: "2026-01-01T00:00:00.000Z" }],
  });
  const beforePrimarySignature = warehouseProductLinksSignature(product);
  const beforeDetailsSignature = warehouseProductLinkDetailsSignature(product);
  product.links[0] = mergeWarehouseLinkForSave(product.links[0], {
    article: "pm-22",
    supplierName: "Supplier Save",
    partnerId: "909",
    updatedBy: "manager",
  }, { now: "2026-05-16T10:00:00.000Z", username: "manager" });
  product.links = normalizeWarehouseProduct(product).links;

  assert.equal(product.links.length, 1);
  assert.equal(product.links[0].id, "link-original");
  assert.equal(product.links[0].supplierName, "Supplier Save");
  assert.equal(product.links[0].partnerId, "909");
  assert.notEqual(warehouseProductLinksSignature(product), beforePrimarySignature);
  assert.notEqual(warehouseProductLinkDetailsSignature(product), beforeDetailsSignature);
});

test("warehouse product normalization keeps same article for different suppliers", () => {
  const product = normalizeWarehouseProduct({
    id: "multi-supplier-link-product",
    offerId: "MULTI-SUPPLIER-1",
    links: [
      { id: "link-a", article: "PM-3", supplierName: "Supplier A", partnerId: "101" },
      { id: "link-b", article: "pm-3", supplierName: "Supplier B", partnerId: "202" },
    ],
  });

  assert.equal(product.links.length, 2);
});

test("warehouse links signature is supplier-aware for same article", () => {
  const supplierA = normalizeWarehouseProduct({
    id: "supplier-aware-a",
    links: [{ id: "link-a", article: "PM-3", supplierName: "Supplier A", partnerId: "101", priceCurrency: "USD" }],
  });
  const supplierB = normalizeWarehouseProduct({
    id: "supplier-aware-b",
    links: [{ id: "link-b", article: "PM-3", supplierName: "Supplier B", partnerId: "202", priceCurrency: "USD" }],
  });
  const both = normalizeWarehouseProduct({
    id: "supplier-aware-both",
    links: [
      { id: "link-a", article: "PM-3", supplierName: "Supplier A", partnerId: "101", priceCurrency: "USD" },
      { id: "link-b", article: "PM-3", supplierName: "Supplier B", partnerId: "202", priceCurrency: "USD" },
    ],
  });

  assert.notEqual(warehouseProductLinksSignature(supplierA), warehouseProductLinksSignature(supplierB));
  assert.match(warehouseProductLinksSignature(both), /supplier a/);
  assert.match(warehouseProductLinksSignature(both), /supplier b/);
});

test("warehouse links keep manual fallback separate from exact supplier rows", () => {
  const product = normalizeWarehouseProduct({
    id: "manual-fallback-link-product",
    offerId: "MANUAL-FALLBACK-1",
    links: [
      { id: "link-a", article: "PM-4", supplierName: "Supplier A", partnerId: "101" },
      { id: "link-manual", article: "pm-4", priceCurrency: "USD" },
    ],
  });

  assert.equal(product.links.length, 2);
});

test("warehouse link locks ignore background-only product updates", () => {
  const product = normalizeWarehouseProduct({
    id: "link-lock-product",
    offerId: "LOCK-1",
    updatedAt: "2026-05-14T10:05:00.000Z",
    links: [{ id: "link-1", article: "PM-1", supplierName: "Supplier A", updatedAt: "2026-05-14T10:00:00.000Z" }],
  });
  const expectedLinksSignature = warehouseProductLinksSignature(product);
  assert.equal(productConflict(product, {
    expectedUpdatedAt: "2026-05-14T10:00:00.000Z",
    expectedLinksSignature,
  }), null);
  const conflict = productConflict({
    ...product,
    links: [...product.links, { id: "link-2", article: "PM-2", supplierName: "Supplier B" }],
  }, {
    expectedUpdatedAt: "2026-05-14T10:00:00.000Z",
    expectedLinksSignature,
  });
  assert.equal(conflict.code, undefined);
  assert.equal(conflict.id, "link-lock-product");
});

test("warehouse link locks detect supplier changes for same PriceMaster target", () => {
  const product = normalizeWarehouseProduct({
    id: "link-enrichment-lock-product",
    offerId: "LOCK-2",
    updatedAt: "2026-05-14T10:00:00.000Z",
    links: [{ id: "link-1", article: "PM-1", updatedAt: "2026-05-14T10:00:00.000Z" }],
  });
  const expectedLinksSignature = warehouseProductLinksSignature(product);
  const enriched = normalizeWarehouseProduct({
    ...product,
    updatedAt: "2026-05-14T10:05:00.000Z",
    links: [{ ...product.links[0], supplierName: "Supplier A", partnerId: "101", updatedAt: "2026-05-14T10:05:00.000Z" }],
  });
  const conflict = productConflict(enriched, {
    expectedUpdatedAt: "2026-05-14T10:00:00.000Z",
    expectedLinksSignature,
  });
  assert.equal(conflict.id, "link-enrichment-lock-product");
});

test("warehouse link save accepts stale lock after the links were already cleared", () => {
  const product = normalizeWarehouseProduct({
    id: "link-readd-after-delete-product",
    offerId: "LOCK-3",
    updatedAt: "2026-05-14T10:05:00.000Z",
    links: [],
  });
  assert.equal(canIgnoreStaleLinkSaveConflict(product, [{ article: "PM-NEW", supplierName: "Supplier A" }], {
    expectedUpdatedAt: "2026-05-14T10:00:00.000Z",
    expectedLinksSignature: "article:pm-old",
  }), true);
  assert.equal(canIgnoreStaleLinkSaveConflict({
    ...product,
    links: [{ article: "PM-OTHER", supplierName: "Supplier B" }],
  }, [{ article: "PM-NEW", supplierName: "Supplier A" }], {
    expectedUpdatedAt: "2026-05-14T10:00:00.000Z",
    expectedLinksSignature: "article:pm-old",
  }), false);
});

test("warehouse link target detection keeps selected rows without supplier article", () => {
  assert.equal(warehouseLinkHasMatchTarget({ article: "A-1" }), true);
  assert.equal(warehouseLinkHasMatchTarget({ matchType: "selected_row", sourceRowId: "991", exactName: "Exact PM row" }), true);
  assert.equal(warehouseLinkHasMatchTarget({ matchType: "exact_name", exactName: "Exact PM row" }), true);
  assert.equal(warehouseLinkHasMatchTarget({ supplierName: "Supplier only" }), false);
});

test("warehouse links can store selected PriceMaster row without supplier article", () => {
  const product = normalizeWarehouseProduct({
    id: "name-link-product",
    links: [{
      id: "draft-row",
      article: "",
      matchType: "selected_row",
      exactName: "EX NIHILO BLUE TALISMAN 7.5ml Extrait De Parfum в коробке",
      sourceRowId: "991",
      supplierName: "Иванна",
      partnerId: "32277",
      priceCurrency: "USD",
    }],
  });
  assert.equal(product.links[0].matchType, "selected_row");
  assert.equal(product.links[0].article, "");
  assert.equal(product.links[0].exactName, "EX NIHILO BLUE TALISMAN 7.5ml Extrait De Parfum в коробке");
  assert.equal(product.links[0].sourceRowId, "991");
  assert.notEqual(
    warehouseLinkIdentityKey(product.links[0]),
    warehouseLinkIdentityKey({ ...product.links[0], sourceRowId: "992" }),
  );
});

test("warehouse links treat synthetic no-article PriceMaster ids as selected rows", () => {
  const product = normalizeWarehouseProduct({
    id: "synthetic-no-article-link-product",
    links: [{
      id: "draft-row",
      article: "__no_article__:2285084",
      matchType: "article",
      exactName: "CLEOPATRA",
      supplierName: "3185",
      priceCurrency: "USD",
    }],
  });
  assert.equal(product.links[0].article, "");
  assert.equal(product.links[0].matchType, "selected_row");
  assert.equal(product.links[0].sourceRowId, "2285084");
  assert.equal(warehouseLinkIdentityKey(product.links[0]).startsWith("selected_row|row:2285084|"), true);
});

test("recovery queues archived linked product when supplier is available", () => {
  const recovered = pickSupplierRecoveryCandidates([
    {
      id: "archived-with-supplier",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      noSupplierAutomation: {},
      marketplaceState: { code: "archived" },
    },
    {
      id: "unknown-with-supplier",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      targetStock: 3,
      noSupplierAutomation: {},
      marketplaceState: { code: "unknown", partial: true },
    },
    {
      id: "zero-stock-with-supplier",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      targetStock: 3,
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 0 },
    },
    {
      id: "active-with-supplier",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      targetStock: 3,
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 3 },
    },
  ]);
  assert.deepEqual(recovered.map((product) => product.id), [
    "archived-with-supplier",
    "unknown-with-supplier",
    "zero-stock-with-supplier",
  ]);
});

test("forced recovery includes already active linked product", () => {
  const recovered = pickSupplierRecoveryCandidates([
    {
      id: "active-linked",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 3 },
    },
  ], { force: true });
  assert.deepEqual(recovered.map((product) => product.id), ["active-linked"]);
});

test("supplier recovery treats delayed Yandex unarchive visibility as pending, not failed", () => {
  const [notVisibleStatus, stillArchivedStatus] = summarizeSupplierRecoveryProducts([
    {
      id: "pending-yandex-unarchive",
      offerId: "CC-AASH5001",
      marketplace: "yandex",
      target: "yandex-real",
      marketplaceState: { code: "archived", archived: true },
    },
    {
      id: "still-archived-after-api",
      offerId: "41044",
      marketplace: "yandex",
      target: "yandex-real",
      marketplaceState: { code: "archived", archived: true },
    },
  ], [
    { id: "pending-yandex-unarchive", type: "restore_stock", ok: true, stock: 2 },
    { id: "still-archived-after-api", type: "restore_stock", ok: true, stock: 2 },
  ], [
    {
      id: "pending-yandex-unarchive",
      type: "unarchive",
      target: "yandex-real",
      offerId: "CC-AASH5001",
      ok: true,
      pending: true,
      verified: false,
      warning: "unarchive_not_visible_after_api",
    },
    {
      id: "still-archived-after-api",
      type: "unarchive",
      target: "yandex-real",
      offerId: "41044",
      ok: true,
      pending: true,
      verified: false,
      warning: "still_archived_after_unarchive",
    },
  ]);
  assert.equal(notVisibleStatus.sellable, false);
  assert.equal(notVisibleStatus.unarchiveFailed, 0);
  assert.equal(notVisibleStatus.unarchivePending, 1);
  assert.equal(notVisibleStatus.warning, "unarchive_not_visible_after_api");
  assert.equal(stillArchivedStatus.sellable, false);
  assert.equal(stillArchivedStatus.unarchiveFailed, 0);
  assert.equal(stillArchivedStatus.unarchivePending, 1);
  assert.equal(stillArchivedStatus.warning, "still_archived_after_unarchive");
});

test("compareSupplierPickingRows sorts by supplier then product", () => {
  const rows = [
    normalizeSupplierPickingRow({ key: "b", supplierName: "Бета", productName: "Яблоко" }),
    normalizeSupplierPickingRow({ key: "a", supplierName: "Альфа", productName: "Банан" }),
    normalizeSupplierPickingRow({ key: "c", supplierName: "Альфа", productName: "Абрикос" }),
  ];
  rows.sort(compareSupplierPickingRows);
  assert.deepEqual(rows.map((row) => row.key), ["c", "a", "b"]);
});

test("picked supplier row creates a finance purchase order", async () => {
  const pickingBackup = await backupFile(supplierPickingListPath);
  const financeBackup = await backupFile(financeStatePath);
  const warehouseBackup = await backupFile(personalWarehousePath);
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: process.env.APP_USER, password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    await restoreFile(supplierPickingListPath, JSON.stringify({ rows: {}, invoices: [] }, null, 2));
    await restoreFile(financeStatePath, JSON.stringify({
      orders: [{
        id: "manual-unlinked-finance",
        marketplace: "ozon",
        orderId: "ORDER-UNLINKED",
        offerId: "UNLINKED-SKU",
        productName: "Unlinked order",
        quantity: 1,
        saleAmount: 1111,
        payoutAmount: 1111,
        purchaseCost: 100,
        profitAmount: 1011,
        source: "manual",
        status: "open",
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      }],
      expenses: [],
    }, null, 2));
    await writeWarehouse({
      products: [{
        id: "warehouse-finance-linked",
        marketplace: "ozon",
        target: "ozon-test",
        offerId: "FIN-SKU-1",
        name: "Finance Test Perfume",
        links: [{ id: "finance-link", article: "FIN-SKU-1", supplierName: "Finance Supplier", partnerId: "supplier-finance", priceCurrency: "RUB" }],
        everHadLinks: true,
        targetStock: 5,
        marketplaceState: { stock: 0, code: "active" },
      }, {
        id: "warehouse-finance-unlinked",
        marketplace: "ozon",
        target: "ozon-test",
        offerId: "UNLINKED-SKU",
        name: "Unlinked order",
        links: [],
        everHadLinks: false,
      }],
      suppliers: [],
    });
    const [row] = await createSupplierPickingRows([{
      key: "finance-picking-test",
      marketplace: "ozon",
      accountName: "Ozon Test",
      warehouseProductId: "warehouse-finance-linked",
      orderId: "ORDER-FIN-1",
      postingNumber: "POST-FIN-1",
      offerId: "FIN-SKU-1",
      productName: "Finance Test Perfume",
      quantity: 2,
      supplierName: "Finance Supplier",
      partnerId: "supplier-finance",
      price: 950,
      priceCurrency: "RUB",
      raw: { product: { price: "7290.00" } },
      ready: true,
    }], { session: { username: "admin", role: "admin" } });

    const picked = await agent
      .patch(`/api/supplier-picking-list/${encodeURIComponent(row.key)}`)
      .send({ status: "picked" })
      .expect(200);
    assert.equal(picked.body.financeOrder.offerId, "FIN-SKU-1");
    assert.equal(picked.body.financeOrder.supplierName, "Finance Supplier");
    assert.equal(picked.body.financeOrder.purchaseCost, 1900);
    assert.equal(picked.body.financeOrder.saleAmount, 14580);
    assert.equal(picked.body.financeOrder.payoutAmount, 14580);
    assert.equal(picked.body.financeOrder.profitAmount, 12680);

    const orders = await agent
      .get("/api/finance/orders?q=FIN-SKU-1&period=all")
      .expect(200);
    assert.equal(orders.body.linkedOnly, true);
    assert.equal(orders.body.orders.length, 1);
    assert.equal(orders.body.orders[0].source, "supplier_picking");
    assert.equal(orders.body.orders[0].purchaseCost, 1900);
    assert.equal(orders.body.orders[0].saleAmount, 14580);

    const allOrders = await agent
      .get("/api/finance/orders?period=all&linkedOnly=false")
      .expect(200);
    assert.equal(allOrders.body.linkedOnly, false);
    assert.equal(allOrders.body.total, 2);

    const summary = await agent
      .get("/api/finance/summary?period=all")
      .expect(200);
    assert.equal(summary.body.linkedOnly, true);
    assert.equal(summary.body.summary.orders, 1);
    assert.equal(summary.body.summary.orderIncome, 14580);

    await agent
      .patch(`/api/supplier-picking-list/${encodeURIComponent(row.key)}`)
      .send({ status: "open" })
      .expect(200);
    const afterAdminRollback = await agent
      .get("/api/finance/orders?q=FIN-SKU-1&period=all")
      .expect(200);
    assert.equal(afterAdminRollback.body.orders.length, 0);
  } finally {
    await restoreFile(supplierPickingListPath, pickingBackup);
    await restoreFile(financeStatePath, financeBackup);
    if (warehouseBackup) await writeWarehouse(JSON.parse(warehouseBackup));
    else await writeWarehouse({ products: [], suppliers: [] });
    await restoreFile(personalWarehousePath, warehouseBackup);
  }
});

test("Yandex target lookup accepts campaign and old generated target", async () => {
  const previousAccounts = await backupFile(marketplaceAccountsPath);
  const previousShops = process.env.YANDEX_SHOPS_JSON;
  try {
    await restoreFile(marketplaceAccountsPath, JSON.stringify({ updatedAt: new Date().toISOString(), accounts: [] }, null, 2));
    process.env.YANDEX_SHOPS_JSON = JSON.stringify([{
      id: "yandex-real",
      name: "Yandex",
      businessId: "171782339",
      campaignId: "128820967",
      apiKey: "token",
    }]);
    assert.equal(getYandexShopByTarget("128820967")?.id, "yandex-real");
    assert.equal(getYandexShopByTarget("yandex-06c2112c")?.id, "yandex-real");
    assert.equal(getYandexShopByTarget("legacy-yandex-target")?.id, "yandex-real");
  } finally {
    await restoreFile(marketplaceAccountsPath, previousAccounts);
    if (previousShops === undefined) delete process.env.YANDEX_SHOPS_JSON;
    else process.env.YANDEX_SHOPS_JSON = previousShops;
  }
});

test("targeted supplier recovery reports no-op without full warehouse side effects", async () => {
  const result = await runSupplierRecoveryAutomation({
    products: [{
      id: "targeted-recovery-no-action",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      noSupplierAutomation: { recoveredAt: "2026-01-01T00:00:00.000Z", stockZeroAt: null, archivedAt: null },
      marketplaceState: { code: "active" },
    }],
  }, { productIds: ["targeted-recovery-no-action"], source: "targeted" });
  assert.equal(result.source, "targeted");
  assert.equal(result.recovered, 0);
  assert.equal(result.restoredStocks, 0);
  assert.equal(result.unarchived, 0);
  assert.deepEqual(result.errors, []);
});

test("warehouse page article search matches exact identifiers only", () => {
  const exact = normalizeWarehouseProduct({
    id: "article-search-exact",
    target: "ozon",
    marketplace: "ozon",
    offerId: "NF-00004538",
    name: "Correct product",
    links: [{ id: "article-search-link", article: "PM-LINK-777", supplierName: "Supplier" }],
  });
  const partial = normalizeWarehouseProduct({
    id: "article-search-partial",
    target: "ozon",
    marketplace: "ozon",
    offerId: "NF-000045380",
    name: "Wrong product with PM-LINK-777 in text",
    links: [{ id: "article-search-link-2", article: "PM-LINK-7770", supplierName: "Supplier" }],
  });

  assert.equal(warehousePageProductMatches(exact, { q: "NF-00004538" }), true);
  assert.equal(warehousePageProductMatches(partial, { q: "NF-00004538" }), false);
  assert.equal(warehousePageProductMatches(exact, { q: "pm-link-777" }), true);
  assert.equal(warehousePageProductMatches(partial, { q: "pm-link-777" }), false);
  assert.equal(warehousePageProductMatches(partial, { q: "Wrong product" }), true);
});

test("warehouse page article search hides supplier-only matches when product id matches", () => {
  const supplierMatch = normalizeWarehouseProduct({
    id: "supplier-link-only-match",
    target: "ozon",
    marketplace: "ozon",
    offerId: "OTHER-41059",
    name: "Wrong product",
    links: [{ id: "supplier-link-only", article: "41059", supplierName: "Supplier" }],
  });
  const offerMatch = normalizeWarehouseProduct({
    id: "primary-offer-match",
    target: "yandex-real",
    marketplace: "yandex",
    offerId: "41059",
    name: "Correct product",
    links: [{ id: "primary-offer-link", article: "DIFFERENT", supplierName: "Supplier" }],
  });
  const filtered = preferWarehousePrimaryIdentityMatches([supplierMatch, offerMatch], { q: "41059" });
  assert.deepEqual(filtered.map((product) => product.id), ["primary-offer-match"]);
});

test("SKU diagnostics focuses exact product matches and reports hidden supplier matches", async () => {
  const previousWarehouse = await backupFile(warehousePath);
  try {
    await writeWarehouse({
      createdAt: "2026-05-19T00:00:00.000Z",
      updatedAt: "2026-05-19T00:00:00.000Z",
      suppliers: [],
      products: [
        {
          id: "diag-supplier-only",
          marketplace: "ozon",
          target: "ozon",
          offerId: "OTHER-41059",
          name: "Wrong product",
          links: [{ id: "diag-link-only", article: "41059", supplierName: "Supplier" }],
          marketplaceState: { code: "active", stock: 1 },
        },
        {
          id: "diag-primary",
          marketplace: "yandex",
          target: "yandex-real",
          offerId: "41059",
          name: "Correct product",
          links: [{ id: "diag-primary-link", article: "DIFFERENT", supplierName: "Supplier" }],
          selectedSupplier: { supplierName: "Supplier", article: "DIFFERENT", price: 12, available: true },
          marketplaceState: { code: "archived", archived: true, stock: 0 },
          noSupplierAutomation: { recoveredAt: "2026-05-19T00:00:00.000Z" },
          lastStockSend: {
            type: "restore_stock",
            status: "success",
            at: "2026-05-19T00:01:00.000Z",
            target: "128820967",
            offerId: "41059",
            stock: 3,
          },
          lastArchiveSend: {
            type: "unarchive",
            status: "success",
            at: "2026-05-19T00:01:01.000Z",
            target: "yandex-real",
            offerId: "41059",
          },
          lastYandexPriceSend: {
            status: "success",
            at: "2026-05-19T00:01:02.000Z",
            requestedPrice: 1990,
          },
        },
        {
          id: "diag-ozon-sibling",
          marketplace: "ozon",
          target: "ozon",
          offerId: "41059",
          productId: "123456",
          name: "Correct product Ozon",
          links: [{ id: "diag-ozon-link", article: "DIFFERENT", supplierName: "Supplier" }],
          selectedSupplier: { supplierName: "Supplier", article: "DIFFERENT", price: 11, available: true },
          marketplaceState: { code: "active", archived: false, stock: 5 },
          targetStock: 5,
        },
      ],
    });
    const diagnostics = await buildWarehouseSkuDiagnostics("41059");
    assert.equal(diagnostics.matched, 2);
    assert.equal(diagnostics.hiddenSupplierOnlyMatches, 1);
    assert.equal(diagnostics.group.groupKey, "offer:41059");
    assert.equal(diagnostics.statusSummary.total, 2);
    assert.equal(diagnostics.statusSummary.archived, 1);
    assert.equal(diagnostics.statusSummary.ready, 0);
    assert.equal(diagnostics.statusSummary.noSupplier, 1);
    assert.deepEqual(diagnostics.statusSummary.marketplaces.sort(), ["ozon", "yandex"]);
    const primary = diagnostics.products.find((product) => product.id === "diag-primary");
    const sibling = diagnostics.products.find((product) => product.id === "diag-ozon-sibling");
    assert.ok(primary);
    assert.ok(sibling);
    assert.equal(primary.archived, true);
    assert.equal(primary.saleStateCode, "archived");
    assert.equal(primary.lastStockSend.type, "restore_stock");
    assert.equal(primary.lastStockSend.stock, 3);
    assert.equal(primary.lastArchiveSend.type, "unarchive");
    assert.equal(primary.lastYandexPriceSend.requestedPrice, 1990);
    assert.equal(primary.automation.protectedFromNoSupplierArchive, true);
    assert.equal(primary.automation.wouldArchiveAsNoSupplier, false);
    assert.equal(sibling.saleStateCode, "no_supplier");
  } finally {
    if (previousWarehouse) await writeWarehouse(JSON.parse(previousWarehouse));
    else await restoreFile(warehousePath, previousWarehouse);
  }
});

test("SKU diagnostics separates stale marketplace stock from missing PriceMaster stock", async () => {
  const previousWarehouse = await backupFile(warehousePath);
  try {
    await writeWarehouse({
      products: [
        {
          id: "diag-stale-stock",
          marketplace: "ozon",
          target: "ozon",
          offerId: "STALE-STOCK-1",
          name: "Stale stock product",
          links: [{ id: "stale-link", article: "PM-STOCK", supplierName: "Supplier" }],
          hasLinks: true,
          supplierCount: 1,
          availableSupplierCount: 1,
          selectedSupplier: { supplierName: "Supplier", article: "PM-STOCK", price: 10, available: true },
          targetStock: 3,
          marketplaceState: { code: "out_of_stock", stock: 0 },
        },
      ],
      suppliers: [],
    });
    const diagnostics = await buildWarehouseSkuDiagnostics("STALE-STOCK-1");
    assert.equal(diagnostics.statusSummary.stockStale, 1);
    assert.equal(diagnostics.statusSummary.noStock, 0);
    assert.equal(diagnostics.products[0].saleStateCode, "stock_stale");
    assert.equal(diagnostics.products[0].saleReason, "linked_supplier_target_stock_positive_but_marketplace_stock_is_zero");
  } finally {
    if (previousWarehouse) await writeWarehouse(JSON.parse(previousWarehouse));
    else await restoreFile(warehousePath, previousWarehouse);
  }
});

test("no-supplier automation does not archive linked products while supplier is recalculating", () => {
  const now = "2026-05-17T12:00:00.000Z";
  const fresh = {
    id: "fresh-linked",
    hasLinks: true,
    selectedSupplier: null,
    links: [{ article: "A-1", updatedAt: now }],
    updatedAt: now,
    noSupplierAutomation: {},
    marketplaceState: { code: "active", stock: 3 },
  };
  const old = {
    ...fresh,
    id: "old-linked",
    links: [{ article: "A-2", updatedAt: "2026-05-17T11:00:00.000Z" }],
    updatedAt: "2026-05-17T11:00:00.000Z",
  };

  const freshResult = pickNoSupplierAutomationCandidates([fresh], { includeNoLinks: true, now });
  assert.equal(freshResult.toZeroStock.length, 0);
  assert.equal(freshResult.toArchive.length, 0);

  const oldResult = pickNoSupplierAutomationCandidates([old], { includeNoLinks: true, now });
  assert.equal(oldResult.toZeroStock.length, 1);
  assert.equal(oldResult.toArchive.length, 0);
});

test("yandex and ozon warehouse imports use deterministic product ids", () => {
  const {
    normalizeWarehouseProduct,
    yandexWarehouseProductId,
    ozonWarehouseProductId,
    warehouseProductCanonicalId,
    mergeProducts,
  } = require("../server.js");
  const shop = { id: "yandex-06c2112c", name: "Yandex test" };
  const offerId = "VP50BP";
  const yandex = normalizeWarehouseProduct({
    target: shop.id,
    marketplace: "yandex",
    offerId,
  });
  assert.equal(yandex.id, yandexWarehouseProductId(shop, offerId));
  assert.equal(
    warehouseProductCanonicalId({ marketplace: "yandex", target: shop.id, offerId }),
    yandexWarehouseProductId(shop, offerId),
  );
  const ozonAccount = { id: "ozon-shop-main" };
  const ozon = normalizeWarehouseProduct({
    target: ozonAccount.id,
    marketplace: "ozon",
    offerId: "SKU-100",
  });
  assert.equal(ozon.id, ozonWarehouseProductId(ozonAccount, "SKU-100"));
  const merged = mergeProducts(
    [{ id: "random-old-id", target: shop.id, marketplace: "yandex", offerId }],
    [{ id: yandexWarehouseProductId(shop, offerId), target: shop.id, marketplace: "yandex", offerId }],
  );
  assert.equal(merged[0].id, yandexWarehouseProductId(shop, offerId));
});

test("warehouse target names resolve to account labels instead of generic Ozon", () => {
  const {
    resolveWarehouseProductTargetName,
  } = require("../server.js");
  assert.equal(
    resolveWarehouseProductTargetName({ marketplace: "ozon", target: "ozon-shop-main", targetName: "Ozon" }),
    "ozon-shop-main",
  );
  assert.equal(
    resolveWarehouseProductTargetName({ marketplace: "yandex", target: "yandex-real", targetName: "Yandex" }),
    "yandex-real",
  );
  assert.equal(
    resolveWarehouseProductTargetName({
      marketplace: "ozon",
      target: "ozon",
      targetName: "Ozon",
      exports: { ozon: { targetName: "Кабинет А" } },
    }),
    "Кабинет А",
  );
});

test("linked activation runs immediately before background-job disable gate", async () => {
  const root = path.join(__dirname, "..");
  const serverSource = readServerSource();
  const start = serverSource.indexOf("async function queueLinkedProductActivation");
  assert.ok(start >= 0);
  const block = serverSource.slice(start, start + 7000);
  const immediateIdx = block.indexOf("requestMeta.immediate === true");
  const disabledIdx = block.indexOf("backgroundMarketplaceJobsBlocked()");
  assert.ok(immediateIdx >= 0);
  assert.ok(disabledIdx >= 0);
  assert.ok(immediateIdx < disabledIdx);
  assert.match(serverSource, /hydrateWarehouseProductsForIds/);
  assert.match(serverSource, /fallbackImmediate: true/);
  assert.match(serverSource, /deferOzonUnarchive: true/);
});

test("pickWarehouseSupplier chooses cheapest supplier purchase price over retail", () => {
  const { pickWarehouseSupplier } = require("../server.js");
  const selected = pickWarehouseSupplier([
    { available: true, priceEligible: true, stockOnly: false, purchaseRubPrice: 1200, effectiveFinalPrice: 900, price: 10, docDate: "2026-01-01" },
    { available: true, priceEligible: true, stockOnly: false, purchaseRubPrice: 800, effectiveFinalPrice: 1500, price: 8, docDate: "2026-01-02" },
    { available: true, priceEligible: true, stockOnly: false, purchaseRubPrice: 950, effectiveFinalPrice: 950, price: 9, docDate: "2026-01-03" },
  ]);
  assert.equal(selected.purchaseRubPrice, 800);
});

test("no-supplier automation can skip linked grace on immediate activation", () => {
  const { pickNoSupplierAutomationCandidates } = require("../server.js");
  const now = new Date("2026-06-05T12:00:00.000Z");
  const product = {
    id: "p1",
    hasLinks: true,
    selectedSupplier: null,
    updatedAt: "2026-06-05T11:59:30.000Z",
    marketplaceState: { code: "active" },
    targetStock: 5,
    stock: 5,
  };
  const withGrace = pickNoSupplierAutomationCandidates([product], { now: now.toISOString() });
  assert.equal(withGrace.toZeroStock.length, 0);
  const withoutGrace = pickNoSupplierAutomationCandidates([product], { now: now.toISOString(), skipLinkedGrace: true });
  assert.equal(withoutGrace.toZeroStock.length, 1);
});

test("ozon unarchive schedule targets 03:00 moscow", () => {
  const { nextOzonUnarchiveScheduledRunAt } = require("../server.js");
  const before = nextOzonUnarchiveScheduledRunAt(new Date("2026-06-05T20:00:00.000Z"));
  const moscowHour = new Date(before.getTime() + 3 * 60 * 60 * 1000).getUTCHours();
  assert.equal(moscowHour, 3);
  const after = nextOzonUnarchiveScheduledRunAt(new Date("2026-06-05T02:00:00.000Z"));
  const afterMoscowHour = new Date(after.getTime() + 3 * 60 * 60 * 1000).getUTCHours();
  assert.equal(afterMoscowHour, 3);
});

test("postgres page siblings ignore marketplace filter", async () => {
  const root = path.join(__dirname, "..");
  const serverSource = readServerSource();
  assert.match(serverSource, /function warehousePageSiblingWhere/);
  assert.match(serverSource, /warehousePageSiblingWhere\(baseWhere\)/);
});

test("postgres yandex materialize and canonical id migration helpers exist", () => {
  const {
    materializeYandexExportedProductsForPostgres,
    migrateWarehouseProductCanonicalIdsPostgres,
    ozonProductHasYandexExport,
    ozonProductShouldMaterializeYandexSibling,
  } = require("../server.js");
  assert.equal(typeof materializeYandexExportedProductsForPostgres, "function");
  assert.equal(typeof migrateWarehouseProductCanonicalIdsPostgres, "function");
  assert.equal(
    ozonProductHasYandexExport({ exports: { yandex: { status: "sent" } } }, { id: "yandex-shop" }),
    true,
  );
  assert.equal(
    ozonProductHasYandexExport({ exports: { "yandex-shop": { status: "sent" } } }, { id: "yandex-shop" }),
    true,
  );
  assert.equal(ozonProductHasYandexExport({ exports: {} }, { id: "yandex-shop" }), false);
  assert.equal(
    ozonProductShouldMaterializeYandexSibling(
      { offerId: "SKU-1", links: [{ article: "A1", supplierName: "PM" }] },
      { id: "yandex-shop" },
      { yandexOfferIds: new Set() },
    ),
    true,
  );
  assert.equal(
    ozonProductShouldMaterializeYandexSibling(
      { offerId: "SKU-1", links: [] },
      { id: "yandex-shop" },
      { yandexOfferIds: new Set(["sku-1"]) },
    ),
    false,
  );
});

test("ozon yandex auto pair helpers resolve source product links", () => {
  const {
    buildOzonYandexAutoPairGroupId,
    extractOzonIdFromAutoPairGroupId,
    extractYandexSourceProductId,
    findOzonMatchForYandexProduct,
    buildOzonProductLookupIndexes,
    warehouseProductPageGroupKey,
  } = require("../server.js");
  const ozon = { id: "ozon-abc", marketplace: "ozon", offerId: "SKU-9", productId: "9001" };
  assert.equal(buildOzonYandexAutoPairGroupId(ozon), "auto-pair-ozon-abc");
  assert.equal(extractOzonIdFromAutoPairGroupId("auto-pair-ozon-abc"), "ozon-abc");
  const yandex = {
    id: "yandex-1",
    marketplace: "yandex",
    offerId: "OTHER",
    yandex: { extra: { sourceProductId: "ozon-abc" } },
    manualGroupId: "auto-pair-ozon-abc",
  };
  assert.equal(extractYandexSourceProductId(yandex), "ozon-abc");
  const indexes = buildOzonProductLookupIndexes([ozon]);
  assert.equal(findOzonMatchForYandexProduct(yandex, indexes)?.id, "ozon-abc");
  assert.equal(warehouseProductPageGroupKey(yandex), "pair:ozon-abc");
  const yandexByOffer = {
    id: "yandex-2",
    marketplace: "yandex",
    offerId: "SKU-9",
    manualGroupId: "auto-pair-ozon-abc",
  };
  assert.equal(extractYandexSourceProductId(yandexByOffer), "");
  assert.equal(extractOzonIdFromAutoPairGroupId(yandexByOffer.manualGroupId), "ozon-abc");
  assert.equal(findOzonMatchForYandexProduct(yandexByOffer, indexes)?.id, "ozon-abc");
  assert.equal(warehouseProductPageGroupKey(yandexByOffer), "pair:ozon-abc");
  assert.equal(warehouseProductPageGroupKey(ozon), "offer:sku-9");
  const {
    buildWarehouseCatalogGroupContext,
    expandWarehouseProductsToGroups,
    buildWarehousePageProductGroups,
  } = require("../server.js");
  const yandexSibling = {
    id: "yandex-3",
    marketplace: "yandex",
    offerId: "SKU-9",
    yandex: { extra: { sourceProductId: "ozon-abc" } },
  };
  const grouped = buildWarehousePageProductGroups([ozon, yandexSibling]);
  assert.equal(grouped.length, 1);
  assert.equal(grouped[0].products.length, 2);
  const expanded = expandWarehouseProductsToGroups([ozon, yandexSibling], [ozon]);
  assert.equal(expanded.length, 2);
  const context = buildWarehouseCatalogGroupContext([ozon, yandexSibling]);
  assert.equal(warehouseProductPageGroupKey(ozon, context), "offer:sku-9");
  assert.equal(warehouseProductPageGroupKey(yandexSibling, context), "offer:sku-9");
  const { resolveWarehouseProductTargetName } = require("../server.js");
  assert.equal(
    resolveWarehouseProductTargetName({ target: "yandex-06c2112c", marketplace: "yandex" }),
    "Yandex Market",
  );
});

test("yandex warehouse targets with legacy aliases stay visible in postgres catalog filter", () => {
  const serverSource = readServerSource();
  assert.match(serverSource, /collectYandexWarehouseTargetAliases/);
  assert.match(serverSource, /yandexShops\.length === 1/);
  const { resolveWarehouseCanonicalTarget } = require("../server.js");
  const shops = require("../server.js").getYandexShops?.({ includeSyncDisabled: true }) || [];
  if (shops.length === 1) {
    const shopId = shops[0].id;
    assert.equal(resolveWarehouseCanonicalTarget({ marketplace: "yandex", target: "parfumerius" }), shopId);
    assert.equal(resolveWarehouseCanonicalTarget({ marketplace: "yandex", target: shopId }), shopId);
  }
});

test("marketplace jobs run inline when background worker disabled", async () => {
  const serverSource = readServerSource();
  assert.match(serverSource, /function marketplaceJobsShouldRunInline/);
  assert.match(serverSource, /ozon-unarchive-queue-process/);
  assert.doesNotMatch(
    serverSource.slice(serverSource.indexOf("async function ensureWarehousePostgresLinksBackfilled"), serverSource.indexOf("async function ensureWarehousePostgresLinksBackfilled") + 900),
    /deferred: true[\s\S]{0,120}warehousePostgresLinkBackfillDone = true/,
  );
});

test("linked product with inactive PriceMaster row is treated as missing supplier", () => {
  const { pickNoSupplierAutomationCandidates } = require("../server.js");
  const product = {
    id: "ozon-inactive-pm",
    marketplace: "ozon",
    target: "ozon",
    offerId: "45352437",
    hasLinks: true,
    links: [{
      id: "link-1",
      article: "042668",
      matchedCount: 1,
      availableCount: 0,
      missingInPriceMaster: true,
      unavailableInPriceMaster: true,
    }],
    selectedSupplier: null,
    stockOnlyFallbackActive: false,
    marketplaceState: { code: "active", stock: 5 },
    updatedAt: "2026-01-01T00:00:00.000Z",
  };
  const { toZeroStock } = pickNoSupplierAutomationCandidates([product], {
    includeNoLinks: false,
    skipLinkedGrace: true,
    now: "2026-06-07T12:00:00.000Z",
  });
  assert.deepEqual(toZeroStock.map((item) => item.id), ["ozon-inactive-pm"]);
});

test("linked warehouse catalog repair groups and syncs ozon yandex siblings", () => {
  const {
    collectWarehouseLinkRepairGroups,
    applyOzonYandexPairGroupIds,
    syncWarehouseProductGroupLinks,
    warehouseGroupLinkSignature,
  } = require("../server.js");
  const ozon = {
    id: "ozon-linked-1",
    marketplace: "ozon",
    offerId: "SKU-LINK-1",
    links: [{ article: "PM-100", matchType: "article", partnerId: "p1", supplierName: "Supplier A" }],
  };
  const yandex = {
    id: "yandex-linked-1",
    marketplace: "yandex",
    offerId: "SKU-LINK-1",
    yandex: { extra: { sourceProductId: "ozon-linked-1" } },
    links: [],
  };
  const groups = collectWarehouseLinkRepairGroups([ozon, yandex]);
  assert.equal(groups.size, 1);
  const products = Array.from(groups.values())[0];
  assert.equal(products.length, 2);
  const pairPatches = applyOzonYandexPairGroupIds(products);
  assert.equal(pairPatches.length, 2);
  const merged = Array.from(new Map([...products, ...pairPatches].map((product) => [product.id, product])).values());
  const before = warehouseGroupLinkSignature(merged);
  assert.equal(before.ok, false);
  const syncResult = syncWarehouseProductGroupLinks(merged, { username: "smoke" });
  assert.ok((syncResult.changedProducts || []).length >= 1);
  const after = warehouseGroupLinkSignature(syncResult.products || merged);
  assert.equal(after.ok, true);
  assert.equal((after.products || []).every((row) => row.linkCount > 0), true);
});

test("postgres hydrated warehouse cache is preserved across readWarehouse", () => {
  const { warehouseMemoryCacheIsHydratedStub } = require("../server.js");
  assert.equal(warehouseMemoryCacheIsHydratedStub({ postgresOnly: true, products: [{ id: "p1" }] }), true);
  assert.equal(warehouseMemoryCacheIsHydratedStub({ postgresOnly: true, products: [] }), false);
  assert.equal(warehouseMemoryCacheIsHydratedStub({ postgresOnly: false, products: [] }), false);
});

test("marketplace maintenance scheduler runs PM sync, marketplace sync and zero-stock checks", async () => {
  const serverSource = readServerSource();
  assert.match(serverSource, /runMarketplaceMaintenanceCycle/);
  assert.match(serverSource, /scheduleMarketplaceMaintenance/);
  assert.match(serverSource, /MARKETPLACE_MAINTENANCE_HOURS/);
  assert.match(serverSource, /marketplace maintenance scheduler enabled standalone/);
  assert.match(serverSource, /runNoSupplierMarketplaceAutomation\(warehouse/);
  assert.match(serverSource, /heavyBackgroundWorkShouldDefer/);
  assert.match(serverSource, /serverUnderMemoryPressure/);
});

test("interval auto sync avoids full marketplace import to prevent OOM", async () => {
  const serverSource = readServerSource();
  assert.match(serverSource, /autoSyncShouldImportMarketplaces/);
  assert.match(serverSource, /buildWarehouseView\(\{ sync: autoSyncShouldImportMarketplaces\(trigger\) \}\)/);
  assert.match(serverSource, /runAutoSyncCycle\("interval"\)/);
});

test("linked reconciler keeps linked products fresh without daily full import", async () => {
  const root = path.join(__dirname, "..");
  const serverSource = readServerSource();
  const ecosystem = await fs.readFile(path.join(root, "ecosystem.config.cjs"), "utf8");
  assert.match(serverSource, /runLinkedReconcilerBatch/);
  assert.match(serverSource, /refreshMarketplaceStateForProducts/);
  assert.match(serverSource, /queueAuthoritativePriceReprice\(\{\s*productIds: ids/);
  assert.match(serverSource, /sendTargetStocksToMarketplace\(stockProducts\)/);
  assert.match(serverSource, /daily sync skipping full marketplace import/);
  assert.match(serverSource, /code: "worker_only"/);
  assert.match(serverSource, /serverRole/);
  assert.match(ecosystem, /LINKED_RECONCILER_ENABLED: "true"/);
  assert.match(ecosystem, /DAILY_FULL_IMPORT_ENABLED: "false"/);
});

test("pm2 split entry files and immediate link activation hooks exist", async () => {
  const root = path.join(__dirname, "..");
  const serverSource = readServerSource();
  assert.match(serverSource, /SERVER_ROLE/);
  assert.match(serverSource, /isApiServer/);
  assert.match(serverSource, /isWorkerServer/);
  assert.match(serverSource, /runLinkedProductActivationImmediate/);
  assert.match(serverSource, /warehouseLinkActivationRequestMeta/);
  assert.match(serverSource, /repairWarehouseProductSupplierSnapshot/);
  await fs.access(path.join(root, "api-entry.js"));
  await fs.access(path.join(root, "worker-entry.js"));
  await fs.access(path.join(root, "ecosystem.config.cjs"));
  const ecosystem = await fs.readFile(path.join(root, "ecosystem.config.cjs"), "utf8");
  assert.match(ecosystem, /davidsklad-api/);
  assert.match(ecosystem, /davidsklad-worker/);
  assert.doesNotMatch(ecosystem, /name: "davidsklad"/);
  assert.match(ecosystem, /max-old-space-size=3072/);
  assert.match(ecosystem, /max-old-space-size=5120/);
  assert.match(ecosystem, /max_memory_restart: "4096M"/);
  assert.match(ecosystem, /max_memory_restart: "6144M"/);
  assert.match(ecosystem, /WAREHOUSE_WARM_ON_STARTUP: "false"/);
  assert.match(ecosystem, /WAREHOUSE_FULL_MEMORY_LOAD_ENABLED: "false"/);
  assert.match(ecosystem, /BULLMQ_ENABLED: "true"/);
  assert.match(serverSource, /marketplaceJobsCanEnqueue/);
  assert.match(serverSource, /ozon unarchive queue scheduler enabled standalone/);
});

test("api producer enqueues marketplace jobs when BullMQ queue exists", () => {
  const { marketplaceJobsCanEnqueue } = require("../server.js");
  assert.equal(typeof marketplaceJobsCanEnqueue, "function");
  const serverSource = readServerSource();
  const enqueueBlock = serverSource.slice(
    serverSource.indexOf("function enqueueMarketplaceJobAccepted"),
    serverSource.indexOf("function enqueueMarketplaceJobAccepted") + 1200,
  );
  assert.match(enqueueBlock, /if \(marketplaceQueue\)/);
  assert.doesNotMatch(enqueueBlock, /!backgroundJobsEnabled\) return Promise\.resolve\(null\)/);
  const activationBlock = serverSource.slice(
    serverSource.indexOf("async function queueLinkedProductActivation"),
    serverSource.indexOf("async function queueLinkedProductActivation") + 4500,
  );
  assert.match(activationBlock, /!marketplaceJobsCanEnqueue\(\) && backgroundMarketplaceJobsBlocked\(\)/);
  assert.match(serverSource, /if \(isApiServer\) return false;/);
  assert.match(serverSource, /queueUnavailable: true/);
  assert.match(enqueueBlock, /if \(isApiServer\)/);
});

test("prod post-deploy checks redis queue and worker consumer", async () => {
  const checkSource = await fs.readFile(path.join(__dirname, "..", "scripts/prod-post-deploy-check.cjs"), "utf8");
  assert.match(checkSource, /components\?\.redis\?\.ok === false/);
  assert.match(checkSource, /queue\?\.consumerReady !== true/);
  assert.match(checkSource, /bullmqFailedMax/);
  assert.match(checkSource, /fetchUnlinkedGrouped/);
});

test("worker role starts background schedulers without api HTTP port", async () => {
  const serverSource = readServerSource();
  assert.match(serverSource, /if \(isWorkerServer\) \{/);
  assert.match(serverSource, /startBackgroundSchedulers\(\)/);
  assert.match(serverSource, /WORKER_HEALTH_PORT/);
});

test("authoritative reprice loads linked products from postgres when warehouse memory is stub", async () => {
  const serverSource = readServerSource();
  assert.match(serverSource, /async function readLinkedProductsForReprice/);
  assert.match(serverSource, /const products = await readLinkedProductsForReprice/);
});

test("Ozon discount quarantine uses staged price steps under 90% drop limit", () => {
  const { planOzonQuarantinePriceSteps, computeOzonQuarantineNextPrice, resolveOzonOldPrice } = require("../server.js");
  assert.deepEqual(planOzonQuarantinePriceSteps(45700, 1346), [5027, 1346]);
  assert.equal(computeOzonQuarantineNextPrice(5027, 1346), 1346);
  assert.deepEqual(planOzonQuarantinePriceSteps(1200, 1100), [1100]);
  assert.ok(resolveOzonOldPrice(54, {}) >= 69);
});
