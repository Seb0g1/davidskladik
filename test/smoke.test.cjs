const { test } = require("node:test");
const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const fs = require("node:fs/promises");
const path = require("node:path");
const { promisify } = require("node:util");
const request = require("supertest");

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
  supplierImpactProductIds,
  pickNoSupplierAutomationCandidates,
  pickSupplierRecoveryCandidates,
  pickWarehouseSupplier,
  warehouseBrandMatches,
  normalizeWarehouseProduct,
  mergeProducts,
  applyOzonInfoToWarehouseProduct,
  productFromPostgres,
  marketplaceStateCodeFromPostgresRow,
  pickOzonDetailOfferIds,
  ozonProductNeedsDetailRefresh,
  isWeakOzonWarehouseProduct,
  pickWeakOzonProductIds,
  buildOzonStockPayloadItems,
  marketplaceHasPositiveStock,
  warehouseLinkIdentityKey,
  pickOzonCabinetListedPrice,
  shouldSkipWarehousePriceSend,
  isDuplicatePriceHistoryEntry,
  buildOzonPricePayload,
  isOzonResourceExhaustedError,
  isOzonPerItemPriceLimitError,
  isOzonOldPriceLessError,
  extractOzonPriceResponseFailures,
  buildPriceRetryItem,
  priceRetryQueueKey,
  findActiveDelayedPriceRetry,
  appendPriceHistoryRows,
  readPriceHistory,
  readPriceRetryQueue,
  writePriceRetryQueue,
  priceRetryQueuePath,
} = require("../server.js");
const postgres = require("../lib/postgres.js");
const seedPostgres = require("../scripts/seed-postgres-from-json.cjs");

test("GET /health", async () => {
  const res = await request(app).get("/health").expect(200);
  assert.equal(res.body.ok, true);
  assert.ok(res.body.service);
  assert.ok(res.body.components);
  assert.equal(res.body.components.storage.mode, "json");
  assert.equal(res.body.components.redis.queueMode, "inline");
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

test("PUT /api/settings saves markup settings", async () => {
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
        fixedUsdRate: 95,
        defaultMarkups: { ozon: 1.91, yandex: 1.62 },
        markupRules: [{ marketplace: "all", minUsd: 0, coefficient: 1.91 }],
        availabilityRules: [{ marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 }],
      })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.settings.defaultMarkups.ozon, 1.91);
    assert.equal(res.body.settings.markupRules[0].coefficient, 1.91);
    assert.equal(res.body.settings.availabilityRules[0].targetStock, 10);
  } finally {
    if (previous) {
      await agent.put("/api/settings").send(previous);
    }
  }
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

test("normalizePriceMasterPrice converts explicitly ruble values to USD", () => {
  const value = normalizePriceMasterPrice(9500, 95, "RUB");
  assert.equal(value.sourceCurrency, "RUB");
  assert.equal(value.convertedFromRub, true);
  assert.equal(value.price, 100);
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

test("resolvePriceMasterRowCurrency prefers supplier fixed currency", () => {
  const currency = resolvePriceMasterRowCurrency(
    { partnerId: "88", partnerName: "Supplier" },
    { priceCurrency: "USD" },
    {
      byPartnerId: new Map([["88", { priceCurrency: "RUB" }]]),
      byName: new Map(),
    },
  );
  assert.equal(currency, "RUB");
});

test("pickWarehouseSupplier chooses the cheapest available calculated price", () => {
  const picked = pickWarehouseSupplier([
    { partnerName: "Expensive", available: true, price: 20, calculatedPrice: 3400, docDate: "2026-01-01" },
    { partnerName: "Cheap", available: true, price: 10, calculatedPrice: 1700, docDate: "2026-01-02" },
    { partnerName: "Missing", available: false, price: 1, calculatedPrice: 100, docDate: "2026-01-03" },
  ]);
  assert.equal(picked.partnerName, "Cheap");
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

test("AI image generation requires OpenAI key before creating draft", async () => {
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
  const previousRelayUrl = process.env.OPENAI_RELAY_URL;
  const previousRelaySecret = process.env.OPENAI_RELAY_SECRET;
  delete process.env.OPENAI_API_KEY;
  delete process.env.OPENAI_RELAY_URL;
  delete process.env.OPENAI_RELAY_SECRET;
  try {
    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/generate`)
      .send({ sourceImageUrl: "http://localhost/uploads/images/source.png" })
      .expect(400);

    assert.equal(res.body.code, "openai_not_configured");
  } finally {
    if (previousKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
    if (previousRelayUrl === undefined) delete process.env.OPENAI_RELAY_URL;
    else process.env.OPENAI_RELAY_URL = previousRelayUrl;
    if (previousRelaySecret === undefined) delete process.env.OPENAI_RELAY_SECRET;
    else process.env.OPENAI_RELAY_SECRET = previousRelaySecret;
    if (product?.id) await agent.delete(`/api/warehouse/products/${encodeURIComponent(product.id)}`).expect(200);
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

    const remove = await agent
      .delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}/links/no-link?expectedUpdatedAt=${encodeURIComponent(staleUpdatedAt)}`)
      .expect(409);
    assert.equal(remove.body.code, "warehouse_product_conflict");
    assert.equal(remove.body.conflicts[0].freshProduct.id, smokeId);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
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

test("targeted automation can process a product after its last link is removed", () => {
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([
    {
      id: "nolinks-targeted",
      hasLinks: false,
      selectedSupplier: null,
      noSupplierAutomation: {},
      marketplaceState: { code: "active", stock: 3 },
    },
  ], { includeNoLinks: true });
  assert.equal(toZeroStock.length, 1);
  assert.equal(toArchive.length, 1);
  assert.equal(toZeroStock[0].id, "nolinks-targeted");
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

test("automation queues linked product for stock=0 when supplier disappeared", () => {
  const { toZeroStock } = pickNoSupplierAutomationCandidates([
    { id: "linked-no-supplier", hasLinks: true, selectedSupplier: null, noSupplierAutomation: {}, marketplaceState: { code: "active" } },
  ]);
  assert.equal(toZeroStock.length, 1);
  assert.equal(toZeroStock[0].id, "linked-no-supplier");
});

test("automation queues archive for linked product without supplier", () => {
  const { toArchive } = pickNoSupplierAutomationCandidates([
    { id: "candidate", hasLinks: true, selectedSupplier: null, noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z" }, marketplaceState: { code: "inactive" } },
    { id: "not-ready", hasLinks: true, selectedSupplier: null, noSupplierAutomation: {}, marketplaceState: { code: "inactive" } },
  ]);
  assert.equal(toArchive.length, 2);
  assert.equal(toArchive[0].id, "candidate");
});

test("automation re-queues stock=0 when marketplace stock returned after prior zero", () => {
  const product = {
    id: "stock-returned",
    hasLinks: true,
    selectedSupplier: null,
    noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z" },
    marketplaceState: { stock: 2 },
  };
  assert.equal(marketplaceHasPositiveStock(product), true);
  const { toZeroStock } = pickNoSupplierAutomationCandidates([product]);
  assert.equal(toZeroStock.length, 1);
  assert.equal(toZeroStock[0].id, "stock-returned");
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

test("warehouse links can store selected PriceMaster row without supplier article", () => {
  const product = normalizeWarehouseProduct({
    id: "name-link-product",
    links: [{
      id: "draft-row",
      article: "EX NIHILO BLUE TALISMAN 7.5ml",
      matchType: "selected_row",
      exactName: "EX NIHILO BLUE TALISMAN 7.5ml Extrait De Parfum в коробке",
      sourceRowId: "991",
      supplierName: "Иванна",
      partnerId: "32277",
      priceCurrency: "USD",
    }],
  });
  assert.equal(product.links[0].matchType, "selected_row");
  assert.equal(product.links[0].exactName, "EX NIHILO BLUE TALISMAN 7.5ml Extrait De Parfum в коробке");
  assert.equal(product.links[0].sourceRowId, "991");
  assert.notEqual(
    warehouseLinkIdentityKey(product.links[0]),
    warehouseLinkIdentityKey({ ...product.links[0], sourceRowId: "992" }),
  );
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
      id: "active-with-supplier",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      noSupplierAutomation: {},
      marketplaceState: { code: "active" },
    },
  ]);
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].id, "archived-with-supplier");
});
