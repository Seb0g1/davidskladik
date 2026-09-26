"use strict";

// Unit tests for filterSelectedRowMatchesToBestPin (server/parts/02a-price-master-match-helpers.js):
// a pinned PriceMaster row must never be swapped for a different product that a supplier
// (Далик) re-used the same article for.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-price-master-match-helpers.js"), "utf8");
const ctx = vm.createContext({
  cleanText: (value) => String(value ?? "").trim(),
  normalizeSupplierName: (value) => String(value || "").trim().toLowerCase(),
});
vm.runInContext(`${source}
this.api = { filterSelectedRowMatchesToBestPin, pmRowConfirmsPinnedName, priceMasterRowMatchesLink };`, ctx);
const { filterSelectedRowMatchesToBestPin: filter, pmRowConfirmsPinnedName, priceMasterRowMatchesLink } = ctx.api;

const link = {
  matchType: "selected_row",
  sourceRowId: "100",
  article: "LQOA100",
  resolvedPriceMasterRow: { name: "LATTAFA QUEEN OF ARABIA EDP 100 ML" },
};
const row = (rowId, name, extra = {}) => ({ rowId, name, article: "LQOA100", active: true, price: 20, ...extra });
const ids = (rows) => Array.from(rows, (r) => r.rowId);

test("pinned row present: kept even when it is the only match", () => {
  assert.deepEqual(ids(filter(link, [row("100", "LATTAFA QUEEN OF ARABIA EDP 100 ML")])), ["100"]);
});

test("pinned row gone, a different product under the same article: nothing is returned", () => {
  assert.deepEqual(ids(filter(link, [row("205", "ALHAMBRA KISMET EDP 100 ML")])), []);
});

test("pinned row gone, different product sharing only the brand: nothing is returned", () => {
  assert.deepEqual(ids(filter(link, [row("206", "LATTAFA KHAMRAH QAHWA EDP 100 ML")])), []);
});

test("pinned row re-uploaded with a new RowID and slightly different name: followed", () => {
  assert.deepEqual(ids(filter(link, [row("300", "LATTAFA QUEEN OF ARABIA 100ML EDP")])), ["300"]);
});

test("several article rows: only the one confirming the pinned name survives", () => {
  const out = filter(link, [
    row("205", "ALHAMBRA KISMET EDP 100 ML"),
    row("300", "LATTAFA QUEEN OF ARABIA EDP 100ML"),
    row("206", "LATTAFA KHAMRAH QAHWA EDP 100 ML"),
  ]);
  assert.deepEqual(ids(out), ["300"]);
});

test("old links without a pinned name keep all rows for order-name disambiguation", () => {
  const legacy = { matchType: "selected_row", sourceRowId: "100", article: "LQOA100" };
  assert.deepEqual(ids(filter(legacy, [row("205", "ALHAMBRA KISMET EDP 100 ML")])), ["205"]);
});

test("pmRowConfirmsPinnedName rejects brand-only overlap", () => {
  assert.equal(pmRowConfirmsPinnedName({ name: "LATTAFA ASAD EDP 100 ML" }, "LATTAFA QUEEN OF ARABIA EDP 100 ML"), false);
  assert.equal(pmRowConfirmsPinnedName({ name: "Lattafa Queen of Arabia 100 ml" }, "LATTAFA QUEEN OF ARABIA EDP 100 ML"), true);
});

// ADW100: Далик moved article 96 from ALHAMBRA DECADENT WONDER to LATTAFA BADEE AL OUD. PriceMaster
// updates rows in place, so the pinned RowID itself now carries the other perfume.
const adw = {
  matchType: "selected_row",
  sourceRowId: "500",
  article: "96",
  partnerId: "7",
  supplierName: "Далик",
  exactName: "ALHAMBRA DECADENT WONDER EDP 100 ML",
};
const dalik = (rowId, name, article = "96") => ({ rowId, name, article, partnerId: "7", partnerName: "Далик", active: true, price: 18 });

test("pinned RowID now carrying another perfume is not trusted", () => {
  assert.deepEqual(ids(filter(adw, [dalik("500", "LATTAFA BADEE AL OUD HONOR GLORY(U) EDP 100 ML")])), []);
});

test("pinned RowID renamed to another perfume, the pinned perfume under a new article: follows the name", () => {
  const out = filter(adw, [
    dalik("500", "LATTAFA BADEE AL OUD HONOR GLORY(U) EDP 100 ML"),
    dalik("777", "ALHAMBRA DECADENT WONDER EDP 100 ML", "412"),
  ]);
  assert.deepEqual(ids(out), ["777"]);
});

test("pinned row with its own name is still returned", () => {
  const out = filter(adw, [
    dalik("500", "ALHAMBRA DECADENT WONDER EDP 100 ML"),
    dalik("501", "LATTAFA BADEE AL OUD HONOR GLORY(U) EDP 100 ML"),
  ]);
  assert.deepEqual(ids(out), ["500"]);
});

test("a row found by the pinned name under another article matches the link", () => {
  assert.equal(priceMasterRowMatchesLink(dalik("777", "ALHAMBRA DECADENT WONDER EDP 100 ML", "412"), adw), true);
  assert.equal(priceMasterRowMatchesLink({ ...dalik("777", "ALHAMBRA DECADENT WONDER EDP 100 ML", "412"), partnerId: "8" }, adw), false);
});

test("article link with a saved row name: a reused article keeps only that product", () => {
  const articleLink = {
    matchType: "article",
    article: "96",
    supplierName: "Далик",
    resolvedPriceMasterRow: { name: "ALHAMBRA DECADENT WONDER EDP 100 ML", partnerId: "7" },
  };
  const out = filter(articleLink, [
    dalik("501", "LATTAFA BADEE AL OUD HONOR GLORY(U) EDP 100 ML"),
    dalik("502", "ALHAMBRA DECADENT WONDER EDP 100ML"),
  ]);
  assert.deepEqual(ids(out), ["502"]);
});

test("article link: other suppliers under the same article are not filtered by the saved name", () => {
  const articleLink = {
    matchType: "article",
    article: "96",
    resolvedPriceMasterRow: { name: "ALHAMBRA DECADENT WONDER EDP 100 ML", partnerId: "7" },
  };
  const other = (rowId, name) => ({ rowId, name, article: "96", partnerId: "9", partnerName: "Другой", active: true, price: 20 });
  const out = filter(articleLink, [
    dalik("502", "ALHAMBRA DECADENT WONDER EDP 100ML"),
    other("601", "Some perfume A"),
    other("602", "Some perfume B"),
  ]);
  assert.deepEqual(ids(out), ["502", "601", "602"]);
});

// normalizeWarehouseLink restores pins the old daily Далик check had cleared to bare articles.
const normCtx = vm.createContext({
  cleanText: (value) => String(value ?? "").trim(),
  crypto: require("crypto"),
  parsePriceMasterNoArticleRowId: () => "",
  cloneAuditValue: (value) => JSON.parse(JSON.stringify(value)),
});
vm.runInContext(`${fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-normalizers-warehouse-links.js"), "utf8")}
this.api = { normalizeWarehouseLink };`, normCtx);
const { normalizeWarehouseLink } = normCtx.api;

test("cleared Далик pin is restored from the saved row", () => {
  const link = normalizeWarehouseLink({
    matchType: "article", article: "96", sourceRowId: null, exactName: null, resolvedBy: "selected_row",
    supplierName: "Далик", partnerId: "7",
    resolvedPriceMasterRow: { rowId: "500", name: "ALHAMBRA DECADENT WONDER EDP 100 ML" },
  });
  assert.equal(link.matchType, "selected_row");
  assert.equal(link.sourceRowId, "500");
  assert.equal(link.exactName, "ALHAMBRA DECADENT WONDER EDP 100 ML");
});

test("a plain article link stays an article link", () => {
  const link = normalizeWarehouseLink({
    matchType: "article", article: "96", resolvedBy: "article_unique",
    resolvedPriceMasterRow: { rowId: "500", name: "ALHAMBRA DECADENT WONDER EDP 100 ML" },
  });
  assert.equal(link.matchType, "article");
  assert.equal(link.sourceRowId, "");
});

// getPriceMasterMatchesForLinks (snapshot): the pinned product moved to another article.
test("snapshot lookup follows the pinned name to a new article", async () => {
  const rows = [
    { rowId: "500", article: "96", name: "LATTAFA BADEE AL OUD HONOR GLORY(U) EDP 100 ML", partnerId: "7", partnerName: "Далик", price: 18, active: true },
    { rowId: "777", article: "412", name: "ALHAMBRA DECADENT WONDER EDP 100 ML", partnerId: "7", partnerName: "Далик", price: 17, active: true },
    { rowId: "888", article: "412", name: "ALHAMBRA DECADENT WONDER EDP 100 ML", partnerId: "9", partnerName: "Другой", price: 10, active: true },
  ];
  const index = (key) => {
    const map = new Map();
    for (const row of rows) {
      const k = key(row);
      if (!map.has(k)) map.set(k, []);
      map.get(k).push(row);
    }
    return map;
  };
  const liveCtx = vm.createContext({
    cleanText: (value) => String(value ?? "").trim(),
    normalizeSupplierName: (value) => String(value || "").trim().toLowerCase(),
    normalizeWarehouseLink: (link) => link,
    stoppedSupplierMap: () => new Map(),
    managedSupplierMaps: () => ({}),
    priceMasterSupplierPricingMeta: () => ({ pricingMode: "usd", stockOnly: false, priceEligible: true, stockEligible: true }),
    resolvePriceMasterRowCurrency: () => "USD",
    normalizePriceMasterPrice: (price) => ({ price: Number(price), originalPrice: Number(price), sourceCurrency: "USD", convertedFromRub: false }),
    getPriceMasterSnapshotIndexes: async () => ({
      byArticle: index((r) => r.article),
      byName: index((r) => r.name.toLowerCase()),
      byRowId: index((r) => r.rowId),
      rows,
    }),
  });
  vm.runInContext(`${source}
${fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-price-master-live-matches.js"), "utf8")}
this.api = { getPriceMasterMatchesForLinks };`, liveCtx);
  const map = await liveCtx.api.getPriceMasterMatchesForLinks([{ ...adw, id: "L1" }], [], 90);
  assert.deepEqual(ids(map.get("L1")), ["777"]);
});
