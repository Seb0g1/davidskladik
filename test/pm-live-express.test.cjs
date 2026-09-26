"use strict";

// Unit tests for the live pinned PriceMaster batch (02a-price-master-link-lookup.js) and the
// express-warehouse configuration guard (02f-sorin-express-sync.js).

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const part = (name) => fs.readFileSync(path.join(__dirname, "..", "server", "parts", name), "utf8");
const cleanText = (value) => String(value ?? "").trim();
const normalizeSupplierName = (value) => String(value || "").trim().toLowerCase().replace(/\s+/g, " ");

function loadLookup(queryRows) {
  const queries = [];
  const ctx = vm.createContext({
    cleanText,
    normalizeSupplierName,
    chunkArray: (list, size) => { const out = []; for (let i = 0; i < list.length; i += size) out.push(list.slice(i, i + size)); return out; },
    priceMasterSupplierPricingMeta: () => ({ pricingMode: "usd", stockOnly: false, priceEligible: true, stockEligible: true }),
    resolvePriceMasterRowCurrency: () => "USD",
    normalizePriceMasterPrice: (price) => ({ price: Number(price), originalPrice: Number(price), sourceCurrency: "USD", convertedFromRub: false }),
    pool: { query: async ({ sql, values }) => { queries.push({ sql, values }); return [queryRows(sql, values || [])]; } },
    process: { env: {} },
  });
  vm.runInContext(`${part("02a-price-master-match-helpers.js")}
${part("02a-price-master-live-matches.js")}
${part("02a-price-master-link-lookup.js")}
this.api = { getLivePinnedPriceMasterMatches };`, ctx);
  return { api: ctx.api, queries };
}

const row = (rowId, partnerId, article, name, extra = {}) => ({
  rowId, partnerId, article, name, partnerName: partnerId === 123 ? "Далик" : "Другой", price: 18, active: 1, ...extra,
});
const ctxArgs = { stoppedMap: new Map(), supplierMaps: {}, usdRate: 90 };
const opts = { activeDocFilter: "", queryTimeout: 1000 };

test("pinned link takes only its partner's row under a shared article", async () => {
  const { api } = loadLookup((sql) => (sql.includes("NativeID") ? [
    row(1, 123, "96", "ALHAMBRA DECADENT WONDER (U) EDP 100 ML"),
    row(2, 9, "96", "SOME OTHER PERFUME"),
  ] : []));
  const link = { id: "L", matchType: "selected_row", article: "96", partnerId: "123", sourceRowId: "1", exactName: "ALHAMBRA DECADENT WONDER (U) EDP 100 ML" };
  const map = await api.getLivePinnedPriceMasterMatches([link], ctxArgs, opts);
  assert.deepEqual(Array.from(map.get("L"), (r) => r.rowId), [1]);
});

test("article moved: pinned product found by its name, candidates not duplicated", async () => {
  const { api, queries } = loadLookup((sql) => {
    if (sql.includes("NativeName IN")) return [row(7, 123, "412", "ALHAMBRA DECADENT WONDER (U) EDP 100 ML")];
    return [row(1, 123, "96", "LATTAFA BADEE AL OUD HONOR GLORY(U) EDP 100 ML"), row(7, 123, "412", "ALHAMBRA DECADENT WONDER (U) EDP 100 ML")];
  });
  const link = { id: "L", matchType: "selected_row", article: "96", partnerId: "123", sourceRowId: "1", exactName: "ALHAMBRA DECADENT WONDER (U) EDP 100 ML" };
  const other = { id: "M", matchType: "selected_row", article: "412", partnerId: "123", sourceRowId: "7", exactName: "ALHAMBRA DECADENT WONDER (U) EDP 100 ML" };
  const map = await api.getLivePinnedPriceMasterMatches([link, other], ctxArgs, opts);
  assert.deepEqual(Array.from(map.get("L"), (r) => r.rowId), [7]);
  assert.equal(queries.filter((q) => q.sql.includes("NativeName IN")).length, 1);
});

test("inactive rows are not in the live result: the pinned product is unavailable", async () => {
  const { api } = loadLookup(() => []);
  const link = { id: "L", matchType: "selected_row", article: "2452", partnerId: "119", sourceRowId: "2308284", exactName: "CACHAREL AMOR AMOR wom edt  50 ml" };
  const map = await api.getLivePinnedPriceMasterMatches([link], ctxArgs, opts);
  assert.equal(map.get("L").length, 0);
});

function loadExpress({ shops, env = {} }) {
  const warnings = [];
  const ctx = vm.createContext({
    cleanText,
    normalizeSupplierName,
    isSorinSupplierName: (name) => /сорин|sorin/i.test(String(name)),
    stripSupplierLegalFormPrefix: (value) => value,
    getYandexShops: () => shops,
    logger: { warn: (msg, meta) => warnings.push({ msg, meta }), info: () => {} },
    process: { env },
  });
  vm.runInContext(`${part("02f-sorin-express-sync.js")}
this.api = { expressSyncConfig, expressSupplierKey };`, ctx);
  return { api: ctx.api, warnings };
}

test("express campaign pointing at the main shop falls back to the express campaign", () => {
  const { api, warnings } = loadExpress({ shops: [{ campaignId: "128820967" }] });
  const config = api.expressSyncConfig({ sorinExpress: { enabled: true, stock: 2, yandexCampaignId: "128820967", ozonWarehouseId: "1020005000398404" } });
  assert.equal(config.yandexCampaignId, "149026853");
  assert.equal(warnings[0].msg, "express_campaign_is_regular_shop");
});

test("express suppliers: Сорин and «Наш склад» only", () => {
  const { api } = loadExpress({ shops: [] });
  assert.equal(api.expressSupplierKey("Сорин"), "sorin");
  assert.equal(api.expressSupplierKey("Наш склад"), "our_stock");
  assert.equal(api.expressSupplierKey("Далик"), "");
  assert.equal(api.expressSupplierKey("Наш склад 2"), "");
});
