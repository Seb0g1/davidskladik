"use strict";

// warehousePairNamesCompatible (server/parts/02a-yandex-warehouse-pairing.js): cards that share an offer id
// across Ozon cabinets and Маркет are paired only when the names can be one product.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-yandex-warehouse-pairing.js"), "utf8");
const ctx = vm.createContext({ cleanText: (v) => String(v ?? "").trim(), isDuplicateMarkerName: (n) => /^дубль/i.test(String(n || "")) });
vm.runInContext(`${source}\nthis.api = { warehousePairNamesCompatible, warehouseOfferPairAllowed };`, ctx);
const { warehousePairNamesCompatible: ok, warehouseOfferPairAllowed: allowed } = ctx.api;

test("different products under one offer id are not a pair", () => {
  assert.equal(ok("DELILAH Lip Line Long Wear Retractable Pencil - Naked - карандаш для губ 0,31 г", "NARCYSS Le Fix Emergency Facial Mask Intense Hydration 3 x 23 ml - тканевая маска для лица"), false);
  assert.equal(ok("Clive Christian Crab Apple Blossom Парфюмированная вода унисекс 10мл", "Keune Кондиционер Шелковый уход CARE Satin Oil Conditioner 250мл"), false);
  assert.equal(ok("PINZETTA PUNTA OBLIQUA - пинцет с косым кончиком малый", "Парфюмерная вода Elizabeth Arden Green Tea Eau de Parfum женская"), false);
});

test("the same product in two shops stays a pair", () => {
  assert.equal(ok("Keune Кондиционер Шелковый уход CARE Satin Oil Conditioner 250мл", "Keune Кондиционер Шелковый уход CARE Satin Oil Conditioner 250мл"), true);
  assert.equal(ok("YSL Libre FLOWERS & FLAMES florale Парфюмерная вода для женщин 50 мл", "Yves Saint Laurent Libre Flowers & Flames 50 мл"), true);
});

test("a name with nothing to judge by never breaks a pair", () => {
  assert.equal(ok("Mona Di Orio Musc 3*10 мл набор парфюмерная вода", "MDO52"), true);
  assert.equal(ok("Дубль108 Chateau", "Chateau de Versailles SET Набор парфюмерный 4*10мл"), true);
  assert.equal(ok("Для волос", "Краска"), true);
});

test("a group set by hand is never replaced by an automatic pair", () => {
  const card = (name, grp) => ({ name, raw: grp ? { manualGroupId: grp } : {} });
  assert.equal(allowed(card("Keune Satin Oil Conditioner", "own-x"), card("Keune Satin Oil Conditioner")), false);
  assert.equal(allowed(card("Keune Satin Oil Conditioner", "auto-pair-ozon-1"), card("Keune Satin Oil Conditioner")), true);
});
