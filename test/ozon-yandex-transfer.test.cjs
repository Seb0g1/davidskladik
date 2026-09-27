"use strict";

// Ozon → Yandex Market transfer rules (server/parts/02a-ozon-yandex-offer-rules.js) and the
// offer-mappings/update send path (sendYandexOfferMappings in 02a-yandex-ozon-yandex-read.js).

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const part = (name) => fs.readFileSync(path.join(__dirname, "..", "server", "parts", name), "utf8");
const arr = (value) => JSON.parse(JSON.stringify(value));

function loadRules(extra = {}) {
  const ctx = vm.createContext({ ...extra });
  vm.runInContext(`${part("02a-ozon-yandex-offer-rules.js")}
this.api = { resolveYandexCategoryForOzonProduct, isValidGtin, pickYandexGtins, isPlaceholderVendor, yandexCountryName,
  resolveYandexOfferName, buildPerfumeVariantParameters, sanitizeYandexCommodityCodes, normalizeYandexCertificate,
  sanitizeYandexShelfLife, diffYandexOfferForUpdate, parseYandexOfferMappingsResult, ozonAttributeValue };`, ctx);
  return ctx.api;
}
const r = loadRules();

test("category: perfumery types, other kinds by the table", () => {
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "Guerlain Colours of Love Парфюмерная вода 50 мл" }).categoryId, 15927546);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93405, name: "Туалетная вода 50 мл" }).categoryId, 15927546);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93950, name: "Kerastase шампунь 250 мл" }).categoryId, 91183);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93466, name: "YSL Y Дезодорант 75 гр" }).categoryId, 8480725);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 95741, name: "Diptyque Baies свеча 190 г" }).categoryId, 91304);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 92718, name: "Диффузор 200 мл" }).categoryId, 61329715);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 92721, name: "Спрей для дома" }).categoryId, 61343235);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 97704, name: "Molton Brown гель для душа 300 мл" }).categoryId, 91176);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93883, name: "Крем для тела 200 мл" }).categoryId, 8475955);
});

test("category: not guessed — face cream, sets, unknown types, conflicts go to manual review", () => {
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93883, name: "Крем для лица 50 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93404, name: "Набор парфюмерный 100 мл + 30 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "STATE OF MIND Open Mind Дорожный набор парфюмерии 20 + 20 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "Mizensir Tres Chere 5x8 ml" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93440, name: "Губная помада" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "Шампунь для волос" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 0, name: "что-то непонятное" }).categoryId, null);
});

test("barcodes: only real GTINs", () => {
  assert.equal(r.isValidGtin("8427395940209"), true);
  assert.equal(r.isValidGtin("3614273069045"), true);
  assert.equal(r.isValidGtin("8427395940208"), false); // wrong check digit
  assert.equal(r.isValidGtin("OZN5767288854"), false);
  assert.equal(r.isValidGtin("2000000000015"), false); // internal 2…
  assert.equal(r.isValidGtin("02000000000015"), false); // internal 02…
  assert.equal(r.isValidGtin("5346523523"), false); // 10 digits
  assert.deepEqual(arr(r.pickYandexGtins(["OZN1", "8427395940209", "532845-875634", "8427395940209"], { offerId: "X" })), ["8427395940209"]);
  assert.deepEqual(arr(r.pickYandexGtins(["8427395940209"], { offerId: "8427395940209" })), []);
});

test("brand placeholders are rejected", () => {
  assert.equal(r.isPlaceholderVendor("Без бренда"), true);
  assert.equal(r.isPlaceholderVendor("Нет бренда"), true);
  assert.equal(r.isPlaceholderVendor("Нф-00001056"), true);
  assert.equal(r.isPlaceholderVendor("L'Artisan Parfumeur"), false);
});

test("name: no article, no all-caps, no Ozon junk", () => {
  assert.equal(r.resolveYandexOfferName({ candidates: ["BLN16", "Byredo Blanche 50 мл"], offerId: "BLN16" }), "Byredo Blanche 50 мл");
  assert.equal(r.resolveYandexOfferName({ candidates: ["Дубль95"], offerId: "124097" }), "");
  assert.equal(r.resolveYandexOfferName({ candidates: ["ъ-Оъ"], offerId: "=980" }), "");
  assert.equal(r.resolveYandexOfferName({ candidates: ["LATTAFA BADEE AL OUD EDP 100 ML"], vendor: "Lattafa" }), "Lattafa Badee Al Oud EDP 100 мл");
  assert.equal(r.resolveYandexOfferName({ candidates: ["MAISON TAHITE SEL VANILLE Парфюмерная вода 1.2 мл"] }), "MAISON TAHITE SEL VANILLE Парфюмерная вода 1.2 мл");
});

test("variant group per aroma + kind, volume and tester as distinguishing params", () => {
  const params = r.buildPerfumeVariantParameters({ categoryId: 15927546, vendor: "Guerlain", model: "Colours of Love", kind: "туалетная вода", name: "Guerlain Colours of Love Туалетная вода 50 мл" });
  const byId = Object.fromEntries(Array.from(params, (item) => [item.parameterId, item.value]));
  assert.equal(byId[200], "Guerlain Colours of Love туалетная вода");
  assert.equal(byId[24139073], "50");
  assert.equal(byId[53763303], "false");
  const tester = r.buildPerfumeVariantParameters({ categoryId: 15927546, vendor: "Guerlain", model: "Colours of Love", kind: "туалетная вода", name: "Guerlain Colours of Love тестер 50 мл" });
  assert.equal(tester.find((item) => item.parameterId === 53763303).value, "true");
  // No model → no group (never merge different aromas).
  const noModel = r.buildPerfumeVariantParameters({ categoryId: 15927546, vendor: "Guerlain", model: "", name: "x 50 мл" });
  assert.equal(noModel.some((item) => item.parameterId === 200), false);
  assert.equal(r.buildPerfumeVariantParameters({ categoryId: 91183, vendor: "A", model: "B", name: "c 50 мл" }).length, 0);
});

test("codes, documents, shelf life, country", () => {
  assert.deepEqual(arr(r.sanitizeYandexCommodityCodes([{ code: "3303001000", type: "CUSTOMS_COMMODITY_CODE" }, { code: "20.42.11.120", type: "OKPD2_CODE" }])),
    [{ code: "3303001000", type: "CUSTOMS_COMMODITY_CODE" }, { code: "20.42.11.120", type: "OKPD2_CODE" }]);
  assert.equal(r.sanitizeYandexCommodityCodes([{ code: "3303001000", type: "CUSTOMS_COMMODITY_CODE" }, { code: "20.42.11", type: "OKPD2_CODE" }]), undefined);
  assert.equal(r.normalizeYandexCertificate("ЕАЭС N RU ДFR.РА03.В.19152/26"), "ЕАЭС N RU Д-FR.РА03.В.19152/26");
  assert.equal(r.normalizeYandexCertificate("ЕАЭС N RU Д-FR.РА03.В.19152/26"), "ЕАЭС N RU Д-FR.РА03.В.19152/26");
  assert.deepEqual(arr(r.sanitizeYandexShelfLife({ timePeriod: 36, timeUnit: "MONTHS" })), { timePeriod: 36, timeUnit: "MONTH" });
  assert.equal(r.yandexCountryName("Корея (Южная)"), "Южная Корея");
  assert.equal(r.yandexCountryName("Франция"), "Франция");
});

test("diff: existing card gets only changed fields, foreign-owned fields never overwritten", () => {
  const built = { offerId: "A", name: "New name", vendor: "Guerlain", marketCategoryId: 15927546, barcodes: ["8427395940209"], weightDimensions: { length: 20, width: 13, height: 13, weight: 0.4 } };
  const current = { offerId: "A", name: "New name", vendor: "Guerlain", barcodes: ["3614273069045"], weightDimensions: { length: 20, width: 13, height: 13, weight: 0.4 } };
  assert.equal(r.diffYandexOfferForUpdate(built, current, 15927546), null);
  const recategorize = r.diffYandexOfferForUpdate(built, current, 91183);
  assert.deepEqual(Object.keys(recategorize).sort(), ["marketCategoryId", "offerId"]);
  const noBarcodes = r.diffYandexOfferForUpdate(built, { ...current, barcodes: [] }, 15927546);
  assert.deepEqual(arr(noBarcodes.barcodes), ["8427395940209"]);
});

function loadSender(respond) {
  const calls = [];
  const logs = [];
  const ctx = vm.createContext({
    cleanText: (value) => String(value ?? "").trim(),
    chunkArray: (list, size) => { const out = []; for (let i = 0; i < list.length; i += size) out.push(list.slice(i, i + size)); return out; },
    logger: { warn: (msg, meta) => logs.push({ msg, meta }), info: (msg, meta) => logs.push({ msg, meta }) },
    assessYandexSmallVolume: () => ({ blocked: false }),
    getYandexOfferMappingsByOfferIds: async () => [{ offer: { offerId: "OLD", name: "Old", barcodes: ["3614273069045"] }, mapping: { marketCategoryId: 91183 } }],
    yandexRequest: async (_shop, _method, _path, body) => { calls.push(body); return respond(body, calls.length); },
  });
  vm.runInContext(`${part("02a-ozon-yandex-offer-rules.js")}
${part("02a-yandex-ozon-yandex-read.js")}
this.api = { sendYandexOfferMappings };`, ctx);
  return { send: ctx.api.sendYandexOfferMappings, calls, logs };
}

test("send: offer with errors is dropped and the rest resent; results parsed per offer", async () => {
  const t = loadSender((body, n) => (n === 1
    ? { status: "OK", results: [{ offerId: "BAD", errors: [{ type: "INVALID_COMMODITY_CODE", message: "Код ОКПД 2 неверный" }] }] }
    : { status: "OK", results: [] }));
  const results = await t.send({ id: "s", businessId: 1 }, [
    { offerId: "GOOD", name: "Good", marketCategoryId: 15927546 },
    { offerId: "BAD", name: "Bad", marketCategoryId: 15927546 },
  ]);
  assert.equal(t.calls.length, 2);
  assert.deepEqual(arr(t.calls[1].offerMappings.map((item) => item.offer.offerId)), ["GOOD"]);
  assert.equal(results.find((item) => item.offerId === "GOOD").ok, true);
  assert.equal(results.find((item) => item.offerId === "BAD").ok, false);
  assert.match(results.find((item) => item.offerId === "BAD").error, /ОКПД/);
});

test("send: existing card — only category fixed, barcodes and price untouched; new card without category skipped", async () => {
  const t = loadSender(() => ({ status: "OK", results: [] }));
  const results = await t.send({ id: "s", businessId: 1 }, [
    { offerId: "OLD", name: "Old", marketCategoryId: 15927546, barcodes: ["8427395940209"], basicPrice: { value: 100, currencyId: "RUR" } },
    { offerId: "NEW", name: "New" },
  ]);
  assert.equal(t.calls.length, 1);
  assert.deepEqual(arr(t.calls[0].offerMappings[0].offer), { offerId: "OLD", marketCategoryId: 15927546 });
  assert.equal(results.find((item) => item.offerId === "NEW").error, "category_manual_review");
});

test("category: wrong Ozon types, refills and '+ product' sets go to manual review", () => {
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "Memo Irish Leather Гель для очищения кожи рук 50 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93405, name: "JACQUES BOGART туалетная вода 90 мл + бальзам после бритья 3 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 92718, name: "Nishane Mexican Woods Заправка для диффузора 200 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "BDK Rouge Smoking Hair Perfume Мист для волос 50 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "Kilian Angels Share Парфюмерная вода 50 мл" }).categoryId, 15927546);
});

test("existing card is fixed only when Ozon and Market describe the same product", () => {
  const ctx = vm.createContext({});
  vm.runInContext(`${part("02a-ozon-yandex-offer-rules.js")}
this.match = yandexCardMatchesOzonProduct;`, ctx);
  assert.equal(ctx.match("KEUNE SEMI COLOR Краска для волос # 5.35, 60мл", "Majda Bekkali Fusion Sacree Clair Духи женские 2 мл", 15927546), false);
  assert.equal(ctx.match("Lattafa Niche Emarati Zikra 100 мл парфюмерная вода", "Lattafa Shampoo Zikra 250 мл шампунь", 91183), false);
  assert.equal(ctx.match("Дубль54", "Stella McCartney Lily 75 мл", 15927546), false);
  assert.equal(ctx.match("AGENT PROVOCATEUR - MAITRESSE 25 мл", "Agent Provocateur Maitresse Парфюмерная вода 25 мл", 15927546), true);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "TOM FORD LOST CHERRY Спрей для тела 150 мл" }).categoryId, null);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93873, name: "SWISS PERFECTION Антицеллюлитная маска для тела 500 мл" }).categoryId, null);
});

test("Cyrillic name rules actually match (no \b / \w next to Cyrillic)", () => {
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 0, name: "Lattafa Zikra 100 мл парфюмерная вода" }).categoryId, 15927546);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 0, name: "Guerlain Shalimar духи 30 мл" }).categoryId, 15927546);
  assert.equal(r.resolveYandexCategoryForOzonProduct({ typeId: 93403, name: "Rudross SAFARI Парфюмерный мист 30 мл" }).categoryId, null);
});
