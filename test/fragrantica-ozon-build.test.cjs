"use strict";

// Building an Ozon card from a Fragrantica perfume (server/parts/02a-fragrantica-ozon-build.js).
// Dictionaries are real Ozon values for «Красота и гигиена / Парфюмерия» (2026-10-01).

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-fragrantica-ozon-build.js"), "utf8");
const ctx = vm.createContext({});
vm.runInContext(`${source}
this.api = { FRAG_OZON_ATTR, buildFragranticaOzonName, buildFragranticaOfferId, fragranticaUniqueOfferId, fragOzonDimensions,
  buildFragranticaCompositionText, matchFragranticaClassification, fragGenderValues, buildFragranticaOzonPrefill,
  buildFragranticaOzonItem, fragranticaNextOzonLimitReset, isOzonLimitErrorText, fragOzonGuessTypeKey, buildFragranticaHashtags };`, ctx);
const b = ctx.api;
const plain = (value) => JSON.parse(JSON.stringify(value));

const CLASSIFICATION = [
  [42349, "Альдегидный"], [42350, "Амбровый"], [42354, "Восточный"], [42358, "Древесный"], [42359, "Зеленый"],
  [42360, "Кожаный"], [42362, "Мускусный"], [42368, "Пудровый"], [42374, "Фруктовый"], [42375, "Фужерный"],
  [42378, "Цветочный"], [42379, "Цитрусовый"], [42380, "Шипровый"], [42381, "Акватический"], [42383, "Гурманский"],
  [970877508, "Алкогольный"], [970877509, "Анималистический"], [970877510, "Бальзамический"], [970877527, "Ягодный"],
].map(([id, value]) => ({ id, value }));
const GENDER = [{ id: 22880, value: "Мужской" }, { id: 22881, value: "Женский" }, { id: 22882, value: "Девочки" }];

const sauvage = {
  id: 31861,
  brand: "Dior",
  name: "Sauvage",
  gender: "male",
  family: "фужерные",
  description: "Sauvage Dior — это аромат для мужчин.",
  notes: {
    top: [{ name: "Калабрийский бергамот" }, { name: "Перец" }],
    middle: [{ name: "Лаванда" }],
    base: [{ name: "Ambroxan" }, { name: "Кедр" }],
    flat: [],
  },
  accords: [
    { name: "свежий пряный", share: 100 },
    { name: "амбровый", share: 71 },
    { name: "цитрусовый", share: 69 },
    { name: "ароматический", share: 60 },
  ],
};

test("name: brand + name + type + volume; brand is not repeated; tester", () => {
  assert.equal(b.buildFragranticaOzonName({ perfume: sauvage, typeKey: "edp", volume: 100 }), "Dior Sauvage Парфюмерная вода мужская 100 мл");
  assert.equal(b.buildFragranticaOzonName({ perfume: sauvage, typeKey: "edt", volume: "7,5", tester: true }), "Dior Sauvage Туалетная вода мужская тестер 7.5 мл");
  assert.equal(
    b.buildFragranticaOzonName({ perfume: { brand: "Marc Jacobs", name: "Marc Jacobs Gardenia" }, typeKey: "edp", volume: 50 }),
    "Marc Jacobs Gardenia Парфюмерная вода 50 мл",
  );
});

test("offer id: FR<id>-<volume>[T], free suffix when taken", () => {
  assert.equal(b.buildFragranticaOfferId({ perfumeId: 31861, volume: 100 }), "FR31861-100");
  assert.equal(b.buildFragranticaOfferId({ perfumeId: 31861, volume: 7.5, tester: true }), "FR31861-7_5T");
  assert.equal(b.fragranticaUniqueOfferId("FR31861-100", new Set(["FR31861-100", "FR31861-100-2"])), "FR31861-100-3");
  assert.equal(b.fragranticaUniqueOfferId("FR1-50", new Set()), "FR1-50");
});

test("dimensions follow the existing perfume cards", () => {
  assert.deepEqual(plain(b.fragOzonDimensions(60)), { depth: 190, width: 120, height: 110, weight: 350 });
  assert.deepEqual(plain(b.fragOzonDimensions(7)), { depth: 150, width: 125, height: 60, weight: 50 });
  assert.equal(b.fragOzonDimensions(100).weight, 450);
});

test("composition text has pyramid lines like the existing cards", () => {
  assert.equal(
    b.buildFragranticaCompositionText(sauvage.notes),
    "Верхние ноты: Калабрийский бергамот, Перец\nНоты сердца: Лаванда\nБазовые ноты: Ambroxan, Кедр",
  );
  assert.equal(b.buildFragranticaCompositionText({ flat: [{ name: "Iso E Super" }] }), "Ноты: Iso E Super");
});

test("classification: family first, then accords by share, max 3, synonyms for missing words", () => {
  const picked = b.matchFragranticaClassification(sauvage, CLASSIFICATION).map((v) => v.value);
  assert.deepEqual(plain(picked), ["Фужерный", "Восточный", "Амбровый"]);
  const fresh = b.matchFragranticaClassification({ accords: [{ name: "водный", share: 90 }, { name: "белые цветы", share: 80 }, { name: "сладкий", share: 70 }] }, CLASSIFICATION);
  assert.deepEqual(plain(fresh.map((v) => v.value)), ["Акватический", "Цветочный", "Гурманский"]);
  assert.deepEqual(plain(b.matchFragranticaClassification({ accords: [{ name: "землистый", share: 50 }] }, CLASSIFICATION)), []);
});

test("gender: unisex gets both values", () => {
  assert.deepEqual(plain(b.fragGenderValues("male", GENDER)), [{ dictionary_value_id: 22880, value: "Мужской" }]);
  assert.deepEqual(plain(b.fragGenderValues("unisex", GENDER).map((v) => v.dictionary_value_id)), [22880, 22881]);
});

test("prefill + item: required attributes are reported missing until filled", () => {
  const prefill = b.buildFragranticaOzonPrefill({
    perfume: sauvage,
    typeKey: "edp",
    volume: 100,
    offerId: "FR31861-100",
    lookups: {
      brand: { id: 115862484, value: "Dior" },
      type: { id: 93403, value: "Вода парфюмерная" },
      gender: b.fragGenderValues("male", GENDER),
      tnved: { id: 971397736, value: "3303001000 - Духи." },
    },
  });
  const byId = Object.fromEntries(prefill.attributes.map((a) => [a.id, a.values]));
  assert.equal(byId[85][0].dictionary_value_id, 115862484);
  assert.equal(byId[9048][0].value, "Sauvage");
  assert.equal(byId[8163][0].value, "100");
  assert.equal(byId[23536][0].value, "false");
  assert.equal(byId[9024][0].value, "FR31861-100");
  assert.equal(prefill.name, "Dior Sauvage Парфюмерная вода мужская 100 мл");

  const required = [85, 8229, 9048, 8205, 23536, 22232, 9163, 4389].map((id) => ({ id, name: `attr${id}`, is_required: true }));
  const { item, missing } = b.buildFragranticaOzonItem({
    typeId: 93403,
    offerId: prefill.offerId,
    name: prefill.name,
    price: "12990",
    oldPrice: "10000",
    ...prefill.dims,
    images: ["https://davidsklad.ru/uploads/fragrantica/cards/31861-main.jpg", "https://davidsklad.ru/uploads/fragrantica/cards/31861-notes.jpg"],
    attributes: prefill.attributes,
  }, required);
  assert.deepEqual(plain(missing), ["attr4389"]);
  assert.equal(item.description_category_id, 17028988);
  assert.equal(item.type_id, 93403);
  assert.equal(item.price, "12990");
  assert.equal(item.old_price, undefined, "old price below price is dropped");
  assert.equal(item.vat, "0.05");
  assert.equal(item.primary_image, "https://davidsklad.ru/uploads/fragrantica/cards/31861-main.jpg");
  assert.deepEqual(plain(item.images), ["https://davidsklad.ru/uploads/fragrantica/cards/31861-notes.jpg"]);

  const empty = b.buildFragranticaOzonItem({ typeId: 93403 }, []);
  assert.deepEqual(plain(empty.missing), ["Артикул", "Название", "Цена", "Фото", "Длина", "Ширина", "Высота", "Вес"]);
});

test("limit reset: Ozon reset_at, else next 00:05 UTC (03:05 MSK)", () => {
  const now = new Date("2026-10-01T15:00:00Z");
  assert.equal(b.fragranticaNextOzonLimitReset(now, "2026-10-02T00:00:00Z").toISOString(), "2026-10-02T00:05:00.000Z");
  assert.equal(b.fragranticaNextOzonLimitReset(now).toISOString(), "2026-10-02T00:05:00.000Z");
  assert.equal(b.isOzonLimitErrorText("daily create limit exceeded"), true);
  assert.equal(b.isOzonLimitErrorText("Превышен лимит на создание товаров"), true);
  assert.equal(b.isOzonLimitErrorText("invalid attribute"), false);
  assert.equal(b.isOzonLimitErrorText("Не получится загрузить товары: вы исчерпали суточный лимит на обновление товаров."), true);
});

test("type guess and hashtags", () => {
  assert.equal(b.fragOzonGuessTypeKey({ name: "Sauvage Eau de Toilette" }), "edt");
  assert.equal(b.fragOzonGuessTypeKey({ name: "Sauvage" }), "edp");
  assert.equal(b.buildFragranticaHashtags({ perfume: { brand: "Maison Margiela" }, typeKey: "edp" }), "#парфюмерная_вода #оригинальная_парфюмерия #maison_margiela");
});

test("supplier row assessment: clones, non-perfume, concentration, flankers (real PriceMaster names)", () => {
  vm.runInContext("this.assess = assessFragranticaSupplierRow;", ctx);
  const a = (row, opts) => plain(ctx.assess(row, { brand: "Dior", name: "Sauvage", typeKey: "edp", ...opts }));
  // clones: the original only in brackets
  assert.equal(a("AlHambra Salvo Intense (Christian Dior Sauvage edP) edP 100 ml").clone, true);
  assert.equal(a("AREEJ DIORIT SOVGEE 100 ml M ( Sauvage Dior )").clone, true);
  assert.equal(a("French Avenue Zenith Blue (Sauvage Dior) edp 100 ml").clone, true);
  assert.equal(a("VITALITY 7.33 EDP 100ML (Coco Mademoiselle)", { brand: "Chanel", name: "Coco Mademoiselle" }).clone, true);
  // the real thing
  const real = a("C.Dior Sauvage men  60ml edt", { typeKey: "edt" });
  assert.deepEqual([real.clone, real.notPerfume, real.concentrationOk, real.extraWords.length], [false, false, true, 0]);
  assert.equal(a("DIOR SAUVAGE (M) EDT 60 ML", { typeKey: "edt" }).extraWords.length, 0);
  assert.equal(a("Christian Dior Sauvage туалетная вода 60мл, шт", { typeKey: "edt" }).extraWords.length, 0);
  // a former brand name is not an extra word («Thierry Mugler» = Mugler), a flanker still is
  assert.equal(a("Thierry Mugler Alien eau EXTRAORDINAIRE W edT 6ml", { brand: "Mugler", name: "Alien Eau Extraordinaire", typeKey: "edt" }).extraWords.length, 0);
  assert.deepEqual(a("THIERRY MUGLER A MEN STELLAR LUMINEUSE edp (m) 100ml", { brand: "Mugler", name: "A*Men" }).extraWords, ["stellar", "lumineuse"]);
  // non-perfume
  assert.equal(a("Dior Sauvage M A/Sh Lotion 100ml New 2015").notPerfume, true);
  assert.equal(a("Dior Sauvage After Shave Balm 100 ml").notPerfume, true);
  assert.equal(a("Chanel  COCO MADEMOISELLE  women 100ml DEO", { brand: "Chanel", name: "Coco Mademoiselle" }).notPerfume, true);
  assert.equal(a("CHANEL COCO WOMAN 100ML VOLIE PARFUME BODY MIST", { brand: "Chanel", name: "Coco Mademoiselle" }).notPerfume, true);
  // concentration: EDT row for an EDP card
  const edt = a("DIOR SAUVAGE (M) EDT 60 ML");
  assert.equal(edt.concentration, "edt");
  assert.equal(edt.concentrationOk, false);
  // flanker: extra meaningful word
  assert.deepEqual(a("Dior Sauvage Elixir 60ml").extraWords, ["elixir"]);
  // brand spelled differently / abbreviated is not a clone
  assert.equal(a("MAISON MARGIELA REPLICA JAZZ CLUB edt 100ml", { brand: "Maison Martin Margiela", name: "Replica Jazz Club" }).clone, false);
  assert.equal(a("YSL Libre edp 90ml", { brand: "Yves Saint Laurent", name: "Libre" }).clone, false);
  assert.equal(a("D&G Light Blue edt 100ml", { brand: "Dolce&Gabbana", name: "Light Blue" }).clone, false);
  // Eau Sauvage is another perfume; «eau de parfum» is just the concentration
  assert.deepEqual(a("Dior EAU SAUVAGE parfum 100 ml m").extraWords, ["eau"]);
  assert.deepEqual(a("Dior Sauvage Eau de Parfum 100 ml").extraWords, []);
  // Chanel Coco is not Coco Mademoiselle
  const coco = (row) => plain(ctx.assess(row, { brand: "Chanel", name: "Coco Mademoiselle", typeKey: "edp" }));
  assert.deepEqual(coco("Chanel  COCO 100ml edP").missingNameWords, ["mademoiselle"]);
  assert.deepEqual(coco("CHANEL COCO MADEMOISELLE (W) EDP 100 ML").missingNameWords, []);
  assert.deepEqual(coco("Chanel - Coco Mademoiselle Парфюмерная вода 100 мл").missingNameWords, []);
});

test("PriceMaster availability: real rows match the model, clones/flankers/testers/lotions do not", () => {
  vm.runInContext("this.pm = { buildFragranticaPmIndex, matchFragranticaPmIndex };", ctx);
  const rows = [
    { name: "C.Dior Sauvage men  60ml edt", usd: 79 },
    { name: "DIOR SAUVAGE (M) EDP 100 ML", usd: 114.1 },
    { name: "Dior Sauvage Elixir 60ml", usd: 140 },
    { name: "AREEJ DIORIT SOVGEE 100 ml M ( Sauvage Dior )", usd: 25 },
    { name: "Dior Sauvage M A/Sh Lotion 100ml New 2015", usd: 65 },
    { name: "Dior Sauvage edp 100ml TESTER", usd: 90 },
    { name: "Dior EAU SAUVAGE parfum 100 ml m", usd: 119.9 },
    { name: "Chanel Coco Mademoiselle w 100ml edp", usd: 167 },
    { name: "Chanel  COCO 100ml edP", usd: 145 },
    { name: "Dior Sauvage edp 1ml", usd: 1.7 },
  ];
  const index = ctx.pm.buildFragranticaPmIndex(rows);
  const { matched: _m1, ...sauvage } = plain(ctx.pm.matchFragranticaPmIndex(index, { brand: "Dior", name: "Sauvage" }));
  assert.deepEqual(sauvage, { count: 2, minUsd: 79, volumes: [60, 100] });
  const { matched: _m2, ...eau } = plain(ctx.pm.matchFragranticaPmIndex(index, { brand: "Dior", name: "Eau Sauvage" }));
  assert.equal(eau.count, 1);
  const { matched: _m3, ...coco } = plain(ctx.pm.matchFragranticaPmIndex(index, { brand: "Chanel", name: "Coco Mademoiselle" }));
  assert.deepEqual(coco, { count: 1, minUsd: 167, volumes: [100] });
  assert.equal(ctx.pm.matchFragranticaPmIndex(index, { brand: "Creed", name: "Aventus" }).count, 0);
});

test("full card: VAT per cabinet, country, producer, weights, packaging, units, shelf life 900", () => {
  vm.runInContext("this.x = { fragranticaVatForClientId, fragranticaCountryRu, fragOzonNetWeight, buildFragranticaYandexParameters };", ctx);
  assert.equal(ctx.x.fragranticaVatForClientId("1304220"), "0.05");
  assert.equal(ctx.x.fragranticaVatForClientId("2533393"), "0");
  assert.equal(ctx.x.fragranticaVatForClientId("2533393", { "2533393": "0.05" }), "0.05");
  assert.equal(ctx.x.fragranticaCountryRu("France"), "Франция");
  assert.equal(ctx.x.fragranticaCountryRu("United-Arab-Emirates"), "ОАЭ");
  const prefill = b.buildFragranticaOzonPrefill({
    perfume: { brand: "Guerlain", name: "L'Heure Bleue" }, typeKey: "edp", volume: 50, offerId: "FR1-50",
    lookups: { country: { id: 90304, value: "Франция" } },
  });
  const byId = Object.fromEntries(prefill.attributes.map((a) => [a.id, a.values]));
  assert.equal(byId[4389][0].dictionary_value_id, 90304);
  assert.equal(byId[23487][0].value, "Guerlain");
  assert.equal(byId[4383][0].value, "200");
  assert.equal(byId[8044][0].value, "200");
  assert.equal(byId[4386][0].dictionary_value_id, 85921);
  assert.equal(byId[8962][0].value, "1");
  assert.equal(byId[11650][0].value, "1");
  assert.equal(byId[22270][0].dictionary_value_id, 971417785);
  assert.equal(byId[8205][0].value, "900");
  const tester = b.buildFragranticaOzonPrefill({ perfume: { brand: "A", name: "B" }, typeKey: "edp", volume: 100, tester: true, offerId: "FR1-100T" });
  assert.equal(Object.fromEntries(tester.attributes.map((a) => [a.id, a.values]))[4386][0].dictionary_value_id, 115933094);
});

test("Market parameters from the real «Парфюмерия» category (15927546)", () => {
  const category = [
    { id: 21194330, name: "Тип", values: [{ id: 22843950, value: "парфюмерная вода" }, { id: 22844210, value: "туалетная вода" }] },
    { id: 14805991, name: "Пол", values: [{ id: 14805993, value: "женский" }, { id: 14805992, value: "мужской" }, { id: 14805994, value: "унисекс" }] },
    { id: 37901030, name: "Семейство", values: [{ id: 38324286, value: "восточные" }, { id: 38324293, value: "восточные цветочные" }, { id: 38324264, value: "древесные" }, { id: 38324299, value: "восточные фужерные" }, { id: 1, value: "фужерные" }, { id: 2, value: "цветочные" }] },
    { id: 27144551, name: "Год", values: [{ id: 5, value: "2015" }] },
    { id: 15927566, name: "Верхние ноты" }, { id: 15927560, name: "Средние ноты" }, { id: 15927641, name: "Базовые ноты" },
    { id: 23674510, name: "Вес" }, { id: 33663230, name: "Количество упаковок в товаре" }, { id: 14805583, name: "Единиц в одной упаковке" },
    { id: 53763303, name: "Тестер" },
  ];
  const params = plain(ctx.x.buildFragranticaYandexParameters(category, {
    typeLabel: "Парфюмерная вода", gender: "мужской", family: "фужерные", accords: ["древесный", "амбровый"], year: 2015,
    topNotes: ["Бергамот", "Перец"], middleNotes: ["Лаванда"], baseNotes: ["Кедр"], netWeight: 300, tester: false,
  }));
  const byId = Object.fromEntries(params.map((p) => [p.parameterId, p]));
  assert.equal(byId[21194330].valueId, 22843950);
  assert.equal(byId[14805991].valueId, 14805992);
  assert.deepEqual(params.filter((p) => p.parameterId === 37901030).map((p) => p.value), ["фужерные", "древесные"]);
  assert.equal(byId[27144551].valueId, 5);
  assert.equal(byId[15927566].value, "Бергамот, Перец");
  assert.equal(byId[23674510].value, "300");
  assert.equal(byId[33663230].value, "1");
  assert.equal(byId[14805583].value, "1");
  assert.equal(byId[53763303].value, "false");
  // nothing known → nothing invented
  const empty = plain(ctx.x.buildFragranticaYandexParameters(category, {}));
  assert.deepEqual(empty.map((p) => p.parameterId), [33663230, 14805583, 53763303]);
});

test("dimension templates by volume: exact, next bigger, biggest; built-in table when empty", () => {
  vm.runInContext("this.d = { fragOzonDimensions, normalizeFragranticaDimsTemplates };", ctx);
  const rows = [
    { volume: 100, depth: 160, width: 130, height: 120, weight: 450 },
    { volume: "50", depth: "140", width: 120, height: 110, weight: 300 },
    { volume: 10, depth: 0, width: 1, height: 1, weight: 1 },
  ];
  assert.equal(ctx.d.normalizeFragranticaDimsTemplates(rows).length, 2);
  assert.deepEqual(plain(ctx.d.fragOzonDimensions(50, rows)), { depth: 140, width: 120, height: 110, weight: 300 });
  assert.deepEqual(plain(ctx.d.fragOzonDimensions(30, rows)), { depth: 140, width: 120, height: 110, weight: 300 });
  assert.deepEqual(plain(ctx.d.fragOzonDimensions(75, rows)), { depth: 160, width: 130, height: 120, weight: 450 });
  assert.deepEqual(plain(ctx.d.fragOzonDimensions(200, rows)), { depth: 160, width: 130, height: 120, weight: 450 });
  assert.equal(ctx.d.fragOzonDimensions(60, []).weight, 350);
});

test("OKPD2 by type and GTIN check digit", () => {
  vm.runInContext("this.g = { fragOkpd2ForType, fragValidGtin, buildFragranticaOzonItem };", ctx);
  assert.equal(ctx.g.fragOkpd2ForType("edp"), "20.42.11.120");
  assert.equal(ctx.g.fragOkpd2ForType("parfum"), "20.42.11.110");
  assert.equal(ctx.g.fragOkpd2ForType("cologne"), "20.42.11.130");
  assert.equal(ctx.g.fragValidGtin("3348901250146"), "3348901250146");
  assert.equal(ctx.g.fragValidGtin("3348901250147"), "");
  assert.equal(ctx.g.fragValidGtin("OZN123"), "");
  assert.equal(ctx.g.buildFragranticaOzonItem({ typeId: 93403, barcode: "3348901250146" }, []).item.barcode, "3348901250146");
  assert.equal(ctx.g.buildFragranticaOzonItem({ typeId: 93403, barcode: "123" }, []).item.barcode, undefined);
});

vm.runInContext("this.conv = { planFragranticaVolumes, fragranticaVolumeInShop, buildFragranticaDraftExportBody, fragranticaDraftMissing };", ctx);

test("conveyor: volumes from PriceMaster rows — type by rows, no testers/samples/sets/flankers/clones", () => {
  const rows = [
    { name: "C.Dior Sauvage men 60ml edt" },
    { name: "DIOR SAUVAGE (M) EDT 100 ML" },
    { name: "Dior Sauvage edt 200ml" },
    { name: "Dior Sauvage edp 100ml" },
    { name: "Dior Sauvage edt 100ml tester" },
    { name: "Dior Sauvage edt 1ml пробник" },
    { name: "Dior Sauvage set edt 100ml + gel 50ml" },
    { name: "Dior Sauvage Elixir 60ml" },
    { name: "AREEJ DIORIT 100 ml (Sauvage Dior)" },
    { name: "Dior Sauvage after shave lotion 100ml" },
    { name: "Dior Sauvage edt 30ml", available: false },
  ];
  const plan = plain(ctx.conv.planFragranticaVolumes(rows, { brand: "Dior", name: "Sauvage" }));
  assert.equal(plan.typeKey, "edt");
  assert.deepEqual(plan.volumes.map((v) => v.volume), [60, 100, 200]);
  // the name says EDP → only EDP rows (and rows without a concentration)
  const edp = plain(ctx.conv.planFragranticaVolumes([...rows, { name: "Dior Sauvage Eau de Parfum 60ml" }], { brand: "Dior", name: "Sauvage Eau de Parfum" }));
  assert.equal(edp.typeKey, "edp");
  assert.deepEqual(plain(ctx.conv.planFragranticaVolumes([], { brand: "Dior", name: "Sauvage" })), { typeKey: "edp", volumes: [] });
});

test("conveyor: volume already in a shop — live export or a warehouse card with that volume", () => {
  const exports = [{ accountId: "a", volume: 100, tester: false, status: "imported" }, { accountId: "b", volume: 50, tester: false, status: "failed" }];
  const stock = { c: { n: 2, v: [60, 100] } };
  const inShop = (shopId, volume, tester = false) => ctx.conv.fragranticaVolumeInShop({ exports, stock, shopId, volume, tester });
  assert.equal(inShop("a", 100), true);
  assert.equal(inShop("a", 60), false);
  assert.equal(inShop("b", 50), false); // failed export does not count
  assert.equal(inShop("c", 60), true);
  assert.equal(inShop("c", 60, true), false);
});

test("conveyor: export body from a draft — own fields, notes per shop without approval, selected links only", () => {
  const draft = {
    perfumeId: 31861,
    data: {
      name: "Dior Sauvage Туалетная вода 100 мл", offerId: "FR31861-100", typeKey: "edt", tester: false,
      dims: { depth: 200, width: 130, height: 120, weight: 450 },
      attributes: [{ id: 85, values: [{ dictionary_value_id: 1, value: "Dior" }] }, { id: 4191, values: [{ value: "old" }] }, { id: 8050, values: [] }],
      requiredAttrs: [{ id: 85, name: "Бренд" }, { id: 9163, name: "Пол" }],
      price: 12990, oldPrice: 25980, yandexPrice: 13500,
      images: { main: "/uploads/fragrantica/cards/31861-main-v2.jpg", notes: { magicstick: "/n-ms.jpg", parfumerius: "/n-p.jpg" } },
      description: "Текст", selectedLinks: ["r1"],
      linkRows: [{ id: "r1", rowId: "11", article: "A1", name: "Dior Sauvage edt 100ml", supplierName: "S", partnerId: "7", priceCurrency: "USD" }, { id: "r2", rowId: "12", name: "x" }],
    },
  };
  const targets = [{ key: "ozon:env", style: "magicstick" }, { key: "yandex:y1", style: "parfumerius" }];
  const body = plain(ctx.conv.buildFragranticaDraftExportBody(draft, targets));
  assert.equal(body.typeId, 93405);
  assert.deepEqual(body.targets, [{ key: "ozon:env", notes: "/n-ms.jpg" }, { key: "yandex:y1", notes: "/n-p.jpg" }]);
  assert.equal(body.attributes.filter((a) => a.id === 4191).length, 1);
  assert.equal(body.attributes.find((a) => a.id === 4191).values[0].value, "Текст");
  assert.equal(body.attributes.find((a) => a.id === 8229).values[0].dictionary_value_id, 93405);
  assert.deepEqual(body.links.map((l) => l.rowId), ["11"]);
  assert.equal(body.price, "12990");
  assert.deepEqual(plain(ctx.conv.fragranticaDraftMissing(draft, targets)), ["Пол"]);
  // Market only: the Market price is the card price
  assert.equal(ctx.conv.buildFragranticaDraftExportBody({ ...draft, data: { ...draft.data, price: 0 } }, [targets[1]]).price, "13500");
  assert.deepEqual(plain(ctx.conv.fragranticaDraftMissing(draft, [])).slice(0, 1), ["Магазины"]);
});

test("Ozon Rich-контент: pyramid, text halves, specs, close-up; video cover complex attribute; media error detection", () => {
  vm.runInContext("this.rich = { buildFragranticaRichContent, buildFragranticaVideoCoverComplex, fragranticaMediaExtrasFailed };", ctx);
  const r = ctx.rich;
  const json = r.buildFragranticaRichContent({
    title: "Dior Sauvage",
    description: "Первый абзац.\n\nВторой абзац.\n\nТретий <b>абзац</b>.",
    images: { notes: "https://x/n.jpg", specs: "https://x/s.jpg", closeup: "https://x/c.jpg" },
  });
  const rich = JSON.parse(json);
  assert.equal(rich.version, 0.3);
  assert.deepEqual(rich.content.map((w) => w.widgetName), ["raShowcase", "raTextBlock", "raShowcase", "raTextBlock", "raShowcase"]);
  assert.equal(rich.content[0].blocks[0].img.src, "https://x/n.jpg");
  assert.deepEqual(rich.content[1].text.content, ["Первый абзац.", "Второй абзац."]);
  assert.deepEqual(rich.content[1].title.content, ["Dior Sauvage"]);
  assert.deepEqual(rich.content[3].text.content, ["Третий абзац."]);
  assert.equal(r.buildFragranticaRichContent({ title: "x", description: "", images: {} }), "");
  assert.deepEqual(plain(r.buildFragranticaVideoCoverComplex("https://x/v.mp4")), [{ attributes: [{ id: 21845, complex_id: 100002, values: [{ dictionary_value_id: 0, value: "https://x/v.mp4" }] }] }]);
  assert.deepEqual(plain(r.buildFragranticaVideoCoverComplex("")), []);
  assert.equal(r.fragranticaMediaExtrasFailed([{ attribute_id: 11254, description: "invalid json" }]), true);
  assert.equal(r.fragranticaMediaExtrasFailed([{ description: "Не удалось загрузить видеообложку" }]), true);
  assert.equal(r.fragranticaMediaExtrasFailed([{ attribute_id: 85, description: "Бренд" }]), false);
});

test("conveyor: PriceMaster memory index — every name word, accents ignored, concentration words dropped", () => {
  vm.runInContext("this.idx = { buildFragranticaRowIndex, findFragranticaPmCandidates, planFragranticaVolumes, fragStripConcentration };", ctx);
  const x = ctx.idx;
  const rows = [
    { name: "Dior Sauvage edp 100ml" }, { name: "DIOR SAUVAGE EDP 60 ML" }, { name: "Dior Sauvage edt 100ml" },
    { name: "Dior Homme Intense 100ml" }, { name: "Lancome La Vie Est Belle Eau de Parfum 50ml" },
    { name: "Hermes Terre d'Hermes 100ml" }, { name: "Guerlain Heritage edt 100ml" },
  ];
  const index = x.buildFragranticaRowIndex(rows);
  const names = (perfume) => plain(x.findFragranticaPmCandidates(index, perfume)).map((r) => r.name);
  assert.deepEqual(names({ brand: "Dior", name: "Sauvage Eau de Parfum" }), ["Dior Sauvage edp 100ml", "DIOR SAUVAGE EDP 60 ML", "Dior Sauvage edt 100ml"]);
  assert.deepEqual(names({ brand: "Guerlain", name: "Héritage" }), ["Guerlain Heritage edt 100ml"]);
  assert.deepEqual(names({ brand: "Chanel", name: "Bleu de Chanel" }), []);
  assert.equal(x.fragStripConcentration("Sauvage Eau de Parfum"), "Sauvage");
  // «Eau de Parfum» in the name no longer throws the EDP rows out
  const plan = plain(x.planFragranticaVolumes(x.findFragranticaPmCandidates(index, { brand: "Dior", name: "Sauvage Eau de Parfum" }), { brand: "Dior", name: "Sauvage Eau de Parfum" }));
  assert.equal(plan.typeKey, "edp");
  assert.deepEqual(plan.volumes.map((v) => v.volume), [60, 100]);
});

test("Market: «Линейка» and «Особенности флакона» (custom values), title 60–120 chars with real accords", () => {
  vm.runInContext("this.ym = { buildFragranticaYandexParameters, buildFragranticaMarketName };", ctx);
  const params = [
    { id: 12782797, name: "Линейка", type: "ENUM", allowCustomValues: true, values: [{ id: 1, value: "Sauvage" }] },
    { id: 27141015, name: "Особенности флакона", type: "ENUM", allowCustomValues: true, values: [{ id: 51090168, value: "рефилл" }] },
  ];
  assert.deepEqual(plain(ctx.ym.buildFragranticaYandexParameters(params, { line: "Sauvage", bottleFeature: "с распылителем" })), [
    { parameterId: 12782797, valueId: 1, value: "Sauvage" },
    { parameterId: 27141015, value: "с распылителем" },
  ]);
  assert.deepEqual(plain(ctx.ym.buildFragranticaYandexParameters(params, { line: "Taormina Orange" }))[0], { parameterId: 12782797, value: "Taormina Orange" });
  const versace = ctx.ym.buildFragranticaMarketName({ perfume: { brand: "Versace", name: "Woman", gender: "female", accords: [{ name: "Цветочный" }, { name: "фруктовый" }] }, typeKey: "edp", volume: 50 });
  assert.equal(versace, "Парфюмерная вода Versace Woman Eau de Parfum женская 50 мл, цветочный фруктовый аромат");
  assert.ok(versace.length >= 60 && versace.length <= 120);
  // already long enough — unchanged
  assert.equal(ctx.ym.buildFragranticaMarketName({ perfume: { brand: "Tom Ford", name: "Taormina Orange", gender: "unisex", accords: [{ name: "цитрусовый" }] }, typeKey: "edp", volume: 100 }), "Парфюмерная вода Tom Ford Taormina Orange Eau de Parfum унисекс 100 мл");
});
