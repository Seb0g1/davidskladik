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
  assert.equal(b.buildFragranticaOzonName({ perfume: sauvage, typeKey: "edp", volume: 100 }), "Dior Sauvage Парфюмерная вода 100 мл");
  assert.equal(b.buildFragranticaOzonName({ perfume: sauvage, typeKey: "edt", volume: "7,5", tester: true }), "Dior Sauvage Туалетная вода тестер 7.5 мл");
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
  assert.equal(prefill.name, "Dior Sauvage Парфюмерная вода 100 мл");

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
});
