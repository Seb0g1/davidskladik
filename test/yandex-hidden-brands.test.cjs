"use strict";

// Brands Market hid «for authenticity doubts» (server/parts/02a-yandex-hidden-brands.js).

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-yandex-hidden-brands.js"), "utf8");
const ctx = vm.createContext({
  require, process: { env: {} }, path, dataDir: "/nonexistent", fs: fs.promises, app: { get() {}, post() {}, delete() {} }, requireAdmin() {},
  cleanText: (v) => String(v ?? "").trim(),
  normalizedBrandIndexKey: (v) => String(v || "").toLowerCase().replace(/[^0-9a-zа-яё]+/gi, " ").replace(/\s+/g, " ").trim(),
});
vm.runInContext(`${source}\nthis.h = { yandexHiddenForAuthenticity, matchYandexHiddenBrand, yandexHiddenBrandKeys };`, ctx);
const { yandexHiddenForAuthenticity, matchYandexHiddenBrand } = ctx.h;

const kilianError = {
  message: "Скрыт сотрудником Маркета",
  comment: "Мы скрыли товары бренда By Kilian, потому что сомневаемся в их подлинности. Чтобы подтвердить оригинальность товаров, отправьте нам документы…",
};

test("the Market error is recognised and the brand read from the comment", () => {
  assert.equal(yandexHiddenForAuthenticity(kilianError).brand, "By Kilian");
  assert.equal(yandexHiddenForAuthenticity({ message: "Скрыт сотрудником Маркета", comment: "Мы скрыли товары бренда VERSACE, потому что сомневаемся в их подлинности." }).brand, "VERSACE");
  assert.equal(yandexHiddenForAuthenticity({ message: "Нет фото" }), null);
});

test("products of a hidden brand: by vendor or by the brand in the title («By Kilian» also as «Kilian»)", () => {
  const brands = { "by kilian": { brand: "By Kilian" } };
  assert.equal(matchYandexHiddenBrand({ name: "Парфюмерная вода By Kilian Angels' Share 50 мл" }, brands), "By Kilian");
  assert.equal(matchYandexHiddenBrand({ name: "KILIAN Vodka on the Rocks 50ml EDP" }, brands), "By Kilian");
  assert.equal(matchYandexHiddenBrand({ brand: "By Kilian", name: "Angels Share 50 мл" }, brands), "By Kilian");
  assert.equal(matchYandexHiddenBrand({ name: "Kilianova Rose 50 мл" }, brands), "");
  assert.equal(matchYandexHiddenBrand({ name: "Dior Sauvage 100 мл" }, brands), "");
});
