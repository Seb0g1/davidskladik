"use strict";
// «Ошибки карточек» (server/parts/02d-card-health.js): Market error texts → issue type and fix
const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const src = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02d-card-health.js"), "utf8");
const rules = src.slice(src.indexOf("const CARD_HEALTH_RULES"), src.indexOf("async function requireCardHealthTables"));
const ctx = vm.createContext({});
vm.runInContext(`function cleanText(v) { return String(v ?? "").trim(); }\n${rules}\nthis.classify = classifyCardIssue;`, ctx);
const c = (text) => ctx.classify(text);

test("Market card errors from the 2026-10-03 export are classified", () => {
  assert.equal(c("Не указаны отличия: Отличительный признак «Объем флакона» не указан").code, "variant_volume");
  assert.equal(c("Дубль варианта: Хотя бы один отличительный признак должен быть уникальным").fix, "resend_card");
  assert.equal(c("Общие признаки не совпадают: Проверьте поле Бренд").code, "variant_brand");
  assert.equal(c("Не заполнено обязательное поле: Укажите код ТН ВЭД").code, "tnved");
  assert.equal(c("Не указаны габариты товара: Укажите длину").code, "dimensions");
  assert.equal(c("Не указан вес товара: Укажите вес").code, "dimensions");
  assert.equal(c("Некорректное название: Название товара не может быть набрано только большими (заглавными, прописными) буквами.").code, "caps_name");
  assert.equal(c("Цена не указана: Передайте цену товара.").fix, "reprice");
  assert.equal(c("Неправильная ставка НДС: Ставка налогообложения").code, "vat");
  assert.equal(c("Цена сильно снизилась: Вы снизили цену в 2,5 раз.").fix, "confirm_quarantine");
  assert.equal(c("Вы не приняли заказ с этим товаром: Считаем, что товар закончился.").fix, "");
  assert.equal(c("Скрыт сотрудником Маркета: Мы скрыли товары бренда VERSACE, потому что сомневаемся в их подлинности.").code, "authenticity");
  assert.equal(c("Штрихкод производителя должен быть с GTIN: Это обязательно для маркируемых товаров").code, "gtin");
  assert.equal(c("Изображение не по правилам: Жестокость или насилие на фото.").code, "media_rules");
  assert.equal(c("Изображение недоступно: Не удаётся скачать изображение").code, "image_unavailable");
  assert.equal(c("Относится к 2 магазинам"), null);
});
