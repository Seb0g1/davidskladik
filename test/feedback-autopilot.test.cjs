"use strict";

// isOriginalityOnlyQuestion (server/parts/02f-feedback-autopilot.js): only a pure «is it original?» question is
// answered automatically; anything else gets a data-backed draft that a person checks.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02f-feedback-autopilot.js"), "utf8");
const start = source.indexOf("// ── what the question is about");
const end = source.indexOf("// ── marketplace I/O");
const ctx = vm.createContext({ cleanText: (v) => String(v ?? "").trim() });
vm.runInContext(`${source.slice(start, end)}\nthis.f = isOriginalityOnlyQuestion;`, ctx);
const only = ctx.f;

test("pure originality questions are answered automatically", () => {
  for (const q of ["Оригинал?", "Это оригинал", "Здравствуйте, это не подделка?", "Подскажите, товар оригинальный?", "Духи настоящие или копия?"]) {
    assert.equal(only(q), true, q);
  }
});

test("anything else needs data and a check", () => {
  for (const q of [
    "Dior Addict Eau Sensuelle Женская туалетная вода 50ml Подскажите страну изготовления, пожалуйста.",
    "Это оригинал? Есть ли честный знак?",
    "Оригинал? Какой срок годности?",
    "Подходит ли для окрашенных волос?",
  ]) {
    assert.equal(only(q), false, q);
  }
});

const ctx2 = vm.createContext({ cleanText: (v) => String(v ?? "").trim() });
const s2 = source.indexOf("// «можно ли проверить");
vm.runInContext(`${source.slice(s2, source.indexOf("function pickupCheckAnswer"))}\nthis.f = isPickupCheckQuestion;`, ctx2);

test("checking at the pickup point gets the owner's answer", () => {
  for (const q of ["Здравствуйте! Можно ли проверить в пункте выдачи как пахнет аромат не распыляя? Спасибо", "Можно понюхать на пункте выдачи?", "Можно открыть при получении?", "В ПВЗ можно проверить запах?"]) assert.equal(ctx2.f(q), true, q);
  for (const q of ["Какой срок годности?", "Крышка флакона на магните или нет?", "Оригинал?"]) assert.equal(ctx2.f(q), false, q);
});
