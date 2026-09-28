"use strict";

// Testers and decants never get price or stock on Yandex Market
// (isTesterOrDecantSupplierRowName in server/parts/02a-price-master-match-helpers.js).

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const part = (name) => fs.readFileSync(path.join(__dirname, "..", "server", "parts", name), "utf8");
const ctx = vm.createContext({});
vm.runInContext(`${part("02a-price-master-match-helpers.js")}
this.isTester = isTesterOrDecantSupplierRowName;`, ctx);

test("supplier rows that are testers or decants", () => {
  for (const name of [
    "CREED CENTAURUS 100 ml EDP TESTER",
    "Bond Nо.9 So New York edp 100ml tester",
    "INITIO LIFT ME UP EXTRAIT 90 ML TEST",
    "Bvlgari LE GEMME KOBRAA (муж) edp 30ml (тестер)",
    "Issey Miyake L Eau d Issey pour Homme Vetiver Intense туалетная вода тестер 100 мл. муж",
    "Parle Moi de Parfum Papyrus Oud / 71 tester edp100ml",
    "Guerlain Les Extraits Tonka Sarrapia Extrait 75 50ml tester с русификатором",
    "Tom Ford Oud Wood отливант 10 мл",
    "Baccarat Rouge 540 распив 5ml",
    "Xerjoff Naxos decant 10ml",
  ]) assert.equal(ctx.isTester(name), true, name);
});

test("boxed products that only mention a tester or a sample", () => {
  for (const name of [
    "VOSKANIAN PARFUMS Histoire d'une rose 50 мл марка (есть тестер послушать)",
    "What We Do Is Secret Messy Sexy Just Rolled Out Of Bed edp 50ml (ПРОБНИК В ПОДАРОК)",
    "Hormone Paris Testosterone Духи унисекс 100ml",
    "Dior Fahrenheit (M) 200ml edt",
    "Chanel Chance edp 100ml",
    "",
  ]) assert.equal(ctx.isTester(name), false, name);
});
