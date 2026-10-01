"use strict";

// Parsers of fragrantica.ru pages (server/parts/02a-fragrantica-catalog-parse.js), run against
// gzipped real pages in test/fixtures/fragrantica.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const zlib = require("zlib");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-fragrantica-catalog-parse.js"), "utf8");
const ctx = vm.createContext({});
vm.runInContext(`${source}
this.api = { parseFragranticaDesignersIndex, parseFragranticaBrandPage, parseFragranticaPerfumePage, fragPerfumeIdFromUrl, fragGenderFromText, fragranticaNoteIconLarge };`, ctx);
const frag = ctx.api;
const fixture = (name) => zlib.gunzipSync(fs.readFileSync(path.join(__dirname, "fixtures", "fragrantica", name))).toString("utf8");
const plain = (value) => JSON.parse(JSON.stringify(value));

test("designers index: brands with slug/url/name and the page list", () => {
  const { brands, pages } = frag.parseFragranticaDesignersIndex(fixture("designers-1.html.gz"));
  assert.ok(brands.length > 800, `brands: ${brands.length}`);
  const ambra = brands.find((b) => b.slug === "Al-Ambra");
  assert.equal(ambra.name, "Al Ambra");
  assert.equal(ambra.url, "https://www.fragrantica.ru/designers/Al-Ambra.html");
  assert.ok(pages.includes(1) && pages.includes(11));
});

test("brand page: every perfume with id, name, gender and year", () => {
  const perfumes = frag.parseFragranticaBrandPage(fixture("brand-dior.html.gz"), { brandSlug: "Dior" });
  assert.ok(perfumes.length > 300, `perfumes: ${perfumes.length}`);
  const bonne = perfumes.find((p) => p.id === 87597);
  assert.deepEqual(plain(bonne), {
    id: 87597,
    url: "https://www.fragrantica.ru/perfume/Dior/Bonne-Etoile-Baby-Dior-87597.html",
    brandSlug: "Dior",
    brand: "Dior",
    name: "Bonne Étoile Baby Dior",
    gender: "unisex",
    year: 2023,
  });
  assert.equal(perfumes.find((p) => p.id === 1383).gender, "female");
  assert.ok(perfumes.every((p) => p.brandSlug === "Dior"));
});

test("perfume page: Sauvage — notes pyramid with icons, accords, description without other-language tail", () => {
  const p = frag.parseFragranticaPerfumePage(fixture("perfume-sauvage.html.gz"));
  assert.equal(p.id, 31861);
  assert.equal(p.name, "Sauvage");
  assert.equal(p.brand, "Dior");
  assert.equal(p.gender, "male");
  assert.equal(p.year, 2015);
  assert.equal(p.family, "фужерные");
  assert.deepEqual(plain(p.perfumers), ["François Demachy"]);
  assert.deepEqual(plain(p.notes.top.map((n) => n.name)), ["Калабрийский бергамот", "Перец"]);
  assert.equal(p.notes.top[0].icon, "https://fimgs.net/mdimg/sastojci/t.75.jpg");
  assert.ok(p.notes.middle.length >= 5);
  assert.ok(p.notes.base.some((n) => n.name === "Кедр"));
  assert.ok(p.accords.length >= 5);
  assert.equal(p.accords[0].share, 100);
  assert.ok(p.accords.some((a) => a.name === "амбровый"));
  assert.match(p.description, /^Sauvage Dior — это аромат для мужчин/);
  assert.doesNotMatch(p.description, /других языках|Deutsch/);
  assert.equal(p.image, "https://fimgs.net/mdimg/perfume-thumbs/375x500.31861.2x.jpg");
  assert.ok(p.votes > 30000);
});

test("perfume page: pyramid without top notes keeps middle/base", () => {
  const p = frag.parseFragranticaPerfumePage(fixture("perfume-black-aoud.html.gz"));
  // The id is global on Fragrantica: /perfume/Montale/Black-Aoud-1408.html serves Marc Jacobs Gardenia.
  assert.equal(p.id, 1408);
  assert.equal(p.brand, "Marc Jacobs");
  assert.equal(p.name, "Marc Jacobs Gardenia");
  assert.equal(p.gender, "female");
  assert.equal(p.notes.top.length, 0);
  assert.ok(p.notes.middle.length > 0 && p.notes.base.length > 0);
});

test("helpers", () => {
  assert.equal(frag.fragPerfumeIdFromUrl("https://www.fragrantica.ru/perfume/Dior/Sauvage-31861.html"), 31861);
  assert.equal(frag.fragPerfumeIdFromUrl("https://www.fragrantica.ru/perfume/Dior/Sauvage-31861.html?x=1"), 31861);
  assert.equal(frag.fragGenderFromText("для мужчин и женщин"), "unisex");
  assert.equal(frag.fragranticaNoteIconLarge("https://fimgs.net/mdimg/sastojci/t.75.jpg"), "https://fimgs.net/mdimg/sastojci/o.75.jpg");
});
