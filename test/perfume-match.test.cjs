"use strict";

// lib/perfume-match.js — real card titles / PriceMaster rows found while tuning the matcher (2026-10-04).
const test = require("node:test");
const assert = require("node:assert/strict");
const { parsePerfumeName, comparePerfumes, matchCardRows, buildBrandIndex } = require("../lib/perfume-match");

const brands = buildBrandIndex([
  "Dior", "Lancome", "Parfums de Marly", "Stefano Ricci", "Versace", "Patrick Ta", "Yves Saint Laurent", "Chanel", "Dolce & Gabbana",
  "Calvin Klein", "Paco Rabanne", "Hugo Boss", "Guerlain", "Ex Nihilo", "Issey Miyake", "Kilian", "Montale", "Armaf", "Givenchy",
  "Maison Francis Kurkdjian", "Tom Ford", "Juliette Has A Gun", "Goldfield & Banks Australia", "Afnan", "Christian Dior", "Al Haramain Perfumes",
]);
const P = (text) => parsePerfumeName(text, { brands });
const cmp = (card, row) => comparePerfumes(P(card), P(row));

test("parse: concentration, gender, volume, brand, name", () => {
  const p = P("ISSEY MIYAKE L'EAU D' ISSEY SPORT men edt 100 ml");
  assert.deepEqual([p.brandKey, p.volume, p.concentration, p.gender, p.name], ["isseymiyake", 100, "edt", "men", "leau dissey sport"]);
  const c = P("Парфюмерная вода Giorgio Armani My Way Intense женская 90 мл");
  assert.deepEqual([c.concentration, c.gender, c.volume], ["edp", "women", 90]);
  assert.equal(P("Paco Rabanne Invictus m edt100ml").volume, 100);
  assert.equal(P("YSL Manifesto L'Eclat (L) 90 edt").volume, 90);
  assert.equal(P("Ex Nihilo Outcast Blue edp 7.5ml spr").name, "outcast blue");
  assert.equal(P("Clean Classic Духи 30 мл").concentration, "parfum");
  assert.equal(P("Nasomatto Sadonaso 30 ml EXTRAIT").concentration, "parfum");
  assert.equal(P("D&G LIGHT BLUE edt 50ml wom").brandKey, "dolcegabbana");
  assert.equal(P("CK ONE REFLECTIONS 100 ML EDT").brandKey, "calvinklein");
  assert.equal(P("Сalvin Klein eternity aqua 100 мл").brandKey, "calvinklein"); // Cyrillic «С»
  assert.equal(P("Чайный набор йод").name.includes("чайный"), true); // й survives diacritics stripping
});

test("same product: spelling, apostrophes, word order, line / packaging words", () => {
  const same = [
    ["ISSEY MIYAKE L'EAU D'ISSEY Туалетная вода для мужчин 100 мл", "Issey Miyake L EAU D ISSEY men edt 100 ml"],
    ["CHANEL №19 POUDRE Парфюмерная вода женская 100 мл", "Chanel N19 Poudre edp 100ml w"],
    ["YSL Black Opium Over Red Женская парфюмерная вода 90ml edp", "YSL Opium Black Over Red (L) 90ml EDP"],
    ["Christian Dior EDEN-ROC парфюмерная вода унисекс 2мл", "Christian Dior MAISON COLLECTION EDEN-ROC edp 2ml tube"],
    ["GOLDFIELD & BANKS SUNSET HOUR Парфюмерная вода 100 мл", "GOLDFIELD & BANKS AUSTRALIA SUNSET HOUR EDP 100 ml"],
    ["Christian Dior - Dior Homme Sport Туалетная вода 125 мл", "DIOR HOMME SPORT 125 ml EDT"],
    ["Armaf Club De Nuit Man Туалетная вода для мужчин 105ml", "Armaf  Club De Nuit  [ M]  edt   105 ml"],
    ["AL HARAMAIN L'AVENTURE Парфюмерная вода для мужчин 100 мл", "Al Haramain L'aventure men 100ml edp"],
  ];
  for (const [card, row] of same) assert.equal(cmp(card, row).ok, true, `${card} ⇐ ${row}: ${cmp(card, row).reason}`);
});

test("another product: flankers, versions, other names, shades, testers, decants", () => {
  const other = [
    ["LANCOME TRESOR LA NUIT LE PARFUM Парфюмерная вода 30 мл", "Lancome Tresor La Nuit w  30ml edp"],
    ["Parfums de Marly Парфюмерная вода Delina 75 мл", "Parfums de Marly Delina Exclusif 75 parfum"],
    ["STEFANO RICCI Paris 100 мл парфюмерная вода женская", "Stefano Ricci New York 100ml edp марк"],
    ["VERSACE POUR HOMME Туалетная вода для мужчин 100 мл", "VERSACE L HOMME edt (m) 100ml"],
    ["Patrick TA Major Skin Палет 12 гр (тон) + 9 гр (пудра) (fair 4)", "Patrick TA Major Skin Палет 12 гр (тон) + 9 гр (пудра) (Deep 2)"],
    ["Givenchy Gentleman Society Extreme Мужская парфюмерная вода 100ml", "Givenchy Gentleman Society Extreme (M) 100ml edp tester"],
    ["Montale Black Aoud Парфюмерная вода 100 мл", "Montale Black Aoud edp 100ml отливант"],
    ["Montale Black Aoud Парфюмерная вода 100 мл", "Montale Black Aoud edt 100ml"],
    ["Montale Black Aoud Парфюмерная вода 100 мл", "Montale Black Aoud edp 50ml"],
    ["Chanel Coco Mademoiselle парфюмерная вода женская 100 мл", "Chanel Coco Mademoiselle Intense edp 100ml w"],
  ];
  for (const [card, row] of other) assert.equal(cmp(card, row).ok, false, `${card} ⇐ ${row} must not match`);
});

test("uncertain: «probable» with a reason, never «exact»", () => {
  const r = cmp("Dolce & Gabbana Devotion Pour Homme Парфюмерная вода 10 мл", "Dolce & Gabbana Devotion Eau de Parfum 10ml Travel");
  assert.equal(r.ok && r.confidence, "probable");
  assert.equal(cmp("Louis Vuitton Sun Song Вода парфюмерная унисекс 100 ml", "Louis Vuitton SUN SONG edp 100 ml(подмят)").confidence, "probable");
  // card without gender, the price list has men and women versions of the name
  const card = P("Calvin Klein Euphoria Туалетная вода 100 мл");
  const rows = ["CALVIN KLEIN EUPHORIA men 100ml edT", "CALVIN KLEIN EUPHORIA w 100ml edT"].map((n) => ({ row: P(n), n }));
  const out = matchCardRows(card, rows);
  assert.equal(out.length, 2);
  assert.ok(out.every((x) => x.result.confidence === "probable"));
});

test("Extrait and Parfum: one strength, told apart by the extrait flag", () => {
  const m = require("../lib/perfume-match");
  const ex = m.parsePerfumeName("Ex Nihilo Fleur Narcotique Extrait de Parfum 50ml");
  assert.equal(ex.concentration, "parfum");
  assert.equal(ex.extrait, true);
  assert.equal(m.parsePerfumeName("Mancera Red Tobacco Intense Духи 120 мл").extrait, true);
  assert.equal(m.parsePerfumeName("Lengling El Pasajero No 1 exdp 50ml").extrait, true);
  const pf = m.parsePerfumeName("Dior Sauvage Parfum 100ml");
  assert.equal(pf.concentration, "parfum");
  assert.equal(pf.extrait, false);
  assert.equal(m.parsePerfumeName("Dior Sauvage Парфюм мужской 100 мл").concentration, "parfum");
  assert.equal(m.parsePerfumeName("Dior Sauvage Парфюмерная вода 100 мл").concentration, "edp");
});
