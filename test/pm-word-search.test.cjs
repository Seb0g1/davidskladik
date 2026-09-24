"use strict";

// Unit tests for the PriceMaster word search (server/parts/02a-pm-word-search.js).
// The part file is evaluated standalone in a VM context — it has no runtime deps
// besides readSnapshot(), which these tests don't touch.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-pm-word-search.js"), "utf8");
const ctx = vm.createContext({});
vm.runInContext(`${source}
this.api = {
  pmQueryToTokenGroups, pmPassesSearchFilter, pmWordMatchScore, pmWordTokenize,
  pmBuildMysqlSearchClause, pmBuildPrismaSearchWhere, pmNormalizeSearchText, pmIsFullMatch,
};`, ctx);
const pm = ctx.api;

const passes = (name, query) => pm.pmPassesSearchFilter(name, pm.pmQueryToTokenGroups(query));

test("pm search: basic match", () => {
  assert.equal(passes("Christian Dior Sauvage EDT 100ml", "dior sauvage"), true);
  assert.equal(passes("Christian Dior Fahrenheit EDT 100ml", "dior sauvage"), false);
});

test("pm search: accented letters in names match plain query and vice versa", () => {
  assert.equal(passes("Hermès Terre d'Hermès EDT 100ml", "hermes terre"), true);
  assert.equal(passes("Chloé Nomade EDP 75ml", "chloe nomade"), true);
  assert.equal(passes("Armani Acqua di Giò EDT 100ml", "acqua gio"), true);
  assert.equal(passes("Hermes Terre d'Hermes EDT 100ml", "hermès"), true);
});

test("pm search: apostrophe words match with or without apostrophe/space", () => {
  assert.equal(passes("Dior J'adore EDP 100ml", "dior j'adore"), true);
  assert.equal(passes("Dior Jadore EDP 100ml", "dior j'adore"), true);
  assert.equal(passes("Dior J`Adore EDP 100ml", "dior j'adore"), true);
  assert.equal(passes("Dior J Adore EDP 100ml", "dior j'adore"), true);
  assert.equal(passes("Issey Miyake L'Eau d'Issey EDT 100ml", "l'eau d'issey"), true);
  assert.equal(passes("Issey Miyake Leau Dissey EDT 100ml", "l'eau d'issey"), true);
});

test("pm search: marketplace-style titles with filler words still match", () => {
  // "eau de parfum" / "парфюмерная вода" / "мл" are filler — PM names use EDP / 100ml.
  assert.equal(passes("Dior Sauvage EDP 100ml", "Dior Sauvage Eau de Parfum 100 ml"), true);
  assert.equal(passes("Dior Sauvage EDP 100", "Dior Sauvage 100 мл"), true);
  assert.equal(passes("Dior Sauvage EDP 100ml", "Dior Sauvage парфюмерная вода 100 мл"), true);
});

test("pm search: volume written together or apart matches either spelling", () => {
  assert.equal(passes("Dior Sauvage EDT 100 мл", "sauvage 100ml"), true);
  assert.equal(passes("Dior Sauvage EDT 100ml", "sauvage 100 ml"), true);
  assert.equal(passes("Dior Sauvage EDT 50ml", "sauvage 100ml"), false);
});

test("pm search: numbers stay strict (5 does not match 50)", () => {
  assert.equal(passes("Chanel Chance EDT 50ml", "chanel chance 5 ml"), false);
  assert.equal(passes("Chanel Chance EDT 5ml", "chanel chance 5 ml"), true);
});

test("pm search: compound article codes", () => {
  assert.equal(passes("Tester BOD-13 blue", "bod13"), true);
  assert.equal(passes("Chanel No 5 EDP 100ml", "chanel no5"), true);
  assert.equal(passes("Chanel Noir 5 EDP", "chanel no5"), false);
});

test("pm search: barcode in haystack", () => {
  assert.equal(passes("Dior Sauvage EDT 100ml SVG100 3348901250146", "3348901250146"), true);
});

test("pm search: n-1 tolerance for long queries", () => {
  assert.equal(passes("Dior Sauvage EDT 100ml", "christian dior sauvage"), true);
});

test("pm search: normalizer", () => {
  assert.equal(pm.pmNormalizeSearchText("Hermès  Ёлка J’adore"), "hermes елка j'adore");
});

test("pm search: MySQL clause mirrors JS filter (n-1 for 3+ required groups)", () => {
  const groups = pm.pmQueryToTokenGroups("christian dior sauvage 100 ml");
  const { where, params, scoreSql, scoreParams } = pm.pmBuildMysqlSearchClause(groups, { name: "r.NativeName", article: "r.NativeID", barcode: "r.BarCode" });
  assert.match(where, />= \?/);
  assert.ok(params.includes("%100%"));
  assert.ok(!params.includes("%ml%"), "unit tokens must not be SQL-required");
  assert.ok(scoreSql.length > 0);
  assert.ok(scoreParams.length > 0);
});

test("pm search: MySQL clause uses longest part for apostrophe tokens", () => {
  const groups = pm.pmQueryToTokenGroups("j'adore");
  const { params } = pm.pmBuildMysqlSearchClause(groups, { name: "r.NativeName", article: "r.NativeID" });
  assert.ok(params.includes("%adore%"));
});

test("pm search: Prisma where requires strict groups (AND), not a flat OR", () => {
  const where = pm.pmBuildPrismaSearchWhere(pm.pmQueryToTokenGroups("dior sauvage 100 ml"));
  assert.ok(Array.isArray(where) && where.length >= 2);
});

test("pm search: full vs partial (n-1) match flag", () => {
  const groups = pm.pmQueryToTokenGroups("christian dior sauvage");
  assert.equal(pm.pmIsFullMatch("Christian Dior Sauvage EDT", groups), true);
  assert.equal(pm.pmIsFullMatch("Dior Sauvage EDT", groups), false);
});

test("pm search: MySQL placeholders match params for many query shapes", () => {
  const count = (sql) => (sql.match(/\?/g) || []).length;
  for (const q of ["dior", "christian dior sauvage 100 ml", "j'adore 50", "bod13", "eau de", "chanel no5 edp", "3348901250146", "шанель шанс 5 мл тестер"]) {
    const clause = pm.pmBuildMysqlSearchClause(pm.pmQueryToTokenGroups(q), { name: "r.NativeName", article: "r.NativeID", barcode: "r.BarCode" });
    assert.equal(count(clause.where), clause.params.length, `where/params for "${q}"`);
    assert.equal(count(clause.scoreSql), clause.scoreParams.length, `score/params for "${q}"`);
  }
});

test("pm search: typed word without apostrophe finds apostrophe names", () => {
  assert.equal(passes("Dior J'adore EDP 100ml", "dior jadore"), true);
  assert.equal(passes("Dior J`adore EDP 100ml", "jadore"), true);
  const { where } = pm.pmBuildMysqlSearchClause(pm.pmQueryToTokenGroups("jadore"), { name: "r.NativeName", article: "r.NativeID" });
  assert.match(where, /REPLACE\(/);
});

test("pm search: word index splits apostrophe words", () => {
  assert.deepEqual([...pm.pmWordTokenize("Dior J'adore")], ["dior", "j", "adore"]);
});
