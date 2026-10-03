// Shop pricing profiles (Настройки → Цены → отдельные настройки магазина, e.g. Ozon AURA).
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const parts = path.join(__dirname, "..", "server", "parts");
const read = (file) => fs.readFileSync(path.join(parts, file), "utf8").replace(/\r\n/g, "\n");
const grab = (src, name) => {
  const i = src.indexOf(`function ${name}(`);
  if (i < 0) throw new Error(`no ${name}`);
  return src.slice(i, src.indexOf("\n}\n", i) + 3);
};
const settingsSrc = read("02a-app-settings.js");
const helpersSrc = read("02a-price-master-warehouse-helpers.js");
const code = [
  "const cleanText = (v) => String(v ?? '').trim();",
  "const parseBooleanSetting = (v, d) => (v === undefined || v === null || v === '' ? d : v === true || v === 'true' || v === 1 || v === '1');",
  "const defaultAppSettings = () => ({ availabilityRules: [] });",
  ...["normalizeMarkupRule", "normalizeAvailabilityRule", "normalizeTargetPricingProfile", "normalizeTargetPricing", "pricingSettingsForTarget", "copyMarketplacePricingProfile"].map((n) => grab(settingsSrc, n)),
  ...["resolveMarkupCoefficient", "resolveAvailabilityPolicy"].map((n) => grab(helpersSrc, n)),
  "module.exports = { normalizeTargetPricing, pricingSettingsForTarget, copyMarketplacePricingProfile, resolveMarkupCoefficient, resolveAvailabilityPolicy };",
].join("\n");
const m = { exports: {} };
new Function("module", code)(m);
const { normalizeTargetPricing, copyMarketplacePricingProfile, resolveMarkupCoefficient, resolveAvailabilityPolicy } = m.exports;

const general = {
  defaultMarkups: { ozon: 1.7258, yandex: 1.6 },
  markupRules: [
    { minUsd: 0, coefficient: 2.2, marketplace: "all" },
    { minUsd: 50, coefficient: 1.9, marketplace: "ozon" },
    { minUsd: 100, coefficient: 1.7, marketplace: "ozon" },
    { minUsd: 100, coefficient: 1.5, marketplace: "yandex" },
  ],
  availabilityRules: [
    { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
    { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 5 },
  ],
};
const markup = (settings, target, usd, marketplace = "ozon") => resolveMarkupCoefficient({ productMarkup: 0, marketplace, target, supplierUsdPrice: usd, usdRate: 95, appSettings: settings });
const policy = (settings, target, count, marketplace = "ozon") => resolveAvailabilityPolicy({ marketplace, target, availableSupplierCount: count, baseMarkup: 2, appSettings: settings });

test("a copy of the Ozon settings prices AURA exactly like the main Ozon shop", () => {
  const settings = { ...general, targetPricing: normalizeTargetPricing({ "ozon-aura": copyMarketplacePricingProfile(general, "ozon", "AURA") }) };
  for (const usd of [0, 10, 49.99, 50, 75, 100, 250, 1000]) {
    assert.equal(markup(settings, "ozon-aura", usd), markup(general, "ozon", usd), `usd ${usd}`);
  }
  const numbers = (p) => ({ markupCoefficient: p.markupCoefficient, targetStock: p.targetStock, coefficientDelta: p.coefficientDelta });
  for (const n of [0, 1, 4, 5, 9]) assert.deepEqual(numbers(policy(settings, "ozon-aura", n)), numbers(policy(general, "ozon", n)), `suppliers ${n}`);
});

test("changing the AURA profile changes AURA only", () => {
  const profile = copyMarketplacePricingProfile(general, "ozon", "AURA");
  profile.markupRules = profile.markupRules.map((r) => ({ ...r, coefficient: r.coefficient + 0.3 }));
  profile.defaultMarkup = 2.5;
  profile.availabilityRules = [{ marketplace: "ozon", minAvailableSuppliers: 1, coefficientDelta: 0.1, targetStock: 2 }];
  const settings = { ...general, targetPricing: normalizeTargetPricing({ "ozon-aura": profile }) };
  assert.equal(markup(settings, "ozon-aura", 120), 2.0);
  assert.equal(markup(settings, "ozon", 120), 1.7, "main Ozon untouched");
  assert.equal(markup(settings, "yandex-shop", 120, "yandex"), 1.5, "Market untouched");
  assert.equal(policy(settings, "ozon-aura", 3).targetStock, 2);
  assert.equal(policy(settings, "ozon-aura", 3).markupCoefficient, 2.1);
  assert.equal(policy(settings, "ozon", 3).targetStock, 5, "main Ozon stock rule untouched");
});

test("a disabled profile or another marketplace falls back to the general settings", () => {
  const profile = { ...copyMarketplacePricingProfile(general, "ozon", "AURA"), enabled: false, defaultMarkup: 9 };
  const settings = { ...general, targetPricing: normalizeTargetPricing({ "ozon-aura": profile }) };
  assert.equal(markup(settings, "ozon-aura", 120), 1.7);
  const ozonProfile = { ...copyMarketplacePricingProfile(general, "ozon", "x"), markupRules: [{ minUsd: 0, coefficient: 5 }] };
  const s2 = { ...general, targetPricing: normalizeTargetPricing({ shop: ozonProfile }) };
  assert.equal(markup(s2, "shop", 120, "yandex"), 1.5, "an Ozon profile never touches a Market price");
});
