"use strict";

// Unit tests for buildOzonPricePayload (server/parts/02a-utils-ozon-price-payload.js), loaded
// standalone in a VM context with the two helpers it needs.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const parts = path.join(__dirname, "..", "server", "parts");
const env = {};
const ctx = vm.createContext({ process: { env } });
const helpers = fs.readFileSync(path.join(parts, "02a-utils-image-file-helpers.js"), "utf8");
const pick = (name) => helpers.slice(helpers.indexOf(`function ${name}(`)).split("\n}\n")[0] + "\n}\n";
vm.runInContext(`${pick("roundPrice")}${pick("parseBooleanSetting")}
${fs.readFileSync(path.join(parts, "02a-utils-ozon-price-payload.js"), "utf8")}
this.api = { buildOzonPricePayload };`, ctx);
const { buildOzonPricePayload } = ctx.api;

test("price push turns off every way Ozon puts the product into its special offers", () => {
  const payload = buildOzonPricePayload({ offerId: "56989", price: 29315 });
  assert.equal(payload.auto_action_enabled, "DISABLED");
  assert.equal(payload.price_strategy_enabled, "DISABLED");
  assert.equal(payload.manage_elastic_boosting_through_price, false);
});

test("OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS=false leaves Ozon's promo settings untouched", () => {
  env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS = "false";
  try {
    const payload = buildOzonPricePayload({ offerId: "56989", price: 29315 });
    assert.equal(payload.auto_action_enabled, undefined);
    assert.equal(payload.manage_elastic_boosting_through_price, undefined);
  } finally {
    delete env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS;
  }
});
