"use strict";

// «Документы Ozon» (server/parts/02d-ozon-doc-status.js): reason of a refused product, Ozon dictionaries.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02d-ozon-doc-status.js"), "utf8");
const ctx = vm.createContext({
  process: { env: {} },
  app: { get() {}, post() {} },
  requireAdmin() {},
  cleanText: (v) => String(v ?? "").trim(),
});
vm.runInContext(`${source}\nthis.api = { ozonDocProductReason, ozonDocDictionary };`, ctx);
const { ozonDocProductReason, ozonDocDictionary } = ctx.api;

const reasons = ozonDocDictionary({ result: [{ code: "not_in_registry", name: "Документа нет в едином реестре" }, { code: "expired_document", name: "Срок действия документа истек" }] });

test("dictionary: code → name, empty answers are fine", () => {
  assert.equal(reasons.not_in_registry, "Документа нет в едином реестре");
  assert.deepEqual(JSON.parse(JSON.stringify(ozonDocDictionary({}))), {});
});

test("declined product: own reason first, else the document's reason and comment", () => {
  assert.equal(ozonDocProductReason({ product_status_code: "declined", rejection_reason_code: "expired_document" }, {}, reasons), "Срок действия документа истек");
  assert.equal(
    ozonDocProductReason({ product_status_code: "declined" }, { status_code: "declined", rejection_reason_code: "not_in_registry", verification_comment: "Проверьте номер" }, reasons),
    "Документа нет в едином реестре. Проверьте номер",
  );
  assert.equal(ozonDocProductReason({ product_status_code: "declined" }, { status_code: "approved" }, reasons), "Ozon не указал причину");
});

test("approved / waiting product has no reason", () => {
  assert.equal(ozonDocProductReason({ product_status_code: "approved" }, { status_code: "approved" }, reasons), "");
  assert.equal(ozonDocProductReason({ product_status_code: "awaiting_verification" }, { status_code: "pending" }, reasons), "");
});
