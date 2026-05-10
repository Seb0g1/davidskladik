const { test } = require("node:test");
const assert = require("node:assert/strict");
const request = require("supertest");

process.env.APP_PASSWORD = process.env.APP_PASSWORD || "smoke-test-password";
process.env.APP_SESSION_SECRET = process.env.APP_SESSION_SECRET || "smoke-test-session-secret-min-32-chars!";
process.env.APP_USER = process.env.APP_USER || "admin";
process.env.AUTO_ARCHIVE_ON_NO_LINKS = "true";
process.env.PUBLIC_BASE_URL = "http://localhost";
process.env.DISABLE_BACKGROUND_JOBS = "true";

const { app, resolveMarkupCoefficient, normalizePriceMasterPrice, pickNoSupplierAutomationCandidates } = require("../server.js");

test("GET /health", async () => {
  const res = await request(app).get("/health").expect(200);
  assert.equal(res.body.ok, true);
  assert.ok(res.body.service);
});

test("POST /api/login неверный пароль", async () => {
  await request(app)
    .post("/api/login")
    .send({ username: "admin", password: "wrong" })
    .expect(401);
});

test("POST /api/login успех", async () => {
  const res = await request(app)
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);
  assert.equal(res.body.ok, true);
  assert.ok(res.headers["set-cookie"]);
});

test("PUT /api/settings saves markup settings", async () => {
  const agent = request.agent(app);
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  const before = await agent.get("/api/settings").expect(200);
  const previous = before.body.settings;
  try {
    const res = await agent
      .put("/api/settings")
      .send({
        fixedUsdRate: 95,
        defaultMarkups: { ozon: 1.91, yandex: 1.62 },
        markupRules: [{ marketplace: "all", minUsd: 0, coefficient: 1.91 }],
      })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.settings.defaultMarkups.ozon, 1.91);
    assert.equal(res.body.settings.markupRules[0].coefficient, 1.91);
  } finally {
    if (previous) {
      await agent.put("/api/settings").send(previous);
    }
  }
});

test("resolveMarkupCoefficient applies threshold >= 10 USD", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace: "ozon",
    supplierUsdPrice: 12,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [{ minUsd: 10, coefficient: 3 }, { minUsd: 20, coefficient: 2.8 }],
    },
  });
  assert.equal(value, 3);
});

test("resolveMarkupCoefficient applies threshold >= 20 USD", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace: "ozon",
    supplierUsdPrice: 25,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [{ minUsd: 10, coefficient: 3 }, { minUsd: 20, coefficient: 2.8 }],
    },
  });
  assert.equal(value, 2.8);
});

test("resolveMarkupCoefficient uses product markup override", () => {
  const value = resolveMarkupCoefficient({
    productMarkup: 2.2,
    marketplace: "ozon",
    supplierUsdPrice: 25,
    appSettings: {
      defaultMarkups: { ozon: 1.7, yandex: 1.6 },
      markupRules: [{ minUsd: 10, coefficient: 3 }, { minUsd: 20, coefficient: 2.8 }],
    },
  });
  assert.equal(value, 2.2);
});

test("normalizePriceMasterPrice converts explicitly ruble values to USD", () => {
  const value = normalizePriceMasterPrice(9500, 95, "RUB");
  assert.equal(value.sourceCurrency, "RUB");
  assert.equal(value.convertedFromRub, true);
  assert.equal(value.price, 100);
  assert.equal(value.originalPrice, 9500);
});

test("normalizePriceMasterPrice keeps dollar-like values in USD", () => {
  const value = normalizePriceMasterPrice(9500, 95);
  assert.equal(value.sourceCurrency, "USD");
  assert.equal(value.convertedFromRub, false);
  assert.equal(value.price, 9500);
});

test("automation ignores products without links", () => {
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates([
    { id: "nolinks", hasLinks: false, selectedSupplier: null, noSupplierAutomation: {} },
  ]);
  assert.equal(toZeroStock.length, 0);
  assert.equal(toArchive.length, 0);
});

test("automation queues linked product for stock=0 when supplier disappeared", () => {
  const { toZeroStock } = pickNoSupplierAutomationCandidates([
    { id: "linked-no-supplier", hasLinks: true, selectedSupplier: null, noSupplierAutomation: {}, marketplaceState: { code: "active" } },
  ]);
  assert.equal(toZeroStock.length, 1);
  assert.equal(toZeroStock[0].id, "linked-no-supplier");
});

test("automation queues archive only after stock zero mark", () => {
  const { toArchive } = pickNoSupplierAutomationCandidates([
    { id: "candidate", hasLinks: true, selectedSupplier: null, noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z" }, marketplaceState: { code: "inactive" } },
    { id: "not-ready", hasLinks: true, selectedSupplier: null, noSupplierAutomation: {}, marketplaceState: { code: "inactive" } },
  ]);
  assert.equal(toArchive.length, 1);
  assert.equal(toArchive[0].id, "candidate");
});
