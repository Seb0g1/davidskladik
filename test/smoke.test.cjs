const { test } = require("node:test");
const assert = require("node:assert/strict");
const request = require("supertest");

process.env.APP_PASSWORD = process.env.APP_PASSWORD || "smoke-test-password";
process.env.APP_SESSION_SECRET = process.env.APP_SESSION_SECRET || "smoke-test-session-secret-min-32-chars!";
process.env.APP_USER = process.env.APP_USER || "admin";

const { app, resolveMarkupCoefficient } = require("../server.js");

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
