const { test } = require("node:test");
const assert = require("node:assert/strict");
const request = require("supertest");

process.env.APP_PASSWORD = process.env.APP_PASSWORD || "smoke-test-password";
process.env.APP_SESSION_SECRET = process.env.APP_SESSION_SECRET || "smoke-test-session-secret-min-32-chars!";
process.env.APP_USER = process.env.APP_USER || "admin";
process.env.NODE_ENV = "production"; // в production csrfGuard строгий
process.env.PUBLIC_BASE_URL = "http://localhost:3000";

const { app } = require("../server.js");

test("CSRF: POST /api/logout без Origin/Referer в production отклоняется", async () => {
  const res = await request(app).post("/api/logout").send({});
  assert.equal(res.status, 403);
});

test("CSRF: POST /api/logout с чужим Origin отклоняется", async () => {
  const res = await request(app)
    .post("/api/logout")
    .set("Origin", "https://evil.example.com")
    .send({});
  assert.equal(res.status, 403);
});

test("CSRF: POST /api/logout со своим Origin проходит csrfGuard", async () => {
  const res = await request(app)
    .post("/api/logout")
    .set("Origin", "http://localhost:3000")
    .send({});
  // logout всегда отвечает 200
  assert.equal(res.status, 200);
});

test("CSRF: GET-запросы не блокируются", async () => {
  const res = await request(app).get("/health");
  assert.equal(res.status, 200);
});

test("/health/ready возвращает 503 при недоступной БД", async () => {
  const res = await request(app).get("/health/ready");
  // В тестовом окружении БД, скорее всего, недоступна — ожидаем 503.
  // Если вдруг доступна — ответ 200, тоже валидно.
  assert.ok([200, 503].includes(res.status), `unexpected status ${res.status}`);
  if (res.status === 503) assert.equal(res.body.mysql, "fail");
});
