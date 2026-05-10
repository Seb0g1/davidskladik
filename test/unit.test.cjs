const { test } = require("node:test");
const assert = require("node:assert/strict");

process.env.APP_PASSWORD = process.env.APP_PASSWORD || "smoke-test-password";
process.env.APP_SESSION_SECRET = process.env.APP_SESSION_SECRET || "smoke-test-session-secret-min-32-chars!";
process.env.APP_USER = process.env.APP_USER || "admin";

const {
  parseMoneyValue,
  pickOzonCabinetListedPrice,
  roundPrice,
  withRetry,
  isTransientError,
} = require("../server.js");

test("parseMoneyValue: пустые/мусор → null", () => {
  assert.equal(parseMoneyValue(null), null);
  assert.equal(parseMoneyValue(""), null);
  assert.equal(parseMoneyValue("abc"), null);
  assert.equal(parseMoneyValue("0"), null);
  assert.equal(parseMoneyValue("-5"), null);
});

test("parseMoneyValue: запятая, пробелы, число", () => {
  assert.equal(parseMoneyValue("1 234,56"), 1234.56);
  assert.equal(parseMoneyValue("99.9"), 99.9);
  assert.equal(parseMoneyValue(42), 42);
});

test("pickOzonCabinetListedPrice: приоритет полей", () => {
  assert.equal(pickOzonCabinetListedPrice({}), null);
  assert.equal(pickOzonCabinetListedPrice({ retailPrice: 100 }), 100);
  assert.equal(
    pickOzonCabinetListedPrice({ marketingPrice: 200, retailPrice: 100 }),
    200,
  );
  assert.equal(
    pickOzonCabinetListedPrice({
      currentPrice: 300,
      marketingSellerPrice: 250,
      marketingPrice: 200,
      retailPrice: 100,
    }),
    300,
  );
});

test("roundPrice: округление и защита от мусора", () => {
  assert.equal(roundPrice(100.4), 100);
  assert.equal(roundPrice(100.5), 101);
  assert.equal(roundPrice("99.9"), 100);
  assert.equal(roundPrice(null), 0);
  assert.equal(roundPrice(-5), 0);
  assert.equal(roundPrice("abc"), 0);
});

test("isTransientError: распознаёт сетевые коды", () => {
  assert.equal(isTransientError({ code: "ECONNRESET" }), true);
  assert.equal(isTransientError({ code: "ETIMEDOUT" }), true);
  assert.equal(isTransientError({ message: "fetch failed" }), true);
  assert.equal(isTransientError({ code: "ER_PARSE_ERROR" }), false);
  assert.equal(isTransientError(null), false);
});

test("withRetry: возвращает результат с первой попытки", async () => {
  let calls = 0;
  const result = await withRetry(async () => {
    calls += 1;
    return "ok";
  });
  assert.equal(result, "ok");
  assert.equal(calls, 1);
});

test("withRetry: повторяет на транзиентной ошибке и в итоге успех", async () => {
  let calls = 0;
  const result = await withRetry(
    async () => {
      calls += 1;
      if (calls < 3) {
        const err = new Error("temp");
        err.code = "ECONNRESET";
        throw err;
      }
      return "done";
    },
    { attempts: 3, baseDelayMs: 10 },
  );
  assert.equal(result, "done");
  assert.equal(calls, 3);
});

test("withRetry: не повторяет на не-транзиентной ошибке", async () => {
  let calls = 0;
  await assert.rejects(
    withRetry(
      async () => {
        calls += 1;
        const err = new Error("bad request");
        err.code = "ER_BAD_FIELD_ERROR";
        throw err;
      },
      { attempts: 3, baseDelayMs: 10 },
    ),
    /bad request/,
  );
  assert.equal(calls, 1);
});
