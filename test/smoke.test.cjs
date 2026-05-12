const { test } = require("node:test");
const assert = require("node:assert/strict");
const request = require("supertest");

process.env.APP_PASSWORD = process.env.APP_PASSWORD || "smoke-test-password";
process.env.APP_SESSION_SECRET = process.env.APP_SESSION_SECRET || "smoke-test-session-secret-min-32-chars!";
process.env.APP_USER = process.env.APP_USER || "admin";
process.env.AUTO_ARCHIVE_ON_NO_LINKS = "true";
process.env.PUBLIC_BASE_URL = "http://localhost";
process.env.DISABLE_BACKGROUND_JOBS = "true";

const {
  app,
  resolveMarkupCoefficient,
  resolveAvailabilityPolicy,
  normalizeManagedSupplier,
  resolvePriceMasterRowCurrency,
  normalizePriceMasterPrice,
  pickNoSupplierAutomationCandidates,
  pickSupplierRecoveryCandidates,
  pickWarehouseSupplier,
  warehouseBrandMatches,
  normalizeWarehouseProduct,
  buildOzonStockPayloadItems,
  marketplaceHasPositiveStock,
  warehouseLinkIdentityKey,
} = require("../server.js");

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

test("POST /api/login supports APP_USERS_JSON roles", async () => {
  const previousUsers = process.env.APP_USERS_JSON;
  process.env.APP_USERS_JSON = JSON.stringify([
    { username: "manager", password: "manager-pass", role: "manager" },
  ]);
  try {
    const res = await request(app)
      .post("/api/login")
      .send({ username: "manager", password: "manager-pass" })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.username, "manager");
    assert.equal(res.body.role, "manager");
  } finally {
    if (previousUsers === undefined) delete process.env.APP_USERS_JSON;
    else process.env.APP_USERS_JSON = previousUsers;
  }
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
        availabilityRules: [{ marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 }],
      })
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.settings.defaultMarkups.ozon, 1.91);
    assert.equal(res.body.settings.markupRules[0].coefficient, 1.91);
    assert.equal(res.body.settings.availabilityRules[0].targetStock, 10);
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

test("resolveAvailabilityPolicy lowers markup and raises stock for many suppliers", () => {
  const policy = resolveAvailabilityPolicy({
    marketplace: "ozon",
    availableSupplierCount: 5,
    baseMarkup: 1.7,
    appSettings: {
      availabilityRules: [
        { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
        { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 },
      ],
    },
  });
  assert.equal(policy.markupCoefficient, 1.65);
  assert.equal(policy.targetStock, 10);
});

test("resolveAvailabilityPolicy keeps base markup and small stock for few suppliers", () => {
  const policy = resolveAvailabilityPolicy({
    marketplace: "ozon",
    availableSupplierCount: 1,
    baseMarkup: 1.7,
    appSettings: {
      availabilityRules: [
        { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
        { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 },
      ],
    },
  });
  assert.equal(policy.markupCoefficient, 1.7);
  assert.equal(policy.targetStock, 3);
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

test("normalizeManagedSupplier defaults PriceMaster currency to USD", () => {
  const supplier = normalizeManagedSupplier({ name: "Supplier" });
  assert.equal(supplier.priceCurrency, "USD");
});

test("resolvePriceMasterRowCurrency prefers supplier fixed currency", () => {
  const currency = resolvePriceMasterRowCurrency(
    { partnerId: "88", partnerName: "Supplier" },
    { priceCurrency: "USD" },
    {
      byPartnerId: new Map([["88", { priceCurrency: "RUB" }]]),
      byName: new Map(),
    },
  );
  assert.equal(currency, "RUB");
});

test("pickWarehouseSupplier chooses the cheapest available calculated price", () => {
  const picked = pickWarehouseSupplier([
    { partnerName: "Expensive", available: true, price: 20, calculatedPrice: 3400, docDate: "2026-01-01" },
    { partnerName: "Cheap", available: true, price: 10, calculatedPrice: 1700, docDate: "2026-01-02" },
    { partnerName: "Missing", available: false, price: 1, calculatedPrice: 100, docDate: "2026-01-03" },
  ]);
  assert.equal(picked.partnerName, "Cheap");
});

test("warehouse brand filter falls back to marketplace product data", () => {
  const product = {
    name: "Нишевый аромат без бренда в корне",
    ozon: {
      name: "AMOUAGE Guidance 100 ml",
      attributes: [],
    },
  };

  assert.equal(warehouseBrandMatches(product, "Amouage"), true);
});

test("warehouse brand filter finds brand in raw Ozon attributes", () => {
  const product = {
    name: "Товар без названия бренда",
    ozon: {
      attributes: [
        {
          id: 85,
          name: "Бренд",
          values: [{ value: "Amouage" }],
        },
      ],
    },
  };

  assert.equal(warehouseBrandMatches(product, "Amouage"), true);
});

test("warehouse brand filter scans non-standard marketplace fields", () => {
  const product = {
    name: "Товар без бренда в названии",
    ozon: {
      rawPayload: {
        brand_name_from_api: "Amouage",
      },
    },
  };

  assert.equal(warehouseBrandMatches(product, "Amouage"), true);
});

test("normalizeWarehouseProduct preserves AI image draft review state", () => {
  const product = normalizeWarehouseProduct({
    target: "ozon",
    offerId: "ai-draft-1",
    name: "AI Test Product",
    aiImages: [
      {
        id: "draft-1",
        status: "approved",
        sourceImageUrl: "http://localhost/uploads/images/source.png",
        resultUrl: "http://localhost/uploads/ai-images/result.png",
        prompt: "Generate for {productName}",
        reviewedAt: "2026-01-01T00:00:00.000Z",
      },
    ],
  });

  assert.equal(product.aiImages.length, 1);
  assert.equal(product.aiImages[0].status, "approved");
  assert.equal(product.aiImages[0].resultUrl, "http://localhost/uploads/ai-images/result.png");
});

test("AI image generation requires OpenAI key before creating draft", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-ai-${Date.now()}`;
  let product;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  const saved = await agent
    .post("/api/warehouse/products")
    .send({
      id: smokeId,
      target: "ozon",
      offerId: smokeId,
      name: "Smoke AI Product",
      ozon: {
        offerId: smokeId,
        name: "Smoke AI Product",
        primaryImage: "http://localhost/uploads/images/source.png",
      },
    })
    .expect(200);

  product = saved.body.warehouse.products.find((item) => item.id === smokeId);
  assert.ok(product);

  const previousKey = process.env.OPENAI_API_KEY;
  const previousRelayUrl = process.env.OPENAI_RELAY_URL;
  const previousRelaySecret = process.env.OPENAI_RELAY_SECRET;
  delete process.env.OPENAI_API_KEY;
  delete process.env.OPENAI_RELAY_URL;
  delete process.env.OPENAI_RELAY_SECRET;
  try {
    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/generate`)
      .send({ sourceImageUrl: "http://localhost/uploads/images/source.png" })
      .expect(400);

    assert.equal(res.body.code, "openai_not_configured");
  } finally {
    if (previousKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
    if (previousRelayUrl === undefined) delete process.env.OPENAI_RELAY_URL;
    else process.env.OPENAI_RELAY_URL = previousRelayUrl;
    if (previousRelaySecret === undefined) delete process.env.OPENAI_RELAY_SECRET;
    else process.env.OPENAI_RELAY_SECRET = previousRelaySecret;
    if (product?.id) await agent.delete(`/api/warehouse/products/${encodeURIComponent(product.id)}`).expect(200);
  }
});

test("warehouse product patch rejects stale expectedUpdatedAt", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-lock-${Date.now()}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    const created = await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        offerId: smokeId,
        name: "Smoke Lock Product",
      })
      .expect(200);

    const currentUpdatedAt = created.body.product.updatedAt;
    assert.ok(currentUpdatedAt);

    await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(smokeId)}`)
      .send({ markup: 1.77, expectedUpdatedAt: "2026-01-01T00:00:00.000Z" })
      .expect(409);

    const ok = await agent
      .patch(`/api/warehouse/products/${encodeURIComponent(smokeId)}`)
      .send({ markup: 1.77, expectedUpdatedAt: currentUpdatedAt })
      .expect(200);

    assert.equal(ok.body.product.markup, 1.77);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
});

test("AI image draft approval updates local Ozon image fields only", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-ai-approve-${Date.now()}`;
  const draftId = "draft-approved-smoke";
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        offerId: smokeId,
        name: "Smoke AI Approve Product",
        ozon: {
          offerId: smokeId,
          name: "Smoke AI Approve Product",
          primaryImage: "http://localhost/uploads/images/original.png",
          images: ["http://localhost/uploads/images/original.png"],
        },
        aiImages: [
          {
            id: draftId,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated.png",
            prompt: "Generate marketplace image",
          },
        ],
      })
      .expect(200);

    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/${encodeURIComponent(draftId)}/approve`)
      .send({})
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.product.ozon.primaryImage, "http://localhost/uploads/ai-images/generated.png");
    assert.equal(res.body.product.ozon.images[0], "http://localhost/uploads/ai-images/generated.png");
    assert.equal(res.body.product.aiImages[0].status, "approved");
    assert.equal(res.body.result, undefined);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
});

test("AI image batch approval keeps selected image first and saves batch gallery", async () => {
  const agent = request.agent(app);
  const smokeId = `smoke-ai-batch-${Date.now()}`;
  await agent
    .post("/api/login")
    .send({ username: "admin", password: process.env.APP_PASSWORD })
    .expect(200);

  try {
    await agent
      .post("/api/warehouse/products")
      .send({
        id: smokeId,
        target: "ozon",
        offerId: smokeId,
        name: "Smoke AI Batch Product",
        ozon: {
          offerId: smokeId,
          name: "Smoke AI Batch Product",
          primaryImage: "http://localhost/uploads/images/original.png",
          images: ["http://localhost/uploads/images/original.png"],
        },
        aiImages: [
          {
            id: "draft-batch-1",
            batchId: "batch-smoke",
            variantIndex: 1,
            variantTotal: 3,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated-1.png",
            prompt: "Main slide",
          },
          {
            id: "draft-batch-2",
            batchId: "batch-smoke",
            variantIndex: 2,
            variantTotal: 3,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated-2.png",
            prompt: "Benefits slide",
          },
          {
            id: "draft-batch-3",
            batchId: "batch-smoke",
            variantIndex: 3,
            variantTotal: 3,
            status: "pending",
            sourceImageUrl: "http://localhost/uploads/images/original.png",
            resultUrl: "http://localhost/uploads/ai-images/generated-3.png",
            prompt: "Notes slide",
          },
        ],
      })
      .expect(200);

    const res = await agent
      .post(`/api/warehouse/products/${encodeURIComponent(smokeId)}/ai-images/draft-batch-2/approve`)
      .send({})
      .expect(200);

    assert.equal(res.body.ok, true);
    assert.equal(res.body.product.ozon.primaryImage, "http://localhost/uploads/ai-images/generated-2.png");
    assert.deepEqual(res.body.product.ozon.images.slice(0, 3), [
      "http://localhost/uploads/ai-images/generated-2.png",
      "http://localhost/uploads/ai-images/generated-1.png",
      "http://localhost/uploads/ai-images/generated-3.png",
    ]);
    assert.equal(res.body.product.aiImages.every((item) => item.status === "approved"), true);
  } finally {
    await agent.delete(`/api/warehouse/products/${encodeURIComponent(smokeId)}`).expect(200);
  }
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

test("automation queues archive for linked product without supplier", () => {
  const { toArchive } = pickNoSupplierAutomationCandidates([
    { id: "candidate", hasLinks: true, selectedSupplier: null, noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z" }, marketplaceState: { code: "inactive" } },
    { id: "not-ready", hasLinks: true, selectedSupplier: null, noSupplierAutomation: {}, marketplaceState: { code: "inactive" } },
  ]);
  assert.equal(toArchive.length, 2);
  assert.equal(toArchive[0].id, "candidate");
});

test("automation re-queues stock=0 when marketplace stock returned after prior zero", () => {
  const product = {
    id: "stock-returned",
    hasLinks: true,
    selectedSupplier: null,
    noSupplierAutomation: { stockZeroAt: "2026-01-01T00:00:00.000Z" },
    marketplaceState: { stock: 2 },
  };
  assert.equal(marketplaceHasPositiveStock(product), true);
  const { toZeroStock } = pickNoSupplierAutomationCandidates([product]);
  assert.equal(toZeroStock.length, 1);
  assert.equal(toZeroStock[0].id, "stock-returned");
});

test("Ozon stock payload targets configured warehouses and zeros all of them", async () => {
  const previous = process.env.OZON_STOCK_WAREHOUSE_IDS;
  process.env.OZON_STOCK_WAREHOUSE_IDS = "111,222";
  try {
    const targetPayload = await buildOzonStockPayloadItems(
      [{ offerId: "sku-1", targetStock: 10 }],
      { id: "ozon" },
      (item) => item.targetStock,
    );
    assert.deepEqual(targetPayload, [{ offer_id: "sku-1", warehouse_id: 111, stock: 10 }]);

    const zeroPayload = await buildOzonStockPayloadItems(
      [{ offerId: "sku-1" }],
      { id: "ozon" },
      () => 0,
      { allWarehouses: true },
    );
    assert.deepEqual(zeroPayload, [
      { offer_id: "sku-1", warehouse_id: 111, stock: 0 },
      { offer_id: "sku-1", warehouse_id: 222, stock: 0 },
    ]);
  } finally {
    if (previous === undefined) delete process.env.OZON_STOCK_WAREHOUSE_IDS;
    else process.env.OZON_STOCK_WAREHOUSE_IDS = previous;
  }
});

test("warehouse link identity ignores client draft id duplicates", () => {
  const a = warehouseLinkIdentityKey({ id: "draft-1", article: "A-1", partnerId: "88", supplierName: " Supplier ", keyword: "Blue", priceCurrency: "rub" });
  const b = warehouseLinkIdentityKey({ id: "draft-2", article: "A-1", partnerId: "88", supplierName: "supplier", keyword: "blue", priceCurrency: "RUB" });
  assert.equal(a, b);
});

test("recovery queues archived linked product when supplier is available", () => {
  const recovered = pickSupplierRecoveryCandidates([
    {
      id: "archived-with-supplier",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      noSupplierAutomation: {},
      marketplaceState: { code: "archived" },
    },
    {
      id: "active-with-supplier",
      hasLinks: true,
      selectedSupplier: { price: 10, available: true },
      noSupplierAutomation: {},
      marketplaceState: { code: "active" },
    },
  ]);
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].id, "archived-with-supplier");
});
