#!/usr/bin/env node
"use strict";

// Цепочка Wildberries НА СЕРВЕРЕ отдельным процессом с большим heap:
//   cd /var/www/davidsklad/davidskladik && \
//   MALLOC_ARENA_MAX=2 NODE_OPTIONS='--max-old-space-size=4096' \
//   node scripts/prod-wb-chain.cjs <chain|preview|apply|cards|errors|media|prices|stocks> [args]
//
// require(server.js) собирает всё приложение, но НЕ вызывает startServer():
// ни HTTP-листенера, ни фоновых шедулеров — только функции.
//
// Прогресс и итог каждого шага пишутся в data/wb-chain-result.json — их отдаёт
// GET /api/wb/chain/result (наблюдение без SSH). Бизнес-правило: на WB только
// товары с закупкой поставщика ≥ minSupplierPriceRub (деф. 15 000 ₽).
//
// stocks/prices трогают ТОЛЬКО карточки, привязанные к нашим товарам Ozon
// (vendorCode = offerId). Ручные карточки кабинета (sultane*/sklad*) не трогаем.

const path = require("path");
const fsSync = require("fs");
process.chdir(path.join(__dirname, ".."));
const server = require(path.join(__dirname, "..", "server.js"));

const mode = process.argv[2] || "preview";
const clean = (value) => String(value == null ? "" : value).trim();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const resultPath = path.join(process.cwd(), "data", "wb-chain-result.json");
const state = { mode, startedAt: new Date().toISOString(), finishedAt: null, error: null, steps: [] };

function saveState() {
  try {
    fsSync.mkdirSync(path.dirname(resultPath), { recursive: true });
    fsSync.writeFileSync(resultPath, JSON.stringify(state, null, 2), "utf8");
  } catch (error) {
    console.error("saveState failed:", error?.message || String(error));
  }
}

function recordStep(step, result) {
  state.steps.push({ step, at: new Date().toISOString(), ...result });
  saveState();
  console.log(`=== ${step} ===`);
  console.log(JSON.stringify(result, null, 2).slice(0, 4000));
}

async function stepPreview() {
  const { evaluated, total, rules } = await server.collectWbImportCandidates({});
  const summary = server.summarizeWbImportPreview(evaluated);
  recordStep("preview", {
    total,
    rules: { subjectId: rules.subjectId, subjectName: rules.subjectName, minSupplierPriceRub: rules.minSupplierPriceRub, defaultStock: rules.defaultStock },
    ...summary,
    sample: evaluated.filter(({ result }) => result.ok).slice(0, 20).map(({ result }) => ({
      vendorCode: result.listing.vendorCode,
      title: result.listing.title,
      purchaseRub: result.listing.purchaseRub,
      priceRub: result.listing.priceRub,
    })),
  });
  return evaluated;
}

async function stepApply(account, evaluated, limit) {
  const rules = await server.readWbImportRules();
  if (!rules.subjectId) throw new Error("Не задан subjectId в правилах WB");
  // ТН ВЭД для новых карточек: код из правил или первый из справочника WB.
  let tnvedCharc = null;
  try {
    tnvedCharc = await server.resolveWbTnvedCharacteristic(account, rules);
  } catch (error) {
    console.error("tnved resolve failed:", error?.message || String(error));
  }
  const existingCards = await server.wbCardsList(account);
  const existingVendorCodes = new Set(existingCards.map((card) => clean(card.vendorCode).toLowerCase()).filter(Boolean));
  const candidates = evaluated
    .filter(({ result }) => result.ok && !existingVendorCodes.has(result.listing.vendorCode.toLowerCase()))
    .slice(0, limit)
    .map(({ result }) => result.listing);
  const withoutBarcode = candidates.filter((listing) => !listing.barcode);
  if (withoutBarcode.length) {
    const generated = await server.wbGenerateBarcodes(account, withoutBarcode.length);
    withoutBarcode.forEach((listing, index) => { listing.barcode = generated[index] || ""; });
  }
  const ready = candidates.filter((listing) => listing.barcode);
  let uploaded = 0;
  const errors = [];
  for (const chunk of server.chunkArray(ready, 50)) {
    try {
      await server.wbCardsUpload(account, chunk.map((listing) => server.buildWbCardPayload(listing, tnvedCharc)));
      uploaded += chunk.length;
      console.log(`uploaded chunk: +${chunk.length} (total ${uploaded})`);
    } catch (error) {
      errors.push({ vendorCodes: chunk.map((listing) => listing.vendorCode).slice(0, 5), error: error?.message || String(error) });
    }
  }
  recordStep("apply", {
    candidates: candidates.length,
    alreadyOnWb: existingVendorCodes.size,
    uploaded,
    tnved: tnvedCharc ? tnvedCharc.code : null,
    skippedNoBarcode: candidates.length - ready.length,
    errors: errors.slice(0, 20),
  });
  return uploaded;
}

// Дозаполнение ТН ВЭД в уже созданных карточках (cards/update).
async function stepTnved(account) {
  const result = await server.backfillWbTnvedCharacteristics(account, {});
  recordStep("tnved", result);
}

// Создание карточек на WB асинхронное: ждём, пока список карточек перестанет
// расти (nmID присвоены), максимум maxWaitMs.
async function stepWaitCards(account, maxWaitMs = 15 * 60 * 1000) {
  const startedAt = Date.now();
  let previousCount = -1;
  let stableRounds = 0;
  let cards = [];
  while (Date.now() - startedAt < maxWaitMs) {
    cards = await server.wbCardsList(account);
    if (cards.length === previousCount) {
      stableRounds += 1;
      if (stableRounds >= 3) break;
    } else {
      stableRounds = 0;
      previousCount = cards.length;
    }
    await sleep(30000);
  }
  recordStep("wait-cards", { cards: cards.length, waitedSec: Math.round((Date.now() - startedAt) / 1000) });
  return cards;
}

async function stepErrors(account) {
  const errors = await server.wbCardErrors(account);
  recordStep("card-errors", { total: errors.length, sample: errors.slice(0, 30) });
}

async function stepMedia(account, limit) {
  // Темп media/save: Token Bucket WB на продавца. Базовая пауза + честный
  // X-Ratelimit-Retry в ретраях wbRequest — иначе штраф лимитера не остывает
  // (ночной прогон: 46/50 ошибок 429 при паузе 1.2с).
  const pauseMs = Math.max(1000, Number(process.env.WB_MEDIA_PAUSE_MS || 10000) || 10000);
  const cards = await server.wbCardsList(account);
  const withoutPhoto = cards
    .filter((card) => Number(card.nmID) > 0 && !(Array.isArray(card.photos) && card.photos.length))
    .slice(0, limit);
  const linked = await server.loadWbLinkedOzonProducts(withoutPhoto.map((card) => card.vendorCode));
  const prisma = server.getPrisma();
  recordStep("media-start", { cardsWithoutPhoto: withoutPhoto.length, linked: linked.size, pauseMs });
  let sent = 0;
  let skippedNoImages = 0;
  let consecutive429 = 0;
  const errors = [];
  for (const card of withoutPhoto) {
    const product = linked.get(clean(card.vendorCode).toLowerCase());
    let imageUrls = [];
    if (product && prisma) {
      const row = await prisma.warehouseProduct.findUnique({ where: { id: product.id }, select: { raw: true } });
      const normalized = row?.raw && typeof row.raw === "object" ? server.normalizeWarehouseProduct(row.raw) : null;
      if (normalized) imageUrls = server.wbExtractImageUrls(normalized);
    }
    if (!imageUrls.length) {
      skippedNoImages += 1;
      continue;
    }
    // Глобальный лимитер WB на продавца: пауза между вызовами + не валим весь
    // шаг из-за одной карточки (ретраи 429 внутри wbRequest). Если WB отклонил
    // весь список URL — пробуем только первое фото (частая причина: один битый
    // URL валит весь вызов media/save).
    try {
      let result = await server.wbMediaSave(account, card.nmID, imageUrls.slice(0, 10));
      if (!result.ok && imageUrls.length > 1) {
        await sleep(pauseMs);
        result = await server.wbMediaSave(account, card.nmID, imageUrls.slice(0, 1));
      }
      if (result.ok) {
        sent += 1;
        consecutive429 = 0;
      } else {
        errors.push({ vendorCode: card.vendorCode, nmID: card.nmID, wb: result.result, error: result.error || "media_save_failed" });
      }
    } catch (error) {
      errors.push({ vendorCode: card.vendorCode, nmID: card.nmID, statusCode: error?.statusCode, rateLimit: error?.rateLimit, wb: error?.wb, error: error?.message || String(error) });
      // Лимит WB исчерпан даже после ретраев — даём лимитеру остыть. Серия
      // сплошных 429 значит, что штраф не остывает, — прерываем шаг, чтобы
      // не кормить лимитер: остаток дошлёт следующий запуск media.
      if (Number(error?.statusCode) === 429) {
        consecutive429 += 1;
        if (consecutive429 >= 8) {
          recordStep("media-aborted", { reason: "8 подряд 429 — лимитер WB не остывает", sent, errors: errors.length, lastError: errors[errors.length - 1] });
          break;
        }
        await sleep(120000);
      } else {
        consecutive429 = 0;
      }
    }
    await sleep(pauseMs);
    // Первые ошибки пишем сразу (диагноз лимитера), дальше — каждые 10 карточек.
    if (errors.length <= 3 || (sent + errors.length) % 10 === 0) {
      state.steps.push({ step: "media-progress", at: new Date().toISOString(), sent, errors: errors.length, lastError: errors[errors.length - 1] || null });
      saveState();
    }
  }
  recordStep("media", { cardsWithoutPhoto: withoutPhoto.length, sent, skippedNoImages, errors: errors.slice(0, 20) });
}

async function stepPrices(account) {
  const rules = await server.readWbImportRules();
  const pricing = await server.loadAvitoPricingContext();
  const cards = (await server.wbCardsList(account)).filter((card) => Number(card.nmID) > 0);
  const linked = await server.loadWbLinkedOzonProducts(cards.map((card) => card.vendorCode));
  const items = [];
  let skippedBelowMin = 0;
  let skippedNoSupplier = 0;
  let skippedNotLinked = 0;
  for (const card of cards) {
    const product = linked.get(clean(card.vendorCode).toLowerCase());
    if (!product) { skippedNotLinked += 1; continue; }
    const purchaseRub = server.wbSupplierPurchaseRub(product.supplier, pricing);
    if (!(purchaseRub > 0)) { skippedNoSupplier += 1; continue; }
    if (purchaseRub < rules.minSupplierPriceRub) { skippedBelowMin += 1; continue; }
    const priceRub = server.wbSupplierPriceRub(product.supplier, pricing);
    if (priceRub > 0) items.push({ nmID: card.nmID, price: priceRub, discount: 0 });
  }
  const result = items.length ? await server.wbSetPrices(account, items) : { ok: true, tasks: [] };
  recordStep("prices", {
    ok: result.ok,
    cards: cards.length,
    prepared: items.length,
    skippedBelowMin,
    skippedNoSupplier,
    skippedNotLinked,
    minSupplierPriceRub: rules.minSupplierPriceRub,
    tasks: result.tasks || [],
  });
}

async function stepStocks(account, warehouseId) {
  if (!warehouseId) throw new Error("stocks: не задан warehouseId (аргумент или campaignId кабинета)");
  const rules = await server.readWbImportRules();
  const pricing = await server.loadAvitoPricingContext();
  const cards = await server.wbCardsList(account);
  const linked = await server.loadWbLinkedOzonProducts(cards.map((card) => card.vendorCode));
  const stocks = [];
  let inStock = 0;
  let zeroed = 0;
  let skippedManual = 0;
  for (const card of cards) {
    const skus = (Array.isArray(card.sizes) ? card.sizes : []).flatMap((size) => (Array.isArray(size.skus) ? size.skus : []));
    if (!skus.length) continue;
    const product = linked.get(clean(card.vendorCode).toLowerCase());
    if (!product) { skippedManual += 1; continue; } // ручные карточки не трогаем
    const purchaseRub = server.wbSupplierPurchaseRub(product.supplier, pricing);
    const sellable = !product.archived && purchaseRub >= rules.minSupplierPriceRub;
    const amount = sellable ? rules.defaultStock : 0;
    if (sellable) inStock += 1; else zeroed += 1;
    for (const sku of skus) stocks.push({ sku, amount });
  }
  const result = stocks.length ? await server.wbUpdateStocks(account, warehouseId, stocks) : { ok: true, sent: 0 };
  recordStep("stocks", {
    ok: result.ok,
    warehouseId,
    cards: cards.length,
    inStock,
    zeroed,
    skippedManual,
    sent: result.sent,
    defaultStock: rules.defaultStock,
    minSupplierPriceRub: rules.minSupplierPriceRub,
  });
}

async function main() {
  saveState(); // сразу фиксируем старт — наблюдаемость через /api/wb/chain/result
  const account = server.getWbAccountByTarget("wb");
  if (!account) throw new Error("Кабинет WB не настроен");
  // Склад FBS «Opt» по умолчанию (id 1048198, deliveryType 1).
  const defaultWarehouseId = Number(process.argv[3] || account.campaignId || 0) || 1048198;

  if (mode === "chain") {
    const evaluated = await stepPreview();
    await stepApply(account, evaluated, 20000);
    await stepWaitCards(account);
    // Отчёт об ошибках карточек — информационный, не валим цепочку из-за него.
    try {
      await stepErrors(account);
    } catch (error) {
      recordStep("card-errors", { error: error?.message || String(error) });
    }
    // ТН ВЭД — тоже не блокирует фото/цены/остатки.
    try {
      await stepTnved(account);
    } catch (error) {
      recordStep("tnved", { statusCode: error?.statusCode, error: error?.message || String(error) });
    }
    await stepMedia(account, 20000);
    await stepPrices(account);
    await stepStocks(account, defaultWarehouseId);
  } else if (mode === "preview") {
    await stepPreview();
  } else if (mode === "apply") {
    const limit = Math.max(1, Math.min(20000, Number(process.argv[3] || 20000) || 20000));
    const evaluated = await stepPreview();
    await stepApply(account, evaluated, limit);
  } else if (mode === "errors") {
    await stepErrors(account);
  } else if (mode === "tnved") {
    await stepTnved(account);
  } else if (mode === "diag") {
    // Полная диагностика падающих вызовов content-API: статус и тело ошибки.
    try {
      const errors = await server.wbCardErrors(account);
      recordStep("diag-card-errors", { ok: true, total: errors.length });
    } catch (error) {
      recordStep("diag-card-errors", { statusCode: error?.statusCode, wb: error?.wb, message: error?.message });
    }
    const cards = await server.wbCardsList(account);
    const target = cards.find((card) => Number(card.nmID) > 0 && !(Array.isArray(card.photos) && card.photos.length));
    if (target) {
      const linked = await server.loadWbLinkedOzonProducts([target.vendorCode]);
      const product = linked.get(clean(target.vendorCode).toLowerCase());
      let imageUrls = [];
      if (product) {
        const prisma = server.getPrisma();
        const row = await prisma.warehouseProduct.findUnique({ where: { id: product.id }, select: { raw: true } });
        const normalized = row?.raw && typeof row.raw === "object" ? server.normalizeWarehouseProduct(row.raw) : null;
        if (normalized) imageUrls = server.wbExtractImageUrls(normalized);
      }
      try {
        const result = await server.wbMediaSave(account, target.nmID, imageUrls.slice(0, 10));
        recordStep("diag-media-save", { nmID: target.nmID, vendorCode: target.vendorCode, urls: imageUrls.slice(0, 3), ...result });
      } catch (error) {
        recordStep("diag-media-save", {
          nmID: target.nmID,
          vendorCode: target.vendorCode,
          urls: imageUrls.slice(0, 3),
          statusCode: error?.statusCode,
          wb: error?.wb,
          message: error?.message,
        });
      }
    } else {
      recordStep("diag-media-save", { note: "нет карточек без фото" });
    }
  } else if (mode === "cards") {
    const cards = await server.wbCardsList(account);
    recordStep("cards", {
      total: cards.length,
      sample: cards.slice(0, Number(process.argv[3] || 40) || 40).map((card) => ({
        nmID: card.nmID,
        vendorCode: card.vendorCode,
        photos: Array.isArray(card.photos) ? card.photos.length : 0,
        title: clean(card.title).slice(0, 50),
      })),
    });
  } else if (mode === "media") {
    await stepMedia(account, Math.max(1, Math.min(20000, Number(process.argv[3] || 20000) || 20000)));
  } else if (mode === "prices") {
    await stepPrices(account);
  } else if (mode === "stocks") {
    await stepStocks(account, defaultWarehouseId);
  } else {
    throw new Error(`unknown mode: ${mode}`);
  }

  state.finishedAt = new Date().toISOString();
  saveState();
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    state.error = error?.stack || error?.message || String(error);
    state.finishedAt = new Date().toISOString();
    saveState();
    console.error(state.error);
    process.exit(1);
  });
