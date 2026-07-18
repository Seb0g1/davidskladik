#!/usr/bin/env node
"use strict";

// Цепочка Wildberries НА СЕРВЕРЕ отдельным процессом с большим heap:
//   cd /var/www/davidsklad/davidskladik && \
//   MALLOC_ARENA_MAX=2 NODE_OPTIONS='--max-old-space-size=4096' \
//   node scripts/prod-wb-chain.cjs <chain|chain-nomedia|preview|apply|cards|errors|media|prices|stocks|trash-recover> [args]
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

// Глобальный лимитер WB штрафует кабинет на минуты, а wbRequest при 429 с
// retry > 60 с кидает сразу (не кормит штраф) — шаг цепочки пережидает штраф
// сам и повторяет попытку. Без этого ночной прогон умирал на prices сразу
// после тяжёлого apply (17.07 03:42), и цены новых карточек ждали автосинка.
// Финальный отказ пишется как шаг с ошибкой, цепочка идёт дальше.
async function runStepWaiting429(stepName, fn, { attempts = 3, maxWaitMs = 30 * 60 * 1000 } = {}) {
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      const statusCode = Number(error?.statusCode) || 0;
      const retrySec = Number(error?.retryAfterSec) || 0;
      if (statusCode === 429 && attempt < attempts) {
        const waitMs = Math.min(maxWaitMs, Math.max(retrySec * 1000 + 5000, 120000));
        state.steps.push({ step: `${stepName}-waiting429`, at: new Date().toISOString(), attempt, retrySec, waitSec: Math.round(waitMs / 1000) });
        saveState();
        console.log(`${stepName}: 429, жду ${Math.round(waitMs / 1000)}с (retry=${retrySec}с, попытка ${attempt}/${attempts})`);
        await sleep(waitMs);
        continue;
      }
      recordStep(stepName, {
        statusCode: statusCode || undefined,
        retrySec: retrySec || undefined,
        error: error?.message || String(error),
      });
      return null;
    }
  }
  return null;
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

// Бинарный поиск vendorCode с «used in other cards»: загружает всё что можно,
// пропускает только те листинги, у которых коллизия vendorCode в кабинете WB.
async function uploadBisect(account, listings, tnvedCharc, bisectResult) {
  if (!listings.length) return;
  try {
    await server.wbCardsUpload(account, listings.map((l) => server.buildWbCardPayload(l, tnvedCharc)));
    bisectResult.uploaded += listings.length;
  } catch (error) {
    const statusCode = Number(error?.statusCode) || 0;
    const wbText = clean(error?.wb?.errorText || error?.message || "");
    if (statusCode === 400 && /used in other cards/i.test(wbText) && listings.length > 1) {
      const mid = Math.ceil(listings.length / 2);
      await uploadBisect(account, listings.slice(0, mid), tnvedCharc, bisectResult);
      await uploadBisect(account, listings.slice(mid), tnvedCharc, bisectResult);
      return;
    }
    if (statusCode === 400 && /used in other cards/i.test(wbText) && listings.length === 1) {
      bisectResult.skippedVendorCodes.push(listings[0].vendorCode);
      return;
    }
    throw error;
  }
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
  // Карточки в корзине WB держат vendorCode занятым: upload с таким артикулом
  // падает «vendor code is used in other cards» и валит весь чанк из 50 —
  // корзину исключаем из кандидатов (восстановление — режим trash-recover).
  let trashVendorCodes = new Set();
  try {
    trashVendorCodes = new Set((await server.wbCardsTrashList(account))
      .map((card) => clean(card.vendorCode).toLowerCase())
      .filter(Boolean));
  } catch (error) {
    console.error("trash list failed:", error?.message || String(error));
  }
  let skippedInTrash = 0;
  let skippedDuplicate = 0;
  const seenVendorCodes = new Set();
  const candidates = evaluated
    .filter(({ result }) => {
      if (!result.ok) return false;
      const code = result.listing.vendorCode.toLowerCase();
      if (existingVendorCodes.has(code)) return false;
      if (trashVendorCodes.has(code)) { skippedInTrash += 1; return false; }
      // Дубли vendorCode внутри самого каталога тоже валят чанк целиком.
      if (seenVendorCodes.has(code)) { skippedDuplicate += 1; return false; }
      seenVendorCodes.add(code);
      return true;
    })
    .slice(0, limit)
    .map(({ result }) => result.listing);
  const withoutBarcode = candidates.filter((listing) => !listing.barcode);
  if (withoutBarcode.length) {
    const generated = await server.wbGenerateBarcodes(account, withoutBarcode.length);
    withoutBarcode.forEach((listing, index) => { listing.barcode = generated[index] || ""; });
  }
  const ready = candidates.filter((listing) => listing.barcode);
  // Лимитер WB на cards/upload — тот же Token Bucket, что у media/save: залп
  // без пауз (прогон 16.07: 20 чанков подряд) сжигает burst, дальше сплошные
  // 429 и штраф не остывает, пока запросы продолжаются. Поэтому базовая пауза
  // между чанками + на 429 ждём X-Ratelimit-Retry и повторяем ТОТ ЖЕ чанк.
  const pauseMs = Math.max(1000, Number(process.env.WB_UPLOAD_PAUSE_MS || 6000) || 6000);
  const chunks = server.chunkArray(ready, 50);
  let uploaded = 0;
  let aborted = false;
  let abortReason = "429 не остывает после 5 попыток";
  const errors = [];
  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let done = false;
    for (let attempt = 1; attempt <= 5 && !done; attempt += 1) {
      try {
        await server.wbCardsUpload(account, chunk.map((listing) => server.buildWbCardPayload(listing, tnvedCharc)));
        uploaded += chunk.length;
        done = true;
        console.log(`uploaded chunk ${index + 1}/${chunks.length}: +${chunk.length} (total ${uploaded})`);
      } catch (error) {
        const statusCode = Number(error?.statusCode) || 0;
        const retrySec = Number(error?.retryAfterSec) || 0;
        const wbText = clean(error?.wb?.errorText || error?.message || "");
        // Дневной лимит WB — 1000 новых карточек/сутки («You have already used
        // up your daily limit — 1000. Try tomorrow»): дальше сегодня бить
        // бессмысленно, остаток дошлёт завтрашний запуск.
        if (/daily limit|Try tomorrow/i.test(wbText)) {
          errors.push({ vendorCodes: chunk.map((listing) => listing.vendorCode).slice(0, 5), statusCode, error: wbText });
          abortReason = "дневной лимит WB на новые карточки (1000/сутки) — остаток дошлёт завтрашний запуск";
          aborted = true;
          done = true;
          break;
        }
        // «Used in other cards» — vendorCode занят другой карточкой кабинета.
        // Находим проблемный артикул бисектом (загружаем по одному), остальные
        // в чанке прокидываем нормально.
        if (statusCode === 400 && /used in other cards/i.test(wbText)) {
          const bisectResult = { uploaded: 0, skippedVendorCodes: [] };
          try {
            await uploadBisect(account, chunk, tnvedCharc, bisectResult);
          } catch (bisectError) {
            // bisect встретил 429 или другую ошибку — обрабатываем ниже
            errors.push({ vendorCodes: chunk.map((l) => l.vendorCode).slice(0, 5), statusCode: Number(bisectError?.statusCode) || 0, error: bisectError?.message || String(bisectError) });
            done = true;
            break;
          }
          uploaded += bisectResult.uploaded;
          if (bisectResult.skippedVendorCodes.length) {
            errors.push({ vendorCodes: bisectResult.skippedVendorCodes, statusCode: 400, error: "used in other cards — vendorCode занят, пропускаем" });
            console.warn(`bisect: пропущено ${bisectResult.skippedVendorCodes.length} vendorCode с коллизией:`, bisectResult.skippedVendorCodes.join(", "));
          }
          console.log(`uploaded chunk ${index + 1}/${chunks.length} via bisect: +${bisectResult.uploaded} (skipped ${bisectResult.skippedVendorCodes.length}, total ${uploaded})`);
          done = true;
          break;
        }
        if (statusCode === 429 && attempt < 5) {
          // Ждём, сколько просит лимитер (не меньше 2 мин — остывание), и
          // повторяем чанк: карточки не теряем.
          const waitMs = Math.min(900000, Math.max(retrySec * 1000 + 1000, 120000));
          state.steps.push({ step: "apply-progress", at: new Date().toISOString(), chunk: index + 1, of: chunks.length, uploaded, waiting429Sec: Math.round(waitMs / 1000), retrySec });
          saveState();
          await sleep(waitMs);
          continue;
        }
        errors.push({
          vendorCodes: chunk.map((listing) => listing.vendorCode).slice(0, 5),
          statusCode,
          retrySec,
          error: error?.message || String(error),
          wb: error?.wb,
        });
        console.error(`chunk ${index + 1}/${chunks.length} failed: status=${statusCode} ${error?.message || error}`, JSON.stringify(error?.wb || null)?.slice(0, 500));
        // Первые ошибки — сразу в state (диагноз без ожидания конца шага).
        if (errors.length <= 3) {
          state.steps.push({ step: "apply-progress", at: new Date().toISOString(), chunk: index + 1, of: chunks.length, uploaded, lastError: errors[errors.length - 1] });
          saveState();
        }
        done = true;
        // Чанк не прошёл даже после долгих ожиданий 429 — лимитер не остывает,
        // дальнейшие чанки только продлят штраф. Остаток дошлёт следующий запуск.
        if (statusCode === 429) {
          aborted = true;
        }
      }
    }
    if (aborted) {
      state.steps.push({ step: "apply-aborted", at: new Date().toISOString(), reason: abortReason, uploadedSoFar: uploaded, remainingChunks: chunks.length - index - 1 });
      saveState();
      break;
    }
    if ((index + 1) % 10 === 0) {
      state.steps.push({ step: "apply-progress", at: new Date().toISOString(), chunk: index + 1, of: chunks.length, uploaded, errors: errors.length, lastError: errors[errors.length - 1] || null });
      saveState();
    }
    if (index + 1 < chunks.length) await sleep(pauseMs);
  }
  recordStep("apply", {
    candidates: candidates.length,
    alreadyOnWb: existingVendorCodes.size,
    uploaded,
    aborted,
    tnved: tnvedCharc ? tnvedCharc.code : null,
    skippedNoBarcode: candidates.length - ready.length,
    skippedInTrash,
    skippedDuplicate,
    errors: errors.slice(0, 20),
  });
  return uploaded;
}

// Дозаполнение ТН ВЭД в уже созданных карточках (cards/update).
async function stepTnved(account) {
  const result = await server.backfillWbTnvedCharacteristics(account, {});
  recordStep("tnved", result);
}

// Обогащение карточек данными Ozon: бренд, описание, объём, артикул Ozon.
async function stepEnrich(account) {
  const result = await server.enrichWbCards(account, {});
  recordStep("enrich", { ...result, errors: (result.errors || []).slice(0, 10) });
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
  let skippedAboveMax = 0;
  let skippedNoSupplier = 0;
  let skippedNotLinked = 0;
  for (const card of cards) {
    const product = linked.get(clean(card.vendorCode).toLowerCase());
    if (!product) { skippedNotLinked += 1; continue; }
    const purchaseRub = server.wbSupplierPurchaseRub(product.supplier, pricing);
    if (!(purchaseRub > 0)) { skippedNoSupplier += 1; continue; }
    if (rules.minSupplierPriceRub > 0 && purchaseRub < rules.minSupplierPriceRub) { skippedBelowMin += 1; continue; }
    const priceRub = server.wbSupplierPriceRub(product.supplier, pricing);
    if (rules.maxWbPriceRub > 0 && priceRub > rules.maxWbPriceRub) { skippedAboveMax += 1; continue; }
    if (priceRub > 0) items.push({ nmID: card.nmID, price: priceRub, discount: 0 });
  }
  const result = items.length ? await server.wbSetPrices(account, items) : { ok: true, tasks: [] };
  recordStep("prices", {
    ok: result.ok,
    cards: cards.length,
    prepared: items.length,
    skippedBelowMin,
    skippedAboveMax,
    skippedNoSupplier,
    skippedNotLinked,
    minSupplierPriceRub: rules.minSupplierPriceRub,
    maxWbPriceRub: rules.maxWbPriceRub,
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
    const priceRub = purchaseRub > 0 ? server.wbSupplierPriceRub(product.supplier, pricing) : 0;
    const sellable = server.wbCardSellable({ product, purchaseRub, priceRub, rules });
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
    maxWbPriceRub: rules.maxWbPriceRub,
  });
}

async function main() {
  saveState(); // сразу фиксируем старт — наблюдаемость через /api/wb/chain/result
  const account = server.getWbAccountByTarget("wb");
  if (!account) throw new Error("Кабинет WB не настроен");
  // Склад FBS «Opt» по умолчанию (id 1048198, deliveryType 1).
  const defaultWarehouseId = Number(process.argv[3] || account.campaignId || 0) || 1048198;

  if (mode === "chain-nomedia") {
    // Полная цепочка без шага media: фото ведёт фоновый шедулер
    // wb-media-backfill на worker (квота WB ~1 фото/15 мин).
    // Все шаги после apply — через runStepWaiting429: отчётные (errors/tnved/
    // enrich) не валят цепочку, а prices/stocks пережидают штраф лимитера.
    const evaluated = await stepPreview();
    await stepApply(account, evaluated, 20000);
    await stepWaitCards(account);
    await runStepWaiting429("card-errors", () => stepErrors(account));
    await runStepWaiting429("tnved", () => stepTnved(account));
    await runStepWaiting429("enrich", () => stepEnrich(account));
    // stocks идёт до prices: цены и остатки на разных лимитерах WB — stocks после
    // enrich даёт лимитеру цен ~1–2 мин на остывание (logs 17.07: stocks ok за 40 с
    // после того как prices дал up на попытке 3; 4-я попытка прошла бы).
    await runStepWaiting429("stocks", () => stepStocks(account, defaultWarehouseId));
    await runStepWaiting429("prices", () => stepPrices(account), { attempts: 15, maxWaitMs: 3 * 60 * 60 * 1000 });
  } else if (mode === "chain") {
    const evaluated = await stepPreview();
    await stepApply(account, evaluated, 20000);
    await stepWaitCards(account);
    await runStepWaiting429("card-errors", () => stepErrors(account));
    await runStepWaiting429("tnved", () => stepTnved(account));
    await runStepWaiting429("enrich", () => stepEnrich(account));
    await runStepWaiting429("stocks", () => stepStocks(account, defaultWarehouseId));
    await runStepWaiting429("prices", () => stepPrices(account), { attempts: 15, maxWaitMs: 3 * 60 * 60 * 1000 });
    // Фото — последними: квота WB media/save крошечная, шаг может идти часами
    // (основную догрузку ведёт фоновый шедулер wb-media-backfill на worker).
    await stepMedia(account, 20000);
  } else if (mode === "trash-recover") {
    // Восстанавливает из корзины WB карточки, которые по текущим правилам
    // должны продаваться (vendorCode среди OK-кандидатов импорта). Их артикулы
    // заняты корзиной и валили upload («vendor code is used in other cards»),
    // а после восстановления карточки живут обычной жизнью: цены/остатки
    // подхватит автосинк, фото — media-backfill.
    const { evaluated } = await server.collectWbImportCandidates({});
    const wanted = new Set(evaluated
      .filter(({ result }) => result.ok)
      .map(({ result }) => result.listing.vendorCode.toLowerCase()));
    const trash = await server.wbCardsTrashList(account);
    const matches = trash.filter((card) => wanted.has(clean(card.vendorCode).toLowerCase()));
    recordStep("trash-scan", {
      trashTotal: trash.length,
      matchedCandidates: matches.length,
      sample: matches.slice(0, 20).map((card) => ({ nmID: card.nmID, vendorCode: card.vendorCode, title: clean(card.title).slice(0, 60) })),
    });
    let recovered = 0;
    for (const chunk of server.chunkArray(matches.map((card) => card.nmID), 1000)) {
      const result = await server.wbCardsRecover(account, chunk);
      recovered += result.recovered || 0;
    }
    recordStep("trash-recover", { recovered });
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
  } else if (mode === "enrich") {
    await stepEnrich(account);
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
