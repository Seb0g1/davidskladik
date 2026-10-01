"use strict";

// Text AI on the DeepSeek proxy (server/parts/02a-text-ai-client.js) and the perfume description
// prompt (02a-ai-content-draft.js). Parts are evaluated standalone in a VM with tiny shims.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const read = (name) => fs.readFileSync(path.join(__dirname, "..", "server", "parts", name), "utf8");
const ctx = vm.createContext({
  process: { env: {} },
  cleanText: (v) => String(v ?? "").trim(),
  openaiTextModel: "gpt-5.4-mini",
  normalizeOpenAiCompatibleBaseUrl: (v) => String(v || "").replace(/\/+$/, ""),
  URL,
});
vm.runInContext(read("02a-openai-client-config.js"), ctx);
vm.runInContext(read("02a-text-ai-client.js"), ctx);
vm.runInContext(read("02a-ai-content-draft.js"), ctx);
vm.runInContext(`this.api = { extractJsonObjectFromText, normalizeParagraphText, formatDescriptionForMarketplace, resolveTextAiProvider,
  effectiveTextAiSettings, buildPerfumeCopyMessages, aiPerfumeTypeFromText, aiPerfumeGenderFromText, isRetryableTextAiError, textAiAuthError };`, ctx);
const ai = ctx.api;
const plain = (value) => JSON.parse(JSON.stringify(value));

test("JSON parsing: ```json wrapper and junk around the object", () => {
  assert.deepEqual(plain(ai.extractJsonObjectFromText('```json\n{"name":"A","bulletPoints":["x"]}\n```')), { name: "A", bulletPoints: ["x"] });
  assert.deepEqual(plain(ai.extractJsonObjectFromText('Вот ответ: {"description":"Текст"} — готово')), { description: "Текст" });
  assert.deepEqual(plain(ai.extractJsonObjectFromText("не json")), {});
});

test("paragraphs survive normalization; spaces inside a paragraph collapse", () => {
  const text = "Первый   абзац\nпродолжение.\n\n\nВторой абзац.  \n \nТретий.";
  assert.equal(ai.normalizeParagraphText(text), "Первый абзац продолжение.\n\nВторой абзац.\n\nТретий.");
  assert.equal(ai.normalizeParagraphText("один\n\nдва\n\nтри", 9), "один\n\nдва");
});

test("marketplace formatting: Ozon <br/>, Market <p>, HTML/plain text untouched", () => {
  assert.equal(ai.formatDescriptionForMarketplace("A & B\n\nC", "ozon"), "A &amp; B<br/><br/>C");
  assert.equal(ai.formatDescriptionForMarketplace("A\n\nC", "yandex"), "<p>A</p><p>C</p>");
  assert.equal(ai.formatDescriptionForMarketplace("<p>A</p>\n<p>B</p>", "yandex"), "<p>A</p>\n<p>B</p>");
  assert.equal(ai.formatDescriptionForMarketplace("Одна строка", "ozon"), "Одна строка");
});

test("provider choice: text provider when its URL+key are set (settings or env), else the image/legacy one", () => {
  const legacy = { baseUrl: "https://codex.sale/v1", textModel: "gpt-5.4-mini", apiKey: "k" };
  assert.equal(ai.resolveTextAiProvider(legacy).kind, "legacy");
  assert.equal(ai.resolveTextAiProvider(legacy).model, "gpt-5.4-mini");
  const viaSettings = ai.resolveTextAiProvider({ ...legacy, textBaseUrl: "https://ai.sebog1.ru/v1", textApiKey: "sk", textProviderModel: "deepseek-reasoner" });
  assert.deepEqual(plain(viaSettings), { kind: "text", baseUrl: "https://ai.sebog1.ru/v1", model: "deepseek-reasoner", fallback: false });
  ctx.process.env.AI_TEXT_BASE_URL = "https://ai.sebog1.ru/v1/";
  ctx.process.env.AI_TEXT_API_KEY = "sk-env";
  const viaEnv = ai.effectiveTextAiSettings(legacy);
  assert.equal(viaEnv.configured, true);
  assert.equal(viaEnv.model, "deepseek-chat");
  assert.equal(viaEnv.baseUrl, "https://ai.sebog1.ru/v1");
  delete ctx.process.env.AI_TEXT_BASE_URL;
  delete ctx.process.env.AI_TEXT_API_KEY;
});

test("errors: 429/5xx/network retry, 401 explains the proxy session", () => {
  assert.equal(ai.isRetryableTextAiError({ status: 429 }), true);
  assert.equal(ai.isRetryableTextAiError({ status: 502 }), true);
  assert.equal(ai.isRetryableTextAiError({ message: "Connection error." }), true);
  assert.equal(ai.isRetryableTextAiError({ status: 400 }), false);
  assert.match(ai.textAiAuthError({ status: 401, message: "session expired" }, "https://ai.sebog1.ru/v1").message, /Слетела авторизация DeepSeek на ai\.sebog1\.ru/);
  assert.match(ai.textAiAuthError({ status: 401, error: { message: "Invalid or missing proxy API key" } }, "https://ai.sebog1.ru/v1").message, /Неверный ключ/);
});

test("description prompt: facts go to the user message, Market forbids «подарок»", () => {
  const source = { brand: "Dior", name: "Dior Sauvage, парфюмерная вода, 100 мл", topNotes: ["Бергамот"], gender: "мужской" };
  const ozon = ai.buildPerfumeCopyMessages(source, "ozon");
  const market = ai.buildPerfumeCopyMessages(source, "yandex");
  assert.equal(ozon.length, 2);
  assert.deepEqual(JSON.parse(ozon[1].content), source);
  assert.match(ozon[0].content, /карточек Ozon/);
  assert.match(ozon[0].content, /1500–2500 знаков/);
  assert.match(ozon[0].content, /как покупка или подарок/);
  assert.match(market[0].content, /карточек Яндекс Маркета/);
  assert.match(market[0].content, /нельзя: «подарок»/);
  assert.doesNotMatch(market[0].content, /«в подарок», сезон/);
  assert.match(market[0].content, /только валидный JSON/);
});

test("type and gender detection from names", () => {
  assert.equal(ai.aiPerfumeTypeFromText("Dior Sauvage EDP 100ml"), "Парфюмерная вода");
  assert.equal(ai.aiPerfumeTypeFromText("C.Dior Sauvage men 60ml edt"), "Туалетная вода");
  assert.equal(ai.aiPerfumeTypeFromText("Chanel No 5 Parfum 7.5ml"), "Духи");
  assert.equal(ai.aiPerfumeGenderFromText("Dior Sauvage men 60ml"), "мужской");
  assert.equal(ai.aiPerfumeGenderFromText("Chanel Coco Mademoiselle w 100ml"), "женский");
  assert.equal(ai.aiPerfumeGenderFromText("Molecule 01 унисекс"), "унисекс");
});
