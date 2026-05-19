#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const { chromium } = require("playwright");
require("dotenv").config();

const baseUrl = (process.env.SMOKE_BASE_URL || "http://127.0.0.1:3000").replace(/\/+$/, "");
const username = process.env.SMOKE_USER || process.env.APP_USER || "admin";
const password = process.env.SMOKE_PASSWORD || process.env.APP_PASSWORD || "";
const skuList = (process.env.SMOKE_SKUS || "41059,41044,CC-AASH5001,НФ-00004538,НФ-000045377")
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);

async function requestJson(response, label) {
  if (!response.ok()) {
    throw new Error(`${label} failed: HTTP ${response.status()} ${await response.text().catch(() => "")}`);
  }
  return response.json();
}

async function pageJson(page, url, options = {}) {
  return page.evaluate(async ({ url, options }) => {
    const response = await fetch(url, {
      credentials: "same-origin",
      ...options,
      headers: {
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...(options.headers || {}),
      },
    });
    const payload = await response.json().catch(() => ({}));
    return { ok: response.ok, status: response.status, payload };
  }, { url, options });
}

async function main() {
  if (!password) {
    throw new Error("Set APP_PASSWORD or SMOKE_PASSWORD before running smoke:ui.");
  }

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    baseURL: baseUrl,
    viewport: { width: 1440, height: 980 },
    permissions: ["clipboard-read", "clipboard-write"],
  });
  const page = await context.newPage();

  try {
    const health = await requestJson(await context.request.get(`${baseUrl}/health`), "health");
    assert.equal(health.ok, true, "health must be ok");

    await page.goto(`${baseUrl}/login.html`, { waitUntil: "domcontentloaded" });
    const loginPayload = await page.evaluate(async ({ username, password }) => {
      const response = await fetch("/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      const payload = await response.json().catch(() => ({}));
      return { ok: response.ok, status: response.status, payload };
    }, { username, password });
    if (!loginPayload.ok) {
      throw new Error(`login failed: HTTP ${loginPayload.status} ${JSON.stringify(loginPayload.payload)}`);
    }

    await page.goto(`${baseUrl}/`, { waitUntil: "domcontentloaded" });
    await page.getByRole("heading", { name: /ДавидСклад/i }).waitFor({ timeout: 15000 });

    await page.goto(`${baseUrl}/legacy`, { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("domcontentloaded");
    assert.ok(await page.locator("body").textContent(), "legacy must render");

    for (const sku of skuList) {
      await page.goto(`${baseUrl}/app/warehouse?q=${encodeURIComponent(sku)}`, { waitUntil: "domcontentloaded" });
      await page.waitForSelector(".product-row", { timeout: 45000 });
      const body = await page.locator("body").textContent();
      assert.ok((body || "").toLowerCase().includes(sku.toLowerCase()), `warehouse search must show ${sku}`);
    }

    const primarySku = skuList[0];
    await page.goto(`${baseUrl}/app/warehouse?q=${encodeURIComponent(primarySku)}`, { waitUntil: "domcontentloaded" });
    await page.waitForSelector(".product-row", { timeout: 45000 });
    const firstRow = page.locator(".product-row").first();
    await firstRow.click();
    await page.locator(".detail-panel").waitFor({ timeout: 10000 });

    const nameCopy = page.locator('button[title="Скопировать название"]').first();
    const articleCopy = page.locator('button[title="Скопировать артикул"]').first();
    if (await nameCopy.count()) {
      await nameCopy.click();
      const copiedName = await page.evaluate(() => navigator.clipboard.readText().catch(() => ""));
      assert.ok(copiedName && !copiedName.includes("?"), "copied name must be readable text");
    }
    if (await articleCopy.count()) {
      await articleCopy.click();
      const copiedArticle = await page.evaluate(() => navigator.clipboard.readText().catch(() => ""));
      assert.ok(copiedArticle.length > 0, "copied article must be non-empty");
    }

    const settingsBefore = await pageJson(page, "/api/settings");
    assert.equal(settingsBefore.ok, true, `settings read failed: HTTP ${settingsBefore.status}`);
    const save = await pageJson(page, "/api/settings", {
      method: "PUT",
      body: JSON.stringify(settingsBefore.payload.settings || {}),
    });
    assert.equal(save.ok, true, `settings save failed: HTTP ${save.status}`);
    assert.equal(save.payload.ok, true, "settings save must be ok");

    await page.goto(`${baseUrl}/app/settings`, { waitUntil: "domcontentloaded" });
    await page.getByText(/Курс USD\/RUB/i).waitFor({ timeout: 10000 });
    const settingsTabs = page.locator(".settings-tabs button");
    await settingsTabs.nth(3).click();
    await page.getByText(/Сотрудники и роли/i).waitFor({ timeout: 10000 });
    await settingsTabs.nth(4).click();
    await page.getByText(/Аудит действий/i).waitFor({ timeout: 10000 });
    await settingsTabs.nth(5).click();
    await page.getByText(/Retry queue/i).waitFor({ timeout: 10000 });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(500);
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
    assert.ok(overflow <= 2, `mobile viewport must not overflow horizontally, got ${overflow}px`);

    await page.goto(`${baseUrl}/app/operations`, { waitUntil: "domcontentloaded" });
    await page.getByRole("heading", { name: /Операции/i }).waitFor({ timeout: 10000 });

    await page.goto(`${baseUrl}/app/ai-drafts`, { waitUntil: "domcontentloaded" });
    await page.getByRole("heading", { name: /AI drafts/i }).waitFor({ timeout: 10000 });

    console.log(JSON.stringify({ ok: true, baseUrl, version: health.version || null, checkedSkus: skuList }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
});
