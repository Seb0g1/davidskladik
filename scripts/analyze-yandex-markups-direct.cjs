#!/usr/bin/env node
"use strict";

/**
 * Standalone: анализирует исторические наценки Яндекса.
 * Подключается напрямую к PostgreSQL (Prisma) и MySQL PM.
 * Запускать на сервере: node scripts/analyze-yandex-markups-direct.cjs
 */

const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const { PrismaClient } = require("@prisma/client");
const mysql = require("mysql2/promise");

const prisma = new PrismaClient();

const pool = mysql.createPool({
  host: process.env.PM_DB_HOST,
  port: Number(process.env.PM_DB_PORT || 3306),
  user: process.env.PM_DB_USER,
  password: process.env.PM_DB_PASSWORD,
  database: process.env.PM_DB_NAME,
  waitForConnections: true,
  connectionLimit: 3,
  charset: "utf8mb4",
});

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function main() {
  const usdRate = Number(process.env.DEFAULT_USD_RATE || 95) || 95;
  console.log(`USD rate: ${usdRate} (из DEFAULT_USD_RATE)\n`);

  // 1. Все Яндекс-товары
  const productRows = await prisma.$queryRawUnsafe(`
    SELECT id, offer_id AS "offerId"
    FROM warehouse_products
    WHERE marketplace = 'yandex' AND archived = false
  `);
  console.log(`Яндекс-товаров: ${productRows.length}`);

  const productIds = productRows.map((r) => String(r.id));

  // 2. PM-артикулы из product_links
  const linkRows = await prisma.$queryRawUnsafe(`
    SELECT product_id AS "productId", supplier_article AS article
    FROM product_links
    WHERE product_id = ANY($1) AND supplier_article IS NOT NULL AND supplier_article != ''
  `, productIds);

  const articlesByProduct = new Map();
  for (const l of linkRows) {
    const pid = String(l.productId);
    if (!articlesByProduct.has(pid)) articlesByProduct.set(pid, []);
    articlesByProduct.get(pid).push(String(l.article).trim());
  }

  const withLinks = productRows.filter((r) => articlesByProduct.has(String(r.id)));
  console.log(`С PM-привязкой: ${withLinks.length}`);

  // 3. Последняя успешная Яндекс-цена из price_history
  const historyRows = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT ON (product_id) product_id, new_price
    FROM price_history
    WHERE product_id = ANY($1)
      AND marketplace = 'yandex'
      AND status = 'success'
    ORDER BY product_id, created_at DESC
  `, withLinks.map((r) => String(r.id)));

  const lastPriceById = new Map(historyRows.map((r) => [String(r.product_id), Number(r.new_price)]));
  console.log(`С историей цен: ${lastPriceById.size}`);

  // 4. PM цены из живого MySQL
  const allArticles = Array.from(new Set(Array.from(articlesByProduct.values()).flat().filter(Boolean)));
  console.log(`Уникальных PM артикулов: ${allArticles.length}`);
  const pmPriceByArticle = new Map();
  for (const batch of chunkArray(allArticles, 500)) {
    const placeholders = batch.map(() => "?").join(",");
    const [rows] = await pool.query(
      `SELECT BINARY TRIM(r.NativeID) AS article, MIN(r.NativePrice) AS price
       FROM OfferRows r
       WHERE BINARY TRIM(r.NativeID) IN (${placeholders}) AND r.Ignored = 0
       GROUP BY BINARY TRIM(r.NativeID)`,
      batch,
    );
    for (const row of rows) {
      const key = String(row.article || "").trim();
      if (key && Number(row.price) > 0 && !pmPriceByArticle.has(key)) {
        pmPriceByArticle.set(key, Number(row.price));
      }
    }
  }
  console.log(`PM цен получено: ${pmPriceByArticle.size}\n`);

  // 5. Вычисляем наценки
  const updates = [];
  let noHistory = 0, noPm = 0, outOfRange = 0;

  for (const product of withLinks) {
    const pid = String(product.id);
    const lastPrice = lastPriceById.get(pid);
    if (!lastPrice || lastPrice <= 0) { noHistory++; continue; }

    const articles = articlesByProduct.get(pid) || [];
    let bestPmUsd = null;
    for (const art of articles) {
      const p = pmPriceByArticle.get(art);
      if (p && p > 0 && (bestPmUsd === null || p < bestPmUsd)) bestPmUsd = p;
    }
    if (!bestPmUsd) { noPm++; continue; }

    const pmRub = bestPmUsd * usdRate;
    const markup = lastPrice / pmRub;
    if (!Number.isFinite(markup) || markup < 1.0 || markup > 6.0) { outOfRange++; continue; }

    updates.push({ offerId: String(product.offerId || ""), pmUsdPrice: bestPmUsd, lastPrice, markup });
  }

  console.log(`Найдено пар для анализа: ${updates.length}`);
  console.log(`  Пропущено — нет истории: ${noHistory}, нет PM цены: ${noPm}, вне диапазона 1–6×: ${outOfRange}`);

  // 6. Тиры
  const tierBoundaries = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 120, 150, 200, 300, 500];

  // Текущие Ozon-правила (= текущие Яндекс-правила, они идентичны)
  const ozonRules = [
    { minUsd: 0, coeff: 16.3758 },
    { minUsd: 50, coeff: 3.0772 },
    { minUsd: 60, coeff: 2.9408 },
    { minUsd: 70, coeff: 2.8 },
    { minUsd: 80, coeff: 2.7 },
    { minUsd: 90, coeff: 2.66 },
    { minUsd: 100, coeff: 2.58 },
    { minUsd: 120, coeff: 2.58 },
    { minUsd: 150, coeff: 2.58 },
    { minUsd: 200, coeff: 2.5 },
    { minUsd: 300, coeff: 2.5 },
    { minUsd: 500, coeff: 2.5 },
  ];
  const getOzon = (usd) => [...ozonRules].reverse().find((r) => usd >= r.minUsd)?.coeff ?? null;

  console.log(`\n${"Тир ($)".padEnd(12)} ${"N".padStart(5)} ${"median".padStart(8)} ${"avg".padStart(8)} ${"min".padStart(7)} ${"max".padStart(7)} ${"Ozon=ЯМ".padStart(9)} ${"Δ%".padStart(6)}`);
  console.log("─".repeat(66));

  const meaningful = [];
  for (let i = 0; i < tierBoundaries.length; i++) {
    const minUsd = tierBoundaries[i];
    const maxUsd = tierBoundaries[i + 1] ?? Infinity;
    const inTier = updates.filter((u) => u.pmUsdPrice >= minUsd && u.pmUsdPrice < maxUsd);
    if (!inTier.length) continue;

    const markups = inTier.map((u) => u.markup).sort((a, b) => a - b);
    const avg = markups.reduce((s, m) => s + m, 0) / markups.length;
    const median = markups[Math.floor(markups.length / 2)];
    const ozon = getOzon(minUsd);
    const delta = ozon ? Math.round(((median - ozon) / ozon) * 100) : null;
    const deltaStr = delta !== null ? `${delta > 0 ? "+" : ""}${delta}%` : "—";

    const label = Number.isFinite(maxUsd) ? `$${minUsd}–$${maxUsd}` : `$${minUsd}+`;
    console.log(
      `${label.padEnd(12)} ${String(inTier.length).padStart(5)} ${String(median.toFixed(3)).padStart(8)} ${String(avg.toFixed(3)).padStart(8)} ${String(markups[0].toFixed(2)).padStart(7)} ${String(markups[markups.length - 1].toFixed(2)).padStart(7)} ${ozon !== null ? String(ozon).padStart(9) : "".padStart(9)} ${deltaStr.padStart(6)}`,
    );
    if (inTier.length >= 3) {
      meaningful.push({ minUsd, count: inTier.length, median: Math.round(median * 10000) / 10000, avg: Math.round(avg * 10000) / 10000, ozon });
    }
  }

  console.log(`\n=== ПРЕДЛАГАЕМЫЕ ЯНДЕКС-ПРАВИЛА (на основе median из истории цен) ===`);
  console.log("(Только тиры с ≥3 товарами)\n");
  for (const r of meaningful) {
    const flag = r.ozon && Math.abs(r.median - r.ozon) > 0.05 ? " ← ОТЛИЧАЕТСЯ от Озон" : "";
    console.log(`  { minUsd: ${String(r.minUsd).padEnd(4)}, coefficient: ${r.median} },  // n=${r.count}, avg=${r.avg}${flag}`);
  }

  console.log(`\n=== ТОП-20 САМЫХ ДОРОГИХ ТОВАРОВ (для проверки) ===`);
  const topByPrice = [...updates].sort((a, b) => b.lastPrice - a.lastPrice).slice(0, 20);
  for (const u of topByPrice) {
    console.log(`  ${u.offerId}: pm=$${u.pmUsdPrice.toFixed(2)}, lastPrice=${u.lastPrice}₽, markup=${u.markup.toFixed(3)}`);
  }
}

main()
  .catch((e) => { console.error(e.message || String(e)); process.exit(1); })
  .finally(async () => {
    await prisma.$disconnect().catch(() => {});
    await pool.end().catch(() => {});
  });
