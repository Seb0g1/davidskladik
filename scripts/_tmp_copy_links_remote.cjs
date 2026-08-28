#!/usr/bin/env node
"use strict";
/**
 * Копирует ProductLink с Yandex-карточки на Ozon/Avito-карточку для одинаковых offerId.
 *
 * Условие: target-товар (ozon/avito) с 0 привязок, Yandex-товар с тем же offerId
 *          имеет хотя бы одну привязку.
 *
 * Запуск:
 *   node scripts/_tmp_copy_links_remote.cjs          -- dry run (только подсчёт)
 *   node scripts/_tmp_copy_links_remote.cjs --apply  -- реальная запись
 */
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const DRY_RUN = !process.argv.includes("--apply");

async function main() {
  const prisma = new PrismaClient();
  try {
    console.log(DRY_RUN ? "=== DRY RUN (передай --apply чтобы записать) ===" : "=== APPLY MODE ===");

    // Одним запросом: найти пары (target_id, yandex_link) для копирования
    // — target marketplace = ozon или avito, 0 ссылок у target-товара
    // — yandex-товар с тем же offerId имеет ссылки
    const pairs = await prisma.$queryRawUnsafe(`
      WITH yandex_linked AS (
        -- Yandex-продукты у которых есть хотя бы одна ссылка
        SELECT wp.id AS yandex_id, wp.raw->>'offerId' AS offer_id
        FROM warehouse_products wp
        WHERE wp.marketplace = 'yandex'
          AND EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = wp.id)
      ),
      targets AS (
        -- Ozon/Avito продукты без ссылок с совпадающим offerId
        SELECT wp.id AS target_id, wp.marketplace AS target_mp, wp.raw->>'offerId' AS offer_id
        FROM warehouse_products wp
        WHERE wp.marketplace = 'ozon'
          AND NOT EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = wp.id)
      )
      SELECT
        t.target_id,
        t.target_mp,
        t.offer_id,
        pl.id            AS src_link_id,
        pl.supplier_article,
        pl.supplier_name,
        pl.partner_id,
        pl.price_currency,
        pl.keyword,
        pl.raw
      FROM targets t
      JOIN yandex_linked yl ON yl.offer_id = t.offer_id
      JOIN product_links pl ON pl.product_id = yl.yandex_id
      ORDER BY t.offer_id, t.target_mp
    `);

    if (!pairs.length) {
      console.log("\nНет пар для копирования. Всё уже привязано.");
      return;
    }

    // Подсчёт
    const byTarget = new Map();
    for (const row of pairs) {
      if (!byTarget.has(row.target_id)) byTarget.set(row.target_id, { mp: row.target_mp, offerId: row.offer_id, links: [] });
      byTarget.get(row.target_id).links.push(row);
    }

    console.log(`\nМожно исправить:`);
    console.log(`  Ozon-продуктов: ${byTarget.size}  (${pairs.length} ссылок)`);

    console.log("\nПримеры (первые 10):");
    for (const [targetId, v] of [...byTarget.entries()].slice(0, 10)) {
      console.log(`  [${v.mp}] ${v.offerId}: ${v.links.length} ссылок (${v.links.map((l) => l.supplier_article).slice(0, 3).join(", ")})`);
    }

    if (DRY_RUN) {
      console.log("\nDry run — ничего не изменено. Запусти с --apply для записи.");
      return;
    }

    // Apply: INSERT батчами по 200
    let inserted = 0;
    let skipped = 0;
    const allRows = [...pairs];
    const BATCH = 200;
    for (let i = 0; i < allRows.length; i += BATCH) {
      const batch = allRows.slice(i, i + BATCH);
      for (const row of batch) {
        try {
          await prisma.productLink.create({
            data: {
              productId: row.target_id,
              supplierArticle: row.supplier_article,
              supplierName: row.supplier_name ?? null,
              partnerId: row.partner_id ?? null,
              priceCurrency: (row.price_currency === "RUB" || row.price_currency === "RUR") ? "RUB" : "USD",
              keyword: row.keyword ?? null,
              raw: row.raw ?? null,
            },
          });
          inserted++;
        } catch (err) {
          if (err.code === "P2002") { skipped++; } // unique constraint — уже есть
          else throw err;
        }
      }
      if ((i + BATCH) % 1000 === 0 || i + BATCH >= allRows.length) {
        console.log(`  Прогресс: ${Math.min(i + BATCH, allRows.length)}/${allRows.length} (добавлено ${inserted}, дублей ${skipped})`);
      }
    }

    console.log(`\nГотово: добавлено ${inserted} ссылок, пропущено дублей ${skipped}.`);
    console.log("Перезапусти импорт склада (POST /api/warehouse/import/run) чтобы обновить targetStock и цены.");

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
