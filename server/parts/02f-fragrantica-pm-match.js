// «Есть в PriceMaster» для каталога «Фрагрантика» (только worker).
//
// Раз в FRAGRANTICA_PM_MATCH_HOURS (деф. 2) все активные строки PriceMaster (последние прайсы
// поставщиков) раскладываются в индекс слов, и каждый аромат каталога сверяется с ним
// (matchFragranticaPmIndex: бренд вне скобок, все слова названия, без фланкеров, тестеров, лосьонов).
// Результат — fragrantica_perfumes.pm_rows / pm_min_usd / pm_volumes / pm_checked_at.

const fragranticaPmMatchEnabled = process.env.FRAGRANTICA_PM_MATCH_ENABLED !== "false";
const fragranticaPmMatchIntervalMs = Math.max(1, Number(process.env.FRAGRANTICA_PM_MATCH_HOURS || 2) || 2) * 3_600_000;
let fragranticaPmMatchRunning = false;

async function loadFragranticaPmRows() {
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const cte = await pmLatestDocsCteSql();
  const [rows] = await pool.query(
    `${cte}
     SELECT r.NativeID AS article, r.NativeName AS name, r.NativePrice AS price, r.Active AS active, r.RowID AS rowId,
            d.PartnerID AS partnerId, p.PartnerName AS partnerName
     FROM pm_latest_docs ld
     JOIN OfferDocs d ON d.DocID = ld.DocID
     JOIN OfferRows r ON r.DocID = d.DocID
     LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
     WHERE r.Ignored = 0 AND r.Active != 0 AND r.NativePrice > 0`,
  );
  const maps = managedSupplierMaps();
  return rows.map((row) => {
    const mapped = mapPriceMasterSearchResponseRow(row, usdRate, maps);
    const rub = cleanText(mapped.priceCurrency).toUpperCase() === "RUB";
    return { name: mapped.name, usd: rub ? Number(mapped.price) / usdRate : Number(mapped.price) };
  });
}

async function runFragranticaPmMatch() {
  if (fragranticaPmMatchRunning) return { status: "already_running" };
  fragranticaPmMatchRunning = true;
  const startedAt = Date.now();
  try {
    const prisma = await requireFragranticaTables();
    const pmRows = await loadFragranticaPmRows();
    const index = buildFragranticaPmIndex(pmRows);
    let lastId = 0;
    let checked = 0;
    let found = 0;
    for (;;) {
      const perfumes = await prisma.$queryRawUnsafe(
        `SELECT id, brand, name FROM fragrantica_perfumes WHERE id > $1 ORDER BY id LIMIT 2000`,
        lastId,
      );
      if (!perfumes.length) break;
      lastId = perfumes[perfumes.length - 1].id;
      const results = perfumes.map((p) => {
        const m = matchFragranticaPmIndex(index, p);
        if (m.count) found += 1;
        return { id: p.id, rows: m.count, min: m.minUsd === null ? null : Math.round(m.minUsd * 100) / 100, vols: m.volumes };
      });
      checked += perfumes.length;
      await prisma.$executeRawUnsafe(
        `UPDATE fragrantica_perfumes p SET pm_rows = (x->>'rows')::int, pm_min_usd = NULLIF(x->>'min', '')::numeric,
           pm_volumes = x->'vols', pm_checked_at = now()
         FROM jsonb_array_elements($1::jsonb) x WHERE p.id = (x->>'id')::int`,
        JSON.stringify(results),
      );
      // let HTTP / other jobs run between batches
      await new Promise((resolve) => setImmediate(resolve));
    }
    const shops = await runFragranticaShopStockMatch(prisma).catch((error) => ({ status: "error", error: error?.message || String(error) }));
    const result = { status: "ok", pmRows: pmRows.length, checked, found, shops, elapsedMs: Date.now() - startedAt };
    logger.info("fragrantica pm match complete", result);
    await writeFragranticaState("pm_match", { ...result, at: new Date().toISOString() });
    return result;
  } catch (error) {
    logger.warn("fragrantica pm match failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    fragranticaPmMatchRunning = false;
  }
}

/**
 * «Уже есть в магазине» — cards that were on the shops before Fragrantica: every active warehouse product
 * (warehouse_products, per shop = target) is indexed like a PriceMaster row and every catalog perfume is
 * matched against each shop's index (brand + all name words, no flankers / testers / samples).
 * Result: fragrantica_perfumes.shop_stock = { "<shop id>": { n: cards, v: [volumes] } }.
 */
async function runFragranticaShopStockMatch(prisma) {
  const products = await prisma.$queryRawUnsafe(
    `SELECT target, name, brand, offer_id AS "offerId", archived FROM warehouse_products WHERE target IS NOT NULL AND name <> ''`,
  );
  const byShop = new Map();
  for (const p of products) {
    const name = cleanText(p.name);
    const brand = cleanText(p.brand);
    const text = brand && !name.toLowerCase().includes(brand.toLowerCase()) ? `${brand} ${name}` : name;
    if (!byShop.has(p.target)) byShop.set(p.target, []);
    byShop.get(p.target).push({ name: text, usd: 0, offerId: cleanText(p.offerId), archived: Boolean(p.archived) });
  }
  // card → perfume (for «Улучшение карточек»): offer key → perfume ids that matched it
  const cardMatches = new Map();
  const indexes = [...byShop.entries()].map(([shop, rows]) => [shop, buildFragranticaPmIndex(rows)]);
  let lastId = 0;
  let found = 0;
  for (;;) {
    const perfumes = await prisma.$queryRawUnsafe(`SELECT id, brand, name FROM fragrantica_perfumes WHERE id > $1 ORDER BY id LIMIT 2000`, lastId);
    if (!perfumes.length) break;
    lastId = perfumes[perfumes.length - 1].id;
    const results = perfumes.map((perfume) => {
      const stock = {};
      for (const [shop, index] of indexes) {
        const m = matchFragranticaPmIndex(index, perfume, { russianExtrasOk: true });
        if (m.count) stock[shop] = { n: m.count, v: m.volumes };
        for (const i of m.matched || []) {
          const row = index.rows[i];
          if (!row.offerId || row.archived) continue;
          const key = `${shop}\u0000${row.offerId}`;
          if (!cardMatches.has(key)) cardMatches.set(key, { shop, offerId: row.offerId, name: row.name, perfumeIds: [] });
          cardMatches.get(key).perfumeIds.push(perfume.id);
        }
      }
      if (Object.keys(stock).length) found += 1;
      return { id: perfume.id, stock };
    });
    await prisma.$executeRawUnsafe(
      `UPDATE fragrantica_perfumes p SET shop_stock = NULLIF(x->'stock', '{}'::jsonb)
         FROM jsonb_array_elements($1::jsonb) x WHERE p.id = (x->>'id')::int`,
      JSON.stringify(results),
    );
    await new Promise((resolve) => setImmediate(resolve));
  }
  // only unambiguous cards (one perfume) are kept; the volume and tester come from the card's own name
  const matches = [];
  for (const m of cardMatches.values()) {
    if (m.perfumeIds.length !== 1) continue;
    const volumes = fragPmVolumes(m.name);
    matches.push({ target: m.shop, offerId: m.offerId, perfumeId: m.perfumeIds[0], volume: volumes.length === 1 ? volumes[0] : null, tester: /тестер|tester/i.test(m.name) });
  }
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_card_matches (
      target TEXT NOT NULL,
      offer_id TEXT NOT NULL,
      perfume_id INTEGER NOT NULL,
      volume_ml NUMERIC,
      tester BOOLEAN NOT NULL DEFAULT false,
      matched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (target, offer_id)
    )`);
  await prisma.$transaction([
    prisma.$executeRawUnsafe(`DELETE FROM fragrantica_card_matches`),
    prisma.$executeRawUnsafe(
      `INSERT INTO fragrantica_card_matches (target, offer_id, perfume_id, volume_ml, tester)
       SELECT x->>'target', x->>'offerId', (x->>'perfumeId')::int, NULLIF(x->>'volume', '')::numeric, (x->>'tester')::boolean
         FROM jsonb_array_elements($1::jsonb) x ON CONFLICT DO NOTHING`,
      JSON.stringify(matches),
    ),
  ]);
  return { status: "ok", products: products.length, shops: byShop.size, found, cards: matches.length };
}

function scheduleFragranticaPmMatch(delayMs = fragranticaPmMatchIntervalMs) {
  if (!fragranticaPmMatchEnabled) return;
  setTimeout(async () => {
    try {
      await runFragranticaPmMatch();
    } finally {
      scheduleFragranticaPmMatch(fragranticaPmMatchIntervalMs);
    }
  }, Math.max(30_000, Number(delayMs) || fragranticaPmMatchIntervalMs)).unref?.();
}
