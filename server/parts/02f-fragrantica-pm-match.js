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
    const result = { status: "ok", pmRows: pmRows.length, checked, found, elapsedMs: Date.now() - startedAt };
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
