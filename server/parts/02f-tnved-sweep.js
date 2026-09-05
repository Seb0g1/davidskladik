// Периодический свип ТН ВЭД: каждые 2 часа находит все товары Ozon у которых
// не задан код ТН ВЭД (атрибут 22232) и проставляет его по категории.
// Работает на worker-процессе для всех активных Ozon-аккаунтов.

const TNVED_SWEEP_INTERVAL_MS = Math.max(
  30 * 60_000,
  Number(process.env.TNVED_SWEEP_INTERVAL_SECONDS || 2 * 3600) * 1000 || 2 * 3_600_000,
);
const tnvedSweepEnabled = process.env.TNVED_SWEEP_ENABLED !== "false";

// descCatId → TNVED-код (10 знаков)
const TNVED_BY_CAT_ID = {
  17028988: "3303001000", // Парфюмерия → Духи
  17028992: "3305900009", // Косметика для ухода за волосами → Прочие средства для волос
  17028991: "3304990000", // Декоративная косметика
  17028990: "3304990000", // Косметика для ухода
  17028993: "3307490000", // Ароматы для дома
  17028994: "3307200000", // Личная гигиена
  17028712: "3304990000", // Парфюмерия (sub)
  200001240: "3306100000", // Средства для гигиены полости рта
  200001242: "3401300000", // Ватно-бумажная продукция
  17027920: "3402909000", // Моющие и чистящие средства
};
const TNVED_DEFAULT = "3304990000"; // Прочие косметические средства

// TNVED_ATTR_ID = 22232 уже объявлен глобально в 02d-routes-catalog-report.js
const TNVED_SWEEP_MARKING_ATTR_ID = 23536;

// Кэш dict-значений: "catId:typeId:code" → { value, dictionary_value_id }
const tnvedDictCache = new Map();

async function tnvedFetchDictEntry(account, descCatId, typeId, code) {
  const cacheKey = `${descCatId}:${typeId}:${code}`;
  if (tnvedDictCache.has(cacheKey)) return tnvedDictCache.get(cacheKey);
  try {
    const entries = await ozonGetAttributeDictValues(account, descCatId, typeId, TNVED_ATTR_ID);
    for (const e of entries) {
      const text = cleanText(e.value || "");
      const m = text.match(/^(\d{10})/);
      if (m) {
        const k = `${descCatId}:${typeId}:${m[1]}`;
        if (!tnvedDictCache.has(k)) {
          tnvedDictCache.set(k, { value: text, dictionary_value_id: Number(e.id) });
        }
      }
    }
  } catch {}
  return tnvedDictCache.get(cacheKey) || null;
}

let tnvedSweepTimer = null;
let tnvedSweepRunning = false;

async function runTnvedSweep({ source = "schedule" } = {}) {
  if (tnvedSweepRunning) return { status: "already_running" };
  tnvedSweepRunning = true;
  const accounts = getOzonAccounts({ includeSyncDisabled: true });
  if (!accounts.length) { tnvedSweepRunning = false; return { status: "no_accounts" }; }

  let totalUpdated = 0;
  let totalSkipped = 0;

  try {
    for (const account of accounts) {
      try {
        // 1. Получить все offer_ids
        const offerIds = [];
        let lastId = "";
        while (true) {
          const data = await ozonRequest("/v3/product/list", {
            filter: { visibility: "ALL" }, last_id: lastId, limit: 1000,
          }, account);
          const items = data.result?.items || [];
          offerIds.push(...items.map((i) => cleanText(i.offer_id)).filter(Boolean));
          lastId = data.result?.last_id || "";
          if (!lastId || items.length < 1000) break;
        }
        if (!offerIds.length) continue;

        // 2. Найти товары без TNVED атрибута
        const noTnvedIds = [];
        for (const chunk of chunkArray(offerIds, 100)) {
          try {
            const data = await ozonRequest("/v4/product/info/attributes", {
              filter: { offer_id: chunk, visibility: "ALL" },
              limit: 100, sort_by: "id", sort_dir: "asc",
            }, account);
            for (const item of (data.result || [])) {
              const tnved = (item.attributes || []).find((a) => a.id === TNVED_ATTR_ID);
              if (!tnved || !tnved.values?.[0]?.value) {
                noTnvedIds.push(cleanText(item.offer_id));
              }
            }
          } catch {}
        }

        if (!noTnvedIds.length) {
          logger.info("tnved_sweep: account ok", { account: account.id, total: offerIds.length, missing: 0 });
          continue;
        }

        // 3. Получить категорию для каждого товара без TNVED
        const catMap = new Map(); // offerId → { descCatId, typeId }
        for (const chunk of chunkArray(noTnvedIds, 100)) {
          try {
            const data = await ozonRequest("/v3/product/info/list", { offer_id: chunk }, account);
            for (const item of (data.items || [])) {
              const offerId = cleanText(item.offer_id || "");
              if (offerId) {
                catMap.set(offerId, {
                  descCatId: Number(item.description_category_id || 0),
                  typeId: Number(item.type_id || 0),
                });
              }
            }
          } catch {}
        }

        // 4. Сформировать обновления
        const updateItems = [];
        for (const offerId of noTnvedIds) {
          const cat = catMap.get(offerId);
          if (!cat) { totalSkipped++; continue; }
          const { descCatId, typeId } = cat;
          const tnvedCode = TNVED_BY_CAT_ID[descCatId] || TNVED_DEFAULT;
          const dictEntry = await tnvedFetchDictEntry(account, descCatId, typeId, tnvedCode);
          if (!dictEntry) { totalSkipped++; continue; }
          updateItems.push({
            offer_id: offerId,
            attributes: [
              { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
              { id: TNVED_SWEEP_MARKING_ATTR_ID, values: [{ value: "false" }] },
            ],
          });
        }

        // 5. Отправить
        let accountUpdated = 0;
        for (const chunk of chunkArray(updateItems, 100)) {
          try {
            await ozonRequest("/v1/product/attributes/update", { items: chunk }, account);
            accountUpdated += chunk.length;
          } catch (err) {
            logger.warn("tnved_sweep: update chunk error", { account: account.id, detail: err?.message });
          }
        }

        totalUpdated += accountUpdated;
        logger.info("tnved_sweep: account done", {
          account: account.id,
          total: offerIds.length,
          missing: noTnvedIds.length,
          updated: accountUpdated,
        });
      } catch (err) {
        logger.warn("tnved_sweep: account error", { account: account.id, detail: err?.message });
      }
    }

    logger.info("tnved_sweep: complete", { source, accounts: accounts.length, totalUpdated, totalSkipped });
    return { status: "ok", totalUpdated, totalSkipped };
  } catch (err) {
    logger.warn("tnved_sweep: fatal", { detail: err?.message });
    return { status: "error", error: err?.message };
  } finally {
    tnvedSweepRunning = false;
  }
}

function scheduleTnvedSweep(delayMs = TNVED_SWEEP_INTERVAL_MS) {
  if (!tnvedSweepEnabled || !backgroundJobsEnabled || isApiServer) return;
  if (tnvedSweepTimer) clearTimeout(tnvedSweepTimer);
  const delay = Math.max(60_000, Number(delayMs) || TNVED_SWEEP_INTERVAL_MS);
  tnvedSweepTimer = setTimeout(async () => {
    let result = null;
    try {
      result = await runTnvedSweep({ source: "schedule" });
    } catch (err) {
      logger.warn("tnved_sweep tick failed", { detail: err?.message });
      result = { status: "error", error: err?.message };
    } finally {
      await recordSweepHeartbeat("tnved_sweep", {
        status: result?.status || "unknown",
        intervalMs: TNVED_SWEEP_INTERVAL_MS,
        detail: result || {},
      }).catch(() => {});
      scheduleTnvedSweep(TNVED_SWEEP_INTERVAL_MS);
    }
  }, delay);
  tnvedSweepTimer.unref?.();
}

if (backgroundJobsEnabled && !isApiServer) {
  scheduleTnvedSweep(10 * 60_000);
  logger.info("tnved sweep scheduler enabled", {
    intervalHours: Math.round(TNVED_SWEEP_INTERVAL_MS / 3_600_000),
    firstRunAt: new Date(Date.now() + 10 * 60_000).toISOString(),
  });
}
