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

        // 2. Найти товары без TNVED атрибута ИЛИ с неверным кодом
        // wrongCodeMap: offerId → currentCode (не пустой, но не совпадает с ожидаемым)
        // hashtagMap: offerId → текущее значение хэштега (23171) — нужно для фикса BR_hashtag ошибок
        const noTnvedIds = [];
        const wrongCodeMap = new Map(); // offerId → текущий код
        const hashtagMap = new Map(); // offerId → текущий хэштег (может быть пустым)
        for (const chunk of chunkArray(offerIds, 100)) {
          try {
            const data = await ozonRequest("/v4/product/info/attributes", {
              filter: { offer_id: chunk, visibility: "ALL" },
              limit: 100, sort_by: "id", sort_dir: "asc",
            }, account);
            for (const item of (data.result || [])) {
              const tnved = (item.attributes || []).find((a) => a.id === TNVED_ATTR_ID);
              const hashtag = (item.attributes || []).find((a) => a.id === 23171);
              const currentCode = cleanText(tnved?.values?.[0]?.value || "").match(/^(\d{7,10})/)?.[1] || "";
              const currentHashtag = cleanText(hashtag?.values?.[0]?.value || "");
              const offerId = cleanText(item.offer_id);
              if (currentHashtag) hashtagMap.set(offerId, currentHashtag);
              if (!currentCode) {
                noTnvedIds.push(offerId);
              } else {
                // Пока не знаем категорию — собираем всех с кодами для проверки ниже
                wrongCodeMap.set(offerId, currentCode);
              }
            }
          } catch {}
        }

        const needsCatCheck = [...noTnvedIds, ...wrongCodeMap.keys()];
        if (!needsCatCheck.length) {
          logger.info("tnved_sweep: account ok", { account: account.id, total: offerIds.length, missing: 0 });
          continue;
        }

        // 3. Получить категорию для товаров без TNVED (+ с потенциально неверным кодом)
        const catMap = new Map(); // offerId → { descCatId, typeId }
        for (const chunk of chunkArray(needsCatCheck, 100)) {
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

        // 4. Сформировать обновления: без кода + с неверным кодом
        const updateItems = [];
        const allToCheck = [...noTnvedIds, ...wrongCodeMap.keys()];
        for (const offerId of allToCheck) {
          const cat = catMap.get(offerId);
          if (!cat) { totalSkipped++; continue; }
          const { descCatId, typeId } = cat;
          const expectedCode = TNVED_BY_CAT_ID[descCatId] || TNVED_DEFAULT;
          const currentCode = wrongCodeMap.get(offerId) || "";
          // Пропускаем если код уже правильный
          if (currentCode && currentCode === expectedCode) continue;
          const dictEntry = await tnvedFetchDictEntry(account, descCatId, typeId, expectedCode);
          if (!dictEntry) { totalSkipped++; continue; }
          const attrs = [
            { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
            { id: TNVED_SWEEP_MARKING_ATTR_ID, values: [{ value: "false" }] },
          ];
          // Продукты с BR_hashtag ошибками блокируют любое обновление атрибутов.
          // Если у товара есть хэштег — заменяем его безопасным значением в том же вызове,
          // чтобы снять блокировку и позволить TNVED сохраниться.
          if (hashtagMap.has(offerId)) {
            attrs.push({ id: 23171, values: [{ value: "#косметика #уход" }] });
          }
          updateItems.push({ offer_id: offerId, attributes: attrs });
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
          wrongCode: wrongCodeMap.size,
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

// Точечный фикс по product_id (SKU) — принимает [{sku, cat}] и обновляет по всем аккаунтам.
const TNVED_BY_CAT_NAME = {
  "Парфюмерия": "3303001000",
  "Косметика для ухода за волосами": "3305900009",
  "Декоративная косметика": "3304990000",
  "Косметика для ухода": "3304990000",
  "Ароматы для дома": "3307490000",
  "Средства для гигиены тела": "3401300000",
  "Средства для гигиены полости рта": "3306100000",
  "Моющие и чистящие средства": "3402909000",
  "Личная гигиена": "3307200000",
  "Маска косметическая": "3304990000",
  "Средства для депиляции": "3307900009",
  "Средства для бритья и груминг": "3307900009",
};

app.post("/api/ozon/tnved/fix-by-sku", requireAdmin, async (req, res, next) => {
  try {
    // items: [{sku: number, cat: string}]
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.status(400).json({ error: "items required" });

    const accounts = getOzonAccounts({ includeSyncDisabled: true });
    let totalUpdated = 0;
    let totalSkipped = 0;
    const errors = [];

    for (const account of accounts) {
      // Resolve SKU → offer_id + descCatId + typeId
      const allSkus = items.map((i) => Number(i.sku)).filter(Boolean);
      const skuInfo = new Map(); // sku → { offerId, descCatId, typeId }

      for (const chunk of chunkArray(allSkus, 100)) {
        try {
          const data = await ozonRequest("/v3/product/info/list", { sku: chunk }, account);
          for (const item of (data.items || [])) {
            const itemSku = item.sku || item.id;
            if (itemSku && item.offer_id) {
              skuInfo.set(Number(itemSku), {
                offerId: cleanText(item.offer_id || ""),
                descCatId: Number(item.description_category_id || 0),
                typeId: Number(item.type_id || 0),
              });
            }
          }
        } catch (err) {
          errors.push(`account ${account.id} info chunk: ${err?.message}`);
        }
      }

      if (!skuInfo.size) continue;

      // Build updates
      const updateItems = [];
      for (const { sku, cat } of items) {
        const info = skuInfo.get(Number(sku));
        if (!info) continue;
        const { offerId, descCatId, typeId } = info;
        if (!offerId || !descCatId) { totalSkipped++; continue; }
        const expectedCode = TNVED_BY_CAT_NAME[cat] || TNVED_DEFAULT;
        const dictEntry = await tnvedFetchDictEntry(account, descCatId, typeId, expectedCode);
        if (!dictEntry) { totalSkipped++; errors.push(`no dict entry: sku=${sku} cat=${cat} code=${expectedCode} descCatId=${descCatId}`); continue; }
        updateItems.push({
          offer_id: offerId,
          attributes: [
            { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
            { id: TNVED_SWEEP_MARKING_ATTR_ID, values: [{ value: "false" }] },
          ],
        });
      }

      for (const chunk of chunkArray(updateItems, 100)) {
        try {
          const result = await ozonRequest("/v1/product/attributes/update", { items: chunk }, account);
          const chunkErrors = result.errors || [];
          totalUpdated += chunk.length - chunkErrors.length;
          if (chunkErrors.length) errors.push(...chunkErrors.map((e) => `sku update: ${JSON.stringify(e)}`));
        } catch (err) {
          totalSkipped += chunk.length;
          errors.push(`account ${account.id} update chunk: ${err?.message}`);
        }
      }

      logger.info("tnved_fix_by_sku: account done", { account: account.id, found: skuInfo.size, updated: updateItems.length });
    }

    logger.info("tnved_fix_by_sku: complete", { totalUpdated, totalSkipped, errorCount: errors.length });
    res.json({ ok: true, totalUpdated, totalSkipped, errors: errors.slice(0, 20) });
  } catch (error) {
    next(error);
  }
});

// Безопасные хештеги по категории для замены проблемных
const SAFE_HASHTAG_BY_CAT = {
  "Парфюмерия": "#парфюм #аромат",
  "Косметика для ухода за волосами": "#уход #волосы",
  "Декоративная косметика": "#косметика #макияж",
  "Косметика для ухода": "#уход #косметика",
  "Ароматы для дома": "#аромат #дом",
  "Средства для гигиены тела": "#гигиена #уход",
  "Средства для гигиены полости рта": "#гигиена #уход",
  "Личная гигиена": "#гигиена",
  "Маска косметическая": "#маска #уход",
  "Средства для депиляции": "#депиляция #уход",
  "Средства для бритья и груминг": "#бритьё #уход",
  "Моющие и чистящие средства": "#чистка",
};

// Точечный фикс с очисткой хештегов: одним вызовом убираем проблемный attr 23171 и ставим TNVED.
// Принимает [{sku, cat}], запускает асинхронно, возвращает 202.
let tnvedFixCleanRunning = false;
app.post("/api/ozon/tnved/fix-with-clean", requireAdmin, async (req, res, next) => {
  try {
    if (tnvedFixCleanRunning) return res.status(409).json({ error: "Уже выполняется" });
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.status(400).json({ error: "items required" });
    res.status(202).json({ ok: true, message: `Запущено в фоне для ${items.length} товаров — следите за логами` });

    tnvedFixCleanRunning = true;
    (async () => {
      try {
        const accounts = getOzonAccounts({ includeSyncDisabled: true });
        let totalUpdated = 0, totalSkipped = 0, totalFailed = 0;
        const errors = [];

        for (const account of accounts) {
          const allSkus = items.map((i) => Number(i.sku)).filter(Boolean);
          const skuInfo = new Map(); // sku → { offerId, descCatId, typeId, cat }

          for (const chunk of chunkArray(allSkus, 100)) {
            try {
              const data = await ozonRequest("/v3/product/info/list", { sku: chunk }, account);
              for (const item of (data.items || [])) {
                const itemSku = item.sku || item.id;
                if (itemSku && item.offer_id) {
                  skuInfo.set(Number(itemSku), {
                    offerId: cleanText(item.offer_id || ""),
                    descCatId: Number(item.description_category_id || 0),
                    typeId: Number(item.type_id || 0),
                  });
                }
              }
            } catch (err) { errors.push(`info chunk: ${err?.message}`); }
          }
          if (!skuInfo.size) continue;

          // Получаем текущие хештеги (attr 23171) для всех найденных товаров
          const offerIds = [...skuInfo.values()].map((v) => v.offerId);
          const hashtagMap = new Map(); // offerId → currentHashtagValue|null
          for (const chunk of chunkArray(offerIds, 100)) {
            try {
              const data = await ozonRequest("/v4/product/info/attributes", {
                filter: { offer_id: chunk, visibility: "ALL" }, limit: 100, sort_by: "id", sort_dir: "asc",
              }, account);
              for (const it of (data.result || [])) {
                const h = (it.attributes || []).find((a) => a.id === 23171);
                hashtagMap.set(cleanText(it.offer_id), h?.values?.[0]?.value || null);
              }
            } catch (err) { errors.push(`attrs chunk: ${err?.message}`); }
          }

          const updateItems = [];
          for (const { sku, cat } of items) {
            const info = skuInfo.get(Number(sku));
            if (!info) continue;
            const { offerId, descCatId, typeId } = info;
            if (!offerId || !descCatId) { totalSkipped++; continue; }

            const expectedCode = TNVED_BY_CAT_NAME[cat] || TNVED_DEFAULT;
            const dictEntry = await tnvedFetchDictEntry(account, descCatId, typeId, expectedCode);
            if (!dictEntry) { totalSkipped++; errors.push(`no dict entry: sku=${sku} cat=${cat}`); continue; }

            const currentHashtag = hashtagMap.get(offerId);
            const safeHashtag = SAFE_HASHTAG_BY_CAT[cat] || "#косметика";

            const attrs = [
              { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
            ];
            // Добавляем замену хештега только если он непустой (иначе ошибка в аннотации — не хештег)
            if (currentHashtag) {
              attrs.push({ id: 23171, values: [{ value: safeHashtag }] });
            }
            updateItems.push({ offer_id: offerId, attributes: attrs });
          }

          // Отправляем батчами по 100
          for (const chunk of chunkArray(updateItems, 100)) {
            try {
              const result = await ozonRequest("/v1/product/attributes/update", { items: chunk }, account);
              const chunkErrors = result.errors || [];
              totalUpdated += chunk.length - chunkErrors.length;
              if (chunkErrors.length) {
                totalFailed += chunkErrors.length;
                errors.push(...chunkErrors.map((e) => `update error: ${JSON.stringify(e)}`));
              }
            } catch (err) {
              totalFailed += chunk.length;
              errors.push(`update chunk: ${err?.message}`);
            }
          }

          logger.info("tnved_fix_clean: account done", {
            account: account.id, found: skuInfo.size, updatesSent: updateItems.length,
          });
        }

        logger.info("tnved_fix_clean: complete", { totalUpdated, totalSkipped, totalFailed, errorCount: errors.length });
        if (errors.length) {
          logger.warn("tnved_fix_clean: errors sample", { errors: errors.slice(0, 10) });
        }
      } finally {
        tnvedFixCleanRunning = false;
      }
    })().catch((err) => {
      tnvedFixCleanRunning = false;
      logger.warn("tnved_fix_clean: fatal", { detail: err?.message });
    });
  } catch (error) {
    next(error);
  }
});

// Ручной запуск sweep из UI или скрипта — доступен и на api-процессе.
app.post("/api/ozon/tnved/sweep/run", requireAdmin, async (req, res, next) => {
  try {
    if (tnvedSweepRunning) {
      return res.status(409).json({ error: "Sweep уже выполняется" });
    }
    // Запускаем асинхронно, чтобы HTTP-запрос не зависал на 5+ минут
    res.status(202).json({ ok: true, message: "Sweep запущен в фоне — проверьте логи для результата" });
    runTnvedSweep({ source: "manual" }).catch((err) => {
      logger.warn("tnved_sweep manual run failed", { detail: err?.message });
    });
  } catch (error) {
    next(error);
  }
});

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
