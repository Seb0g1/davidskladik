// API-роуты Avito: профиль автозагрузки, запуск загрузок, отчёты v4,
// ID-маппинги, категории, объявления фида и импорт Ozon → Avito.

function resolveAvitoAccountOr404(request, response) {
  const account = getAvitoAccountByTarget(cleanText(request.query.target || request.body?.target || "avito"));
  if (!account) {
    response.status(400).json({ error: "Кабинет Avito не настроен. Добавьте Client ID и Client Secret в настройках кабинетов или в .env." });
    return null;
  }
  return account;
}

// --- Профиль автозагрузки ---

app.get("/api/avito/profile", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoAutoloadProfile(account));
  } catch (error) {
    if (error?.statusCode === 404) return response.json({ exists: false, autoload_enabled: false });
    next(error);
  }
});

app.post("/api/avito/profile", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    const { target, ...payload } = request.body || {};
    const result = await saveAvitoAutoloadProfile(account, payload);
    await appendAudit(request, "avito.profile.save", { newValue: payload });
    response.json({ ok: true, result });
  } catch (error) {
    next(error);
  }
});

// --- Запуск автозагрузки (не чаще раза в час на стороне Avito) ---

app.post("/api/avito/upload", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    const result = await triggerAvitoAutoloadUpload(account);
    await appendAudit(request, "avito.upload.trigger", {});
    response.json({ ok: true, result });
  } catch (error) {
    next(error);
  }
});

// --- Загрузки v4 ---

app.get("/api/avito/uploads", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoUploads(account, {
      perPage: Number(request.query.perPage || 10) || 10,
      page: Number(request.query.page || 1) || 1,
      dateFrom: cleanText(request.query.dateFrom),
      dateTo: cleanText(request.query.dateTo),
    }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/avito/uploads/current", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoCurrentUpload(account));
  } catch (error) {
    if (error?.statusCode === 404) return response.status(404).json({ error: "Загрузок ещё не было." });
    next(error);
  }
});

app.get("/api/avito/uploads/last-successful", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoLastSuccessfulUpload(account));
  } catch (error) {
    if (error?.statusCode === 404) return response.status(404).json({ error: "Успешных загрузок ещё не было." });
    next(error);
  }
});

app.get("/api/avito/uploads/current/items", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoCurrentUploadItems(account, {
      query: cleanText(request.query.query),
      sections: cleanText(request.query.sections),
      perPage: Number(request.query.perPage || 20) || 20,
      page: Number(request.query.page || 1) || 1,
    }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/avito/uploads/last-successful/items", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoLastSuccessfulUploadItems(account, {
      query: cleanText(request.query.query),
      sections: cleanText(request.query.sections),
      perPage: Number(request.query.perPage || 20) || 20,
      page: Number(request.query.page || 1) || 1,
    }));
  } catch (error) {
    next(error);
  }
});

// --- Соответствие ID объявлений ---

app.get("/api/avito/items/ad-ids", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoAdIdsByAvitoIds(account, cleanText(request.query.query)));
  } catch (error) {
    next(error);
  }
});

app.get("/api/avito/items/avito-ids", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoIdsByAdIds(account, cleanText(request.query.query)));
  } catch (error) {
    next(error);
  }
});

// --- Категории ---

app.get("/api/avito/categories/tree", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoCategoriesTree(account));
  } catch (error) {
    next(error);
  }
});

app.get("/api/avito/categories/:slug/fields", async (request, response, next) => {
  try {
    const account = resolveAvitoAccountOr404(request, response);
    if (!account) return;
    response.json(await getAvitoCategoryFields(account, request.params.slug));
  } catch (error) {
    next(error);
  }
});

// --- Правила импорта Ozon → Avito ---

app.get("/api/avito/import/rules", async (_request, response, next) => {
  try {
    response.json(await readAvitoImportRules());
  } catch (error) {
    next(error);
  }
});

app.put("/api/avito/import/rules", requireAdmin, async (request, response, next) => {
  try {
    const before = await readAvitoImportRules();
    const saved = await writeAvitoImportRules(request.body || {});
    await appendAudit(request, "avito.import.rules.save", { oldValue: before, newValue: saved });
    response.json(saved);
  } catch (error) {
    next(error);
  }
});

// Предпросмотр: что попадёт в фид и почему остальные пропущены. Правила можно
// передать в body для проверки без сохранения.
app.post("/api/avito/import/preview", async (request, response, next) => {
  try {
    const rules = request.body && Object.keys(request.body).length ? request.body : null;
    response.json(await previewAvitoOzonImport({ rules }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/avito/import/apply", requireAdmin, async (request, response, next) => {
  try {
    const rules = request.body && Object.keys(request.body).length ? request.body : null;
    const result = await applyAvitoOzonImport({ rules });
    await appendAudit(request, "avito.import.apply", { newValue: result });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

// --- Объявления фида ---

app.get("/api/avito/listings", async (_request, response, next) => {
  try {
    const state = await readAvitoListingsFile();
    response.json({ updatedAt: state.updatedAt, total: state.items.length, items: state.items });
  } catch (error) {
    next(error);
  }
});

app.post("/api/avito/listings", requireAdmin, async (request, response, next) => {
  try {
    const items = Array.isArray(request.body?.items) ? request.body.items : [request.body];
    const result = await upsertAvitoListings(items, { source: "manual" });
    await appendAudit(request, "avito.listings.upsert", { newValue: result });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/avito/listings", requireAdmin, async (request, response, next) => {
  try {
    const adIds = request.body?.adIds ?? request.query.adIds;
    const result = await removeAvitoListings(adIds);
    await appendAudit(request, "avito.listings.remove", { newValue: result });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

// Статус товаров склада в фиде Avito — для панели Avito в карточке товара:
// в фиде ли товар, живая цена (от поставщика), категория, описание; если не в
// фиде — причины пропуска и потенциальная цена.
app.post("/api/avito/product-status", async (request, response, next) => {
  try {
    const productIds = (Array.isArray(request.body?.productIds) ? request.body.productIds : [])
      .map((value) => cleanText(value))
      .filter(Boolean)
      .slice(0, 50);
    if (!productIds.length) return response.json({ items: [] });

    const [state, rules] = await Promise.all([readAvitoListingsFile(), readAvitoImportRules()]);
    const listingByProductId = new Map();
    for (const item of state.items) {
      const sourceProductId = cleanText(item.sourceProductId);
      if (sourceProductId && !listingByProductId.has(sourceProductId)) listingByProductId.set(sourceProductId, item);
    }
    const pricing = await loadAvitoPricingContext();
    const liveStates = await loadAvitoLiveProductStates(productIds.map((id) => ({ sourceProductId: id })));
    // Поставщики: снапшот из raw + живой расчёт из PriceMaster для товаров без
    // снапшота (карточка — до 50 товаров, это быстро).
    const supplierMap = await loadAvitoSupplierPricingMap(productIds);
    const prisma = getPrisma();

    const items = [];
    for (const productId of productIds) {
      const listing = listingByProductId.get(productId) || null;
      const liveRaw = liveStates instanceof Map ? liveStates.get(productId) : null;
      const live = liveRaw ? { ...liveRaw, supplier: liveRaw.supplier || supplierMap.get(productId) || null } : liveRaw;
      if (listing) {
        const { listing: fresh, outOfStock } = liveStates === null
          ? { listing, outOfStock: listing.outOfStock === true }
          : applyAvitoLiveState(listing, live, rules, pricing);
        const markupOverride = Number(listing.markupCoefficient) > 0 ? Number(listing.markupCoefficient) : 0;
        // Разбивка «цена поставщика × курс × коэффициент» для карточки товара.
        const breakdown = live?.supplier ? avitoSupplierPriceBreakdown(live.supplier, pricing, markupOverride) : null;
        items.push({
          productId,
          inFeed: true,
          adId: listing.adId,
          enabled: listing.enabled !== false,
          outOfStock,
          priceRub: fresh.priceRub,
          storedPriceRub: listing.priceRub,
          markupCoefficient: markupOverride,
          categoryKey: listing.categoryKey || "",
          categoryPath: avitoListingCategoryPath(listing),
          categoryAutoDefaulted: listing.categoryAutoDefaulted === true,
          hasDescription: Boolean(cleanText(listing.description)),
          priceSource: markupOverride > 0 ? "markup" : breakdown ? "supplier" : "stored",
          priceFormula: breakdown
            ? {
              supplierUsd: breakdown.supplierUsd,
              purchaseRub: breakdown.purchaseRub,
              usdRate: breakdown.usdRate,
              rubNative: breakdown.rubNative === true,
              coefficient: breakdown.coefficient,
            }
            : null,
        });
        continue;
      }
      if (!prisma) {
        items.push({ productId, inFeed: false, reasons: ["postgres_unavailable"] });
        continue;
      }
      const row = await prisma.warehouseProduct.findUnique({
        where: { id: productId },
        select: { id: true, offerId: true, name: true, brand: true, images: true, archived: true, targetStock: true, targetPrice: true, marketplace: true },
      });
      if (!row || row.marketplace !== "ozon") {
        items.push({ productId, inFeed: false, reasons: [row ? "not_ozon" : "not_found"] });
        continue;
      }
      const result = evaluateAvitoImportCandidate(row, rules, {
        ...pricing,
        supplierByProductId: new Map([[productId, live?.supplier || null]]),
      });
      items.push({
        productId,
        inFeed: false,
        reasons: result.reasons,
        potential: result.ok
          ? { priceRub: result.listing.priceRub, categoryPath: avitoListingCategoryPath(result.listing) }
          : null,
      });
    }
    response.json({ items });
  } catch (error) {
    next(error);
  }
});

// --- Фид ---

// Информация о фиде: публичная ссылка для настроек автозагрузки Avito.
app.get("/api/avito/feed-info", async (request, response, next) => {
  try {
    const token = await ensureAvitoFeedToken();
    const { count, total, hiddenOutOfStock, hiddenNoImages, hiddenDuplicates, liveSource } = await buildAvitoFeedXml();
    const baseUrl = cleanText(process.env.PUBLIC_BASE_URL) || `${request.protocol}://${request.get("host")}`;
    response.json({
      feedUrl: `${baseUrl}/public/avito-feed/${token}.xml`,
      enabledCount: count,
      totalListings: total,
      hiddenOutOfStock,
      hiddenNoImages,
      hiddenDuplicates,
      liveSource,
      autoRefresh: {
        enabled: avitoFeedRefreshEnabled,
        intervalMinutes: Math.round(avitoFeedRefreshIntervalMs / 60000),
        nextRunAt: avitoFeedRefreshNextRunAt,
        lastResult: avitoFeedRefreshLastResult,
      },
      autoSync: {
        enabled: avitoAutoSyncEnabled,
        intervalHours: Math.round(avitoAutoSyncIntervalMs / 3600000),
        nextRunAt: avitoAutoSyncNextRunAt,
        lastResult: avitoAutoSyncLastResult,
      },
    });
  } catch (error) {
    next(error);
  }
});

// Быстрая корректировка цен Avito, как у Ozon/Yandex: меняет базовый
// коэффициент и все гибкие правила цены на указанный процент, затем сразу
// переносит новые цены в объявления фида.
app.post("/api/avito/price/adjust-percent", requireAdmin, async (request, response, next) => {
  try {
    const direction = cleanText(request.body?.direction).toLowerCase() === "increase" ? "increase" : "decrease";
    const percent = Number(request.body?.percent || 0);
    if (!Number.isFinite(percent) || percent <= 0 || percent >= 90) {
      return response.status(400).json({ error: "Процент должен быть от 0.01 до 90." });
    }
    const multiplier = direction === "increase" ? 1 + percent / 100 : 1 - percent / 100;
    const before = await readAvitoImportRules();
    const saved = await writeAvitoImportRules({
      ...before,
      priceCoefficient: Number((before.priceCoefficient * multiplier).toFixed(4)),
      priceRules: before.priceRules.map((rule) => ({ ...rule, coefficient: Number((rule.coefficient * multiplier).toFixed(4)) })),
    });
    const refresh = await runAvitoFeedRefresh({ source: "price_adjust" });
    await appendAudit(request, "avito.price.adjust", { oldValue: before, newValue: saved, details: { direction, percent } });
    response.json({ ok: true, rules: saved, refresh });
  } catch (error) {
    next(error);
  }
});

// Личный коэффициент наценки Avito на конкретное объявление (как markup у
// Ozon/Yandex): цена = закупка поставщика × коэффициент, общие правила наценки
// не применяются. coefficient = 0 возвращает общие правила. Цена
// пересчитывается сразу из живых данных склада.
app.post("/api/avito/price/markup", requireAdmin, async (request, response, next) => {
  try {
    const adId = cleanText(request.body?.adId);
    const productId = cleanText(request.body?.productId);
    if (!adId && !productId) return response.status(400).json({ error: "Нужен adId или productId объявления." });
    const coefficient = Number(request.body?.coefficient || 0);
    if (!Number.isFinite(coefficient) || coefficient < 0 || coefficient > 100) {
      return response.status(400).json({ error: "Коэффициент должен быть числом от 0 до 100 (0 — вернуть общие правила наценки)." });
    }

    const state = await readAvitoListingsFile();
    const listing = state.items.find((item) =>
      (adId && item.adId === adId) || (!adId && productId && cleanText(item.sourceProductId) === productId));
    if (!listing) return response.status(404).json({ error: "Объявление не найдено в фиде Avito." });

    const before = { adId: listing.adId, markupCoefficient: listing.markupCoefficient || 0, priceRub: listing.priceRub };
    const updatedListing = { ...listing, markupCoefficient: coefficient };
    // Пересчитываем цену сразу, не дожидаясь фонового рефреша.
    const [rules, pricing, liveStates] = await Promise.all([
      readAvitoImportRules(),
      loadAvitoPricingContext(),
      loadAvitoLiveProductStates([listing]),
    ]);
    const live = liveStates instanceof Map ? liveStates.get(cleanText(listing.sourceProductId)) : null;
    if (live) {
      if (!live.supplier) {
        const supplierMap = await loadAvitoSupplierPricingMap([cleanText(listing.sourceProductId)]);
        live.supplier = supplierMap.get(cleanText(listing.sourceProductId)) || null;
      }
      const priceRub = resolveAvitoListingPriceRub(live, live.supplier, rules, pricing, coefficient > 0 ? coefficient : 0);
      if (priceRub > 0) updatedListing.priceRub = priceRub;
    }
    await upsertAvitoListings([updatedListing]);
    await appendAudit(request, "avito.price.markup", {
      oldValue: before,
      newValue: { adId: listing.adId, markupCoefficient: coefficient, priceRub: updatedListing.priceRub },
    });
    response.json({
      ok: true,
      adId: listing.adId,
      markupCoefficient: coefficient,
      priceRub: updatedListing.priceRub,
      priceSource: coefficient > 0 ? "markup" : "auto",
    });
  } catch (error) {
    next(error);
  }
});

// Ручное дозаполнение описаний объявлений с Ozon (для объявлений без
// description). Порциями, чтобы HTTP-запрос не висел дольше пары минут.
app.post("/api/avito/descriptions/backfill", requireAdmin, async (request, response, next) => {
  try {
    const limit = Math.min(500, Math.max(1, Number(request.body?.limit || 200) || 200));
    const result = await backfillAvitoListingDescriptionsFromOzon({ limit, source: "manual" });
    await appendAudit(request, "avito.descriptions.backfill", { newValue: result });
    response.json({ ok: result.status === "ok" || result.status === "done", ...result });
  } catch (error) {
    next(error);
  }
});

// Ручное дозаполнение фото объявлений без картинок: из Postgres, недостающие —
// с Ozon API (с персистом обратно в склад).
app.post("/api/avito/images/backfill", requireAdmin, async (request, response, next) => {
  try {
    const limit = Math.min(1000, Math.max(1, Number(request.body?.limit || 300) || 300));
    const result = await backfillAvitoListingImages({ limit, source: "manual" });
    await appendAudit(request, "avito.images.backfill", { newValue: result });
    response.json({ ok: result.status === "ok" || result.status === "done", ...result });
  } catch (error) {
    next(error);
  }
});

// Полный ручной синк: добавляет новые привязанные товары, удаляет потерявшие
// поставщика. Эквивалент авто-синка по расписанию.
app.post("/api/avito/feed/sync", requireAdmin, async (request, response, next) => {
  try {
    const result = await runAvitoAutoSync({ source: "manual" });
    await appendAudit(request, "avito.feed.sync", { newValue: result });
    response.json({ ok: result.status !== "error", ...result });
  } catch (error) {
    next(error);
  }
});

// Ручной перенос актуальных цен/остатков склада в сохранённые объявления.
app.post("/api/avito/feed/refresh", requireAdmin, async (request, response, next) => {
  try {
    const result = await runAvitoFeedRefresh({ source: "manual" });
    if (result.status === "postgres_unavailable") {
      return response.status(503).json({ error: "Postgres недоступен: обновить цены и остатки сейчас нельзя.", ...result });
    }
    await appendAudit(request, "avito.feed.refresh", { newValue: result });
    response.json({ ok: result.status !== "error", ...result });
  } catch (error) {
    next(error);
  }
});

// Авторизованный предпросмотр XML.
app.get("/api/avito/feed.xml", async (_request, response, next) => {
  try {
    const { xml } = await buildAvitoFeedXml();
    response.type("application/xml").send(xml);
  } catch (error) {
    next(error);
  }
});

// Публичный фид для скачивания Авито — без сессии, доступ по секретному токену
// в URL (см. исключение в requireAuth).
app.get("/public/avito-feed/:token.xml", async (request, response, next) => {
  try {
    const state = await readAvitoListingsFile();
    if (!state.feedToken || !timingSafeEqual(cleanText(request.params.token), state.feedToken)) {
      return response.status(404).send("Not found");
    }
    const { xml } = await buildAvitoFeedXml();
    response.type("application/xml").send(xml);
  } catch (error) {
    next(error);
  }
});
