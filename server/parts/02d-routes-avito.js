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

// --- Фид ---

// Информация о фиде: публичная ссылка для настроек автозагрузки Avito.
app.get("/api/avito/feed-info", async (request, response, next) => {
  try {
    const token = await ensureAvitoFeedToken();
    const { count, total } = await buildAvitoFeedXml();
    const baseUrl = cleanText(process.env.PUBLIC_BASE_URL) || `${request.protocol}://${request.get("host")}`;
    response.json({
      feedUrl: `${baseUrl}/public/avito-feed/${token}.xml`,
      enabledCount: count,
      totalListings: total,
    });
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
    if (!state.feedToken || cleanText(request.params.token) !== state.feedToken) {
      return response.status(404).send("Not found");
    }
    const { xml } = await buildAvitoFeedXml();
    response.type("application/xml").send(xml);
  } catch (error) {
    next(error);
  }
});
