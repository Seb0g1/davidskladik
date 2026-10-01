// API страницы «Фрагрантика» (/app/fragrantica): каталог, карточка аромата, обход, фото.
// Фото отдаются публично (/uploads/fragrantica/… — исключение в requireAuth): Ozon скачивает их по ссылке.

const fragranticaDetailMaxAgeMs = 30 * 86_400_000;

function fragranticaDetailResponse(row) {
  const detail = row.detail || {};
  return {
    id: Number(row.id),
    url: row.url,
    brand: row.brand,
    brandSlug: row.brand_slug,
    name: row.name,
    gender: row.gender || "",
    year: row.year ?? null,
    votes: row.votes ?? null,
    rating: row.rating ?? null,
    family: detail.family || "",
    perfumers: detail.perfumers || [],
    notes: detail.notes || { top: [], middle: [], base: [], flat: [] },
    accords: detail.accords || [],
    description: detail.description || "",
    image: fragranticaMediaUrl("images", `${Number(row.id)}.jpg`),
    detailAt: row.detail_at || null,
    detailError: row.detail_error || null,
    pm: fragranticaPmFromRow(row),
  };
}

app.get("/api/fragrantica/catalog", requireAdmin, async (request, response, next) => {
  try {
    const result = await listFragranticaPerfumes(request.query || {});
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/catalog/brands", requireAdmin, async (request, response, next) => {
  try {
    response.json({ ok: true, items: await listFragranticaBrands({ q: request.query.q, limit: request.query.limit }) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/catalog/:id", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const id = Number(request.params.id);
    let row = await readFragranticaPerfume(id);
    if (!row) return response.status(404).json({ error: "Аромат не найден в каталоге." });
    const stale = !row.detail_at || Date.now() - new Date(row.detail_at).getTime() > fragranticaDetailMaxAgeMs;
    if (stale || request.query.refresh === "1") {
      try {
        await fetchAndStoreFragranticaPerfume(row.url);
        row = await readFragranticaPerfume(id);
      } catch (error) {
        // Фрагрантика не ответила — отдаём то, что есть, и просим worker докачать.
        await getPrisma().$executeRawUnsafe(`UPDATE fragrantica_perfumes SET detail_wanted_at = now() WHERE id = $1 AND detail_at IS NULL`, id);
        if (!row.detail_at) {
          return response.status(502).json({ error: `Не удалось загрузить страницу с Фрагрантики: ${error?.message || error}`, code: "fragrantica_unavailable" });
        }
      }
    }
    await ensureFragranticaPerfumeImage(id).catch((error) => logger.warn("fragrantica image download failed", { id, detail: error?.message }));
    response.json({ ok: true, perfume: fragranticaDetailResponse(row) });
  } catch (error) {
    next(error);
  }
});

// Добавить аромат по ссылке fragrantica.* (если его ещё нет в каталоге).
app.post("/api/fragrantica/catalog/import", requireAdmin, async (request, response, next) => {
  try {
    const parsed = normalizeFragranticaPerfumeUrl(request.body?.url);
    if (!parsed) return response.status(400).json({ error: "Вставьте ссылку на страницу аромата Фрагрантики.", code: "fragrantica_url_invalid" });
    const detail = await fetchAndStoreFragranticaPerfume(parsed.url);
    response.json({ ok: true, id: detail.id });
  } catch (error) {
    if (error instanceof FragranticaHttpError) {
      return response.status(502).json({ error: error.message, code: "fragrantica_unavailable" });
    }
    next(error);
  }
});

app.get("/api/fragrantica/crawler", requireAdmin, async (_request, response, next) => {
  try {
    const [stats, state, status] = await Promise.all([
      readFragranticaCatalogStats(),
      readFragranticaState("crawler"),
      readFragranticaState("crawler_status"),
    ]);
    response.json({
      ok: true,
      stats,
      paused: Boolean(state.paused),
      indexPages: state.indexPages || null,
      indexDoneAt: state.indexDoneAt || null,
      status,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/crawler", requireAdmin, async (request, response, next) => {
  try {
    const paused = Boolean(request.body?.paused);
    await writeFragranticaState("crawler", { paused });
    await appendAudit(request, paused ? "fragrantica.crawler.pause" : "fragrantica.crawler.resume", { entityType: "fragrantica_crawler" });
    response.json({ ok: true, paused });
  } catch (error) {
    next(error);
  }
});

// Фото, которых ещё нет на диске (express.static пропускает запрос дальше), скачиваются при первом показе.
app.get("/uploads/fragrantica/:kind/:file", async (request, response, next) => {
  if (!["thumbs", "images"].includes(request.params.kind)) return next();
  const match = String(request.params.file).match(/^(\d+)\.jpg$/);
  if (!match) return response.status(404).end();
  const id = Number(match[1]);
  try {
    const source = request.params.kind === "thumbs" ? fragranticaThumbSource(id) : fragranticaImageSource(id);
    const file = await downloadFragranticaMedia(source, request.params.kind, `${id}.jpg`);
    response.set("Cache-Control", "public, max-age=2592000");
    response.type("jpg").sendFile(file);
  } catch (error) {
    response.status(error instanceof FragranticaHttpError && error.status === 404 ? 404 : 502).end();
  }
});

app.get("/uploads/fragrantica/cards/:file", (request, response) => {
  const file = fragranticaMediaPath("cards", request.params.file);
  if (!fragFs.existsSync(file)) return response.status(404).end();
  response.set("Cache-Control", "public, max-age=2592000");
  response.type("jpg").sendFile(file);
});

// Пересчитать «Есть в PriceMaster» сейчас (обычно — раз в 6 ч на worker).
app.post("/api/fragrantica/pm-match/run", requireAdmin, async (_request, response, next) => {
  try {
    if (fragranticaPmMatchRunning) return response.json({ ok: true, started: false, running: true });
    const wait = runFragranticaPmMatch();
    response.json({ ok: true, started: true, result: await Promise.race([wait, sleep(2000).then(() => null)]) });
  } catch (error) {
    next(error);
  }
});
