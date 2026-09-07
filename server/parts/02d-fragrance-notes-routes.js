// Fragrance notes management routes:
// POST /api/warehouse/fragrance-notes/generate  — generate notes for ONE product via LLM
// POST /api/warehouse/fragrance-notes/batch     — batch generate for all products without notes
// PATCH /api/warehouse/fragrance-notes/:id      — manual notes save from admin UI
// GET  /api/warehouse/fragrance-notes/progress  — batch progress (SSE or poll)

const _fnBatch = { running: false, total: 0, done: 0, errors: 0, startedAt: null };

// ── LLM notes generation ─────────────────────────────────────────────────────

async function generateNotesForProduct(brand, name) {
  const aiSettings = await readEffectiveAiSettings();
  if (!isOpenAiDirectConfigured(aiSettings)) return null;

  const client = getOpenAiClient(aiSettings);
  const prompt = `You are a fragrance expert. Return JSON with the fragrance notes for "${name}" by ${brand}.
JSON format: {"topNotes":[],"middleNotes":[],"baseNotes":[],"accords":[],"gender":"male|female|unisex","seasons":["spring|summer|fall|winter"]}
- topNotes: 2-4 top/opening notes (e.g. "Bergamot","Lemon")
- middleNotes: 2-4 heart notes
- baseNotes: 2-4 base/dry-down notes
- accords: 2-5 overall accords (e.g. "Woody","Aromatic","Spicy")
- gender: target gender
- seasons: recommended seasons
Return ONLY valid JSON, no other text.`;

  try {
    const completion = await createOpenAiChatCompletionWithFallback(client, {
      model: aiSettings.textModel || "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      max_tokens: 400,
      temperature: 0.2,
      response_format: { type: "json_object" },
    });
    const text = completion.choices[0]?.message?.content || "{}";
    const parsed = extractJsonObjectFromText(text);
    if (!parsed || !Array.isArray(parsed.topNotes)) return null;
    return {
      topNotes: parsed.topNotes.filter(Boolean).slice(0, 6),
      middleNotes: parsed.middleNotes?.filter(Boolean).slice(0, 6) ?? [],
      baseNotes: parsed.baseNotes?.filter(Boolean).slice(0, 6) ?? [],
      accords: parsed.accords?.filter(Boolean).slice(0, 8) ?? [],
      gender: parsed.gender || "unisex",
      seasons: Array.isArray(parsed.seasons) ? parsed.seasons.filter(Boolean) : [],
      source: "llm",
      generatedAt: new Date().toISOString(),
    };
  } catch {
    return null;
  }
}

// ── Single product ────────────────────────────────────────────────────────────

app.post("/api/warehouse/fragrance-notes/generate", requireAuth, async (request, response, next) => {
  try {
    const { productId, brand, name } = request.body || {};
    if (!productId || !brand || !name) {
      return response.status(400).json({ error: "productId, brand and name are required" });
    }
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "DB unavailable" });

    const notes = await generateNotesForProduct(brand, name);
    if (!notes) return response.status(500).json({ error: "AI generation failed or not configured" });

    await prisma.warehouseProduct.update({
      where: { id: productId },
      data: { fragranceNotes: notes },
    });

    response.json({ ok: true, notes });
  } catch (error) { next(error); }
});

// ── Manual save ───────────────────────────────────────────────────────────────

app.patch("/api/warehouse/fragrance-notes/:id", requireAuth, async (request, response, next) => {
  try {
    const { id } = request.params;
    const { topNotes, middleNotes, baseNotes, accords, gender, seasons } = request.body || {};
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "DB unavailable" });

    const notes = {
      topNotes: Array.isArray(topNotes) ? topNotes.filter(Boolean) : [],
      middleNotes: Array.isArray(middleNotes) ? middleNotes.filter(Boolean) : [],
      baseNotes: Array.isArray(baseNotes) ? baseNotes.filter(Boolean) : [],
      accords: Array.isArray(accords) ? accords.filter(Boolean) : [],
      gender: gender || "unisex",
      seasons: Array.isArray(seasons) ? seasons.filter(Boolean) : [],
      source: "manual",
      generatedAt: new Date().toISOString(),
    };

    await prisma.warehouseProduct.update({ where: { id }, data: { fragranceNotes: notes } });
    response.json({ ok: true, notes });
  } catch (error) { next(error); }
});

// ── Batch generate ────────────────────────────────────────────────────────────

app.get("/api/warehouse/fragrance-notes/batch/status", requireAuth, (_req, response) => {
  response.json({ ..._fnBatch });
});

app.post("/api/warehouse/fragrance-notes/batch", requireAuth, async (request, response, next) => {
  try {
    if (_fnBatch.running) {
      return response.json({ ok: true, message: "already running", ..._fnBatch });
    }

    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "DB unavailable" });

    const aiSettings = await readEffectiveAiSettings();
    if (!isOpenAiDirectConfigured(aiSettings)) {
      return response.status(503).json({ error: "AI не настроен. Добавьте OpenAI API ключ в настройках." });
    }

    // Count products without notes — use raw SQL because Prisma requires Prisma.DbNull for JSONB null filter
    const countRows = await prisma.$queryRaw`
      SELECT COUNT(*)::int AS count FROM warehouse_products
      WHERE archived = false AND brand IS NOT NULL AND fragrance_notes IS NULL
    `;
    const total = Number(countRows[0]?.count ?? 0);

    if (!total) return response.json({ ok: true, message: "Все товары уже имеют ноты", total: 0, done: 0 });

    // Start async batch
    _fnBatch.running = true;
    _fnBatch.total = total;
    _fnBatch.done = 0;
    _fnBatch.errors = 0;
    _fnBatch.startedAt = new Date().toISOString();

    response.json({ ok: true, message: "Batch started", total, done: 0 });

    // Run in background — don't await
    (async () => {
      const PAGE = 50;
      let offset = 0;
      while (true) {
        const products = await prisma.$queryRaw`
          SELECT id, brand, name FROM warehouse_products
          WHERE archived = false AND brand IS NOT NULL AND fragrance_notes IS NULL
          ORDER BY id
          LIMIT ${PAGE} OFFSET ${offset}
        `;
        if (!products.length) break;

        for (const p of products) {
          try {
            const notes = await generateNotesForProduct(p.brand, p.name);
            if (notes) {
              await prisma.warehouseProduct.update({ where: { id: p.id }, data: { fragranceNotes: notes } });
            } else {
              _fnBatch.errors++;
            }
          } catch {
            _fnBatch.errors++;
          }
          _fnBatch.done++;
          // Small delay to avoid hammering the AI API
          await new Promise((r) => setTimeout(r, 200));
        }
        offset += PAGE;
      }
      _fnBatch.running = false;
    })().catch(() => { _fnBatch.running = false; });
  } catch (error) { next(error); }
});
