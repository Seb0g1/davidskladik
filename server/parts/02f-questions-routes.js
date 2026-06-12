// Customer questions (Ozon only — Yandex has no questions API).
// List: POST /v1/question/list, answer: POST /v1/question/answer/create.
// Templates live in data/question-templates.json (id, title, text).

const questionTemplatesPath = path.join(dataDir, "question-templates.json");

async function readQuestionTemplates() {
  try {
    const parsed = JSON.parse(await fs.readFile(questionTemplatesPath, "utf8"));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function writeQuestionTemplates(templates) {
  await fs.mkdir(dataDir, { recursive: true }).catch(() => {});
  await fs.writeFile(questionTemplatesPath, JSON.stringify(templates, null, 2), "utf8");
}

// SKU -> product name cache (Ozon question/review payloads carry only the SKU).
const ozonSkuNameCache = new Map();
async function resolveOzonSkuNames(skus = [], account) {
  const result = new Map();
  const missing = [];
  for (const sku of new Set(skus.map((value) => cleanText(value)).filter(Boolean))) {
    const cached = ozonSkuNameCache.get(sku);
    if (cached && Date.now() - cached.at < 30 * 60_000) result.set(sku, cached.name);
    else missing.push(sku);
  }
  for (const chunk of chunkArray(missing, 100)) {
    try {
      const data = await ozonRequest("/v3/product/info/list", { sku: chunk.map((value) => Number(value) || value) }, account);
      for (const item of data?.items || data?.result?.items || []) {
        const sku = cleanText(item.sku || (item.sources || []).find((sourceItem) => sourceItem.sku)?.sku);
        const name = cleanText(item.name);
        if (!name) continue;
        for (const key of [sku, cleanText(item.id)]) {
          if (!key) continue;
          ozonSkuNameCache.set(key, { name, at: Date.now() });
          result.set(key, name);
        }
      }
    } catch {
      /* name enrichment is best-effort */
    }
  }
  return result;
}

app.get("/api/questions", requireAdmin, async (request, response, next) => {
  try {
    const onlyNew = String(request.query.unanswered ?? "true") !== "false";
    const limit = Math.max(1, Math.min(500, Number(request.query.limit || 100) || 100));
    const questions = [];
    const warnings = [];
    for (const account of getOzonAccounts()) {
      try {
        const rows = [];
        let lastId = "";
        for (let pageIndex = 0; pageIndex < 10 && rows.length < limit; pageIndex += 1) {
          const data = await ozonRequest("/v1/question/list", {
            filter: onlyNew ? { status: "NEW" } : {},
            ...(lastId ? { last_id: lastId } : {}),
          }, account);
          const batch = data?.questions || data?.result?.questions || [];
          rows.push(...batch);
          lastId = cleanText(data?.last_id || data?.result?.last_id || "");
          if (!batch.length || !lastId) break;
        }
        const nameBySku = await resolveOzonSkuNames(rows.map((question) => question.sku), account);
        for (const question of rows.slice(0, limit)) {
          questions.push({
            id: `ozon:${cleanText(question.id)}`,
            marketplace: "ozon",
            target: account.id || "ozon",
            externalId: cleanText(question.id),
            sku: cleanText(question.sku),
            productName: cleanText(question.product_name || question.product_title || "") || nameBySku.get(cleanText(question.sku)) || "",
            productUrl: cleanText(question.product_url || ""),
            text: cleanText(question.text || ""),
            authorName: cleanText(question.author_name || ""),
            createdAt: cleanText(question.published_at || question.created_at || ""),
            status: cleanText(question.status || ""),
            answersCount: Number(question.answers_count || 0) || 0,
            needsAnswer: cleanText(question.status).toUpperCase() !== "PROCESSED" && !(Number(question.answers_count || 0) > 0),
          });
        }
      } catch (error) {
        warnings.push(`Ozon ${account.id}: ${error?.message || "ошибка"}`);
      }
    }
    questions.sort((a, b) => cleanText(b.createdAt).localeCompare(cleanText(a.createdAt)));
    response.json({ ok: true, rows: questions.slice(0, limit), warnings });
  } catch (error) {
    next(error);
  }
});

app.post("/api/questions/reply", requireAdmin, async (request, response, next) => {
  try {
    const externalId = cleanText(request.body?.externalId);
    const sku = Number(request.body?.sku || 0) || undefined;
    const target = cleanText(request.body?.target);
    const text = cleanText(request.body?.text);
    if (!externalId || !text) return response.status(400).json({ error: "Нужны externalId и text." });
    const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
    if (!account) return response.status(400).json({ error: "Ozon аккаунт не найден." });
    const result = await ozonRequest("/v1/question/answer/create", {
      question_id: externalId,
      ...(sku ? { sku } : {}),
      text,
    }, account);
    await appendAudit(request, "questions.reply", { entityType: "question", entityId: `ozon:${externalId}` });
    response.json({ ok: true, result });
  } catch (error) {
    next(error);
  }
});

app.get("/api/questions/templates", requireAdmin, async (_request, response, next) => {
  try {
    response.json({ ok: true, templates: await readQuestionTemplates() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/questions/templates", requireAdmin, async (request, response, next) => {
  try {
    const templates = await readQuestionTemplates();
    const title = cleanText(request.body?.title);
    const text = cleanText(request.body?.text);
    if (!title || !text) return response.status(400).json({ error: "Нужны title и text." });
    const id = cleanText(request.body?.id) || crypto.randomUUID();
    const existingIndex = templates.findIndex((item) => item.id === id);
    const template = { id, title: title.slice(0, 80), text: text.slice(0, 1500), updatedAt: new Date().toISOString() };
    if (existingIndex >= 0) templates[existingIndex] = template;
    else templates.push(template);
    await writeQuestionTemplates(templates);
    response.json({ ok: true, template, templates });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/questions/templates/:id", requireAdmin, async (request, response, next) => {
  try {
    const templates = await readQuestionTemplates();
    const next_ = templates.filter((item) => item.id !== cleanText(request.params.id));
    await writeQuestionTemplates(next_);
    response.json({ ok: true, templates: next_ });
  } catch (error) {
    next(error);
  }
});
