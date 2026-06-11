// Customer questions (Ozon only — Yandex has no questions API).
// List: POST /v1/question/list, answer: POST /v1/question/answer/create.

app.get("/api/questions", requireAdmin, async (request, response, next) => {
  try {
    const onlyNew = String(request.query.unanswered ?? "true") !== "false";
    const limit = Math.max(1, Math.min(100, Number(request.query.limit || 50) || 50));
    const questions = [];
    const warnings = [];
    for (const account of getOzonAccounts()) {
      try {
        const data = await ozonRequest("/v1/question/list", {
          filter: onlyNew ? { status: "NEW" } : {},
        }, account);
        const rows = data?.questions || data?.result?.questions || [];
        for (const question of rows.slice(0, limit)) {
          questions.push({
            id: `ozon:${cleanText(question.id)}`,
            marketplace: "ozon",
            target: account.id || "ozon",
            externalId: cleanText(question.id),
            sku: cleanText(question.sku),
            productName: cleanText(question.product_name || question.product_title || ""),
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
