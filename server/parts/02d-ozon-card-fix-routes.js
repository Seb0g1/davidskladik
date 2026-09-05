
async function fetchOzonErrorListItems(account) {
  const visibilities = ["NOT_MODERATED", "STATE_FAILED"];
  const seen = new Set();
  const allItems = [];
  for (const visibility of visibilities) {
    let lastId = "";
    while (true) {
      let data;
      try {
        data = await ozonRequest("/v3/product/list", {
          filter: { visibility },
          limit: 1000,
          last_id: lastId,
        }, account);
      } catch (error) {
        logger.warn("ozon card fix list failed", { visibility, detail: error?.message || String(error) });
        break;
      }
      const batch = data.result?.items || [];
      for (const item of batch) {
        const key = String(item.product_id || item.offer_id);
        if (!seen.has(key)) {
          seen.add(key);
          allItems.push({ ...item, _fetchedVisibility: visibility });
        }
      }
      lastId = data.result?.last_id || "";
      if (!batch.length || !lastId) break;
    }
  }
  return allItems;
}

// GET /api/ozon/card-errors — список карточек с ошибками/на доработку
app.get("/api/ozon/card-errors", async (request, response, next) => {
  try {
    const accountTarget = cleanText(request.query.account || "ozon");
    const account = getOzonAccountByTarget(accountTarget);
    if (!account?.clientId || !account?.apiKey) {
      return response.status(404).json({ error: "Ozon аккаунт не настроен." });
    }

    const warehouse = await readWarehouse();
    const warehouseByOfferId = new Map();
    for (const product of (warehouse.products || [])) {
      const oid = cleanText(product.offerId || product.ozon?.offerId || "");
      if (oid) warehouseByOfferId.set(oid.toLowerCase(), product);
    }

    const listItems = await fetchOzonErrorListItems(account);
    if (!listItems.length) {
      return response.json({ ok: true, items: [], total: 0, accountId: account.id || accountTarget });
    }

    const offerIds = listItems.map((i) => cleanText(i.offer_id)).filter(Boolean);
    const infoMap = await getOzonProductInfoMap(offerIds, account, { continueOnError: true });

    const items = listItems.map((listItem) => {
      const offerId = cleanText(listItem.offer_id || "");
      const info = getOzonOfferMapValue(infoMap, offerId) || {};
      const status = info.status || {};
      const warehouseProduct = offerId ? warehouseByOfferId.get(offerId.toLowerCase()) : null;

      const rawErrors = [
        ...(Array.isArray(info.errors) ? info.errors : []),
        ...(Array.isArray(info.validation_errors) ? info.validation_errors : []),
      ];

      const errors = rawErrors.map((err) => ({
        code: cleanText(err.code || ""),
        field: cleanText(err.attribute_name || err.field || ""),
        description: cleanText(err.description || err.message || ""),
        level: cleanText(err.level || "error"),
      }));

      if (!errors.length && cleanText(status.state_description)) {
        errors.push({
          code: "moderation",
          field: "",
          description: cleanText(status.state_description),
          level: "error",
        });
      }

      return {
        productId: String(info.id || listItem.product_id || ""),
        offerId,
        name: cleanText(info.name || warehouseProduct?.name || ""),
        description: cleanText(info.description || warehouseProduct?.ozon?.description || warehouseProduct?.description || ""),
        primaryImage: cleanText(info.primary_image || warehouseProduct?.imageUrl || ""),
        visibility: cleanText(listItem.visibility || listItem._fetchedVisibility || ""),
        state: cleanText(status.state || ""),
        stateName: cleanText(status.state_name || ""),
        stateDescription: cleanText(status.state_description || ""),
        validationState: cleanText(status.validation_state || ""),
        errors,
        warehouseProductId: warehouseProduct?.id || null,
      };
    });

    response.json({ ok: true, items, total: items.length, accountId: account.id || accountTarget });
  } catch (error) {
    next(error);
  }
});

// POST /api/ozon/card-errors/ai-fix — AI-исправление для одной карточки
app.post("/api/ozon/card-errors/ai-fix", async (request, response, next) => {
  try {
    const { offerId, name, description, errors } = request.body || {};
    if (!cleanText(offerId)) return response.status(400).json({ error: "offerId обязателен." });

    const aiSettings = await readEffectiveAiSettings();
    assertTextGenerationConfigured(aiSettings);

    const errorDescriptions = (Array.isArray(errors) ? errors : [])
      .map((e) => cleanText(e.description || e.code || ""))
      .filter(Boolean)
      .join("; ");

    const systemPrompt = [
      "Ты редактор карточек маркетплейса Ozon для парфюмерии и косметики.",
      "Исправь название и описание товара так, чтобы устранить ошибки модерации.",
      "Ошибки: " + (errorDescriptions || "общие нарушения правил Ozon"),
      "Требования: используй только кириллицу, латиницу, цифры и знаки . , ! ? : - — % « » ( ).",
      "Запрещено: HTML-теги, эмодзи, символы ® © ™ ° ✓ → ★ и другие спецсимволы.",
      "Не добавляй медицинских утверждений, агрессивной рекламы, сравнений с конкурентами.",
      "Не используй «лучший», «самый» без подтверждения. Не выдумывай характеристики.",
      "Если в тексте нецензурная или оскорбительная лексика — полностью замени такие слова нейтральными.",
      "Сохрани всю фактическую информацию: бренд, объём, состав, описание аромата.",
      "Верни только JSON: { \"name\": \"...\", \"description\": \"...\" }.",
    ].join(" ");

    const client = getOpenAiClient(aiSettings);
    const completion = await createOpenAiChatCompletionWithFallback(
      client,
      {
        model: aiSettings.textModel,
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: JSON.stringify({ name: cleanText(name), description: cleanText(description) }) },
        ],
        response_format: { type: "json_object" },
        max_tokens: 2000,
        temperature: 0.3,
      },
      { preferCompatible: isCodexSaleAiProvider(aiSettings) },
    );

    const rawContent = completion?.choices?.[0]?.message?.content || "";
    const parsed = extractJsonObjectFromText(rawContent);
    const fixedName = cleanText(parsed.name || "");
    const fixedDescription = cleanText(parsed.description || "");

    if (!fixedName && !fixedDescription) {
      const err = new Error("AI не вернул исправленный текст. Попробуйте ещё раз.");
      err.statusCode = 502;
      err.code = "ai_empty_fix";
      throw err;
    }

    response.json({
      ok: true,
      offerId: cleanText(offerId),
      original: { name: cleanText(name), description: cleanText(description) },
      fixed: {
        name: fixedName || cleanText(name),
        description: fixedDescription || cleanText(description),
      },
      model: cleanText(completion?.model) || aiSettings.textModel,
    });
  } catch (error) {
    next(error);
  }
});

// POST /api/ozon/card-errors/apply — применить исправление и отправить на Ozon
app.post("/api/ozon/card-errors/apply", async (request, response, next) => {
  try {
    const { offerId, warehouseProductId, name, description } = request.body || {};
    if (!cleanText(offerId)) return response.status(400).json({ error: "offerId обязателен." });
    if (!cleanText(name) && !cleanText(description)) {
      return response.status(400).json({ error: "Необходимо передать name или description." });
    }

    const warehouse = await readWarehouse();
    const lowerOfferId = cleanText(offerId).toLowerCase();
    const product = warehouseProductId
      ? (warehouse.products || []).find((p) => p.id === warehouseProductId)
      : (warehouse.products || []).find((p) => cleanText(p.offerId || p.ozon?.offerId || "").toLowerCase() === lowerOfferId);

    if (!product) {
      return response.status(404).json({ error: "Товар не найден на складе.", code: "product_not_found" });
    }

    const fixedName = cleanText(name) || cleanText(product.ozon?.name || product.name || "");
    const fixedDescription = cleanText(description) || cleanText(product.ozon?.description || product.description || "");

    const patchedOzon = normalizeOzonDraft({
      ...(product.ozon || {}),
      name: fixedName,
      description: fixedDescription,
    });

    const patchedProduct = normalizeWarehouseProduct({ ...product, ozon: patchedOzon });

    const sendResult = await sendApprovedOzonProductContent(patchedProduct, { mode: "content" });

    if (!sendResult.ok) {
      const err = new Error(cleanText(sendResult.error) || "Ошибка отправки на Ozon.");
      err.statusCode = 400;
      err.code = sendResult.code || sendResult.error || "ozon_send_failed";
      err.result = sendResult;
      return next(err);
    }

    Object.assign(product, {
      ozon: patchedOzon,
      updatedAt: new Date().toISOString(),
    });

    await writeWarehouseProductPatch([product], { reason: "ozon_card_fix_applied", writeLinks: false });

    response.json({
      ok: true,
      offerId: cleanText(offerId),
      warehouseProductId: product.id,
      sentFields: sendResult.fields || ["name", "description"],
      result: sendResult,
    });

    appendAudit(request, "ozon.card_fix.applied", {
      productId: product.id,
      offerId: cleanText(offerId),
      name: fixedName.slice(0, 200),
    }).catch((err) => logger.warn("ozon card fix audit failed", { detail: err?.message || String(err) }));
  } catch (error) {
    next(error);
  }
});
