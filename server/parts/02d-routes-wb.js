// API-роуты Wildberries: проверка кабинета, справочник предметов, правила и
// импорт Ozon → WB, карточки, фото, цены и остатки FBS.
// Бизнес-правило: закупка поставщика ниже minSupplierPriceRub (деф. 15 000 ₽) —
// товар не загружается, цена не шлётся, остаток обнуляется.

function resolveWbAccountOr404(request, response) {
  const account = getWbAccountByTarget(cleanText(request.query.target || request.body?.target || "wb"));
  if (!account) {
    response.status(400).json({ error: "Кабинет Wildberries не настроен. Добавьте API-токен в настройках маркетплейсов." });
    return null;
  }
  return account;
}

// Товары Ozon по vendorCode карточек WB (vendorCode = наш offerId) + снапшот
// поставщика — общий кусок для отправки цен и остатков.
async function loadWbLinkedOzonProducts(vendorCodes = []) {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) {
    const error = new Error("Postgres недоступен.");
    error.statusCode = 503;
    throw error;
  }
  const codes = [...new Set(vendorCodes.map((value) => cleanText(value).toLowerCase()).filter(Boolean))];
  const byOfferId = new Map();
  const chunkSize = 5000;
  for (let index = 0; index < codes.length; index += chunkSize) {
    const chunk = codes.slice(index, index + chunkSize);
    const rows = await prisma.$queryRaw`
      SELECT id, offer_id, target_stock, archived, raw->'selectedSupplier' AS supplier
      FROM warehouse_products
      WHERE marketplace = 'ozon' AND LOWER(offer_id) = ANY(${chunk})
    `;
    for (const row of rows) {
      const key = cleanText(row.offer_id).toLowerCase();
      if (key && !byOfferId.has(key)) {
        byOfferId.set(key, {
          id: cleanText(row.id),
          offerId: cleanText(row.offer_id),
          targetStock: Number(row.target_stock || 0),
          archived: Boolean(row.archived),
          supplier: row.supplier && typeof row.supplier === "object" ? row.supplier : null,
        });
      }
    }
  }
  // Живая цена поставщика для товаров без снапшота (см. avito: у большинства
  // товаров raw->selectedSupplier пуст).
  const missing = [...byOfferId.values()].filter((product) => !product.supplier).map((product) => product.id);
  if (missing.length) {
    const supplierMap = await loadAvitoSupplierPricingMap(missing);
    for (const product of byOfferId.values()) {
      if (!product.supplier) product.supplier = supplierMap.get(product.id) || null;
    }
  }
  return byOfferId;
}

// --- Кабинет и справочники ---

app.get("/api/wb/ping", async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const [ping, seller] = await Promise.all([
      wbPing(account),
      wbSellerInfo(account).catch(() => null),
    ]);
    response.json({ ok: true, ping, seller });
  } catch (error) {
    next(error);
  }
});

app.get("/api/wb/subjects", async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    response.json({ subjects: await wbSearchSubjects(account, request.query.query, { limit: Number(request.query.limit || 30) || 30 }) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/wb/subjects/:id/characteristics", async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    response.json({ characteristics: await wbSubjectCharacteristics(account, request.params.id) });
  } catch (error) {
    next(error);
  }
});

// Справочник ТН ВЭД по предмету (subjectId — из query или правил импорта).
app.get("/api/wb/tnved", async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const subjectId = Number(request.query.subjectId || 0) || (await readWbImportRules()).subjectId;
    response.json({ subjectId, tnved: await wbTnvedList(account, subjectId, request.query.search) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/wb/warehouses", async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    response.json({ warehouses: await wbWarehouses(account) });
  } catch (error) {
    next(error);
  }
});

// --- Правила импорта ---

app.get("/api/wb/import/rules", async (_request, response, next) => {
  try {
    response.json(await readWbImportRules());
  } catch (error) {
    next(error);
  }
});

app.put("/api/wb/import/rules", requireAdmin, async (request, response, next) => {
  try {
    const before = await readWbImportRules();
    const saved = await writeWbImportRules(request.body || {});
    await appendAudit(request, "wb.import.rules.save", { oldValue: before, newValue: saved });
    response.json(saved);
  } catch (error) {
    next(error);
  }
});

// --- Импорт Ozon → WB ---

// Предпросмотр: сколько товаров пройдёт порог закупки 15 000 ₽ и почему
// остальные отсеяны. Правила можно передать в body без сохранения.
app.post("/api/wb/import/preview", async (request, response, next) => {
  try {
    const rules = request.body && Object.keys(request.body).length ? request.body : null;
    const { evaluated, total, rules: effectiveRules } = await collectWbImportCandidates({ rules });
    const summary = summarizeWbImportPreview(evaluated);
    response.json({
      total,
      minSupplierPriceRub: effectiveRules.minSupplierPriceRub,
      subjectId: effectiveRules.subjectId,
      subjectName: effectiveRules.subjectName,
      ...summary,
      sample: evaluated
        .filter(({ result }) => result.ok)
        .slice(0, 30)
        .map(({ result }) => ({
          vendorCode: result.listing.vendorCode,
          title: result.listing.title,
          purchaseRub: result.listing.purchaseRub,
          priceRub: result.listing.priceRub,
        })),
      skippedSample: evaluated
        .filter(({ result }) => !result.ok)
        .slice(0, 30)
        .map(({ product, result }) => ({ offerId: product.offerId, name: product.name, reasons: result.reasons })),
    });
  } catch (error) {
    next(error);
  }
});

// Создание карточек на WB. Асинхронно на стороне WB: результат смотреть в
// GET /api/wb/cards/errors и GET /api/wb/cards. Фото досылаются отдельно
// через POST /api/wb/media/backfill после присвоения nmID.
app.post("/api/wb/import/apply", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const limit = Math.max(1, Math.min(1000, Number(request.body?.limit || 200) || 200));
    const rules = request.body?.rules && Object.keys(request.body.rules).length ? request.body.rules : null;
    const { evaluated, rules: effectiveRules } = await collectWbImportCandidates({ rules });
    if (!effectiveRules.subjectId) {
      return response.status(400).json({ error: "Не задан предмет (subjectId) WB: выберите категорию в правилах импорта (поиск: GET /api/wb/subjects?query=духи)." });
    }

    // Уже существующие карточки не пересоздаём — сверяемся по vendorCode.
    const existingCards = await wbCardsList(account);
    const existingVendorCodes = new Set(existingCards.map((card) => cleanText(card.vendorCode).toLowerCase()).filter(Boolean));
    const candidates = evaluated
      .filter(({ result }) => result.ok && !existingVendorCodes.has(result.listing.vendorCode.toLowerCase()))
      .slice(0, limit)
      .map(({ result }) => result.listing);

    // Характеристика «ТНВЭД» — автозаполнение из справочника WB по предмету
    // (или код из правил). Ошибка справочника не блокирует импорт.
    let tnvedCharc = null;
    try {
      tnvedCharc = await resolveWbTnvedCharacteristic(account, effectiveRules);
    } catch (error) {
      logger.warn("wb tnved resolve failed", { detail: error?.message || String(error) });
    }

    // WB требует уникальный штрихкод на размер: недостающие генерируем API.
    const withoutBarcode = candidates.filter((listing) => !listing.barcode);
    if (withoutBarcode.length) {
      const generated = await wbGenerateBarcodes(account, withoutBarcode.length);
      withoutBarcode.forEach((listing, index) => {
        listing.barcode = generated[index] || "";
      });
    }
    const ready = candidates.filter((listing) => listing.barcode);

    let uploaded = 0;
    const errors = [];
    for (const chunk of chunkArray(ready, 50)) {
      try {
        await wbCardsUpload(account, chunk.map((listing) => buildWbCardPayload(listing, tnvedCharc)));
        uploaded += chunk.length;
      } catch (error) {
        errors.push({ vendorCodes: chunk.map((listing) => listing.vendorCode), error: error?.message || String(error) });
      }
    }
    await appendAudit(request, "wb.import.apply", { newValue: { uploaded, candidates: candidates.length, errors: errors.length } });
    response.json({
      ok: errors.length === 0,
      candidates: candidates.length,
      alreadyOnWb: existingVendorCodes.size,
      uploaded,
      skippedNoBarcode: candidates.length - ready.length,
      errors: errors.slice(0, 20),
      hint: "Создание карточек на WB асинхронное: ошибки — GET /api/wb/cards/errors, фото — POST /api/wb/media/backfill.",
    });
  } catch (error) {
    next(error);
  }
});

// --- Карточки ---

app.get("/api/wb/cards", async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const cards = await wbCardsList(account, {
      limit: Number(request.query.limit || 100) || 100,
      textSearch: cleanText(request.query.query),
    });
    response.json({ total: cards.length, cards });
  } catch (error) {
    next(error);
  }
});

app.get("/api/wb/cards/errors", async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const errors = await wbCardErrors(account);
    response.json({ total: errors.length, errors: errors.slice(0, 200) });
  } catch (error) {
    next(error);
  }
});

// Дозаполнение ТН ВЭД в уже созданных карточках WB (код — из правил или
// первый из справочника WB по предмету).
app.post("/api/wb/tnved/backfill", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const limit = Math.max(1, Math.min(20000, Number(request.body?.limit || 20000) || 20000));
    const result = await backfillWbTnvedCharacteristics(account, { limit });
    await appendAudit(request, "wb.tnved.backfill", { newValue: { updated: result.updated, missingTnved: result.missingTnved, tnved: result.tnved } });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

// Досылка фото с нашего склада (Ozon-картинки) в карточки WB без фото.
app.post("/api/wb/media/backfill", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const limit = Math.max(1, Math.min(500, Number(request.body?.limit || 200) || 200));
    const cards = await wbCardsList(account);
    const withoutPhoto = cards
      .filter((card) => Number(card.nmID) > 0 && !(Array.isArray(card.photos) && card.photos.length))
      .slice(0, limit);
    const linked = await loadWbLinkedOzonProducts(withoutPhoto.map((card) => card.vendorCode));
    const prisma = getPrisma();

    let sent = 0;
    let skippedNoImages = 0;
    const errors = [];
    for (const card of withoutPhoto) {
      const product = linked.get(cleanText(card.vendorCode).toLowerCase());
      let imageUrls = [];
      if (product && prisma) {
        const row = await prisma.warehouseProduct.findUnique({ where: { id: product.id }, select: { raw: true } });
        const normalized = row?.raw && typeof row.raw === "object" ? normalizeWarehouseProduct(row.raw) : null;
        if (normalized) imageUrls = wbExtractImageUrls(normalized);
      }
      if (!imageUrls.length) {
        skippedNoImages += 1;
        continue;
      }
      const result = await wbMediaSave(account, card.nmID, imageUrls.slice(0, 10));
      if (result.ok) sent += 1;
      else errors.push({ vendorCode: card.vendorCode, nmID: card.nmID, error: result.error || "media_save_failed" });
    }
    await appendAudit(request, "wb.media.backfill", { newValue: { sent, skippedNoImages, errors: errors.length } });
    response.json({ ok: errors.length === 0, cardsWithoutPhoto: withoutPhoto.length, sent, skippedNoImages, errors: errors.slice(0, 20) });
  } catch (error) {
    next(error);
  }
});

// Статус товаров склада на WB — для панели Wildberries в карточке товара:
// есть ли карточка на WB (vendorCode = наш offerId), nmID и фото, закупка и
// цена WB, порог 15 000 ₽; если карточки нет — причины пропуска импорта.
app.post("/api/wb/product-status", async (request, response, next) => {
  try {
    const productIds = (Array.isArray(request.body?.productIds) ? request.body.productIds : [])
      .map((value) => cleanText(value))
      .filter(Boolean)
      .slice(0, 20);
    const account = getWbAccountByTarget(cleanText(request.body?.target || "wb"));
    if (!productIds.length) return response.json({ configured: Boolean(account), items: [] });

    const prisma = getPrisma();
    if (!prisma || !shouldUsePostgresStorage()) {
      return response.json({ configured: Boolean(account), items: productIds.map((productId) => ({ productId, onWb: false, reasons: ["postgres_unavailable"] })) });
    }
    const [rules, pricing, supplierMap, rows] = await Promise.all([
      readWbImportRules(),
      loadAvitoPricingContext(),
      loadAvitoSupplierPricingMap(productIds),
      prisma.warehouseProduct.findMany({
        where: { id: { in: productIds } },
        select: { id: true, marketplace: true, raw: true },
      }),
    ]);
    const rowById = new Map(rows.map((row) => [cleanText(row.id), row]));

    // Карточки WB по vendorCode: точечный textSearch — в карточке товара
    // максимум несколько строк Ozon, полный список не нужен.
    const cardByVendorCode = new Map();
    let wbError = "";
    if (account) {
      const vendorCodes = new Set();
      for (const row of rows) {
        if (row.marketplace !== "ozon") continue;
        const offerId = cleanText(row.raw && typeof row.raw === "object" ? row.raw.offerId : "");
        if (offerId) vendorCodes.add(offerId);
      }
      for (const vendorCode of vendorCodes) {
        try {
          const found = await wbCardsList(account, { textSearch: vendorCode, limit: 20 });
          for (const card of found) {
            const key = cleanText(card.vendorCode).toLowerCase();
            if (key && !cardByVendorCode.has(key)) cardByVendorCode.set(key, card);
          }
        } catch (error) {
          wbError = error?.message || String(error);
          break;
        }
      }
    }

    const items = [];
    for (const productId of productIds) {
      const row = rowById.get(productId);
      if (!row || row.marketplace !== "ozon") {
        items.push({ productId, onWb: false, reasons: [row ? "not_ozon" : "not_found"] });
        continue;
      }
      const product = row.raw && typeof row.raw === "object" ? normalizeWarehouseProduct(row.raw) : null;
      if (!product) {
        items.push({ productId, onWb: false, reasons: ["not_found"] });
        continue;
      }
      const supplier = supplierMap.get(productId) || product.selectedSupplier || null;
      const purchaseRub = Math.round(wbSupplierPurchaseRub(supplier, pricing));
      const priceRub = purchaseRub > 0 ? wbSupplierPriceRub(supplier, pricing) : 0;
      const evaluated = evaluateWbImportCandidate(product, rules, {
        ...pricing,
        supplierByProductId: new Map([[productId, supplier]]),
      });
      const card = cardByVendorCode.get(cleanText(product.offerId).toLowerCase()) || null;
      const belowMin = purchaseRub > 0 && purchaseRub < rules.minSupplierPriceRub;
      const sellable = Boolean(card) && !product.archived && !belowMin && purchaseRub > 0;
      items.push({
        productId,
        onWb: Boolean(card),
        nmID: card ? Number(card.nmID) || 0 : 0,
        vendorCode: card ? cleanText(card.vendorCode) : cleanText(product.offerId),
        hasPhotos: card ? Boolean(Array.isArray(card.photos) && card.photos.length) : false,
        purchaseRub,
        priceRub,
        minSupplierPriceRub: rules.minSupplierPriceRub,
        belowMin,
        // Ниже порога товар гасится синком остатков даже при созданной карточке.
        sellable,
        // Остаток FBS, который выставит синк: defaultStock или 0.
        stock: sellable ? rules.defaultStock : 0,
        reasons: evaluated.ok ? [] : evaluated.reasons,
      });
    }
    response.json({ configured: Boolean(account), wbError: wbError || undefined, items });
  } catch (error) {
    next(error);
  }
});

// Результат и лог последнего запуска цепочки импорта WB на сервере
// (scripts/prod-wb-chain.cjs) — наблюдение за импортом без SSH.
app.get("/api/wb/chain/result", requireAdmin, async (request, response, next) => {
  try {
    let result = null;
    try {
      result = JSON.parse(await fs.readFile(path.join(dataDir, "wb-chain-result.json"), "utf8"));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    let logTail = "";
    try {
      const log = await fs.readFile(path.join(dataDir, "wb-chain.log"), "utf8");
      logTail = log.split("\n").slice(-120).join("\n");
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    response.json({ result, logTail });
  } catch (error) {
    next(error);
  }
});

// --- Цены ---

// Отправка цен: закупка поставщика × наценка WB. Ниже порога 15 000 ₽ — цена
// не шлётся (skippedBelowMin), товар гасится синком остатков.
app.post("/api/wb/prices/send", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const rules = await readWbImportRules();
    const pricing = await loadAvitoPricingContext();
    const onlyVendorCodes = new Set((Array.isArray(request.body?.vendorCodes) ? request.body.vendorCodes : [])
      .map((value) => cleanText(value).toLowerCase())
      .filter(Boolean));

    const cards = await wbCardsList(account);
    const targetCards = cards.filter((card) => (
      Number(card.nmID) > 0 && (!onlyVendorCodes.size || onlyVendorCodes.has(cleanText(card.vendorCode).toLowerCase()))
    ));
    const linked = await loadWbLinkedOzonProducts(targetCards.map((card) => card.vendorCode));

    const items = [];
    let skippedBelowMin = 0;
    let skippedNoSupplier = 0;
    let skippedNotLinked = 0;
    for (const card of targetCards) {
      const product = linked.get(cleanText(card.vendorCode).toLowerCase());
      if (!product) {
        skippedNotLinked += 1;
        continue;
      }
      const purchaseRub = wbSupplierPurchaseRub(product.supplier, pricing);
      if (!(purchaseRub > 0)) {
        skippedNoSupplier += 1;
        continue;
      }
      if (purchaseRub < rules.minSupplierPriceRub) {
        skippedBelowMin += 1;
        continue;
      }
      const priceRub = wbSupplierPriceRub(product.supplier, pricing);
      if (priceRub > 0) items.push({ nmID: card.nmID, price: priceRub, discount: 0 });
    }

    const dryRun = request.body?.dryRun === true;
    const result = dryRun ? { ok: true, sent: 0 } : await wbSetPrices(account, items);
    if (!dryRun) await appendAudit(request, "wb.prices.send", { newValue: { prepared: items.length, skippedBelowMin } });
    response.json({
      ok: result.ok,
      dryRun,
      cards: targetCards.length,
      prepared: items.length,
      sent: dryRun ? 0 : items.length,
      skippedBelowMin,
      skippedNoSupplier,
      skippedNotLinked,
      minSupplierPriceRub: rules.minSupplierPriceRub,
      tasks: result.tasks || [],
      sample: items.slice(0, 20),
    });
  } catch (error) {
    next(error);
  }
});

// --- Остатки (FBS) ---

// Синк остатков: defaultStock при валидном поставщике с закупкой ≥ порога,
// иначе 0 (товар снимается с продажи). warehouseId — из body или campaignId
// кабинета (список складов: GET /api/wb/warehouses).
app.post("/api/wb/stocks/sync", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveWbAccountOr404(request, response);
    if (!account) return;
    const warehouseId = Number(request.body?.warehouseId || account.campaignId || 0);
    if (!warehouseId) {
      return response.status(400).json({ error: "Нужен warehouseId склада WB (FBS): передайте в body или сохраните в поле Campaign ID кабинета. Список: GET /api/wb/warehouses." });
    }
    const rules = await readWbImportRules();
    const pricing = await loadAvitoPricingContext();
    // Опциональный фильтр: трогаем только перечисленные vendorCode — карточки,
    // созданные на WB вручную (не из нашего импорта), синк иначе обнулит.
    const onlyVendorCodes = new Set((Array.isArray(request.body?.vendorCodes) ? request.body.vendorCodes : [])
      .map((value) => cleanText(value).toLowerCase())
      .filter(Boolean));
    const allCards = await wbCardsList(account);
    const cards = onlyVendorCodes.size
      ? allCards.filter((card) => onlyVendorCodes.has(cleanText(card.vendorCode).toLowerCase()))
      : allCards;
    const linked = await loadWbLinkedOzonProducts(cards.map((card) => card.vendorCode));

    const stocks = [];
    let inStock = 0;
    let zeroed = 0;
    for (const card of cards) {
      const skus = (Array.isArray(card.sizes) ? card.sizes : []).flatMap((size) => (Array.isArray(size.skus) ? size.skus : []));
      if (!skus.length) continue;
      const product = linked.get(cleanText(card.vendorCode).toLowerCase());
      const purchaseRub = product ? wbSupplierPurchaseRub(product.supplier, pricing) : 0;
      const sellable = product && !product.archived && purchaseRub >= rules.minSupplierPriceRub;
      const amount = sellable ? rules.defaultStock : 0;
      if (sellable) inStock += 1;
      else zeroed += 1;
      for (const sku of skus) stocks.push({ sku, amount });
    }

    const dryRun = request.body?.dryRun === true;
    const result = dryRun ? { ok: true, sent: 0 } : await wbUpdateStocks(account, warehouseId, stocks);
    if (!dryRun) await appendAudit(request, "wb.stocks.sync", { newValue: { inStock, zeroed, warehouseId } });
    response.json({
      ok: result.ok,
      dryRun,
      warehouseId,
      cards: cards.length,
      inStock,
      zeroed,
      sent: dryRun ? 0 : result.sent,
      minSupplierPriceRub: rules.minSupplierPriceRub,
    });
  } catch (error) {
    next(error);
  }
});
