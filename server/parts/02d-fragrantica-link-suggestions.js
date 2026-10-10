// «Предложения привязки» в форме «Добавить на Ozon» (страница «Фрагрантика»).
//
//   GET  /api/fragrantica/ozon/link-suggestions — строки PriceMaster по «бренд + название + мл»:
//        только доступные, без строк другого объёма (supplierRowVolumeMismatch — те же правила, что
//        у склада), тестеры только для тестера, без пробников; рекомендованные — с совпадением
//        названия и объёма. Для каждой — цена Ozon по правилам наценки.
//   POST /api/fragrantica/ozon/price-preview   — цена карточки по выбранным строкам (как у склада:
//        наценка по закупке + поправка за число поставщиков, берётся самая низкая) и цена до скидки.
//
// После создания карточки (imported) товар сразу заводится на склад и получает выбранные привязки —
// дальше цена и остаток идут обычной активацией привязанных товаров (applyFragranticaExportLinks).

async function fragranticaSearchPmRows(q, limit = 60) {
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const tokenGroups = pmQueryToTokenGroups(q);
  const rows = [];
  const seen = new Set();
  const push = (mapped) => {
    const key = `${cleanText(mapped.partnerId)}|${cleanText(mapped.article).toLowerCase()}|${cleanText(mapped.name).toLowerCase()}`;
    if (seen.has(key) || seen.has(`row:${mapped.rowId}`)) return;
    seen.add(key);
    if (mapped.rowId) seen.add(`row:${mapped.rowId}`);
    rows.push(mapped);
  };
  try {
    if (tokenGroups && tokenGroups.length) {
      const cte = await pmLatestDocsCteSql();
      const clause = pmBuildMysqlSearchClause(tokenGroups, { name: "r.NativeName", article: "r.NativeID", barcode: "r.BarCode" });
      const [liveRows] = await pool.query(
        `${cte}
         SELECT r.NativeID AS article, r.NativeName AS name, r.BarCode AS barcode, r.NativePrice AS price,
                r.Active AS active, r.RowID AS rowId, d.DocDate AS docDate, d.PartnerID AS partnerId,
                p.PartnerName AS partnerName, ${clause.scoreSql} AS matchScore
         FROM pm_latest_docs ld
         JOIN OfferDocs d ON d.DocID = ld.DocID
         JOIN OfferRows r ON r.DocID = d.DocID
         LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
         WHERE r.Ignored = 0 AND r.Active != 0 AND (${clause.where})
         ORDER BY matchScore DESC, d.DocDate DESC, r.RowID DESC
         LIMIT ?`,
        [...clause.scoreParams, ...clause.params, limit * 10],
      );
      for (const row of liveRows) {
        const hay = [cleanText(row.name), cleanText(row.article), cleanText(row.barcode)].join(" ");
        if (!pmPassesSearchFilter(hay, tokenGroups)) continue;
        push(mapPriceMasterSearchResponseRow(row, usdRate));
      }
    }
  } catch (error) {
    logger.warn("fragrantica pm search live failed, using snapshot", { detail: error?.message || String(error) });
  }
  const snapshotRows = await searchPriceMasterSnapshotOffers({ search: q, partner: "", limit: limit * 2, usdRate, tokenGroups }).catch(() => []);
  for (const row of snapshotRows || []) push(mapPriceMasterSearchResponseRow(row, usdRate));
  return { rows: rows.slice(0, limit * 3), usdRate, settings };
}

function fragranticaRowRubContext(row = {}) {
  const currency = cleanText(row.priceCurrency || row.currency).toUpperCase();
  return { ...row, rubNative: currency === "RUB" || currency === "RUR" };
}

function fragranticaRowPrice(row, { usdRate, settings, supplierCount = 1, marketplace = "ozon" }) {
  const baseMarkup = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace,
    supplierUsdPrice: row.price,
    supplierPriceCurrency: row.priceCurrency || row.currency,
    usdRate,
    appSettings: settings,
  });
  const policy = resolveAvailabilityPolicy({ marketplace, availableSupplierCount: supplierCount, baseMarkup, appSettings: settings });
  const markup = Number(policy?.markupCoefficient || baseMarkup);
  return { markup, price: calculateRubPrice(row.price, usdRate, markup, fragranticaRowRubContext(row)) };
}

async function fragranticaLinkSuggestionsData(query = {}) {
  const perfume = await fragranticaPerfumeForExport(Number(query.perfumeId));
  const typeKey = FRAG_OZON_TYPES.some((t) => t.key === query.typeKey) ? query.typeKey : fragOzonGuessTypeKey(perfume);
  const volume = Number(fragFormatVolume(query.volume)) || 0;
  const tester = query.tester === "1" || query.tester === "true" || query.tester === true;
  const productName = buildFragranticaOzonName({ perfume, typeKey, volume, tester });
  const anchor = `${fragNameWithBrand(perfume)}${volume ? ` ${volume}ml` : ""}${tester ? " tester" : ""}`;
  const custom = cleanText(query.q);
  const queries = custom
    ? [custom]
    : [...new Set([`${fragNameWithBrand(perfume)} ${volume || ""}`.trim(), fragNameWithBrand(perfume)])];

  let found = [];
  let usdRate = 95;
  let settings = {};
  // Conveyor: candidate rows from the in-memory PriceMaster index (no SQL per volume)
  const preloaded = !custom && Array.isArray(query.rows) && query.rows.length ? query.rows : null;
  if (preloaded) {
    found = preloaded;
    usdRate = Number(query.usdRate) || usdRate;
    settings = query.settings || settings;
  }
  for (const q of preloaded ? [] : queries) {
    const result = await fragranticaSearchPmRows(q, 60);
    usdRate = result.usdRate;
    settings = result.settings;
    for (const row of result.rows) if (!found.some((r) => r.id === row.id)) found.push(row);
    if (found.length >= 40) break;
  }

  // suppliers excluded on «Подбор поставщиков» are never suggested here either
  const excludedSuppliers = typeof supplierMatchExcludedKeys === "function" ? await supplierMatchExcludedKeys().catch(() => new Set()) : new Set();
  const rows = found
    .filter((row) => row.available)
    .filter((row) => !supplierRowExcluded(row, excludedSuppliers))
    .filter((row) => custom || !supplierRowVolumeMismatch(productName, row.name))
    .filter((row) => custom || isTesterOrDecantSupplierRowName(row.name) === tester)
    .filter((row) => custom || volume <= 3 || !isSingleSampleName(row.name))
    .map((row) => ({ row, check: assessFragranticaSupplierRow(row.name, { brand: perfume.brand, name: fragStripConcentration(perfume.name), typeKey, oilAllowed: typeKey === "oil" }) }))
    // Клоны («… (Sauvage Dior)») и не-парфюм (лосьон, дезодорант, мист…) не предлагаем вовсе
    .filter(({ check }) => custom || (!check.clone && !check.notPerfume))
    .map(({ row, check }) => {
      const volumes = priceMasterBottleVolumes(row.name);
      const volumeOk = Boolean(volume) && volumes.some((v) => Math.abs(v - volume) < 0.01);
      const nameOk = !check.clone && pmRowConfirmsPinnedName(row, anchor);
      // a placeholder / rouble-as-dollar price (Montblanc went out at 668 169 ₽) is never recommended
      const priceProblem = priceGuardRowProblem(row, productName);
      const recommended = !priceProblem && volumeOk && nameOk && !check.notPerfume && check.concentrationOk && !check.extraWords.length && !check.missingNameWords.length;
      const issues = [
        priceProblem,
        check.clone ? "клон/аналог" : "",
        check.notPerfume ? "не парфюм" : "",
        check.concentration && !check.concentrationOk ? `другая концентрация (${check.concentration.toUpperCase()})` : "",
        check.extraWords.length ? `лишние слова: ${check.extraWords.slice(0, 3).join(", ")}` : "",
        check.missingNameWords.length ? `нет слов: ${check.missingNameWords.slice(0, 3).join(", ")}` : "",
        volume && !volumeOk ? "объём не указан" : "",
      ].filter(Boolean);
      const { markup, price } = fragranticaRowPrice(row, { usdRate, settings });
      const yandexPrice = fragranticaRowPrice(row, { usdRate, settings, marketplace: "yandex" }).price;
      return {
        id: row.id,
        rowId: row.rowId,
        article: row.article,
        name: row.name,
        supplierName: row.supplierName,
        partnerId: row.partnerId,
        price: row.price,
        priceCurrency: row.priceCurrency || row.currency || "USD",
        updatedAt: row.updatedAt,
        volumeOk,
        nameOk,
        recommended,
        issues,
        markup,
        ozonPrice: price,
        yandexPrice,
      };
    })
    .sort((a, b) => Number(b.recommended) - Number(a.recommended) || a.issues.length - b.issues.length || Number(b.nameOk) - Number(a.nameOk) || a.ozonPrice - b.ozonPrice)
    .slice(0, 40);

  // Предвыбор: рекомендованные строки — по одной (самой дешёвой) у каждого поставщика, до трёх.
  const suggested = [];
  const suppliers = new Set();
  for (const row of rows) {
    if (!row.recommended || suppliers.has(row.partnerId || row.supplierName)) continue;
    suppliers.add(row.partnerId || row.supplierName);
    suggested.push(row.id);
    if (suggested.length >= 3) break;
  }
  return { ok: true, productName, usdRate, rows, suggested };
}

app.get("/api/fragrantica/ozon/link-suggestions", requireAdmin, async (request, response, next) => {
  try {
    response.json(await fragranticaLinkSuggestionsData(request.query));
  } catch (error) {
    next(error);
  }
});

// Ticked supplier rows of the form are kept in the database per perfume + type + volume + tester, so a page
// reload, another device or a server restart shows the same ticks. No row yet = nobody ticked anything there,
// the form takes the suggested rows.
//   GET /api/fragrantica/ozon/link-picks?perfumeId&typeKey&volume&tester → { rows: LinkRow[] | null }
//   PUT /api/fragrantica/ozon/link-picks { perfumeId, typeKey, volume, tester, rows }
let fragranticaLinkPicksReady = false;
async function ensureFragranticaLinkPicks() {
  if (fragranticaLinkPicksReady) return;
  await requireFragranticaTables();
  await getPrisma().$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_link_picks (
      perfume_id BIGINT NOT NULL,
      type_key TEXT NOT NULL,
      volume TEXT NOT NULL,
      tester BOOLEAN NOT NULL,
      rows JSONB NOT NULL DEFAULT '[]'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (perfume_id, type_key, volume, tester)
    )`);
  fragranticaLinkPicksReady = true;
}

function fragranticaLinkPicksKey(source = {}) {
  const perfumeId = Number(source.perfumeId);
  if (!Number.isFinite(perfumeId) || perfumeId <= 0) {
    const error = new Error("perfumeId не указан");
    error.statusCode = 400;
    throw error;
  }
  const tester = source.tester === true || source.tester === 1 || ["1", "true"].includes(cleanText(source.tester));
  return [perfumeId, cleanText(source.typeKey), cleanText(source.volume), tester];
}

app.get("/api/fragrantica/ozon/link-picks", requireAdmin, async (request, response, next) => {
  try {
    await ensureFragranticaLinkPicks();
    const [row] = await getPrisma().$queryRawUnsafe(
      `SELECT rows FROM fragrantica_link_picks WHERE perfume_id = $1 AND type_key = $2 AND volume = $3 AND tester = $4`,
      ...fragranticaLinkPicksKey(request.query),
    );
    response.json({ rows: row ? row.rows : null });
  } catch (error) {
    next(error);
  }
});

app.put("/api/fragrantica/ozon/link-picks", requireAdmin, async (request, response, next) => {
  try {
    await ensureFragranticaLinkPicks();
    const body = request.body || {};
    const rows = (Array.isArray(body.rows) ? body.rows : [])
      .filter((row) => row && typeof row === "object" && cleanText(row.id))
      .slice(0, 200);
    await getPrisma().$executeRawUnsafe(
      `INSERT INTO fragrantica_link_picks (perfume_id, type_key, volume, tester, rows, updated_at)
       VALUES ($1, $2, $3, $4, $5::jsonb, now())
       ON CONFLICT (perfume_id, type_key, volume, tester) DO UPDATE SET rows = EXCLUDED.rows, updated_at = now()`,
      ...fragranticaLinkPicksKey(body), JSON.stringify(rows),
    );
    response.json({ ok: true, count: rows.length });
  } catch (error) {
    next(error);
  }
});

async function fragranticaPricePreview(rows = []) {
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const cheapest = (marketplace) => rows
    .filter((row) => Number(row.price) > 0)
    // the same guard as the warehouse price send: such a row never sets the card's price
    .filter((row) => !priceGuardRowProblem(row, row.name))
    .map((row) => ({ row, ...fragranticaRowPrice(row, { usdRate, settings, supplierCount: rows.length, marketplace }) }))
    .sort((a, b) => a.price - b.price)[0];
  const best = cheapest("ozon");
  if (!best) return { price: 0, oldPrice: 0, supplierName: "", markup: 0, yandexPrice: 0 };
  const yandex = cheapest("yandex");
  return {
    price: best.price,
    oldPrice: resolveOzonOldPrice(best.price),
    supplierName: cleanText(best.row.supplierName),
    markup: best.markup,
    yandexPrice: yandex?.price || 0,
    yandexMarkup: yandex?.markup || 0,
  };
}

app.post("/api/fragrantica/ozon/price-preview", requireAdmin, async (request, response, next) => {
  try {
    const rows = (Array.isArray(request.body?.rows) ? request.body.rows : []).slice(0, 20);
    response.json({ ok: true, ...(await fragranticaPricePreview(rows)) });
  } catch (error) {
    next(error);
  }
});

function fragranticaLinkDraft(row = {}) {
  return {
    article: cleanText(row.article),
    supplierName: cleanText(row.supplierName),
    keyword: "",
    priceCurrency: cleanText(row.priceCurrency) || "USD",
    partnerId: cleanText(row.partnerId),
    sourceRowId: cleanText(row.rowId || row.id),
    exactName: cleanText(row.name),
    matchType: "selected_row",
  };
}

// Завести созданную карточку на склад (как discovery) и привязать выбранные строки (как кнопка на складе).
async function applyFragranticaExportLinks(row) {
  const drafts = Array.isArray(row.links) ? row.links : [];
  if (!drafts.length) return { links: "none" };
  let productId;
  let product = null;
  if (row.marketplace === "yandex") {
    // The Market row was written by exportOzonProductsToYandex
    const found = await getPrisma().warehouseProduct.findFirst({ where: { marketplace: "yandex", target: cleanText(row.account_id), offerId: row.offer_id }, select: { id: true } })
      || await getPrisma().warehouseProduct.findFirst({ where: { marketplace: "yandex", offerId: row.offer_id }, select: { id: true } });
    if (!found) throw new Error("Карточка Маркета ещё не появилась на складе");
    productId = found.id;
    product = await findWarehouseProductById(productId);
  } else {
    if (!row.product_id) return { links: "none" };
    productId = ozonWarehouseProductId(fragranticaResolveOzonAccount(row.account_id), row.offer_id);
    product = await findWarehouseProductById(productId);
  }
  const account = row.marketplace === "yandex" ? null : fragranticaResolveOzonAccount(row.account_id);
  if (!product) {
    const offerIds = [row.offer_id];
    const [infoMap, stockMap, priceMap] = await Promise.all([
      getOzonProductInfoMap(offerIds, account, { continueOnError: true }),
      getOzonStockMap(offerIds, account, { continueOnError: true }),
      getOzonPriceMap(offerIds, account, { continueOnError: true }),
    ]);
    const imported = buildOzonImportedWarehouseProduct(account, { offer_id: row.offer_id, product_id: Number(row.product_id), name: row.item?.name }, {
      info: getOzonOfferMapValue(infoMap, row.offer_id) || {},
      stockInfo: getOzonOfferMapValue(stockMap, row.offer_id) || {},
      priceInfo: getOzonOfferMapValue(priceMap, row.offer_id) || {},
    });
    await writeWarehouseProductPatch([imported], { reason: "fragrantica_export", writeLinks: false });
    product = await findWarehouseProductById(productId);
    if (!product) throw new Error("Карточка не появилась на складе");
  }
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const username = cleanText(row.created_by) || "fragrantica";
  let added = 0;
  await withWarehouseProductMutationLock([productId], async () => {
    const current = await findWarehouseProductById(productId);
    await hydrateWarehouseProductsForIds([productId], { expandGroups: true });
    const warehouse = await readWarehouse();
    const context = normalizeWarehouseProduct(current);
    const now = new Date().toISOString();
    current.links = Array.isArray(current.links) ? current.links : [];
    for (const draft of drafts) {
      let link = normalizeWarehouseLink(fragranticaLinkDraft(draft));
      link = await resolvePriceMasterLinkForSave(link, usdRate, warehouse.suppliers, { live: true, timeoutMs: 2500, cacheEmpty: false, productContext: context });
      if (current.links.some((item) => warehouseLinksEqualForSave(item, link))) continue;
      current.links.push(normalizeWarehouseLink({ ...link, raw: { ...(link.raw || {}), createdBy: "fragrantica" }, createdAt: now, updatedAt: now, createdBy: username, updatedBy: username }));
      added += 1;
    }
    current.links = compactWarehouseLinks(current.links);
    if (!added) return;
    current.autoPriceEnabled = true;
    current.everHadLinks = true;
    current.updatedAt = now;
    current.userUpdatedAt = now;
    await withWarehouseMutation(async () => {
      await writeWarehouseProductPatch([current], { reason: "warehouse_link_save" });
    });
  });
  if (added) {
    await queueLinkedProductActivation([productId], "fragrantica_export_link", warehouseLinkActivationRequestMeta([productId], { username }))
      .catch((error) => logger.warn("fragrantica link activation failed", { productId, detail: error?.message }));
    void triggerLinkedProductStockSync([productId], "fragrantica_export_link").catch(() => {});
  }
  return { links: "linked", linksAdded: added, warehouseProductId: productId };
}
