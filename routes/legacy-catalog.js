"use strict";

function registerLegacyCatalogRoutes(app, deps) {
  const {
    collectHealthDetails,
    readSnapshot,
    readHistory,
    pool,
    cleanLimit,
    cleanText,
    likeSearch,
    requireAdmin,
    shouldUsePostgresStorage,
    getPrisma,
    auditRowToEntry,
    jsonFallbackEnabled,
    toDateOrNull,
    logger,
    getPriceMasterSearchCache,
    setPriceMasterSearchCache,
    readAppSettings,
    readAudit,
    readAuditFiltered,
    normalizePriceMasterPrice,
    normalizeSupplierName,
    searchPriceMasterSnapshotPartners,
    searchPriceMasterSnapshotOffers,
    listBrandFallbackCandidates,
    getOzonAccountByTarget,
    listPriceMasterPartners,
    getOzonCategoryList,
    ozonRequest,
    buildOzonAttributesTemplate,
    buildOzonPricePreview,
    buildOzonPricePayload,
    sendOzonPricePayloadChunks,
    buildOzonProductPreview,
    buildOzonManualProductItem,
    auditEntryProductIds,
    publicLinkAuditEntry,
  } = deps;

app.get("/api/health", async (_request, response) => {
  const health = await collectHealthDetails({ deep: true });
  response.status(health.ok ? 200 : 503).json(health);
});

app.get("/api/summary", async (_request, response, next) => {
  try {
    const snapshot = await readSnapshot();
    const [[products], [offerDocs], [latestDoc], [partners]] = await Promise.all([
      pool.query("SELECT COUNT(*) AS count FROM Products WHERE ProductID <> 0"),
      pool.query("SELECT COUNT(*) AS count FROM OfferDocs"),
      pool.query("SELECT MAX(DocDate) AS docDate FROM OfferDocs"),
      pool.query("SELECT COUNT(DISTINCT PartnerID) AS count FROM OfferDocs"),
    ]);

    const changeCounts = (snapshot.changes || []).reduce((acc, change) => {
      acc[change.type] = (acc[change.type] || 0) + 1;
      return acc;
    }, {});

    response.json({
      products: products[0].count,
      offerDocs: offerDocs[0].count,
      partners: partners[0].count,
      latestDocDate: latestDoc[0].docDate,
      snapshotCreatedAt: snapshot.createdAt,
      snapshotItems: Object.keys(snapshot.items || {}).length,
      changeCounts,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/products", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 100, 500);
    const search = String(request.query.search || "").trim();
    const params = [];
    let where = "WHERE p.ProductID <> 0";

    if (search) {
      where += " AND (p.ProductName LIKE ? OR p.ExtID LIKE ?)";
      params.push(likeSearch(search), likeSearch(search));
    }

    params.push(limit);
    const [rows] = await pool.query(
      `
      SELECT
        p.ProductID AS id,
        p.ProductName AS name,
        p.SalePrice AS salePrice,
        p.Stor AS stock,
        p.ExtID AS externalId,
        p.Vol AS volume,
        t.ProductTypeNameShort AS type,
        pack.PackName AS pack
      FROM Products p
      LEFT JOIN ProductTypes t ON t.ProductTypeID = p.ProductTypeID
      LEFT JOIN Packs pack ON pack.PackID = p.PackID
      ${where}
      ORDER BY p.ProductName
      LIMIT ?
      `,
      params,
    );

    response.json(rows);
  } catch (error) {
    next(error);
  }
});

app.get("/api/partners/search", async (request, response, next) => {
  try {
    const q = String(request.query.q || "").trim();
    const limit = cleanLimit(request.query.limit, 25, 80);
    if (!q) {
      return response.json({ items: [] });
    }

    const cacheKey = `partners:${q.toLowerCase()}:${limit}`;
    const cached = getPriceMasterSearchCache(cacheKey);
    if (cached) return response.json(cached);

    const snapshotRows = await searchPriceMasterSnapshotPartners(q, limit);
    if (snapshotRows) {
      const payload = { items: snapshotRows, source: "postgres_snapshot" };
      setPriceMasterSearchCache(cacheKey, payload);
      return response.json(payload);
    }

    const [rows] = await pool.query(
      `
      SELECT PartnerID AS id, PartnerName AS name
      FROM Partners
      WHERE PartnerName IS NOT NULL AND TRIM(PartnerName) <> '' AND PartnerName LIKE ?
      ORDER BY PartnerName ASC
      LIMIT ?
      `,
      [likeSearch(q), limit],
    );

    const payload = { items: rows };
    setPriceMasterSearchCache(cacheKey, payload);
    response.json(payload);
  } catch (error) {
    next(error);
  }
});
app.get("/api/ozon/brands/suggest", async (request, response, next) => {
  try {
    const query = cleanText(request.query.q);
    const categoryId = Number(request.query.categoryId || 0);
    const target = cleanText(request.query.target || "ozon");
    const limit = cleanLimit(request.query.limit, 20, 100);
    if (!query) return response.json({ brands: [] });
    if (!categoryId) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }
    const account = getOzonAccountByTarget(target) || getOzonAccountByTarget("ozon");
    if (!account) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }
    const categories = await getOzonCategoryList(account);
    const selectedCategory = categories.find((item) => Number(item.id) === categoryId);
    const descriptionTypeId = Number(selectedCategory?.descriptionTypeId || 0);
    if (!descriptionTypeId) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }

    const payload = {
      attribute_id: 85,
      description_category_id: categoryId,
      type_id: descriptionTypeId,
      language: "DEFAULT",
      limit,
      last_value_id: 0,
      value: query,
    };
    const data = await ozonRequest("/v1/description-category/attribute/values", payload, account);
    const raw = Array.isArray(data.result)
      ? data.result
      : Array.isArray(data.result?.values)
        ? data.result.values
        : data.values || [];
    const brands = Array.isArray(raw)
      ? raw
          .map((item) => cleanText(item.value || item.name))
          .filter(Boolean)
          .slice(0, 40)
      : [];
    if (!brands.length) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }
    response.json({ brands, source: "ozon" });
  } catch (error) {
    logger.warn("ozon brand suggest failed", { detail: error?.message || String(error) });
    const fallback = await listBrandFallbackCandidates(request.query.q, 40);
    response.json({ brands: fallback, source: "fallback" });
  }
});

app.get("/api/ozon/categories/suggest", async (request, response, next) => {
  try {
    const query = cleanText(request.query.q);
    const target = cleanText(request.query.target || "ozon");
    if (query.length < 2) return response.json({ categories: [] });
    const account = getOzonAccountByTarget(target) || getOzonAccountByTarget("ozon");
    if (!account) return response.json({ categories: [] });
    const all = await getOzonCategoryList(account);
    const q = normalizeSupplierName(query);
    const categories = all
      .filter((item) => normalizeSupplierName(item.name).includes(q))
      .slice(0, 50);
    response.json({ categories });
  } catch (error) {
    logger.warn("ozon category suggest failed", { detail: error?.message || String(error) });
    response.json({ categories: [] });
  }
});

app.get("/api/ozon/categories/:id/attributes-template", async (request, response, next) => {
  try {
    const categoryId = Number(request.params.id || 0);
    const target = cleanText(request.query.target || "ozon");
    if (!categoryId) return response.json({ template: [] });
    const account = getOzonAccountByTarget(target) || getOzonAccountByTarget("ozon");
    if (!account) return response.json({ template: [] });
    const categories = await getOzonCategoryList(account);
    const selectedCategory = categories.find((item) => Number(item.id) === categoryId);
    const descriptionTypeId = Number(selectedCategory?.descriptionTypeId || 0);
    const data = await ozonRequest("/v1/description-category/attribute", {
      description_category_id: categoryId,
      ...(descriptionTypeId ? { type_id: descriptionTypeId } : {}),
      language: "DEFAULT",
    }, account);
    const rows = data.result || data.attributes || [];
    response.json({ template: buildOzonAttributesTemplate(rows) });
  } catch (error) {
    logger.warn("ozon attribute template failed", { detail: error?.message || String(error) });
    response.json({ template: [] });
  }
});

app.get("/api/offers", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 150, 500);
    const search = String(request.query.search || "").trim();
    const partner = String(request.query.partner || "").trim();
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const cacheKey = `offers:${search.toLowerCase()}:${partner}:${limit}:${usdRate.toFixed(4)}`;
    const cached = getPriceMasterSearchCache(cacheKey);
    if (cached) return response.json(cached);

    const snapshotRows = await searchPriceMasterSnapshotOffers({
      search,
      partner,
      limit,
      usdRate,
    });
    if (snapshotRows) {
      setPriceMasterSearchCache(cacheKey, snapshotRows);
      return response.json(snapshotRows);
    }

    const params = [];
    const conditions = ["r.Ignored = 0"];

    if (search) {
      conditions.push("(r.NativeName LIKE ? OR r.NativeID LIKE ? OR r.BarCode LIKE ?)");
      params.push(likeSearch(search), likeSearch(search), likeSearch(search));
    }

    if (partner) {
      conditions.push("d.PartnerID = ?");
      params.push(Number(partner));
    }

    params.push(limit);
    const [rows] = await pool.query(
      `
      SELECT
        r.RowID AS rowId,
        r.NativeID AS article,
        r.BarCode AS barcode,
        r.NativeName AS name,
        r.ProductID AS productId,
        r.NativePrice AS price,
        r.Active AS active,
        r.IsNew AS isNew,
        d.DocDate AS docDate,
        d.PartnerID AS partnerId,
        p.PartnerName AS partnerName
      FROM OfferRows r
      JOIN OfferDocs d ON d.DocID = r.DocID
      LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
      WHERE ${conditions.join(" AND ")}
      ORDER BY d.DocDate DESC, r.RowID DESC
      LIMIT ?
      `,
      params,
    );

    const payload = rows.map((row) => ({ ...row, ...normalizePriceMasterPrice(row.price, usdRate) }));
    setPriceMasterSearchCache(cacheKey, payload);
    response.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get("/api/partners", async (_request, response, next) => {
  try {
    const rows = await listPriceMasterPartners();
    response.json(rows.map((row) => ({ id: row.partnerId, name: row.name })));
  } catch (error) {
    next(error);
  }
});

app.get("/api/changes", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const snapshot = await readSnapshot();
    response.json({
      createdAt: snapshot.createdAt,
      changes: (snapshot.changes || []).slice(0, limit),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 300, 2000);
    response.json({ history: await readHistory(limit) });
  } catch (error) {
    next(error);
  }
});
app.get("/api/audit-log", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const filters = {
      user: request.query.user,
      action: request.query.action,
      q: request.query.q || request.query.product,
      dateFrom: request.query.dateFrom,
      dateTo: request.query.dateTo,
    };
    const audit = await readAuditFiltered(filters, limit);
    response.json({ audit, total: audit.length, filters });
  } catch (error) {
    next(error);
  }
});
app.get("/api/warehouse/products/audit", async (request, response, next) => {
  try {
    const productIds = cleanText(request.query.productId || request.query.productIds || "")
      .split(",")
      .map((id) => cleanText(id))
      .filter(Boolean);
    if (!productIds.length) return response.json({ items: [] });
    const idSet = new Set(productIds.map(String));
    const limit = cleanLimit(request.query.limit, 10, 50);
    const actions = new Set(["warehouse.link.save", "warehouse.links.bulk_save", "warehouse.link.delete"]);
    const audit = await readAudit(Math.max(200, limit * 20));
    const items = audit
      .filter((entry) => actions.has(entry.action))
      .filter((entry) => auditEntryProductIds(entry).some((id) => idSet.has(String(id))))
      .slice(0, limit)
      .map(publicLinkAuditEntry);
    response.json({ items });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon/prices/preview", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 500, 5000);
    const multiplier = Number(request.query.multiplier || process.env.OZON_PRICE_MULTIPLIER || 1);
    const onlyChanged = String(request.query.onlyChanged || "true") !== "false";
    response.json(await buildOzonPricePreview({ limit, multiplier, onlyChanged }));
  } catch (error) {
    next(error);
  }
});

// Отправка цен на Ozon меняет живой кабинет — только админ.
app.post("/api/ozon/prices/send", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({
        error: "Ozon prices were not sent because manual confirmation is required.",
      });
    }

    const items = Array.isArray(request.body.items) ? request.body.items : [];
    const prices = items
      .map((item) => buildOzonPricePayload(item))
      .filter((item) => item.offer_id && Number(item.price) > 0);

    if (!prices.length) {
      return response.status(400).json({ error: "No valid selected prices to send." });
    }

    const account = getOzonAccountByTarget(cleanText(request.body.target || "ozon"));
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const sent = await sendOzonPricePayloadChunks(account, prices);
    if (sent.failed.length) {
      return response.status(502).json({
        ok: false,
        sent: prices.length - sent.failed.length,
        failed: sent.failed.length,
        detail: sent.failed[0]?.error?.message || "Ozon price send failed",
        results: sent.results,
      });
    }

    response.json({
      ok: true,
      sent: prices.length,
      results: sent.results,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon/products/preview", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const search = String(request.query.search || "").trim();
    response.json(await buildOzonProductPreview({ limit, search }));
  } catch (error) {
    next(error);
  }
});

// Создание карточки в кабинете Ozon — только админ.
app.post("/api/ozon/products/create", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({
        error: "Ozon product was not created because manual confirmation is required.",
      });
    }

    const built = buildOzonManualProductItem(request.body);
    if (!built.ready) {
      return response.status(400).json({ error: "Не хватает обязательных полей Ozon.", missing: built.missing });
    }

    const account = getOzonAccountByTarget(cleanText(request.body.target || "ozon"));
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const data = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
    response.json({ ok: true, target: account.id, item: built.item, result: data });
  } catch (error) {
    next(error);
  }
});


}

module.exports = { registerLegacyCatalogRoutes };
