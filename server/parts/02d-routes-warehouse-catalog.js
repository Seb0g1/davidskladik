
app.post("/api/warehouse/products/:id/repair", requireAdmin, async (request, response, next) => {
  try {
    const initialWarehouse = await readWarehouse();
    const seed = (initialWarehouse.products || []).find((product) => String(product.id) === String(request.params.id));
    if (!seed) return response.status(404).json({ error: "Warehouse product not found." });
    const initialGroup = expandWarehouseProductsToGroups(initialWarehouse.products || [], [seed]);
    const productIds = initialGroup.map((product) => String(product.id)).filter(Boolean).slice(0, 100);
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type: "problem-products-repair",
      title: operationTitle("problem-products-repair"),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: { productIds, source: "single_product_repair" },
      progress: 0,
    });
    startOperationJob(job);
    appendAudit(request, "warehouse.product.repair_queued", {
      productId: seed.id,
      offerId: seed.offerId,
      productIds,
      jobId: job.id,
    }).catch((auditError) => logger.warn("product repair queue audit append failed", { detail: auditError?.message || String(auditError) }));
    return response.status(202).json({ ok: true, accepted: true, productIds, job: operationJobPublic(job) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/diagnostics", async (request, response, next) => {
  try {
    const sku = cleanText(request.query.sku || request.query.q || request.query.offerId || "");
    const limit = cleanLimit(request.query.limit, 50, 100);
    const auditLimit = cleanLimit(request.query.auditLimit, 30, 100);
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    response.json(await buildWarehouseSkuDiagnostics(sku, { limit, auditLimit, usdRate }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/page", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const refreshPrices = request.query.refreshPrices === "true";
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    const page = Math.max(1, Number(request.query.page || 1) || 1);
    const pageSize = Math.min(250, Math.max(10, Number(request.query.pageSize || 60) || 60));
    const q = cleanText(request.query.q || "").toLowerCase();
    const autoOnly = request.query.autoOnly === "true";
    const linked = cleanText(request.query.linked || "all");
    const marketplace = cleanText(request.query.marketplace || "all");
    const stateCode = cleanText(request.query.state || "all");
    const brandFilter = cleanText(request.query.brand || "");
    const grouped = parseBooleanSetting(request.query.grouped, false);

    if (shouldUsePostgresStorage() && !sync && !refreshPrices) {
      const fastPage = await buildFastWarehousePage({
        page,
        pageSize,
        usdRate,
        filters: {
          q,
          autoOnly,
          linked,
          marketplace,
          state: stateCode,
          brand: brandFilter,
          groupPage: grouped,
        },
      });
      if (fastPage.partial || fastPage.sourceError) {
        logger.warn("warehouse page partial response", {
          page,
          pageSize,
          grouped,
          partial: Boolean(fastPage.partial),
          stale: Boolean(fastPage.stale),
          sourceError: cleanText(fastPage.sourceError || ""),
          items: Array.isArray(fastPage.items) ? fastPage.items.length : 0,
          total: Number(fastPage.total || 0),
        });
      }
      if (!fastPage.partial) {
        const priceRows = grouped
          ? (fastPage.groups || fastPage.items || []).flatMap((group) => group.products || [])
          : fastPage.items;
        queueChangedWarehousePrices(priceRows, "warehouse_page_detected_changed_prices");
      }
      if (grouped && fastPage.grouped) return response.json(fastPage);
      if (grouped) {
        const groups = buildWarehousePageProductGroups(fastPage.items).map(mapServerWarehousePageGroup);
        return response.json({
          ...fastPage,
          grouped: true,
          rowTotal: fastPage.total,
          total: Math.max(groups.length, Math.ceil(Number(fastPage.total || 0) / 1.6)),
          groups,
          items: groups,
        });
      }
      return response.json(fastPage);
    }

    const data = await buildWarehouseViewCached({ sync, usdRate, refreshPrices });
    let rows = Array.isArray(data.products) ? data.products.slice() : [];
    if (!sync && !refreshPrices) queueChangedWarehousePrices(rows, "warehouse_page_detected_changed_prices");

    const filters = {
      q,
      autoOnly,
      linked,
      marketplace,
      state: stateCode,
      brand: brandFilter,
    };
    rows = sortWarehouseProductsForSearch(
      preferWarehousePrimaryIdentityMatches(
        rows.filter((item) => warehousePageProductMatches(item, filters)),
        filters,
      ),
      filters,
    );

    const total = rows.length;
    const offset = (page - 1) * pageSize;
    const items = rows.slice(offset, offset + pageSize).map((item) => ({
      ...item,
      autoPriceEnabled: item.autoPriceEnabled !== false,
      links: Array.isArray(item.links) ? item.links : [],
      suppliers: Array.isArray(item.suppliers) ? item.suppliers : [],
      selectedSupplier: item.selectedSupplier || null,
      noSupplierAutomation: item.noSupplierAutomation || {},
      marketplaceState: item.marketplaceState || {},
      partial: false,
    }));

    const groups = grouped ? buildWarehousePageProductGroups(items) : null;
    response.json({
      createdAt: data.createdAt,
      updatedAt: data.updatedAt || null,
      totalAll: data.total,
      ready: data.ready,
      changed: data.changed,
      withoutSupplier: data.withoutSupplier,
      linkedArchived: data.linkedArchived || 0,
      ozonArchived: data.ozonArchived || 0,
      ozonInactive: data.ozonInactive || 0,
      ozonOutOfStock: data.ozonOutOfStock || 0,
      usdRate: data.usdRate,
      priceMaster: data.priceMaster || await getPriceMasterSnapshotMeta(),
      sourceError: data.sourceError || "",
      noSupplierAlerts: Array.isArray(data.noSupplierAlerts) ? data.noSupplierAlerts.slice(0, 10) : [],
      page,
      pageSize,
      total: grouped ? groups.length : total,
      rowTotal: total,
      grouped,
      groups: groups || undefined,
      hasMore: offset + items.length < total,
      items: groups || items,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/group-detail", async (request, response, next) => {
  let slotHeld = false;
  try {
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    const refreshPrices = request.query.refreshPrices === "true" || request.query.live === "true";
    const group = cleanText(request.query.group || "");
    if (!group) return response.status(400).json({ error: "Укажите group." });
    if (!refreshPrices && shouldUsePostgresStorage()) {
      const cachedDetail = await peekWarehouseGroupDetailCached(group, { usdRate, refreshPrices });
      if (cachedDetail) return response.json(cachedDetail);
    }
    const slotGranted = await acquireWarehousePageBuildSlot();
    if (!slotGranted) {
      return response.status(503).json({
        error: "Сервер занят загрузкой каталога. Повторите через секунду.",
        retryAfterMs: 1500,
      });
    }
    slotHeld = true;
    const detail = await buildWarehouseGroupDetail(group, {
      usdRate,
      refreshPrices,
      filters: {
        marketplace: cleanText(request.query.marketplace || "all"),
        state: cleanText(request.query.state || "all"),
      },
    });
    if (!detail) return response.status(404).json({ error: "Карточка не найдена." });
    response.json(detail);
  } catch (error) {
    next(error);
  } finally {
    if (slotHeld) releaseWarehousePageBuildSlot();
  }
});

app.get("/api/warehouse/products/:id/detail", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const refreshPrices = request.query.refreshPrices === "true";
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    if (shouldUsePostgresStorage() && !sync && !refreshPrices) {
      const detail = await buildWarehouseProductDetailFromPostgres(request.params.id, { usdRate });
      if (!detail) return response.status(404).json({ error: "Товар не найден." });
      return response.json(detail);
    }
    const data = await buildWarehouseViewCached({ sync, usdRate, refreshPrices });
    const product = (data.products || []).find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар не найден." });
    response.json({ product, createdAt: data.createdAt });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/no-supplier", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const refreshPrices = request.query.refreshPrices === "true";
    const data = await buildWarehouseViewCached({ sync, refreshPrices });
    response.json({
      createdAt: data.createdAt,
      total: data.total,
      withoutSupplier: data.withoutSupplier,
      alerts: buildNoSupplierAlerts(data.products, { limit: Number.POSITIVE_INFINITY }),
    });
  } catch (error) {
    next(error);
  }
});

