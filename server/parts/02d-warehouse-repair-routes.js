app.post("/api/warehouse/products/enrich", async (request, response, next) => {
  try {
    const products = await enrichWarehouseProducts(request.body.productIds || request.body.ids || []);
    response.json({ ok: true, products });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/catalog/repair-linked", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(400).json({ error: "Postgres storage is required for linked catalog repair." });
    }
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "Postgres is unavailable." });
    const dryRun = request.body?.dryRun === true || request.query?.dryRun === "true";
    const limit = Math.max(0, Math.min(100000, Number(request.body?.limit || request.query?.limit || 0) || 0));
    const batchSize = Math.max(50, Math.min(2000, Number(request.body?.batchSize || request.query?.batchSize || 500) || 500));
    const inline = request.body?.inline === true || request.query?.inline === "true";
    if (!inline && !dryRun) {
      const job = await upsertOperationJob({
        id: crypto.randomUUID(),
        type: "repair-pricemaster-group-links",
        title: operationTitle("repair-pricemaster-group-links"),
        status: "queued",
        user: request.session?.username || "system",
        role: request.session?.role || "admin",
        payload: { limit, batchSize },
        progress: 0,
      });
      startOperationJob(job);
      return response.status(202).json({
        ok: true,
        accepted: true,
        jobId: job.id,
        statusUrl: `/api/operations/${job.id}`,
        job: operationJobPublic(job),
      });
    }
    const result = await repairLinkedWarehouseCatalogPostgres(prisma, { dryRun, limit, batchSize });
    return response.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/repair-weak-ozon", async (request, response, next) => {
  try {
    const limit = Math.max(1, Math.min(1000, Number(request.body?.limit || 400) || 400));
    const warehouse = await readWarehouse();
    const totalWeak = (warehouse.products || []).filter(isWeakOzonWarehouseProduct).length;
    const productIds = pickWeakOzonProductIds(warehouse.products || [], limit);
    if (!productIds.length) {
      return response.json({ ok: true, totalWeak: 0, processed: 0, updated: 0, remainingWeak: 0, products: [] });
    }
    const products = await enrichWarehouseProducts(productIds);
    const nextWarehouse = await readWarehouse();
    const remainingWeak = (nextWarehouse.products || []).filter(isWeakOzonWarehouseProduct).length;
    response.json({
      ok: true,
      totalWeak,
      processed: productIds.length,
      updated: products.length,
      remainingWeak,
      products,
    });
  } catch (error) {
    next(error);
  }
});
