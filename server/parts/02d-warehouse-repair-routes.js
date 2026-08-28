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

app.post("/api/warehouse/links/fix-stale-row-ids", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "Postgres is unavailable." });
    const dryRun = request.body?.dryRun !== false;
    const staleLinks = await prisma.$queryRawUnsafe(`
      SELECT
        pl.id                           AS link_id,
        pl.product_id,
        pl.supplier_article,
        pl.partner_id,
        COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') AS pinned_row_id,
        pm_old.price::float            AS old_price,
        pm_old.active                  AS old_active,
        pm_old.native_name             AS old_name,
        pm_new.row_id                  AS new_row_id,
        pm_new.price::float            AS new_price,
        pm_new.native_name             AS new_name,
        wp.raw->>'offerId'             AS offer_id,
        wp.id                          AS product_id_str,
        wp.marketplace
      FROM product_links pl
      JOIN warehouse_products wp ON wp.id = pl.product_id
      LEFT JOIN pm_snapshot_items pm_old
        ON pm_old.row_id = COALESCE(pl.source_row_id, pl.raw->>'sourceRowId')
        AND pm_old.partner_id::text = pl.partner_id::text
      JOIN LATERAL (
        SELECT pm2.row_id, pm2.price, pm2.native_name, pm2.doc_date
        FROM pm_snapshot_items pm2
        WHERE pm2.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
          AND pm2.partner_id::text = pl.partner_id::text
          AND pm2.active = true
          AND pm2.price IS NOT NULL AND pm2.price > 0
        ORDER BY pm2.doc_date DESC, pm2.row_id DESC
        LIMIT 1
      ) pm_new ON true
      WHERE pl.raw->>'matchType' = 'selected_row'
        AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') IS NOT NULL
        AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') != ''
        AND pm_new.row_id != COALESCE(pl.source_row_id, pl.raw->>'sourceRowId')
        AND (
          pm_old.row_id IS NULL
          OR pm_old.active = false
          OR pm_old.price IS NULL
          OR pm_old.price = 0
        )
    `);

    const linkSummary = staleLinks.map((r) => ({
      offerId: r.offer_id,
      marketplace: r.marketplace,
      productId: r.product_id_str,
      partnerId: r.partner_id,
      article: r.supplier_article,
      oldRowId: r.pinned_row_id,
      oldPrice: r.old_price,
      oldActive: r.old_active,
      oldName: r.old_name,
      newRowId: r.new_row_id,
      newPrice: r.new_price,
      newName: r.new_name,
    }));

    if (!staleLinks.length) {
      return response.json({ ok: true, dryRun, found: 0, fixed: 0, productIds: [] });
    }
    if (dryRun) {
      return response.json({ ok: true, dryRun: true, found: staleLinks.length, fixed: 0, links: linkSummary, productIds: [...new Set(staleLinks.map((r) => r.product_id_str))] });
    }

    let fixed = 0;
    for (const row of staleLinks) {
      await prisma.$executeRawUnsafe(`
        UPDATE product_links
        SET raw = jsonb_set(
              jsonb_set(raw, '{sourceRowId}', $1::jsonb),
              '{resolvedBy}', '"stale_row_id_fix"'
            ),
            source_row_id = $3,
            updated_at = now()
        WHERE id = $2
      `, JSON.stringify(row.new_row_id), row.link_id, row.new_row_id);
      fixed++;
    }

    const productIds = [...new Set(staleLinks.map((r) => r.product_id_str))];
    logger.info("fix_stale_row_ids", { found: staleLinks.length, fixed, products: productIds.length });
    return response.json({ ok: true, dryRun: false, found: staleLinks.length, fixed, productIds });
  } catch (error) {
    next(error);
  }
});

// Targeted stock recovery: runs buildFreshWarehouseProducts + runSupplierRecoveryAutomation
// for specific product IDs without the full sendWarehousePrices marketplace price refresh
// that causes timeouts. Mirrors what the linked reconciler does per batch.
app.post("/api/warehouse/links/recover-stale-stocks", requireAdmin, async (request, response, next) => {
  try {
    const rawIds = Array.isArray(request.body?.productIds) ? request.body.productIds : [];
    const productIds = rawIds.map(cleanText).filter(Boolean).slice(0, 200);
    if (!productIds.length) return response.status(400).json({ error: "productIds required" });
    const fresh = await buildFreshWarehouseProducts(productIds, {
      livePriceMaster: true,
      batchPriceMaster: true,
      persistMutations: true,
      priceMasterTimeoutMs: autoPricePmTimeoutMs,
    });
    const recovery = await runSupplierRecoveryAutomation(
      { products: fresh },
      { productIds, source: "stale_stock_recovery", force: true },
    );
    logger.info("stale_stock_recovery", { productIds: productIds.length, fresh: fresh.length, recovery: { recovered: recovery.recovered, restoredStocks: recovery.restoredStocks } });
    response.json({ ok: true, productCount: productIds.length, fresh: fresh.length, recovery });
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
