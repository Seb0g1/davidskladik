async function runOperationPayload(job, options = {}) {
  const auditRequest = { session: { username: job.user || "system", role: job.role || "admin" } };
  if (job.type === "yandex-import-send") {
    return runOzonYandexImportSend({ ...(job.payload || {}), confirmed: true }, auditRequest);
  }
  if (job.type === "yandex-stock-sync") {
    const requestedLimit = Number(job.payload?.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const warehouse = await readWarehouse();
    const products = (warehouse.products || [])
      .filter((product) => product.marketplace === "ozon")
      .slice(0, limit);
    const existingOfferIds = getLocalYandexExportedOfferIdSet(warehouse.products || []);
    const result = await sendYandexStocksFromOzonProducts(products, {
      dryRun: job.payload?.dryRun === true,
      warehouseProducts: warehouse.products || [],
      existingOfferIds,
    });
    await appendAudit(auditRequest, "yandex.stock.sync", {
      entityType: "yandex_stock_sync",
      entityId: "ozon_to_yandex",
      limit,
      sent: Number(result.sent || 0),
      failed: Number(result.failed || 0),
      skipped: Number(result.skipped || 0),
      newValue: result,
    });
    return { ok: result.ok, limit, ...result };
  }
  if (job.type === "yandex-price-push") {
    const result = await runYandexPricePushOperation(job.payload || {});
    await appendAudit(auditRequest, "yandex.price.push", {
      entityType: "yandex_price_push",
      entityId: "yandex",
      newValue: result,
    });
    return result;
  }
  if (job.type === "linked-supplier-recovery") {
    const result = await runLinkedSupplierRecoveryOperation(job.payload || {});
    await appendAudit(auditRequest, "marketplace.linked.recovery", {
      entityType: "linked_supplier_recovery",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "ozon-linked-unarchive") {
    const result = await runLinkedSupplierRecoveryOperation({
      ...(job.payload || {}),
      marketplace: "ozon",
      force: true,
    });
    await appendAudit(auditRequest, "marketplace.ozon.linked_unarchive", {
      entityType: "ozon_linked_unarchive",
      entityId: "ozon",
      newValue: result,
    });
    return result;
  }
  if (job.type === "ozon-unarchive-queue-process") {
    const result = await processOzonUnarchiveQueue({
      source: "ozon_unarchive_queue_operation",
      limit: job.payload?.limit || ozonUnarchiveQueueBatchLimit,
      force: job.payload?.force === true,
    });
    await appendAudit(auditRequest, "ozon.unarchive_queue.operation", {
      entityType: "ozon_unarchive_queue",
      entityId: "operation",
      newValue: result,
    });
    return result;
  }
  if (job.type === "sales-automation-run") {
    const result = await runSalesAutomationOperation(job.payload || {});
    await appendAudit(auditRequest, "sales_automation.run", {
      entityType: "sales_automation",
      entityId: "manual",
      newValue: result,
    });
    return result;
  }
  if (job.type === "problem-products-repair") {
    const result = await runProblemProductsRepairOperation(job.payload || {}, auditRequest, options);
    await appendAudit(auditRequest, "problem_products.repair", {
      entityType: "problem_products",
      entityId: "bulk",
      newValue: result,
    });
    return result;
  }
  if (job.type === "brand-index-rebuild") {
    const result = await runBrandIndexRebuildOperation(job.payload || {});
    await appendAudit(auditRequest, "warehouse.brands.rebuild_index", {
      entityType: "brand_index",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "restore-archived-stock") {
    const result = await runArchivedStockRestoreOperation(job.payload || {}, options);
    await appendAudit(auditRequest, "marketplace.archived.restore_stock", {
      entityType: "archived_stock_restore",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "yandex-card-quality-ai-drafts") {
    const result = await runYandexCardQualityAiDraftOperation(job.payload || {}, options);
    await appendAudit(auditRequest, "yandex.card_quality.ai_drafts", {
      entityType: "yandex_card_quality",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "repair-pricemaster-group-links") {
    const result = await runPriceMasterGroupLinksRepairOperation(job.payload || {}, options);
    await appendAudit(auditRequest, "warehouse.links.repair_group", {
      entityType: "warehouse_pricemaster_group_links",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "marketplace-supplier-cart-preview") {
    const result = await runSupplierCartPreviewOperation(job.payload || {});
    await appendAudit(auditRequest, "supplier_cart.preview", {
      entityType: "supplier_cart",
      entityId: "preview",
      newValue: result,
    });
    return result;
  }
  if (job.type === "marketplace-supplier-cart-commit") {
    return runSupplierCartCommitOperation(job.payload || {}, auditRequest);
  }
  if (job.type === "health-deep") {
    return collectHealthDetails({ deep: true });
  }
  if (job.type === "restore-yandex-markups") {
    const result = await runRestoreYandexMarkupsOperation(job.payload || {});
    await appendAudit(auditRequest, "warehouse.yandex.restore_markups", {
      entityType: "yandex_markups",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  const error = new Error(`Unsupported operation type: ${job.type}`);
  error.statusCode = 400;
  throw error;
}

