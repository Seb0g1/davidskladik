function productLooksArchived(product = {}) {
  const state = product.marketplaceState || {};
  const code = cleanText(state.code || product.status).toLowerCase();
  const visibility = cleanText(state.visibility || product.visibility).toUpperCase();
  return Boolean(
    product.archived
      || state.archived
      || code === "archived"
      || visibility === "ARCHIVED"
      || product.noSupplierAutomation?.archivedAt
  );
}

function pickArchivedStockRestoreCandidates(products = [], { marketplace = "all", limit = 30000 } = {}) {
  const marketplaceFilter = cleanText(marketplace || "all").toLowerCase();
  const max = Math.max(1, Math.min(50000, Math.round(Number(limit || 30000) || 30000)));
  return (Array.isArray(products) ? products : [])
    .filter((product) => {
      const productMarketplace = cleanText(product.marketplace).toLowerCase();
      if (!["ozon", "yandex"].includes(productMarketplace)) return false;
      if (marketplaceFilter !== "all" && productMarketplace !== marketplaceFilter) return false;
      if (!cleanText(product.offerId || product.offer_id)) return false;
      if (productMarketplace === "ozon" && !Number(product.productId || product.product_id || 0)) return false;
      return productLooksArchived(product);
    })
    .slice(0, max);
}

async function applyArchivedStockRestoreLocalPatch(warehouse, targetProducts, stockActions, unarchiveActions, stock, now = new Date().toISOString()) {
  const restoredStockIds = new Set((Array.isArray(stockActions) ? stockActions : []).filter((item) => item.ok).map((item) => String(item.id)));
  const unarchivedIds = new Set((Array.isArray(unarchiveActions) ? unarchiveActions : []).filter((item) => item.ok).map((item) => String(item.id)));
  const stockActionById = new Map((Array.isArray(stockActions) ? stockActions : []).map((item) => [String(item.id), item]));
  const unarchiveActionById = new Map((Array.isArray(unarchiveActions) ? unarchiveActions : []).map((item) => [String(item.id), item]));
  const touchedIds = new Set((Array.isArray(targetProducts) ? targetProducts : []).map((product) => String(product.id)));
  const changedProducts = [];
  for (const product of warehouse.products || []) {
    if (!touchedIds.has(String(product.id))) continue;
    const stockAction = stockActionById.get(String(product.id));
    const unarchiveAction = unarchiveActionById.get(String(product.id));
    if (stockAction) product.lastStockSend = marketplaceCommandFromAction(stockAction, product, now);
    if (unarchiveAction) product.lastArchiveSend = marketplaceCommandFromAction(unarchiveAction, product, now);
    product.targetStock = stock;
    product.noSupplierAutomation = product.noSupplierAutomation || {};
    product.noSupplierAutomation.stockZeroAt = null;
    product.noSupplierAutomation.archivedAt = null;
    product.noSupplierAutomation.recoveredAt = now;
    product.noSupplierAutomation.manualSellableAt = now;
    product.noSupplierAutomation.lastError = null;
    if (restoredStockIds.has(String(product.id)) || unarchivedIds.has(String(product.id))) {
      product.marketplaceState = {
        ...(product.marketplaceState || {}),
        code: "active",
        status: "active",
        archived: false,
        stock,
      };
      product.status = "active";
      product.archived = false;
    }
    product.updatedAt = now;
    changedProducts.push(product);
  }
  if (changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "archived_stock_restore", writeLinks: false });
  }
  return changedProducts.length;
}

async function runArchivedStockRestoreOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const stock = Math.max(1, Math.min(9999, Math.round(Number(payload?.stock || 3) || 3)));
  const requestedMarketplace = cleanText(payload?.marketplace || "yandex").toLowerCase();
  const marketplace = ["yandex", "ozon", "all"].includes(requestedMarketplace) ? requestedMarketplace : "yandex";
  const batchSize = Math.max(20, Math.min(300, Math.round(Number(payload?.batchSize || 100) || 100)));
  const reportProgress = async (progress, summary) => {
    if (typeof options.onProgress !== "function") return;
    await options.onProgress({
      progress: Math.max(5, Math.min(99, Math.round(Number(progress || 5) || 5))),
      summary,
    });
  };
  const warehouse = await readWarehouse();
  const candidates = pickArchivedStockRestoreCandidates(warehouse.products || [], { marketplace, limit });
  if (!candidates.length) {
    const result = {
      ok: true,
      scanned: Math.min(limit, (warehouse.products || []).length),
      candidates: 0,
      marketplace,
      stock,
      restoredStocks: 0,
      unarchived: 0,
      errors: [],
      summary: "Архивных товаров для восстановления не найдено.",
    };
    logger.info("archived stock restore complete", {
      candidates: 0,
      stock,
      restoredStocks: 0,
      unarchived: 0,
      sellableRecovered: 0,
      stockFailed: 0,
      unarchiveFailed: 0,
      errors: 0,
    });
    return result;
  }

  const targetProducts = candidates.map((product) => normalizeWarehouseProduct({
    ...product,
    targetStock: stock,
    marketplaceState: {
      ...(product.marketplaceState || {}),
      stock,
    },
  }));
  await reportProgress(8, `Найдено архивных товаров: ${targetProducts.length}. Запускаю восстановление пачками по ${batchSize}.`);
  const firstStockActions = [];
  const unarchiveActions = [];
  const secondStockActions = [];
  let localPatched = 0;
  const batches = chunkArray(targetProducts, batchSize);
  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index];
    const processedBefore = index * batchSize;
    const processedAfter = Math.min(targetProducts.length, processedBefore + batch.length);
    logger.info("archived stock restore batch started", {
      batch: index + 1,
      batches: batches.length,
      products: batch.length,
      processed: processedBefore,
      total: targetProducts.length,
    });
    const batchFirstStockActions = await restoreStocksOnMarketplaces(batch);
    firstStockActions.push(...batchFirstStockActions);
    await reportProgress(
      10 + ((processedBefore + Math.floor(batch.length / 3)) / Math.max(1, targetProducts.length)) * 80,
      `Восстанавливаю остатки: ${processedBefore + Math.floor(batch.length / 3)} из ${targetProducts.length}.`,
    );
    const batchUnarchiveActions = await verifyYandexUnarchiveActions(
      batch,
      await unarchiveProductsOnMarketplaces(batch),
    );
    unarchiveActions.push(...batchUnarchiveActions);
    await reportProgress(
      10 + ((processedBefore + Math.floor((batch.length * 2) / 3)) / Math.max(1, targetProducts.length)) * 80,
      `Разархивирую карточки: ${processedBefore + Math.floor((batch.length * 2) / 3)} из ${targetProducts.length}.`,
    );
    const batchSecondStockActions = await restoreStocksOnMarketplaces(batch);
    secondStockActions.push(...batchSecondStockActions);
    localPatched += await applyArchivedStockRestoreLocalPatch(
      warehouse,
      batch,
      [...batchFirstStockActions, ...batchSecondStockActions],
      batchUnarchiveActions,
      stock,
      new Date().toISOString(),
    );
    await reportProgress(
      10 + (processedAfter / Math.max(1, targetProducts.length)) * 80,
      `Обработано ${processedAfter} из ${targetProducts.length}.`,
    );
    logger.info("archived stock restore batch complete", {
      batch: index + 1,
      batches: batches.length,
      processed: processedAfter,
      total: targetProducts.length,
      localPatched,
    });
  }
  const stockActions = [...firstStockActions, ...secondStockActions];
  const productStatuses = summarizeSupplierRecoveryProducts(targetProducts, stockActions, unarchiveActions);

  const stockOkIds = new Set(stockActions.filter((item) => item.ok).map((item) => String(item.id)));
  const unarchiveOkIds = new Set(unarchiveActions.filter((item) => item.ok).map((item) => String(item.id)));
  const sellableRecovered = targetProducts.filter((product) => stockOkIds.has(String(product.id)) && unarchiveOkIds.has(String(product.id))).length;
  const errors = [...stockActions, ...unarchiveActions]
    .filter((item) => !item.ok)
    .map((item) => ({ id: item.id, offerId: item.offerId, type: item.type, target: item.target, error: item.error }));
  const restoredStocks = stockActions.filter((item) => item.ok).length;
  const unarchived = unarchiveActions.filter((item) => item.ok).length;
  const unarchivePending = unarchiveActions.filter((item) => item.pending).length;
  const stockFailed = stockActions.filter((item) => !item.ok).length;
  const unarchiveFailed = unarchiveActions.filter((item) => !item.ok).length;
  const result = {
    ok: errors.length === 0,
    partial: Boolean(errors.length && (restoredStocks || unarchived)),
    scanned: Math.min(limit, (warehouse.products || []).length),
    candidates: candidates.length,
    marketplace,
    stock,
    restoredStocks,
    unarchived,
    unarchivePending,
    sellableRecovered,
    stockFailed,
    unarchiveFailed,
    errors,
    productStatuses,
    summary: `Архивных товаров ${candidates.length}; остаток ${stock}; отправок остатка ${restoredStocks}; разархивировано ${unarchived}; готово к продаже ${sellableRecovered}; ошибки остатков ${stockFailed}; ошибки разархива ${unarchiveFailed}.`,
  };
  logger.info("archived stock restore complete", {
    candidates: result.candidates,
    stock,
    restoredStocks,
    unarchived,
    unarchivePending,
    sellableRecovered,
    stockFailed,
    unarchiveFailed,
    errors: errors.length,
  });
  return result;
}

