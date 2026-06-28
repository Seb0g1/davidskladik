async function runSync() {
  let connection;
  try {
    connection = await pool.getConnection();
    const previous = await readSnapshot();
    const currentOffers = await getCurrentOffers(connection);
    const { currentItems, changes } = compareSnapshots(previous.items || {}, currentOffers);
    const syncId = crypto.randomUUID();
    const snapshot = {
      syncId,
      createdAt: new Date().toISOString(),
      items: currentItems,
      changes,
    };
    await writeSnapshot(snapshot);
    await appendHistory(snapshot);

    const result = {
      syncId,
      createdAt: snapshot.createdAt,
      items: Object.keys(currentItems).length,
      changes: changes.length,
      changeCounts: changes.reduce((acc, change) => {
        acc[change.type] = (acc[change.type] || 0) + 1;
        return acc;
      }, {}),
    };
    Object.defineProperty(result, "changedRows", {
      value: changes,
      enumerable: false,
      configurable: false,
    });
    return result;
  } finally {
    if (connection) connection.release();
  }
}

async function sendZeroStocksToMarketplace(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.offerId || !product?.target) {
      actions.push({
        id: product?.id || "",
        type: "zero_stock",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.offerId ? "missing_offer_id" : "missing_target"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 100)) {
        const payload = { stocks: await buildOzonStockPayloadItems(chunk, account, () => 0, { allWarehouses: true }) };
        if (!payload.stocks.length) continue;
        try {
          for (const stockChunk of chunkArray(payload.stocks, 100)) {
            await ozonRequest("/v2/products/stocks", { stocks: stockChunk }, account);
          }
          actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", ok: true })));
        } catch (error) {
          const detail = error?.message || "stock_zero_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", ok: false, error: detail })));
        }
      }
      continue;
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      const stockShops = yandexStockShops([shop]);
      if (!stockShops.length) {
        actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target: shop.id, ok: false, error: "yandex_stock_campaign_not_configured" })));
        continue;
      }
      for (const stockShop of stockShops) {
        for (const chunk of chunkArray(items, 100)) {
          try {
            await sendYandexStockChunk(stockShop, chunk.map((item) => ({ offerId: item.offerId, stock: 0 })));
            actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", target: stockShop.id, ok: true })));
          } catch (error) {
            const detail = error?.message || "stock_zero_failed";
            actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", target: stockShop.id, ok: false, error: detail })));
          }
        }
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target, ok: false, error: "unsupported_marketplace" })));
    }
  }

  return actions;
}

async function sendTargetStocksToMarketplace(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of pickTargetStockSendProducts(products)) {
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) continue;
      for (const chunk of chunkArray(items, 100)) {
        const payload = { stocks: await buildOzonStockPayloadItems(chunk, account, (item) => item.targetStock) };
        if (!payload.stocks.length) continue;
        try {
          for (const stockChunk of chunkArray(payload.stocks, 100)) {
            await ozonRequest("/v2/products/stocks", { stocks: stockChunk }, account);
          }
          actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", stock: item.targetStock, ok: true })));
        } catch (error) {
          const detail = error?.message || "target_stock_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", stock: item.targetStock, ok: false, error: detail })));
        }
      }
      continue;
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;
      const restoreActions = await restoreYandexArchivedStocks(items, [shop]);
      const restoreFailed = restoreActions.filter((item) => !item.ok);
      if (restoreFailed.length) {
        logger.warn("yandex stock restore before target send failed", {
          target: shop.id,
          items: restoreFailed.length,
        });
      }
      for (const stockShop of yandexStockShops([shop])) {
        for (const chunk of chunkArray(items, 100)) {
          try {
            await sendYandexStockChunk(stockShop, chunk.map((item) => ({ offerId: item.offerId, stock: item.targetStock })));
            actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", target: stockShop.id, stock: item.targetStock, ok: true })));
          } catch (error) {
            const detail = error?.message || "target_stock_failed";
            actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", target: stockShop.id, stock: item.targetStock, ok: false, error: detail })));
          }
        }
      }
    }
  }

  return actions;
}

async function archiveProductsOnMarketplaces(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.target || (product.marketplace === "yandex" && !cleanText(product.offerId))) {
      actions.push({
        id: product?.id || "",
        type: "archive",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.target ? "missing_target" : "missing_offer_id"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "archive", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 100)) {
        const productIds = chunk.map((item) => Number(item.productId || 0)).filter((id) => id > 0);
        if (!productIds.length) continue;
        try {
          await ozonRequest("/v1/product/archive", { product_id: productIds }, account);
          actions.push(...chunk.map((item) => ({ id: item.id, type: "archive", ok: true })));
        } catch (error) {
          const detail = error?.message || "archive_failed";
          const expected = isExpectedMarketplaceArchiveBlock(detail);
          actions.push(...chunk.map((item) => ({
            id: item.id,
            type: "archive",
            ok: false,
            skipped: expected,
            reason: expected ? "marketplace_has_stock" : undefined,
            error: detail,
          })));
        }
      }
      continue;
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "archive", target, offerId: item.offerId, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 200)) {
        const archiveResults = await sendYandexOfferArchiveState(shop, chunk.map((item) => item.offerId), true);
        const byOfferId = new Map(archiveResults.map((item) => [cleanText(item.offerId).toLowerCase(), item]));
        actions.push(...chunk.map((item) => {
          const result = byOfferId.get(cleanText(item.offerId).toLowerCase());
          return {
            id: item.id,
            type: "archive",
            target: shop.id,
            offerId: item.offerId,
            ok: Boolean(result?.ok),
            error: result?.ok ? undefined : (result?.error || "archive_failed"),
          };
        }));
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "archive", target, ok: false, error: "unsupported_marketplace" })));
    }
  }
  return actions;
}

async function restoreStocksOnMarketplaces(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.offerId || !product?.target) {
      actions.push({
        id: product?.id || "",
        type: "restore_stock",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.offerId ? "missing_offer_id" : "missing_target"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 100)) {
        const payload = {
          stocks: await buildOzonStockPayloadItems(
            chunk,
            account,
            (item) => Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
          ),
        };
        if (!payload.stocks.length) continue;
        try {
          for (const stockChunk of chunkArray(payload.stocks, 100)) {
            await ozonRequest("/v2/products/stocks", { stocks: stockChunk }, account);
          }
          actions.push(...chunk.map((item) => ({
            id: item.id,
            type: "restore_stock",
            stock: Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
            ok: true,
          })));
        } catch (error) {
          const detail = error?.message || "restore_stock_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", ok: false, error: detail })));
        }
      }
      continue;
    }
    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      const stockShops = yandexStockShops([shop]);
      if (!stockShops.length) {
        actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target: shop.id, ok: false, error: "yandex_stock_campaign_not_configured" })));
        continue;
      }
      const archivedItems = items.filter((item) => productLooksArchived(item));
      if (archivedItems.length) {
        await unarchiveProductsOnMarketplaces(archivedItems).catch((err) =>
          logger.warn("restore_stocks_unarchive_failed", { count: archivedItems.length, detail: err?.message || String(err) }),
        );
      }
      for (const stockShop of stockShops) {
        for (const chunk of chunkArray(items, 100)) {
          try {
            await sendYandexStockChunk(stockShop, chunk.map((item) => ({
              offerId: item.offerId,
              stock: Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
            })));
            actions.push(...chunk.map((item) => ({
              id: item.id,
              type: "restore_stock",
              target: stockShop.id,
              stock: Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
              ok: true,
            })));
          } catch (error) {
            const detail = error?.message || "restore_stock_failed";
            actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", target: stockShop.id, ok: false, error: detail })));
          }
        }
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target, offerId: item.offerId, ok: false, error: "unsupported_marketplace" })));
    }
  }
  return actions;
}

