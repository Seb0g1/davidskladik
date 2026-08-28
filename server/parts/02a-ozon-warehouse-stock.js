function normalizeOzonWarehouse(input = {}) {
  const warehouseId = cleanText(input.warehouseId || input.warehouse_id || input.id);
  const warehouseName = cleanText(input.warehouseName || input.warehouse_name || input.name);
  return warehouseId || warehouseName ? { warehouseId, warehouseName } : null;
}

function normalizeOzonStockWarehouse(input = {}) {
  const normalized = normalizeOzonWarehouse(input);
  if (!normalized) return null;
  const present = Number(input.present || 0);
  const reserved = Number(input.reserved || 0);
  const stock = Number.isFinite(Number(input.stock))
    ? Number(input.stock)
    : Math.max(0, present - reserved);
  return {
    ...normalized,
    present: Number.isFinite(present) ? present : 0,
    reserved: Number.isFinite(reserved) ? reserved : 0,
    stock,
  };
}

function parseOzonStockWarehouseIds(account = {}) {
  const accountKey = cleanText(account.id || account.name || "ozon")
    .replace(/[^a-z0-9]/gi, "_")
    .toUpperCase();
  const accountSpecific = splitList(process.env[`OZON_STOCK_WAREHOUSE_IDS_${accountKey}`] || "");
  if (accountSpecific.length) return accountSpecific;
  // Only fall back to the generic env vars for the primary account ("OZON").
  // Secondary accounts must use their own OZON_STOCK_WAREHOUSE_IDS_<KEY> env var;
  // leaking Ozon1 warehouse IDs to Ozon2 causes "warehouse not found" API errors.
  if (accountKey !== "OZON") return [];
  return splitList(
    process.env.OZON_STOCK_WAREHOUSE_IDS
      || process.env.OZON_STOCK_WAREHOUSE_ID
      || "",
  );
}

function parseOzonStockWarehouseNames(account = {}) {
  const accountKey = cleanText(account.id || account.name || "ozon")
    .replace(/[^a-z0-9]/gi, "_")
    .toUpperCase();
  const accountSpecific = splitList(process.env[`OZON_STOCK_WAREHOUSE_NAMES_${accountKey}`] || "");
  if (accountSpecific.length) return accountSpecific.map((name) => normalizeSupplierName(name));
  if (accountKey !== "OZON") return [];
  return splitList(process.env.OZON_STOCK_WAREHOUSE_NAMES || "").map((name) => normalizeSupplierName(name));
}

async function getOzonWarehouses(account = null, { refresh = false } = {}) {
  const selectedAccount = account || getOzonAccountByTarget("ozon");
  const cacheKey = cleanText(selectedAccount?.id || selectedAccount?.clientId || "ozon");
  const cached = ozonWarehouseCache.get(cacheKey);
  if (!refresh && cached && Date.now() - cached.at < 10 * 60 * 1000) return cached.items;
  let items = [];
  try {
    const data = await ozonRequest("/v1/warehouse/list", {}, selectedAccount);
    const raw = data.result || data.warehouses || data.items || [];
    items = (Array.isArray(raw) ? raw : raw.warehouses || raw.items || [])
      .map(normalizeOzonWarehouse)
      .filter(Boolean);
  } catch (warehouseListError) {
    // /v1/warehouse/list is marked obsolete for some account types — fall back to delivery methods
    if (!String(warehouseListError?.message || "").includes("obsolete")) {
      // Always cache to avoid spamming the API on repeated failures
      ozonWarehouseCache.set(cacheKey, { at: Date.now(), items: [] });
      throw warehouseListError;
    }
    // Try several fallback endpoints to discover FBS warehouse IDs
    const fallbackEndpoints = [
      { path: "/v1/delivery-method/list", body: { filter: { status: "ACTIVE" }, limit: 50, offset: 0 }, extract: (d) => (Array.isArray(d.result) ? d.result : []).map((m) => ({ warehouseId: cleanText(String(m.warehouse_id || "")), warehouseName: cleanText(m.warehouse_name || m.name || "") })) },
      { path: "/v2/warehouse/list", body: {}, extract: (d) => { const raw = d.result || d.warehouses || d.items || []; return (Array.isArray(raw) ? raw : []).map((w) => ({ warehouseId: cleanText(String(w.warehouse_id || w.id || "")), warehouseName: cleanText(w.warehouse_name || w.name || "") })); } },
    ];
    let fallbackUsed = "";
    for (const { path, body, extract } of fallbackEndpoints) {
      try {
        const data = await ozonRequest(path, body, selectedAccount);
        const extracted = extract(data);
        const seen = new Set();
        for (const { warehouseId, warehouseName } of extracted) {
          if (warehouseId && !seen.has(warehouseId)) {
            seen.add(warehouseId);
            items.push({ warehouseId, warehouseName });
          }
        }
        fallbackUsed = path;
        break;
      } catch (fallbackError) {
        logger.warn("ozon warehouse fallback endpoint failed", { account: cacheKey, path, detail: fallbackError?.message || String(fallbackError) });
      }
    }
    // Last resort: discover warehouse from an existing product's stock data
    if (!items.length) {
      try {
        const productList = await ozonRequest("/v3/product/list", { filter: { visibility: "ALL" }, limit: 1, last_id: "" }, selectedAccount);
        const firstOfferId = productList.result?.items?.[0]?.offer_id;
        if (firstOfferId) {
          const stockData = await ozonRequest("/v4/product/info/stocks", { filter: { offer_id: [firstOfferId], visibility: "ALL" }, limit: 1, last_id: "" }, selectedAccount);
          const seen = new Set();
          for (const stockItem of stockData.result?.items || []) {
            for (const stockEntry of stockItem.stocks || []) {
              const warehouseId = cleanText(String(stockEntry.warehouse_id || ""));
              if (warehouseId && warehouseId !== "0" && !seen.has(warehouseId)) {
                seen.add(warehouseId);
                items.push({ warehouseId, warehouseName: cleanText(stockEntry.warehouse_name || "") });
              }
            }
          }
          if (items.length) fallbackUsed = "/v4/product/info/stocks";
        }
      } catch (stockError) {
        logger.warn("ozon warehouse fallback endpoint failed", { account: cacheKey, path: "/v4/product/info/stocks", detail: stockError?.message || String(stockError) });
      }
    }
    if (items.length) {
      logger.info("ozon warehouse discovery via fallback", { account: cacheKey, fallback: fallbackUsed, warehouses: items.length, warehouseIds: items.map((w) => w.warehouseId) });
    } else {
      logger.warn("ozon warehouse discovery failed — set OZON_STOCK_WAREHOUSE_IDS_OZON_3D10EC43 in env to configure manually", { account: cacheKey });
    }
  }
  // Always cache result (even empty) to avoid re-hitting the API on every product call
  ozonWarehouseCache.set(cacheKey, { at: Date.now(), items });
  return items;
}

async function resolveOzonStockWarehouses(account = null, product = null) {
  const configuredIds = parseOzonStockWarehouseIds(account);
  if (configuredIds.length) {
    return configuredIds.map((warehouseId) => ({ warehouseId, warehouseName: "" }));
  }

  const configuredNames = parseOzonStockWarehouseNames(account);
  const storedWarehouses = Array.isArray(product?.marketplaceState?.warehouses)
    ? product.marketplaceState.warehouses.map(normalizeOzonWarehouse).filter(Boolean)
    : [];
  if (storedWarehouses.length) {
    if (configuredNames.length) {
      const matchedStored = storedWarehouses.filter((warehouse) =>
        configuredNames.some((name) => normalizeSupplierName(warehouse.warehouseName).includes(name)),
      );
      if (matchedStored.length) return matchedStored;
    } else {
      return storedWarehouses;
    }
  }

  // For the primary account the operator has already configured OZON_STOCK_WAREHOUSE_IDS,
  // so respect the flag. For secondary accounts (Ozon2, Ozon3 …) there is no stored data
  // from import, so we must fetch from the API to get the correct warehouse_id.
  // Exception: when storedWarehouses is also empty (product never had FBS stock), there is
  // no fallback at all — fall through to API discovery so the stock payload includes a
  // warehouse_id (without it Ozon silently rejects the FBS stock update).
  const isPrimaryAccount = !account?.id || account.id === "ozon";
  if (!ozonWarehouseListEnabled && isPrimaryAccount && storedWarehouses.length > 0 && !configuredNames.length) return [];

  try {
    const warehouses = await getOzonWarehouses(account);
    if (configuredNames.length) {
      return warehouses.filter((warehouse) =>
        configuredNames.some((name) => normalizeSupplierName(warehouse.warehouseName).includes(name)),
      );
    }
    if (warehouses.length) return warehouses;
  } catch (error) {
    logger.warn("ozon warehouse list failed", {
      account: account?.id || account?.name || "ozon",
      detail: error?.message || String(error),
    });
  }
  return [];
}

async function buildOzonStockPayloadItems(items = [], account = null, stockResolver = () => 0, { allWarehouses = false } = {}) {
  const payloadItems = [];
  for (const item of items) {
    const offerId = cleanText(item.offerId || item.offer_id);
    if (!offerId) continue;
    const stock = Math.max(0, Math.round(Number(stockResolver(item) || 0)));
    const warehouses = await resolveOzonStockWarehouses(account, item);
    if (!warehouses.length) {
      payloadItems.push({ offer_id: offerId, stock });
      continue;
    }
    const targetWarehouses = allWarehouses ? warehouses : warehouses.slice(0, 1);
    for (const warehouse of targetWarehouses) {
      payloadItems.push({
        offer_id: offerId,
        warehouse_id: Number(warehouse.warehouseId),
        stock,
      });
    }
  }
  return payloadItems.filter((item) => item.offer_id && (item.warehouse_id || item.warehouse_id === undefined));
}


