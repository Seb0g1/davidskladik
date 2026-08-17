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
  const data = await ozonRequest("/v1/warehouse/list", {}, selectedAccount);
  const raw = data.result || data.warehouses || data.items || [];
  const items = (Array.isArray(raw) ? raw : raw.warehouses || raw.items || [])
    .map(normalizeOzonWarehouse)
    .filter(Boolean);
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
  const isPrimaryAccount = !account?.id || account.id === "ozon";
  if (!ozonWarehouseListEnabled && isPrimaryAccount) return [];

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


