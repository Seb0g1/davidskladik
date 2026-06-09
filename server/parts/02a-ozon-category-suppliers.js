const ozonCategoryCache = new Map();
const ozonCategoryCacheTtlMs = 10 * 60 * 1000;

function flattenOzonCategoryTree(nodes = [], result = []) {
  for (const node of nodes || []) {
    const id = Number(node.description_category_id || node.category_id || node.id || 0);
    const descriptionTypeId = Number(node.description_type_id || node.type_id || node.descriptionTypeId || 0);
    const name = cleanText(node.category_name || node.name || node.title);
    if (id && name) result.push({ id, name, descriptionTypeId });
    const children = node.children || node.child || node.items || [];
    if (Array.isArray(children) && children.length) flattenOzonCategoryTree(children, result);
  }
  return result;
}

async function getOzonCategoryList(account, { force = false } = {}) {
  const cacheKey = account?.id || "ozon";
  const cached = ozonCategoryCache.get(cacheKey);
  if (!force && cached && Date.now() - cached.at < ozonCategoryCacheTtlMs) return cached.items;
  const payload = { language: "DEFAULT" };
  const data = await ozonRequest("/v1/description-category/tree", payload, account);
  const tree = data.result || data.items || data.categories || [];
  const items = flattenOzonCategoryTree(Array.isArray(tree) ? tree : [tree], []);
  ozonCategoryCache.set(cacheKey, { at: Date.now(), items });
  return items;
}

function buildOzonAttributesTemplate(rows = []) {
  return (rows || [])
    .filter((row) => Number(row?.is_required) === 1 || row?.required === true)
    .slice(0, 40)
    .map((row) => ({
      id: Number(row.id || row.attribute_id || 0),
      values: [],
    }))
    .filter((row) => row.id > 0);
}

function buildNoSupplierAlerts(products = [], { limit = 12 } = {}) {
  const rows = (products || [])
    .filter((product) => !product.selectedSupplier && Number(product.supplierCount || 0) > 0)
    .map((product) => ({
      id: product.id,
      offerId: product.offerId,
      name: product.name,
      marketplace: product.marketplace,
      nextPrice: 0,
      supplierCount: Number(product.supplierCount || 0),
      availableSupplierCount: Number(product.availableSupplierCount || 0),
      action: "Проверить наличие",
    }));
  if (Number.isFinite(Number(limit)) && Number(limit) > 0) return rows.slice(0, Number(limit));
  return rows;
}

function syncWarehouseSuppliersFromPriceMaster(warehouse, partners = []) {
  if (!warehouse || !Array.isArray(warehouse.suppliers)) return { changed: false, imported: 0 };
  const byId = new Map();
  const byName = new Map();
  for (const supplier of warehouse.suppliers) {
    if (supplier.partnerId) byId.set(String(supplier.partnerId), supplier);
    byName.set(normalizeSupplierName(supplier.name), supplier);
  }

  let imported = 0;
  for (const partner of partners) {
    const keyName = normalizeSupplierName(partner.name);
    const existing = (partner.partnerId && byId.get(String(partner.partnerId))) || byName.get(keyName);
    if (existing) {
      if (!existing.partnerId && partner.partnerId) existing.partnerId = String(partner.partnerId);
      if (!existing.source) existing.source = "pricemaster";
      continue;
    }
    const id = partner.partnerId ? `pm-${partner.partnerId}` : `pm-${crypto.randomUUID()}`;
    const supplier = normalizeManagedSupplier({
      id,
      partnerId: partner.partnerId,
      source: "pricemaster",
      name: partner.name,
      priceCurrency: isInnaSupplierName(partner.name) ? "RUB" : "USD",
      stopped: false,
    });
    warehouse.suppliers.push(supplier);
    byName.set(keyName, supplier);
    if (supplier.partnerId) byId.set(String(supplier.partnerId), supplier);
    imported += 1;
  }
  return { changed: imported > 0, imported };
}
