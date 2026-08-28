function yandexOfferIdFromMapping(item = {}) {
  const offer = pickYandexOfferFromMapping(item);
  return cleanText(offer.offerId || item.offerId || item.mapping?.offerId || item.offer?.offerId);
}

function getLocalYandexExportedOfferIdSet(products = []) {
  const set = new Set();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const offerId = cleanText(normalized.offerId || normalized.offer_id).toLowerCase();
    if (!offerId) continue;
    if (normalized.marketplace === "yandex") set.add(offerId);
    const exports = normalized.exports || {};
    if (exports.yandex?.status === "sent") set.add(offerId);
    if (Object.values(exports).some((entry) => entry?.status === "sent" && /yandex/i.test(cleanText(entry.targetName || entry.marketplace || entry.target)))) {
      set.add(offerId);
    }
  }
  return set;
}

async function readYandexExistingOfferIdCache({ maxAgeMs = 24 * 60 * 60 * 1000 } = {}) {
  try {
    const data = JSON.parse(await fs.readFile(yandexExistingOffersCachePath, "utf8"));
    const ageMs = Date.now() - new Date(data.updatedAt || 0).getTime();
    if (maxAgeMs > 0 && (!Number.isFinite(ageMs) || ageMs > maxAgeMs)) return new Set();
    return new Set((Array.isArray(data.offerIds) ? data.offerIds : [])
      .map((id) => cleanText(id).toLowerCase())
      .filter(Boolean));
  } catch (error) {
    if (error.code === "ENOENT") return new Set();
    logger.warn("read Yandex existing offer cache failed", { detail: error?.message || String(error) });
    return new Set();
  }
}

async function writeYandexExistingOfferIdCache(offerIds = []) {
  const normalized = Array.from(new Set(Array.from(offerIds)
    .map((id) => cleanText(id).toLowerCase())
    .filter(Boolean)))
    .sort();
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(
    yandexExistingOffersCachePath,
    JSON.stringify({ updatedAt: new Date().toISOString(), offerIds: normalized }, null, 2),
    "utf8",
  );
  return new Set(normalized);
}

async function refreshYandexExistingOfferIdCache({ limit = Number(process.env.YANDEX_EXISTING_CATALOG_LIMIT || 50000) || 50000 } = {}) {
  const maxItems = Math.max(1, Math.min(100000, Number(limit) || 50000));
  const set = new Set();
  for (const shop of uniqueYandexShopsByBusiness()) {
    for (const archived of [false, true]) {
      const remaining = Math.max(0, maxItems - set.size);
      if (!remaining) break;
      const mappings = await getYandexOfferMappings(shop, remaining, { archived });
      for (const item of mappings) {
        const offerId = yandexOfferIdFromMapping(item).toLowerCase();
        if (offerId) set.add(offerId);
      }
    }
  }
  return writeYandexExistingOfferIdCache(set);
}

async function getKnownYandexExistingOfferIds(offerIds = [], {
  products = [],
  warnings = [],
  allowCatalogRefresh = false,
  allowDirectCheck = true,
} = {}) {
  const set = new Set([
    ...await readYandexExistingOfferIdCache(),
    ...getLocalYandexExportedOfferIdSet(products),
  ]);
  if (allowCatalogRefresh) {
    const refreshTimeoutMs = Math.max(1000, Number(process.env.YANDEX_EXISTING_CATALOG_TIMEOUT_MS || 20000) || 20000);
    try {
      const refreshed = await Promise.race([
        refreshYandexExistingOfferIdCache(),
        promiseTimeout(refreshTimeoutMs, "yandex_existing_catalog_timeout"),
      ]);
      for (const id of refreshed) set.add(id);
    } catch (error) {
      const label = error?.message === "yandex_existing_catalog_timeout"
        ? `таймаут ${Math.round(refreshTimeoutMs / 1000)} с`
        : error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось обновить полный список SKU (${label}), использую локальный кэш.`);
      logger.warn("yandex existing catalog refresh failed", { detail: label });
    }
  }
  const normalizedOfferIds = Array.from(new Set((offerIds || []).map(cleanText).filter(Boolean)));
  if (allowDirectCheck && normalizedOfferIds.length) {
    const checkTimeoutMs = Math.max(1000, Number(process.env.OZON_YANDEX_EXISTING_CHECK_TIMEOUT_MS || 8000) || 8000);
    try {
      const direct = await Promise.race([
        getExistingYandexOfferIdSet(normalizedOfferIds),
        promiseTimeout(checkTimeoutMs, "yandex_existing_check_timeout"),
      ]);
      for (const id of direct) set.add(id);
      if (direct.size) writeYandexExistingOfferIdCache(new Set([...set, ...direct]))
        .catch((error) => logger.warn("write Yandex existing offer cache failed", { detail: error?.message || String(error) }));
    } catch (error) {
      const label = error?.message === "yandex_existing_check_timeout"
        ? `таймаут ${Math.round(checkTimeoutMs / 1000)} с`
        : error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось проверить существующие SKU (${label}), использую локальный кэш.`);
      logger.warn("yandex existing offers check failed", { detail: label });
    }
  }
  return set;
}

