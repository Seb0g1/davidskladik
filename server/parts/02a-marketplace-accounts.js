function normalizeMarketplaceAccount(input = {}, current = {}) {
  const rawMarketplace = cleanText(input.marketplace || current.marketplace).toLowerCase();
  const marketplace = ["yandex", "avito"].includes(rawMarketplace) ? rawMarketplace : "ozon";
  const fallbackName = marketplace === "ozon" ? "Ozon" : marketplace === "avito" ? "Avito" : "Yandex Market";
  return {
    id: cleanText(input.id || current.id) || `${marketplace}-${crypto.randomUUID().slice(0, 8)}`,
    marketplace,
    name: cleanText(input.name ?? current.name) || fallbackName,
    clientId: cleanText(input.clientId ?? input.client_id ?? current.clientId),
    apiKey: cleanText(input.apiKey ?? input.api_key ?? current.apiKey),
    businessId: cleanText(input.businessId ?? input.business_id ?? current.businessId),
    campaignId: cleanText(input.campaignId ?? input.campaign_id ?? current.campaignId),
    hidden: Boolean(input.hidden ?? current.hidden),
    syncEnabled: parseBooleanSetting(input.syncEnabled ?? input.sync_enabled, current.syncEnabled !== false),
    createdAt: current.createdAt || input.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function readMarketplaceAccountsSync() {
  try {
    const data = JSON.parse(fsSync.readFileSync(marketplaceAccountsPath, "utf8"));
    const accounts = Array.isArray(data.accounts) ? data.accounts : [];
    return accounts.map((account) => normalizeMarketplaceAccount(account));
  } catch (_error) {
    return [];
  }
}

async function readMarketplaceAccounts() {
  return readMarketplaceAccountsSync();
}

async function writeMarketplaceAccounts(accounts) {
  await fs.mkdir(dataDir, { recursive: true });
  const normalized = accounts.map((account) => normalizeMarketplaceAccount(account));
  await fs.writeFile(
    marketplaceAccountsPath,
    JSON.stringify({ updatedAt: new Date().toISOString(), accounts: normalized }, null, 2),
  );
  return normalized;
}

function getEnvOzonAccounts() {
  if (!process.env.OZON_CLIENT_ID || !process.env.OZON_API_KEY) return [];
  return [
    {
      id: "ozon",
      marketplace: "ozon",
      name: process.env.OZON_NAME || "Ozon",
      clientId: process.env.OZON_CLIENT_ID,
      apiKey: process.env.OZON_API_KEY,
      source: "env",
      readOnly: true,
    },
  ];
}

function getEnvYandexShops() {
  try {
    const shops = JSON.parse(process.env.YANDEX_SHOPS_JSON || "[]");
    if (!Array.isArray(shops)) return [];
    return shops.map((shop, index) => ({
      id: cleanText(shop.id) || `yandex-env-${index + 1}`,
      marketplace: "yandex",
      name: cleanText(shop.name) || "Yandex Market",
      businessId: cleanText(shop.businessId || shop.business_id),
      campaignId: cleanText(shop.campaignId || shop.campaign_id),
      apiKey: cleanText(shop.apiKey || shop.api_key),
      source: "env",
      readOnly: true,
    }));
  } catch (_error) {
    return [];
  }
}

function getEnvAvitoAccounts() {
  if (!process.env.AVITO_CLIENT_ID || !process.env.AVITO_CLIENT_SECRET) return [];
  return [
    {
      id: "avito",
      marketplace: "avito",
      name: process.env.AVITO_NAME || "Avito",
      clientId: process.env.AVITO_CLIENT_ID,
      apiKey: process.env.AVITO_CLIENT_SECRET,
      source: "env",
      readOnly: true,
    },
  ];
}

function getMarketplaceAccounts() {
  const envAccounts = [...getEnvOzonAccounts(), ...getEnvYandexShops(), ...getEnvAvitoAccounts()];
  const localAccounts = readMarketplaceAccountsSync().map((account) => ({ ...account, source: "local", readOnly: false }));
  const localById = new Map(localAccounts.map((account) => [account.id, account]));
  const hiddenIds = new Set(localAccounts.filter((account) => account.hidden).map((account) => account.id));
  const usedIds = new Set();
  const mergedEnvAccounts = envAccounts
    .filter((account) => !hiddenIds.has(account.id))
    .map((account) => {
      usedIds.add(account.id);
      const override = localById.get(account.id);
      return override
        ? { ...account, ...override, source: "local", readOnly: false, inheritedFromEnv: true }
        : account;
    });
  const standaloneLocalAccounts = localAccounts.filter((account) => !account.hidden && !usedIds.has(account.id));
  return [...mergedEnvAccounts, ...standaloneLocalAccounts];
}

function getHiddenMarketplaceAccounts() {
  const envById = new Map([...getEnvOzonAccounts(), ...getEnvYandexShops(), ...getEnvAvitoAccounts()].map((account) => [account.id, account]));
  return readMarketplaceAccountsSync()
    .filter((account) => account.hidden)
    .map((account) => ({ ...(envById.get(account.id) || account), ...account, source: "local", readOnly: false }));
}

function maskSecret(value) {
  const text = cleanText(value);
  if (!text) return "";
  if (text.length <= 6) return `${text[0] || ""}***`;
  return `${text.slice(0, 3)}...${text.slice(-3)}`;
}

function sanitizeMarketplaceAccount(account = {}) {
  return {
    id: account.id,
    marketplace: account.marketplace,
    name: account.name,
    clientId: account.clientId ? maskSecret(account.clientId) : "",
    apiKey: account.apiKey ? maskSecret(account.apiKey) : "",
    businessId: account.businessId || "",
    campaignId: account.campaignId || "",
    configured: account.marketplace === "ozon" || account.marketplace === "avito"
      ? Boolean(account.clientId && account.apiKey)
      : Boolean(account.apiKey && account.businessId),
    source: account.source || "local",
    readOnly: Boolean(account.readOnly),
    inheritedFromEnv: Boolean(account.inheritedFromEnv),
    syncEnabled: account.syncEnabled !== false,
    updatedAt: account.updatedAt || account.createdAt || null,
  };
}

function findMarketplaceAccount(id) {
  const accountId = cleanText(id);
  if (!accountId) return null;
  return getMarketplaceAccounts().find((account) => account.id === accountId) || null;
}

async function testMarketplaceAccountConnection(account = {}) {
  const marketplace = cleanText(account.marketplace).toLowerCase();
  if (marketplace === "ozon") {
    if (!account.clientId || !account.apiKey) {
      const error = new Error("Для проверки Ozon нужны Client-Id и Api-Key.");
      error.statusCode = 400;
      throw error;
    }
    const data = await ozonRequest(
      "/v3/product/list",
      { filter: { visibility: "ALL" }, limit: 1, last_id: "" },
      account,
    );
    const sampleCount = Array.isArray(data?.result?.items) ? data.result.items.length : 0;
    return {
      ok: true,
      marketplace: "ozon",
      sampleCount,
      message: "Ozon подключен. Ключи работают, список товаров доступен.",
      checkedAt: new Date().toISOString(),
    };
  }

  if (marketplace === "yandex") {
    if (!account.businessId || !account.apiKey) {
      const error = new Error("Для проверки Yandex Market нужны Business ID и Api-Key.");
      error.statusCode = 400;
      throw error;
    }
    const mappings = await getYandexOfferMappings(account, 1);
    return {
      ok: true,
      marketplace: "yandex",
      sampleCount: Array.isArray(mappings) ? mappings.length : 0,
      message: "Yandex Market подключен. Ключи работают, каталог доступен.",
      checkedAt: new Date().toISOString(),
    };
  }

  if (marketplace === "avito") {
    if (!account.clientId || !account.apiKey) {
      const error = new Error("Для проверки Avito нужны Client ID и Client Secret.");
      error.statusCode = 400;
      throw error;
    }
    await getAvitoAccessToken(account, { forceRefresh: true });
    try {
      const profile = await getAvitoAutoloadProfile(account);
      return {
        ok: true,
        marketplace: "avito",
        autoloadEnabled: Boolean(profile?.autoload_enabled),
        message: "Avito подключен. Ключи работают, профиль автозагрузки доступен.",
        checkedAt: new Date().toISOString(),
      };
    } catch (error) {
      if (error?.statusCode === 404) {
        return {
          ok: true,
          marketplace: "avito",
          autoloadEnabled: false,
          message: "Avito подключен. Ключи работают, профиль автозагрузки ещё не создан.",
          checkedAt: new Date().toISOString(),
        };
      }
      throw error;
    }
  }

  const error = new Error("Неизвестный маркетплейс.");
  error.statusCode = 400;
  throw error;
}

function accountPayloadWithSecretFallback(body = {}, current = {}) {
  const payload = { ...body };
  if (!cleanText(payload.clientId ?? payload.client_id) && current.clientId) payload.clientId = current.clientId;
  if (!cleanText(payload.apiKey ?? payload.api_key) && current.apiKey) payload.apiKey = current.apiKey;
  if (!cleanText(payload.businessId ?? payload.business_id) && current.businessId) payload.businessId = current.businessId;
  if (!cleanText(payload.campaignId ?? payload.campaign_id) && current.campaignId) payload.campaignId = current.campaignId;
  return payload;
}

async function appendAudit(request, action, details = {}) {
  const entry = {
    at: new Date().toISOString(),
    user: request.session?.username || "system",
    role: request.session?.role || "admin",
    action,
    productId: details.productId || details.productIds || null,
    oldValue: details.oldValue ?? details.before ?? null,
    newValue: details.newValue ?? details.after ?? null,
    details,
  };
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const user = entry.user && entry.user !== "system"
        ? await prisma.appUser.findUnique({ where: { username: entry.user } }).catch(() => null)
        : null;
      await prisma.auditLog.create({
        data: {
          username: entry.user,
          userId: user?.id || null,
          action: entry.action,
          entityType: cleanText(details.entityType || action.split(".")[0]) || null,
          entityId: cleanText(details.entityId || details.productId || details.id || "") || null,
          oldValue: cloneAuditValue(entry.oldValue),
          newValue: cloneAuditValue(entry.newValue),
          details: cloneAuditValue(details) || {},
          createdAt: toDateOrNull(entry.at) || new Date(),
        },
      });
      if (!jsonFallbackEnabled()) return;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("append audit postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  await fs.appendFile(auditLogPath, `${JSON.stringify(entry)}\n`, "utf8");
}

function auditRowToEntry(row = {}) {
  return {
    at: row.createdAt ? row.createdAt.toISOString() : null,
    user: row.username,
    action: row.action,
    productId: row.entityId || row.details?.productId || null,
    oldValue: row.oldValue,
    newValue: row.newValue,
    details: row.details || {},
  };
}

async function readAudit(limit = 200) {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().auditLog.findMany({
        take: limit,
        orderBy: { createdAt: "desc" },
      });
      return rows.map(auditRowToEntry);
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read audit postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const content = await fs.readFile(auditLogPath, "utf8");
    return content.trim().split("\n").filter(Boolean).slice(-limit).reverse().map((line) => JSON.parse(line));
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

async function readAuditSince(since) {
  if (shouldUsePostgresStorage()) {
    try {
      const sinceDate = toDateOrNull(since) || new Date(0);
      const rows = await getPrisma().auditLog.findMany({
        where: { createdAt: { gte: sinceDate } },
        orderBy: { createdAt: "asc" },
      });
      return rows.map(auditRowToEntry);
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read audit since postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const sinceMs = new Date(since).getTime();
    const content = await fs.readFile(auditLogPath, "utf8");
    return content
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line))
      .filter((entry) => new Date(entry.at).getTime() >= sinceMs);
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

function isAccountSyncEnabled(account = {}) {
  return account.syncEnabled !== false;
}

function getOzonAccounts({ includeSyncDisabled = false } = {}) {
  return getMarketplaceAccounts()
    .filter((account) => account.marketplace === "ozon")
    .filter((account) => includeSyncDisabled || isAccountSyncEnabled(account));
}

function getYandexShops({ includeSyncDisabled = false } = {}) {
  return getMarketplaceAccounts()
    .filter((account) => account.marketplace === "yandex")
    .filter((account) => includeSyncDisabled || isAccountSyncEnabled(account));
}

function getAvitoAccounts({ includeSyncDisabled = false } = {}) {
  return getMarketplaceAccounts()
    .filter((account) => account.marketplace === "avito")
    .filter((account) => includeSyncDisabled || isAccountSyncEnabled(account));
}

function getAvitoAccountByTarget(targetId) {
  const accounts = getAvitoAccounts();
  const target = cleanText(targetId || "");
  if (!target || target === "avito") return accounts[0] || null;
  return accounts.find((account) => (
    cleanText(account.id) === target
    || cleanText(account.clientId) === target
    || cleanText(account.name).toLowerCase() === target.toLowerCase()
  )) || null;
}

function getOzonAccountByTarget(targetId) {
  const accounts = getOzonAccounts();
  const target = cleanText(targetId || "");
  if (target === "ozon") return accounts[0] || null;
  return accounts.find((account) => (
    cleanText(account.id) === target
    || cleanText(account.clientId) === target
  )) || null;
}

function getYandexShopByTarget(targetId) {
  const shops = getYandexShops();
  const target = cleanText(targetId || "");
  const normalizedTarget = target.toLowerCase();
  if (target === "yandex") return shops[0] || null;
  const exact = shops.find((shop) => (
    cleanText(shop.id).toLowerCase() === normalizedTarget
    || cleanText(shop.campaignId).toLowerCase() === normalizedTarget
    || cleanText(shop.businessId).toLowerCase() === normalizedTarget
    || cleanText(shop.name).toLowerCase() === normalizedTarget
    || parseYandexCampaignIds(shop.campaignId).some((campaignId) => cleanText(campaignId).toLowerCase() === normalizedTarget)
    || parseYandexCampaignIds(shop.campaignId).some((campaignId) => `${cleanText(shop.id).toLowerCase()}-${cleanText(campaignId).toLowerCase()}` === normalizedTarget)
  ));
  if (exact) return exact;
  if (shops.length === 1 && target && target !== "ozon") return shops[0] || null;
  return null;
}

function matchesOzonTarget(targetId, accountId) {
  const target = cleanText(targetId || "");
  return target === cleanText(accountId || "") || target === "ozon";
}

function matchesYandexTarget(targetId, shopId) {
  const target = cleanText(targetId || "");
  const shop = getYandexShopByTarget(target);
  if (shop) return cleanText(shop.id).toLowerCase() === cleanText(shopId || "").toLowerCase();
  return target.toLowerCase() === cleanText(shopId || "").toLowerCase() || target.toLowerCase() === "yandex";
}
